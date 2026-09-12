// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Connect a local TopK's improving threshold to its native Parquet reader.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{lit, Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::distribution_requirements::InputDistributionRequirements;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::parquet_reader_filter::try_attach_parquet_reader_filter;

/// Keep an unexecuted template in the Spark plan. Each stream gets a fresh TopK
/// and reader predicate, so a previous execution's threshold cannot discard rows
/// in a later execution. Only metric handles outlive the stream.
#[derive(Debug)]
pub(crate) struct DynamicFilterTopKExec {
    template: SortExec,
    config: ConfigOptions,
    metrics: ExecutionPlanMetricsSet,
}

struct RuntimeDynamicFilterTopK {
    sort: SortExec,
    reader_filter_attached: bool,
}

impl DynamicFilterTopKExec {
    pub(crate) fn try_new(sort: &SortExec, config: &ConfigOptions) -> Result<Option<Self>> {
        if !config.optimizer.enable_dynamic_filter_pushdown
            || !config.optimizer.enable_topk_dynamic_filter_pushdown
            || !matches!(sort.fetch(), Some(fetch) if fetch > 0)
            || sort.input().output_partitioning().partition_count() != 1
            || sort.expr().len() != 1
        {
            return Ok(None);
        }
        let key = &sort.expr()[0].expr;
        if !key.is::<Column>()
            || !matches!(
                key.data_type(sort.input().schema().as_ref())?,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        {
            return Ok(None);
        }
        Ok(Some(Self::new(sort, config.clone())))
    }

    fn new(sort: &SortExec, config: ConfigOptions) -> Self {
        Self {
            template: Self::fresh_sort(sort, Arc::clone(sort.input())),
            config,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn fresh_sort(template: &SortExec, input: Arc<dyn ExecutionPlan>) -> SortExec {
        SortExec::new(template.expr().clone(), input)
            .with_preserve_partitioning(template.preserve_partitioning())
            .with_fetch(template.fetch())
    }

    fn build_runtime_sort(&self) -> Result<RuntimeDynamicFilterTopK> {
        let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&self.template.expr()[0].expr)],
            lit(true),
        ));
        let reader = try_attach_parquet_reader_filter(
            self.template.input(),
            Arc::clone(&predicate),
            &self.config,
        )?;
        let reader_filter_attached = reader.is_some();
        let input = reader.unwrap_or_else(|| Arc::clone(self.template.input()));
        let sort = Self::fresh_sort(&self.template, input).with_dynamic_filter_expr(predicate)?;
        Ok(RuntimeDynamicFilterTopK {
            sort,
            reader_filter_attached,
        })
    }

    fn execute_runtime_sort(
        &self,
        sort: SortExec,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let result = sort.execute(partition, context);
        // SortExec registers its TopK metrics synchronously in execute(). Keep
        // those live counters, including on error, without retaining its filter.
        for metric in sort.metrics().unwrap_or_default().iter() {
            self.metrics.register(Arc::clone(metric));
        }
        drop(sort);
        let input = result?;
        // Release the reader and heap at EOF or error even when the caller keeps
        // the exhausted stream. Cancellation drops this stream and its input.
        let stream = futures::stream::unfold(Some(input), |input| async move {
            let mut input = input?;
            let batch = input.next().await?;
            let remaining = if batch.is_ok() { Some(input) } else { None };
            Some((batch, remaining))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for DynamicFilterTopKExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometDynamicFilterTopKExec: ")?;
        self.template.fmt_as(t, f)
    }
}

impl ExecutionPlan for DynamicFilterTopKExec {
    fn name(&self) -> &str {
        "CometDynamicFilterTopKExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.template.properties()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.template.input_distribution_requirements()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.template.maintains_input_order()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.template.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // The live predicate belongs to the runtime sort. The template's
        // unused predicate is not a consumer in this permanent plan.
        datafusion::physical_plan::apply_expression_roots(
            self.template.expr().iter().map(|order| &order.expr),
            f,
        )
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("CometDynamicFilterTopKExec requires one child");
        }
        let sort = Self::fresh_sort(&self.template, children.remove(0));
        match Self::try_new(&sort, &self.config)? {
            Some(wrapper) => Ok(Arc::new(wrapper)),
            None => Ok(Arc::new(sort)),
        }
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(&self.template, self.config.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let runtime = self.build_runtime_sort()?;
        let attachment_metric = if runtime.reader_filter_attached {
            "dynamic_filter_reader_filters_attached"
        } else {
            "dynamic_filter_reader_filters_skipped"
        };
        MetricBuilder::new(&self.metrics)
            .counter(attachment_metric, partition)
            .add(1);
        self.execute_runtime_sort(runtime.sort, partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fetch(&self) -> Option<usize> {
        self.template.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.template.cardinality_effect()
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.template.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.template.statistics_from_inputs(input_stats, args)
    }
}

#[cfg(test)]
mod tests;
