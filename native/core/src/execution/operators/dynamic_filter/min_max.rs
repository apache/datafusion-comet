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

//! Connect one partial MIN/MAX aggregate's improving bound to its Parquet reader.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result, Statistics};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::min_max::{Max, Min};
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::{OrderingRequirements, PhysicalExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::distribution_requirements::InputDistributionRequirements;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};

use super::parquet_reader::{parquet_file_source, try_attach_parquet_reader_filter};

/// Own an unexecuted aggregate template and metric handles. Each execution owns
/// a fresh aggregate, accumulator bounds, and reader predicate; none of that live
/// state is retained by the template or shared with another Spark task.
#[derive(Debug)]
pub(crate) struct MinMaxReaderFilterExec {
    template: AggregateExec,
    config: ConfigOptions,
    metrics: ExecutionPlanMetricsSet,
}

struct RuntimeAggregate {
    aggregate: Arc<dyn ExecutionPlan>,
    reader_filter_attached: bool,
}

impl MinMaxReaderFilterExec {
    // Spark planner integration is a separate step of apache/datafusion-comet#5774.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn try_new(
        aggregate: &AggregateExec,
        config: &ConfigOptions,
    ) -> Result<Option<Self>> {
        if !config.optimizer.enable_dynamic_filter_pushdown
            || !config.optimizer.enable_aggregate_dynamic_filter_pushdown
            || aggregate.mode() != &AggregateMode::Partial
            || !aggregate.group_expr().is_true_no_grouping()
            || aggregate.limit_options().is_some()
            || aggregate.filter_expr().iter().any(Option::is_some)
            || aggregate.input().output_partitioning().partition_count() != 1
            || aggregate.input().fetch().is_some()
        {
            return Ok(None);
        }
        let [expression] = aggregate.aggr_expr() else {
            return Ok(None);
        };
        let function = expression.fun().inner();
        if !(function.is::<Min>() || function.is::<Max>())
            || expression.is_distinct()
            || !expression.order_bys().is_empty()
        {
            return Ok(None);
        }
        let arguments = expression.expressions();
        let [key] = arguments.as_slice() else {
            return Ok(None);
        };
        if !key.is::<Column>()
            || !matches!(
                key.data_type(aggregate.input().schema().as_ref())?,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        {
            return Ok(None);
        }
        // Keep expressions, residuals, limits, and native/JVM boundaries out of
        // this first path. The shared helper still handles reader column mapping
        // and per-file schema guards once this direct scan is accepted.
        let Some(scan) = aggregate.input().downcast_ref::<DataSourceExec>() else {
            return Ok(None);
        };
        let Some((_, source)) = parquet_file_source(scan) else {
            return Ok(None);
        };
        if source.filter().is_some()
            || source
                .projection()
                .is_some_and(|projection| projection.iter().any(|expr| !expr.expr.is::<Column>()))
        {
            // Pruning before a computed scan projection could change its stateful
            // evaluation or suppress errors, even for an unselected output column.
            return Ok(None);
        }
        Ok(Some(Self::new(aggregate, config.clone())?))
    }

    fn new(aggregate: &AggregateExec, config: ConfigOptions) -> Result<Self> {
        Ok(Self {
            template: Self::fresh_aggregate(aggregate, Arc::clone(aggregate.input()))?,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn fresh_aggregate(
        template: &AggregateExec,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<AggregateExec> {
        // AggregateExec::clone/replace_children keep shared accumulator bounds.
        // Replacing just its dynamic expression would leave those bounds stale.
        AggregateExec::try_new(
            *template.mode(),
            template.group_expr().clone(),
            template.aggr_expr().to_vec(),
            template.filter_expr().to_vec(),
            input,
            template.input_schema(),
        )
    }

    fn build_runtime_aggregate(&self) -> Result<RuntimeAggregate> {
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(Self::fresh_aggregate(
            &self.template,
            Arc::clone(self.template.input()),
        )?);
        let predicate = aggregate
            .dynamic_expressions_produced()
            .into_iter()
            .find_map(|expr| {
                (expr as Arc<dyn std::any::Any + Send + Sync>)
                    .downcast::<DynamicFilterPhysicalExpr>()
                    .ok()
            });
        let Some(predicate) = predicate else {
            return Ok(RuntimeAggregate {
                aggregate,
                reader_filter_attached: false,
            });
        };
        let reader =
            try_attach_parquet_reader_filter(self.template.input(), predicate, &self.config)?;
        let reader_filter_attached = reader.is_some();
        // Replacement preserves the fresh aggregate's predicate and accumulator
        // bounds, so the reader observes exactly the state the aggregate updates.
        let aggregate = match reader {
            Some(reader) => aggregate.replace_children(
                vec![reader],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
            None => aggregate,
        };
        Ok(RuntimeAggregate {
            aggregate,
            reader_filter_attached,
        })
    }

    fn execute_runtime_aggregate(
        &self,
        aggregate: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        super::execute_runtime_plan(aggregate, partition, context, &self.metrics)
    }
}

impl DisplayAs for MinMaxReaderFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometMinMaxReaderFilterExec: ")?;
        self.template.fmt_as(t, f)
    }
}

impl ExecutionPlan for MinMaxReaderFilterExec {
    fn name(&self) -> &str {
        "CometMinMaxReaderFilterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.template.properties()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.template.input_distribution_requirements()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.template.required_input_ordering()
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
        // The template's unused predicate is not a permanent-plan consumer.
        datafusion::physical_plan::apply_expression_roots(
            self.template.aggr_expr()[0].expressions(),
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
            return internal_err!("CometMinMaxReaderFilterExec requires one child");
        }
        let aggregate = Self::fresh_aggregate(&self.template, children.remove(0))?;
        match Self::try_new(&aggregate, &self.config)? {
            Some(wrapper) => Ok(Arc::new(wrapper)),
            None => Ok(Arc::new(aggregate)),
        }
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(&self.template, self.config.clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let runtime = self.build_runtime_aggregate()?;
        let attachment_metric = if runtime.reader_filter_attached {
            "dynamic_filter_minmax_filters_attached"
        } else {
            "dynamic_filter_minmax_filters_skipped"
        };
        MetricBuilder::new(&self.metrics)
            .counter(attachment_metric, partition)
            .add(1);
        self.execute_runtime_aggregate(runtime.aggregate, partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
