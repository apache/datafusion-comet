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

//! A DataFusion filter whose metrics remain owned by its Spark plan node.
//!
//! Comet normally keeps a one-to-one Spark/native plan tree so native metric
//! handles map back to the corresponding Spark operator. Some execution-local
//! rewrites need to replace a filter's child. DataFusion gives that replacement
//! a new private metric set, so this adapter owns the stable metric set and
//! registers the handles from the filter that actually executes.

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, ChildrenPropertiesMode, ReplaceChildrenOptions,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

#[derive(Debug)]
pub(crate) struct CometFilterExec {
    filter: FilterExec,
    metrics: ExecutionPlanMetricsSet,
}

impl CometFilterExec {
    pub(crate) fn from_datafusion(filter: FilterExec) -> Self {
        Self {
            filter,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub(crate) fn input(&self) -> &Arc<dyn ExecutionPlan> {
        self.filter.input()
    }

    pub(crate) fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        self.filter.predicate()
    }

    pub(crate) fn has_projection(&self) -> bool {
        self.filter.projection().is_some()
    }

    fn replace_input(
        &self,
        input: Arc<dyn ExecutionPlan>,
        options: ReplaceChildrenOptions,
    ) -> Result<FilterExec> {
        let replaced = Arc::new(self.filter.clone()).replace_children(vec![input], options)?;
        let Some(filter) = replaced.downcast_ref::<FilterExec>() else {
            return internal_err!("FilterExec child replacement changed its plan type");
        };
        Ok(filter.clone())
    }

    /// Replace the child for one execution while keeping the metric identity
    /// owned by the permanent Spark filter node.
    pub(crate) fn with_execution_input(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            filter: self.replace_input(
                input,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
            metrics: self.metrics.clone(),
        }))
    }
}

impl DisplayAs for CometFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.filter.fmt_as(t, f)
    }
}

impl ExecutionPlan for CometFilterExec {
    fn name(&self) -> &str {
        self.filter.name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.filter.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.filter.input()]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.filter.apply_expressions(f)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.filter.maintains_input_order()
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("CometFilterExec requires one child");
        }
        Ok(Arc::new(Self {
            filter: self.replace_input(children.remove(0), options)?,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
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

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Every execution gets fresh DataFusion filter metrics. Register their
        // handles on the stable set before returning the stream, including when
        // opening the child fails.
        let filter = self.replace_input(
            Arc::clone(self.filter.input()),
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        let result = filter.execute(partition, context);
        for metric in filter.metrics().unwrap_or_default().iter() {
            self.metrics.register(Arc::clone(metric));
        }
        result
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.filter.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.filter.statistics_from_inputs(input_stats, args)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.filter.cardinality_effect()
    }

    fn fetch(&self) -> Option<usize> {
        self.filter.fetch()
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let filter = self.filter.with_fetch(fetch)?;
        let filter = filter.downcast_ref::<FilterExec>()?.clone();
        Some(Arc::new(Self {
            filter,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        let filter = self.filter.with_preserve_order(preserve_order)?;
        let filter = filter.downcast_ref::<FilterExec>()?.clone();
        Some(Arc::new(Self {
            filter,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::stats::Precision;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_plan::statistics::StatisticsContext;

    fn filter() -> Result<FilterExec> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let partitions = [1..11, 11..41]
            .into_iter()
            .map(|values| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from_iter_values(values))],
                )
                .map(|batch| vec![batch])
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let input = MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&schema), None)?;
        let predicate = binary(col("key", &schema)?, Operator::Gt, lit(5i32), &schema)?;
        FilterExec::try_new(predicate, input)
    }

    #[test]
    fn expression_visitor_preserves_predicate_root_and_recursion() -> Result<()> {
        let filter = CometFilterExec::from_datafusion(filter()?);
        assert_eq!(filter.predicate().children().len(), 2);

        for recursion in [TreeNodeRecursion::Continue, TreeNodeRecursion::Stop] {
            let mut visits = 0;
            let result = filter.apply_expressions(&mut |expression| {
                visits += 1;
                assert!(Arc::ptr_eq(expression, filter.predicate()));
                Ok(recursion)
            })?;
            assert_eq!(visits, 1);
            assert_eq!(result, recursion);
        }
        Ok(())
    }

    #[test]
    fn statistics_match_datafusion_for_each_partition() -> Result<()> {
        let filter = filter()?;
        let wrapper = CometFilterExec::from_datafusion(filter.clone());
        let context = StatisticsContext::new();
        let mut row_counts = Vec::new();

        for partition in [None, Some(0), Some(1)] {
            let args = StatisticsArgs::new().with_partition(partition);
            let expected = context.compute(&filter, &args)?;
            let actual = context.compute(&wrapper, &args)?;
            assert_eq!(actual, expected);
            assert_ne!(actual.num_rows, Precision::Absent);
            row_counts.push(actual.num_rows);
        }
        assert_ne!(row_counts[0], row_counts[1]);
        assert_ne!(row_counts[1], row_counts[2]);
        Ok(())
    }
}
