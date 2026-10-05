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

//! A DataFusion projection whose metrics remain owned by its Spark plan node.
//!
//! Comet normally keeps a one-to-one Spark/native plan tree so native metric
//! handles map back to the corresponding Spark operator. Some execution-local
//! rewrites need to replace a projection's child. DataFusion gives that replacement
//! a new private metric set, so this adapter owns the stable metric set and
//! registers the handles from the projection that actually executes.

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, ChildrenPropertiesMode, ReplaceChildrenOptions,
};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

#[derive(Debug)]
pub struct CometProjectionExec {
    projection: ProjectionExec,
    metrics: ExecutionPlanMetricsSet,
}

impl CometProjectionExec {
    pub fn from_datafusion(projection: ProjectionExec) -> Self {
        Self {
            projection,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        self.projection.input()
    }

    pub fn projection(&self) -> &ProjectionExec {
        &self.projection
    }

    fn replace_input(
        &self,
        input: Arc<dyn ExecutionPlan>,
        options: ReplaceChildrenOptions,
    ) -> Result<ProjectionExec> {
        let replaced = Arc::new(self.projection.clone()).replace_children(vec![input], options)?;
        let Some(projection) = replaced.downcast_ref::<ProjectionExec>() else {
            return internal_err!("ProjectionExec child replacement changed its plan type");
        };
        Ok(projection.clone())
    }

    /// Replace the child for one execution while keeping the metric identity
    /// owned by the permanent Spark projection node.
    pub fn with_execution_input(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            projection: self.replace_input(
                input,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
            metrics: self.metrics.clone(),
        }))
    }
}

impl DisplayAs for CometProjectionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.projection.fmt_as(t, f)
    }
}

impl ExecutionPlan for CometProjectionExec {
    fn name(&self) -> &str {
        self.projection.name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.projection.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.projection.input()]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.projection.apply_expressions(f)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.projection.benefits_from_input_partitioning()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.projection.maintains_input_order()
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("CometProjectionExec requires one child");
        }
        Ok(Arc::new(Self {
            projection: self.replace_input(children.remove(0), options)?,
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
        // Every execution gets fresh DataFusion projection metrics. Register their
        // handles on the stable set before returning the stream, including when
        // opening the child fails.
        let projection = self.replace_input(
            Arc::clone(self.projection.input()),
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        let result = projection.execute(partition, context);
        for metric in projection.metrics().unwrap_or_default().iter() {
            self.metrics.register(Arc::clone(metric));
        }
        result
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.projection.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.projection.statistics_from_inputs(input_stats, args)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.projection.cardinality_effect()
    }

    fn fetch(&self) -> Option<usize> {
        self.projection.fetch()
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let projection = self.projection.with_fetch(fetch)?;
        let projection = projection.downcast_ref::<ProjectionExec>()?.clone();
        Some(Arc::new(Self {
            projection,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        let projection = self.projection.with_preserve_order(preserve_order)?;
        let projection = projection.downcast_ref::<ProjectionExec>()?.clone();
        Some(Arc::new(Self {
            projection,
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

    fn projection() -> Result<ProjectionExec> {
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
        ProjectionExec::try_new(
            vec![
                (
                    binary(col("key", &schema)?, Operator::Plus, lit(5i32), &schema)?,
                    "offset".into(),
                ),
                (col("key", &schema)?, "key".into()),
            ],
            input,
        )
    }

    #[test]
    fn expression_visitor_preserves_roots_and_recursion() -> Result<()> {
        let wrapper = CometProjectionExec::from_datafusion(projection()?);
        assert_eq!(wrapper.projection().expr()[0].expr.children().len(), 2);
        for recursion in [TreeNodeRecursion::Continue, TreeNodeRecursion::Stop] {
            let mut visits = 0;
            let result = wrapper.apply_expressions(&mut |expression| {
                assert!(Arc::ptr_eq(
                    expression,
                    &wrapper.projection().expr()[visits].expr
                ));
                visits += 1;
                Ok(recursion)
            })?;
            assert_eq!(
                visits,
                if recursion == TreeNodeRecursion::Stop {
                    1
                } else {
                    2
                }
            );
            assert_eq!(result, recursion);
        }
        Ok(())
    }

    #[test]
    fn statistics_match_datafusion_for_each_partition() -> Result<()> {
        let projection = projection()?;
        let wrapper = CometProjectionExec::from_datafusion(projection.clone());
        let context = StatisticsContext::new();
        let mut row_counts = Vec::new();
        for partition in [None, Some(0), Some(1)] {
            let args = StatisticsArgs::new().with_partition(partition);
            let expected = context.compute(&projection, &args)?;
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
