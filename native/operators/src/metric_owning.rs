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

//! Unary DataFusion operators whose metrics remain owned by their Spark plan node.
//!
//! Execution-local child replacements share the owner's metric set. Each execution
//! registers the fresh DataFusion operator's handles on that set, even if opening
//! its child fails. Plan rewrites, including `reset_state`, start a new metric set.
//! Both keep the operator's Comet settings.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, ChildrenPropertiesMode, ReplaceChildrenOptions,
};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

/// The typed input accessor and Comet settings for the unary operators wrapped in this module.
pub trait UnaryExec: ExecutionPlan + Clone {
    /// Comet settings that child replacements and plan rewrites keep.
    type Settings: Clone + Debug + Default + Send + Sync;

    fn input(&self) -> &Arc<dyn ExecutionPlan>;
}

#[derive(Debug)]
pub struct MetricOwningExec<T: UnaryExec> {
    inner: T,
    metrics: ExecutionPlanMetricsSet,
    settings: T::Settings,
}

impl<T: UnaryExec> MetricOwningExec<T> {
    pub fn from_datafusion(inner: T) -> Self {
        Self {
            inner,
            metrics: ExecutionPlanMetricsSet::new(),
            settings: T::Settings::default(),
        }
    }

    pub(crate) fn inner(&self) -> &T {
        &self.inner
    }

    pub(crate) fn settings(&self) -> &T::Settings {
        &self.settings
    }

    pub(crate) fn settings_mut(&mut self) -> &mut T::Settings {
        &mut self.settings
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        self.inner.input()
    }

    fn replace_input(
        &self,
        input: Arc<dyn ExecutionPlan>,
        options: ReplaceChildrenOptions,
    ) -> Result<T> {
        let replaced = Arc::new(self.inner.clone()).replace_children(vec![input], options)?;
        let Some(inner) = replaced.downcast_ref::<T>() else {
            return internal_err!(
                "{} child replacement changed its plan type",
                self.inner.name()
            );
        };
        Ok(inner.clone())
    }

    fn rebuild(&self, inner: T, metrics: ExecutionPlanMetricsSet) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            inner,
            metrics,
            settings: self.settings.clone(),
        })
    }

    /// Replace the child for one execution while keeping the metric identity
    /// owned by the permanent Spark plan node.
    pub fn with_execution_input(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner = self.replace_input(
            input,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        Ok(self.rebuild(inner, self.metrics.clone()))
    }
}

impl<T: UnaryExec> DisplayAs for MetricOwningExec<T> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl<T: UnaryExec> ExecutionPlan for MetricOwningExec<T> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.input()]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("{} requires one child", self.inner.name());
        }
        let inner = self.replace_input(children.remove(0), options)?;
        Ok(self.rebuild(inner, ExecutionPlanMetricsSet::new()))
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
        // Each execution gets fresh DataFusion metrics. Register their handles
        // before returning the stream, including when opening the child fails.
        let inner = self.replace_input(
            Arc::clone(self.input()),
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        let result = inner.execute(partition, context);
        for metric in inner.metrics().unwrap_or_default().iter() {
            self.metrics.register(Arc::clone(metric));
        }
        result
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.inner.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.inner.statistics_from_inputs(input_stats, args)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.inner.cardinality_effect()
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.with_fetch(fetch)?;
        let inner = inner.downcast_ref::<T>()?.clone();
        Some(self.rebuild(inner, ExecutionPlanMetricsSet::new()))
    }

    // DataFusion 56 deprecates `with_preserve_order` because it no longer calls it, but the
    // method stays on the trait, so keep forwarding it to the wrapped operator.
    #[allow(deprecated)]
    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.with_preserve_order(preserve_order)?;
        let inner = inner.downcast_ref::<T>()?.clone();
        Some(self.rebuild(inner, ExecutionPlanMetricsSet::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit, IsNotNullExpr};
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use futures::TryStreamExt;

    use crate::{CometFilterExec, CometProjectionExec};

    fn input(replacement: bool) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
        let values = if replacement {
            vec![vec![Some(1), None, Some(3)], vec![Some(4), None]]
        } else {
            vec![vec![Some(1), None], vec![None, Some(2)]]
        };
        let partitions = values
            .into_iter()
            .map(|values| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(values))],
                )
                .map(|batch| vec![batch])
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(MemorySourceConfig::try_new_exec(&partitions, schema, None)?)
    }

    fn non_null(input: &Arc<dyn ExecutionPlan>) -> Arc<dyn PhysicalExpr> {
        Arc::new(IsNotNullExpr::new(
            col(input.schema().field(0).name(), &input.schema()).unwrap(),
        ))
    }

    fn filter(input: Arc<dyn ExecutionPlan>) -> Result<FilterExec> {
        FilterExec::try_new(non_null(&input), input)
    }

    fn projection(input: Arc<dyn ExecutionPlan>) -> Result<ProjectionExec> {
        ProjectionExec::try_new(vec![(non_null(&input), "valid".into())], input)
    }

    fn rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    async fn check_execution_metrics<T: UnaryExec>(
        make_exec: fn(Arc<dyn ExecutionPlan>) -> Result<T>,
        expected: [usize; 2],
    ) -> Result<()> {
        let original_input = input(false)?;
        let owner = MetricOwningExec::from_datafusion(make_exec(Arc::clone(&original_input))?);
        let replacement = input(true)?;
        let execution = owner.with_execution_input(Arc::clone(&replacement))?;
        assert!(Arc::ptr_eq(owner.input(), &original_input));
        assert!(Arc::ptr_eq(
            execution
                .downcast_ref::<MetricOwningExec<T>>()
                .unwrap()
                .input(),
            &replacement
        ));

        let context = Arc::new(TaskContext::default());
        let stream = execution.execute(0, Arc::clone(&context))?;
        // The original owner already holds the live handles before the stream is polled.
        let registered = owner.metrics().unwrap();
        assert_eq!(registered.output_rows(), Some(0));
        assert_eq!(rows(&stream.try_collect::<Vec<_>>().await?), expected[0]);
        assert_eq!(registered.output_rows(), Some(expected[0]));

        for run in 1..=2 {
            // collect executes both partitions, whose metrics must reach the same owner.
            let batches = collect(Arc::clone(&execution), Arc::clone(&context)).await?;
            assert_eq!(rows(&batches), expected.iter().sum::<usize>());
            assert_eq!(
                owner.metrics().unwrap().output_rows(),
                Some(expected[0] + run * expected.iter().sum::<usize>())
            );
        }
        // Later executions use fresh counters rather than updating the first execution's handles.
        assert_eq!(registered.output_rows(), Some(expected[0]));
        Ok(())
    }

    #[tokio::test]
    async fn execution_rewrites_share_metrics_across_partitions_and_runs() -> Result<()> {
        check_execution_metrics(filter, [2, 1]).await?;
        check_execution_metrics(projection, [3, 2]).await
    }

    async fn check_plan_metrics<T: UnaryExec>(
        make_exec: fn(Arc<dyn ExecutionPlan>) -> Result<T>,
        expected: usize,
        supports_fetch: bool,
    ) -> Result<()> {
        let owner = Arc::new(MetricOwningExec::from_datafusion(make_exec(input(true)?)?));
        let context = Arc::new(TaskContext::default());
        collect(
            Arc::clone(&owner) as Arc<dyn ExecutionPlan>,
            Arc::clone(&context),
        )
        .await?;
        assert_eq!(owner.metrics().unwrap().output_rows(), Some(expected));

        let mut rewrites = vec![
            Arc::clone(&owner).replace_children(
                vec![input(true)?],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
            Arc::clone(&owner).replace_children(
                vec![Arc::clone(owner.input())],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
            )?,
            Arc::clone(&owner).reset_state()?,
        ];
        let limited = owner.with_fetch(Some(1));
        assert_eq!(limited.is_some(), supports_fetch);
        if let Some(limited) = limited {
            assert_eq!(limited.fetch(), Some(1));
            rewrites.push(limited);
        }
        // The memory source does not support order-preserving rewrites for either operator.
        #[allow(deprecated)]
        let preserved = owner.with_preserve_order(true);
        assert!(preserved.is_none());
        for rewritten in rewrites {
            assert!(rewritten.downcast_ref::<MetricOwningExec<T>>().is_some());
            assert_eq!(rewritten.metrics().unwrap().iter().count(), 0);
            let batches = collect(Arc::clone(&rewritten), Arc::clone(&context)).await?;
            assert!(rows(&batches) > 0);
            assert_eq!(
                rewritten.metrics().unwrap().output_rows(),
                Some(rows(&batches))
            );
            assert_eq!(owner.metrics().unwrap().output_rows(), Some(expected));
        }
        Ok(())
    }

    #[tokio::test]
    async fn plan_rewrites_and_reset_start_new_metrics() -> Result<()> {
        check_plan_metrics(filter, 3, true).await?;
        check_plan_metrics(projection, 5, false).await
    }

    fn check_failed_execute<T: UnaryExec>(
        make_exec: fn(Arc<dyn ExecutionPlan>) -> Result<T>,
    ) -> Result<()> {
        let child = Arc::new(EmptyExec::new(input(true)?.schema()));
        let owner = MetricOwningExec::from_datafusion(make_exec(child)?);
        let execution = owner.with_execution_input(Arc::clone(owner.input()))?;
        assert_eq!(owner.metrics().unwrap().iter().count(), 0);
        let error = execution
            .execute(1, Arc::new(TaskContext::default()))
            .err()
            .unwrap();
        assert!(error.to_string().contains("EmptyExec invalid partition"));
        assert!(owner.metrics().unwrap().iter().count() > 0);
        Ok(())
    }

    #[test]
    fn failed_execute_registers_metrics_on_original_owner() -> Result<()> {
        check_failed_execute(filter)?;
        check_failed_execute(projection)
    }

    #[test]
    fn partitioning_hints_match_each_inner_operator() -> Result<()> {
        let filter = filter(input(true)?)?;
        let expected = filter.benefits_from_input_partitioning();
        assert_eq!(expected, vec![true]);
        assert_eq!(
            CometFilterExec::from_datafusion(filter).benefits_from_input_partitioning(),
            expected
        );

        let source = input(true)?;
        let schema = source.schema();
        for (expr, expected) in [
            (col("key", &schema)?, false),
            (
                binary(col("key", &schema)?, Operator::Plus, lit(5i32), &schema)?,
                true,
            ),
        ] {
            let projection =
                ProjectionExec::try_new(vec![(expr, "key".into())], Arc::clone(&source))?;
            assert_eq!(
                projection.benefits_from_input_partitioning(),
                vec![expected]
            );
            assert_eq!(
                CometProjectionExec::from_datafusion(projection).benefits_from_input_partitioning(),
                vec![expected]
            );
        }
        Ok(())
    }
}
