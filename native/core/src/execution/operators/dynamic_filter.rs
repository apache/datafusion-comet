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

//! Runtime dynamic filter for hash join probe sides.
//!
//! [`DynamicFilterExec`] evaluates a join's [`DynamicFilterPhysicalExpr`] against
//! probe-side batches before they reach the hash probe. The expression starts as a
//! `lit(true)` placeholder and is populated by DataFusion's `HashJoinExec` build phase
//! (min/max bounds plus `InList` or hash-table-lookup membership). Until then — or if
//! the join never populates it — batches pass through untouched, so correctness never
//! depends on population.
//!
//! [`attach_join_dynamic_filter`] rewires an eligible `HashJoinExec` so that the join
//! and a new `DynamicFilterExec` wrapping its probe child share the same filter.

use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{lit, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

/// Stop evaluating the filter for the remainder of a partition stream when, after at
/// least [`GUARD_MIN_ROWS`] filtered rows, it keeps more than this fraction of them.
const GUARD_MAX_SELECTIVITY: f64 = 0.95;
/// Minimum number of rows to observe before the selectivity guard may disable the
/// filter, so a few unrepresentative leading batches don't make the decision.
const GUARD_MIN_ROWS: usize = 65_536;

/// Filters probe-side batches with a join's shared [`DynamicFilterPhysicalExpr`].
///
/// Distinct from a generic `FilterExec` in three ways: a pass-through fast path while
/// the filter is still the constant-`true` placeholder, a selectivity guard that
/// disables evaluation on non-selective streams, and dedicated metrics
/// (`dynamic_filter_rows_pruned`).
#[derive(Debug)]
pub struct DynamicFilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl DynamicFilterExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, predicate: Arc<DynamicFilterPhysicalExpr>) -> Self {
        // Filtering preserves schema, ordering, and partitioning.
        let cache = Arc::clone(input.properties());
        Self {
            input,
            predicate,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    pub fn predicate(&self) -> &Arc<DynamicFilterPhysicalExpr> {
        &self.predicate
    }
}

impl DisplayAs for DynamicFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometDynamicFilterExec")
    }
}

impl ExecutionPlan for DynamicFilterExec {
    fn name(&self) -> &str {
        "CometDynamicFilterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DynamicFilterExec::new(
            children.swap_remove(0),
            Arc::clone(&self.predicate),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let rows_pruned =
            MetricBuilder::new(&self.metrics).counter("dynamic_filter_rows_pruned", partition);
        Ok(Box::pin(DynamicFilterStream {
            schema: self.input.schema(),
            input,
            predicate: Arc::clone(&self.predicate),
            baseline_metrics,
            rows_pruned,
            rows_evaluated: 0,
            rows_kept: 0,
            guard_disabled: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct DynamicFilterStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    baseline_metrics: BaselineMetrics,
    rows_pruned: Count,
    /// Rows seen since the filter became a real (non-placeholder) predicate.
    rows_evaluated: usize,
    rows_kept: usize,
    /// Set once the selectivity guard decides the filter is not worth evaluating.
    guard_disabled: bool,
}

impl DynamicFilterStream {
    fn filter_batch(&mut self, batch: RecordBatch) -> DataFusionResult<Option<RecordBatch>> {
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        match self.predicate.evaluate(&batch)? {
            // Placeholder (or degenerate all-true) predicate: pass through untouched.
            // Not counted toward the selectivity guard — the real filter may not have
            // arrived yet.
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => Ok(Some(batch)),
            // Constant false/null (e.g. empty build side): the whole batch is pruned.
            ColumnarValue::Scalar(_) => {
                self.rows_pruned.add(batch.num_rows());
                self.rows_evaluated += batch.num_rows();
                Ok(None)
            }
            ColumnarValue::Array(array) => {
                let mask = as_boolean_array(&array)?;
                let filtered = filter_record_batch(&batch, mask)?;
                let kept = filtered.num_rows();
                self.rows_pruned.add(batch.num_rows() - kept);
                self.rows_evaluated += batch.num_rows();
                self.rows_kept += kept;
                if self.rows_evaluated >= GUARD_MIN_ROWS
                    && (self.rows_kept as f64 / self.rows_evaluated as f64) > GUARD_MAX_SELECTIVITY
                {
                    self.guard_disabled = true;
                }
                if kept == 0 {
                    Ok(None)
                } else {
                    Ok(Some(filtered))
                }
            }
        }
    }
}

impl Stream for DynamicFilterStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if self.guard_disabled {
                        self.baseline_metrics.record_output(batch.num_rows());
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    match self.filter_batch(batch) {
                        Ok(Some(filtered)) => {
                            self.baseline_metrics.record_output(filtered.num_rows());
                            return Poll::Ready(Some(Ok(filtered)));
                        }
                        // Entire batch pruned: keep polling the input.
                        Ok(None) => continue,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                other => return other,
            }
        }
    }
}

impl RecordBatchStream for DynamicFilterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Attaches a runtime dynamic filter to an eligible [`HashJoinExec`], wrapping its
/// probe (right) child in a [`DynamicFilterExec`] that shares the same filter.
///
/// Accepts either a `HashJoinExec` or a `ProjectionExec` directly above one (the shape
/// `HashJoinExec::swap_inputs` produces) and returns the input plan unchanged when the
/// join is not eligible. Eligibility mirrors DataFusion's own gate: the probe side must
/// be preserved under the ON clause (`JoinType::on_lr_is_preserved().1`), which admits
/// Inner, Left, LeftSemi, RightSemi, LeftAnti, and LeftMark joins — a probe row removed
/// by the filter could not have matched any build row, so results are unchanged.
///
/// Callers must not pass null-aware anti joins (Spark NOT IN semantics); that gate
/// lives at the call site where the flag is known.
pub fn attach_join_dynamic_filter(
    plan: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    // swap_inputs may have inserted a projection above the join to restore column order.
    if plan.is::<datafusion::physical_plan::projection::ProjectionExec>() {
        let child = Arc::clone(plan.children()[0]);
        let new_child = attach_join_dynamic_filter(child)?;
        return plan.with_new_children(vec![new_child]);
    }

    let Some(hash_join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };
    if !hash_join.join_type().on_lr_is_preserved().1 {
        return Ok(plan);
    }
    let probe_keys: Vec<Arc<dyn PhysicalExpr>> = hash_join
        .on()
        .iter()
        .map(|(_, right)| Arc::clone(right))
        .collect();
    if probe_keys.is_empty() {
        return Ok(plan);
    }

    let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(probe_keys, lit(true)));
    let wrapped_probe: Arc<dyn ExecutionPlan> = Arc::new(DynamicFilterExec::new(
        Arc::clone(hash_join.right()),
        Arc::clone(&dynamic_filter),
    ));
    let new_join = hash_join
        .builder()
        .with_new_children(vec![Arc::clone(hash_join.left()), wrapped_probe])?
        .build()?
        .with_dynamic_filter_expr(dynamic_filter)
        .map_err(|e| {
            DataFusionError::Internal(format!("failed to attach join dynamic filter: {e}"))
        })?;
    Ok(Arc::new(new_join))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinType;
    use datafusion::common::NullEquality;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{col, BinaryExpr};
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::prelude::SessionContext;

    fn int_batch(name: &str, values: Vec<i32>) -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap();
        (schema, batch)
    }

    fn memory_exec(schema: &SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let config = MemorySourceConfig::try_new(&[batches], Arc::clone(schema), None).unwrap();
        Arc::new(DataSourceExec::new(Arc::new(config)))
    }

    #[tokio::test]
    async fn test_pass_through_then_filter_then_constant_false() {
        let (schema, batch) = int_batch("a", (0..100).collect());
        let input = memory_exec(&schema, vec![batch]);
        let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![col("a", &schema).unwrap()],
            lit(true),
        ));
        let exec = Arc::new(DynamicFilterExec::new(input, Arc::clone(&predicate)));
        let task_ctx = SessionContext::new().task_ctx();

        // Placeholder: everything passes through.
        let batches = collect(Arc::clone(&exec) as _, Arc::clone(&task_ctx))
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 100);

        // Populated: a >= 90 keeps 10 rows.
        predicate
            .update(Arc::new(BinaryExpr::new(
                col("a", &schema).unwrap(),
                Operator::GtEq,
                lit(90),
            )))
            .unwrap();
        let batches = collect(Arc::clone(&exec) as _, Arc::clone(&task_ctx))
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
        let pruned = exec
            .metrics()
            .unwrap()
            .sum_by_name("dynamic_filter_rows_pruned")
            .map(|m| m.as_usize())
            .unwrap_or(0);
        assert_eq!(pruned, 90);

        // Constant false (e.g. empty build side): everything is pruned.
        predicate.update(lit(false)).unwrap();
        let batches = collect(Arc::clone(&exec) as _, task_ctx).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    }

    fn test_hash_join(join_type: JoinType) -> Arc<dyn ExecutionPlan> {
        let (build_schema, build_batch) = int_batch("a", vec![10, 20]);
        let (probe_schema, probe_batch) = int_batch("b", (0..100).collect());
        let join_on = vec![(
            col("a", &build_schema).unwrap(),
            col("b", &probe_schema).unwrap(),
        )];
        Arc::new(
            HashJoinExec::try_new(
                memory_exec(&build_schema, vec![build_batch]),
                memory_exec(&probe_schema, vec![probe_batch]),
                join_on,
                None,
                &join_type,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_attach_wraps_probe_side_and_preserves_results() {
        let plain = test_hash_join(JoinType::Inner);
        let attached = attach_join_dynamic_filter(Arc::clone(&plain) as _).unwrap();

        let join = attached
            .downcast_ref::<HashJoinExec>()
            .expect("expected HashJoinExec");
        assert!(join.dynamic_filter_expr().is_some());
        let wrapper = join
            .right()
            .downcast_ref::<DynamicFilterExec>()
            .expect("probe side should be wrapped in CometDynamicFilterExec");

        let task_ctx = SessionContext::new().task_ctx();
        let expected = collect(plain as _, Arc::clone(&task_ctx)).await.unwrap();
        let actual = collect(Arc::clone(&attached), Arc::clone(&task_ctx))
            .await
            .unwrap();
        let expected_rows: usize = expected.iter().map(|b| b.num_rows()).sum();
        let actual_rows: usize = actual.iter().map(|b| b.num_rows()).sum();
        assert_eq!(expected_rows, 2);
        assert_eq!(actual_rows, 2);

        // The build phase populated the filter and probe rows were pruned before the join.
        let pruned = wrapper
            .metrics()
            .unwrap()
            .sum_by_name("dynamic_filter_rows_pruned")
            .map(|m| m.as_usize())
            .unwrap_or(0);
        assert!(pruned > 0, "expected probe rows to be pruned, got 0");
    }

    #[tokio::test]
    async fn test_attach_skips_non_probe_preserved_join_types() {
        for join_type in [JoinType::Right, JoinType::Full, JoinType::RightAnti] {
            let plain = test_hash_join(join_type);
            let attached = attach_join_dynamic_filter(Arc::clone(&plain) as _).unwrap();
            let join = attached.downcast_ref::<HashJoinExec>().unwrap();
            assert!(
                join.dynamic_filter_expr().is_none(),
                "join type {join_type:?} must not get a dynamic filter"
            );
            assert!(join.right().downcast_ref::<DynamicFilterExec>().is_none());
        }
    }
}
