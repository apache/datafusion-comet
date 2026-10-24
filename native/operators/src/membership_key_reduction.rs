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

//! Opportunistically remove duplicate right-side rows before a left-semi join's sort.
//! Grouping complete integer tuples keeps the set of matching keys, including nulls.
//! The downstream sort restores ordering and the join preserves duplicate left rows.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::{make_array, ArrayRef, MutableArrayData};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::memory::get_record_batch_memory_size;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::execution_plan::{CardinalityEffect, EmissionType};
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

// Skip whole batches ending within the initial prefix. After that, small batches pass
// through unchanged, while an oversized or unprofitable attempt disables later attempts.
const PREFIX_ROWS: usize = 8192;
const MIN_BATCH_ROWS: usize = 1024;
const MAX_BATCH_ROWS: usize = 64 * 1024;

#[derive(Debug)]
pub struct MembershipKeyReductionExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl MembershipKeyReductionExec {
    /// The join planner must establish that the input consists of signed integers, and
    /// that rows equal in every column yield the same membership key without side effects.
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        // The helper can reorder rows in a partition and does not establish global uniqueness.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            input.boundedness(),
        ));
        Self {
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for MembershipKeyReductionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometMembershipKeyReductionExec")
    }
}

impl ExecutionPlan for MembershipKeyReductionExec {
    fn name(&self) -> &str {
        "CometMembershipKeyReductionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [_; 1] = children.try_into().map_err(|_| {
            DataFusionError::Internal("Membership key reduction requires one child".into())
        })?;
        Ok(Arc::new(Self::new(input)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let input_rows =
            MetricBuilder::new(&self.metrics).counter("right_key_reduction_input_rows", partition);
        let output_rows =
            MetricBuilder::new(&self.metrics).counter("right_key_reduction_output_rows", partition);
        let bypass_rows =
            MetricBuilder::new(&self.metrics).counter("right_key_reduction_bypass_rows", partition);
        let time =
            MetricBuilder::new(&self.metrics).subset_time("right_key_reduction_time", partition);
        let stream = futures::stream::try_unfold(
            (input, 0usize, false),
            move |(mut input, seen, mut bypass)| {
                let context = Arc::clone(&context);
                let (input_rows, output_rows, bypass_rows, time) = (
                    input_rows.clone(),
                    output_rows.clone(),
                    bypass_rows.clone(),
                    time.clone(),
                );
                async move {
                    let Some(batch) = input.next().await.transpose()? else {
                        return Ok(None);
                    };
                    input_rows.add(batch.num_rows());
                    let seen = seen.saturating_add(batch.num_rows());
                    let reduced =
                        if bypass || seen <= PREFIX_ROWS || batch.num_rows() < MIN_BATCH_ROWS {
                            None
                        } else {
                            let reduced = match try_reduce_batch(&batch, &context, &time).await {
                                Ok(reduced) => reduced,
                                Err(e)
                                    if matches!(
                                        e.find_root(),
                                        DataFusionError::ResourcesExhausted(_)
                                    ) =>
                                {
                                    None
                                }
                                Err(e) => return Err(e),
                            };
                            bypass = reduced.is_none();
                            reduced
                        };
                    let batch = match reduced {
                        Some(reduced) => reduced,
                        None => {
                            bypass_rows.add(batch.num_rows());
                            let _timer = time.timer();
                            compact_batch(batch, &context)?
                        }
                    };
                    output_rows.add(batch.num_rows());
                    Ok(Some((batch, (input, seen, bypass))))
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn retained_bytes(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
        + std::mem::size_of::<RecordBatch>()
        + batch.num_columns() * std::mem::size_of::<ArrayRef>()
}

fn oversized_backing(array: &ArrayRef) -> bool {
    let values = array.len() * array.data_type().primitive_width().unwrap_or(0);
    let validity = if array.nulls().is_some() {
        array.len().div_ceil(8)
    } else {
        0
    };
    array.get_buffer_memory_size() > 2 * (values + validity) + 128
}

// A slice can keep a much larger allocation alive in the parent sort. Only copy such
// columns when the pool has room and the resulting batch retains no more unique bytes.
fn compact_batch(batch: RecordBatch, context: &TaskContext) -> Result<RecordBatch> {
    if !batch.columns().iter().any(oversized_backing) {
        return Ok(batch);
    }
    let scratch = MemoryConsumer::new("MembershipKeyReduction compact")
        .with_can_spill(true)
        .register(context.memory_pool());
    if let Err(error) = scratch.try_grow(retained_bytes(&batch)) {
        return if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
            Ok(batch)
        } else {
            Err(error)
        };
    }
    let columns = batch
        .columns()
        .iter()
        .map(|array| {
            if !oversized_backing(array) {
                return Ok(Arc::clone(array));
            }
            let data = array.to_data();
            let mut compact = MutableArrayData::new(vec![&data], false, array.len());
            compact.try_extend(0, 0, array.len())?;
            Ok(make_array(compact.freeze()))
        })
        .collect::<Result<Vec<_>>>()?;
    let compact = RecordBatch::try_new(batch.schema(), columns)?;
    if get_record_batch_memory_size(&compact) <= get_record_batch_memory_size(&batch) {
        Ok(compact)
    } else {
        Ok(batch)
    }
}

/// Collect one bounded partial aggregate privately so any refusal can return the entire
/// original batch. No helper reservation or unpublished aggregate output crosses a yield.
async fn try_reduce_batch(
    original: &RecordBatch,
    context: &Arc<TaskContext>,
    time: &Time,
) -> Result<Option<RecordBatch>> {
    let setup_timer = time.timer();
    if original.num_rows() > MAX_BATCH_ROWS {
        return Ok(None);
    }
    let reservation = MemoryConsumer::new("MembershipKeyReduction batch")
        .with_can_spill(true)
        .register(context.memory_pool());
    reservation.try_grow(retained_bytes(original))?;
    let schema = original.schema();
    let row_width: usize = schema
        .fields()
        .iter()
        .map(|field| field.data_type().primitive_width().unwrap_or(1) + 1)
        .sum();
    let scratch =
        2 * (original.num_rows() * (std::mem::size_of::<usize>() + row_width) + 64 * 1024);
    // Probe for working space before setting up the aggregate. Its real allocations
    // use the same pool, so release the estimate while the aggregate runs.
    reservation.try_grow(scratch)?;
    reservation.shrink(scratch);
    let source =
        MemorySourceConfig::try_new_exec(&[vec![original.clone()]], Arc::clone(&schema), None)?;
    let keys = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            (
                Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            )
        })
        .collect();
    let aggregate = AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::new_single(keys),
        vec![],
        vec![],
        source,
        Arc::clone(&schema),
    )?;
    let mut stream = aggregate.execute(0, Arc::clone(context))?;
    drop(setup_timer);
    let mut output = Vec::new();
    while let Some(batch) = stream.next().await.transpose()? {
        let _timer = time.timer();
        let batch = compact_batch(batch, context)?;
        reservation.try_grow(retained_bytes(&batch))?;
        output.push(batch);
    }
    let _timer = time.timer();
    if let Some(elapsed) = aggregate.metrics().and_then(|m| m.elapsed_compute()) {
        time.add_duration(std::time::Duration::from_nanos(elapsed as u64));
    }
    drop(stream);
    drop(aggregate);
    let rows: usize = output.iter().map(RecordBatch::num_rows).sum();
    if 5 * rows >= 4 * original.num_rows() {
        return Ok(None);
    }
    let reduced = if output.len() == 1 {
        output.pop().unwrap()
    } else {
        reservation.try_grow(output.iter().map(retained_bytes).sum())?;
        concat_batches(&schema, &output)?
    };
    Ok(
        (get_record_batch_memory_size(&reduced) <= get_record_batch_memory_size(original))
            .then_some(reduced),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{cast::AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryLimit, MemoryPool, MemoryReservation,
    };
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_from_iter([("key", Arc::new(Int32Array::from(values)) as ArrayRef)])
            .unwrap()
    }

    fn reducer(batches: Vec<RecordBatch>) -> MembershipKeyReductionExec {
        let schema = batches[0].schema();
        MembershipKeyReductionExec::new(
            MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap(),
        )
    }

    fn metric(plan: &MembershipKeyReductionExec, name: &str) -> usize {
        plan.metrics()
            .unwrap()
            .aggregate_by_name()
            .iter()
            .find(|m| m.value().name() == name)
            .unwrap()
            .value()
            .as_usize()
    }

    #[tokio::test]
    async fn duplicate_composite_rows_preserve_nulls_and_signed_keys() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, false),
        ]));
        let keys = [None, Some(0), Some(i32::MIN), Some(0)];
        let n = PREFIX_ROWS + 2;
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from_iter((0..n).map(|i| keys[i % 4]))),
                Arc::new(Int32Array::from_iter_values((0..n).map(|i| (i % 4) as i32))),
            ],
        )
        .unwrap()
        .slice(1, PREFIX_ROWS);
        let plan = reducer(vec![input.clone(), input.clone()]);
        let out = collect(plan.execute(0, SessionContext::new().task_ctx()).unwrap())
            .await
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], input);
        assert_eq!(out[1].schema(), schema);
        assert_eq!(out[1].num_rows(), 4);
        let pairs = out[1]
            .column(0)
            .as_primitive::<Int32Type>()
            .iter()
            .zip(
                out[1]
                    .column(1)
                    .as_primitive::<Int32Type>()
                    .values()
                    .iter()
                    .copied(),
            )
            .collect::<BTreeSet<_>>();
        assert_eq!(
            pairs,
            BTreeSet::from([(None, 0), (Some(0), 1), (Some(i32::MIN), 2), (Some(0), 3)])
        );
        assert_eq!(
            metric(&plan, "right_key_reduction_bypass_rows"),
            PREFIX_ROWS
        );
        assert_eq!(
            metric(&plan, "right_key_reduction_output_rows"),
            PREFIX_ROWS + 4
        );
        assert!(plan.properties().output_ordering().is_none());
        assert!(plan.properties().eq_properties.constraints().is_empty());
    }

    #[tokio::test]
    async fn mostly_unique_batch_replays_all_rows_and_bypasses_later_batches() {
        let prefix = batch(vec![0; PREFIX_ROWS]);
        // Exactly 80% distinct is the bypass boundary.
        let marginal = batch((0..1025).map(|i| i % 820).collect());
        let duplicate = batch(vec![7; MIN_BATCH_ROWS]);
        let input = vec![prefix, marginal, duplicate];
        let plan = reducer(input.clone());
        let out = collect(plan.execute(0, SessionContext::new().task_ctx()).unwrap())
            .await
            .unwrap();
        assert_eq!(out, input);
        assert_eq!(
            metric(&plan, "right_key_reduction_bypass_rows"),
            PREFIX_ROWS + 1025 + MIN_BATCH_ROWS
        );
    }

    #[tokio::test]
    async fn small_batches_keep_reduction_enabled_while_oversized_batches_disable_it() {
        let prefix = batch(vec![1; PREFIX_ROWS]);
        let small = batch(vec![2; MIN_BATCH_ROWS - 1]);
        let useful = batch(vec![3; MIN_BATCH_ROWS]);
        let large = batch(vec![4; MAX_BATCH_ROWS + 1]);
        let input = vec![
            prefix.clone(),
            small.clone(),
            useful.clone(),
            large.clone(),
            useful.clone(),
        ];
        let plan = reducer(input);
        let context = SessionContext::new_with_config(
            SessionConfig::new().with_batch_size(MAX_BATCH_ROWS + 1),
        )
        .task_ctx();
        let out = collect(plan.execute(0, context).unwrap()).await.unwrap();
        assert_eq!(out, vec![prefix, small, batch(vec![3]), large, useful]);
    }

    // Fail the reservation for aggregate output, after the original and the working-space
    // probe have been admitted. The stream must release the unpublished result on either error.
    #[derive(Debug)]
    struct FailingPool {
        inner: GreedyMemoryPool,
        admissions: AtomicUsize,
        exhausted: bool,
    }

    impl std::fmt::Display for FailingPool {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingPool")
        }
    }

    impl MemoryPool for FailingPool {
        fn name(&self) -> &str {
            "FailingPool"
        }
        fn grow(&self, reservation: &MemoryReservation, size: usize) {
            self.inner.grow(reservation, size);
        }
        fn shrink(&self, reservation: &MemoryReservation, size: usize) {
            self.inner.shrink(reservation, size);
        }
        fn try_grow(&self, reservation: &MemoryReservation, size: usize) -> Result<()> {
            if reservation.consumer().name() == "MembershipKeyReduction batch"
                && self.admissions.fetch_add(1, Ordering::SeqCst) == 2
            {
                return Err(if self.exhausted {
                    DataFusionError::ResourcesExhausted("test refusal".into())
                } else {
                    DataFusionError::Execution("test failure".into())
                });
            }
            self.inner.try_grow(reservation, size)
        }
        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
        fn memory_limit(&self) -> MemoryLimit {
            self.inner.memory_limit()
        }
    }

    #[tokio::test]
    async fn resource_refusals_replay_the_batch_and_other_errors_propagate() {
        for exhausted in [true, false] {
            let pool = Arc::new(FailingPool {
                inner: GreedyMemoryPool::new(8 * 1024 * 1024),
                admissions: AtomicUsize::new(0),
                exhausted,
            });
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
                .build_arc()
                .unwrap();
            let context =
                SessionContext::new_with_config_rt(SessionConfig::new(), runtime).task_ctx();
            let prefix = batch(vec![0; PREFIX_ROWS]);
            let eligible = batch(vec![1; MIN_BATCH_ROWS]);
            let plan = reducer(vec![prefix.clone(), eligible.clone(), eligible.clone()]);
            let mut stream = plan.execute(0, context).unwrap();
            assert_eq!(stream.next().await.unwrap().unwrap(), prefix);
            if exhausted {
                assert_eq!(stream.next().await.unwrap().unwrap(), eligible);
                assert_eq!(pool.reserved(), 0);
                assert_eq!(stream.next().await.unwrap().unwrap(), eligible);
                assert!(stream.next().await.is_none());
            } else {
                assert!(
                    matches!(stream.next().await.unwrap().unwrap_err(), DataFusionError::Execution(message) if message == "test failure")
                );
            }
            assert_eq!(pool.admissions.load(Ordering::SeqCst), 3);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[test]
    fn compacted_slices_never_increase_the_unique_allocation_held_by_sort() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1; 2200]).slice(0, 1000));
        let context = SessionContext::new().task_ctx();
        assert!(oversized_backing(&values));
        let single = RecordBatch::try_from_iter([("a", Arc::clone(&values))]).unwrap();
        let compact = compact_batch(single.clone(), &context).unwrap();
        assert_eq!(compact, single);
        assert!(get_record_batch_memory_size(&compact) < get_record_batch_memory_size(&single));
        let limited_runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(1)))
            .build_arc()
            .unwrap();
        let limited =
            SessionContext::new_with_config_rt(SessionConfig::new(), limited_runtime).task_ctx();
        let passthrough = compact_batch(single.clone(), &limited).unwrap();
        assert!(Arc::ptr_eq(passthrough.column(0), single.column(0)));
        assert_eq!(limited.memory_pool().reserved(), 0);
        let shared = RecordBatch::try_from_iter([
            ("a", Arc::clone(&values)),
            ("b", Arc::clone(&values)),
            ("c", values),
        ])
        .unwrap();
        let compact = compact_batch(shared.clone(), &context).unwrap();
        assert_eq!(compact, shared);
        assert!(get_record_batch_memory_size(&compact) <= get_record_batch_memory_size(&shared));
        assert_eq!(context.memory_pool().reserved(), 0);
    }
}
