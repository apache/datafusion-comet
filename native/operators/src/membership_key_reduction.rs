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

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::BuildHasher;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{make_array, ArrayRef, MutableArrayData};
use arrow::buffer::Buffer;
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::{create_hashes, RandomState};
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
use futures::{StreamExt, TryStreamExt};

// Skip whole batches ending within the initial prefix. After that, small batches pass
// through unchanged, while an oversized or unprofitable attempt disables later attempts.
const PREFIX_ROWS: usize = 8192;
const MIN_BATCH_ROWS: usize = 1024;
const SMALL_BATCH_MAX_ROWS: usize = 8192;
const MAX_BATCH_ROWS: usize = 64 * 1024;
// Track identities only. Stop attempting reduction if many tiny batches exhaust the bound.
const MAX_FORWARDED_BUFFERS: usize = 256;

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
            (input, 0usize, false, HashSet::new()),
            move |(mut input, seen, mut bypass, mut forwarded)| {
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
                    let reduced = if bypass
                        || seen <= PREFIX_ROWS
                        || batch.num_rows() < MIN_BATCH_ROWS
                        || (batch.num_rows() <= MAX_BATCH_ROWS
                            && shares_input_backing(&batch, &forwarded, &time))
                    {
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
                            // Sort ignores empty batches. Nonempty forwarded buffers may
                            // stay live until it has consumed the entire partition.
                            if !bypass && batch.num_rows() != 0 {
                                let _timer = time.timer();
                                for buffer in key_buffers(&batch) {
                                    forwarded.insert(buffer.data_ptr().addr());
                                    if forwarded.len() > MAX_FORWARDED_BUFFERS {
                                        bypass = true;
                                        forwarded = HashSet::new();
                                        break;
                                    }
                                }
                            }
                            batch
                        }
                    };
                    output_rows.add(batch.num_rows());
                    Ok(Some((batch, (input, seen, bypass, forwarded))))
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

fn key_buffers(batch: &RecordBatch) -> impl Iterator<Item = &Buffer> {
    batch.columns().iter().flat_map(|column| {
        let column = column.as_ref();
        let values = arrow::downcast_primitive_array!(
            column => column.values().inner(),
            _ => unreachable!("membership keys must be signed integers")
        );
        std::iter::once(values).chain(column.nulls().map(|nulls| nulls.buffer()))
    })
}

fn shares_input_backing(
    batch: &RecordBatch,
    forwarded: &HashSet<NonZeroUsize>,
    time: &Time,
) -> bool {
    let _timer = time.timer();
    let mut shared = Vec::new();
    for buffer in key_buffers(batch) {
        if forwarded.contains(&buffer.data_ptr().addr()) {
            return true;
        }
        if buffer.strong_count() > 1 {
            shared.push(buffer);
        }
    }
    // IPC columns can own different slices of the same body. Count each Buffer
    // object once, even when a projection repeats the same array. Other live views
    // can belong to a later input batch that the sort will have to retain.
    shared.sort_unstable_by_key(|b| (b.data_ptr(), std::ptr::from_ref(*b)));
    shared.dedup_by(|a, b| std::ptr::eq(*a, *b));
    shared
        .chunk_by(|a, b| a.data_ptr() == b.data_ptr())
        .any(|views| views[0].strong_count() > views.len())
}

fn retained_bytes(batch: &RecordBatch) -> usize {
    // Shuffle columns can be slices of a single IPC allocation.
    get_record_batch_memory_size(batch)
        + std::mem::size_of::<RecordBatch>()
        + std::mem::size_of_val(batch.columns())
        + batch
            .columns()
            .iter()
            .map(|array| std::mem::size_of_val(array.as_ref()))
            .sum::<usize>()
}

fn logical_bytes(array: &ArrayRef) -> usize {
    let values = array.len() * array.data_type().primitive_width().unwrap_or(0);
    let validity = if array.nulls().is_some() {
        array.len().div_ceil(8)
    } else {
        0
    };
    values + validity
}

fn oversized_backing(array: &ArrayRef) -> bool {
    array.get_buffer_memory_size() > 2 * logical_bytes(array) + 128
}

// Only compact the private aggregate output. Columns can share one packed allocation,
// so check whether the entire batch could reclaim space before copying any columns.
fn compact_batch(batch: RecordBatch, context: &TaskContext) -> Result<RecordBatch> {
    if !batch.columns().iter().any(oversized_backing) {
        return Ok(batch);
    }
    let bytes = get_record_batch_memory_size(&batch);
    if bytes <= 2 * batch.columns().iter().map(logical_bytes).sum::<usize>() + 128 {
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
    if get_record_batch_memory_size(&compact) < bytes {
        Ok(compact)
    } else {
        Ok(batch)
    }
}

// Look for repeated complete tuples before paying for a full aggregate. Short windows
// let Arrow hash contiguous values without copying them. Vary the offset within each
// section of the batch to avoid sampling only one phase of a periodic key sequence.
fn sample_has_repeated_keys(batch: &RecordBatch) -> Result<bool> {
    const SAMPLE_ROWS: usize = 256;
    const WINDOW_ROWS: usize = 16;
    const WINDOWS: usize = SAMPLE_ROWS / WINDOW_ROWS;
    debug_assert!(batch.num_rows() >= SAMPLE_ROWS);
    let state = RandomState::with_seed(0);
    let mut windows = [[0; WINDOW_ROWS]; WINDOWS];
    for (i, window) in windows.iter_mut().enumerate() {
        let start = i * batch.num_rows() / WINDOWS;
        let end = (i + 1) * batch.num_rows() / WINDOWS;
        let offset = start + state.hash_one(i) as usize % (end - start - WINDOW_ROWS + 1);
        let columns = batch.columns().iter().map(|c| c.slice(offset, WINDOW_ROWS));
        create_hashes(columns, &state, window)?;
        // DataFusion skips null values when hashing integers. Include their column
        // positions so (tenant, k, NULL) and (tenant, NULL, k) hash differently.
        if batch.num_columns() > 1 {
            for (index, column) in batch.columns().iter().enumerate() {
                let Some(nulls) = column.nulls().filter(|n| n.null_count() > 0) else {
                    continue;
                };
                for (row, hash) in window.iter_mut().enumerate() {
                    if nulls.is_null(offset + row) {
                        *hash = state.hash_one((*hash, index));
                    }
                }
            }
        }
    }
    let hashes = windows.as_flattened_mut();
    hashes.sort_unstable();
    let repeats = hashes.windows(2).filter(|pair| pair[0] == pair[1]).count();
    // The sample only decides whether to try. Check the actual output size before emitting it.
    Ok(repeats >= SAMPLE_ROWS / 16)
}

/// Collect one bounded partial aggregate privately so any refusal can return the entire
/// original batch. No helper reservation or unpublished aggregate output crosses a yield.
async fn try_reduce_batch(
    original: &RecordBatch,
    context: &Arc<TaskContext>,
    time: &Time,
) -> Result<Option<RecordBatch>> {
    let setup_timer = time.timer();
    if original.num_rows() > MAX_BATCH_ROWS || !sample_has_repeated_keys(original)? {
        return Ok(None);
    }
    let reservation = MemoryConsumer::new("MembershipKeyReduction batch")
        .with_can_spill(true)
        .register(context.memory_pool());
    reservation.try_grow(retained_bytes(original))?;
    let schema = original.schema();
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
    let stream = aggregate.execute(0, Arc::clone(context))?;
    drop(setup_timer);
    let output = stream
        .map(|batch| -> Result<_> {
            let _timer = time.timer();
            let batch = compact_batch(batch?, context)?;
            reservation.try_grow(retained_bytes(&batch))?;
            Ok(batch)
        })
        .try_collect::<Vec<_>>()
        .await;
    let _timer = time.timer();
    // Account for completed hash work even if retaining its output failed.
    if let Some(elapsed) = aggregate.metrics().and_then(|m| m.elapsed_compute()) {
        time.add_duration(std::time::Duration::from_nanos(elapsed as u64));
    }
    drop(aggregate);
    let mut output = output?;
    let rows: usize = output.iter().map(RecordBatch::num_rows).sum();
    // Small batches must save more sort work to repay per-batch aggregate setup.
    let input_rows = original.num_rows();
    let unprofitable = if input_rows <= SMALL_BATCH_MAX_ROWS {
        rows > input_rows.div_ceil(3)
    } else {
        2 * rows >= input_rows
    };
    if unprofitable {
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
    use std::sync::Once;
    use std::time::Duration;

    use arrow::array::{cast::AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::common::utils::memory::RecordBatchMemoryCounter;
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
    async fn shared_shuffle_columns_fit_in_a_bounded_pool() {
        // Model a decoded IPC body with eight disjoint columns in one allocation.
        let rows = 4096;
        let columns = 8;
        let inputs = (0..16).map(|_| {
            let values = Int32Array::from_iter_values((0..rows * columns).map(|i| (i % 4) as i32));
            RecordBatch::try_from_iter((0..columns).map(|i| {
                (
                    format!("k{i}"),
                    Arc::new(values.slice(i * rows, rows)) as ArrayRef,
                )
            }))
            .unwrap()
        });
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(384 * 1024)))
            .build_arc()
            .unwrap();
        let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime).task_ctx();
        let plan = reducer(inputs.collect());
        let out = collect(plan.execute(0, Arc::clone(&context)).unwrap())
            .await
            .unwrap();
        assert_eq!(
            out.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2 * rows + 14 * 4
        );
        assert_eq!(context.memory_pool().reserved(), 0);
    }

    #[tokio::test]
    async fn duplicate_composite_rows_preserve_nulls_and_signed_keys() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, false),
        ]));
        let keys = [None, Some(0), Some(i32::MIN), Some(0)];
        let n = PREFIX_ROWS + 2;
        let input = || {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from_iter((0..n).map(|i| keys[i % 4]))),
                    Arc::new(Int32Array::from_iter_values((0..n).map(|i| (i % 4) as i32))),
                ],
            )
            .unwrap()
            .slice(1, PREFIX_ROWS)
        };
        let prefix = input();
        let plan = reducer(vec![prefix.clone(), input()]);
        let out = collect(plan.execute(0, SessionContext::new().task_ctx()).unwrap())
            .await
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], prefix);
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

    #[test]
    fn sample_distinguishes_null_positions_in_composite_keys() {
        let input = |distinct: usize| {
            let column = |slot| {
                Arc::new(Int32Array::from_iter(
                    (0..MIN_BATCH_ROWS)
                        .map(|i| (i % 2 == slot).then_some(((i / 2) % distinct) as i32)),
                )) as ArrayRef
            };
            RecordBatch::try_from_iter([
                (
                    "tenant",
                    Arc::new(Int32Array::from(vec![7; MIN_BATCH_ROWS])) as ArrayRef,
                ),
                ("a", column(0)),
                ("b", column(1)),
            ])
            .unwrap()
        };
        assert!(!sample_has_repeated_keys(&input(MIN_BATCH_ROWS / 2)).unwrap());
        assert!(sample_has_repeated_keys(&input(4)).unwrap());
    }

    #[tokio::test]
    async fn unique_batches_bypass_before_reserving_aggregate_memory() {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(0)))
            .build_arc()
            .unwrap();
        let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime).task_ctx();
        for rows in [PREFIX_ROWS, MAX_BATCH_ROWS] {
            // Every tuple is unique, although each individual column repeats.
            let first = Int32Array::from_iter(
                (0..rows).map(|i| [None, Some(0), Some(-1), Some(i32::MIN)][i % 4]),
            );
            let second = Int32Array::from_iter_values((0..rows).map(|i| (i / 4) as i32));
            let input = RecordBatch::try_from_iter([
                ("a", Arc::new(first) as ArrayRef),
                ("b", Arc::new(second) as ArrayRef),
            ])
            .unwrap();
            assert!(try_reduce_batch(&input, &context, &Time::new())
                .await
                .unwrap()
                .is_none());
        }
    }

    #[tokio::test]
    async fn unprofitable_batches_disable_later_attempts() {
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_batch_size(MAX_BATCH_ROWS))
                .task_ctx();
        for (rows, distinct) in [(1024, 499), (4096, 3200), (8192, 2732), (16_384, 8192)] {
            let prefix = batch(vec![0; PREFIX_ROWS]);
            // A repeated suffix admits the batch to the full aggregate.
            let marginal = batch((0..rows).map(|i| i.min(distinct - 1) as i32).collect());
            assert!(sample_has_repeated_keys(&marginal).unwrap());
            let duplicate = batch(vec![7; MIN_BATCH_ROWS]);
            let input = vec![prefix, marginal, duplicate];
            let plan = reducer(input.clone());
            let out = collect(plan.execute(0, Arc::clone(&context)).unwrap())
                .await
                .unwrap();
            assert_eq!(out, input);
            assert_eq!(
                metric(&plan, "right_key_reduction_bypass_rows"),
                PREFIX_ROWS + rows + MIN_BATCH_ROWS
            );
        }
    }

    #[tokio::test]
    async fn profitable_batches_reduce_at_both_batch_sizes() {
        let context = SessionContext::new().task_ctx();
        let hot_keys = batch((0..1024).map(|i| i % 7).collect());
        let triples = batch((0..1024).map(|i| i / 3).collect());
        let large = batch((0..8193).map(|i| i.min(3199)).collect());
        for (input, distinct) in [(hot_keys, 7), (triples, 342), (large, 3200)] {
            let reduced = try_reduce_batch(&input, &context, &Time::new())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reduced.num_rows(), distinct);
        }
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

    // Spend a known time inside the aggregate, then refuse its first output reservation.
    const AGGREGATE_DELAY: Duration = Duration::from_millis(10);

    #[derive(Debug)]
    struct FailingPool {
        inner: GreedyMemoryPool,
        admissions: AtomicUsize,
        aggregate_work: Once,
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
            let name = reservation.consumer().name();
            if name.starts_with("PartialHashAggregateStream[") {
                self.aggregate_work
                    .call_once(|| std::thread::sleep(AGGREGATE_DELAY));
            }
            if name == "MembershipKeyReduction batch"
                && self.admissions.fetch_add(1, Ordering::SeqCst) == 1
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
                aggregate_work: Once::new(),
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
            assert_eq!(pool.admissions.load(Ordering::SeqCst), 2);
            assert_eq!(pool.reserved(), 0);
            assert!(pool.aggregate_work.is_completed());
            assert!(
                metric(&plan, "right_key_reduction_time") >= AGGREGATE_DELAY.as_nanos() as usize
            );
        }
    }

    #[tokio::test]
    async fn bypass_preserves_buffers_shared_by_input_batches() {
        let backing = batch((0..(2 * PREFIX_ROWS) as i32).collect());
        // The first two slices are within the prefix. Later slices reuse that allocation.
        let input = vec![
            backing.slice(0, 4096),
            backing.slice(4096, 2048),
            backing.slice(6144, 4096),
            backing.slice(10240, 2048),
        ];
        let plan = reducer(input.clone());
        let output = collect(plan.execute(0, SessionContext::new().task_ctx()).unwrap())
            .await
            .unwrap();
        assert_eq!(output, input);
        for (out, original) in output.iter().zip(&input) {
            assert!(Arc::ptr_eq(out.column(0), original.column(0)));
        }
    }

    #[tokio::test]
    async fn shared_slices_do_not_add_to_sort_memory_in_either_order() {
        let bytes = |batches: &[RecordBatch]| {
            let mut counter = RecordBatchMemoryCounter::new();
            for batch in batches {
                counter.count_batch(batch);
            }
            counter.memory_usage()
        };
        // The first parent crosses the prefix. The second supplies a reducible batch
        // before a small slice that will have to be forwarded unchanged.
        let parent = batch(vec![1; 2 * PREFIX_ROWS]);
        let next = batch(vec![2; PREFIX_ROWS + 1]);
        let input = vec![
            parent.slice(0, PREFIX_ROWS),
            batch(vec![0]),
            parent.slice(PREFIX_ROWS, PREFIX_ROWS),
            next.slice(0, PREFIX_ROWS),
            next.slice(PREFIX_ROWS, 1),
        ];
        let expected_bytes = bytes(&input);
        let mut batches = input.clone();
        batches.push(batch(vec![3; MIN_BATCH_ROWS]));
        let plan = reducer(batches);
        let out = collect(plan.execute(0, SessionContext::new().task_ctx()).unwrap())
            .await
            .unwrap();
        assert_eq!(bytes(&out[..input.len()]), expected_bytes);
        assert_eq!(out[..input.len()], input);
        assert_eq!(out.last().unwrap(), &batch(vec![3]));
    }

    #[tokio::test]
    async fn forwarded_columns_protect_cloned_arrays_and_bound_tracking() {
        let context = SessionContext::new().task_ctx();
        // Both batches reuse the same ArrayRef for one column. This does not add
        // another reference to its Buffer, so the previously forwarded identity matters.
        let common = Arc::new(Int32Array::from(vec![7; PREFIX_ROWS])) as ArrayRef;
        let pair = |first: &ArrayRef, value| {
            RecordBatch::try_from_iter([
                ("shared", Arc::clone(first)),
                (
                    "private",
                    Arc::new(Int32Array::from(vec![value; PREFIX_ROWS])) as ArrayRef,
                ),
            ])
            .unwrap()
        };
        let prefix = pair(&common, 0);
        let mixed = pair(&common, 1);
        let plan = reducer(vec![prefix, mixed.clone()]);
        let out = collect(plan.execute(0, Arc::clone(&context)).unwrap())
            .await
            .unwrap();
        assert_eq!(out[1], mixed);

        for (size, expected) in [(0, 1), (1, MIN_BATCH_ROWS)] {
            let mut inputs = vec![batch(vec![0; PREFIX_ROWS])];
            inputs.extend((0..=MAX_FORWARDED_BUFFERS).map(|_| batch(vec![0]).slice(0, size)));
            inputs.push(batch(vec![1; MIN_BATCH_ROWS]));
            let plan = reducer(inputs);
            let out = collect(plan.execute(0, Arc::clone(&context)).unwrap())
                .await
                .unwrap();
            assert_eq!(out.last().unwrap().num_rows(), expected);
        }
    }

    #[test]
    fn private_output_compaction_reclaims_oversized_buffers_when_the_pool_allows() {
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
        let packed = Int32Array::from_iter_values(0..4096);
        let shared = RecordBatch::try_from_iter((0..4).map(|i| {
            (
                format!("k{i}"),
                Arc::new(packed.slice(i * 1024, 1024)) as ArrayRef,
            )
        }))
        .unwrap();
        assert!(shared.columns().iter().all(oversized_backing));
        let unchanged = compact_batch(shared.clone(), &context).unwrap();
        for (out, original) in unchanged.columns().iter().zip(shared.columns()) {
            assert!(Arc::ptr_eq(out, original));
        }
        assert_eq!(context.memory_pool().reserved(), 0);
    }
}
