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

//! Combines small input batches before a native sort.
//!
//! On its spill path DataFusion's sort merges one stream per buffered batch, so thousands of
//! tiny shuffle blocks make it very slow. Below the in-place threshold the sort concatenates its
//! input itself, so batches pass through until a partition's input reaches that threshold. The
//! gate never closes again, so a sort whose share is below the threshold gets a copy it does not
//! need.
//!
//! This sits in front of the sort rather than in the shuffle reader because only the sort's merge
//! cost grows with the number of batches. Arrow's `BatchCoalescer` has no byte limit and copies
//! as batches arrive, and `DecodedBatchCoalescer` takes only flat types, so neither lets the held
//! memory be reserved exactly for any input.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::memory::get_record_batch_memory_size;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{async_try_stream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, RecordOutput,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::TryStreamExt;

/// The cap is half the in-place threshold because the sort reserves about twice a batch's size.
/// The bounds also keep string and binary offsets far below the i32 limit.
const MIN_GROUP_BYTES: usize = 64 * 1024;
const MAX_GROUP_BYTES: usize = 4 * 1024 * 1024;

/// In `sort_small_batches`, combining 512-row batches made a spilling single-key sort slower,
/// while 384 rows or fewer was never slower.
const MAX_SMALL_BATCH_ROWS: usize = 512;

/// Small batches passed through after a refusal, so a pool under pressure is not asked again for
/// each one.
const REFUSED_BACKOFF_BATCHES: usize = 64;

#[derive(Debug)]
pub struct SortInputCoalesceExec {
    input: Arc<dyn ExecutionPlan>,
    target_rows: usize,
    gate_bytes: usize,
    max_bytes: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl SortInputCoalesceExec {
    /// `gate_bytes` is the sort's in-place threshold.
    pub fn new(input: Arc<dyn ExecutionPlan>, target_rows: usize, gate_bytes: usize) -> Self {
        Self {
            input,
            target_rows: target_rows.max(1),
            gate_bytes,
            max_bytes: (gate_bytes / 2).clamp(MIN_GROUP_BYTES, MAX_GROUP_BYTES),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    #[cfg(test)]
    fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// The plan below a coalescer, or `plan` itself when it is not one.
    pub fn unwrap_input(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
        plan.downcast_ref::<Self>().map_or(plan, Self::input)
    }
}

impl DisplayAs for SortInputCoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "CometSortInputCoalesceExec: target_rows={}, gate_bytes={}, max_bytes={}",
            self.target_rows, self.gate_bytes, self.max_bytes
        )
    }
}

impl ExecutionPlan for SortInputCoalesceExec {
    fn name(&self) -> &str {
        "CometSortInputCoalesceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
        CardinalityEffect::Equal
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [_; 1] = children.try_into().map_err(|_| {
            DataFusionError::Internal("Sort input coalescing requires one child".into())
        })?;
        Ok(Arc::new(Self {
            input,
            target_rows: self.target_rows,
            gate_bytes: self.gate_bytes,
            max_bytes: self.max_bytes,
            // Shared, so the copy reports through the node the Spark plan registered for its
            // metrics if a rewrite rebuilds the sort's input.
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, Arc::clone(&context))?;
        let reservation = MemoryConsumer::new(format!("SortInputCoalesceExec[{partition}]"))
            .register(context.memory_pool());
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let mut grouper = Grouper::new(self, reservation, partition);
        let stream = async_try_stream(|mut out| async move {
            while let Some(batch) = input.try_next().await? {
                // A group that has to go out first is emitted before the new batch is held, so
                // the sort reserves it while this operator holds nothing.
                let (step, flushed) = {
                    let _timer = baseline.elapsed_compute().timer();
                    grouper.prepare(&batch)?
                };
                if let Some(batch) = flushed {
                    (&batch).record_output(&baseline);
                    out.emit(batch).await;
                }
                let ready = {
                    let _timer = baseline.elapsed_compute().timer();
                    grouper.add(batch, step)?
                };
                for batch in ready.into_iter().flatten() {
                    (&batch).record_output(&baseline);
                    out.emit(batch).await;
                }
            }
            let last = {
                let _timer = baseline.elapsed_compute().timer();
                grouper.flush()?
            };
            if let Some(batch) = last {
                (&batch).record_output(&baseline);
                out.emit(batch).await;
            }
            Ok(())
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Concatenating dictionaries merges their values, which can overflow a small key type where
/// the batches on their own fit. Dictionaries nested in structs, lists and maps count too.
fn has_small_dictionary_keys(batch: &RecordBatch) -> bool {
    fn small(data_type: &DataType) -> bool {
        match data_type {
            DataType::Dictionary(key, value) => {
                key.primitive_width().is_some_and(|width| width < 4) || small(value)
            }
            DataType::Struct(fields) => fields.iter().any(|f| small(f.data_type())),
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => small(f.data_type()),
            // Run ends count rows, so a small run-end type can overflow too.
            DataType::RunEndEncoded(run_ends, values) => {
                run_ends
                    .data_type()
                    .primitive_width()
                    .is_some_and(|width| width < 4)
                    || small(values.data_type())
            }
            _ => false,
        }
    }
    batch.schema().fields().iter().any(|f| small(f.data_type()))
}

/// The bytes the batch's rows take, without unused buffer capacity, as DataFusion's sort counts
/// it.
fn sliced_size(batch: &RecordBatch) -> Result<usize> {
    batch.columns().iter().try_fold(0usize, |total, column| {
        Ok(total + column.to_data().get_slice_memory_size()?)
    })
}

/// `held` is what a batch keeps alive, counting a shared buffer once. `copy` is what concatenating
/// it writes, which counts a buffer once per column that uses it.
#[derive(Clone, Copy)]
struct Size {
    held: usize,
    copy: usize,
}

enum Step {
    Skip,
    PassThrough,
    Hold(Size),
}

/// Groups whole input batches in arrival order and concatenates each group. Inputs are never
/// split, so the reservation always covers the batches being held.
struct Grouper {
    target_rows: usize,
    small_rows: usize,
    gate_bytes: usize,
    max_bytes: usize,
    seen_bytes: usize,
    past_gate: bool,
    pending: Vec<RecordBatch>,
    rows: usize,
    held: usize,
    copy: usize,
    reservation: MemoryReservation,
    backoff: usize,
    input_batches: Count,
    output_batches: Count,
}

impl Grouper {
    fn new(exec: &SortInputCoalesceExec, reservation: MemoryReservation, partition: usize) -> Self {
        let metrics = MetricBuilder::new(&exec.metrics);
        Self {
            target_rows: exec.target_rows,
            small_rows: exec.target_rows.div_ceil(2).min(MAX_SMALL_BATCH_ROWS),
            gate_bytes: exec.gate_bytes,
            max_bytes: exec.max_bytes,
            seen_bytes: 0,
            past_gate: exec.gate_bytes == 0,
            pending: Vec::new(),
            rows: 0,
            held: 0,
            copy: 0,
            reservation,
            backoff: 0,
            input_batches: metrics
                .clone()
                .counter("coalesced_input_batches", partition),
            output_batches: metrics.counter("coalesced_output_batches", partition),
        }
    }

    /// Decides what `add` does with `batch`. Returns the held group too when it has to go out
    /// first: before a batch that passes through, or before one that does not fit in the group.
    fn prepare(&mut self, batch: &RecordBatch) -> Result<(Step, Option<RecordBatch>)> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok((Step::Skip, None));
        }
        // A small slice of a large batch is held by its whole buffer, so it passes through.
        let size = if !self.past_gate {
            // Counted as DataFusion's `get_reserved_bytes_for_record_batch` counts it, which the
            // sort compares with the threshold.
            self.seen_bytes = self
                .seen_bytes
                .saturating_add(get_record_batch_memory_size(batch) + sliced_size(batch)?);
            self.past_gate = self.seen_bytes >= self.gate_bytes;
            None
        } else if rows >= self.small_rows || has_small_dictionary_keys(batch) {
            None
        } else if self.backoff > 0 {
            self.backoff -= 1;
            None
        } else {
            Some(Size {
                held: get_record_batch_memory_size(batch),
                copy: sliced_size(batch)?,
            })
        };
        match size.filter(|size| size.held.max(size.copy) <= self.max_bytes) {
            Some(size) => {
                // A batch with another schema starts a new group, since the concat needs one.
                let fits = rows <= self.target_rows - self.rows
                    && size.copy <= self.max_bytes - self.copy
                    && self
                        .pending
                        .first()
                        .is_none_or(|held| held.schema() == batch.schema());
                let flushed = if fits { None } else { self.flush()? };
                Ok((Step::Hold(size), flushed))
            }
            None => Ok((Step::PassThrough, self.flush()?)),
        }
    }

    /// Returns what is ready, in input order: a flushed group, then `batch` when it is not held.
    fn add(&mut self, batch: RecordBatch, step: Step) -> Result<[Option<RecordBatch>; 2]> {
        let size = match step {
            Step::Skip => return Ok([None, None]),
            Step::PassThrough => return Ok([None, Some(batch)]),
            Step::Hold(size) => size,
        };
        let rows = batch.num_rows();
        if !self.reserve(size)? {
            // The sort can spill and this cannot, so hand it everything.
            self.backoff = REFUSED_BACKOFF_BATCHES;
            return Ok([self.flush()?, Some(batch)]);
        }
        self.pending.push(batch);
        self.rows += rows;
        self.held += size.held;
        self.copy += size.copy;
        if self.rows == self.target_rows || self.copy == self.max_bytes {
            return Ok([self.flush()?, None]);
        }
        Ok([None, None])
    }

    #[cfg(test)]
    fn push(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let (step, flushed) = self.prepare(&batch)?;
        let mut ready: Vec<_> = flushed.into_iter().collect();
        ready.extend(self.add(batch, step)?.into_iter().flatten());
        Ok(ready)
    }

    /// Reserves the held batches and the combined copy with `size` added, and no more: a consumer
    /// that cannot spill is checked only against the whole pool, so asking ahead could make Spark
    /// spill another consumer for memory never used. Returns false when refused.
    fn reserve(&mut self, size: Size) -> Result<bool> {
        let needed = self.held + size.held + self.copy + size.copy;
        let size = self.reservation.size();
        if needed <= size {
            return Ok(true);
        }
        match self.reservation.try_grow(needed - size) {
            Ok(()) => Ok(true),
            Err(DataFusionError::ResourcesExhausted(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        let out = match self.pending.len() {
            0 => None,
            1 => self.pending.pop(),
            n => {
                // The inputs' own schema, which a batch that passes through also keeps.
                let combined = concat_batches(&self.pending[0].schema(), &self.pending)?;
                self.pending.clear();
                self.input_batches.add(n);
                self.output_batches.add(1);
                Some(combined)
            }
        };
        // The sort reserves what it buffers, so hold nothing between groups.
        self.reservation.free();
        self.rows = 0;
        self.held = 0;
        self.copy = 0;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, AsArray, Date32Array, Decimal128Array, Int32Array, Int64Array, ListArray,
        MapArray, StringArray, StringDictionaryBuilder, StructArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Int32Type, Int64Type, Schema, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ints(values: std::ops::Range<i32>) -> RecordBatch {
        RecordBatch::try_from_iter([(
            "k",
            Arc::new(Int32Array::from_iter_values(values)) as ArrayRef,
        )])
        .unwrap()
    }

    fn ten_row_batches(n: i32) -> Vec<RecordBatch> {
        (0..n).map(|i| ints(i * 10..i * 10 + 10)).collect()
    }

    fn run_with(
        input: Vec<RecordBatch>,
        schema: SchemaRef,
        exec: impl FnOnce(Arc<dyn ExecutionPlan>) -> SortInputCoalesceExec,
        ctx: SessionContext,
    ) -> (Vec<RecordBatch>, MetricsSet) {
        let source = MemorySourceConfig::try_new_exec(&[input], schema, None).unwrap();
        let exec = exec(source);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let output = rt
            .block_on(collect(exec.execute(0, ctx.task_ctx()).unwrap()))
            .unwrap();
        (output, exec.metrics().unwrap())
    }

    fn run(
        input: Vec<RecordBatch>,
        target_rows: usize,
        gate_bytes: usize,
        max_bytes: usize,
    ) -> (Vec<RecordBatch>, MetricsSet) {
        let schema = input[0].schema();
        run_with(
            input,
            schema,
            |source| {
                SortInputCoalesceExec::new(source, target_rows, gate_bytes)
                    .with_max_bytes(max_bytes)
            },
            SessionContext::new(),
        )
    }

    fn row_counts(batches: &[RecordBatch]) -> Vec<usize> {
        batches.iter().map(|b| b.num_rows()).collect()
    }

    fn same_rows(output: &[RecordBatch], input: &[RecordBatch]) {
        let schema = input[0].schema();
        assert_eq!(
            concat_batches(&schema, output).unwrap(),
            concat_batches(&schema, input).unwrap()
        );
    }

    fn metric(metrics: &MetricsSet, name: &str) -> usize {
        metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0)
    }

    #[test]
    fn small_partitions_pass_through_without_a_copy() {
        let input = ten_row_batches(10);
        let (output, _) = run(input.clone(), 1024, usize::MAX, MAX_GROUP_BYTES);
        assert_eq!(output, input);
        for (out, inp) in output.iter().zip(&input) {
            assert!(Arc::ptr_eq(out.column(0), inp.column(0)));
        }
    }

    #[test]
    fn combines_small_batches_after_the_gate_in_input_order() {
        let input = ten_row_batches(10);
        // A zero gate starts combining with the first batch.
        let (output, metrics) = run(input.clone(), 40, 0, MAX_GROUP_BYTES);
        assert_eq!(row_counts(&output), [40, 40, 20]);
        same_rows(&output, &input);
        assert_eq!(metric(&metrics, "coalesced_input_batches"), 10);
        assert_eq!(metric(&metrics, "coalesced_output_batches"), 3);
        assert_eq!(metrics.output_rows(), Some(100));
    }

    #[test]
    fn a_rebuilt_coalescer_reports_through_the_original() {
        let input = ten_row_batches(10);
        let schema = input[0].schema();
        let source = MemorySourceConfig::try_new_exec(&[input], schema, None).unwrap();
        let original = Arc::new(SortInputCoalesceExec::new(
            Arc::clone(&source) as Arc<dyn ExecutionPlan>,
            40,
            0,
        ));
        let rebuilt = (Arc::clone(&original) as Arc<dyn ExecutionPlan>)
            .replace_children(
                vec![source],
                datafusion::physical_plan::ReplaceChildrenOptions::new(
                    datafusion::physical_plan::ChildrenPropertiesMode::Recompute,
                ),
            )
            .unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(collect(
            rebuilt
                .execute(0, SessionContext::new().task_ctx())
                .unwrap(),
        ))
        .unwrap();
        let metrics = original.metrics().unwrap();
        assert_eq!(metric(&metrics, "coalesced_input_batches"), 10);
    }

    #[test]
    fn the_gate_counts_bytes_before_combining() {
        let input = ten_row_batches(10);
        // The gate counts each batch as the sort does: memory size plus sliced size.
        let one = get_record_batch_memory_size(&input[0]) + sliced_size(&input[0]).unwrap();
        // The first three batches pass through; the third one crosses the gate.
        let (output, _) = run(input.clone(), 40, one * 2 + 1, MAX_GROUP_BYTES);
        assert_eq!(row_counts(&output), [10, 10, 10, 40, 30]);
        same_rows(&output, &input);
    }

    #[test]
    fn byte_cap_flushes_before_the_row_target() {
        let input = ten_row_batches(6);
        let one = get_record_batch_memory_size(&input[0]);
        let (output, _) = run(input.clone(), 1000, 0, one * 2);
        assert_eq!(row_counts(&output), [20, 20, 20]);
        same_rows(&output, &input);
    }

    #[test]
    fn large_batches_pass_through_after_flushing_the_group() {
        let small = ints(0..10);
        let large = ints(100..150);
        let input = vec![small.clone(), small.clone(), large.clone(), small.clone()];
        let (output, _) = run(input.clone(), 100, 0, MAX_GROUP_BYTES);
        assert_eq!(row_counts(&output), [20, 50, 10]);
        assert!(Arc::ptr_eq(output[1].column(0), large.column(0)));
        same_rows(&output, &input);
    }

    #[test]
    fn batches_of_512_rows_or_more_pass_through_at_the_default_target() {
        let mid = ints(0..MAX_SMALL_BATCH_ROWS as i32);
        let small = ints(0..(MAX_SMALL_BATCH_ROWS - 1) as i32);
        let input = vec![mid.clone(), mid.clone(), small.clone(), small.clone()];
        let (output, _) = run(input.clone(), 8192, 0, MAX_GROUP_BYTES);
        assert_eq!(row_counts(&output), [512, 512, 1022]);
        assert!(Arc::ptr_eq(output[0].column(0), mid.column(0)));
        same_rows(&output, &input);
    }

    #[test]
    fn empty_input_and_empty_batches() {
        let schema = ints(0..1).schema();
        let (output, _) = run_with(
            vec![],
            Arc::clone(&schema),
            |source| SortInputCoalesceExec::new(source, 8, 0),
            SessionContext::new(),
        );
        assert!(output.is_empty());

        let input = vec![ints(0..0), ints(0..3), ints(0..0), ints(3..5)];
        let (output, _) = run(input.clone(), 8, 0, MAX_GROUP_BYTES);
        assert_eq!(row_counts(&output), [5]);
        same_rows(&output, &input);
    }

    #[test]
    fn passes_through_when_memory_is_short() {
        let input = ten_row_batches(4);
        let schema = input[0].schema();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .unwrap();
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let (output, _) = run_with(
            input.clone(),
            schema,
            |source| SortInputCoalesceExec::new(source, 40, 0),
            ctx,
        );
        assert_eq!(output, input);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn the_group_cap_is_half_the_in_place_threshold() {
        let cap = |gate| {
            let source =
                MemorySourceConfig::try_new_exec(&[vec![]], ints(0..1).schema(), None).unwrap();
            SortInputCoalesceExec::new(source, 8192, gate).max_bytes
        };
        assert_eq!(cap(1024 * 1024), 512 * 1024);
        assert_eq!(cap(0), MIN_GROUP_BYTES);
        assert_eq!(cap(usize::MAX), MAX_GROUP_BYTES);
    }

    #[test]
    fn reservation_covers_a_group_and_is_released_after_it() {
        let input = ten_row_batches(3);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let source = MemorySourceConfig::try_new_exec(&[vec![]], input[0].schema(), None).unwrap();
        let exec = SortInputCoalesceExec::new(source, 1000, 0);
        let mut grouper = Grouper::new(&exec, MemoryConsumer::new("test").register(&pool), 0);
        // The reservation covers exactly the held batches and their copy.
        let mut needed = 0;
        for batch in &input {
            needed += get_record_batch_memory_size(batch) + sliced_size(batch).unwrap();
            assert!(grouper.push(batch.clone()).unwrap().is_empty());
            assert_eq!(pool.reserved(), needed);
        }
        let combined = grouper.flush().unwrap().unwrap();
        assert_eq!(
            combined,
            concat_batches(&input[0].schema(), &input).unwrap()
        );
        // The sort reserves the combined batch itself.
        assert_eq!(pool.reserved(), 0);
        assert!(grouper.flush().unwrap().is_none());
    }

    fn run_in_pool(
        input: Vec<RecordBatch>,
        target_rows: usize,
        pool_bytes: usize,
    ) -> Vec<RecordBatch> {
        let schema = input[0].schema();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .unwrap();
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let (output, _) = run_with(
            input,
            schema,
            |source| SortInputCoalesceExec::new(source, target_rows, 0),
            ctx,
        );
        assert_eq!(pool.reserved(), 0);
        output
    }

    #[test]
    fn combines_when_the_pool_holds_the_group_but_not_a_full_one() {
        let input = ten_row_batches(3);
        let one = get_record_batch_memory_size(&input[0]);
        // Room for these three batches and their copy, far less than a full group.
        let output = run_in_pool(input.clone(), 30, 2 * 3 * one);
        assert_eq!(row_counts(&output), [30]);
        same_rows(&output, &input);
    }

    #[tokio::test]
    async fn holds_no_memory_while_the_sort_takes_a_batch() {
        let input = ten_row_batches(10);
        let schema = input[0].schema();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .unwrap();
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let source = MemorySourceConfig::try_new_exec(&[input], schema, None).unwrap();
        // A 25-row target makes every third batch overflow the group.
        let exec = SortInputCoalesceExec::new(source, 25, 0);
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let mut rows = vec![];
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert_eq!(pool.reserved(), 0, "held memory while handing on a batch");
            rows.push(batch.num_rows());
        }
        assert_eq!(rows, [20, 20, 20, 20, 20]);
    }

    #[test]
    fn combining_resumes_after_the_backoff() {
        let input = ten_row_batches(70);
        let one = get_record_batch_memory_size(&input[0]);
        // Room for two batches and their copy. The third is refused, so the held group goes out
        // first and the third passes through, then the next 64 pass through under the backoff,
        // and then two more are combined before the next refusal.
        let output = run_in_pool(input.clone(), 1000, 2 * 2 * one);
        let mut expected = vec![20];
        expected.extend([10; 1 + REFUSED_BACKOFF_BATCHES]);
        expected.extend([20, 10]);
        assert_eq!(row_counts(&output), expected);
        same_rows(&output, &input);
    }

    #[test]
    fn a_small_slice_of_a_large_batch_passes_through() {
        // The size counts the whole buffer the slice shares, which is over the cap.
        let slice = ints(0..100_000).slice(0, 10);
        let input = vec![slice.clone(), slice.clone()];
        let output = run_in_pool(input.clone(), 1000, 1 << 30);
        assert_eq!(row_counts(&output), [10, 10]);
        assert!(Arc::ptr_eq(output[0].column(0), slice.column(0)));
    }

    #[test]
    fn a_schema_change_starts_a_new_group() {
        let nullable = |values: std::ops::Range<i32>| {
            let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(values))])
                .unwrap()
        };
        let input = vec![
            ints(0..10),
            ints(10..20),
            nullable(20..30),
            nullable(30..40),
        ];
        let (output, _) = run(input.clone(), 1000, 0, MAX_GROUP_BYTES);
        assert_eq!(row_counts(&output), [20, 20]);
        assert_eq!(output[0].schema(), input[0].schema());
        assert_eq!(output[1].schema(), input[2].schema());
    }

    #[test]
    fn batches_with_small_dictionary_keys_pass_through() {
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Int8Type;
        // Combined, the 200 distinct values would overflow the Int8 keys, wherever the
        // dictionary sits.
        let dict = |start: usize| -> ArrayRef {
            let mut builder = StringDictionaryBuilder::<Int8Type>::new();
            for i in start..start + 10 {
                builder.append_value(format!("v{i}"));
            }
            Arc::new(builder.finish())
        };
        let wraps: [fn(ArrayRef) -> ArrayRef; 3] = [
            |d| d,
            |d| {
                let field = Field::new("d", d.data_type().clone(), false);
                Arc::new(StructArray::new(Fields::from(vec![field]), vec![d], None))
            },
            |d| {
                let field = Arc::new(Field::new("item", d.data_type().clone(), false));
                let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1, d.len()));
                Arc::new(ListArray::new(field, offsets, d, None))
            },
        ];
        for wrap in wraps {
            let input: Vec<_> = (0..20)
                .map(|i| RecordBatch::try_from_iter([("c", wrap(dict(i * 10)))]).unwrap())
                .collect();
            let (output, _) = run(input.clone(), 1000, 0, MAX_GROUP_BYTES);
            assert_eq!(output, input, "{}", input[0].schema());
        }
    }

    #[tokio::test]
    async fn an_input_error_reaches_the_sort_and_releases_memory() {
        use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};

        #[derive(Debug)]
        struct FailsAfterTwo(SchemaRef);
        impl PartitionStream for FailsAfterTwo {
            fn schema(&self) -> &SchemaRef {
                &self.0
            }
            fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
                let items = vec![
                    Ok(ints(0..10)),
                    Ok(ints(10..20)),
                    Err(DataFusionError::Execution("input failed".into())),
                ];
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.0),
                    futures::stream::iter(items),
                ))
            }
        }

        let schema = ints(0..1).schema();
        let source = Arc::new(
            StreamingTableExec::try_new(
                Arc::clone(&schema),
                vec![Arc::new(FailsAfterTwo(Arc::clone(&schema)))],
                None,
                vec![],
                false,
                None,
            )
            .unwrap(),
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .unwrap();
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let exec = SortInputCoalesceExec::new(source, 1000, 0);
        let error = collect(exec.execute(0, ctx.task_ctx()).unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("input failed"), "{error}");
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn dropping_the_stream_while_it_holds_a_group_releases_the_reservation() {
        use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
        use futures::StreamExt;

        // Two short batches, then an input that never yields again.
        #[derive(Debug)]
        struct TwoThenPending(SchemaRef);
        impl PartitionStream for TwoThenPending {
            fn schema(&self) -> &SchemaRef {
                &self.0
            }
            fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
                let batches = futures::stream::iter([Ok(ints(0..10)), Ok(ints(10..20))])
                    .chain(futures::stream::pending());
                Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&self.0), batches))
            }
        }

        let schema = ints(0..1).schema();
        let source = Arc::new(
            StreamingTableExec::try_new(
                Arc::clone(&schema),
                vec![Arc::new(TwoThenPending(Arc::clone(&schema)))],
                None,
                vec![],
                false,
                None,
            )
            .unwrap(),
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .unwrap();
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let exec = SortInputCoalesceExec::new(source, 1000, 0);
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        assert!(futures::poll!(stream.next()).is_pending());
        assert!(pool.reserved() > 0, "the two batches should be held");
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn a_copy_of_shared_columns_is_reserved_and_capped() {
        use arrow::array::StringArray;
        // Eight fields share one string buffer of about 100 KiB. Holding a batch keeps one copy
        // alive, but concatenating writes the strings once per field.
        let strings: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..256).map(|i| format!("{i:0>400}")),
        ));
        let batch = |fields: usize| {
            let names: Vec<_> = (0..fields)
                .map(|i| Field::new(format!("f{i}"), DataType::Utf8, false))
                .collect();
            let column = StructArray::new(
                Fields::from(names),
                vec![Arc::clone(&strings); fields],
                None,
            );
            RecordBatch::try_from_iter([("s", Arc::new(column) as ArrayRef)]).unwrap()
        };
        let run_capped = |input: Vec<RecordBatch>, pool_bytes: usize| {
            let schema = input[0].schema();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build_arc()
                .unwrap();
            let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
            let (output, _) = run_with(
                input,
                schema,
                |source| SortInputCoalesceExec::new(source, 8192, 0).with_max_bytes(512 * 1024),
                ctx,
            );
            assert_eq!(pool.reserved(), 0);
            output
        };

        // Each batch's copy is about 800 KiB, over the 512 KiB cap, so neither is combined.
        let wide = vec![batch(8), batch(8)];
        assert_eq!(run_capped(wide.clone(), 1 << 30), wide);

        // With two fields the two copies fit the cap. They are combined only when the pool holds
        // the retained input and the copy together.
        let narrow = vec![batch(2), batch(2)];
        let held: usize = narrow.iter().map(get_record_batch_memory_size).sum();
        let copy: usize = narrow.iter().map(|b| sliced_size(b).unwrap()).sum();
        assert!(
            copy > held,
            "the copy counts the shared strings once per field"
        );
        assert_eq!(row_counts(&run_capped(narrow.clone(), held + copy)), [512]);
        assert_eq!(run_capped(narrow.clone(), held + copy - 1), narrow);
    }

    /// Sorts `input` by `keys` (column name, descending) under a memory limit that forces
    /// spills.
    async fn spilling_sort(
        input: Vec<RecordBatch>,
        keys: &[(&str, bool)],
        coalesce: bool,
    ) -> (Vec<RecordBatch>, MetricsSet, Option<MetricsSet>) {
        use datafusion::physical_expr::expressions::col;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_plan::sorts::sort::SortExec;

        let schema = input[0].schema();
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(2 * 1024 * 1024, 1.0)
            .build_arc()
            .unwrap();
        let config = SessionConfig::new()
            .with_batch_size(256)
            .with_sort_spill_reservation_bytes(64 * 1024)
            .with_sort_in_place_threshold_bytes(64 * 1024);
        let ctx = SessionContext::new_with_config_rt(config, runtime);
        let source = MemorySourceConfig::try_new_exec(&[input], Arc::clone(&schema), None).unwrap();
        let (sort_input, coalescer): (Arc<dyn ExecutionPlan>, _) = if coalesce {
            let coalescer = Arc::new(SortInputCoalesceExec::new(source, 256, 64 * 1024));
            (Arc::clone(&coalescer) as _, Some(coalescer))
        } else {
            (source, None)
        };
        let ordering = LexOrdering::new(keys.iter().map(|&(name, descending)| {
            PhysicalSortExpr::new(
                col(name, &schema).unwrap(),
                arrow::compute::SortOptions {
                    descending,
                    nulls_first: false,
                },
            )
        }))
        .unwrap();
        let sort = Arc::new(SortExec::new(ordering, sort_input));
        let output = collect(sort.execute(0, ctx.task_ctx()).unwrap())
            .await
            .unwrap();
        (
            output,
            sort.metrics().unwrap(),
            coalescer.map(|c| c.metrics().unwrap()),
        )
    }

    async fn check_spilling_sort(keys: &[(&str, bool)]) {
        // 24,000 wide rows in 8-row batches, shuffled so the sort has real work to do.
        let mut input: Vec<_> = (0..3000).map(|i| wide((i * 389) % 3000 * 8, 8)).collect();
        input.rotate_left(17);

        let (expected, raw_sort, _) = spilling_sort(input.clone(), keys, false).await;
        let (actual, sort, coalescer) = spilling_sort(input, keys, true).await;

        assert!(
            raw_sort.spill_count().unwrap_or(0) > 0,
            "the plain sort must spill"
        );
        assert!(
            sort.spill_count().unwrap_or(0) > 0,
            "the sort must still spill"
        );
        let schema = expected[0].schema();
        assert_eq!(
            concat_batches(&schema, &actual).unwrap(),
            concat_batches(&schema, &expected).unwrap()
        );
        let coalescer = coalescer.unwrap();
        // A little input passes through first. After that 8-row inputs are grouped up to
        // 256 rows or the 64 KiB cap that the 64 KiB threshold gives, whichever comes first.
        let combined_inputs = metric(&coalescer, "coalesced_input_batches");
        let combined_outputs = metric(&coalescer, "coalesced_output_batches");
        assert!(combined_inputs > 2500, "combined {combined_inputs} inputs");
        assert!(
            combined_outputs * 2 < combined_inputs,
            "{combined_inputs} inputs gave {combined_outputs} outputs"
        );
    }

    #[tokio::test]
    async fn a_spilling_sort_gives_the_same_result_from_combined_input() {
        check_spilling_sort(&[("id", true)]).await;
    }

    #[tokio::test]
    async fn a_spilling_multi_key_sort_gives_the_same_result_from_combined_input() {
        // A two-value date first, then a unique id, so both keys decide the order.
        check_spilling_sort(&[("d", false), ("id", true)]).await;
    }

    /// A batch of `rows` rows covering nested, decimal, temporal, dictionary and non-ASCII data.
    fn wide(start: usize, rows: usize) -> RecordBatch {
        let ids: Vec<i64> = (start..start + rows).map(|i| i as i64).collect();
        let point_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("label", DataType::Utf8, true),
        ]);
        let points = StructArray::new(
            point_fields.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values((0..rows).map(|i| i as i32))) as ArrayRef,
                Arc::new(StringArray::from_iter(
                    (0..rows).map(|i| (i % 3 != 0).then(|| format!("点{i}"))),
                )),
            ],
            None,
        );
        let list = ListArray::from_iter_primitive::<Int64Type, _, _>(
            (0..rows).map(|i| (i % 4 != 0).then(|| (0..(i % 3) as i64).map(Some))),
        );
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(StringArray::from_iter_values(
                    (0..rows * 2).map(|i| format!("键{i}")),
                )) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, true)),
                Arc::new(Int32Array::from_iter(
                    (0..rows * 2).map(|i| (i % 5 != 0).then_some(i as i32)),
                )) as ArrayRef,
            ),
        ]);
        let offsets: Vec<i32> = (0..=rows).map(|i| (i * 2) as i32).collect();
        let map = MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            arrow::buffer::OffsetBuffer::new(offsets.into()),
            entries,
            None,
            false,
        )
        .unwrap();
        let nested = StructArray::new(
            Fields::from(vec![
                Field::new("m", map.data_type().clone(), true),
                Field::new("l", list.data_type().clone(), true),
            ]),
            vec![Arc::new(map) as ArrayRef, Arc::new(list) as ArrayRef],
            Some((0..rows).map(|i| i % 7 != 0).collect()),
        );
        let decimals = Decimal128Array::from_iter((0..rows).map(|i| match i % 3 {
            0 => Some(99_999_999_999_999_999_999_999_999_999_999_999_999i128),
            1 => Some(-99_999_999_999_999_999_999_999_999_999_999_999_999i128),
            _ => None,
        }))
        .with_precision_and_scale(38, 10)
        .unwrap();
        let timestamps = TimestampMicrosecondArray::from_iter_values((0..rows).map(|i| {
            if i % 2 == 0 {
                -62_135_596_800_000_000
            } else {
                253_402_300_799_999_999
            }
        }))
        .with_timezone("America/Los_Angeles");
        let dates = Date32Array::from_iter_values((0..rows).map(|i| {
            if i % 2 == 0 {
                -719_162
            } else {
                2_932_896
            }
        }));
        let mut dict = StringDictionaryBuilder::<Int32Type>::new();
        for i in 0..rows {
            dict.append_value(["東京", "Zürich", "москва"][(start + i) % 3]);
        }
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
            ("point", Arc::new(points) as ArrayRef),
            ("nested", Arc::new(nested) as ArrayRef),
            ("dec", Arc::new(decimals) as ArrayRef),
            ("ts", Arc::new(timestamps) as ArrayRef),
            ("d", Arc::new(dates) as ArrayRef),
            ("dict", Arc::new(dict.finish()) as ArrayRef),
        ])
        .unwrap()
    }

    #[test]
    fn combines_nested_and_typed_columns() {
        let input: Vec<_> = (0..12).map(|i| wide(i * 7, 7)).collect();
        let (output, _) = run(input.clone(), 30, 0, MAX_GROUP_BYTES);
        assert_eq!(row_counts(&output), [28, 28, 28]);
        same_rows(&output, &input);
        let dict = output[0].column(6).as_dictionary::<Int32Type>();
        assert_eq!(dict.len(), 28);
        assert_eq!(output[0].schema(), input[0].schema());
    }

    #[test]
    fn zero_column_batches_keep_their_row_counts() {
        let schema = Arc::new(Schema::empty());
        let batch = |rows| {
            RecordBatch::try_new_with_options(
                Arc::clone(&schema),
                vec![],
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap()
        };
        let (output, _) = run(vec![batch(3), batch(4)], 100, 0, MAX_GROUP_BYTES);
        assert_eq!(row_counts(&output), [7]);
    }
}
