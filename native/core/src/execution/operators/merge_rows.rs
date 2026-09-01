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

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow::compute::kernels::boolean::{and, and_not, not};
use arrow::compute::{filter_record_batch, prep_null_mask_filter};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::utils::memory::estimate_memory_size;
use datafusion::common::{DataFusionError, HashSet, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream,
    },
};
use datafusion_comet_common::{cast_and_stamp_schema, SparkError};
use futures::{Stream, StreamExt};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// A MergeRows instruction: condition plus zero (Discard), one (Keep), or two (Split)
/// output row projections.
#[derive(Debug, Clone)]
pub struct MergeInstructionExec {
    pub condition: Arc<dyn PhysicalExpr>,
    pub outputs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
}

/// Immutable MergeRows predicates, instruction groups, and optional cardinality row ID.
#[derive(Debug)]
struct MergeConfig {
    is_source_row_present: Arc<dyn PhysicalExpr>,
    is_target_row_present: Arc<dyn PhysicalExpr>,
    matched_instructions: Vec<MergeInstructionExec>,
    not_matched_instructions: Vec<MergeInstructionExec>,
    not_matched_by_source_instructions: Vec<MergeInstructionExec>,
    /// `Some(ordinal)` when cardinality checking is requested; `None` turns it off. One field
    /// instead of a `(bool, usize)` pair, since the ordinal is meaningless without the flag.
    row_id_ordinal: Option<usize>,
}

impl MergeConfig {
    /// Validate the optional row ID against the current child schema.
    fn validate(&self, child: &Arc<dyn ExecutionPlan>) -> Result<(), DataFusionError> {
        if let Some(ordinal) = self.row_id_ordinal {
            let child_schema = child.schema();
            let child_fields = child_schema.fields().len();
            if ordinal >= child_fields {
                return Err(DataFusionError::Internal(format!(
                    "MergeRows: row id ordinal {ordinal} is out of range for a child with \
                     {child_fields} columns"
                )));
            }
            let data_type = child_schema.field(ordinal).data_type();
            if data_type != &DataType::Int64 {
                return Err(DataFusionError::Internal(format!(
                    "MergeRows: row id column at ordinal {ordinal} must be Int64, got {data_type}"
                )));
            }
        }
        Ok(())
    }
}

/// Native implementation of Spark's row-level `MergeRowsExec` dispatch operator.
#[derive(Debug)]
pub struct MergeRowsExec {
    config: Arc<MergeConfig>,
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl MergeRowsExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        is_source_row_present: Arc<dyn PhysicalExpr>,
        is_target_row_present: Arc<dyn PhysicalExpr>,
        matched_instructions: Vec<MergeInstructionExec>,
        not_matched_instructions: Vec<MergeInstructionExec>,
        not_matched_by_source_instructions: Vec<MergeInstructionExec>,
        row_id_ordinal: Option<usize>,
        child: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let config = Arc::new(MergeConfig {
            is_source_row_present,
            is_target_row_present,
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            row_id_ordinal,
        });
        config.validate(&child)?;

        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            // One output batch per input batch -- nothing is buffered until the input ends, so
            // this is `Incremental`, not `Final`.
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            config,
            child,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for MergeRowsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometMergeRowsExec")
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for MergeRowsExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let [child] = children.as_slice() else {
            return Err(DataFusionError::Internal(format!(
                "MergeRows expects exactly one child, got {}",
                children.len()
            )));
        };
        let child = Arc::clone(child);
        // A replacement child may have a different row-ID schema.
        self.config.validate(&child)?;
        Ok(Arc::new(MergeRowsExec {
            config: Arc::clone(&self.config),
            child,
            schema: Arc::clone(&self.schema),
            cache: Arc::clone(&self.cache),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let reservation = MemoryConsumer::new(format!("CometMergeRowsExec[{partition}]"))
            .register(&context.runtime_env().memory_pool);
        let child_stream = self.child.execute(partition, Arc::clone(&context))?;
        Ok(Box::pin(MergeRowsStream {
            config: Arc::clone(&self.config),
            child_stream,
            schema: Arc::clone(&self.schema),
            // Cardinality state is partition-scoped and must survive batch boundaries.
            seen: HashSet::new(),
            reservation,
            baseline: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn name(&self) -> &str {
        "CometMergeRowsExec"
    }
}

pub struct MergeRowsStream {
    config: Arc<MergeConfig>,
    child_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    /// Target row ids already seen in a matched pair. Accumulated across *every* batch polled
    /// from this stream (i.e. for the lifetime of the partition), not reset per batch -- a
    /// cardinality violation where the two matching source rows land in different Arrow batches
    /// must still be caught. Mirrors Spark's `MergeRowsExec.BitmapCardinalityValidator`, which is
    /// task-scoped, not batch-scoped.
    seen: HashSet<i64>,
    /// Pool accounting for [`MergeRowsStream::seen`]. Held for the life of the stream and
    /// released on drop.
    reservation: MemoryReservation,
    /// `elapsed_compute` / `output_rows` / `output_batches`. Without these the merge operator is
    /// invisible in the Spark UI and in benchmarking, so its share of a slow MERGE cannot be
    /// separated from the upstream join/scan or the downstream write. `record_poll` (called at
    /// the end of every `poll_next`) increments `output_rows` and `output_batches` itself for
    /// every emitted batch -- do not additionally track either metric alongside `baseline`, or
    /// the pair double-counts.
    ///
    /// `output_rows / output_batches` is this operator's average output batch size -- a
    /// fragmented merge output slows the downstream writer even when the writer itself is fast,
    /// so this is the number to check first when a MERGE's write phase is slow.
    baseline: BaselineMetrics,
}

/// Fixed overhead of the `seen` `HashSet` control struct itself (not its bucket array), passed as
/// the `fixed_size` term to [`estimate_memory_size`] so the reservation covers the whole
/// collection rather than only its bucket array.
const SEEN_FIXED_BYTES: usize = std::mem::size_of::<HashSet<i64>>();

/// Conservative correction for details DataFusion's generic hash-table estimator deliberately
/// does not model exactly. hashbrown's raw table stores an additional mirrored control group and,
/// for very small tables, enforces a minimum bucket count so failed lookups always terminate.
/// `estimate_memory_size::<i64>(1, ...)`, for example, models one bucket while hashbrown allocates
/// four i64 buckets plus its control group. 64 bytes covers that first-allocation gap as well as
/// control-group/alignment overhead at subsequent rehashes on current 32/64-bit targets. The tests
/// below compare the reservation against hashbrown's own `allocation_size()` so a future layout
/// change that exceeds this correction fails loudly rather than silently weakening the pool limit.
const SEEN_HASH_TABLE_SLACK_BYTES: usize = 64;

fn estimate_seen_memory_size(num_elements: usize) -> Result<usize, DataFusionError> {
    estimate_memory_size::<i64>(num_elements, SEEN_FIXED_BYTES)?
        .checked_add(SEEN_HASH_TABLE_SLACK_BYTES)
        .ok_or_else(|| {
            DataFusionError::ResourcesExhausted(
                "MergeRows: cardinality memory estimate overflow".to_string(),
            )
        })
}

/// Rewrites NULL slots to `false`. Every boolean in this operator goes through Spark's
/// `BasePredicate.eval`, which collapses a NULL predicate result to `false`, but Arrow's
/// `and`/`and_not` kernels propagate NULL -- left unflattened, a NULL condition would poison
/// `run_group`'s shrinking `remaining` mask and silently drop the row from every later
/// instruction in the group, including the catch-all `Keep(TrueLiteral, ...)` Spark's
/// `RewriteMergeIntoTable` appends. `arrow::compute::prep_null_mask_filter` does the flattening
/// but panics when there are no nulls, hence the guard.
fn null_to_false(array: &BooleanArray) -> BooleanArray {
    if array.null_count() == 0 {
        array.clone()
    } else {
        prep_null_mask_filter(array)
    }
}

fn eval_bool(
    expr: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<BooleanArray, DataFusionError> {
    let array: ArrayRef = expr.evaluate(batch)?.into_array(batch.num_rows())?;
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .map(null_to_false)
        .ok_or_else(|| DataFusionError::Internal("MergeRows: expected boolean array".to_string()))
}

fn project(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let mut columns = Vec::with_capacity(exprs.len());
    for expr in exprs {
        columns.push(expr.evaluate(batch)?.into_array(batch.num_rows())?);
    }
    // A clause's projected nested types (e.g. a struct built by `named_struct`) reflect only
    // that expression's own nullability, not the wider nullability `self.schema` carries to
    // accommodate every instruction's output -- a later `Keep` referencing the target schema's
    // nullable field, for instance. `cast_and_stamp_schema` reconciles each column with the
    // declared schema the way `ExpandStream::expand` does for the same reason.
    cast_and_stamp_schema("MergeRows", schema, columns, batch.num_rows())
}

/// Filters `batch` to `mask`, skipping the copy when every row is already selected.
fn filter_or_pass_through(
    batch: &RecordBatch,
    mask: &BooleanArray,
) -> Result<RecordBatch, DataFusionError> {
    if mask.true_count() == batch.num_rows() {
        Ok(batch.clone())
    } else {
        filter_record_batch(batch, mask).map_err(|e| e.into())
    }
}

/// Runs one instruction group (matched / not_matched / not_matched_by_source) over the rows
/// selected by `group_mask`, producing zero or more output batches. Reproduces Spark's ordered,
/// first-match-wins clause evaluation (`MergeRows`: "the first matching expression is used")
/// by physically shrinking the working batch to the rows still unclaimed after each instruction.
///
/// Output rows come out grouped by the instruction that produced them rather than in input row
/// order -- this operator is set-at-a-time where Spark's is row-at-a-time. That is safe because
/// nothing downstream depends on this operator's row order: Iceberg applies its required
/// distribution and ordering to the *write's* input, so `DistributionAndOrderingUtils` places the
/// repartition and sort above `MergeRows`, not below it. A partitioned `ClusteredWriter` therefore
/// still receives partition-clustered input. Do not wire a writer directly to this operator's
/// output without preserving that sort.
fn run_group(
    batch: &RecordBatch,
    group_mask: &BooleanArray,
    instructions: &[MergeInstructionExec],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if instructions.is_empty() || group_mask.true_count() == 0 {
        return Ok(vec![]);
    }

    // Narrow to the group's rows *before* evaluating any condition. Spark reaches
    // `applyInstructions` only after a row has been routed to a group, so a clause condition is
    // never evaluated against a row belonging to another group. Evaluating over the whole batch
    // would additionally expose rows the clause was never meant to see -- e.g. a NOT MATCHED
    // condition `s.a / s.b > 1` evaluated on matched rows, where `s.b` is a real value and may
    // be 0, raising an ANSI divide-by-zero that Spark would never produce.
    let mut current = filter_or_pass_through(batch, group_mask)?;
    let mut out = Vec::new();
    let last = instructions.len() - 1;

    for (idx, instr) in instructions.iter().enumerate() {
        if current.num_rows() == 0 {
            // Nothing left in this group can fire.
            break;
        }

        // A row already claimed by an earlier instruction must never reach a later one's
        // condition -- not just have its result masked out, but be physically absent from
        // `current` -- since evaluating the condition itself (e.g. `s.a / s.b > 1`) can raise
        // under ANSI for a row Spark would never have reevaluated. This is why `current` shrinks
        // every iteration instead of narrowing a same-sized mask alongside a stable batch.
        //
        // Spark's `RewriteMergeIntoTable` appends an unconditional catch-all
        // `Keep(TrueLiteral, ...)` as the last instruction of the matched / not-matched-by-source
        // groups. A literal condition evaluates to a `ColumnarValue::Scalar`, so handle it
        // without materializing an all-true same-value array.
        let fire = match instr.condition.evaluate(&current)? {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                BooleanArray::from(vec![true; current.num_rows()])
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false) | None)) => continue,
            value => value
                .into_array(current.num_rows())?
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(null_to_false)
                .ok_or_else(|| {
                    DataFusionError::Internal("MergeRows: expected boolean array".to_string())
                })?,
        };

        if fire.true_count() == 0 {
            continue;
        }

        let filtered = filter_or_pass_through(&current, &fire)?;
        for output_exprs in &instr.outputs {
            out.push(project(&filtered, output_exprs, schema)?);
        }

        if idx != last {
            current = if fire.true_count() == current.num_rows() {
                current.slice(0, 0)
            } else {
                filter_record_batch(&current, &not(&fire)?)?
            };
        }
    }

    Ok(out)
}

/// Detects a target row matched by more than one source row (Spark's
/// `MERGE_CARDINALITY_VIOLATION`), mirroring `MergeRowsExec.BitmapCardinalityValidator`: track
/// row ids seen within the matched group and fail on the first repeat.
fn check_cardinality(
    batch: &RecordBatch,
    matched_mask: &BooleanArray,
    row_id_ordinal: usize,
    seen: &mut HashSet<i64>,
    reservation: &mut MemoryReservation,
) -> Result<(), DataFusionError> {
    // Read the row-id column in place and walk only the positions the mask selects. Filtering
    // first would allocate a copy of the column on every poll purely to iterate it, and
    // `filter_record_batch` over the whole batch would copy every other column too -- neither is
    // needed, since this check reads one column and keeps nothing.
    let row_ids = batch
        .column(row_id_ordinal)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("MergeRows: row id column must be Int64".to_string())
        })?;

    // Walk the matched rows one at a time, and only ask the pool for more memory at the moment
    // `seen` is about to grow its bucket array -- never speculatively for the whole batch.
    //
    // Two invariants this ordering preserves:
    //  1. A duplicate id is detected *before* any allocation it would have triggered, so a MERGE
    //     that is already a cardinality violation reports `MERGE_CARDINALITY_VIOLATION` rather
    //     than a memory error, matching Spark's row-at-a-time `BitmapCardinalityValidator`.
    //  2. The DataFusion pool gets to reject the growth before hashbrown asks the allocator for
    //     the enlarged table (charging afterwards is too late -- the allocation already happened).
    //
    // DataFusion's estimator intentionally models hashbrown approximately, so
    // `estimate_seen_memory_size` adds a small conservative correction for the mirrored control
    // group / minimum small-table buckets. The reservation only grows, since `seen` never shrinks.
    for i in matched_mask.values().set_indices() {
        // Spark's `BitmapCardinalityValidator.validate` reads `InternalRow.getLong(ordinal)`
        // unconditionally, with no null check (confirmed via bytecode): a null long field reads
        // as 0 (`UnsafeRow.setNullAt` zeroes the value slot; `GenericInternalRow`'s boxed-null
        // unboxes to 0 via Scala's `null.asInstanceOf[Long]`). Mirror that exactly rather than
        // skipping null row ids -- skipping would miss a real cardinality violation where two
        // matched rows both carry a null row id, and reading the Arrow value buffer's raw byte
        // content at a null slot (`row_ids.value(i)` without the null check) would not, since
        // Arrow does not guarantee null slots are zero-filled.
        let id = if row_ids.is_null(i) {
            0
        } else {
            row_ids.value(i)
        };

        if seen.len() < seen.capacity() {
            // `insert` cannot grow the table here, so a single lookup covers both the
            // duplicate check and the insert.
            if !seen.insert(id) {
                return Err(DataFusionError::External(Box::new(
                    SparkError::MergeCardinalityViolation,
                )));
            }
        } else {
            // The table is full: an `insert` of a new id would rehash. Check for the duplicate
            // first (no allocation), then reserve for the growth before it happens. The extra
            // `contains` lookup only runs at a rehash boundary -- O(log n) times over the set.
            if seen.contains(&id) {
                return Err(DataFusionError::External(Box::new(
                    SparkError::MergeCardinalityViolation,
                )));
            }
            let next_len = seen.len().checked_add(1).ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "MergeRows: cardinality set length overflow".to_string(),
                )
            })?;
            let projected_bytes = estimate_seen_memory_size(next_len)?;
            let additional = projected_bytes.saturating_sub(reservation.size());
            reservation.try_grow(additional)?;
            if let Err(e) = seen.try_reserve(1) {
                // Pool admission happened before allocation by design. If the allocator itself
                // then fails, return that admission immediately instead of leaving the pool
                // artificially charged until the whole stream is dropped.
                if additional != 0 {
                    reservation.shrink(additional);
                }
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "MergeRows: failed to allocate cardinality set: {e}"
                )));
            }
            seen.insert(id);
        }
    }
    Ok(())
}

fn process_batch(
    batch: RecordBatch,
    config: &MergeConfig,
    // Caller-owned and threaded across every batch of the partition -- must NOT be created
    // fresh per call, or a cardinality violation split across two batches goes undetected.
    seen: &mut HashSet<i64>,
    reservation: &mut MemoryReservation,
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let source_present = eval_bool(&config.is_source_row_present, &batch)?;
    let target_present = eval_bool(&config.is_target_row_present, &batch)?;

    let matched_mask = and(&target_present, &source_present)?;
    let not_matched_mask = and_not(&source_present, &target_present)?;
    let not_matched_by_source_mask = and_not(&target_present, &source_present)?;

    // Checks cardinality for every matched row in the batch before evaluating any instruction,
    // whereas Spark validates and applies instructions row-at-a-time, interleaved in scan order.
    // When a single batch contains both a cardinality violation and an unrelated
    // instruction-evaluation error (e.g. an ANSI divide-by-zero) on different rows, this can
    // surface a different error than Spark would for the same input, depending on which row
    // comes first. Reordering these two phases would only flip which case diverges, not fix it --
    // a true fix needs row-at-a-time evaluation, which conflicts with this operator's vectorized
    // design (see `run_group`'s doc comment). Accepted as a known limitation: both paths still
    // fail the query, just with a different error.
    if let Some(row_id_ordinal) = config.row_id_ordinal {
        check_cardinality(&batch, &matched_mask, row_id_ordinal, seen, reservation)?;
    }

    let mut batches = Vec::new();
    for (mask, instructions) in [
        (&matched_mask, &config.matched_instructions),
        (&not_matched_mask, &config.not_matched_instructions),
        (
            &not_matched_by_source_mask,
            &config.not_matched_by_source_instructions,
        ),
    ] {
        batches.extend(run_group(&batch, mask, instructions, schema)?);
    }

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }

    arrow::compute::concat_batches(schema, &batches).map_err(|e| e.into())
}

/// Upper bound on how many consecutive all-discarded input batches `poll_next` will absorb
/// within a single call before yielding to the executor. Without this, a MERGE dominated by
/// DELETE clauses against an upstream that resolves synchronously (e.g. an already-materialized
/// child) could loop indefinitely inside one `poll_next` call without ever returning
/// `Poll::Pending`, starving other tasks on the same worker thread. 128 mirrors the budget Tokio
/// itself applies to cooperative scheduling.
const MAX_DISCARDED_BATCHES_PER_POLL: u32 = 128;

impl Stream for MergeRowsStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `MergeRowsStream` is structurally `Unpin` (every field is), so projecting a plain
        // `&mut Self` out of the `Pin` is sound. Doing so lets us split disjoint field borrows --
        // `&mut this.seen` alongside the other `&this.*` borrows -- so cardinality state
        // accumulates across every batch polled from this stream instead of resetting per batch.
        let this = self.get_mut();
        // Loop rather than return the empty result: an input batch whose rows are all discarded
        // (a copy-on-write DELETE clause, say) produces no output rows, and forwarding a zero-row
        // batch makes every downstream stage pay for nothing -- an FFI export/import pair into
        // the write pipeline, and in the partitioned case a full `RecordBatchPartitionSplitter`
        // pass. Keep pulling until there is something to emit or the child is done.
        let mut discarded_budget = MAX_DISCARDED_BATCHES_PER_POLL;
        loop {
            let poll = match this.child_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    // Times only this operator's own work; the upstream poll above is
                    // deliberately outside the timer so `elapsed_compute` is not the whole
                    // pipeline's wall clock.
                    let _timer = this.baseline.elapsed_compute().timer();
                    let result = process_batch(
                        batch,
                        &this.config,
                        &mut this.seen,
                        &mut this.reservation,
                        &this.schema,
                    );
                    match result {
                        Ok(batch) if batch.num_rows() == 0 => {
                            discarded_budget -= 1;
                            if discarded_budget == 0 {
                                // Give the executor a chance to run other tasks before pulling
                                // more batches; re-polling this stream is what drives progress
                                // here, so wake immediately rather than waiting on the child.
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                            continue;
                        }
                        other => Poll::Ready(Some(other)),
                    }
                }
                other => other,
            };
            return this.baseline.record_poll(poll);
        }
    }
}

impl RecordBatchStream for MergeRowsStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StructArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
    use datafusion::logical_expr::Operator as DFOperator;
    use datafusion::physical_expr::expressions::{binary, col, lit};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, true),
            Field::new("val", DataType::Int32, true),
            Field::new("target_present", DataType::Boolean, false),
            Field::new("source_present", DataType::Boolean, false),
        ]))
    }

    fn test_batch(
        row_ids: Vec<i64>,
        vals: Vec<i32>,
        target: Vec<bool>,
        source: Vec<bool>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(row_ids)),
                Arc::new(Int32Array::from(vals)),
                Arc::new(BooleanArray::from(target)),
                Arc::new(BooleanArray::from(source)),
            ],
        )
        .unwrap()
    }

    /// Pool accounting is not what these tests exercise, so they run against an unbounded pool.
    fn test_reservation() -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        MemoryConsumer::new("test").register(&pool)
    }

    fn out_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, true)]))
    }

    fn keep_all() -> MergeInstructionExec {
        MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![col("val", &test_schema()).unwrap()]],
        }
    }

    fn discard_all() -> MergeInstructionExec {
        MergeInstructionExec {
            condition: lit(true),
            outputs: vec![],
        }
    }

    fn test_config(
        matched_instructions: Vec<MergeInstructionExec>,
        not_matched_instructions: Vec<MergeInstructionExec>,
        not_matched_by_source_instructions: Vec<MergeInstructionExec>,
        row_id_ordinal: Option<usize>,
    ) -> MergeConfig {
        MergeConfig {
            is_source_row_present: col("source_present", &test_schema()).unwrap(),
            is_target_row_present: col("target_present", &test_schema()).unwrap(),
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            row_id_ordinal,
        }
    }

    #[test]
    fn keep_matched_discard_rest() {
        let batch = test_batch(
            vec![1, 2, 3],
            vec![10, 20, 30],
            vec![true, false, true],
            vec![true, true, false],
        );
        let config = test_config(
            vec![keep_all()],
            vec![keep_all()],
            vec![discard_all()],
            None,
        );
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut got: Vec<i32> = vals.iter().flatten().collect();
        got.sort();
        assert_eq!(got, vec![10, 20]);
    }

    #[test]
    fn first_match_wins_ordering() {
        let batch = test_batch(vec![1], vec![5], vec![true], vec![true]);
        let cond_false = MergeInstructionExec {
            condition: binary(
                col("val", &test_schema()).unwrap(),
                DFOperator::Gt,
                lit(100i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(1i32)]],
        };
        let cond_true = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(2i32)]],
        };
        let config = test_config(vec![cond_false, cond_true], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.value(0), 2);
    }

    #[test]
    fn later_condition_is_not_evaluated_on_an_already_claimed_row() {
        let batch = test_batch(vec![1, 2], vec![0, 2], vec![true, true], vec![true, true]);
        let claims_zero = MergeInstructionExec {
            condition: binary(
                col("val", &test_schema()).unwrap(),
                DFOperator::Eq,
                lit(0i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(111i32)]],
        };
        let divides_by_val = MergeInstructionExec {
            condition: binary(
                binary(
                    lit(2i32),
                    DFOperator::Divide,
                    col("val", &test_schema()).unwrap(),
                    &test_schema(),
                )
                .unwrap(),
                DFOperator::Gt,
                lit(0i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(222i32)]],
        };
        let config = test_config(vec![claims_zero, divides_by_val], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut got: Vec<i32> = vals.iter().flatten().collect();
        got.sort();
        assert_eq!(got, vec![111, 222]);
    }

    #[test]
    fn null_condition_falls_through_to_next_instruction() {
        let batch = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int32Array::from(vec![None::<i32>])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )
        .unwrap();
        let cond_null = MergeInstructionExec {
            condition: binary(
                col("val", &test_schema()).unwrap(),
                DFOperator::Gt,
                lit(100i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(1i32)]],
        };
        let keep_catch_all = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(2i32)]],
        };
        let config = test_config(vec![cond_null, keep_catch_all], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        assert_eq!(
            out.num_rows(),
            1,
            "row with a NULL clause condition must fall through to the catch-all Keep, not \
             disappear from the rewritten data file"
        );
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.value(0), 2);
    }

    #[test]
    fn null_row_presence_flag_treated_as_false() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, true),
            Field::new("val", DataType::Int32, true),
            Field::new("target_present", DataType::Boolean, true),
            Field::new("source_present", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(BooleanArray::from(vec![None::<bool>])),
            ],
        )
        .unwrap();
        let config = MergeConfig {
            is_source_row_present: col("source_present", &schema).unwrap(),
            is_target_row_present: col("target_present", &schema).unwrap(),
            matched_instructions: vec![],
            not_matched_instructions: vec![],
            not_matched_by_source_instructions: vec![MergeInstructionExec {
                condition: lit(true),
                outputs: vec![vec![col("val", &schema).unwrap()]],
            }],
            row_id_ordinal: None,
        };
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 1);
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.value(0), 10);
    }

    #[test]
    fn condition_not_evaluated_outside_its_group() {
        let batch = test_batch(vec![1, 2], vec![0, 5], vec![true, false], vec![true, true]);
        let div_cond = MergeInstructionExec {
            condition: binary(
                binary(
                    lit(10i32),
                    DFOperator::Divide,
                    col("val", &test_schema()).unwrap(),
                    &test_schema(),
                )
                .unwrap(),
                DFOperator::Gt,
                lit(1i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![col("val", &test_schema()).unwrap()]],
        };
        assert!(
            eval_bool(&div_cond.condition, &batch).is_err(),
            "test is only meaningful if batch-wide evaluation of this condition errors"
        );
        let config = test_config(vec![keep_all()], vec![div_cond], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .expect("not-matched condition must not be evaluated against the matched row");
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut got: Vec<i32> = vals.iter().flatten().collect();
        got.sort();
        assert_eq!(got, vec![0, 5]);
    }

    fn bounded_reservation(limit: usize) -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        MemoryConsumer::new("test").register(&pool)
    }

    fn run_cardinality(
        n: usize,
        batch_rows: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<HashSet<i64>, DataFusionError> {
        let mut seen = HashSet::new();
        let mut next = 0i64;
        while (next as usize) < n {
            let end = ((next as usize) + batch_rows).min(n) as i64;
            let ids: Vec<i64> = (next..end).collect();
            let len = ids.len();
            let batch = test_batch(ids, vec![0; len], vec![true; len], vec![true; len]);
            let mask = BooleanArray::from(vec![true; len]);
            check_cardinality(&batch, &mask, 0, &mut seen, reservation)?;
            next = end;
        }
        Ok(seen)
    }

    fn assert_seen_fully_reserved(seen: &HashSet<i64>, reservation: &MemoryReservation) {
        let actual = seen.allocation_size().saturating_add(SEEN_FIXED_BYTES);
        assert!(
            reservation.size() >= actual,
            "reserved {} bytes < actual HashSet footprint {} bytes (len={}, capacity={})",
            reservation.size(),
            actual,
            seen.len(),
            seen.capacity()
        );
    }

    #[test]
    fn cardinality_state_is_accounted_to_the_memory_pool() {
        let mut reservation = test_reservation();
        let seen = run_cardinality(9, 4, &mut reservation).unwrap();
        assert!(
            reservation.size() > 0,
            "`seen` must be visible to the memory pool"
        );
        assert_seen_fully_reserved(&seen, &reservation);
    }

    #[test]
    fn cardinality_reservation_covers_actual_hashbrown_allocations() {
        for &(n, batch_rows) in &[
            (1usize, 1usize),
            (2, 1),
            (3, 1),
            (7, 1),
            (8, 1),
            (9, 1),
            (17, 1),
            (64, 8),
            (200, 16),
        ] {
            let mut reservation = test_reservation();
            let seen = run_cardinality(n, batch_rows, &mut reservation).unwrap();
            assert_seen_fully_reserved(&seen, &reservation);
        }
    }

    #[test]
    fn cardinality_state_cannot_exceed_a_bounded_pool() {
        let n = 917_505;
        let mut reservation = bounded_reservation(16 * 1024 * 1024);
        let err = run_cardinality(n, 4096, &mut reservation).unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "expected the pool to reject the oversized cardinality table, got {err}"
        );

        let needed = estimate_seen_memory_size(n).unwrap();
        let mut ok_reservation = bounded_reservation(needed + 8 * 1024 * 1024);
        let seen = run_cardinality(n, 4096, &mut ok_reservation).unwrap();
        assert_eq!(seen.len(), n);
        assert_seen_fully_reserved(&seen, &ok_reservation);
    }

    #[test]
    fn cardinality_violation_wins_over_memory_exhaustion() {
        let mut seen = HashSet::new();
        let mut reservation = bounded_reservation(0);
        let batch = test_batch(vec![1], vec![0], vec![true], vec![true]);
        check_cardinality(
            &batch,
            &BooleanArray::from(vec![true]),
            0,
            &mut seen,
            &mut test_reservation(),
        )
        .unwrap();
        let dup = test_batch(vec![1, 2], vec![0, 0], vec![true, true], vec![true, true]);
        let err = check_cardinality(
            &dup,
            &BooleanArray::from(vec![true, true]),
            0,
            &mut seen,
            &mut reservation,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("MERGE_CARDINALITY_VIOLATION"),
            "expected cardinality violation, got {err}"
        );
    }

    #[test]
    fn cardinality_violation_detected() {
        let batch = test_batch(vec![1, 1], vec![10, 20], vec![true, true], vec![true, true]);
        let matched_mask = BooleanArray::from(vec![true, true]);
        let mut seen = HashSet::new();
        let result =
            check_cardinality(&batch, &matched_mask, 0, &mut seen, &mut test_reservation());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }

    #[test]
    fn split_produces_two_rows() {
        let batch = test_batch(vec![1], vec![7], vec![true], vec![true]);
        let split = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(1i32)], vec![lit(2i32)]],
        };
        let config = test_config(vec![split], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let got: Vec<i32> = vals.iter().flatten().collect();
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn cardinality_violation_detected_for_null_row_ids() {
        let batch = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![None, None])),
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(BooleanArray::from(vec![true, true])),
                Arc::new(BooleanArray::from(vec![true, true])),
            ],
        )
        .unwrap();
        let matched_mask = BooleanArray::from(vec![true, true]);
        let result = check_cardinality(
            &batch,
            &matched_mask,
            0,
            &mut HashSet::new(),
            &mut test_reservation(),
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }

    #[tokio::test]
    async fn all_discarded_batch_is_not_emitted() {
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::prelude::SessionContext;

        let discarded = test_batch(vec![1], vec![10], vec![true], vec![true]);
        let kept = test_batch(vec![2], vec![20], vec![false], vec![true]);
        let source =
            MemorySourceConfig::try_new_exec(&[vec![discarded, kept]], test_schema(), None)
                .unwrap();

        let exec = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![discard_all()],
            vec![keep_all()],
            vec![],
            None,
            source,
            out_schema(),
        )
        .unwrap();

        let ctx = SessionContext::new();
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn out_of_range_row_id_ordinal_is_rejected() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let err = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![keep_all()],
            vec![],
            vec![],
            Some(99),
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("row id ordinal"));
    }

    #[test]
    fn non_int64_row_id_is_rejected_at_plan_construction() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let wrong_schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
            Field::new("target_present", DataType::Boolean, false),
            Field::new("source_present", DataType::Boolean, false),
        ]));
        let source =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&wrong_schema), None).unwrap();
        let err = MergeRowsExec::try_new(
            col("source_present", &wrong_schema).unwrap(),
            col("target_present", &wrong_schema).unwrap(),
            vec![],
            vec![],
            vec![],
            Some(0),
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("must be Int64"));
    }

    #[test]
    fn with_new_children_rejects_ordinal_out_of_range_for_new_child() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let original_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("row_id", DataType::Int64, true),
        ]));
        let source =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&original_schema), None)
                .unwrap();
        let exec = Arc::new(
            MergeRowsExec::try_new(
                lit(true),
                lit(true),
                vec![],
                vec![],
                vec![],
                Some(3),
                source,
                out_schema(),
            )
            .unwrap(),
        );

        let narrow_schema = Arc::new(Schema::new(vec![Field::new(
            "row_id",
            DataType::Int64,
            true,
        )]));
        let narrow_child =
            MemorySourceConfig::try_new_exec(&[vec![]], narrow_schema, None).unwrap();
        let err = exec.with_new_children(vec![narrow_child]).unwrap_err();
        assert!(
            err.to_string().contains("out of range"),
            "expected an out-of-range ordinal error, got: {err}"
        );
    }

    #[test]
    fn with_new_children_rejects_wrong_arity() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let exec = Arc::new(
            MergeRowsExec::try_new(
                col("source_present", &test_schema()).unwrap(),
                col("target_present", &test_schema()).unwrap(),
                vec![keep_all()],
                vec![],
                vec![],
                Some(0),
                source,
                out_schema(),
            )
            .unwrap(),
        );
        let no_children = Arc::clone(&exec).with_new_children(vec![]).unwrap_err();
        assert!(no_children.to_string().contains("exactly one child"));

        let child_a = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let child_b = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let two_children = exec.with_new_children(vec![child_a, child_b]).unwrap_err();
        assert!(two_children.to_string().contains("exactly one child"));
    }

    #[test]
    fn with_new_children_revalidates_row_id_schema() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let exec = Arc::new(
            MergeRowsExec::try_new(
                col("source_present", &test_schema()).unwrap(),
                col("target_present", &test_schema()).unwrap(),
                vec![keep_all()],
                vec![],
                vec![],
                Some(0),
                source,
                out_schema(),
            )
            .unwrap(),
        );

        let narrow_schema = Arc::new(Schema::new(vec![Field::new(
            "only_col",
            DataType::Int32,
            true,
        )]));
        let narrow_child =
            MemorySourceConfig::try_new_exec(&[vec![]], narrow_schema, None).unwrap();
        let err = exec.with_new_children(vec![narrow_child]).unwrap_err();
        assert!(err.to_string().contains("must be Int64"));
    }

    #[test]
    fn cardinality_violation_detected_across_batches() {
        let mut seen = HashSet::new();
        let batch1 = test_batch(vec![1], vec![10], vec![true], vec![true]);
        let batch2 = test_batch(vec![1], vec![20], vec![true], vec![true]);
        let config = test_config(vec![keep_all()], vec![], vec![], Some(0));

        let first = process_batch(
            batch1,
            &config,
            &mut seen,
            &mut test_reservation(),
            &out_schema(),
        );
        assert!(first.is_ok());

        let second = process_batch(
            batch2,
            &config,
            &mut seen,
            &mut test_reservation(),
            &out_schema(),
        );
        assert!(second.is_err());
        assert!(second
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }

    #[test]
    fn project_reconciles_nested_struct_nullability_with_declared_schema() {
        let source_field = Field::new("n", DataType::Boolean, false);
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(vec![source_field.clone()].into()),
            true,
        )]));
        let inner_values: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let payload_array: ArrayRef = Arc::new(StructArray::new(
            vec![source_field].into(),
            vec![inner_values],
            None,
        ));
        let batch = RecordBatch::try_new(Arc::clone(&source_schema), vec![payload_array]).unwrap();

        let target_field = Field::new("n", DataType::Boolean, true);
        let out_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(vec![target_field].into()),
            true,
        )]));

        let out = project(
            &batch,
            &[col("payload", &source_schema).unwrap()],
            &out_schema,
        )
        .expect("project must reconcile projected nested nullability with the declared schema");
        assert_eq!(
            out.schema().field(0).data_type(),
            out_schema.field(0).data_type()
        );
    }
}
