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

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchOptions};
use arrow::compute::kernels::boolean::{and, and_not};
use arrow::compute::{filter_record_batch, prep_null_mask_filter};
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, ScalarValue};
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
use datafusion_comet_common::SparkError;
use futures::{Stream, StreamExt};
use std::collections::HashSet;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// One `MergeRows.Instruction` (Keep / Discard / Split), expressed uniformly as a gating
/// condition plus zero, one, or two output row projections -- matching Spark's real
/// `condition: Expression, outputs: Seq[Seq[Expression]]` shape (Discard has zero output
/// projections, Keep has one, Split has two).
#[derive(Debug, Clone)]
pub struct MergeInstructionExec {
    pub condition: Arc<dyn PhysicalExpr>,
    pub outputs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
}

/// Configuration shared by `MergeRowsExec` and its `MergeRowsStream`: the row-presence
/// predicates, the three per-group instruction lists, and (when Spark's
/// `MergeRowsExec.checkCardinality` is on) the target row-id column's ordinal in the child
/// schema. Bundled into one struct, held behind an `Arc`, so `with_new_children` and `execute`
/// each clone one reference instead of threading seven fields by hand.
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
    /// `row_id_ordinal` indexes directly into a child batch's columns, so a value out of range
    /// for `child`'s schema would panic inside `check_cardinality` on the first batch. Called
    /// from both `try_new` and `with_new_children`, since the latter can swap in a child whose
    /// schema differs from the one this config was originally validated against.
    fn validate(&self, child: &Arc<dyn ExecutionPlan>) -> Result<(), DataFusionError> {
        if let Some(ordinal) = self.row_id_ordinal {
            let child_fields = child.schema().fields().len();
            if ordinal >= child_fields {
                return Err(DataFusionError::Internal(format!(
                    "MergeRows: row id ordinal {ordinal} is out of range for a child with \
                     {child_fields} columns"
                )));
            }
        }
        Ok(())
    }
}

/// A Comet native operator that reproduces Spark's `MergeRowsExec` (the row-level MERGE
/// dispatch operator introduced to Spark core in Iceberg 1.4.0 / SPARK-52403). Sits between the
/// target/source join and the write, deciding per row whether it becomes a kept row, is
/// discarded (a copy-on-write delete), or is split into two output rows.
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
        let child = Arc::clone(&children[0]);
        // Re-validate: an optimizer pass replacing the child here could hand back a schema the
        // row-id ordinal no longer fits, and this path bypasses `try_new` entirely otherwise.
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
            // One `seen` set per partition, created here and threaded through every batch this
            // stream polls -- see the field doc on `MergeRowsStream::seen` for why it must not
            // be reset per batch.
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

/// Conservative per-entry cost of `seen`. hashbrown stores an 8-byte key plus a 1-byte control
/// slot at a ~87.5% load factor (~10.3 bytes/element) and doubles its table on growth; 16 bytes
/// per entry covers both without needing to observe the actual capacity.
const SEEN_ENTRY_BYTES: usize = 16;

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
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|e| e.into())
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
/// via a shrinking `remaining` mask.
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
    let group_batch = filter_or_pass_through(batch, group_mask)?;
    let mut remaining = BooleanArray::from(vec![true; group_batch.num_rows()]);
    let mut out = Vec::new();
    let last = instructions.len() - 1;

    for (idx, instr) in instructions.iter().enumerate() {
        if remaining.true_count() == 0 {
            // Every later instruction's condition would AND against an all-false mask; nothing
            // left in this group can fire.
            break;
        }

        // Spark's `RewriteMergeIntoTable` appends an unconditional catch-all
        // `Keep(TrueLiteral, ...)` as the last instruction of the matched / not-matched-by-source
        // groups. A literal condition evaluates to a `ColumnarValue::Scalar`, so handle it
        // without materializing (and then AND-ing against) a same-value n-row array.
        let fire = match instr.condition.evaluate(&group_batch)? {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => remaining.clone(),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false) | None)) => continue,
            value => {
                let cond = value
                    .into_array(group_batch.num_rows())?
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map(null_to_false)
                    .ok_or_else(|| {
                        DataFusionError::Internal("MergeRows: expected boolean array".to_string())
                    })?;
                and(&remaining, &cond)?
            }
        };

        if fire.true_count() > 0 {
            let filtered = filter_or_pass_through(&group_batch, &fire)?;
            for output_exprs in &instr.outputs {
                out.push(project(&filtered, output_exprs, schema)?);
            }
        }

        if idx != last {
            remaining = and_not(&remaining, &fire)?;
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

    let mut new_entries = 0usize;
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
        if !seen.insert(id) {
            return Err(DataFusionError::External(Box::new(
                SparkError::MergeCardinalityViolation,
            )));
        }
        new_entries += 1;
    }

    // `seen` grows for the lifetime of the partition and is unbounded in the number of matched
    // target rows, so it must be visible to the memory pool -- otherwise a large MERGE grows
    // native memory with nothing to push back on it. Accounted after the fact (rather than
    // reserving the batch's row count up front and releasing the remainder) since the overshoot
    // is bounded by one batch.
    reservation.try_grow(new_entries * SEEN_ENTRY_BYTES)?;
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
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
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
        // 3 rows: matched, not-matched (insert-only), not-matched-by-source.
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
        // row 1 (matched, kept) and row 2 (not-matched, inserted); row 3 discarded.
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
    fn null_condition_falls_through_to_next_instruction() {
        // Regression test for the NULL-propagation data-loss bug. Models the plan
        // `RewriteMergeIntoTable` actually builds for
        //   MERGE ... WHEN MATCHED AND s.val > 100 THEN UPDATE ...
        // namely a two-instruction matched group whose second entry is the appended catch-all
        // `Keep(TrueLiteral, target.output)`. With `val` NULL the first condition evaluates to
        // NULL, which Spark treats as `false` and falls through to the catch-all; before the
        // `null_to_false` normalization Arrow's NULL-propagating `and`/`not` poisoned the
        // `remaining` mask and the row vanished from the output entirely.
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
        // The presence flags feed the same mask arithmetic as clause conditions, so a nullable
        // `__row_from_source` / `__row_from_target` must also collapse to `false` rather than
        // NULL -- otherwise the row falls out of all three group masks and is dropped.
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
        // source NULL -> false, target true => not-matched-by-source group.
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
        // Spark reaches `applyInstructions` only after a row is routed to a group, so a NOT
        // MATCHED condition never sees a matched row. Row 1 is matched with `val = 0`; row 2 is
        // the only not-matched row. Evaluating the not-matched condition `10 / val > 1` over the
        // whole batch (the pre-fix behaviour) divides by row 1's zero and fails the query with an
        // error Spark would never raise.
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
        // Guard the test's own premise: evaluated over the whole batch this condition really
        // does fail, so a regression back to batch-wide evaluation cannot slip through silently.
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
        // row 1 kept by the matched group; row 2 kept by the not-matched group (10 / 5 > 1).
        assert_eq!(got, vec![0, 5]);
    }

    #[test]
    fn cardinality_state_is_accounted_to_the_memory_pool() {
        let mut reservation = test_reservation();
        let batch = test_batch(vec![1, 2], vec![10, 20], vec![true, true], vec![true, true]);
        let matched_mask = BooleanArray::from(vec![true, true]);
        check_cardinality(
            &batch,
            &matched_mask,
            0,
            &mut HashSet::new(),
            &mut reservation,
        )
        .unwrap();
        assert_eq!(
            reservation.size(),
            2 * SEEN_ENTRY_BYTES,
            "`seen` must be visible to the memory pool so an unbounded MERGE has something \
             pushing back on it"
        );
    }

    #[test]
    fn cardinality_violation_detected() {
        // Same target row_id (1) matched twice -> must error.
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
        // Asserting the ordered values (not just num_rows()) catches a regression that applies
        // outputs[0] twice instead of outputs[0] then outputs[1].
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn cardinality_violation_detected_for_null_row_ids() {
        // Two matched rows both carrying a NULL row id must trip the cardinality check, mirroring
        // Spark's `BitmapCardinalityValidator`, which reads `InternalRow.getLong(ordinal)`
        // unconditionally (no null check) -- a null long field reads as 0 on the Spark side, so
        // two nulls collide exactly like two real zeros would.
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
        assert!(
            result.is_err(),
            "two matched rows with a NULL row id must trip MERGE_CARDINALITY_VIOLATION, not be \
             silently skipped"
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }

    /// A batch whose rows are all discarded must not surface as a zero-row batch: the stream
    /// swallows it and pulls the next input instead, so downstream stages (the FFI hop into the
    /// write pipeline, and `RecordBatchPartitionSplitter` for a partitioned table) never pay for
    /// a batch with nothing in it.
    #[tokio::test]
    async fn all_discarded_batch_is_not_emitted() {
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::prelude::SessionContext;

        // Batch 1: matched-only rows, all discarded. Batch 2: one row that survives.
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
        assert_eq!(
            batches.len(),
            1,
            "the all-discarded batch must be swallowed, not forwarded as a zero-row batch"
        );
        assert_eq!(batches[0].num_rows(), 1);
    }

    /// A `row_id_ordinal` that does not address a real child column must be rejected at plan
    /// construction. Reaching `check_cardinality` with it would panic on an out-of-bounds column
    /// access instead of failing the query with a message.
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
            // `test_schema()` has 4 columns, so 99 cannot be a row-id column.
            Some(99),
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("row id ordinal"),
            "expected an out-of-range ordinal error, got: {err}"
        );
    }

    /// `with_new_children` must re-run the same bounds check `try_new` does, not just copy the
    /// old ordinal onto a new child. Constructs a valid `MergeRowsExec` against a 4-column child,
    /// then swaps in a 1-column child the ordinal cannot possibly fit.
    #[test]
    fn with_new_children_rejects_ordinal_out_of_range_for_new_child() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let exec = Arc::new(
            MergeRowsExec::try_new(
                col("source_present", &test_schema()).unwrap(),
                col("target_present", &test_schema()).unwrap(),
                vec![keep_all()],
                vec![],
                vec![],
                Some(3), // valid: test_schema() has 4 columns.
                source,
                out_schema(),
            )
            .unwrap(),
        );

        let narrow_schema = Arc::new(Schema::new(vec![Field::new(
            "only_col",
            DataType::Int64,
            true,
        )]));
        let narrow_child =
            MemorySourceConfig::try_new_exec(&[vec![]], narrow_schema, None).unwrap();

        let err = exec.with_new_children(vec![narrow_child]).unwrap_err();
        assert!(
            err.to_string().contains("row id ordinal"),
            "expected an out-of-range ordinal error, got: {err}"
        );
    }

    #[test]
    fn cardinality_violation_detected_across_batches() {
        // Regression test for the per-batch `seen` reset bug: `process_batch` must accept the
        // caller's `seen` set and mutate it in place so state accumulates across the whole
        // partition. Two separate batches each carry one of two source rows matching the same
        // target row_id=1 -- exactly the case a fresh-per-batch `HashSet` would miss.
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
        assert!(
            first.is_ok(),
            "first batch introduces row_id=1 and should not trip the cardinality check"
        );

        let second = process_batch(
            batch2,
            &config,
            &mut seen,
            &mut test_reservation(),
            &out_schema(),
        );
        assert!(
            second.is_err(),
            "second batch reuses row_id=1 from a *different* Arrow batch and must trip \
             MERGE_CARDINALITY_VIOLATION"
        );
        assert!(second
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }
}
