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

//! A fork of DataFusion's `UnnestExec`, kept for Comet's specialized unnesting kernels.
//!
//! # Why this fork exists
//!
//! It was created because `UnnestExec` emitted exactly one output batch per input batch,
//! however many rows the unnesting produced, and never consulted
//! `datafusion.execution.batch_size`. That fix is now upstream
//! (apache/datafusion#24384, in DataFusion 55.1.0), so it is no longer the reason to keep
//! the fork. What is left are performance improvements that have not been upstreamed:
//!
//! * `list_output_lens`, which computes per-row output lengths in one pass for `List` columns
//!   sharing offsets and validity, instead of chaining arrow kernels;
//! * the contiguous-run fast path in `unnest_list_array`, which returns a slice of the child
//!   values instead of gathering them, and the buffer fills in `create_take_indices`.
//! * splitting individual List rows before repeating carried values, including a repetition
//!   bound that prevents copied variable-width buffers and child offsets from overflowing, and
//!   trimming list children so retained slices do not inflate Arrow take's preallocation.
//! * skipping gather indices and placeholders when every column is being unnested.
//!
//! # Deleting this file
//!
//! Upstream these improvements, then delete this module and go back to
//! `datafusion::physical_plan::unnest::UnnestExec` in the planner. Deleting it before that
//! would regress `explode`, so measure with `native/operators/benches/explode.rs` first.
//!
//! # What was forked
//!
//! The unnesting kernels below (`build_batch` and everything it calls) started as a copy of
//! `datafusion/physical-plan/src/unnest.rs` at DataFusion 54.1.0, upstream revision
//! `cc7565be1ee97ba8fa2f5d6da373c5e38d81bb13`. They are private to
//! `datafusion-physical-plan`, so they cannot be called from here without copying them.
//!
//! They have since been specialized for the shapes Comet actually plans, so this is no longer a
//! copy that can be diffed against upstream line by line. The deliberate divergences are:
//!
//! * the `eq` and `lt` import path noted below, since Comet does not depend on `arrow_ord`
//!   directly;
//! * dropping upstream's `ListUnnest` declaration in favor of importing the public one;
//! * `find_longest_length` applies the empty-list bump once after the row-wise maximum rather
//!   than once per array, which is equivalent because `max` is associative;
//! * the performance improvements described above.
//!
//! `ExplodeExec` and `ExplodeStream` were always Comet's own.

use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanBufferBuilder, FixedSizeListArray,
    GenericListArray, Int64Array, LargeListArray, LargeListViewArray, ListArray, ListViewArray,
    MapArray, OffsetSizeTrait, PrimitiveArray, Scalar, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::kernels::length::length;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{cast, is_not_null, kernels, sum};
use arrow::datatypes::{DataType, Int64Type, SchemaRef};
use arrow::record_batch::RecordBatch;
// Upstream imports these as `arrow_ord::cmp::{eq, lt}`; Comet reaches them through `arrow`,
// which does not have `arrow_ord` as a direct dependency.
use arrow::compute::kernels::cmp::{eq, lt};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{
    exec_datafusion_err, exec_err, internal_err, Constraints, HashMap, HashSet, Result,
    UnnestOptions,
};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::equivalence::ProjectionMapping;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, RecordOutput,
    SplitMetrics,
};
use datafusion::physical_plan::stream::BatchSplitStream;
// `ListUnnest` is the one item the copied region below does NOT need to duplicate: unlike the
// kernels, upstream exports it publicly.
use datafusion::physical_plan::unnest::ListUnnest;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use std::cmp::{self, Ordering};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// Comet's explode operator: DataFusion's `UnnestExec` with the input consumed in chunks so
/// that output batches respect `datafusion.execution.batch_size`.
#[derive(Debug)]
pub struct ExplodeExec {
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    list_column_indices: Vec<ListUnnest>,
    struct_column_indices: Vec<usize>,
    options: UnnestOptions,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl ExplodeExec {
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        list_column_indices: Vec<ListUnnest>,
        struct_column_indices: Vec<usize>,
        schema: SchemaRef,
        options: UnnestOptions,
    ) -> Result<Self> {
        let cache = Self::compute_properties(
            &child,
            &list_column_indices,
            &struct_column_indices,
            &schema,
        )?;

        Ok(Self {
            child,
            schema,
            list_column_indices,
            struct_column_indices,
            options,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        })
    }

    /// Compute the plan properties, keeping whatever the child guarantees about the columns
    /// that unnesting passes through untouched.
    ///
    /// Copied from `UnnestExec::compute_properties`. Unnesting only rewrites the list and
    /// struct columns, so the child's orderings and equivalences on the remaining columns
    /// still hold and are projected across rather than discarded: a downstream aggregate over
    /// a passthrough key that arrives sorted can still stream instead of buffering. Only the
    /// constraints go, since unnesting duplicates rows and so invalidates any uniqueness or
    /// primary-key guarantee.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        list_column_indices: &[ListUnnest],
        struct_column_indices: &[usize],
        schema: &SchemaRef,
    ) -> Result<PlanProperties> {
        let input_schema = input.schema();
        let mut unnested_indices = BooleanBufferBuilder::new(input_schema.fields().len());
        unnested_indices.append_n(input_schema.fields().len(), false);
        for list_unnest in list_column_indices {
            unnested_indices.set_bit(list_unnest.index_in_input_schema, true);
        }
        for struct_unnest in struct_column_indices {
            unnested_indices.set_bit(*struct_unnest, true)
        }
        let unnested_indices = unnested_indices.finish();
        let non_unnested_indices: Vec<usize> = (0..input_schema.fields().len())
            .filter(|idx| !unnested_indices.value(*idx))
            .collect();

        // Map each non-unnested input column to wherever it landed in the output schema.
        let projection_mapping: ProjectionMapping = non_unnested_indices
            .iter()
            .map(|&input_idx| {
                let input_field = input_schema.field(input_idx);
                let output_idx = schema
                    .fields()
                    .iter()
                    .position(|output_field| output_field.name() == input_field.name())
                    .ok_or_else(|| {
                        exec_datafusion_err!(
                            "Non-unnested column '{}' must exist in output schema",
                            input_field.name()
                        )
                    })?;

                let input_col =
                    Arc::new(Column::new(input_field.name(), input_idx)) as Arc<dyn PhysicalExpr>;
                let target_col =
                    Arc::new(Column::new(input_field.name(), output_idx)) as Arc<dyn PhysicalExpr>;
                let targets = vec![(target_col, output_idx)].into();
                Ok((input_col, targets))
            })
            .collect::<Result<ProjectionMapping>>()?;

        let eq_properties = input
            .equivalence_properties()
            .project(&projection_mapping, Arc::clone(schema))
            .with_constraints(Constraints::default());

        let output_partitioning = input
            .output_partitioning()
            .project(&projection_mapping, &eq_properties);

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        ))
    }
}

impl DisplayAs for ExplodeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometExplodeExec")
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for ExplodeExec {
    fn name(&self) -> &str {
        "CometExplodeExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // ExplodeExec holds no physical expressions of its own; the columns to unnest are
        // identified by index, so there is nothing to visit here.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("ExplodeExec expects exactly one child");
        }
        Ok(Arc::new(ExplodeExec::new(
            Arc::clone(&children[0]),
            self.list_column_indices.clone(),
            self.struct_column_indices.clone(),
            Arc::clone(&self.schema),
            self.options.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        let input = self.child.execute(partition, context)?;

        let stream = Box::pin(ExplodeStream {
            input,
            schema: Arc::clone(&self.schema),
            list_type_columns: self.list_column_indices.clone(),
            struct_column_indices: self.struct_column_indices.iter().copied().collect(),
            options: self.options.clone(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            input_batches: MetricBuilder::new(&self.metrics).counter("input_batches", partition),
            input_rows: MetricBuilder::new(&self.metrics).counter("input_rows", partition),
            batch_size,
            pending_input: None,
        });

        // Depth-1 List inputs are split before materialization, including within one row.
        // Keep the output splitter for the generic shapes described in `predict_output_lens`.
        Ok(Box::pin(BatchSplitStream::new(
            stream,
            batch_size,
            SplitMetrics::new(&self.metrics, partition),
        )))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// An input batch being unnested incrementally, a chunk of rows at a time.
struct PendingInput {
    /// The full input batch. Rows before `row_offset` have already been unnested.
    batch: RecordBatch,
    /// Index of the next input row to unnest.
    row_offset: usize,
    /// Elements already emitted from the current input row.
    element_offset: usize,
    /// Repetition safe for every slice of this input. None for unsupported list shapes.
    safe_repetitions: Option<usize>,
    /// How many output rows each input row expands into, indexed by input row.
    ///
    /// `None` when the expansion factor cannot be predicted from the input alone, in which
    /// case the whole remaining input is unnested in one call and only the output is split.
    /// See [`ExplodeStream::predict_output_lens`].
    output_lens: Option<PrimitiveArray<Int64Type>>,
}

impl PendingInput {
    fn remaining_rows(&self) -> usize {
        self.batch.num_rows() - self.row_offset
    }

    /// How many input rows to unnest next so the resulting batch holds at most `batch_size`
    /// rows.
    ///
    /// Oversized List rows are handled separately by `next_chunk`.
    fn next_chunk_rows(&self, batch_size: usize) -> usize {
        let Some(output_lens) = &self.output_lens else {
            return self.remaining_rows();
        };

        let lens = &output_lens.values()[self.row_offset..];
        let batch_size = batch_size as i64;
        let mut output_rows = 0i64;
        for (rows, len) in lens.iter().enumerate() {
            if rows > 0 && output_rows + len > batch_size {
                return rows;
            }
            output_rows += len;
        }
        lens.len()
    }

    /// The per-row output lengths covering the next `rows` input rows, so the unnesting does
    /// not have to recompute what [`ExplodeStream::predict_output_lens`] already derived.
    fn chunk_lengths(&self, rows: usize) -> Option<PrimitiveArray<Int64Type>> {
        self.output_lens
            .as_ref()
            .map(|lens| lens.slice(self.row_offset, rows))
    }

    fn next_chunk(
        &mut self,
        batch_size: usize,
        list_columns: &[ListUnnest],
    ) -> Result<(RecordBatch, Option<Int64Array>)> {
        let mut rows = self.next_chunk_rows(batch_size);
        let mut chunk = self.batch.slice(self.row_offset, rows);
        let mut lengths = self.chunk_lengths(rows);
        if let Some(safe_repetitions) = self.safe_repetitions {
            // The input-wide bound only skips checks. A candidate's maximum repetition times
            // its span is sufficient, but skewed byte columns can use their actual repeats.
            let limit = if safe_repetitions >= batch_size {
                batch_size
            } else {
                loop {
                    let limit = batch_size.min(repetition_limit(&chunk, list_columns));
                    let repeats = lengths.as_ref().unwrap();
                    let max_repeat = *repeats.values().iter().max().unwrap() as usize;
                    if rows == 1 || max_repeat <= limit {
                        break limit;
                    }
                    if chunk
                        .columns()
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| {
                            !list_columns
                                .iter()
                                .any(|list| list.index_in_input_schema == *index)
                        })
                        .all(|(_, column)| {
                            repeated_offsets_fit(column.as_ref(), repeats, max_repeat)
                        })
                    {
                        break batch_size;
                    }
                    rows = rows.div_ceil(2);
                    chunk = self.batch.slice(self.row_offset, rows);
                    lengths = self.chunk_lengths(rows);
                }
            };
            let row_length = self.output_lens.as_ref().unwrap().value(self.row_offset) as usize;
            if row_length > limit {
                let length = limit.min(row_length - self.element_offset);
                let mut columns = chunk.columns().to_vec();
                for unnest in list_columns {
                    let list = columns[unnest.index_in_input_schema].as_list::<i32>();
                    let DataType::List(field) = list.data_type() else {
                        unreachable!();
                    };
                    let available = if list.is_null(0) {
                        0
                    } else {
                        list.value_length(0) as usize
                    };
                    let skip = self.element_offset.min(available);
                    let start = list.value_offsets()[0] + skip as i32;
                    let end = start + length.min(available - skip) as i32;
                    // Narrow the entries and parallel positions together, preserving their
                    // original values and sharing their buffers. Shorter lists still pad.
                    columns[unnest.index_in_input_schema] = Arc::new(ListArray::new(
                        Arc::clone(field),
                        OffsetBuffer::new(vec![start, end].into()),
                        Arc::clone(list.values()),
                        list.nulls().cloned(),
                    ));
                }
                self.element_offset += length;
                if self.element_offset == row_length {
                    self.row_offset += 1;
                    self.element_offset = 0;
                }
                return Ok((
                    RecordBatch::try_new(chunk.schema(), columns)?,
                    Some(Int64Array::from(vec![length as i64])),
                ));
            }
        }
        self.row_offset += rows;
        Ok((chunk, lengths))
    }
}

/// The largest visible span addressed by 32-bit offsets. Fixed-width buffers do not constrain
/// these offsets; a sliced list still needs its selected child count even when that child has
/// no buffers, as with List<Null>.
fn max_offset_span(array: &dyn Array) -> usize {
    match array.data_type() {
        DataType::Utf8 => {
            let offsets = array.as_string::<i32>().value_offsets();
            (offsets[array.len()] - offsets[0]) as usize
        }
        DataType::Binary => {
            let offsets = array.as_binary::<i32>().value_offsets();
            (offsets[array.len()] - offsets[0]) as usize
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let start = offsets[0] as usize;
            let length = offsets[list.len()] as usize - start;
            length.max(max_offset_span(list.values().slice(start, length).as_ref()))
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            let start = offsets[0] as usize;
            let length = offsets[map.len()] as usize - start;
            length.max(max_offset_span(&map.entries().slice(start, length)))
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .map(|column| max_offset_span(column.as_ref()))
            .max()
            .unwrap_or(0),
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            let start = offsets[0] as usize;
            let length = offsets[list.len()] as usize - start;
            max_offset_span(list.values().slice(start, length).as_ref())
        }
        DataType::FixedSizeList(_, _) => {
            max_offset_span(array.as_fixed_size_list().values().as_ref())
        }
        _ => 0,
    }
}

/// A conservative bound safe for every subset of these rows and repetitions up to the limit.
fn repetition_limit(batch: &RecordBatch, list_columns: &[ListUnnest]) -> usize {
    let span = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            !list_columns
                .iter()
                .any(|list| list.index_in_input_schema == *index)
        })
        .map(|(_, column)| max_offset_span(column.as_ref()))
        .max()
        .unwrap_or(0);
    (i32::MAX as usize / span.max(1)).max(1)
}

/// Tighten the byte budget for skewed chunks while retaining collection allocation bounds.
fn repeated_offsets_fit(array: &dyn Array, repeats: &Int64Array, max_repeat: usize) -> bool {
    fn bytes_fit(array: &dyn Array, offsets: &[i32], repeats: &Int64Array) -> bool {
        offsets
            .windows(2)
            .zip(repeats.values())
            .enumerate()
            .try_fold(i32::MAX as usize, |remaining, (row, (w, &repeat))| {
                if array.is_null(row) {
                    Some(remaining)
                } else {
                    let bytes = (w[1] - w[0]) as usize;
                    remaining.checked_sub(bytes.saturating_mul(repeat as usize))
                }
            })
            .is_some()
    }

    if max_repeat <= i32::MAX as usize / max_offset_span(array).max(1) {
        return true;
    }
    match array.data_type() {
        DataType::Utf8 => bytes_fit(array, array.as_string::<i32>().value_offsets(), repeats),
        DataType::Binary => bytes_fit(array, array.as_binary::<i32>().value_offsets(), repeats),
        // Arrow Struct take copies children even when the parent struct is NULL.
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .all(|column| repeated_offsets_fit(column.as_ref(), repeats, max_repeat)),
        // List/map take also estimates child capacity from the unweighted input average.
        // Keep their conservative bound even when the logical weighted output would fit.
        _ => false,
    }
}

/// A stream that unnests its input, bounding output batches to `batch_size` rows.
struct ExplodeStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    list_type_columns: Vec<ListUnnest>,
    struct_column_indices: HashSet<usize>,
    options: UnnestOptions,
    baseline_metrics: BaselineMetrics,
    input_batches: Count,
    input_rows: Count,
    /// Target number of rows per output batch, from `datafusion.execution.batch_size`.
    batch_size: usize,
    /// Rows of the current input batch that have not been unnested yet. Unnesting one input
    /// batch can produce arbitrarily many output rows, so the input is consumed in chunks
    /// small enough that each chunk's output stays near `batch_size`.
    ///
    /// Comet's depth-1 List rows are also split within a row before carried values repeat.
    /// Generic recursive unnesting still materializes its expansion before output splitting.
    pending_input: Option<PendingInput>,
}

impl RecordBatchStream for ExplodeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for ExplodeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl ExplodeStream {
    fn poll_next_impl(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            // Unnest the next chunk of the input batch already in hand.
            if let Some(pending) = self.pending_input.as_mut() {
                // Pending input is non-empty; each chunk advances a row or its element cursor.
                debug_assert!(pending.remaining_rows() > 0);

                let (chunk, chunk_lengths) =
                    pending.next_chunk(self.batch_size, &self.list_type_columns)?;
                let drained = pending.remaining_rows() == 0;

                let timer = self.baseline_metrics.elapsed_compute().timer();
                let result = build_batch(
                    &chunk,
                    &self.schema,
                    &self.list_type_columns,
                    &self.struct_column_indices,
                    &self.options,
                    chunk_lengths.as_ref(),
                );
                timer.done();

                // Release the source batch and its predicted lengths before handing the last
                // chunk downstream, rather than holding them for the whole of its processing.
                if drained {
                    self.pending_input = None;
                }

                // A chunk can legitimately produce no rows at all (for example rows whose
                // arrays are all empty under `NullHandling::Drop`, which is plain `explode`);
                // `build_batch` signals that with `None` rather than an empty batch, so move on
                // to the next chunk. Under the outer handling every row yields at least one row,
                // so this cannot happen there.
                if let Some(batch) = result? {
                    debug_assert!(batch.num_rows() > 0);
                    (&batch).record_output(&self.baseline_metrics);
                    return Poll::Ready(Some(Ok(batch)));
                }
                continue;
            }

            // Otherwise pull the next input batch.
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.input_batches.add(1);
                    self.input_rows.add(batch.num_rows());
                    if batch.num_rows() > 0 {
                        let timer = self.baseline_metrics.elapsed_compute().timer();
                        let output_lens = self.predict_output_lens(&batch);
                        let safe_repetitions = (!self.list_type_columns.is_empty()
                            && self.list_type_columns.iter().all(|unnest| {
                                unnest.depth == 1
                                    && matches!(
                                        batch.column(unnest.index_in_input_schema).data_type(),
                                        DataType::List(_)
                                    )
                            }))
                        .then(|| repetition_limit(&batch, &self.list_type_columns));
                        timer.done();
                        self.pending_input = Some(PendingInput {
                            batch,
                            row_offset: 0,
                            element_offset: 0,
                            safe_repetitions,
                            output_lens: output_lens?,
                        });
                    }
                }
                other => {
                    // In the non-error case, i.e. the input is simply depleted, release the
                    // child pipeline's resources now instead of at drop: a downstream writer
                    // or shuffle can still be holding this stream open long after the last
                    // batch, and whatever the child reserved (a hash join's build side, for
                    // instance) is memory that writer may need.
                    if other.is_none() {
                        let input_schema = self.input.schema();
                        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
                    }
                    return Poll::Ready(other);
                }
            }
        }
    }

    /// Compute how many output rows each input row of `batch` will expand into, so the input
    /// can be chunked to keep each build bounded.
    ///
    /// Returns `None` when the count cannot be derived from the input alone, which is the
    /// signal to unnest the whole batch in one call:
    ///
    /// * With no list columns, unnesting only widens structs and leaves the row count alone,
    ///   so the output is already bounded by the input batch size.
    /// * With recursion (`depth > 1`), a row's expansion depends on the lengths of inner
    ///   lists that only exist after the outer levels have been unnested, so it cannot be
    ///   predicted up front. Comet only plans depth-1 explode today, but the fallback keeps
    ///   this correct if that changes.
    fn predict_output_lens(
        &self,
        batch: &RecordBatch,
    ) -> Result<Option<PrimitiveArray<Int64Type>>> {
        if self.list_type_columns.is_empty()
            || self
                .list_type_columns
                .iter()
                .any(|unnest| unnest.depth != 1)
        {
            return Ok(None);
        }

        let list_arrays: Vec<ArrayRef> = self
            .list_type_columns
            .iter()
            .map(|unnest| Arc::clone(batch.column(unnest.index_in_input_schema)))
            .collect();

        // This is exactly the per-row length that `list_unnest_at_level` derives when it
        // actually unnests, so the chunk boundaries are exact rather than estimated, and each
        // chunk's slice of it is handed back to `build_batch` instead of recomputed there.
        if let Some(list) = list_arrays[0].as_any().downcast_ref::<ListArray>() {
            // Shared offset and validity views guarantee identical per-row lengths.
            // Check validity's bit offset too; bitmap bytes alone do not suffice.
            if list_arrays.iter().skip(1).all(|other| {
                let Some(other) = other.as_any().downcast_ref::<ListArray>() else {
                    return false;
                };
                list.offsets().ptr_eq(other.offsets())
                    && match (list.nulls(), other.nulls()) {
                        (None, None) => true,
                        (Some(a), Some(b)) => a.inner().ptr_eq(b.inner()),
                        _ => false,
                    }
            }) {
                return Ok(Some(list_output_lens(list, &self.options)));
            }
        }
        let longest_length = find_longest_length(&list_arrays, &self.options)?;
        Ok(Some(longest_length.as_primitive::<Int64Type>().clone()))
    }
}

/// The per-row unnested length of a single `List` column: the row's list length, with NULL and
/// empty rows substituted according to [`datafusion::common::NullHandling`].
///
/// What [`find_longest_length`] computes when handed one array, in one pass over the offsets
/// rather than the kernels it chains to stay generic over list types — `length` (which returns
/// `Int32` for `List`), `cast` to widen it, `is_not_null` and `zip` to substitute the NULL
/// length, and a second `eq`/`zip` pair to bump the empty rows. Comet only ever plans `List`,
/// and parallel lists sharing offsets and validity have the same per-row length, so one
/// pass also covers `posexplode` when its position and value lists share those views.
fn list_output_lens(list: &ListArray, options: &UnnestOptions) -> PrimitiveArray<Int64Type> {
    // The floor every row's length is raised to. Under `PreserveAndExpandEmpty` an empty row
    // expands to one NULL just like a NULL row, including in an array with no validity buffer,
    // which can still hold empty rows.
    let min_length = if options.expand_empty_as_null() { 1 } else { 0 };
    // `find_longest_length` applies that floor after substituting the NULL length, so it reaches
    // substituted NULL rows too. Fold it in rather than leaning on `expand_empty_as_null`
    // implying `preserve_nulls`, so the two agree for any future `NullHandling`.
    let null_length = cmp::max(if options.preserve_nulls() { 1 } else { 0 }, min_length);
    let row_length = |w: &[i32]| cmp::max((w[1] - w[0]) as i64, min_length);
    let offsets = list.offsets();
    // Like `find_longest_length`, the result is non-null throughout: a NULL row reports
    // `null_length` rather than a NULL length, which `create_take_indices` relies on.
    let lens: Vec<i64> = match list.nulls() {
        None => offsets.windows(2).map(row_length).collect(),
        Some(nulls) => offsets
            .windows(2)
            .enumerate()
            .map(|(row, w)| {
                if nulls.is_valid(row) {
                    row_length(w)
                } else {
                    null_length
                }
            })
            .collect(),
    };
    PrimitiveArray::<Int64Type>::from(lens)
}

// ---------------------------------------------------------------------------------------
// Everything below started as a copy of DataFusion 54.1.0 `physical-plan/src/unnest.rs`
// (revision cc7565be1ee97ba8fa2f5d6da373c5e38d81bb13), since specialized for Comet. See the
// module docs for what diverges and why.
// ---------------------------------------------------------------------------------------

/// Given a set of struct column indices to flatten
/// try converting the column in input into multiple subfield columns
/// For example
/// struct_col: [a: struct(item: int, name: string), b: int]
/// with a batch
/// {a: {item: 1, name: "a"}, b: 2},
/// {a: {item: 3, name: "b"}, b: 4]
/// will be converted into
/// {a.item: 1, a.name: "a", b: 2},
/// {a.item: 3, a.name: "b", b: 4}
fn flatten_struct_cols(
    input_batch: &[Arc<dyn Array>],
    schema: &SchemaRef,
    struct_column_indices: &HashSet<usize>,
) -> Result<RecordBatch> {
    // horizontal expansion because of struct unnest
    let mut columns_expanded = Vec::with_capacity(schema.fields().len());
    for (idx, column_data) in input_batch.iter().enumerate() {
        match struct_column_indices.get(&idx) {
            Some(_) => match column_data.data_type() {
                DataType::Struct(_) => {
                    let struct_arr = column_data.as_any().downcast_ref::<StructArray>().unwrap();
                    columns_expanded.extend_from_slice(struct_arr.columns());
                }
                data_type => {
                    return internal_err!(
                        "expecting column {idx} from input plan to be a struct, got {data_type}"
                    )
                }
            },
            None => columns_expanded.push(Arc::clone(column_data)),
        }
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns_expanded)?)
}

/// This function is used to execute the unnesting on multiple columns all at once, but
/// one level at a time, and is called n times, where n is the highest recursion level among
/// the unnest exprs in the query.
///
/// For example giving the following query:
/// ```sql
/// select unnest(colA, max_depth:=3) as P1, unnest(colA,max_depth:=2) as P2, unnest(colB, max_depth:=1) as P3 from temp;
/// ```
/// Then the total times this function being called is 3
///
/// It needs to be aware of which level the current unnesting is, because if there exists
/// multiple unnesting on the same column, but with different recursion levels, say
/// **unnest(colA, max_depth:=3)** and **unnest(colA, max_depth:=2)**, then the unnesting
/// of expr **unnest(colA, max_depth:=3)** will start at level 3, while unnesting for expr
/// **unnest(colA, max_depth:=2)** has to start at level 2
///
/// Set *colA* as a 3-dimension columns and *colB* as an array (1-dimension). As stated,
/// this function is called with the descending order of recursion depth
///
/// Depth = 3
/// - colA(3-dimension) unnest into temp column temp_P1(2_dimension) (unnesting of P1 starts
///   from this level)
/// - colA(3-dimension) having indices repeated by the unnesting operation above
/// - colB(1-dimension) having indices repeated by the unnesting operation above
///
/// Depth = 2
/// - temp_P1(2-dimension) unnest into temp column temp_P1(1-dimension)
/// - colA(3-dimension) unnest into temp column temp_P2(2-dimension) (unnesting of P2 starts
///   from this level)
/// - colB(1-dimension) having indices repeated by the unnesting operation above
///
/// Depth = 1
/// - temp_P1(1-dimension) unnest into P1
/// - temp_P2(2-dimension) unnest into P2
/// - colB(1-dimension) unnest into P3 (unnesting of P3 starts from this level)
///
/// The returned array will has the same size as the input batch
/// and only contains original columns that are not being unnested.
fn list_unnest_at_level(
    batch: &[ArrayRef],
    list_type_unnests: &[ListUnnest],
    temp_unnested_arrs: &mut HashMap<ListUnnest, ArrayRef>,
    level_to_unnest: usize,
    options: &UnnestOptions,
    precomputed_lengths: Option<&PrimitiveArray<Int64Type>>,
) -> Result<Option<Vec<ArrayRef>>> {
    // Extract unnestable columns at this level
    let (arrs_to_unnest, list_unnest_specs): (Vec<Arc<dyn Array>>, Vec<_>) = list_type_unnests
        .iter()
        .filter_map(|unnesting| {
            if level_to_unnest == unnesting.depth {
                return Some((
                    Arc::clone(&batch[unnesting.index_in_input_schema]),
                    *unnesting,
                ));
            }
            // This means the unnesting on this item has started at higher level
            // and need to continue until depth reaches 1
            if level_to_unnest < unnesting.depth {
                return Some((
                    Arc::clone(temp_unnested_arrs.get(unnesting).unwrap()),
                    *unnesting,
                ));
            }
            None
        })
        .unzip();

    // Filter out so that list_arrays only contain column with the highest depth
    // at the same time, during iteration remove this depth so next time we don't have to unnest them again
    //
    // The caller may already have computed these lengths to decide how many input rows to feed
    // us; reusing them avoids running the kernel chain twice over the same rows. Cloning is an
    // `Arc` bump on the underlying buffer, not a copy.
    let longest_length = match precomputed_lengths {
        Some(lengths) => lengths.clone(),
        None => find_longest_length(&arrs_to_unnest, options)?
            .as_primitive::<Int64Type>()
            .clone(),
    };
    let unnested_length = &longest_length;
    let total_length = if unnested_length.is_empty() {
        0
    } else {
        sum(unnested_length)
            .ok_or_else(|| exec_datafusion_err!("Failed to calculate the total unnested length"))?
            as usize
    };
    if total_length == 0 {
        return Ok(None);
    }

    // Unnest all the list arrays
    let unnested_temp_arrays =
        unnest_list_arrays(arrs_to_unnest.as_ref(), unnested_length, total_length)?;

    unnested_temp_arrays
        .into_iter()
        .zip(list_unnest_specs.iter())
        .for_each(|(flatten_arr, unnesting)| {
            temp_unnested_arrs.insert(*unnesting, flatten_arr);
        });

    let repeat_mask: Vec<bool> = batch
        .iter()
        .enumerate()
        .map(|(i, _)| {
            // Check if the column is needed in future levels (levels below the current one)
            let needed_in_future_levels = list_type_unnests.iter().any(|unnesting| {
                unnesting.index_in_input_schema == i && unnesting.depth < level_to_unnest
            });

            // Check if the column is involved in unnesting at any level
            let is_involved_in_unnesting = list_type_unnests
                .iter()
                .any(|unnesting| unnesting.index_in_input_schema == i);

            // Repeat columns needed in future levels or not unnested.
            needed_in_future_levels || !is_involved_in_unnesting
        })
        .collect();

    // Dimension of arrays in batch is untouched, but the values are repeated
    // as the side effect of unnesting
    let ret = if repeat_mask.iter().any(|&repeat| repeat) {
        let take_indices = create_take_indices(unnested_length, total_length);
        repeat_arrs_from_indices(batch, &take_indices, &repeat_mask)?
    } else {
        // Every column is replaced by an unnested result before the output is built.
        batch.to_vec()
    };

    Ok(Some(ret))
}
struct UnnestingResult {
    arr: ArrayRef,
    depth: usize,
}

/// For each row in a `RecordBatch`, some list/struct columns need to be unnested.
/// - For list columns: We will expand the values in each list into multiple rows,
///   taking the longest length among these lists, and shorter lists are padded with NULLs.
/// - For struct columns: We will expand the struct columns into multiple subfield columns.
///
/// For columns that don't need to be unnested, repeat their values until reaching the longest length.
///
/// Note: unnest has a big difference in behavior between Postgres and DuckDB
///
/// Take this example
///
/// 1. Postgres
/// ```ignored
/// create table temp (
///     i integer[][][], j integer[]
/// )
/// insert into temp values ('{{{1,2},{3,4}},{{5,6},{7,8}}}', '{1,2}');
/// select unnest(i), unnest(j) from temp;
/// ```
///
/// Result
/// ```text
///     1   1
///     2   2
///     3
///     4
///     5
///     6
///     7
///     8
/// ```
/// 2. DuckDB
/// ```ignore
///     create table temp (i integer[][][], j integer[]);
///     insert into temp values ([[[1,2],[3,4]],[[5,6],[7,8]]], [1,2]);
///     select unnest(i,recursive:=true), unnest(j,recursive:=true) from temp;
/// ```
/// Result:
/// ```text
///
///     ┌────────────────────────────────────────────────┬────────────────────────────────────────────────┐
///     │ unnest(i, "recursive" := CAST('t' AS BOOLEAN)) │ unnest(j, "recursive" := CAST('t' AS BOOLEAN)) │
///     │                     int32                      │                     int32                      │
///     ├────────────────────────────────────────────────┼────────────────────────────────────────────────┤
///     │                                              1 │                                              1 │
///     │                                              2 │                                              2 │
///     │                                              3 │                                              1 │
///     │                                              4 │                                              2 │
///     │                                              5 │                                              1 │
///     │                                              6 │                                              2 │
///     │                                              7 │                                              1 │
///     │                                              8 │                                              2 │
///     └────────────────────────────────────────────────┴────────────────────────────────────────────────┘
/// ```
///
/// The following implementation refer to DuckDB's implementation
fn build_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    list_type_columns: &[ListUnnest],
    struct_column_indices: &HashSet<usize>,
    options: &UnnestOptions,
    precomputed_lengths: Option<&PrimitiveArray<Int64Type>>,
) -> Result<Option<RecordBatch>> {
    let transformed = match list_type_columns.len() {
        0 => flatten_struct_cols(batch.columns(), schema, struct_column_indices),
        _ => {
            let mut temp_unnested_result = HashMap::with_capacity(list_type_columns.len());
            let max_recursion = list_type_columns
                .iter()
                .fold(0, |highest_depth, ListUnnest { depth, .. }| {
                    cmp::max(highest_depth, *depth)
                });

            // This arr always has the same column count with the input batch
            let mut flatten_arrs = vec![];

            // Original batch has the same columns
            // All unnesting results are written to temp_batch
            for depth in (1..=max_recursion).rev() {
                let input = match depth == max_recursion {
                    true => batch.columns(),
                    false => &flatten_arrs,
                };
                // Only sound for a single non-recursive level: with recursion the deeper
                // levels' lengths depend on arrays that do not exist yet, which is also why
                // the caller does not predict lengths in that case.
                let level_lengths = if max_recursion == 1 {
                    precomputed_lengths
                } else {
                    None
                };
                let Some(temp_result) = list_unnest_at_level(
                    input,
                    list_type_columns,
                    &mut temp_unnested_result,
                    depth,
                    options,
                    level_lengths,
                )?
                else {
                    return Ok(None);
                };
                flatten_arrs = temp_result;
            }
            let unnested_array_map: HashMap<usize, Vec<UnnestingResult>> =
                temp_unnested_result.into_iter().fold(
                    HashMap::with_capacity(list_type_columns.len()),
                    |mut acc,
                     (
                        ListUnnest {
                            index_in_input_schema,
                            depth,
                        },
                        flattened_array,
                    )| {
                        acc.entry(index_in_input_schema)
                            .or_default()
                            .push(UnnestingResult {
                                arr: flattened_array,
                                depth,
                            });
                        acc
                    },
                );
            let output_order: HashMap<ListUnnest, usize> = list_type_columns
                .iter()
                .enumerate()
                .map(|(order, unnest_def)| (*unnest_def, order))
                .collect();

            // One original column may be unnested multiple times into separate columns
            let mut multi_unnested_per_original_index = unnested_array_map
                .into_iter()
                .map(
                    // Each item in unnested_columns is the result of unnesting the same input column
                    // we need to sort them to conform with the original expression order
                    // e.g unnest(unnest(col)) must goes before unnest(col)
                    |(original_index, mut unnested_columns)| {
                        unnested_columns.sort_by(
                            |UnnestingResult { depth: depth1, .. },
                             UnnestingResult { depth: depth2, .. }|
                             -> Ordering {
                                output_order
                                    .get(&ListUnnest {
                                        depth: *depth1,
                                        index_in_input_schema: original_index,
                                    })
                                    .unwrap()
                                    .cmp(
                                        output_order
                                            .get(&ListUnnest {
                                                depth: *depth2,
                                                index_in_input_schema: original_index,
                                            })
                                            .unwrap(),
                                    )
                            },
                        );
                        (
                            original_index,
                            unnested_columns
                                .into_iter()
                                .map(|result| result.arr)
                                .collect::<Vec<_>>(),
                        )
                    },
                )
                .collect::<HashMap<_, _>>();

            let mut ret = Vec::with_capacity(flatten_arrs.len() + list_type_columns.len());
            for (col_idx, arr) in flatten_arrs.into_iter().enumerate() {
                // Convert original column into its unnested version(s)
                // Plural because one column can be unnested with different recursion level
                // and into separate output columns
                match multi_unnested_per_original_index.remove(&col_idx) {
                    Some(unnested_arrays) => ret.extend(unnested_arrays),
                    None => ret.push(arr),
                }
            }

            flatten_struct_cols(&ret, schema, struct_column_indices)
        }
    }?;
    Ok(Some(transformed))
}

/// Find the longest list length among the given list arrays for each row.
///
/// The per-row length of one array is its list length, with NULL and empty rows substituted
/// according to [`datafusion::common::NullHandling`]. For a single array:
///
/// ```ignore
/// l1:                        [1, 2, 3], null, [], [3]
///
/// Drop:                              3,    0,  0,   1
/// Preserve:                          3,    1,  0,   1
/// PreserveAndExpandEmpty:            3,    1,  1,   1
/// ```
///
/// The substitution happens per array, before the row-wise maximum, so a row that is empty in
/// one array still takes the longer length from another:
///
/// ```ignore
/// l2:                          [4, 5],   [], null, [6, 7]
///
/// PreserveAndExpandEmpty:           2,    1,    1,      2
/// longest_length(l1, l2):           3,    1,    1,      2
/// ```
fn find_longest_length(list_arrays: &[ArrayRef], options: &UnnestOptions) -> Result<ArrayRef> {
    // The length of a NULL list
    let null_length = if options.preserve_nulls() {
        Scalar::new(Int64Array::from_value(1, 1))
    } else {
        Scalar::new(Int64Array::from_value(0, 1))
    };
    let list_lengths: Vec<ArrayRef> = list_arrays
        .iter()
        .map(|list_array| {
            let mut length_array = length(list_array)?;
            // Make sure length arrays have the same type. Int64 is the most general one.
            length_array = cast(&length_array, &DataType::Int64)?;
            length_array = zip(&is_not_null(&length_array)?, &length_array, &null_length)?;
            Ok(length_array)
        })
        .collect::<Result<_>>()?;

    let longest_length = list_lengths.iter().skip(1).try_fold(
        Arc::clone(&list_lengths[0]),
        |longest, current| {
            let is_lt = lt(&longest, &current)?;
            zip(&is_lt, &current, &longest)
        },
    )?;

    if !options.expand_empty_as_null() {
        return Ok(longest_length);
    }
    // Bump empty lists to length 1 so they produce a single NULL-padded output row. Upstream
    // does this per array, inside the map above. `max` is associative and commutative, so
    // `max(max(a, 1), max(b, 1)) == max(max(a, b), 1)` and one pass here is equivalent to one
    // pass per array. The NULL substitution above has already set NULL rows to the preserved
    // length, which is at least 1 whenever this mode is set, so they are not matched.
    let zero = Scalar::new(Int64Array::from_value(0, 1));
    let one = Scalar::new(Int64Array::from_value(1, 1));
    Ok(zip(&eq(&longest_length, &zero)?, &one, &longest_length)?)
}

/// Trait defining common methods used for unnesting, implemented by list array types.
trait ListArrayType: Array {
    /// Returns a reference to the values of this list.
    fn values(&self) -> &ArrayRef;

    /// Returns the start and end offset of the values for the given row.
    fn value_offsets(&self, row: usize) -> (i64, i64);

    /// Whether consecutive rows occupy consecutive ranges of [`values`](Self::values), so that a
    /// run of rows is one slice of it.
    ///
    /// True for the offset-based list types, where row `i` is `[offsets[i], offsets[i + 1])` and
    /// so ends exactly where row `i + 1` begins. False for the view types, whose per-row offsets
    /// are independent and may overlap, repeat, or leave gaps.
    fn is_contiguous(&self) -> bool;
}

impl ListArrayType for ListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row].into(), offsets[row + 1].into())
    }

    fn is_contiguous(&self) -> bool {
        true
    }
}

impl ListArrayType for LargeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row], offsets[row + 1])
    }

    fn is_contiguous(&self) -> bool {
        true
    }
}

impl ListArrayType for FixedSizeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let start = self.value_offset(row) as i64;
        (start, start + self.value_length() as i64)
    }

    fn is_contiguous(&self) -> bool {
        true
    }
}

impl ListArrayType for ListViewArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offset = self.value_offsets()[row] as i64;
        let size = self.value_sizes()[row] as i64;
        (offset, offset + size)
    }

    fn is_contiguous(&self) -> bool {
        false
    }
}

impl ListArrayType for LargeListViewArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offset = self.value_offsets()[row];
        let size = self.value_sizes()[row];
        (offset, offset + size)
    }

    fn is_contiguous(&self) -> bool {
        false
    }
}

/// Unnest multiple list arrays according to the length array.
fn unnest_list_arrays(
    list_arrays: &[ArrayRef],
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> Result<Vec<ArrayRef>> {
    let typed_arrays = list_arrays
        .iter()
        .map(|list_array| match list_array.data_type() {
            DataType::List(_) => Ok(list_array.as_list::<i32>() as &dyn ListArrayType),
            DataType::LargeList(_) => Ok(list_array.as_list::<i64>() as &dyn ListArrayType),
            DataType::FixedSizeList(_, _) => {
                Ok(list_array.as_fixed_size_list() as &dyn ListArrayType)
            }
            DataType::ListView(_) => Ok(list_array.as_list_view::<i32>() as &dyn ListArrayType),
            DataType::LargeListView(_) => {
                Ok(list_array.as_list_view::<i64>() as &dyn ListArrayType)
            }
            other => exec_err!("Invalid unnest datatype {other }"),
        })
        .collect::<Result<Vec<_>>>()?;

    typed_arrays
        .iter()
        .map(|list_array| unnest_list_array(*list_array, length_array, capacity))
        .collect::<Result<_>>()
}

/// Whether unnesting `list_array` against `length_array` would gather exactly the contiguous run
/// `values[offsets.first()..offsets.last()]`, in which case [`unnest_list_array`] can slice the
/// child instead of building an index array and gathering through it.
///
/// The run is the right answer only if the loop in [`unnest_list_array`] would emit each row's
/// values in order with nothing added and nothing skipped, which needs three things:
///
/// * The rows are laid out consecutively in the child, so a run of them is one slice. The view
///   types are excluded here rather than checked, since their offsets are unordered.
/// * No row is padded. `target >= value` holds per row, so it is enough that the totals agree:
///   `capacity` is the sum of the targets and the offset span is the sum of the values.
/// * No NULL row holds elements. Arrow permits a NULL list slot to span a non-empty range, and
///   the loop skips those elements while the slice would include them. Builders and the Parquet
///   reader emit an empty range, so this scan almost always confirms rather than rejects, and it
///   only runs when the array has nulls at all.
///
/// The last two conditions are independent: a NULL row spanning elements can cancel out padding
/// elsewhere and leave the totals matching a run that is not the one to take.
fn is_contiguous_unnest(list_array: &dyn ListArrayType, capacity: usize) -> bool {
    let len = list_array.len();
    if len == 0 || !list_array.is_contiguous() {
        return false;
    }

    let (first, _) = list_array.value_offsets(0);
    let (_, last) = list_array.value_offsets(len - 1);
    if last - first != capacity as i64 {
        return false;
    }

    if list_array.null_count() > 0 {
        let has_populated_null = (0..len).any(|row| {
            if !list_array.is_null(row) {
                return false;
            }
            let (start, end) = list_array.value_offsets(row);
            end > start
        });
        if has_populated_null {
            return false;
        }
    }

    true
}

/// Unnest a list array according the target length array.
///
/// Consider a list array like this:
///
/// ```ignore
/// [1], [2, 3, 4], null, [5], [],
/// ```
///
/// and the length array is:
///
/// ```ignore
/// [2, 3, 2, 1, 2]
/// ```
///
/// If the length of a certain list is less than the target length, pad with NULLs.
/// So the unnested array will look like this:
///
/// ```ignore
/// [1, null, 2, 3, 4, null, null, 5, null, null]
/// ```
fn unnest_list_array(
    list_array: &dyn ListArrayType,
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> Result<ArrayRef> {
    let values = list_array.values();

    // Unnesting a single list column pads nothing, so the elements come out in the order they
    // are already stored and the gather below would read straight through them. Hand back a
    // slice of the child instead: no index buffer, no copy of the element data, which for a
    // string or nested element type is the bulk of the operator's work. Comet reaches this for
    // plain `explode`, and for both columns of `posexplode`, whose position array is built with
    // the same per-row lengths. `explode_outer` falls through as soon as a row is NULL or empty,
    // because those rows are padded.
    //
    // The result aliases the child rather than owning a compacted copy, and `ListArray::slice`
    // leaves `values` whole, so this is the child of the whole input batch and not of the chunk.
    // Every chunk of one input batch therefore pins the same buffer, which between them they
    // fill; a downstream operator that keeps only some of those chunks pins all of it. That is
    // bounded by one input batch's expansion, which `pending_input` already holds materialized,
    // and slicing the input is what `BatchSplitStream` above does too.
    if is_contiguous_unnest(list_array, capacity) {
        let (first, _) = list_array.value_offsets(0);
        return Ok(values.slice(first as usize, capacity));
    }

    let mut take_indices_builder = PrimitiveArray::<Int64Type>::builder(capacity);
    for row in 0..list_array.len() {
        let mut value_length = 0;
        if !list_array.is_null(row) {
            let (start, end) = list_array.value_offsets(row);
            value_length = end - start;
            for i in start..end {
                take_indices_builder.append_value(i)
            }
        }
        let target_length = length_array.value(row);
        debug_assert!(
            value_length <= target_length,
            "value length is beyond the longest length"
        );
        // Pad with NULL values
        take_indices_builder.append_nulls((target_length - value_length) as usize);
    }
    Ok(kernels::take::take(
        &values,
        &take_indices_builder.finish(),
        None,
    )?)
}

/// Creates take indices that will be used to expand all columns except for the list type
/// [`columns`](UnnestExec::list_column_indices) that is being unnested.
/// Every column value needs to be repeated multiple times according to the length array.
///
/// If the length array looks like this:
///
/// ```ignore
/// [2, 3, 1]
/// ```
/// Then [`create_take_indices`] will return an array like this
///
/// ```ignore
/// [0, 0, 1, 1, 1, 2]
/// ```
fn create_take_indices(
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> PrimitiveArray<Int64Type> {
    // `find_longest_length()` guarantees this.
    debug_assert!(
        length_array.null_count() == 0,
        "length array should not contain nulls"
    );
    // A run of one index at a time, so fill the buffer directly rather than appending element by
    // element through a builder: there is no validity to track, and each row becomes one fill of
    // `repeat` slots rather than `repeat` calls.
    let mut indices: Vec<i64> = Vec::with_capacity(capacity);
    for (index, repeat) in length_array.values().iter().enumerate() {
        indices.resize(indices.len() + *repeat as usize, index as i64);
    }
    PrimitiveArray::<Int64Type>::from(indices)
}

/// Arrow take estimates list child capacity from its full length, even for a sliced parent.
/// Restrict that estimate to the referenced children before repeating carried columns.
/// See https://github.com/apache/arrow-rs/pull/11360.
fn trim_list_children(array: &ArrayRef) -> Result<Option<ArrayRef>> {
    fn trim_list<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> Result<Option<ArrayRef>> {
        let start = list.value_offsets()[0].as_usize();
        let end = list.value_offsets()[list.len()].as_usize();
        if start == 0 && end == list.values().len() {
            return Ok(None);
        }
        let (field, offsets, values, nulls) = list.clone().into_parts();
        let base = offsets[0];
        Ok(Some(Arc::new(GenericListArray::<O>::try_new(
            field,
            offsets.subtract(base),
            values.slice(start, end - start),
            nulls,
        )?)))
    }

    let trimmed: ArrayRef = match array.data_type() {
        DataType::List(_) => return trim_list(array.as_list::<i32>()),
        DataType::LargeList(_) => return trim_list(array.as_list::<i64>()),
        DataType::Map(field, ordered) => {
            let map = array.as_map();
            let start = map.value_offsets()[0] as usize;
            let end = map.value_offsets()[map.len()] as usize;
            if start == 0 && end == map.entries().len() {
                return Ok(None);
            }
            Arc::new(MapArray::try_new(
                Arc::clone(field),
                map.offsets().clone().subtract(start as i32),
                map.entries().slice(start, end - start),
                map.nulls().cloned(),
                *ordered,
            )?)
        }
        DataType::Struct(fields) => {
            let array = array.as_struct();
            let mut columns = None;
            for (index, column) in array.columns().iter().enumerate() {
                if let Some(trimmed) = trim_list_children(column)? {
                    columns.get_or_insert_with(|| array.columns().to_vec())[index] = trimmed;
                }
            }
            let Some(columns) = columns else {
                return Ok(None);
            };
            Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                array.nulls().cloned(),
            )?)
        }
        DataType::FixedSizeList(field, size) => {
            let array = array.as_fixed_size_list();
            let Some(values) = trim_list_children(array.values())? else {
                return Ok(None);
            };
            Arc::new(FixedSizeListArray::try_new_with_length(
                Arc::clone(field),
                *size,
                values,
                array.nulls().cloned(),
                array.len(),
            )?)
        }
        _ => return Ok(None),
    };
    Ok(Some(trimmed))
}

/// Create a batch of arrays based on an input `batch` and a `indices` array.
/// The `indices` array is used by the take kernel to repeat values in the arrays
/// that are marked with `true` in the `repeat_mask`. Arrays marked with `false`
/// in the `repeat_mask` will be replaced with arrays filled with nulls of the
/// appropriate length.
///
/// For example if we have the following batch:
///
/// ```ignore
/// c1: [1], null, [2, 3, 4], null, [5, 6]
/// c2: 'a', 'b',  'c', null, 'd'
/// ```
///
/// then the `unnested_list_arrays` contains the unnest column that will replace `c1` in
/// the final batch under [`datafusion::common::NullHandling::Preserve`]:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6
/// ```
///
/// And the `indices` array contains the indices that are used by `take` kernel to
/// repeat the values in `c2`:
///
/// ```ignore
/// 0, 1, 2, 2, 2, 3, 4, 4
/// ```
///
/// so that the final batch will look like:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6
/// c2: 'a', 'b', 'c', 'c', 'c', null, 'd', 'd'
/// ```
///
/// The `repeat_mask` determines whether an array's values are repeated or replaced with nulls.
/// For example, if the `repeat_mask` is:
///
/// ```ignore
/// [true, false]
/// ```
///
/// The final batch will look like:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6  // Repeated using `indices`
/// c2: null, null, null, null, null, null, null, null  // Replaced with nulls
fn repeat_arrs_from_indices(
    batch: &[ArrayRef],
    indices: &PrimitiveArray<Int64Type>,
    repeat_mask: &[bool],
) -> Result<Vec<Arc<dyn Array>>> {
    batch
        .iter()
        .zip(repeat_mask.iter())
        .map(|(arr, &repeat)| {
            if repeat {
                let trimmed = trim_list_children(arr)?;
                Ok(kernels::take::take(
                    trimmed.as_ref().unwrap_or(arr),
                    indices,
                    None,
                )?)
            } else {
                Ok(new_null_array(arr.data_type(), arr.len()))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use datafusion::common::NullHandling;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::prelude::SessionConfig;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    /// Build a single-column `List<Int32>` batch where row `i` holds `lens[i]` elements,
    /// numbered consecutively across the whole batch. `None` is a NULL list.
    fn list_batch(lens: &[Option<usize>]) -> RecordBatch {
        let mut next = 0i32;
        let rows: Vec<Option<Vec<Option<i32>>>> = lens
            .iter()
            .map(|len| {
                len.map(|len| {
                    (0..len)
                        .map(|_| {
                            next += 1;
                            Some(next - 1)
                        })
                        .collect()
                })
            })
            .collect();
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(rows);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            list.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap()
    }

    /// Explode column "l" of `input` with the given `batch_size`, returning output batches.
    async fn explode(
        input: Vec<RecordBatch>,
        batch_size: usize,
        null_handling: NullHandling,
    ) -> Result<Vec<RecordBatch>> {
        let input_schema = input[0].schema();
        let source = MemorySourceConfig::try_new_exec(&[input], input_schema, None)?;
        explode_child(source, batch_size, null_handling).await
    }

    /// As [`explode`], but over an arbitrary child plan.
    async fn explode_child(
        child: Arc<dyn ExecutionPlan>,
        batch_size: usize,
        null_handling: NullHandling,
    ) -> Result<Vec<RecordBatch>> {
        let mut fields: Vec<Field> = child
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields[0] = Field::new("l", DataType::Int32, true);
        let output_schema = Arc::new(Schema::new(fields));
        let explode = ExplodeExec::new(
            child,
            vec![ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            }],
            vec![],
            output_schema,
            UnnestOptions::new().with_null_handling(null_handling),
        )?;
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(batch_size)),
        );
        datafusion::physical_plan::common::collect(explode.execute(0, task_ctx)?).await
    }

    fn values(batches: &[RecordBatch]) -> Vec<Option<i32>> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<Int32Type>()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn sizes(batches: &[RecordBatch]) -> Vec<usize> {
        batches.iter().map(|b| b.num_rows()).collect()
    }

    fn seq(n: i32) -> Vec<Option<i32>> {
        (0..n).map(Some).collect()
    }

    #[tokio::test]
    async fn respects_batch_size() {
        // 10 rows x 3 elements = 30 output rows from ONE input batch, which upstream
        // `UnnestExec` returns as a single 30-row batch.
        //
        // The array length is deliberately smaller than batch_size so that chunks pack
        // *several* input rows (2 rows -> 6 rows out; a third would overshoot 8). Using
        // arrays longer than batch_size would send every row down the oversized-build path
        // instead, which `single_row_exceeding_batch_size_is_sliced` already covers.
        let batches = explode(vec![list_batch(&[Some(3); 10])], 8, NullHandling::Preserve)
            .await
            .unwrap();
        assert_eq!(sizes(&batches), vec![6, 6, 6, 6, 6]);
        assert_eq!(values(&batches), seq(30));
    }

    #[tokio::test]
    async fn chunks_input_rather_than_slicing_output() {
        // Pins *how* the limit is met, which is what bounds peak memory. 3 rows of 3
        // elements at batch_size=4 gives [3, 3, 3] when the input is chunked per row;
        // building all 9 first and slicing would give [4, 4, 1].
        let batches = explode(
            vec![list_batch(&[Some(3), Some(3), Some(3)])],
            4,
            NullHandling::Preserve,
        )
        .await
        .unwrap();
        assert_eq!(
            sizes(&batches),
            vec![3, 3, 3],
            "input should be chunked per row, not built whole and sliced into [4, 4, 1]"
        );
        assert_eq!(values(&batches), seq(9));
    }

    #[tokio::test]
    async fn single_row_exceeding_batch_size_is_sliced() {
        let batches = explode(vec![list_batch(&[Some(25)])], 10, NullHandling::Preserve)
            .await
            .unwrap();
        assert_eq!(sizes(&batches), vec![10, 10, 5]);
        assert_eq!(values(&batches), seq(25));
    }

    #[tokio::test]
    async fn ordinary_carried_strings_keep_multi_row_chunks() {
        let lists = list_batch(&vec![Some(300); 8192]);
        let payload = "x".repeat(1024);
        let strings = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            payload.as_str(),
            8192,
        ))) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            lists.schema().field(0).clone(),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::clone(lists.column(0)), strings],
        )
        .unwrap();
        let source = MemorySourceConfig::try_new_exec(&[vec![input]], schema, None).unwrap();
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Int32, true),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let exec = ExplodeExec::new(
            source,
            vec![ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            }],
            vec![],
            output_schema,
            UnnestOptions::new().with_null_handling(NullHandling::Drop),
        )
        .unwrap();
        let context = Arc::new(
            TaskContext::default().with_session_config(SessionConfig::new().with_batch_size(8192)),
        );
        let mut stream = exec.execute(0, context).unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 27 * 300);
        assert_eq!(
            first.column(1).as_string::<i32>().value_data().len(),
            8100 * 1024
        );
    }

    #[tokio::test]
    async fn skewed_carried_bytes_use_each_rows_repetition() {
        use arrow::array::BinaryArray;

        let wide = "w".repeat(16 * 1024 * 1024);
        for wide_row in [0, 16, 32] {
            let mut lens = vec![Some(300); 33];
            lens[wide_row] = Some(1);
            let lists = list_batch(&lens);
            let strings = StringArray::from_iter_values((0..33).map(|row| {
                if row == wide_row {
                    wide.as_str()
                } else {
                    "x"
                }
            }));
            let binary = Arc::new(BinaryArray::new(
                strings.offsets().clone(),
                strings.values().clone(),
                None,
            )) as ArrayRef;
            let strings = Arc::new(strings) as ArrayRef;
            let structure = Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("s", DataType::Utf8, false)),
                    Arc::clone(&strings),
                ),
                (
                    Arc::new(Field::new("b", DataType::Binary, false)),
                    Arc::clone(&binary),
                ),
            ])) as ArrayRef;
            let indices = Int64Array::from_iter_values(
                lens.iter()
                    .enumerate()
                    .flat_map(|(row, len)| std::iter::repeat_n(row as i64, len.unwrap())),
            );
            for carried in [strings, binary, structure] {
                let input = RecordBatch::try_from_iter(vec![
                    ("l", Arc::clone(lists.column(0))),
                    ("payload", Arc::clone(&carried)),
                ])
                .unwrap();
                let batches = explode(vec![input], 8192, NullHandling::Drop)
                    .await
                    .unwrap();
                assert_eq!(batches.len(), 2, "wide row at {wide_row}");
                let schema = batches[0].schema();
                let expected = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::clone(lists.column(0).as_list::<i32>().values()),
                        kernels::take::take(carried.as_ref(), &indices, None).unwrap(),
                    ],
                )
                .unwrap();
                assert_eq!(expected.num_rows(), 9601);
                assert_eq!(
                    arrow::compute::concat_batches(&schema, &batches).unwrap(),
                    expected
                );
            }
        }
    }

    #[tokio::test]
    async fn sliced_carried_bytes_ignore_unused_backing() {
        let padding = "p".repeat(64 * 1024 * 1024);
        let carried = StringArray::from(vec![padding.as_str(), "x"]).slice(1, 1);
        let lists = list_batch(&[Some(1024)]);
        let input = RecordBatch::try_from_iter(vec![
            ("l", Arc::clone(lists.column(0))),
            ("payload", Arc::new(carried) as ArrayRef),
        ])
        .unwrap();
        let batches = explode(vec![input], 8192, NullHandling::Drop)
            .await
            .unwrap();
        assert_eq!(sizes(&batches), vec![1024]);
        assert_eq!(values(&batches), seq(1024));
        assert!(batches[0]
            .column(1)
            .as_string::<i32>()
            .iter()
            .all(|s| s == Some("x")));
    }

    #[tokio::test]
    async fn null_carried_bytes_do_not_spend_the_offset_budget() {
        let wide = "w".repeat(2 * 1024 * 1024);
        let strings = StringArray::from(vec![wide.as_str(), "x"]);
        let carried = StringArray::new(
            strings.offsets().clone(),
            strings.values().clone(),
            Some(NullBuffer::from(vec![false, true])),
        );
        let lists = list_batch(&[Some(1024), Some(300)]);
        let input = RecordBatch::try_from_iter(vec![
            ("l", Arc::clone(lists.column(0))),
            ("payload", Arc::new(carried) as ArrayRef),
        ])
        .unwrap();
        let batches = explode(vec![input], 8192, NullHandling::Drop)
            .await
            .unwrap();
        assert_eq!(sizes(&batches), vec![1324]);
        assert_eq!(values(&batches), seq(1324));
        let carried = batches[0].column(1).as_string::<i32>();
        assert_eq!(carried.null_count(), 1024);
        assert_eq!(carried.value_data().len(), 300);
    }

    #[test]
    fn null_struct_children_still_constrain_copied_offsets() {
        let wide = "w".repeat(2 * 1024 * 1024);
        let strings = StringArray::from(vec![wide.as_str(), "x"]);
        let repeats = Int64Array::from(vec![1024, 300]);
        assert!(!repeated_offsets_fit(&strings, &repeats, 1024));
        let nullable_strings = StringArray::new(
            strings.offsets().clone(),
            strings.values().clone(),
            Some(NullBuffer::from(vec![false, true])),
        );
        assert!(repeated_offsets_fit(&nullable_strings, &repeats, 1024));
        let structure = StructArray::new(
            vec![Arc::new(Field::new("s", DataType::Utf8, false))].into(),
            vec![Arc::new(strings)],
            Some(NullBuffer::from(vec![false, true])),
        );
        assert!(!repeated_offsets_fit(&structure, &repeats, 1024));
    }

    #[test]
    fn repetition_limit_ignores_fixed_width_buffers() {
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        )])
        .unwrap();
        assert_eq!(repetition_limit(&batch, &[]), i32::MAX as usize);
    }

    #[tokio::test]
    async fn carried_nested_columns_do_not_preallocate_for_unselected_children() {
        use arrow::array::MapArray;

        let lists = list_batch(&vec![Some(2); 1024]);
        let list = lists.column(0).as_list::<i32>();
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::clone(list.values()),
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, true)),
                Arc::clone(list.values()),
            ),
        ]);
        let map = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            list.offsets().clone(),
            entries,
            None,
            false,
        )) as ArrayRef;
        let nested_field = Arc::new(Field::new("item", map.data_type().clone(), true));
        let carried: Vec<ArrayRef> = vec![
            Arc::clone(&map),
            Arc::clone(lists.column(0)),
            Arc::new(LargeListArray::new(
                Arc::new(Field::new("item", DataType::Int32, true)),
                OffsetBuffer::new(list.value_offsets().iter().map(|&o| i64::from(o)).collect()),
                Arc::clone(list.values()),
                None,
            )),
            Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("m", map.data_type().clone(), true)),
                Arc::clone(&map),
            )])),
            Arc::new(FixedSizeListArray::new(
                Arc::clone(&nested_field),
                1,
                Arc::clone(&map),
                None,
            )),
            Arc::new(ListArray::new(
                nested_field,
                OffsetBuffer::new((0..=1024).collect()),
                map,
                None,
            )),
        ];

        fn assert_small_buffers(array: &ArrayRef) {
            match array.data_type() {
                DataType::Map(_, _) => {
                    for column in array.as_map().entries().columns() {
                        assert_small_buffers(column);
                    }
                }
                DataType::List(_) => assert_small_buffers(array.as_list::<i32>().values()),
                DataType::LargeList(_) => assert_small_buffers(array.as_list::<i64>().values()),
                DataType::FixedSizeList(_, _) => {
                    assert_small_buffers(array.as_fixed_size_list().values());
                }
                DataType::Struct(_) => {
                    for column in array.as_struct().columns() {
                        assert_small_buffers(column);
                    }
                }
                _ => assert!(
                    array.get_buffer_memory_size() <= 1024,
                    "eight output rows must not allocate for the 1024-row source: {} bytes",
                    array.get_buffer_memory_size()
                ),
            }
        }

        for carried in carried {
            let input = RecordBatch::try_from_iter(vec![
                ("l", Arc::clone(lists.column(0))),
                ("carried", Arc::clone(&carried)),
            ])
            .unwrap();
            let schema = Arc::new(Schema::new(vec![
                Field::new("l", DataType::Int32, true),
                input.schema().field(1).clone(),
            ]));
            let input_schema = input.schema();
            let source =
                MemorySourceConfig::try_new_exec(&[vec![input]], input_schema, None).unwrap();
            let exec = ExplodeExec::new(
                source,
                vec![ListUnnest {
                    index_in_input_schema: 0,
                    depth: 1,
                }],
                vec![],
                Arc::clone(&schema),
                UnnestOptions::new(),
            )
            .unwrap();
            let context = Arc::new(
                TaskContext::default().with_session_config(SessionConfig::new().with_batch_size(8)),
            );
            let mut stream = exec.execute(0, context).unwrap();
            for start in [0, 8] {
                let actual = stream.next().await.unwrap().unwrap();
                let expected = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from_iter_values(start..start + 8)),
                        kernels::take::take(
                            &carried,
                            &Int64Array::from_iter_values(
                                (start / 2..start / 2 + 4).flat_map(|row| [i64::from(row); 2]),
                            ),
                            None,
                        )
                        .unwrap(),
                    ],
                )
                .unwrap();
                assert_eq!(actual, expected);
                assert_small_buffers(actual.column(1));
            }
        }
    }

    #[test]
    fn repetition_limit_uses_selected_nested_offsets() {
        use arrow::array::NullArray;

        let length = i32::MAX as usize / 2 + 1;
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            OffsetBuffer::new(vec![0, length as i32 - 1, length as i32].into()),
            Arc::new(NullArray::new(length)),
            None,
        );
        let batch =
            RecordBatch::try_from_iter(vec![("carried", Arc::new(list) as ArrayRef)]).unwrap();
        assert_eq!(repetition_limit(&batch.slice(1, 1), &[]), i32::MAX as usize);
    }

    #[test]
    fn repetition_limit_follows_nested_variable_width_slices() {
        use arrow::array::{BinaryArray, MapArray};

        let strings = Arc::new(StringArray::from(vec!["ignored prefix", "a", "bc"])) as ArrayRef;
        let field = Arc::new(Field::new("item", DataType::Utf8, false));
        let list = ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::new(vec![0, 1, 3].into()),
            Arc::clone(&strings),
            None,
        );
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Utf8, false)),
                Arc::clone(&strings),
            ),
        ]);
        let map = MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            list.offsets().clone(),
            entries,
            None,
            false,
        );
        let arrays: Vec<ArrayRef> = vec![
            strings.slice(1, 2),
            Arc::new(BinaryArray::from(vec![b"prefix".as_slice(), b"a", b"bc"]).slice(1, 2)),
            Arc::new(list.slice(1, 1)),
            Arc::new(map.slice(1, 1)),
            Arc::new(
                StructArray::from(vec![(Arc::clone(&field), Arc::clone(&strings))]).slice(1, 2),
            ),
            Arc::new(
                LargeListArray::new(
                    Arc::clone(&field),
                    OffsetBuffer::new(vec![0i64, 1, 3].into()),
                    Arc::clone(&strings),
                    None,
                )
                .slice(1, 1),
            ),
            Arc::new(FixedSizeListArray::new(field, 1, strings, None).slice(1, 2)),
        ];
        for array in arrays {
            let batch = RecordBatch::try_from_iter(vec![("carried", array)]).unwrap();
            assert_eq!(repetition_limit(&batch, &[]), i32::MAX as usize / 3);
        }
    }

    #[test]
    fn chunk_offset_bound_counts_all_repeated_rows() {
        use arrow::array::NullArray;

        let child_length = i32::MAX / 3;
        let carried = ListArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            OffsetBuffer::new(vec![0, child_length, 2 * child_length].into()),
            Arc::new(NullArray::new(2 * child_length as usize)),
            None,
        );
        let lists = list_batch(&[Some(2), Some(2)]);
        let batch = RecordBatch::try_from_iter(vec![
            ("l", Arc::clone(lists.column(0))),
            ("carried", Arc::new(carried) as ArrayRef),
        ])
        .unwrap();
        let list_columns = [ListUnnest {
            index_in_input_schema: 0,
            depth: 1,
        }];
        let mut pending = PendingInput {
            batch,
            row_offset: 0,
            element_offset: 0,
            safe_repetitions: Some(1),
            output_lens: Some(Int64Array::from(vec![2, 2])),
        };
        // Each row can repeat twice, but their combined child offsets would overflow.
        for _ in 0..2 {
            let (chunk, lengths) = pending.next_chunk(4, &list_columns).unwrap();
            assert_eq!(chunk.num_rows(), 1);
            assert_eq!(lengths.unwrap().values(), &[2]);
        }
        assert_eq!(pending.remaining_rows(), 0);
    }

    #[tokio::test]
    async fn splits_one_list_before_repeating_carried_values() {
        let list = list_batch(&[Some(25)]);
        let schema = Arc::new(Schema::new(vec![
            list.schema().field(0).clone(),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let payload = "x".repeat(1024);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::clone(list.column(0)),
                Arc::new(StringArray::from(vec![payload])),
            ],
        )
        .unwrap();
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Int32, true),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let exec = ExplodeExec::new(
            source,
            vec![ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            }],
            vec![],
            output_schema,
            UnnestOptions::new().with_null_handling(NullHandling::Drop),
        )
        .unwrap();
        let context = Arc::new(
            TaskContext::default().with_session_config(SessionConfig::new().with_batch_size(10)),
        );
        let mut stream = exec.execute(0, context).unwrap();
        let mut actual = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let strings = batch.column(1).as_string::<i32>();
            assert_eq!(
                strings.value_data().len(),
                batch.num_rows() * 1024,
                "carried values must be materialized only for this output chunk"
            );
            actual.extend(batch.column(0).as_primitive::<Int32Type>().iter());
        }
        assert_eq!(actual, seq(25));
    }

    #[tokio::test]
    async fn wide_carried_string_respects_offset_limit_at_large_batch_size() {
        let list = list_batch(&[Some(2049)]);
        let schema = Arc::new(Schema::new(vec![
            list.schema().field(0).clone(),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::clone(list.column(0)),
                Arc::new(StringArray::from(vec!["x".repeat(1024 * 1024)])),
            ],
        )
        .unwrap();
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Int32, true),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let exec = ExplodeExec::new(
            source,
            vec![ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            }],
            vec![],
            output_schema,
            UnnestOptions::new().with_null_handling(NullHandling::Drop),
        )
        .unwrap();
        let context = Arc::new(
            TaskContext::default().with_session_config(SessionConfig::new().with_batch_size(8192)),
        );
        let mut stream = exec.execute(0, context).unwrap();
        let mut next = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            assert!(batch.num_rows() < 2049);
            let strings = batch.column(1).as_string::<i32>();
            for (i, value) in batch
                .column(0)
                .as_primitive::<Int32Type>()
                .iter()
                .enumerate()
            {
                assert_eq!(value, Some(next));
                assert_eq!(strings.value(i).len(), 1024 * 1024);
                next += 1;
            }
        }
        assert_eq!(next, 2049);
    }

    #[tokio::test]
    async fn splitting_preserves_map_entries_positions_and_nested_carried_columns() {
        use arrow::array::{BinaryArray, BooleanArray};
        let lists = list_batch(&[Some(1), None, Some(11), Some(0), Some(3)]);
        let list = lists.column(0).as_list::<i32>();
        let fields = vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Boolean, true),
        ];
        let entries = StructArray::new(
            fields.clone().into(),
            vec![
                Arc::clone(list.values()),
                Arc::new(BooleanArray::from_iter(
                    (0..list.values().len()).map(|i| (!i.is_multiple_of(3)).then_some(i % 5 < 2)),
                )),
            ],
            None,
        );
        let entries = ListArray::new(
            Arc::new(Field::new("entry", entries.data_type().clone(), false)),
            list.offsets().clone(),
            Arc::new(entries),
            list.nulls().cloned(),
        );
        let positions = ListArray::new(
            Arc::new(Field::new("pos", DataType::Int32, true)),
            list.offsets().clone(),
            Arc::new(Int32Array::from_iter_values(
                list.offsets().windows(2).flat_map(|w| 0..w[1] - w[0]),
            )),
            list.nulls().cloned(),
        );
        let carried = StructArray::from(vec![
            (
                Arc::new(Field::new("s", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![
                    Some("skip"),
                    Some("null map"),
                    Some("populated"),
                    None,
                    Some("tail"),
                ])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Binary, true)),
                Arc::new(BinaryArray::from(vec![
                    Some(b"skip".as_slice()),
                    None,
                    Some(b"binary"),
                    Some(b"empty"),
                    Some(b"tail"),
                ])) as ArrayRef,
            ),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("pos", positions.data_type().clone(), true),
            Field::new("entries", entries.data_type().clone(), true),
            Field::new("carried", carried.data_type().clone(), true),
        ]));
        let input = RecordBatch::try_new(
            schema,
            vec![Arc::new(positions), Arc::new(entries), Arc::new(carried)],
        )
        .unwrap()
        .slice(1, 4);
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("pos", DataType::Int32, true),
            fields[0].clone().with_nullable(true),
            fields[1].clone(),
            input.schema().field(2).clone(),
        ]));
        let list_columns = vec![
            ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            },
            ListUnnest {
                index_in_input_schema: 1,
                depth: 1,
            },
        ];
        for handling in [NullHandling::Drop, NullHandling::PreserveAndExpandEmpty] {
            let options = UnnestOptions::new().with_null_handling(handling);
            let expected = build_batch(
                &input,
                &output_schema,
                &list_columns,
                &HashSet::from([1]),
                &options,
                None,
            )
            .unwrap()
            .unwrap();
            let source =
                MemorySourceConfig::try_new_exec(&[vec![input.clone()]], input.schema(), None)
                    .unwrap();
            let exec = ExplodeExec::new(
                source,
                list_columns.clone(),
                vec![1],
                Arc::clone(&output_schema),
                options,
            )
            .unwrap();
            let context = Arc::new(
                TaskContext::default().with_session_config(SessionConfig::new().with_batch_size(4)),
            );
            let output =
                datafusion::physical_plan::common::collect(exec.execute(0, context).unwrap())
                    .await
                    .unwrap();
            assert!(output.iter().all(|batch| batch.num_rows() <= 4));
            let actual = arrow::compute::concat_batches(&output_schema, &output).unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn repetition_limit_counts_zero_width_children() {
        use arrow::array::NullArray;
        let length = i32::MAX as usize / 2 + 1;
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            OffsetBuffer::new(vec![0, length as i32].into()),
            Arc::new(NullArray::new(length)),
            None,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "carried",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();
        assert_eq!(repetition_limit(&batch, &[]), 1);
    }

    /// Chunked output must match unchunked output exactly, and hold the invariants that
    /// apply regardless of null handling: bounded, non-empty, and totalling `expected_rows`.
    async fn assert_chunking_matches_whole(
        lens: &[Option<usize>],
        null_handling: NullHandling,
        expected_rows: usize,
    ) {
        let chunked = explode(vec![list_batch(lens)], 2, null_handling)
            .await
            .unwrap();
        let whole = explode(vec![list_batch(lens)], 1024, null_handling)
            .await
            .unwrap();

        let chunked_sizes = sizes(&chunked);
        assert!(
            chunked_sizes.iter().all(|s| *s <= 2 && *s > 0),
            "chunked batches must be bounded and non-empty, got {chunked_sizes:?}"
        );
        assert_eq!(chunked_sizes.iter().sum::<usize>(), expected_rows);
        assert_eq!(values(&chunked), values(&whole));
    }

    #[tokio::test]
    async fn chunking_preserves_outer_semantics() {
        // Spark's explode_outer: a NULL array and an empty one each yield one NULL row. The
        // per-row counts driving chunking must agree, or boundaries drift out of step with the
        // unnesting.
        assert_chunking_matches_whole(
            &[Some(3), None, Some(0), Some(2), None],
            NullHandling::PreserveAndExpandEmpty,
            8,
        )
        .await;
    }

    #[tokio::test]
    async fn chunking_preserves_non_outer_semantics() {
        // Plain explode: NULL and empty arrays both produce nothing. Chunks made up entirely of
        // such rows must not stall the stream or emit an empty batch.
        assert_chunking_matches_whole(
            &[None, Some(4), Some(0), None, Some(1)],
            NullHandling::Drop,
            5,
        )
        .await;
    }

    #[tokio::test]
    async fn multiple_input_batches() {
        // Chunk boundaries are per input batch, so a short tail chunk at each boundary is
        // expected; nothing may exceed batch_size and no rows may be lost.
        let input = vec![
            list_batch(&[Some(5), Some(5)]),
            list_batch(&[Some(1)]),
            list_batch(&[Some(7), Some(2)]),
        ];
        let batches = explode(input, 4, NullHandling::Preserve).await.unwrap();
        let sizes = sizes(&batches);
        assert!(
            sizes.iter().all(|s| *s <= 4),
            "every output batch must respect batch_size=4, got {sizes:?}"
        );
        assert_eq!(sizes.iter().sum::<usize>(), 20);
    }

    /// A child stream that flips a flag when it is dropped, so a test can observe *when* the
    /// operator lets go of it rather than only that it eventually does.
    struct DropTrackingStream {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for DropTrackingStream {
        fn drop(&mut self) {
            self.dropped.store(true, AtomicOrdering::SeqCst);
        }
    }

    impl Stream for DropTrackingStream {
        type Item = Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.batches.pop_front().map(Ok))
        }
    }

    impl RecordBatchStream for DropTrackingStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[tokio::test]
    async fn releases_exhausted_child_at_eof() {
        // A downstream writer or shuffle can hold this stream open long after the last batch,
        // so the child's resources — a hash join build side, a memory reservation — must be
        // released on the poll that reports EOF, not deferred until this stream is dropped.
        let dropped = Arc::new(AtomicBool::new(false));
        let batch = list_batch(&[Some(2), Some(2)]);
        let input = Box::pin(DropTrackingStream {
            schema: batch.schema(),
            batches: VecDeque::from(vec![batch]),
            dropped: Arc::clone(&dropped),
        });

        let metrics = ExecutionPlanMetricsSet::new();
        let mut stream = ExplodeStream {
            input,
            schema: Arc::new(Schema::new(vec![Field::new("l", DataType::Int32, true)])),
            list_type_columns: vec![ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            }],
            struct_column_indices: HashSet::new(),
            options: UnnestOptions::new(),
            baseline_metrics: BaselineMetrics::new(&metrics, 0),
            input_batches: MetricBuilder::new(&metrics).counter("input_batches", 0),
            input_rows: MetricBuilder::new(&metrics).counter("input_rows", 0),
            batch_size: 4,
            pending_input: None,
        };

        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows();
            assert!(
                !dropped.load(AtomicOrdering::SeqCst),
                "child must stay live while it can still produce batches"
            );
        }
        assert_eq!(rows, 4);
        assert!(
            dropped.load(AtomicOrdering::SeqCst),
            "child must be released by the poll that returns EOF, not at stream drop"
        );
    }

    #[tokio::test]
    async fn preserves_passthrough_orderings() {
        // Unnesting rewrites only the list column, so an ordering the child guarantees on a
        // passthrough column still holds afterwards. Dropping it makes a downstream aggregate
        // on that key buffer or spill where it could have streamed, so pin that it survives.
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1)])]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("l", list.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1])), Arc::new(list)],
        )
        .unwrap();

        let key = Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>;
        let source = MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&schema), None)
            .unwrap()
            .try_with_sort_information(vec![LexOrdering::new(vec![PhysicalSortExpr::new_default(
                Arc::clone(&key),
            )])
            .unwrap()])
            .unwrap();
        let source = DataSourceExec::from_data_source(source);
        assert!(source.properties().output_ordering().is_some());

        // Output schema: the passthrough key, then the unnested element column.
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("l", DataType::Int32, true),
        ]));
        let explode = ExplodeExec::new(
            source,
            vec![ListUnnest {
                index_in_input_schema: 1,
                depth: 1,
            }],
            vec![],
            output_schema,
            UnnestOptions::new(),
        )
        .unwrap();

        let ordering = explode
            .properties()
            .output_ordering()
            .expect("ordering on the passthrough key must survive unnesting");
        assert_eq!(ordering.len(), 1);
        assert_eq!(ordering[0].expr.as_ref(), key.as_ref());
    }

    // ---------------------------------------------------------------------------------------
    // Contiguous-run fast path in `unnest_list_array`
    // ---------------------------------------------------------------------------------------

    fn int_list(offsets: Vec<i32>, values: Vec<i32>, nulls: Option<Vec<bool>>) -> ListArray {
        ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(Int32Array::from(values)),
            nulls.map(NullBuffer::from),
        )
    }

    /// Unnest `list` against `lens`, returning the elements and whether the fast path was taken.
    fn unnest(list: &ListArray, lens: Vec<i64>) -> (Vec<Option<i32>>, bool) {
        let capacity = lens.iter().sum::<i64>() as usize;
        let length_array = PrimitiveArray::<Int64Type>::from(lens);
        let took_fast_path = is_contiguous_unnest(list as &dyn ListArrayType, capacity);
        let out = unnest_list_array(list as &dyn ListArrayType, &length_array, capacity).unwrap();
        let out = out.as_primitive::<Int32Type>().iter().collect();
        (out, took_fast_path)
    }

    /// The pointer the array's first buffer starts at, to tell a shared child from a copy.
    fn buffer_ptr(array: &dyn Array) -> *const u8 {
        array.to_data().buffers()[0].as_ptr()
    }

    #[test]
    fn contiguous_unnest_returns_a_slice_of_the_child() {
        // Rows [1,2,3], [4], [5,6] unnest to the child verbatim, so the result must share the
        // child's buffer rather than gather into a fresh one. This is the plain-`explode` path.
        let list = int_list(vec![0, 3, 4, 6], vec![1, 2, 3, 4, 5, 6], None);
        let (values, fast) = unnest(&list, vec![3, 1, 2]);
        assert!(
            fast,
            "a single unpadded list column must take the fast path"
        );
        assert_eq!(values, (1..=6).map(Some).collect::<Vec<_>>());

        let length_array = PrimitiveArray::<Int64Type>::from(vec![3i64, 1, 2]);
        let out = unnest_list_array(&list as &dyn ListArrayType, &length_array, 6).unwrap();
        assert_eq!(
            buffer_ptr(out.as_ref()),
            buffer_ptr(ListArrayType::values(&list).as_ref()),
            "the fast path must alias the child, not copy it"
        );
    }

    #[test]
    fn contiguous_unnest_honors_a_non_zero_offset_base() {
        // Slicing leaves `offsets.first() > 0` while `values` stays whole, so a fast path that
        // sliced from 0 would silently return the wrong elements.
        let list = int_list(vec![0, 2, 3, 5, 6], vec![1, 2, 3, 4, 5, 6], None);
        let sliced = list.slice(1, 2);
        let (values, fast) = unnest(&sliced, vec![1, 2]);
        assert!(fast);
        assert_eq!(values, vec![Some(3), Some(4), Some(5)]);
    }

    #[test]
    fn contiguous_unnest_covers_empty_rows_and_dropped_nulls() {
        // Row 1 is NULL and row 2 is empty, and neither holds elements.
        let list = int_list(
            vec![0, 2, 2, 2, 5],
            vec![1, 2, 3, 4, 5],
            Some(vec![true, false, true, true]),
        );

        // Plain `explode` drops both, so nothing is padded and the run stays unbroken.
        let (values, fast) = unnest(&list, vec![2, 0, 0, 3]);
        assert!(fast, "rows contributing nothing must not break the run");
        assert_eq!(values, (1..=5).map(Some).collect::<Vec<_>>());

        // `explode_outer` pads each of them to one NULL, which no slice of the child can
        // produce, so the same array must now take the gather.
        let (values, fast) = unnest(&list, vec![2, 1, 1, 3]);
        assert!(!fast, "padded rows must break the run");
        assert_eq!(
            values,
            vec![Some(1), Some(2), None, None, Some(3), Some(4), Some(5)]
        );
    }

    #[test]
    fn padded_rows_fall_back_to_the_gather() {
        // `explode_outer`: the NULL row is padded to one element, so the output interleaves a
        // NULL that no slice of the child contains.
        let list = int_list(
            vec![0, 2, 2, 4],
            vec![1, 2, 3, 4],
            Some(vec![true, false, true]),
        );
        let (values, fast) = unnest(&list, vec![2, 1, 2]);
        assert!(!fast, "a padded row cannot be served by a slice");
        assert_eq!(values, vec![Some(1), Some(2), None, Some(3), Some(4)]);
    }

    #[test]
    fn populated_null_row_falls_back_even_when_the_totals_agree() {
        // The arithmetic check alone is not enough. Arrow allows a NULL slot to span elements;
        // here row 0 is NULL over two of them and row 1 is padded by two, so the offset span
        // (5) equals the capacity (5) while the correct output skips the NULL row's elements.
        // Slicing would return [1,2,3,4,5] instead.
        let list = int_list(vec![0, 2, 5], vec![1, 2, 3, 4, 5], Some(vec![false, true]));
        let (values, fast) = unnest(&list, vec![0, 5]);
        assert!(
            !fast,
            "a NULL row holding elements breaks the run even when the totals match"
        );
        assert_eq!(values, vec![Some(3), Some(4), Some(5), None, None]);
    }

    #[test]
    fn list_view_input_falls_back() {
        // View offsets are independent per row, so consecutive rows need not be adjacent and a
        // run cannot be assumed. Here they are deliberately out of order.
        let view = ListViewArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            ScalarBuffer::from(vec![3, 0]),
            ScalarBuffer::from(vec![2, 3]),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            None,
        );
        let capacity = 5;
        assert!(!is_contiguous_unnest(&view as &dyn ListArrayType, capacity));

        let length_array = PrimitiveArray::<Int64Type>::from(vec![2i64, 3]);
        let out = unnest_list_array(&view as &dyn ListArrayType, &length_array, capacity).unwrap();
        let values: Vec<Option<i32>> = out.as_primitive::<Int32Type>().iter().collect();
        assert_eq!(values, vec![Some(4), Some(5), Some(1), Some(2), Some(3)]);
    }

    // ---------------------------------------------------------------------------------------
    // Fused per-row length computation
    // ---------------------------------------------------------------------------------------

    /// `list_output_lens` must agree with `find_longest_length` element for element, since the
    /// chunking in `ExplodeStream` and the unnesting in `build_batch` both consume it. Both are
    /// also pinned against `expected`, given per [`NullHandling`] in the order below, so that a
    /// mistake made in both at once cannot pass.
    fn assert_output_lens(list: ListArray, expected_per_mode: [&[i64]; 3]) {
        let modes = [
            NullHandling::Drop,
            NullHandling::Preserve,
            NullHandling::PreserveAndExpandEmpty,
        ];
        for (null_handling, expected) in modes.into_iter().zip(expected_per_mode) {
            let options = UnnestOptions::new().with_null_handling(null_handling);
            let arrays = vec![Arc::new(list.clone()) as ArrayRef];
            let general = find_longest_length(&arrays, &options).unwrap();
            let general = general.as_primitive::<Int64Type>();
            let fused = list_output_lens(&list, &options);

            assert_eq!(
                &general.values()[..],
                expected,
                "general, {null_handling:?}"
            );
            assert_eq!(&fused.values()[..], expected, "fused, {null_handling:?}");
            // `create_take_indices` reads the values buffer directly and relies on this.
            assert_eq!(general.null_count(), 0, "lengths must never be NULL");
            assert_eq!(fused.null_count(), 0, "lengths must never be NULL");
        }
    }

    /// `find_longest_length` bumps empty rows once after the row-wise maximum rather than once
    /// per array. That is only observably different with more than one array, which is the
    /// `posexplode` shape, so pin the combined result directly.
    #[test]
    fn longest_length_combines_arrays_before_the_empty_bump() {
        // Row 0 is non-empty in both. Row 1 is empty in l1 but 2 long in l2, so the bump must
        // not inflate it past the other array. Row 2 is empty in both, the only row where
        // `PreserveAndExpandEmpty` differs from `Preserve`. Row 3 is empty in l1 and NULL in l2.
        let l1 = int_list(vec![0, 2, 2, 2, 2], vec![1, 2], None);
        let l2 = int_list(
            vec![0, 1, 3, 3, 3],
            vec![4, 5, 6],
            Some(vec![true, true, true, false]),
        );
        let arrays = vec![Arc::new(l1) as ArrayRef, Arc::new(l2) as ArrayRef];

        for (null_handling, expected) in [
            (NullHandling::Drop, [2, 2, 0, 0]),
            (NullHandling::Preserve, [2, 2, 0, 1]),
            (NullHandling::PreserveAndExpandEmpty, [2, 2, 1, 1]),
        ] {
            let options = UnnestOptions::new().with_null_handling(null_handling);
            let longest = find_longest_length(&arrays, &options).unwrap();
            let longest = longest.as_primitive::<Int64Type>();
            assert_eq!(&longest.values()[..], &expected, "{null_handling:?}");
            assert_eq!(longest.null_count(), 0, "lengths must never be NULL");
        }
    }

    #[tokio::test]
    async fn parallel_lists_match_upstream_with_shared_and_distinct_views() {
        use datafusion::physical_plan::{common::collect, unnest::UnnestExec};

        let offsets = OffsetBuffer::new(vec![0, 2, 2, 5, 10, 11].into());
        let values = Arc::new(Int32Array::from_iter_values(0..11)) as ArrayRef;
        let other_values = Arc::new(Int32Array::from_iter_values(100..111)) as ArrayRef;
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let validity = NullBuffer::from(vec![true, false, true, false, true]);
        let nulls = validity.slice(0, 4);
        // These views share the same bitmap bytes and null count, but different valid rows.
        let shifted_nulls = validity.slice(1, 4);
        let validity_pairs = [
            (None, None),
            (Some(nulls.clone()), Some(nulls.clone())),
            (Some(nulls.clone()), Some(shifted_nulls)),
            (
                Some(nulls.clone()),
                Some(NullBuffer::from(vec![true, false, true, false])),
            ),
            (Some(nulls.clone()), None),
            (None, Some(nulls)),
        ];

        for (left_nulls, right_nulls) in validity_pairs {
            for offset in [0, 1] {
                let left = ListArray::new(
                    Arc::clone(&field),
                    offsets.slice(0, 4),
                    Arc::clone(&values),
                    left_nulls.clone(),
                );
                let right = ListArray::new(
                    Arc::clone(&field),
                    offsets.slice(offset, 4),
                    Arc::clone(&other_values),
                    right_nulls.clone(),
                );
                let large_right = LargeListArray::new(
                    Arc::clone(&field),
                    OffsetBuffer::new(
                        right
                            .value_offsets()
                            .iter()
                            .map(|&o| i64::from(o))
                            .collect(),
                    ),
                    Arc::clone(&other_values),
                    right_nulls.clone(),
                );
                for right in [Arc::new(right) as ArrayRef, Arc::new(large_right)] {
                    let batch = RecordBatch::try_from_iter(vec![
                        ("left", Arc::new(left.clone()) as ArrayRef),
                        ("right", right),
                        ("id", Arc::new(Int32Array::from_iter_values(0..4))),
                    ])
                    .unwrap();
                    for batch in [batch.clone(), batch.slice(1, 3)] {
                        let source: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
                            &[vec![batch.clone()]],
                            batch.schema(),
                            None,
                        )
                        .unwrap();
                        let schema = Arc::new(Schema::new(vec![
                            Field::new("left", DataType::Int32, true),
                            Field::new("right", DataType::Int32, true),
                            batch.schema().field(2).clone(),
                        ]));
                        let specs: Vec<_> = (0..2)
                            .map(|index_in_input_schema| ListUnnest {
                                index_in_input_schema,
                                depth: 1,
                            })
                            .collect();
                        for mode in [
                            NullHandling::Drop,
                            NullHandling::Preserve,
                            NullHandling::PreserveAndExpandEmpty,
                        ] {
                            let options = UnnestOptions::new().with_null_handling(mode);
                            let comet = ExplodeExec::new(
                                Arc::clone(&source),
                                specs.clone(),
                                vec![],
                                Arc::clone(&schema),
                                options.clone(),
                            )
                            .unwrap();
                            let upstream = UnnestExec::new(
                                Arc::clone(&source),
                                specs.clone(),
                                vec![],
                                Arc::clone(&schema),
                                options,
                            )
                            .unwrap();
                            for size in [1, 3, 8] {
                                let context = Arc::new(TaskContext::default().with_session_config(
                                    SessionConfig::new().with_batch_size(size),
                                ));
                                let actual =
                                    collect(comet.execute(0, Arc::clone(&context)).unwrap())
                                        .await
                                        .unwrap();
                                let expected = collect(upstream.execute(0, context).unwrap())
                                    .await
                                    .unwrap();
                                assert!(actual.iter().all(|b| b.num_rows() <= size));
                                assert_eq!(
                                    arrow::compute::concat_batches(&schema, &actual).unwrap(),
                                    arrow::compute::concat_batches(&schema, &expected).unwrap(),
                                    "offset={offset}, {mode:?}, batch_size={size}, {batch:?}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn output_lens_substitute_per_null_handling() {
        // Row 2 is empty with no validity buffer at all, which is the case
        // `PreserveAndExpandEmpty` must still bump.
        assert_output_lens(
            int_list(vec![0, 3, 4, 4, 6], vec![1, 2, 3, 4, 5, 6], None),
            [&[3, 1, 0, 2], &[3, 1, 0, 2], &[3, 1, 1, 2]],
        );

        // Row 1 is NULL and row 2 is an empty non-null row, so the two substitutions are
        // distinguishable: only `PreserveAndExpandEmpty` bumps both.
        assert_output_lens(
            int_list(
                vec![0, 2, 2, 2, 5],
                vec![1, 2, 3, 4, 5],
                Some(vec![true, false, true, true]),
            ),
            [&[2, 0, 0, 3], &[2, 1, 0, 3], &[2, 1, 1, 3]],
        );

        assert_output_lens(int_list(vec![0], vec![], None), [&[], &[], &[]]);
    }

    #[test]
    fn output_lens_handle_a_sliced_input() {
        // Sliced offsets start away from zero; the length is still the per-row difference.
        let list = int_list(vec![0, 2, 3, 3, 6], vec![1, 2, 3, 4, 5, 6], None);
        assert_output_lens(list.slice(1, 3), [&[1, 0, 3], &[1, 0, 3], &[1, 1, 3]]);

        // `ListArray::slice` slices the validity buffer alongside the offsets, so the row index
        // the fused pass hands to `is_valid` has to be in the sliced row space, not the
        // original. Rows 1..4 of this array are NULL, empty, non-empty.
        let with_nulls = int_list(
            vec![0, 2, 4, 4, 7],
            vec![1, 2, 3, 4, 5, 6, 7],
            Some(vec![true, false, true, true]),
        );
        assert_output_lens(with_nulls.slice(1, 3), [&[0, 0, 3], &[1, 0, 3], &[1, 1, 3]]);
    }
}
