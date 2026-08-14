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

//! A temporary fork of DataFusion's `UnnestExec` that respects
//! `datafusion.execution.batch_size`.
//!
//! # Why this fork exists
//!
//! DataFusion's `UnnestExec` emits exactly one output batch per input batch, however many
//! rows the unnesting produces, and never consults `batch_size`. For `explode` this means
//! an 8192-row batch of 100-element arrays comes back as a single 819,200-row batch, and
//! peak memory scales with input batch size times array length rather than with
//! `batch_size`.
//!
//! The fix has been submitted upstream:
//!
//! * <https://github.com/apache/datafusion/issues/24383>
//! * <https://github.com/apache/datafusion/pull/24384>
//!
//! # Deleting this file
//!
//! Once Comet moves to a DataFusion release carrying that PR, delete this module and go
//! back to `datafusion::physical_plan::unnest::UnnestExec` in the planner. Tracked in
//! <https://github.com/apache/datafusion-comet/issues/5210> alongside the other
//! unnest-related workarounds.
//!
//! # What was forked
//!
//! The unnesting kernels below (`build_batch` and everything it calls) are copied verbatim
//! from `datafusion/physical-plan/src/unnest.rs` at DataFusion 54.1.0, upstream revision
//! `cc7565be1ee97ba8fa2f5d6da373c5e38d81bb13`. They are private to
//! `datafusion-physical-plan`, so they cannot be called from here without copying them.
//! Keep them byte-identical to upstream so the eventual deletion is mechanical; the
//! Comet-specific behavior lives entirely in `ExplodeExec` and `ExplodeStream`.
//!
//! Note that 54.1.0 predates upstream's `NullHandling` enum and still uses
//! `UnnestOptions::preserve_nulls`, which is why the planner wraps empty arrays with
//! `ListEmptyToNullExpr` to get Spark's `explode_outer` semantics.

use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, FixedSizeListArray, Int64Array, LargeListArray,
    LargeListViewArray, ListArray, ListViewArray, PrimitiveArray, Scalar, StructArray,
};
use arrow::compute::kernels::length::length;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{cast, is_not_null, kernels, sum};
use arrow::datatypes::{DataType, Int64Type, SchemaRef};
use arrow::record_batch::RecordBatch;
// Upstream imports this as `arrow_ord::cmp::lt`; Comet reaches it through `arrow`,
// which does not have `arrow_ord` as a direct dependency.
use arrow::compute::kernels::cmp::lt;
use datafusion::common::{
    exec_datafusion_err, exec_err, internal_err, HashMap, HashSet, Result, UnnestOptions,
};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, RecordOutput,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use std::cmp::{self, Ordering};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// Comet's explode operator: DataFusion's `UnnestExec` with the input consumed in chunks so
/// that output batches respect `datafusion.execution.batch_size`.
///
/// Everything below this point is Comet code, not forked from upstream.
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
    ) -> Self {
        // Unnesting invalidates the child's orderings and constraints for the unnested
        // columns, and Comet plans explode on a single partition, so start from empty
        // equivalences rather than trying to project the child's.
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Self {
            child,
            schema,
            list_column_indices,
            struct_column_indices,
            options,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
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
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        let input = self.child.execute(partition, context)?;

        Ok(Box::pin(ExplodeStream {
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
            pending_output: None,
        }))
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
    /// How many output rows each input row expands into, indexed by input row.
    ///
    /// Empty when the expansion factor cannot be predicted from the input alone, in which
    /// case the whole remaining input is unnested in one call and only the output is split.
    /// See [`ExplodeStream::predict_output_lens`].
    output_lens: Vec<usize>,
}

impl PendingInput {
    fn remaining_rows(&self) -> usize {
        self.batch.num_rows() - self.row_offset
    }

    /// How many input rows to unnest next so the resulting batch holds at most `batch_size`
    /// rows.
    ///
    /// Always returns at least 1 while rows remain, so the stream always makes progress: a
    /// single input row is never split across output batches, so one row whose array is
    /// longer than `batch_size` still produces one oversized build, which is then sliced
    /// down on the way out.
    fn next_chunk_rows(&self, batch_size: usize) -> usize {
        let remaining = self.remaining_rows();
        if self.output_lens.is_empty() {
            return remaining;
        }

        let mut rows = 0;
        let mut output_rows = 0usize;
        while rows < remaining {
            let len = self.output_lens[self.row_offset + rows];
            if rows > 0 && output_rows.saturating_add(len) > batch_size {
                break;
            }
            output_rows += len;
            rows += 1;
        }
        rows
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
    input_batches: datafusion::physical_plan::metrics::Count,
    input_rows: datafusion::physical_plan::metrics::Count,
    /// Target number of rows per output batch, from `datafusion.execution.batch_size`.
    batch_size: usize,
    /// Rows of the current input batch that have not been unnested yet.
    pending_input: Option<PendingInput>,
    /// Unnested rows that have been built but not emitted yet.
    pending_output: Option<RecordBatch>,
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
            // Emit already-unnested rows first, at most `batch_size` at a time.
            if let Some(batch) = self.pending_output.take() {
                let (emit, rest) = split_off_head(batch, self.batch_size);
                self.pending_output = rest;
                (&emit).record_output(&self.baseline_metrics);
                return Poll::Ready(Some(Ok(emit)));
            }

            // Unnest the next chunk of the input batch already in hand.
            if let Some(pending) = self.pending_input.as_mut() {
                if pending.remaining_rows() == 0 {
                    self.pending_input = None;
                    continue;
                }

                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();

                let rows = pending.next_chunk_rows(self.batch_size);
                let chunk = pending.batch.slice(pending.row_offset, rows);
                pending.row_offset += rows;

                let result = build_batch(
                    &chunk,
                    &self.schema,
                    &self.list_type_columns,
                    &self.struct_column_indices,
                    &self.options,
                );
                timer.done();

                // A chunk can legitimately produce no rows at all (for example rows whose
                // arrays are all empty and `preserve_nulls` is false); move on to the next
                // chunk rather than emitting an empty batch.
                match result? {
                    Some(batch) if batch.num_rows() > 0 => self.pending_output = Some(batch),
                    _ => {}
                }
                continue;
            }

            // Otherwise pull the next input batch.
            return Poll::Ready(match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.input_batches.add(1);
                    self.input_rows.add(batch.num_rows());
                    if batch.num_rows() == 0 {
                        continue;
                    }

                    let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                    let timer = elapsed_compute.timer();
                    let output_lens = self.predict_output_lens(&batch);
                    timer.done();

                    match output_lens {
                        Ok(output_lens) => {
                            self.pending_input = Some(PendingInput {
                                batch,
                                row_offset: 0,
                                output_lens,
                            });
                            continue;
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
                other => other,
            });
        }
    }

    /// Compute how many output rows each input row of `batch` will expand into, so the input
    /// can be chunked to keep each build bounded.
    ///
    /// Returns an empty vec when the count cannot be derived from the input alone, which is
    /// the signal to unnest the whole batch in one call:
    ///
    /// * With no list columns, unnesting only widens structs and leaves the row count alone,
    ///   so the output is already bounded by the input batch size.
    /// * With recursion (`depth > 1`), a row's expansion depends on the lengths of inner
    ///   lists that only exist after the outer levels have been unnested, so it cannot be
    ///   predicted up front. Comet only plans depth-1 explode today, but the fallback keeps
    ///   this correct if that changes.
    fn predict_output_lens(&self, batch: &RecordBatch) -> Result<Vec<usize>> {
        if self.list_type_columns.is_empty()
            || self
                .list_type_columns
                .iter()
                .any(|unnest| unnest.depth != 1)
        {
            return Ok(vec![]);
        }

        let list_arrays: Vec<ArrayRef> = self
            .list_type_columns
            .iter()
            .map(|unnest| Arc::clone(batch.column(unnest.index_in_input_schema)))
            .collect();

        // The same per-row length that `list_unnest_at_level` derives when it actually
        // unnests, so the chunk boundaries it produces are exact.
        let longest_length = find_longest_length(&list_arrays, &self.options)?;
        Ok(longest_length
            .as_primitive::<Int64Type>()
            .values()
            .iter()
            .map(|len| usize::try_from(*len).unwrap_or(0))
            .collect())
    }
}

/// Split at most `batch_size` rows off the front of `batch`, returning the remainder.
///
/// Both halves are zero-copy slices, so this bounds the row count of an emitted batch but
/// not its memory footprint — the slices still reference the full underlying buffers.
/// Keeping peak memory down is the job of chunking the *input*, which is what
/// [`PendingInput::next_chunk_rows`] does.
fn split_off_head(batch: RecordBatch, batch_size: usize) -> (RecordBatch, Option<RecordBatch>) {
    if batch.num_rows() <= batch_size {
        return (batch, None);
    }
    let head = batch.slice(0, batch_size);
    let tail = batch.slice(batch_size, batch.num_rows() - batch_size);
    (head, Some(tail))
}

// ---------------------------------------------------------------------------------------
// Everything below is copied verbatim from DataFusion 54.1.0
// `datafusion/physical-plan/src/unnest.rs` (upstream revision
// cc7565be1ee97ba8fa2f5d6da373c5e38d81bb13). These helpers are private to
// `datafusion-physical-plan`, so they cannot be called from Comet without copying them.
// Keep them unmodified so the eventual deletion of this fork is mechanical.
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
    let columns_expanded = input_batch
        .iter()
        .enumerate()
        .map(|(idx, column_data)| match struct_column_indices.get(&idx) {
            Some(_) => match column_data.data_type() {
                DataType::Struct(_) => {
                    let struct_arr = column_data.as_any().downcast_ref::<StructArray>().unwrap();
                    Ok(struct_arr.columns().to_vec())
                }
                data_type => internal_err!(
                    "expecting column {idx} from input plan to be a struct, got {data_type}"
                ),
            },
            None => Ok(vec![Arc::clone(column_data)]),
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(RecordBatch::try_new(Arc::clone(schema), columns_expanded)?)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ListUnnest {
    pub index_in_input_schema: usize,
    pub depth: usize,
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
    let longest_length = find_longest_length(&arrs_to_unnest, options)?;
    let unnested_length = longest_length.as_primitive::<Int64Type>();
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

    // Create the take indices array for other columns
    let take_indices = create_take_indices(unnested_length, total_length);
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
    let ret = repeat_arrs_from_indices(batch, &take_indices, &repeat_mask)?;

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
) -> Result<Option<RecordBatch>> {
    let transformed = match list_type_columns.len() {
        0 => flatten_struct_cols(batch.columns(), schema, struct_column_indices),
        _ => {
            let mut temp_unnested_result = HashMap::new();
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
                let Some(temp_result) = list_unnest_at_level(
                    input,
                    list_type_columns,
                    &mut temp_unnested_result,
                    depth,
                    options,
                )?
                else {
                    return Ok(None);
                };
                flatten_arrs = temp_result;
            }
            let unnested_array_map: HashMap<usize, Vec<UnnestingResult>> =
                temp_unnested_result.into_iter().fold(
                    HashMap::new(),
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

            let ret = flatten_arrs
                .into_iter()
                .enumerate()
                .flat_map(|(col_idx, arr)| {
                    // Convert original column into its unnested version(s)
                    // Plural because one column can be unnested with different recursion level
                    // and into separate output columns
                    match multi_unnested_per_original_index.remove(&col_idx) {
                        Some(unnested_arrays) => unnested_arrays,
                        None => vec![arr],
                    }
                })
                .collect::<Vec<_>>();

            flatten_struct_cols(&ret, schema, struct_column_indices)
        }
    }?;
    Ok(Some(transformed))
}

/// Find the longest list length among the given list arrays for each row.
///
/// For example if we have the following two list arrays:
///
/// ```ignore
/// l1: [1, 2, 3], null, [], [3]
/// l2: [4,5], [], null, [6, 7]
/// ```
///
/// If `preserve_nulls` is false, the longest length array will be:
///
/// ```ignore
/// longest_length: [3, 0, 0, 2]
/// ```
///
/// whereas if `preserve_nulls` is true, the longest length array will be:
///
///
/// ```ignore
/// longest_length: [3, 1, 1, 2]
/// ```
fn find_longest_length(list_arrays: &[ArrayRef], options: &UnnestOptions) -> Result<ArrayRef> {
    // The length of a NULL list
    let null_length = if options.preserve_nulls {
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
    Ok(longest_length)
}

/// Trait defining common methods used for unnesting, implemented by list array types.
trait ListArrayType: Array {
    /// Returns a reference to the values of this list.
    fn values(&self) -> &ArrayRef;

    /// Returns the start and end offset of the values for the given row.
    fn value_offsets(&self, row: usize) -> (i64, i64);
}

impl ListArrayType for ListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row].into(), offsets[row + 1].into())
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
}

impl ListArrayType for FixedSizeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let start = self.value_offset(row) as i64;
        (start, start + self.value_length() as i64)
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
        for _ in value_length..target_length {
            take_indices_builder.append_null();
        }
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
    let mut builder = PrimitiveArray::<Int64Type>::builder(capacity);
    for (index, repeat) in length_array.iter().enumerate() {
        // The length array should not contain nulls, so unwrap is safe
        let repeat = repeat.unwrap();
        (0..repeat).for_each(|_| builder.append_value(index as i64));
    }
    builder.finish()
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
/// the final batch if `preserve_nulls` is true:
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
                Ok(kernels::take::take(arr, indices, None)?)
            } else {
                Ok(new_null_array(arr.data_type(), arr.len()))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Int32Type, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionConfig;

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
        preserve_nulls: bool,
    ) -> Result<Vec<RecordBatch>> {
        let input_schema = input[0].schema();
        let output_schema = Arc::new(Schema::new(vec![Field::new("l", DataType::Int32, true)]));
        let source = MemorySourceConfig::try_new_exec(&[input], input_schema, None)?;
        let explode = ExplodeExec::new(
            source,
            vec![ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            }],
            vec![],
            output_schema,
            UnnestOptions {
                preserve_nulls,
                recursions: vec![],
            },
        );
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
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn respects_batch_size() {
        // 10 rows x 10 elements = 100 output rows from ONE input batch. Upstream
        // `UnnestExec` returns this as a single 100-row batch.
        let batches = explode(vec![list_batch(&[Some(10); 10])], 8, true)
            .await
            .unwrap();
        let sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
        assert!(
            sizes.iter().all(|s| *s <= 8),
            "every output batch must respect batch_size=8, got {sizes:?}"
        );
        assert_eq!(sizes.iter().sum::<usize>(), 100);
        assert_eq!(values(&batches), (0..100).map(Some).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn chunks_input_rather_than_slicing_output() {
        // Pins *how* the limit is met, which is what bounds peak memory. 3 rows of 3
        // elements at batch_size=4 gives [3, 3, 3] when the input is chunked per row;
        // building all 9 first and slicing would give [4, 4, 1].
        let batches = explode(vec![list_batch(&[Some(3), Some(3), Some(3)])], 4, true)
            .await
            .unwrap();
        let sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
        assert_eq!(
            sizes,
            vec![3, 3, 3],
            "input should be chunked per row, not built whole and sliced into [4, 4, 1]"
        );
        assert_eq!(values(&batches), (0..9).map(Some).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn single_row_exceeding_batch_size_is_sliced() {
        // One row cannot be chunked on the input side, so the oversized build is sliced.
        let batches = explode(vec![list_batch(&[Some(25)])], 10, true)
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![10, 10, 5]
        );
        assert_eq!(values(&batches), (0..25).map(Some).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn chunking_preserves_outer_semantics() {
        // With preserve_nulls (Spark's explode_outer, after the planner has rewritten empty
        // arrays to NULL), a NULL array yields one NULL row. The per-row counts driving
        // chunking must agree, or boundaries drift out of step with the unnesting.
        let lens = &[Some(3), None, Some(2), None];
        let chunked = explode(vec![list_batch(lens)], 2, true).await.unwrap();
        let whole = explode(vec![list_batch(lens)], 1024, true).await.unwrap();

        assert!(chunked.iter().all(|b| b.num_rows() <= 2));
        assert_eq!(chunked.iter().map(|b| b.num_rows()).sum::<usize>(), 7);
        assert_eq!(values(&chunked), values(&whole));
    }

    #[tokio::test]
    async fn chunking_preserves_non_outer_semantics() {
        // Without preserve_nulls (plain explode), NULL arrays produce nothing. Chunks made
        // up entirely of such rows must not stall the stream or emit an empty batch.
        let lens = &[None, Some(4), None, Some(1)];
        let chunked = explode(vec![list_batch(lens)], 2, false).await.unwrap();
        let whole = explode(vec![list_batch(lens)], 1024, false).await.unwrap();

        assert!(chunked.iter().all(|b| b.num_rows() > 0));
        assert_eq!(chunked.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
        assert_eq!(values(&chunked), values(&whole));
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
        let batches = explode(input, 4, true).await.unwrap();
        let sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
        assert!(
            sizes.iter().all(|s| *s <= 4),
            "every output batch must respect batch_size=4, got {sizes:?}"
        );
        assert_eq!(sizes.iter().sum::<usize>(), 20);
    }
}
