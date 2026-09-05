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
//! Once Comet moves to a DataFusion release carrying apache/datafusion#24384, delete this
//! module and go back to `datafusion::physical_plan::unnest::UnnestExec` in the planner.
//!
//! Note that <https://github.com/apache/datafusion-comet/issues/5210> is a *different*
//! unnest cleanup — it tracks adopting upstream `unnest_outer`
//! (apache/datafusion#22100) to retire `ListEmptyToNullExpr`. The two upstream PRs can
//! land in different releases, so closing 5210 is not a signal to delete this fork.
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
//! * the `lt` import path noted below, since Comet does not depend on `arrow_ord` directly;
//! * dropping upstream's `ListUnnest` declaration in favor of importing the public one;
//! * the `precomputed_lengths` parameter on `build_batch` and `list_unnest_at_level`, which is
//!   part of apache/datafusion#24384;
//! * the contiguous-run fast path in `unnest_list_array`, which returns a slice of the child
//!   values instead of gathering them, and the buffer fills in `create_take_indices`.
//!
//! The performance work is Comet-specific and is not held to upstream's shape. When the fork is
//! eventually retired in favor of `UnnestExec`, these paths are what would have to be measured
//! again — or upstreamed first — rather than simply deleted. `ExplodeExec` and `ExplodeStream`
//! were always Comet's own.
//!
//! Note that 54.1.0 predates upstream's `NullHandling` enum and still uses
//! `UnnestOptions::preserve_nulls`, which is why the planner wraps empty arrays with
//! `ListEmptyToNullExpr` to get Spark's `explode_outer` semantics.

use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanBufferBuilder, FixedSizeListArray, Int64Array,
    LargeListArray, LargeListViewArray, ListArray, ListViewArray, PrimitiveArray, Scalar,
    StructArray,
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

        // Chunking the input bounds each build to roughly `batch_size` rows, but two cases can
        // still produce an oversized batch (see `predict_output_lens`), so the output goes
        // through DataFusion's splitter to make the bound unconditional. Note this only bounds
        // the emitted row count; bounding peak memory is the job of the input chunking.
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
    /// Always returns at least 1 while rows remain, so the stream always makes progress: a
    /// single input row is never split across output batches, so one row whose array is
    /// longer than `batch_size` still produces one oversized build, which `BatchSplitStream`
    /// slices down on the way out.
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
    /// Note the scope of the memory bound this buys: chunking removes the input batch size
    /// from the peak, but not the length of an individual list. A single row whose list is
    /// longer than `batch_size`, and recursive unnesting (where the expansion cannot be
    /// predicted up front), both still materialize their full expansion in one build.
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
                // `PendingInput` is only built from a non-empty batch and `next_chunk_rows`
                // always consumes at least one row, so it is dropped the moment it drains.
                debug_assert!(pending.remaining_rows() > 0);

                let rows = pending.next_chunk_rows(self.batch_size);
                let chunk = pending.batch.slice(pending.row_offset, rows);
                let chunk_lengths = pending.chunk_lengths(rows);
                pending.row_offset += rows;
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
                // arrays are all empty and `preserve_nulls` is false); `build_batch` signals
                // that with `None` rather than an empty batch, so move on to the next chunk.
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
                        timer.done();
                        self.pending_input = Some(PendingInput {
                            batch,
                            row_offset: 0,
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
        if let [single] = list_arrays.as_slice() {
            if let Some(list) = single.as_any().downcast_ref::<ListArray>() {
                return Ok(Some(list_output_lens(list, self.options.preserve_nulls)));
            }
        }
        let longest_length = find_longest_length(&list_arrays, &self.options)?;
        Ok(Some(longest_length.as_primitive::<Int64Type>().clone()))
    }
}

/// The per-row unnested length of a single `List` column: the row's list length, or `null_length`
/// for a NULL row.
///
/// What [`find_longest_length`] computes when handed one array, in one pass over the offsets
/// rather than the four allocating kernels it chains to stay generic over list types — `length`
/// (which returns `Int32` for `List`), `cast` to widen it, `is_not_null`, and `zip` to substitute
/// the NULL length. Comet only ever plans `List`, and only ever one or two of them, so this is
/// the path every explode takes; anything else still falls back to the general version.
fn list_output_lens(list: &ListArray, preserve_nulls: bool) -> PrimitiveArray<Int64Type> {
    let null_length = if preserve_nulls { 1 } else { 0 };
    let offsets = list.offsets();
    // Like `find_longest_length`, the result is non-null throughout: a NULL row reports
    // `null_length` rather than a NULL length, which `create_take_indices` relies on.
    let lens: Vec<i64> = match list.nulls() {
        None => offsets.windows(2).map(|w| (w[1] - w[0]) as i64).collect(),
        Some(nulls) => offsets
            .windows(2)
            .enumerate()
            .map(|(row, w)| {
                if nulls.is_valid(row) {
                    (w[1] - w[0]) as i64
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
    precomputed_lengths: Option<&PrimitiveArray<Int64Type>>,
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
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{Field, Int32Type, Schema};
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
        preserve_nulls: bool,
    ) -> Result<Vec<RecordBatch>> {
        let input_schema = input[0].schema();
        let source = MemorySourceConfig::try_new_exec(&[input], input_schema, None)?;
        explode_child(source, batch_size, preserve_nulls).await
    }

    /// As [`explode`], but over an arbitrary child plan.
    async fn explode_child(
        child: Arc<dyn ExecutionPlan>,
        batch_size: usize,
        preserve_nulls: bool,
    ) -> Result<Vec<RecordBatch>> {
        let output_schema = Arc::new(Schema::new(vec![Field::new("l", DataType::Int32, true)]));
        let explode = ExplodeExec::new(
            child,
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
        let batches = explode(vec![list_batch(&[Some(3); 10])], 8, true)
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
        let batches = explode(vec![list_batch(&[Some(3), Some(3), Some(3)])], 4, true)
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
        // One row cannot be chunked on the input side, so the oversized build is sliced.
        let batches = explode(vec![list_batch(&[Some(25)])], 10, true)
            .await
            .unwrap();
        assert_eq!(sizes(&batches), vec![10, 10, 5]);
        assert_eq!(values(&batches), seq(25));
    }

    /// Chunked output must match unchunked output exactly, and hold the invariants that
    /// apply regardless of null handling: bounded, non-empty, and totalling `expected_rows`.
    async fn assert_chunking_matches_whole(
        lens: &[Option<usize>],
        preserve_nulls: bool,
        expected_rows: usize,
    ) {
        let chunked = explode(vec![list_batch(lens)], 2, preserve_nulls)
            .await
            .unwrap();
        let whole = explode(vec![list_batch(lens)], 1024, preserve_nulls)
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
        // With preserve_nulls (Spark's explode_outer, after the planner has rewritten empty
        // arrays to NULL), a NULL array yields one NULL row. The per-row counts driving
        // chunking must agree, or boundaries drift out of step with the unnesting.
        assert_chunking_matches_whole(&[Some(3), None, Some(2), None], true, 7).await;
    }

    #[tokio::test]
    async fn chunking_preserves_non_outer_semantics() {
        // Without preserve_nulls (plain explode), NULL arrays produce nothing. Chunks made
        // up entirely of such rows must not stall the stream or emit an empty batch.
        assert_chunking_matches_whole(&[None, Some(4), None, Some(1)], false, 5).await;
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
            options: UnnestOptions {
                preserve_nulls: true,
                recursions: vec![],
            },
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
            UnnestOptions {
                preserve_nulls: true,
                recursions: vec![],
            },
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
        // Plain `explode`: a NULL row and an empty row both contribute no elements, and with
        // `preserve_nulls` false neither is padded, so the run stays unbroken across them.
        let list = int_list(
            vec![0, 2, 2, 2, 5],
            vec![1, 2, 3, 4, 5],
            Some(vec![true, false, true, true]),
        );
        let (values, fast) = unnest(&list, vec![2, 0, 0, 3]);
        assert!(fast, "rows contributing nothing must not break the run");
        assert_eq!(values, (1..=5).map(Some).collect::<Vec<_>>());
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
    /// chunking in `ExplodeStream` and the unnesting in `build_batch` both consume it.
    fn assert_lens_match_general(list: ListArray, preserve_nulls: bool) {
        let options = UnnestOptions {
            preserve_nulls,
            recursions: vec![],
        };
        let arrays = vec![Arc::new(list.clone()) as ArrayRef];
        let expected = find_longest_length(&arrays, &options).unwrap();
        let expected = expected.as_primitive::<Int64Type>();
        let actual = list_output_lens(&list, preserve_nulls);
        assert_eq!(&actual, expected, "preserve_nulls = {preserve_nulls}");
        assert_eq!(actual.null_count(), 0, "lengths must never be NULL");
    }

    #[test]
    fn fused_lengths_match_the_general_kernel() {
        let plain = int_list(vec![0, 3, 4, 4, 6], vec![1, 2, 3, 4, 5, 6], None);
        assert_lens_match_general(plain.clone(), true);
        assert_lens_match_general(plain, false);

        let with_nulls = int_list(
            vec![0, 2, 2, 2, 5],
            vec![1, 2, 3, 4, 5],
            Some(vec![true, false, true, true]),
        );
        assert_lens_match_general(with_nulls.clone(), true);
        assert_lens_match_general(with_nulls, false);

        let empty = int_list(vec![0], vec![], None);
        assert_lens_match_general(empty.clone(), true);
        assert_lens_match_general(empty, false);
    }

    #[test]
    fn fused_lengths_handle_a_sliced_input() {
        // Sliced offsets start away from zero; the length is still the per-row difference.
        let list = int_list(vec![0, 2, 3, 3, 6], vec![1, 2, 3, 4, 5, 6], None);
        let sliced = list.slice(1, 3);
        assert_lens_match_general(sliced, true);
    }
}
