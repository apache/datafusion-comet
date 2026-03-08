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

//! Stream implementation for semi/anti sort-merge joins.
//!
//! Ported from Apache DataFusion.

use std::cmp::Ordering;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBufferBuilder, RecordBatch};
use arrow::compute::{filter_record_batch, not, BatchCoalescer, SortOptions};
use arrow::datatypes::SchemaRef;
use arrow::util::bit_chunk_iterator::UnalignedBitChunk;
use arrow::util::bit_util::apply_bitwise_binary_op;
use datafusion::common::{internal_err, JoinSide, JoinType, NullEquality, Result, ScalarValue};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr_common::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::utils::{compare_join_arrays, JoinFilter};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder,
};
use datafusion::physical_plan::RecordBatchStream;

use futures::{ready, Stream, StreamExt};

/// Evaluates join key expressions against a batch, returning one array per key.
fn evaluate_join_keys(batch: &RecordBatch, on: &[PhysicalExprRef]) -> Result<Vec<ArrayRef>> {
    on.iter()
        .map(|expr: &PhysicalExprRef| {
            let num_rows = batch.num_rows();
            let val = expr.evaluate(batch)?;
            val.into_array(num_rows)
        })
        .collect()
}

/// Find the first index in `key_arrays` starting from `from` where the key
/// differs from the key at `from`. Uses binary search with `compare_join_arrays`.
fn find_key_group_end(
    key_arrays: &[ArrayRef],
    from: usize,
    len: usize,
    sort_options: &[SortOptions],
    null_equality: NullEquality,
) -> Result<usize> {
    let mut lo = from + 1;
    let mut hi = len;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if compare_join_arrays(
            key_arrays,
            from,
            key_arrays,
            mid,
            sort_options,
            null_equality,
        )? == Ordering::Equal
        {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Tracks whether we're mid-key-group when `poll_next_outer_batch` returns
/// `Poll::Pending` inside the Equal branch's boundary loop.
#[derive(Debug)]
enum BoundaryState {
    /// Normal processing — not inside a boundary poll.
    Normal,
    /// The no-filter boundary loop's `poll_next_outer_batch` returned
    /// Pending. Carries the key arrays and index from the last emitted
    /// batch so we can compare with the next batch's first key.
    NoFilterPending {
        saved_keys: Vec<ArrayRef>,
        saved_idx: usize,
    },
    /// The filtered boundary loop's `poll_next_outer_batch` returned
    /// Pending. The `inner_key_buffer` field already holds the buffered
    /// inner rows needed to resume filter evaluation.
    FilteredPending,
}

pub(super) struct SemiAntiSortMergeJoinStream {
    /// true for semi (emit matched), false for anti (emit unmatched)
    is_semi: bool,

    // Input streams
    outer: SendableRecordBatchStream,
    inner: SendableRecordBatchStream,

    // Current batches and cursor positions
    outer_batch: Option<RecordBatch>,
    outer_offset: usize,
    outer_key_arrays: Vec<ArrayRef>,
    inner_batch: Option<RecordBatch>,
    inner_offset: usize,
    inner_key_arrays: Vec<ArrayRef>,

    // Per-outer-batch match tracking (bit-packed)
    matched: BooleanBufferBuilder,

    // Inner key group buffer for filtered joins
    inner_key_buffer: Vec<RecordBatch>,

    // Tracks partial buffering across Pending re-entries
    buffering_inner_pending: bool,

    // Boundary re-entry state
    boundary_state: BoundaryState,

    // Join condition
    on_outer: Vec<PhysicalExprRef>,
    on_inner: Vec<PhysicalExprRef>,
    filter: Option<JoinFilter>,
    sort_options: Vec<SortOptions>,
    null_equality: NullEquality,
    outer_is_left: bool,

    // Output
    coalescer: BatchCoalescer,
    schema: SchemaRef,

    // Metrics
    join_time: datafusion::physical_plan::metrics::Time,
    input_batches: Count,
    input_rows: Count,
    baseline_metrics: BaselineMetrics,

    // Guards against double-emit on Pending re-entry
    batch_emitted: bool,
}

impl SemiAntiSortMergeJoinStream {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        schema: SchemaRef,
        sort_options: Vec<SortOptions>,
        null_equality: NullEquality,
        outer: SendableRecordBatchStream,
        inner: SendableRecordBatchStream,
        on_outer: Vec<PhysicalExprRef>,
        on_inner: Vec<PhysicalExprRef>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        batch_size: usize,
        partition: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let is_semi = matches!(join_type, JoinType::LeftSemi | JoinType::RightSemi);
        let outer_is_left = matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti);

        let join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);
        let input_batches = MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let baseline_metrics = BaselineMetrics::new(metrics, partition);

        Ok(Self {
            is_semi,
            outer,
            inner,
            outer_batch: None,
            outer_offset: 0,
            outer_key_arrays: vec![],
            inner_batch: None,
            inner_offset: 0,
            inner_key_arrays: vec![],
            matched: BooleanBufferBuilder::new(0),
            inner_key_buffer: vec![],
            buffering_inner_pending: false,
            boundary_state: BoundaryState::Normal,
            on_outer,
            on_inner,
            filter,
            sort_options,
            null_equality,
            outer_is_left,
            coalescer: BatchCoalescer::new(Arc::clone(&schema), batch_size)
                .with_biggest_coalesce_batch_size(Some(batch_size / 2)),
            schema,
            join_time,
            input_batches,
            input_rows,
            baseline_metrics,
            batch_emitted: false,
        })
    }

    /// Poll for the next outer batch. Returns true if a batch was loaded.
    fn poll_next_outer_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool>> {
        loop {
            match ready!(self.outer.poll_next_unpin(cx)) {
                None => return Poll::Ready(Ok(false)),
                Some(Err(e)) => return Poll::Ready(Err(e)),
                Some(Ok(batch)) => {
                    self.input_batches.add(1);
                    self.input_rows.add(batch.num_rows());
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let keys = evaluate_join_keys(&batch, &self.on_outer)?;
                    let num_rows = batch.num_rows();
                    self.outer_batch = Some(batch);
                    self.outer_offset = 0;
                    self.outer_key_arrays = keys;
                    self.batch_emitted = false;
                    self.matched = BooleanBufferBuilder::new(num_rows);
                    self.matched.append_n(num_rows, false);
                    return Poll::Ready(Ok(true));
                }
            }
        }
    }

    /// Poll for the next inner batch. Returns true if a batch was loaded.
    fn poll_next_inner_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool>> {
        loop {
            match ready!(self.inner.poll_next_unpin(cx)) {
                None => return Poll::Ready(Ok(false)),
                Some(Err(e)) => return Poll::Ready(Err(e)),
                Some(Ok(batch)) => {
                    self.input_batches.add(1);
                    self.input_rows.add(batch.num_rows());
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let keys = evaluate_join_keys(&batch, &self.on_inner)?;
                    self.inner_batch = Some(batch);
                    self.inner_offset = 0;
                    self.inner_key_arrays = keys;
                    return Poll::Ready(Ok(true));
                }
            }
        }
    }

    /// Emit the current outer batch through the coalescer, applying the
    /// matched bitset as a selection mask.
    fn emit_outer_batch(&mut self) -> Result<()> {
        if self.batch_emitted {
            return Ok(());
        }
        self.batch_emitted = true;

        let batch = self.outer_batch.as_ref().unwrap();

        let selection = BooleanArray::new(self.matched.finish(), None);

        let selection = if self.is_semi {
            selection
        } else {
            not(&selection)?
        };

        let filtered = filter_record_batch(batch, &selection)?;
        if filtered.num_rows() > 0 {
            self.coalescer.push_batch(filtered)?;
        }
        Ok(())
    }

    /// Process a key match between outer and inner sides (no filter).
    fn process_key_match_no_filter(&mut self) -> Result<()> {
        let outer_batch = self.outer_batch.as_ref().unwrap();
        let num_outer = outer_batch.num_rows();

        let outer_group_end = find_key_group_end(
            &self.outer_key_arrays,
            self.outer_offset,
            num_outer,
            &self.sort_options,
            self.null_equality,
        )?;

        for i in self.outer_offset..outer_group_end {
            self.matched.set_bit(i, true);
        }

        self.outer_offset = outer_group_end;
        Ok(())
    }

    /// Advance inner past the current key group. Returns Ok(true) if inner
    /// is exhausted.
    fn advance_inner_past_key_group(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool>> {
        loop {
            let inner_batch = match &self.inner_batch {
                Some(b) => b,
                None => return Poll::Ready(Ok(true)),
            };
            let num_inner = inner_batch.num_rows();

            let group_end = find_key_group_end(
                &self.inner_key_arrays,
                self.inner_offset,
                num_inner,
                &self.sort_options,
                self.null_equality,
            )?;

            if group_end < num_inner {
                self.inner_offset = group_end;
                return Poll::Ready(Ok(false));
            }

            // Key group extends to end of batch — need to check next batch
            let last_key_idx = num_inner - 1;
            let saved_inner_keys = self.inner_key_arrays.clone();

            match ready!(self.poll_next_inner_batch(cx)) {
                Err(e) => return Poll::Ready(Err(e)),
                Ok(false) => {
                    return Poll::Ready(Ok(true));
                }
                Ok(true) => {
                    if keys_match(
                        &saved_inner_keys,
                        last_key_idx,
                        &self.inner_key_arrays,
                        0,
                        &self.sort_options,
                        self.null_equality,
                    )? {
                        continue;
                    } else {
                        return Poll::Ready(Ok(false));
                    }
                }
            }
        }
    }

    /// Buffer inner key group for filter evaluation. Collects all inner rows
    /// with the current key across batch boundaries.
    fn buffer_inner_key_group(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool>> {
        let mut resume_from_poll = false;
        if self.buffering_inner_pending {
            self.buffering_inner_pending = false;
            resume_from_poll = true;
        } else {
            self.inner_key_buffer.clear();
        }

        loop {
            let inner_batch = match &self.inner_batch {
                Some(b) => b,
                None => return Poll::Ready(Ok(true)),
            };
            let num_inner = inner_batch.num_rows();
            let group_end = find_key_group_end(
                &self.inner_key_arrays,
                self.inner_offset,
                num_inner,
                &self.sort_options,
                self.null_equality,
            )?;

            if !resume_from_poll {
                let slice = inner_batch.slice(self.inner_offset, group_end - self.inner_offset);
                self.inner_key_buffer.push(slice);

                if group_end < num_inner {
                    self.inner_offset = group_end;
                    return Poll::Ready(Ok(false));
                }
            }
            resume_from_poll = false;

            // Key group extends to end of batch — check next
            let last_key_idx = num_inner - 1;
            let saved_inner_keys = self.inner_key_arrays.clone();

            self.buffering_inner_pending = true;
            match ready!(self.poll_next_inner_batch(cx)) {
                Err(e) => {
                    self.buffering_inner_pending = false;
                    return Poll::Ready(Err(e));
                }
                Ok(false) => {
                    self.buffering_inner_pending = false;
                    return Poll::Ready(Ok(true));
                }
                Ok(true) => {
                    self.buffering_inner_pending = false;
                    if keys_match(
                        &saved_inner_keys,
                        last_key_idx,
                        &self.inner_key_arrays,
                        0,
                        &self.sort_options,
                        self.null_equality,
                    )? {
                        continue;
                    } else {
                        return Poll::Ready(Ok(false));
                    }
                }
            }
        }
    }

    /// Process a key match with a filter. For each inner row in the buffered
    /// key group, evaluates the filter against the outer key group and ORs
    /// the results into the matched bitset.
    fn process_key_match_with_filter(&mut self) -> Result<()> {
        let filter = self.filter.as_ref().unwrap();
        let outer_batch = self.outer_batch.as_ref().unwrap();
        let num_outer = outer_batch.num_rows();

        debug_assert!(
            !self.inner_key_buffer.is_empty(),
            "process_key_match_with_filter called with empty inner_key_buffer"
        );
        debug_assert!(
            self.outer_offset < num_outer,
            "outer_offset must be within the current batch"
        );
        debug_assert!(
            self.matched.len() == num_outer,
            "matched vector must be sized for the current outer batch"
        );

        let outer_group_end = find_key_group_end(
            &self.outer_key_arrays,
            self.outer_offset,
            num_outer,
            &self.sort_options,
            self.null_equality,
        )?;
        let outer_group_len = outer_group_end - self.outer_offset;
        let outer_slice = outer_batch.slice(self.outer_offset, outer_group_len);

        let mut matched_count =
            UnalignedBitChunk::new(self.matched.as_slice(), self.outer_offset, outer_group_len)
                .count_ones();

        'outer: for inner_slice in &self.inner_key_buffer {
            for inner_row in 0..inner_slice.num_rows() {
                if matched_count == outer_group_len {
                    break 'outer;
                }

                let filter_result = evaluate_filter_for_inner_row(
                    self.outer_is_left,
                    filter,
                    &outer_slice,
                    inner_slice,
                    inner_row,
                )?;

                let filter_buf = filter_result.values();
                apply_bitwise_binary_op(
                    self.matched.as_slice_mut(),
                    self.outer_offset,
                    filter_buf.inner().as_slice(),
                    filter_buf.offset(),
                    outer_group_len,
                    |a, b| a | b,
                );

                matched_count = UnalignedBitChunk::new(
                    self.matched.as_slice(),
                    self.outer_offset,
                    outer_group_len,
                )
                .count_ones();
            }
        }

        self.outer_offset = outer_group_end;
        Ok(())
    }

    /// Main loop: drive the merge-scan to produce output batches.
    #[allow(clippy::panicking_unwrap)]
    fn poll_join(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<RecordBatch>>> {
        let join_time = self.join_time.clone();
        let _timer = join_time.timer();

        loop {
            // 1. Ensure we have an outer batch
            if self.outer_batch.is_none() {
                match ready!(self.poll_next_outer_batch(cx)) {
                    Err(e) => return Poll::Ready(Err(e)),
                    Ok(false) => {
                        // Outer exhausted — flush coalescer
                        self.boundary_state = BoundaryState::Normal;
                        self.coalescer.finish_buffered_batch()?;
                        if let Some(batch) = self.coalescer.next_completed_batch() {
                            return Poll::Ready(Ok(Some(batch)));
                        }
                        return Poll::Ready(Ok(None));
                    }
                    Ok(true) => {
                        match std::mem::replace(&mut self.boundary_state, BoundaryState::Normal) {
                            BoundaryState::NoFilterPending {
                                saved_keys,
                                saved_idx,
                            } => {
                                let same_key = keys_match(
                                    &saved_keys,
                                    saved_idx,
                                    &self.outer_key_arrays,
                                    0,
                                    &self.sort_options,
                                    self.null_equality,
                                )?;
                                if same_key {
                                    self.process_key_match_no_filter()?;
                                    let num_outer = self.outer_batch.as_ref().unwrap().num_rows();
                                    if self.outer_offset >= num_outer {
                                        let new_saved = self.outer_key_arrays.clone();
                                        let new_idx = num_outer - 1;
                                        self.boundary_state = BoundaryState::NoFilterPending {
                                            saved_keys: new_saved,
                                            saved_idx: new_idx,
                                        };
                                        self.emit_outer_batch()?;
                                        self.outer_batch = None;
                                        continue;
                                    }
                                }
                            }
                            BoundaryState::FilteredPending => {
                                if !self.inner_key_buffer.is_empty() {
                                    let first_inner = &self.inner_key_buffer[0];
                                    let inner_keys =
                                        evaluate_join_keys(first_inner, &self.on_inner)?;
                                    let same_key = keys_match(
                                        &self.outer_key_arrays,
                                        0,
                                        &inner_keys,
                                        0,
                                        &self.sort_options,
                                        self.null_equality,
                                    )?;
                                    if same_key {
                                        self.process_key_match_with_filter()?;
                                        let num_outer =
                                            self.outer_batch.as_ref().unwrap().num_rows();
                                        if self.outer_offset >= num_outer {
                                            self.boundary_state = BoundaryState::FilteredPending;
                                            self.emit_outer_batch()?;
                                            self.outer_batch = None;
                                            continue;
                                        }
                                    }
                                }
                                self.inner_key_buffer.clear();
                            }
                            BoundaryState::Normal => {}
                        }
                    }
                }
            }

            // 2. Ensure we have an inner batch (unless inner is exhausted)
            if self.inner_batch.is_none() && matches!(self.boundary_state, BoundaryState::Normal) {
                match ready!(self.poll_next_inner_batch(cx)) {
                    Err(e) => return Poll::Ready(Err(e)),
                    Ok(false) => {
                        // Inner exhausted — emit remaining outer batches
                        self.emit_outer_batch()?;
                        self.outer_batch = None;

                        loop {
                            match ready!(self.poll_next_outer_batch(cx)) {
                                Err(e) => return Poll::Ready(Err(e)),
                                Ok(false) => break,
                                Ok(true) => {
                                    self.emit_outer_batch()?;
                                    self.outer_batch = None;
                                }
                            }
                        }

                        self.coalescer.finish_buffered_batch()?;
                        if let Some(batch) = self.coalescer.next_completed_batch() {
                            return Poll::Ready(Ok(Some(batch)));
                        }
                        return Poll::Ready(Ok(None));
                    }
                    Ok(true) => {}
                }
            }

            // 3. Main merge-scan loop
            let outer_batch = self.outer_batch.as_ref().unwrap();
            let num_outer = outer_batch.num_rows();

            if self.outer_offset >= num_outer {
                self.emit_outer_batch()?;
                self.outer_batch = None;

                if let Some(batch) = self.coalescer.next_completed_batch() {
                    return Poll::Ready(Ok(Some(batch)));
                }
                continue;
            }

            let inner_batch = match &self.inner_batch {
                Some(b) => b,
                None => {
                    self.emit_outer_batch()?;
                    self.outer_batch = None;
                    continue;
                }
            };
            let num_inner = inner_batch.num_rows();

            if self.inner_offset >= num_inner {
                match ready!(self.poll_next_inner_batch(cx)) {
                    Err(e) => return Poll::Ready(Err(e)),
                    Ok(false) => {
                        self.inner_batch = None;
                        continue;
                    }
                    Ok(true) => continue,
                }
            }

            // 4. Compare keys at current positions
            let cmp = compare_join_arrays(
                &self.outer_key_arrays,
                self.outer_offset,
                &self.inner_key_arrays,
                self.inner_offset,
                &self.sort_options,
                self.null_equality,
            )?;

            match cmp {
                Ordering::Less => {
                    let group_end = find_key_group_end(
                        &self.outer_key_arrays,
                        self.outer_offset,
                        num_outer,
                        &self.sort_options,
                        self.null_equality,
                    )?;
                    self.outer_offset = group_end;
                }
                Ordering::Greater => {
                    let group_end = find_key_group_end(
                        &self.inner_key_arrays,
                        self.inner_offset,
                        num_inner,
                        &self.sort_options,
                        self.null_equality,
                    )?;
                    if group_end >= num_inner {
                        let saved_keys = self.inner_key_arrays.clone();
                        let saved_idx = num_inner - 1;
                        match ready!(self.poll_next_inner_batch(cx)) {
                            Err(e) => return Poll::Ready(Err(e)),
                            Ok(false) => {
                                self.inner_batch = None;
                                continue;
                            }
                            Ok(true) => {
                                if keys_match(
                                    &saved_keys,
                                    saved_idx,
                                    &self.inner_key_arrays,
                                    0,
                                    &self.sort_options,
                                    self.null_equality,
                                )? {
                                    match ready!(self.advance_inner_past_key_group(cx)) {
                                        Err(e) => return Poll::Ready(Err(e)),
                                        Ok(_) => continue,
                                    }
                                }
                                continue;
                            }
                        }
                    } else {
                        self.inner_offset = group_end;
                    }
                }
                Ordering::Equal => {
                    if self.filter.is_some() {
                        // Buffer inner key group (may span batches)
                        match ready!(self.buffer_inner_key_group(cx)) {
                            Err(e) => return Poll::Ready(Err(e)),
                            Ok(_inner_exhausted) => {}
                        }

                        // Process outer rows against buffered inner group
                        loop {
                            self.process_key_match_with_filter()?;

                            let outer_batch = self.outer_batch.as_ref().unwrap();
                            if self.outer_offset >= outer_batch.num_rows() {
                                self.emit_outer_batch()?;
                                self.boundary_state = BoundaryState::FilteredPending;

                                match ready!(self.poll_next_outer_batch(cx)) {
                                    Err(e) => return Poll::Ready(Err(e)),
                                    Ok(false) => {
                                        self.boundary_state = BoundaryState::Normal;
                                        self.outer_batch = None;
                                        break;
                                    }
                                    Ok(true) => {
                                        self.boundary_state = BoundaryState::Normal;
                                        if !self.inner_key_buffer.is_empty() {
                                            let first_inner = &self.inner_key_buffer[0];
                                            let inner_keys =
                                                evaluate_join_keys(first_inner, &self.on_inner)?;
                                            let same = keys_match(
                                                &self.outer_key_arrays,
                                                0,
                                                &inner_keys,
                                                0,
                                                &self.sort_options,
                                                self.null_equality,
                                            )?;
                                            if same {
                                                continue;
                                            }
                                        }
                                        break;
                                    }
                                }
                            } else {
                                break;
                            }
                        }

                        self.inner_key_buffer.clear();
                    } else {
                        // No filter: advance inner past key group, then
                        // mark all outer rows with this key as matched.
                        match ready!(self.advance_inner_past_key_group(cx)) {
                            Err(e) => return Poll::Ready(Err(e)),
                            Ok(_inner_exhausted) => {}
                        }

                        loop {
                            self.process_key_match_no_filter()?;

                            let num_outer = self.outer_batch.as_ref().unwrap().num_rows();
                            if self.outer_offset >= num_outer {
                                let saved_keys = self.outer_key_arrays.clone();
                                let saved_idx = num_outer - 1;

                                self.emit_outer_batch()?;
                                self.boundary_state = BoundaryState::NoFilterPending {
                                    saved_keys,
                                    saved_idx,
                                };

                                match ready!(self.poll_next_outer_batch(cx)) {
                                    Err(e) => return Poll::Ready(Err(e)),
                                    Ok(false) => {
                                        self.boundary_state = BoundaryState::Normal;
                                        self.outer_batch = None;
                                        break;
                                    }
                                    Ok(true) => {
                                        // Recover saved_keys from boundary state
                                        let BoundaryState::NoFilterPending {
                                            saved_keys,
                                            saved_idx,
                                        } = std::mem::replace(
                                            &mut self.boundary_state,
                                            BoundaryState::Normal,
                                        )
                                        else {
                                            unreachable!()
                                        };
                                        let same_key = keys_match(
                                            &saved_keys,
                                            saved_idx,
                                            &self.outer_key_arrays,
                                            0,
                                            &self.sort_options,
                                            self.null_equality,
                                        )?;
                                        if same_key {
                                            continue;
                                        }
                                        break;
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
            }

            // Check for completed coalescer batch
            if let Some(batch) = self.coalescer.next_completed_batch() {
                return Poll::Ready(Ok(Some(batch)));
            }
        }
    }
}

/// Compare two key rows for equality.
fn keys_match(
    left_arrays: &[ArrayRef],
    left_idx: usize,
    right_arrays: &[ArrayRef],
    right_idx: usize,
    sort_options: &[SortOptions],
    null_equality: NullEquality,
) -> Result<bool> {
    let cmp = compare_join_arrays(
        left_arrays,
        left_idx,
        right_arrays,
        right_idx,
        sort_options,
        null_equality,
    )?;
    Ok(cmp == Ordering::Equal)
}

/// Evaluate the join filter for one inner row against a slice of outer rows.
fn evaluate_filter_for_inner_row(
    outer_is_left: bool,
    filter: &JoinFilter,
    outer_slice: &RecordBatch,
    inner_batch: &RecordBatch,
    inner_idx: usize,
) -> Result<BooleanArray> {
    let num_outer_rows = outer_slice.num_rows();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(filter.column_indices().len());
    for col_idx in filter.column_indices() {
        let (side_batch, side_idx) = if outer_is_left {
            match col_idx.side {
                JoinSide::Left => (outer_slice, None),
                JoinSide::Right => (inner_batch, Some(inner_idx)),
                JoinSide::None => {
                    return internal_err!("Unexpected JoinSide::None in filter");
                }
            }
        } else {
            match col_idx.side {
                JoinSide::Left => (inner_batch, Some(inner_idx)),
                JoinSide::Right => (outer_slice, None),
                JoinSide::None => {
                    return internal_err!("Unexpected JoinSide::None in filter");
                }
            }
        };

        match side_idx {
            None => {
                columns.push(Arc::clone(side_batch.column(col_idx.index)));
            }
            Some(idx) => {
                let scalar =
                    ScalarValue::try_from_array(side_batch.column(col_idx.index).as_ref(), idx)?;
                columns.push(scalar.to_array_of_size(num_outer_rows)?);
            }
        }
    }

    let filter_batch = RecordBatch::try_new(Arc::clone(filter.schema()), columns)?;
    let result = filter
        .expression()
        .evaluate(&filter_batch)?
        .into_array(num_outer_rows)?;
    let bool_arr = result
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "Filter expression did not return BooleanArray".to_string(),
            )
        })?;
    // Treat nulls as false
    if bool_arr.null_count() > 0 {
        Ok(arrow::compute::prep_null_mask_filter(bool_arr))
    } else {
        Ok(bool_arr.clone())
    }
}

impl Stream for SemiAntiSortMergeJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_join(cx).map(|result| result.transpose());
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for SemiAntiSortMergeJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
