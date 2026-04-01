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

//! Streaming state machine for the sort merge join operator.
//!
//! The [`SortMergeJoinStream`] drives two sorted input streams (streamed and
//! buffered), compares join keys, collects matching buffered rows into a
//! [`BufferedMatchGroup`], and produces joined output batches via the
//! [`OutputBuilder`].

use std::cmp::Ordering;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::common::{NullEquality, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::utils::{compare_join_arrays, JoinFilter};
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};

use futures::{Stream, StreamExt};

use super::buffered_batch::{evaluate_join_keys, BufferedMatchGroup};
use super::filter::{apply_join_filter, build_filter_candidate_batch};
use super::metrics::SortMergeJoinMetrics;
use super::output_builder::{JoinIndex, OutputBuilder};

/// States of the sort merge join state machine.
#[derive(Debug, PartialEq, Eq)]
enum JoinState {
    /// Need to poll the next streamed row.
    PollStreamed,
    /// Need to poll the next buffered batch.
    PollBuffered,
    /// Initial state: decide what to poll next.
    Init,
    /// Compare the current streamed key with the current buffered key.
    Comparing,
    /// Collecting more buffered batches into the match group (key spans batches).
    CollectingBuffered,
    /// Produce join output for the current streamed row against the match group.
    Joining,
    /// Flush accumulated output.
    OutputReady,
    /// Drain unmatched rows after one side is exhausted.
    DrainUnmatched,
    /// No more output.
    Exhausted,
}

/// A streaming sort merge join that merges two sorted inputs by join keys.
pub(super) struct SortMergeJoinStream {
    /// The type of join (Inner, Left, Right, Full, LeftSemi, LeftAnti).
    join_type: JoinType,
    /// How nulls compare during key matching.
    null_equality: NullEquality,
    /// Optional post-join filter.
    join_filter: Option<JoinFilter>,
    /// Sort options for each join key column.
    sort_options: Vec<SortOptions>,

    /// The streamed (driving) input.
    streamed_input: SendableRecordBatchStream,
    /// The buffered (probe) input.
    buffered_input: SendableRecordBatchStream,
    /// Expressions to evaluate join keys on the streamed side.
    streamed_join_exprs: Vec<PhysicalExprRef>,
    /// Expressions to evaluate join keys on the buffered side.
    buffered_join_exprs: Vec<PhysicalExprRef>,

    /// Current streamed batch.
    streamed_batch: Option<RecordBatch>,
    /// Pre-evaluated join key arrays for the current streamed batch.
    streamed_join_arrays: Option<Vec<ArrayRef>>,
    /// Current row index within the streamed batch.
    streamed_idx: usize,
    /// Whether the streamed input is exhausted.
    streamed_exhausted: bool,

    /// Pending buffered batch (batch + join arrays) not yet consumed.
    buffered_pending: Option<(RecordBatch, Vec<ArrayRef>)>,
    /// Whether the buffered input is exhausted.
    buffered_exhausted: bool,
    /// The current match group of buffered rows sharing the same join key.
    match_group: BufferedMatchGroup,

    /// Converts join keys to comparable row format for key-reuse optimization.
    row_converter: RowConverter,
    /// Cached key of the previous streamed row (for key-reuse detection).
    cached_streamed_key: Option<OwnedRow>,

    /// Accumulates output indices and builds result batches.
    output_builder: OutputBuilder,
    /// Schema of the output record batches.
    output_schema: SchemaRef,

    /// Memory reservation for buffered data.
    reservation: MemoryReservation,
    /// Manages spilling buffered batches to disk when memory is tight.
    spill_manager: SpillManager,
    /// Metrics for this join operator.
    metrics: SortMergeJoinMetrics,

    /// Current state of the state machine.
    state: JoinState,
}

impl SortMergeJoinStream {
    /// Create a new `SortMergeJoinStream`.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        output_schema: SchemaRef,
        streamed_schema: SchemaRef,
        buffered_schema: SchemaRef,
        join_type: JoinType,
        null_equality: NullEquality,
        join_filter: Option<JoinFilter>,
        sort_options: Vec<SortOptions>,
        streamed_input: SendableRecordBatchStream,
        buffered_input: SendableRecordBatchStream,
        streamed_join_exprs: Vec<PhysicalExprRef>,
        buffered_join_exprs: Vec<PhysicalExprRef>,
        reservation: MemoryReservation,
        spill_manager: SpillManager,
        metrics: SortMergeJoinMetrics,
        target_batch_size: usize,
    ) -> Result<Self> {
        // Build SortFields from the streamed join key data types.
        let sort_fields: Vec<SortField> = streamed_join_exprs
            .iter()
            .zip(sort_options.iter())
            .map(|(expr, opts)| {
                let dt = expr.data_type(&streamed_schema)?;
                Ok(SortField::new_with_options(dt, *opts))
            })
            .collect::<Result<_>>()?;

        let row_converter = RowConverter::new(sort_fields)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))?;

        let output_builder = OutputBuilder::new(
            Arc::clone(&output_schema),
            streamed_schema,
            buffered_schema,
            join_type,
            target_batch_size,
        );

        Ok(Self {
            join_type,
            null_equality,
            join_filter,
            sort_options,
            streamed_input,
            buffered_input,
            streamed_join_exprs,
            buffered_join_exprs,
            streamed_batch: None,
            streamed_join_arrays: None,
            streamed_idx: 0,
            streamed_exhausted: false,
            buffered_pending: None,
            buffered_exhausted: false,
            match_group: BufferedMatchGroup::new(),
            row_converter,
            cached_streamed_key: None,
            output_builder,
            output_schema,
            reservation,
            spill_manager,
            metrics,
            state: JoinState::Init,
        })
    }

    /// Drive the state machine, returning `Poll::Ready(Some(batch))` when a
    /// batch is available, `Poll::Ready(None)` when done, or `Poll::Pending`
    /// if waiting on input.
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match self.state {
                JoinState::Init => {
                    // Decide what to poll based on current state.
                    if self.streamed_batch.is_none() && !self.streamed_exhausted {
                        self.state = JoinState::PollStreamed;
                    } else if self.buffered_pending.is_none() && !self.buffered_exhausted {
                        self.state = JoinState::PollBuffered;
                    } else if self.streamed_exhausted {
                        self.state = JoinState::DrainUnmatched;
                    } else {
                        // Have streamed data; compare regardless of buffered state.
                        self.state = JoinState::Comparing;
                    }
                }

                JoinState::PollStreamed => {
                    match self.streamed_input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            if batch.num_rows() == 0 {
                                // Skip empty batches.
                                continue;
                            }
                            self.metrics.input_batches.add(1);
                            self.metrics.input_rows.add(batch.num_rows());
                            let join_arrays =
                                evaluate_join_keys(&batch, &self.streamed_join_exprs)?;
                            self.streamed_batch = Some(batch);
                            self.streamed_join_arrays = Some(join_arrays);
                            self.streamed_idx = 0;
                            // Now ensure we have buffered data too.
                            if self.buffered_pending.is_none() && !self.buffered_exhausted {
                                self.state = JoinState::PollBuffered;
                            } else {
                                self.state = JoinState::Comparing;
                            }
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        Poll::Ready(None) => {
                            self.streamed_exhausted = true;
                            self.state = JoinState::DrainUnmatched;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }

                JoinState::PollBuffered => {
                    match self.buffered_input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            self.metrics.input_batches.add(1);
                            self.metrics.input_rows.add(batch.num_rows());
                            let join_arrays =
                                evaluate_join_keys(&batch, &self.buffered_join_exprs)?;
                            self.buffered_pending = Some((batch, join_arrays));
                            if self.streamed_batch.is_some() {
                                self.state = JoinState::Comparing;
                            } else if !self.streamed_exhausted {
                                self.state = JoinState::PollStreamed;
                            } else {
                                self.state = JoinState::DrainUnmatched;
                            }
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        Poll::Ready(None) => {
                            self.buffered_exhausted = true;
                            if self.streamed_batch.is_some() {
                                self.state = JoinState::Comparing;
                            } else {
                                self.state = JoinState::DrainUnmatched;
                            }
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }

                JoinState::Comparing => {
                    // We have a streamed row. Compare its key against the
                    // buffered key (first row of buffered_pending).
                    let streamed_idx = self.streamed_idx;

                    // Check if the streamed key has nulls.
                    let streamed_has_null = self
                        .streamed_join_arrays
                        .as_ref()
                        .unwrap()
                        .iter()
                        .any(|a| a.is_null(streamed_idx));

                    // For inner/semi joins, skip null keys entirely.
                    if streamed_has_null
                        && self.null_equality == NullEquality::NullEqualsNothing
                    {
                        match self.join_type {
                            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => {
                                self.advance_streamed()?;
                                self.determine_next_state();
                                continue;
                            }
                            JoinType::Left
                            | JoinType::Right
                            | JoinType::Full
                            | JoinType::LeftAnti
                            | JoinType::RightAnti => {
                                self.output_builder
                                    .add_streamed_null_join(streamed_idx);
                                self.advance_streamed()?;
                                if self.output_builder.should_flush() {
                                    self.state = JoinState::OutputReady;
                                } else {
                                    self.determine_next_state();
                                }
                                continue;
                            }
                            _ => {
                                self.advance_streamed()?;
                                self.determine_next_state();
                                continue;
                            }
                        }
                    }

                    // Check if the streamed key matches the cached key (reuse
                    // the existing match group).
                    if self.try_reuse_match_group()? {
                        self.state = JoinState::Joining;
                        continue;
                    }

                    // Clear old match group.
                    self.match_group.clear(&mut self.reservation);
                    self.cached_streamed_key = None;

                    if self.buffered_exhausted && self.buffered_pending.is_none() {
                        // No buffered data at all. Streamed row is unmatched.
                        self.emit_streamed_unmatched(streamed_idx);
                        self.advance_streamed()?;
                        if self.output_builder.should_flush() {
                            self.state = JoinState::OutputReady;
                        } else {
                            self.determine_next_state();
                        }
                        continue;
                    }

                    // Compare streamed key with buffered key.
                    let ordering = {
                        let streamed_arrays =
                            self.streamed_join_arrays.as_ref().unwrap();
                        let (_buffered_batch, buffered_arrays) =
                            self.buffered_pending.as_ref().unwrap();
                        compare_join_arrays(
                            streamed_arrays,
                            streamed_idx,
                            buffered_arrays,
                            0,
                            &self.sort_options,
                            self.null_equality,
                        )?
                    };

                    match ordering {
                        Ordering::Less => {
                            // Streamed key < buffered key: streamed row has no match.
                            self.emit_streamed_unmatched(streamed_idx);
                            self.advance_streamed()?;
                            if self.output_builder.should_flush() {
                                self.state = JoinState::OutputReady;
                            } else {
                                self.determine_next_state();
                            }
                        }
                        Ordering::Greater => {
                            // Streamed key > buffered key: advance buffered.
                            self.buffered_pending = None;
                            if self.buffered_exhausted {
                                self.emit_streamed_unmatched(streamed_idx);
                                self.advance_streamed()?;
                                if self.output_builder.should_flush() {
                                    self.state = JoinState::OutputReady;
                                } else {
                                    self.determine_next_state();
                                }
                            } else {
                                self.state = JoinState::PollBuffered;
                            }
                        }
                        Ordering::Equal => {
                            // Keys match. Build the match group.
                            let needs_more = self.build_match_group()?;
                            self.cache_streamed_key()?;
                            if needs_more {
                                self.state = JoinState::CollectingBuffered;
                            } else {
                                self.state = JoinState::Joining;
                            }
                        }
                    }
                }

                JoinState::CollectingBuffered => {
                    // We consumed an entire buffered batch into the match group
                    // and need to check if more buffered rows have the same key.
                    if self.buffered_exhausted {
                        // No more buffered data. Match group is complete.
                        self.state = JoinState::Joining;
                        continue;
                    }
                    match self.buffered_input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            self.metrics.input_batches.add(1);
                            self.metrics.input_rows.add(batch.num_rows());
                            let join_arrays =
                                evaluate_join_keys(&batch, &self.buffered_join_exprs)?;
                            self.buffered_pending = Some((batch, join_arrays));
                            let needs_more = self.build_match_group()?;
                            if needs_more {
                                // Still consuming; keep collecting.
                                continue;
                            }
                            self.state = JoinState::Joining;
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        Poll::Ready(None) => {
                            self.buffered_exhausted = true;
                            self.state = JoinState::Joining;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }

                JoinState::Joining => {
                    // Produce join pairs for the current streamed row against
                    // all rows in the match group.
                    let streamed_idx = self.streamed_idx;
                    self.produce_join_pairs(streamed_idx)?;

                    self.advance_streamed()?;

                    if self.output_builder.should_flush() {
                        self.state = JoinState::OutputReady;
                    } else {
                        self.determine_next_state();
                    }
                }

                JoinState::OutputReady => {
                    let batch = self.flush_output()?;
                    if batch.num_rows() > 0 {
                        self.metrics.output_rows.add(batch.num_rows());
                        self.metrics.output_batches.add(1);
                        // After flushing, figure out what to do next.
                        self.determine_next_state();
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    self.determine_next_state();
                }

                JoinState::DrainUnmatched => {
                    // Drain remaining streamed rows as null-joined (for outer/anti).
                    self.drain_remaining()?;

                    if self.output_builder.has_pending() {
                        let batch = self.flush_output()?;
                        self.state = JoinState::Exhausted;
                        if batch.num_rows() > 0 {
                            self.metrics.output_rows.add(batch.num_rows());
                            self.metrics.output_batches.add(1);
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }
                    self.state = JoinState::Exhausted;
                }

                JoinState::Exhausted => {
                    self.metrics
                        .update_peak_mem(self.reservation.size());
                    return Poll::Ready(None);
                }
            }
        }
    }

    /// Determine the next state after flushing output.
    fn determine_next_state(&mut self) {
        if self.streamed_batch.is_some()
            && self.streamed_idx < self.streamed_batch.as_ref().unwrap().num_rows()
        {
            // More rows in current streamed batch.
            self.state = JoinState::Comparing;
        } else if !self.streamed_exhausted {
            self.state = JoinState::Init;
        } else {
            self.state = JoinState::DrainUnmatched;
        }
    }

    /// Advance the streamed side to the next row. If the current batch is
    /// exhausted, clear it so we poll the next one.
    fn advance_streamed(&mut self) -> Result<()> {
        self.streamed_idx += 1;
        if let Some(batch) = &self.streamed_batch {
            if self.streamed_idx >= batch.num_rows() {
                self.streamed_batch = None;
                self.streamed_join_arrays = None;
                self.streamed_idx = 0;
            }
        }
        Ok(())
    }

    /// Try to reuse the existing match group if the current streamed key
    /// matches the cached key.
    fn try_reuse_match_group(&mut self) -> Result<bool> {
        if self.cached_streamed_key.is_none() || self.match_group.is_empty() {
            return Ok(false);
        }

        let streamed_arrays = self.streamed_join_arrays.as_ref().unwrap();
        let rows = self
            .row_converter
            .convert_columns(streamed_arrays)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))?;
        let current_key = rows.row(self.streamed_idx);

        if let Some(ref cached) = self.cached_streamed_key {
            if current_key == cached.row() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Cache the current streamed key as an OwnedRow.
    fn cache_streamed_key(&mut self) -> Result<()> {
        let streamed_arrays = self.streamed_join_arrays.as_ref().unwrap();
        let rows = self
            .row_converter
            .convert_columns(streamed_arrays)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))?;
        self.cached_streamed_key = Some(rows.row(self.streamed_idx).owned());
        Ok(())
    }

    /// Build a match group by collecting all buffered rows with the same key
    /// as the current streamed row.
    ///
    /// Returns `true` if the entire buffered batch was consumed and more data
    /// may need to be polled to complete the match group.
    fn build_match_group(&mut self) -> Result<bool> {
        let streamed_arrays = self.streamed_join_arrays.as_ref().unwrap();
        let streamed_idx = self.streamed_idx;
        let full_outer = matches!(self.join_type, JoinType::Full);

        // Take the pending buffered batch.
        let (batch, arrays) = self.buffered_pending.take().unwrap();

        // Find how many rows from this batch have the same key.
        let boundary = find_key_boundary(
            streamed_arrays,
            streamed_idx,
            &arrays,
            &self.sort_options,
            self.null_equality,
        )?;

        let needs_more = boundary == batch.num_rows();

        if needs_more {
            // Entire batch matches. Add it to the group.
            self.match_group.add_batch(
                batch,
                arrays,
                full_outer,
                &mut self.reservation,
                &self.spill_manager,
                &self.metrics,
            )?;
            // buffered_pending remains None; caller should poll more.
        } else {
            // Split the batch: rows [0..boundary) match, [boundary..) don't.
            if boundary > 0 {
                let matching = batch.slice(0, boundary);
                let matching_arrays: Vec<ArrayRef> =
                    arrays.iter().map(|a| a.slice(0, boundary)).collect();
                self.match_group.add_batch(
                    matching,
                    matching_arrays,
                    full_outer,
                    &mut self.reservation,
                    &self.spill_manager,
                    &self.metrics,
                )?;
            }

            // Keep the remaining rows as the new pending batch.
            let remaining = batch.slice(boundary, batch.num_rows() - boundary);
            let remaining_arrays: Vec<ArrayRef> = arrays
                .iter()
                .map(|a| a.slice(boundary, a.len() - boundary))
                .collect();
            self.buffered_pending = Some((remaining, remaining_arrays));
        }

        self.metrics.update_peak_mem(self.reservation.size());
        Ok(needs_more)
    }

    /// Emit a streamed row as unmatched (null-joined) for outer/anti joins,
    /// or skip it for inner/semi joins.
    fn emit_streamed_unmatched(&mut self, streamed_idx: usize) {
        match self.join_type {
            JoinType::Left | JoinType::Full | JoinType::LeftAnti | JoinType::RightAnti => {
                self.output_builder.add_streamed_null_join(streamed_idx);
            }
            // For Right outer: the streamed side is actually the right side,
            // so unmatched streamed rows need null-joining.
            JoinType::Right => {
                self.output_builder.add_streamed_null_join(streamed_idx);
            }
            // Inner and semi joins: unmatched rows are dropped.
            _ => {}
        }
    }

    /// Produce join pairs for the current streamed row against all rows in
    /// the match group, applying the join filter if present.
    fn produce_join_pairs(&mut self, streamed_idx: usize) -> Result<()> {
        if self.match_group.is_empty() {
            return Ok(());
        }

        match self.join_type {
            JoinType::LeftSemi | JoinType::RightSemi => {
                self.produce_semi_pairs(streamed_idx)?;
            }
            JoinType::LeftAnti | JoinType::RightAnti => {
                self.produce_anti_pairs(streamed_idx)?;
            }
            _ => {
                self.produce_standard_pairs(streamed_idx)?;
            }
        }
        Ok(())
    }

    /// Produce pairs for inner/outer joins.
    fn produce_standard_pairs(&mut self, streamed_idx: usize) -> Result<()> {
        if let Some(ref filter) = self.join_filter {
            // Build candidate pairs and apply filter.
            let mut pair_indices = Vec::new();
            for (batch_idx, buffered_batch) in self.match_group.batches.iter().enumerate() {
                for row_idx in 0..buffered_batch.num_rows {
                    pair_indices.push(JoinIndex {
                        streamed_idx,
                        batch_idx,
                        buffered_idx: row_idx,
                    });
                }
            }

            if pair_indices.is_empty() {
                self.emit_streamed_unmatched(streamed_idx);
                return Ok(());
            }

            // Build candidate batch for filter evaluation.
            let streamed_batch = self.streamed_batch.as_ref().unwrap();
            let candidate_batch = self.build_filter_batch(
                filter,
                streamed_batch,
                &pair_indices,
            )?;

            let filtered = apply_join_filter(
                filter,
                &candidate_batch,
                &pair_indices,
                &self.join_type,
            )?;

            // Apply filtered results.
            for idx in &filtered.passed_indices {
                self.output_builder
                    .add_match(idx.streamed_idx, idx.batch_idx, idx.buffered_idx);
            }
            for &si in &filtered.streamed_null_joins {
                self.output_builder.add_streamed_null_join(si);
            }
            // Mark matched buffered rows for full outer.
            for &(batch_idx, buffered_idx) in &filtered.buffered_matched {
                if let Some(ref mut matched) = self.match_group.batches[batch_idx].matched {
                    matched[buffered_idx] = true;
                }
            }
        } else {
            // No filter: all pairs match.
            for (batch_idx, buffered_batch) in self.match_group.batches.iter_mut().enumerate() {
                for row_idx in 0..buffered_batch.num_rows {
                    self.output_builder
                        .add_match(streamed_idx, batch_idx, row_idx);
                    if let Some(ref mut matched) = buffered_batch.matched {
                        matched[row_idx] = true;
                    }
                }
            }
        }
        Ok(())
    }

    /// Produce pairs for semi joins: emit the streamed row if any match passes.
    fn produce_semi_pairs(&mut self, streamed_idx: usize) -> Result<()> {
        if let Some(ref filter) = self.join_filter {
            let mut pair_indices = Vec::new();
            for (batch_idx, buffered_batch) in self.match_group.batches.iter().enumerate() {
                for row_idx in 0..buffered_batch.num_rows {
                    pair_indices.push(JoinIndex {
                        streamed_idx,
                        batch_idx,
                        buffered_idx: row_idx,
                    });
                }
            }

            if pair_indices.is_empty() {
                return Ok(());
            }

            let streamed_batch = self.streamed_batch.as_ref().unwrap();
            let candidate_batch =
                self.build_filter_batch(filter, streamed_batch, &pair_indices)?;

            let filtered = apply_join_filter(
                filter,
                &candidate_batch,
                &pair_indices,
                &self.join_type,
            )?;

            // Semi: emit the streamed row if any pair passed.
            if !filtered.passed_indices.is_empty() {
                let idx = &filtered.passed_indices[0];
                self.output_builder
                    .add_match(idx.streamed_idx, idx.batch_idx, idx.buffered_idx);
            }
        } else {
            // No filter: key match is sufficient for semi join.
            if !self.match_group.is_empty() {
                self.output_builder.add_match(streamed_idx, 0, 0);
            }
        }
        Ok(())
    }

    /// Produce pairs for anti joins: emit the streamed row if no match passes.
    fn produce_anti_pairs(&mut self, streamed_idx: usize) -> Result<()> {
        if let Some(ref filter) = self.join_filter {
            let mut pair_indices = Vec::new();
            for (batch_idx, buffered_batch) in self.match_group.batches.iter().enumerate() {
                for row_idx in 0..buffered_batch.num_rows {
                    pair_indices.push(JoinIndex {
                        streamed_idx,
                        batch_idx,
                        buffered_idx: row_idx,
                    });
                }
            }

            if pair_indices.is_empty() {
                // No buffered matches at all => emit for anti.
                self.output_builder.add_streamed_null_join(streamed_idx);
                return Ok(());
            }

            let streamed_batch = self.streamed_batch.as_ref().unwrap();
            let candidate_batch =
                self.build_filter_batch(filter, streamed_batch, &pair_indices)?;

            let filtered = apply_join_filter(
                filter,
                &candidate_batch,
                &pair_indices,
                &self.join_type,
            )?;

            // Anti: emit streamed rows that had no passing pair.
            for &si in &filtered.streamed_null_joins {
                self.output_builder.add_streamed_null_join(si);
            }
        } else {
            // No filter: key match means the streamed row is NOT emitted (anti).
            // Do nothing.
        }
        Ok(())
    }

    /// Build a filter candidate batch for the given pairs.
    fn build_filter_batch(
        &self,
        filter: &JoinFilter,
        streamed_batch: &RecordBatch,
        pair_indices: &[JoinIndex],
    ) -> Result<RecordBatch> {
        // We need to combine rows from potentially multiple buffered batches
        // into a single batch for filter evaluation.
        // First, build streamed and buffered index arrays.
        let streamed_indices: Vec<u32> = pair_indices
            .iter()
            .map(|idx| idx.streamed_idx as u32)
            .collect();
        let streamed_idx_array = UInt32Array::from(streamed_indices);

        // For the buffered side, we need to build a single batch containing
        // all referenced rows.
        let buffered_batch = self.collect_buffered_rows(pair_indices)?;
        let buffered_indices: Vec<u32> = (0..pair_indices.len() as u32).collect();
        let buffered_idx_array = UInt32Array::from(buffered_indices);

        build_filter_candidate_batch(
            filter,
            streamed_batch,
            &buffered_batch,
            &streamed_idx_array,
            &buffered_idx_array,
        )
    }

    /// Collect all referenced buffered rows into a single batch.
    fn collect_buffered_rows(
        &self,
        pair_indices: &[JoinIndex],
    ) -> Result<RecordBatch> {
        if pair_indices.is_empty() {
            // Return an empty batch with the correct schema.
            let schema = Arc::clone(self.spill_manager.schema());
            return Ok(RecordBatch::new_empty(schema));
        }

        // Group indices by batch_idx, then take rows and concatenate.
        let schema = Arc::clone(self.spill_manager.schema());
        let num_cols = schema.fields().len();
        let mut result_columns: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_cols];

        let mut current_batch_idx: Option<usize> = None;
        let mut current_row_indices: Vec<u32> = Vec::new();

        let flush = |batch_idx: usize,
                     row_indices: &[u32],
                     result_columns: &mut Vec<Vec<ArrayRef>>,
                     match_group: &BufferedMatchGroup,
                     spill_manager: &SpillManager|
         -> Result<()> {
            let batch = match_group.get_batch(batch_idx, spill_manager)?;
            let idx_array = UInt32Array::from(row_indices.to_vec());
            for (col_idx, col_parts) in result_columns.iter_mut().enumerate() {
                let col = batch.column(col_idx);
                let taken = arrow::compute::take(col.as_ref(), &idx_array, None)?;
                col_parts.push(taken);
            }
            Ok(())
        };

        for idx in pair_indices {
            if current_batch_idx == Some(idx.batch_idx) {
                current_row_indices.push(idx.buffered_idx as u32);
            } else {
                if let Some(bi) = current_batch_idx {
                    flush(
                        bi,
                        &current_row_indices,
                        &mut result_columns,
                        &self.match_group,
                        &self.spill_manager,
                    )?;
                    current_row_indices.clear();
                }
                current_batch_idx = Some(idx.batch_idx);
                current_row_indices.push(idx.buffered_idx as u32);
            }
        }
        if let Some(bi) = current_batch_idx {
            flush(
                bi,
                &current_row_indices,
                &mut result_columns,
                &self.match_group,
                &self.spill_manager,
            )?;
        }

        // Concatenate column parts.
        let columns: Vec<ArrayRef> = result_columns
            .into_iter()
            .map(|parts| {
                if parts.len() == 1 {
                    Ok(parts.into_iter().next().unwrap())
                } else {
                    let refs: Vec<&dyn arrow::array::Array> =
                        parts.iter().map(|a| a.as_ref()).collect();
                    Ok(arrow::compute::concat(&refs)?)
                }
            })
            .collect::<Result<_>>()?;

        Ok(RecordBatch::try_new(schema, columns)?)
    }

    /// Flush accumulated output indices into a RecordBatch.
    fn flush_output(&mut self) -> Result<RecordBatch> {
        let streamed_batch = match &self.streamed_batch {
            Some(b) => b.clone(),
            None => {
                // If the streamed batch has been consumed, we might still
                // have pending output from DrainUnmatched (buffered null joins).
                // Create an empty streamed batch.
                let schema = self.output_builder_streamed_schema();
                RecordBatch::new_empty(schema)
            }
        };

        self.output_builder.build(
            &streamed_batch,
            &self.match_group,
            &self.spill_manager,
            &self.buffered_join_exprs,
        )
    }

    /// Get the streamed schema from the output builder (needed for empty batch creation).
    fn output_builder_streamed_schema(&self) -> SchemaRef {
        // We can derive this from the output schema and join type,
        // but for simplicity use the streamed input's schema.
        self.streamed_input.schema()
    }

    /// Drain remaining rows after one side is exhausted.
    fn drain_remaining(&mut self) -> Result<()> {
        match self.join_type {
            JoinType::Left | JoinType::Full | JoinType::LeftAnti | JoinType::RightAnti => {
                // Drain remaining streamed rows as null-joined.
                if let Some(batch) = &self.streamed_batch {
                    let num_rows = batch.num_rows();
                    for idx in self.streamed_idx..num_rows {
                        self.output_builder.add_streamed_null_join(idx);
                    }
                }
            }
            JoinType::Right => {
                // Right outer: streamed side is right, so drain remaining.
                if let Some(batch) = &self.streamed_batch {
                    let num_rows = batch.num_rows();
                    for idx in self.streamed_idx..num_rows {
                        self.output_builder.add_streamed_null_join(idx);
                    }
                }
            }
            _ => {}
        }

        // For full outer: drain unmatched buffered rows.
        if matches!(self.join_type, JoinType::Full) {
            for (batch_idx, buffered_batch) in self.match_group.batches.iter().enumerate() {
                if let Some(ref matched) = buffered_batch.matched {
                    for (row_idx, &was_matched) in matched.iter().enumerate() {
                        if !was_matched {
                            self.output_builder
                                .add_buffered_null_join(batch_idx, row_idx);
                        }
                    }
                }
            }
        }

        // Clear streamed state.
        self.streamed_batch = None;
        self.streamed_join_arrays = None;
        self.streamed_idx = 0;

        Ok(())
    }
}

/// Find the boundary index in a buffered batch where the key changes relative
/// to the streamed key. Returns the number of rows from the start that have
/// the same key as the streamed row at `streamed_idx`.
///
/// Uses binary search since the buffered side is sorted.
fn find_key_boundary(
    streamed_arrays: &[ArrayRef],
    streamed_idx: usize,
    buffered_arrays: &[ArrayRef],
    sort_options: &[SortOptions],
    null_equality: NullEquality,
) -> Result<usize> {
    let num_rows = buffered_arrays[0].len();
    if num_rows == 0 {
        return Ok(0);
    }

    // Quick check: if the last row also matches, the entire batch is in the group.
    let last_cmp = compare_join_arrays(
        streamed_arrays,
        streamed_idx,
        buffered_arrays,
        num_rows - 1,
        sort_options,
        null_equality,
    )?;
    if last_cmp == Ordering::Equal {
        return Ok(num_rows);
    }

    // Binary search for the boundary.
    let mut lo = 0usize;
    let mut hi = num_rows;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cmp = compare_join_arrays(
            streamed_arrays,
            streamed_idx,
            buffered_arrays,
            mid,
            sort_options,
            null_equality,
        )?;
        if cmp == Ordering::Equal {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

impl Stream for SortMergeJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let join_time = self.metrics.join_time.clone();
        let timer = join_time.timer();
        let result = self.poll_next_inner(cx);
        timer.done();
        result
    }
}

impl RecordBatchStream for SortMergeJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}
