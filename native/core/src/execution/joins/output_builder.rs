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

//! Output batch builder for the sort merge join operator.
//!
//! The [`OutputBuilder`] accumulates matched and null-joined index pairs during
//! the join's Joining state and materializes them into Arrow [`RecordBatch`]es
//! during the OutputReady state.

use std::sync::Arc;

use arrow::array::{new_null_array, ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::kernels::concat::concat;
use arrow::compute::kernels::take::take;
use arrow::datatypes::SchemaRef;
use datafusion::common::{JoinType, Result};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::spill::SpillManager;

use super::buffered_batch::BufferedMatchGroup;

/// An index pair representing a matched row from the streamed and buffered sides.
#[derive(Debug, Clone, Copy)]
pub(super) struct JoinIndex {
    /// Row index in the current streamed batch.
    pub streamed_idx: usize,
    /// Index of the buffered batch within the match group.
    pub batch_idx: usize,
    /// Row index within the buffered batch.
    pub buffered_idx: usize,
}

/// Accumulates join output indices and materializes them into Arrow record batches.
///
/// During the join process, matched pairs and null-joined rows are recorded as
/// index tuples. When enough indices have been accumulated (reaching the target
/// batch size), they are materialized into a `RecordBatch` by gathering columns
/// from the streamed and buffered sides using Arrow's `take` kernel.
pub(super) struct OutputBuilder {
    /// Schema of the output record batch.
    output_schema: SchemaRef,
    /// Schema of the streamed (left) side.
    streamed_schema: SchemaRef,
    /// Schema of the buffered (right) side.
    buffered_schema: SchemaRef,
    /// The type of join being performed.
    join_type: JoinType,
    /// Target number of rows per output batch.
    target_batch_size: usize,
    /// Matched pairs: (streamed_idx, batch_idx, buffered_idx).
    indices: Vec<JoinIndex>,
    /// Streamed row indices that need a null buffered counterpart (left outer, left anti).
    streamed_null_joins: Vec<usize>,
    /// Buffered row indices that need a null streamed counterpart (full outer).
    /// Each entry is (batch_idx, row_idx).
    buffered_null_joins: Vec<(usize, usize)>,
}

impl OutputBuilder {
    /// Create a new `OutputBuilder`.
    pub fn new(
        output_schema: SchemaRef,
        streamed_schema: SchemaRef,
        buffered_schema: SchemaRef,
        join_type: JoinType,
        target_batch_size: usize,
    ) -> Self {
        Self {
            output_schema,
            streamed_schema,
            buffered_schema,
            join_type,
            target_batch_size,
            indices: Vec::new(),
            streamed_null_joins: Vec::new(),
            buffered_null_joins: Vec::new(),
        }
    }

    /// Record a matched pair between streamed and buffered rows.
    pub fn add_match(&mut self, streamed_idx: usize, batch_idx: usize, buffered_idx: usize) {
        self.indices.push(JoinIndex {
            streamed_idx,
            batch_idx,
            buffered_idx,
        });
    }

    /// Record a streamed row that needs a null buffered counterpart
    /// (used for outer joins and anti joins).
    pub fn add_streamed_null_join(&mut self, streamed_idx: usize) {
        self.streamed_null_joins.push(streamed_idx);
    }

    /// Record a buffered row that needs a null streamed counterpart
    /// (used for full outer joins).
    pub fn add_buffered_null_join(&mut self, batch_idx: usize, buffered_idx: usize) {
        self.buffered_null_joins.push((batch_idx, buffered_idx));
    }

    /// Return the total number of pending output rows.
    pub fn pending_count(&self) -> usize {
        self.indices.len() + self.streamed_null_joins.len() + self.buffered_null_joins.len()
    }

    /// Returns `true` if the pending row count has reached or exceeded the target batch size.
    pub fn should_flush(&self) -> bool {
        self.pending_count() >= self.target_batch_size
    }

    /// Returns `true` if there are any pending output rows.
    pub fn has_pending(&self) -> bool {
        self.pending_count() > 0
    }

    /// Materialize the accumulated indices into a [`RecordBatch`].
    ///
    /// For `LeftSemi` and `LeftAnti` joins, only streamed columns are included.
    /// For all other join types, columns from both sides are concatenated.
    ///
    /// After building, all accumulated indices are cleared.
    pub fn build(
        &mut self,
        streamed_batch: &RecordBatch,
        match_group: &BufferedMatchGroup,
        spill_manager: &SpillManager,
        _buffered_join_exprs: &[PhysicalExprRef],
    ) -> Result<RecordBatch> {
        let result = match self.join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => self.build_semi_anti(streamed_batch),
            _ => self.build_full(streamed_batch, match_group, spill_manager),
        };

        // Clear all accumulated indices after building
        self.indices.clear();
        self.streamed_null_joins.clear();
        self.buffered_null_joins.clear();

        result
    }

    /// Build output for LeftSemi/LeftAnti joins (streamed columns only).
    fn build_semi_anti(&self, streamed_batch: &RecordBatch) -> Result<RecordBatch> {
        // For semi/anti joins, we only output streamed rows from matched pairs
        // and streamed null joins. No buffered columns.
        let indices: Vec<u32> = self
            .indices
            .iter()
            .map(|idx| idx.streamed_idx as u32)
            .chain(self.streamed_null_joins.iter().map(|&idx| idx as u32))
            .collect();

        let indices_array = UInt32Array::from(indices);

        let columns: Vec<ArrayRef> = streamed_batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices_array, None).map_err(Into::into))
            .collect::<Result<_>>()?;

        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            columns,
        )?)
    }

    /// Build output for all other join types (both streamed and buffered columns).
    fn build_full(
        &self,
        streamed_batch: &RecordBatch,
        match_group: &BufferedMatchGroup,
        spill_manager: &SpillManager,
    ) -> Result<RecordBatch> {
        let streamed_columns = self.build_streamed_columns(streamed_batch)?;
        let buffered_columns =
            self.build_buffered_columns(match_group, spill_manager)?;

        let mut columns = streamed_columns;
        columns.extend(buffered_columns);

        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            columns,
        )?)
    }

    /// Build the streamed side columns by gathering rows using the `take` kernel.
    ///
    /// The order is: matched pairs, streamed null joins, then None for buffered null joins.
    fn build_streamed_columns(&self, streamed_batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        let total_rows = self.pending_count();
        let num_buffered_nulls = self.buffered_null_joins.len();

        // Build indices: matched streamed_idx, then streamed_null_join indices, then None
        let indices: Vec<Option<u32>> = self
            .indices
            .iter()
            .map(|idx| Some(idx.streamed_idx as u32))
            .chain(self.streamed_null_joins.iter().map(|&idx| Some(idx as u32)))
            .chain(std::iter::repeat_n(None, num_buffered_nulls))
            .collect();

        debug_assert_eq!(indices.len(), total_rows);

        let indices_array = UInt32Array::from(indices);

        streamed_batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices_array, None).map_err(Into::into))
            .collect()
    }

    /// Build the buffered side columns by gathering rows from buffered batches.
    ///
    /// For matched pairs: take from buffered batches (loading spilled ones as needed).
    /// For streamed null joins: null arrays.
    /// For buffered null joins: take from buffered batches.
    fn build_buffered_columns(
        &self,
        match_group: &BufferedMatchGroup,
        spill_manager: &SpillManager,
    ) -> Result<Vec<ArrayRef>> {
        let num_buffered_cols = self.buffered_schema.fields().len();
        let num_streamed_nulls = self.streamed_null_joins.len();

        // Build one column at a time
        (0..num_buffered_cols)
            .map(|col_idx| {
                self.build_single_buffered_column(
                    col_idx,
                    match_group,
                    spill_manager,
                    num_streamed_nulls,
                )
            })
            .collect()
    }

    /// Build a single buffered column by concatenating parts from matched pairs,
    /// null arrays for streamed null joins, and parts from buffered null joins.
    fn build_single_buffered_column(
        &self,
        col_idx: usize,
        match_group: &BufferedMatchGroup,
        spill_manager: &SpillManager,
        num_streamed_nulls: usize,
    ) -> Result<ArrayRef> {
        let data_type = self.buffered_schema.field(col_idx).data_type();
        let mut parts: Vec<ArrayRef> = Vec::new();

        // Part 1: Matched pairs — gather from buffered batches
        if !self.indices.is_empty() {
            let matched_part =
                self.take_buffered_matched(col_idx, match_group, spill_manager)?;
            parts.push(matched_part);
        }

        // Part 2: Streamed null joins — null arrays for buffered side
        if num_streamed_nulls > 0 {
            parts.push(new_null_array(data_type, num_streamed_nulls));
        }

        // Part 3: Buffered null joins — gather from buffered batches
        if !self.buffered_null_joins.is_empty() {
            let null_join_part =
                self.take_buffered_null_joins(col_idx, match_group, spill_manager)?;
            parts.push(null_join_part);
        }

        if parts.is_empty() {
            return Ok(new_null_array(data_type, 0));
        }

        if parts.len() == 1 {
            return Ok(parts.into_iter().next().unwrap());
        }

        let part_refs: Vec<&dyn arrow::array::Array> =
            parts.iter().map(|a| a.as_ref()).collect();
        Ok(concat(&part_refs)?)
    }

    /// Take values from buffered batches for matched pairs.
    fn take_buffered_matched(
        &self,
        col_idx: usize,
        match_group: &BufferedMatchGroup,
        spill_manager: &SpillManager,
    ) -> Result<ArrayRef> {
        // Group indices by batch_idx to minimize batch lookups
        // For simplicity, we build per-index and concatenate, but a more
        // optimized version would group by batch_idx.

        // Simple approach: gather one at a time using take with single-element indices
        // A more efficient approach groups by batch_idx, but this is correct and clear.
        let mut parts: Vec<ArrayRef> = Vec::new();

        // Group consecutive indices by batch_idx for efficiency
        let mut current_batch_idx: Option<usize> = None;
        let mut current_row_indices: Vec<u32> = Vec::new();

        for join_idx in &self.indices {
            if current_batch_idx == Some(join_idx.batch_idx) {
                current_row_indices.push(join_idx.buffered_idx as u32);
            } else {
                // Flush previous group
                if let Some(batch_idx) = current_batch_idx {
                    let batch =
                        match_group.batches[batch_idx].get_batch(spill_manager)?;
                    let col = batch.column(col_idx);
                    let row_indices = UInt32Array::from(std::mem::take(&mut current_row_indices));
                    parts.push(take(col.as_ref(), &row_indices, None)?);
                }
                current_batch_idx = Some(join_idx.batch_idx);
                current_row_indices.push(join_idx.buffered_idx as u32);
            }
        }

        // Flush last group
        if let Some(batch_idx) = current_batch_idx {
            let batch = match_group.batches[batch_idx].get_batch(spill_manager)?;
            let col = batch.column(col_idx);
            let row_indices = UInt32Array::from(current_row_indices);
            parts.push(take(col.as_ref(), &row_indices, None)?);
        }

        if parts.len() == 1 {
            return Ok(parts.into_iter().next().unwrap());
        }

        let part_refs: Vec<&dyn arrow::array::Array> =
            parts.iter().map(|a| a.as_ref()).collect();
        Ok(concat(&part_refs)?)
    }

    /// Take values from buffered batches for buffered null joins (full outer).
    fn take_buffered_null_joins(
        &self,
        col_idx: usize,
        match_group: &BufferedMatchGroup,
        spill_manager: &SpillManager,
    ) -> Result<ArrayRef> {
        let mut parts: Vec<ArrayRef> = Vec::new();

        // Group consecutive entries by batch_idx
        let mut current_batch_idx: Option<usize> = None;
        let mut current_row_indices: Vec<u32> = Vec::new();

        for &(batch_idx, row_idx) in &self.buffered_null_joins {
            if current_batch_idx == Some(batch_idx) {
                current_row_indices.push(row_idx as u32);
            } else {
                if let Some(bi) = current_batch_idx {
                    let batch = match_group.batches[bi].get_batch(spill_manager)?;
                    let col = batch.column(col_idx);
                    let row_indices = UInt32Array::from(std::mem::take(&mut current_row_indices));
                    parts.push(take(col.as_ref(), &row_indices, None)?);
                }
                current_batch_idx = Some(batch_idx);
                current_row_indices.push(row_idx as u32);
            }
        }

        if let Some(bi) = current_batch_idx {
            let batch = match_group.batches[bi].get_batch(spill_manager)?;
            let col = batch.column(col_idx);
            let row_indices = UInt32Array::from(current_row_indices);
            parts.push(take(col.as_ref(), &row_indices, None)?);
        }

        if parts.len() == 1 {
            return Ok(parts.into_iter().next().unwrap());
        }

        let part_refs: Vec<&dyn arrow::array::Array> =
            parts.iter().map(|a| a.as_ref()).collect();
        Ok(concat(&part_refs)?)
    }
}
