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
    pub streamed_idx: usize,
    pub batch_idx: usize,
    pub buffered_idx: usize,
}

/// Accumulates join output indices and materializes them into Arrow record batches.
pub(super) struct OutputBuilder {
    output_schema: SchemaRef,
    buffered_schema: SchemaRef,
    join_type: JoinType,
    target_batch_size: usize,
    indices: Vec<JoinIndex>,
    streamed_null_joins: Vec<usize>,
    buffered_null_joins: Vec<(usize, usize)>,
}

impl OutputBuilder {
    pub fn new(
        output_schema: SchemaRef,
        _streamed_schema: SchemaRef,
        buffered_schema: SchemaRef,
        join_type: JoinType,
        target_batch_size: usize,
    ) -> Self {
        Self {
            output_schema,
            buffered_schema,
            join_type,
            target_batch_size,
            indices: Vec::new(),
            streamed_null_joins: Vec::new(),
            buffered_null_joins: Vec::new(),
        }
    }

    pub fn add_match(&mut self, streamed_idx: usize, batch_idx: usize, buffered_idx: usize) {
        self.indices.push(JoinIndex {
            streamed_idx,
            batch_idx,
            buffered_idx,
        });
    }

    pub fn add_streamed_null_join(&mut self, streamed_idx: usize) {
        self.streamed_null_joins.push(streamed_idx);
    }

    pub fn add_buffered_null_join(&mut self, batch_idx: usize, buffered_idx: usize) {
        self.buffered_null_joins.push((batch_idx, buffered_idx));
    }

    pub fn pending_count(&self) -> usize {
        self.indices.len() + self.streamed_null_joins.len() + self.buffered_null_joins.len()
    }

    pub fn should_flush(&self) -> bool {
        self.pending_count() >= self.target_batch_size
    }

    pub fn has_pending(&self) -> bool {
        self.pending_count() > 0
    }

    /// Materialize the accumulated indices into a [`RecordBatch`].
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

        self.indices.clear();
        self.streamed_null_joins.clear();
        self.buffered_null_joins.clear();

        result
    }

    fn build_semi_anti(&self, streamed_batch: &RecordBatch) -> Result<RecordBatch> {
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

    fn build_full(
        &self,
        streamed_batch: &RecordBatch,
        match_group: &BufferedMatchGroup,
        spill_manager: &SpillManager,
    ) -> Result<RecordBatch> {
        let streamed_columns = self.build_streamed_columns(streamed_batch)?;
        let buffered_columns = self.build_buffered_columns(match_group, spill_manager)?;

        let mut columns = streamed_columns;
        columns.extend(buffered_columns);

        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            columns,
        )?)
    }

    fn build_streamed_columns(&self, streamed_batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        let total_rows = self.pending_count();
        let num_buffered_nulls = self.buffered_null_joins.len();

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

    /// Build all buffered columns at once, loading each batch only once across all columns.
    fn build_buffered_columns(
        &self,
        match_group: &BufferedMatchGroup,
        spill_manager: &SpillManager,
    ) -> Result<Vec<ArrayRef>> {
        let num_cols = self.buffered_schema.fields().len();
        let num_streamed_nulls = self.streamed_null_joins.len();

        // Pre-compute which batches we need and their grouped row indices.
        // This avoids loading the same spilled batch N times (once per column).
        let matched_groups = group_by_batch(&self.indices);
        let null_join_groups = group_by_batch_tuple(&self.buffered_null_joins);

        // Load each referenced batch once
        let mut batch_cache: std::collections::HashMap<usize, RecordBatch> =
            std::collections::HashMap::new();
        for &(batch_idx, _) in matched_groups.iter().chain(null_join_groups.iter()) {
            if let std::collections::hash_map::Entry::Vacant(e) = batch_cache.entry(batch_idx) {
                e.insert(match_group.batches[batch_idx].get_batch(spill_manager)?);
            }
        }

        // Build all columns using the cached batches
        let mut result: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let data_type = self.buffered_schema.field(col_idx).data_type();
            let mut parts: Vec<ArrayRef> = Vec::with_capacity(3);

            // Matched pairs
            if !matched_groups.is_empty() {
                parts.push(take_from_groups(col_idx, &matched_groups, &batch_cache)?);
            }

            // Null arrays for streamed null joins
            if num_streamed_nulls > 0 {
                parts.push(new_null_array(data_type, num_streamed_nulls));
            }

            // Buffered null joins
            if !null_join_groups.is_empty() {
                parts.push(take_from_groups(col_idx, &null_join_groups, &batch_cache)?);
            }

            result.push(concat_parts(parts, data_type)?);
        }

        Ok(result)
    }
}

/// Group JoinIndex entries by batch_idx into (batch_idx, row_indices) pairs.
fn group_by_batch(indices: &[JoinIndex]) -> Vec<(usize, Vec<u32>)> {
    let mut groups: Vec<(usize, Vec<u32>)> = Vec::new();
    for idx in indices {
        if let Some(last) = groups.last_mut() {
            if last.0 == idx.batch_idx {
                last.1.push(idx.buffered_idx as u32);
                continue;
            }
        }
        groups.push((idx.batch_idx, vec![idx.buffered_idx as u32]));
    }
    groups
}

/// Group (batch_idx, row_idx) tuples by batch_idx into (batch_idx, row_indices) pairs.
fn group_by_batch_tuple(indices: &[(usize, usize)]) -> Vec<(usize, Vec<u32>)> {
    let mut groups: Vec<(usize, Vec<u32>)> = Vec::new();
    for &(batch_idx, row_idx) in indices {
        if let Some(last) = groups.last_mut() {
            if last.0 == batch_idx {
                last.1.push(row_idx as u32);
                continue;
            }
        }
        groups.push((batch_idx, vec![row_idx as u32]));
    }
    groups
}

/// Take a single column from pre-loaded batches using grouped indices.
fn take_from_groups(
    col_idx: usize,
    groups: &[(usize, Vec<u32>)],
    batch_cache: &std::collections::HashMap<usize, RecordBatch>,
) -> Result<ArrayRef> {
    let mut parts: Vec<ArrayRef> = Vec::with_capacity(groups.len());
    for (batch_idx, row_indices) in groups {
        let batch = &batch_cache[batch_idx];
        let col = batch.column(col_idx);
        let index_array = UInt32Array::from(row_indices.clone());
        parts.push(take(col.as_ref(), &index_array, None)?);
    }
    concat_parts(
        parts,
        batch_cache
            .values()
            .next()
            .unwrap()
            .column(col_idx)
            .data_type(),
    )
}

/// Concat array parts, handling empty and single-element cases.
fn concat_parts(parts: Vec<ArrayRef>, data_type: &arrow::datatypes::DataType) -> Result<ArrayRef> {
    if parts.is_empty() {
        return Ok(new_null_array(data_type, 0));
    }
    if parts.len() == 1 {
        return Ok(parts.into_iter().next().expect("checked len == 1"));
    }
    let refs: Vec<&dyn arrow::array::Array> = parts.iter().map(|a| a.as_ref()).collect();
    Ok(concat(&refs)?)
}
