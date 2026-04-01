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

//! Join filter evaluation with corrected masks for outer, semi, and anti joins.
//!
//! In outer joins, if all candidate pairs for a streamed row fail the filter,
//! the streamed row must be null-joined (not dropped). Semi joins emit a
//! streamed row if ANY pair passes. Anti joins emit if NO pair passes. This
//! module groups filter results by streamed row to implement these semantics.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt32Array};
use arrow::compute::take;
use datafusion::common::{internal_err, JoinSide, Result};
use datafusion::logical_expr::JoinType;
use datafusion::physical_plan::joins::utils::JoinFilter;

use super::output_builder::JoinIndex;

/// Result of applying a join filter to a set of candidate pairs.
pub(super) struct FilteredOutput {
    /// Pairs that passed the filter (or were selected for null-join in outer).
    pub passed_indices: Vec<JoinIndex>,
    /// Streamed row indices that had no passing pair and should be null-joined
    /// (applies to outer and anti joins).
    pub streamed_null_joins: Vec<usize>,
    /// (batch_idx, buffered_idx) pairs that passed the filter, used for
    /// tracking matched buffered rows in full outer joins.
    pub buffered_matched: Vec<(usize, usize)>,
}

/// Evaluate a join filter on candidate pairs and return corrected results
/// based on the join type.
///
/// `pair_indices` contains the candidate pairs as `JoinIndex` values.
/// `candidate_batch` is the intermediate batch built for filter evaluation.
pub(super) fn apply_join_filter(
    filter: &JoinFilter,
    candidate_batch: &RecordBatch,
    pair_indices: &[JoinIndex],
    join_type: &JoinType,
) -> Result<FilteredOutput> {
    // Evaluate the filter expression on the candidate batch
    let filter_result = filter
        .expression()
        .evaluate(candidate_batch)?
        .into_array(candidate_batch.num_rows())?;

    let mask = filter_result
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("join filter expression must return BooleanArray");

    match join_type {
        JoinType::Inner => Ok(apply_inner_filter(mask, pair_indices)),
        JoinType::Left | JoinType::Right => Ok(apply_outer_filter(mask, pair_indices)),
        JoinType::Full => Ok(apply_full_outer_filter(mask, pair_indices)),
        JoinType::LeftSemi | JoinType::RightSemi => Ok(apply_semi_filter(mask, pair_indices)),
        JoinType::LeftAnti | JoinType::RightAnti => Ok(apply_anti_filter(mask, pair_indices)),
        _ => Ok(apply_inner_filter(mask, pair_indices)),
    }
}

/// Build the intermediate batch used for filter evaluation.
///
/// For each column in the filter's `column_indices`, we take the appropriate
/// rows from either the streamed or buffered batch using the provided index
/// arrays.
pub(super) fn build_filter_candidate_batch(
    filter: &JoinFilter,
    streamed_batch: &RecordBatch,
    buffered_batch: &RecordBatch,
    streamed_indices: &UInt32Array,
    buffered_indices: &UInt32Array,
) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = filter
        .column_indices()
        .iter()
        .map(|col_idx| {
            let (batch, indices) = match col_idx.side {
                JoinSide::Left => (streamed_batch, streamed_indices),
                JoinSide::Right => (buffered_batch, buffered_indices),
            };
            let column = batch.column(col_idx.index);
            Ok(take(column.as_ref(), indices, None)?)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_new(
        Arc::clone(filter.schema()),
        columns,
    )?)
}

/// Returns true if the mask value at `i` is true and not null.
#[inline]
fn mask_passed(mask: &BooleanArray, i: usize) -> bool {
    mask.value(i) && !mask.is_null(i)
}

/// Inner join: keep rows where mask is true (and not null).
fn apply_inner_filter(mask: &BooleanArray, indices: &[JoinIndex]) -> FilteredOutput {
    let passed_indices: Vec<JoinIndex> = indices
        .iter()
        .enumerate()
        .filter(|(i, _)| mask_passed(mask, *i))
        .map(|(_, idx)| *idx)
        .collect();

    let buffered_matched = passed_indices
        .iter()
        .map(|idx| (idx.batch_idx, idx.buffered_idx))
        .collect();

    FilteredOutput {
        passed_indices,
        streamed_null_joins: Vec::new(),
        buffered_matched,
    }
}

/// Outer join (Left/Right): group by streamed_idx. If any pair passes for a
/// streamed row, keep those passing pairs. If none pass, add streamed row to
/// null-joins.
fn apply_outer_filter(mask: &BooleanArray, indices: &[JoinIndex]) -> FilteredOutput {
    let mut groups: HashMap<usize, Vec<(usize, JoinIndex)>> = HashMap::new();
    for (i, idx) in indices.iter().enumerate() {
        groups.entry(idx.streamed_idx).or_default().push((i, *idx));
    }

    let mut passed_indices = Vec::new();
    let mut streamed_null_joins = Vec::new();
    let mut buffered_matched = Vec::new();

    for (streamed_idx, pairs) in &groups {
        let passing: Vec<JoinIndex> = pairs
            .iter()
            .filter(|(i, _)| mask_passed(mask, *i))
            .map(|(_, idx)| *idx)
            .collect();

        if passing.is_empty() {
            streamed_null_joins.push(*streamed_idx);
        } else {
            for idx in &passing {
                buffered_matched.push((idx.batch_idx, idx.buffered_idx));
            }
            passed_indices.extend(passing);
        }
    }

    FilteredOutput {
        passed_indices,
        streamed_null_joins,
        buffered_matched,
    }
}

/// Full outer join: same grouping logic as outer, but buffered tracking is
/// done via the matched bitvector on BufferedBatch (caller uses
/// `buffered_matched`).
fn apply_full_outer_filter(mask: &BooleanArray, indices: &[JoinIndex]) -> FilteredOutput {
    // Same logic as outer — the caller handles buffered-side null joins
    // via the BufferedBatch matched bitvector.
    apply_outer_filter(mask, indices)
}

/// Semi join: group by streamed_idx. If any pair passes for a streamed row,
/// emit one JoinIndex for that row (the first passing pair).
fn apply_semi_filter(mask: &BooleanArray, indices: &[JoinIndex]) -> FilteredOutput {
    let mut groups: HashMap<usize, Vec<(usize, JoinIndex)>> = HashMap::new();
    for (i, idx) in indices.iter().enumerate() {
        groups.entry(idx.streamed_idx).or_default().push((i, *idx));
    }

    let mut passed_indices = Vec::new();
    let mut buffered_matched = Vec::new();

    for (_streamed_idx, pairs) in &groups {
        if let Some((_, idx)) = pairs.iter().find(|(i, _)| mask_passed(mask, *i)) {
            passed_indices.push(*idx);
            buffered_matched.push((idx.batch_idx, idx.buffered_idx));
        }
    }

    FilteredOutput {
        passed_indices,
        streamed_null_joins: Vec::new(),
        buffered_matched,
    }
}

/// Anti join: group by streamed_idx. If no pair passes for a streamed row,
/// add it to streamed_null_joins.
fn apply_anti_filter(mask: &BooleanArray, indices: &[JoinIndex]) -> FilteredOutput {
    let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
    for (i, idx) in indices.iter().enumerate() {
        groups.entry(idx.streamed_idx).or_default().push(i);
    }

    let mut streamed_null_joins = Vec::new();

    for (streamed_idx, mask_indices) in &groups {
        let any_passed = mask_indices.iter().any(|i| mask_passed(mask, *i));

        if !any_passed {
            streamed_null_joins.push(*streamed_idx);
        }
    }

    FilteredOutput {
        passed_indices: Vec::new(),
        streamed_null_joins,
        buffered_matched: Vec::new(),
    }
}
