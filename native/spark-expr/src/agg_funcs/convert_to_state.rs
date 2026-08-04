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

use arrow::array::{ArrayRef, BooleanArray};
use datafusion::common::Result;
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};

/// Implements [`GroupsAccumulator::convert_to_state`] by treating every input row as
/// its own group.
///
/// DataFusion calls `convert_to_state` when the Partial phase of a multi-phase
/// aggregation is not reducing cardinality enough to be worth maintaining a hash
/// table, and it instead forwards intermediate state straight to the next phase.
///
/// `accumulator` must be a freshly created accumulator of the same shape as the one
/// being converted, so that the state columns produced here match those produced by
/// [`GroupsAccumulator::state`]. Row `i` of the input is accumulated into group `i`,
/// which also means null and filtered-out rows are skipped exactly as
/// [`GroupsAccumulator::update_batch`] skips them.
pub(super) fn convert_to_state_per_row<A: GroupsAccumulator>(
    mut accumulator: A,
    values: &[ArrayRef],
    opt_filter: Option<&BooleanArray>,
) -> Result<Vec<ArrayRef>> {
    let num_rows = values[0].len();
    let group_indices: Vec<usize> = (0..num_rows).collect();
    accumulator.update_batch(values, &group_indices, opt_filter, num_rows)?;
    accumulator.state(EmitTo::All)
}
