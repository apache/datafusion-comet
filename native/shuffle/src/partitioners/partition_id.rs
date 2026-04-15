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

//! Shared partition ID computation used by both immediate and buffered shuffle modes.

use crate::comet_partitioning;
use arrow::row::Rows;

/// Assign partition IDs from pre-computed hash values using Spark-compatible pmod.
pub(crate) fn assign_hash_partition_ids(
    hashes: &[u32],
    partition_ids: &mut [u32],
    num_partitions: usize,
) {
    for (idx, hash) in hashes.iter().enumerate() {
        partition_ids[idx] = comet_partitioning::pmod(*hash, num_partitions) as u32;
    }
}

/// Assign partition IDs using binary search on range boundaries.
pub(crate) fn assign_range_partition_ids(
    rows: &Rows,
    partition_ids: &mut [u32],
    bounds: &[arrow::row::OwnedRow],
) {
    for (row_idx, row) in rows.iter().enumerate() {
        partition_ids[row_idx] = bounds.as_ref().partition_point(|bound| bound.row() <= row) as u32;
    }
}
