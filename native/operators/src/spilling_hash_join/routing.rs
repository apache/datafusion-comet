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

//! Routing rows of [`super::SpillingHashJoinExec`] to buckets by a hash of their join keys.

use arrow::array::{Array, ArrayRef, RecordBatch, UInt32Array};
use arrow::buffer::NullBuffer;
use arrow::compute::take_record_batch;
use datafusion::common::hash_utils::{create_hashes, QualityRandomState};
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_expr_common::utils::evaluate_expressions_to_arrays;

/// Each level splits a bucket on the next `BUCKET_BITS` bits of the same hash, from the top.
pub(super) const BUCKET_BITS: u32 = 4;
pub(super) const NUM_BUCKETS: usize = 1 << BUCKET_BITS;
/// The deepest level a bucket that does not fit is split to before it is joined in chunks.
pub(super) const MAX_LEVEL: u32 = 3;
/// The bucket of a row with a null join key, which matches nothing.
pub(super) const NULL_KEY: u8 = u8::MAX;

/// The hash that routes rows. Its seed differs from `HashJoinExec`'s, so that the rows of one
/// bucket do not crowd a part of an inner join's table.
const ROUTING_STATE: QualityRandomState = QualityRandomState::with_seed(0x2545_9e37_79b9_7f4a);

/// Routes the rows of batches on one side of the join.
pub(super) struct Router {
    keys: Vec<PhysicalExprRef>,
    hashes: Vec<u64>,
}

impl Router {
    pub(super) fn new(keys: Vec<PhysicalExprRef>) -> Self {
        Self {
            keys,
            hashes: Vec::new(),
        }
    }

    fn bucket_of(hash: u64, level: u32) -> u8 {
        let shift = 64 - BUCKET_BITS * (level + 1);
        ((hash >> shift) as usize & (NUM_BUCKETS - 1)) as u8
    }

    /// The routing hash of each row of `batch`.
    pub(super) fn hash(&mut self, batch: &RecordBatch) -> Result<&[u64]> {
        self.hash_keys(batch)?;
        Ok(&self.hashes)
    }

    /// The routing hash of each row of the batch last hashed or routed.
    pub(super) fn hashes(&self) -> &[u64] {
        &self.hashes
    }

    fn hash_keys(&mut self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        let keys = evaluate_expressions_to_arrays(&self.keys, batch)?;
        self.hashes.clear();
        self.hashes.resize(batch.num_rows(), 0);
        create_hashes(&keys, &ROUTING_STATE, &mut self.hashes)?;
        Ok(keys)
    }

    /// The bucket of each row of `batch` at `level`. With `separate_null_keys`, a row with a
    /// null in any join key gets [`NULL_KEY`] instead.
    pub(super) fn route(
        &mut self,
        batch: &RecordBatch,
        level: u32,
        separate_null_keys: bool,
    ) -> Result<Vec<u8>> {
        let keys = self.hash_keys(batch)?;
        let nulls = if separate_null_keys {
            keys.iter().fold(None, |nulls: Option<NullBuffer>, key| {
                NullBuffer::union(nulls.as_ref(), key.logical_nulls().as_ref())
            })
        } else {
            None
        };
        Ok(self
            .hashes
            .iter()
            .enumerate()
            .map(|(row, hash)| match &nulls {
                Some(nulls) if nulls.is_null(row) => NULL_KEY,
                _ => Self::bucket_of(*hash, level),
            })
            .collect())
    }
}

/// The row indices of each bucket, in row order.
pub(super) fn rows_by_bucket(buckets: &[u8]) -> Vec<Vec<u32>> {
    let mut rows = vec![Vec::new(); NUM_BUCKETS];
    for (row, bucket) in buckets.iter().enumerate() {
        if *bucket != NULL_KEY {
            rows[*bucket as usize].push(row as u32);
        }
    }
    rows
}

/// The rows of `batch` at `indices`, which are in row order. Returns the batch itself when the
/// indices are all of its rows.
pub(super) fn take_rows(batch: &RecordBatch, indices: Vec<u32>) -> Result<RecordBatch> {
    if indices.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    Ok(take_record_batch(batch, &UInt32Array::from(indices))?)
}
