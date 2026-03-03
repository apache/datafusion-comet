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

use arrow::array::RecordBatch;
use arrow::compute::interleave_record_batch;
use datafusion::common::DataFusionError;

/// A helper struct to produce shuffled batches.
/// This struct takes ownership of the buffered batches and partition indices from the
/// ShuffleRepartitioner, and provides an iterator over the batches in the specified partitions.
pub(super) struct PartitionedBatchesProducer {
    buffered_batches: Vec<RecordBatch>,
    partition_indices: Vec<Vec<(u32, u32)>>,
    batch_size: usize,
}

impl PartitionedBatchesProducer {
    pub(super) fn new(
        buffered_batches: Vec<RecordBatch>,
        indices: Vec<Vec<(u32, u32)>>,
        batch_size: usize,
    ) -> Self {
        Self {
            partition_indices: indices,
            buffered_batches,
            batch_size,
        }
    }

    pub(super) fn produce(&mut self, partition_id: usize) -> PartitionedBatchIterator<'_> {
        PartitionedBatchIterator::new(
            &self.partition_indices[partition_id],
            &self.buffered_batches,
            self.batch_size,
        )
    }
}

pub(crate) struct PartitionedBatchIterator<'a> {
    record_batches: Vec<&'a RecordBatch>,
    batch_size: usize,
    indices: Vec<(usize, usize)>,
    pos: usize,
}

impl<'a> PartitionedBatchIterator<'a> {
    fn new(
        indices: &'a [(u32, u32)],
        buffered_batches: &'a [RecordBatch],
        batch_size: usize,
    ) -> Self {
        if indices.is_empty() {
            // Avoid unnecessary allocations when the partition is empty
            return Self {
                record_batches: vec![],
                batch_size,
                indices: vec![],
                pos: 0,
            };
        }
        let record_batches = buffered_batches.iter().collect::<Vec<_>>();
        let current_indices = indices
            .iter()
            .map(|(i_batch, i_row)| (*i_batch as usize, *i_row as usize))
            .collect::<Vec<_>>();
        Self {
            record_batches,
            batch_size,
            indices: current_indices,
            pos: 0,
        }
    }
}

impl Iterator for PartitionedBatchIterator<'_> {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.indices.len() {
            return None;
        }

        let indices_end = std::cmp::min(self.pos + self.batch_size, self.indices.len());
        let indices = &self.indices[self.pos..indices_end];
        match interleave_record_batch(&self.record_batches, indices) {
            Ok(batch) => {
                self.pos = indices_end;
                Some(Ok(batch))
            }
            Err(e) => Some(Err(DataFusionError::ArrowError(
                Box::from(e),
                Some(DataFusionError::get_back_trace()),
            ))),
        }
    }
}
