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
use datafusion::physical_plan::metrics::Time;

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

    /// References to all buffered batches. Build this once per write cycle and share it
    /// across every partition's [`Self::produce`] call instead of rebuilding a fresh
    /// `Vec<&RecordBatch>` over all buffered batches for each partition.
    pub(super) fn batch_refs(&self) -> Vec<&RecordBatch> {
        self.buffered_batches.iter().collect()
    }

    pub(super) fn produce<'a>(
        &'a self,
        refs: &'a [&'a RecordBatch],
        partition_id: usize,
        interleave_time: &'a Time,
    ) -> PartitionedBatchIterator<'a> {
        // Partition indices index into `buffered_batches`; a refs slice built from a
        // different producer would silently interleave wrong rows.
        debug_assert_eq!(
            refs.len(),
            self.buffered_batches.len(),
            "refs slice must cover every buffered batch"
        );
        PartitionedBatchIterator::new(
            &self.partition_indices[partition_id],
            refs,
            self.batch_size,
            interleave_time,
        )
    }
}

/// Iterates over the shuffled record batches belonging to a single output partition.
pub(crate) struct PartitionedBatchIterator<'a> {
    record_batches: &'a [&'a RecordBatch],
    batch_size: usize,
    indices: &'a [(u32, u32)],
    /// Scratch for the current chunk's indices widened to what `interleave_record_batch`
    /// expects. Reused across chunks so each partition costs one small allocation
    /// (capacity at most `batch_size`) rather than re-materializing its whole index list.
    chunk_scratch: Vec<(usize, usize)>,
    pos: usize,
    interleave_time: &'a Time,
}

impl<'a> PartitionedBatchIterator<'a> {
    fn new(
        indices: &'a [(u32, u32)],
        record_batches: &'a [&'a RecordBatch],
        batch_size: usize,
        interleave_time: &'a Time,
    ) -> Self {
        if indices.is_empty() {
            // Avoid unnecessary allocations when the partition is empty
            return Self {
                record_batches: &[],
                batch_size,
                indices: &[],
                chunk_scratch: vec![],
                pos: 0,
                interleave_time,
            };
        }
        Self {
            record_batches,
            batch_size,
            indices,
            chunk_scratch: Vec::with_capacity(batch_size.min(indices.len())),
            pos: 0,
            interleave_time,
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
        self.chunk_scratch.clear();
        self.chunk_scratch.extend(
            self.indices[self.pos..indices_end]
                .iter()
                .map(|(i_batch, i_row)| (*i_batch as usize, *i_row as usize)),
        );
        let mut timer = self.interleave_time.timer();
        let result = interleave_record_batch(self.record_batches, &self.chunk_scratch);
        timer.stop();
        match result {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        (0..3)
            .map(|b| {
                let values: Vec<i32> = (0..5).map(|r| b * 100 + r).collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int32Array::from(values))],
                )
                .unwrap()
            })
            .collect()
    }

    /// Chunked index conversion must interleave exactly like converting the whole partition's
    /// index list up front, including the short tail chunk, and share one batch-ref slice
    /// across partitions.
    #[test]
    fn chunked_interleave_matches_full_conversion() {
        let buffered = batches();
        let indices: Vec<(u32, u32)> = vec![
            (0, 0),
            (2, 4),
            (1, 1),
            (0, 3),
            (2, 0),
            (1, 4),
            (0, 1),
            (2, 2),
            (1, 0),
            (0, 4),
        ];
        let batch_size = 4; // chunks of 4, 4, and a tail of 2
        let producer = PartitionedBatchesProducer::new(
            buffered.clone(),
            vec![indices.clone(), Vec::new()],
            batch_size,
        );
        let refs = producer.batch_refs();
        let time = Time::default();

        let produced: Vec<RecordBatch> = producer
            .produce(&refs, 0, &time)
            .collect::<datafusion::common::Result<_>>()
            .unwrap();

        let expected_refs: Vec<&RecordBatch> = buffered.iter().collect();
        let full: Vec<(usize, usize)> = indices
            .iter()
            .map(|(b, r)| (*b as usize, *r as usize))
            .collect();
        let expected: Vec<RecordBatch> = full
            .chunks(batch_size)
            .map(|chunk| interleave_record_batch(&expected_refs, chunk).unwrap())
            .collect();

        assert_eq!(produced, expected);
        assert_eq!(produced.last().unwrap().num_rows(), 2, "tail chunk");

        let empty: Vec<RecordBatch> = producer
            .produce(&refs, 1, &time)
            .collect::<datafusion::common::Result<_>>()
            .unwrap();
        assert!(empty.is_empty());
    }

    /// A refs slice that does not cover every buffered batch (e.g. built from a different
    /// producer) must fail fast in debug builds instead of interleaving wrong rows.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "refs slice must cover every buffered batch")]
    fn produce_rejects_mismatched_refs() {
        let buffered = batches();
        let producer = PartitionedBatchesProducer::new(buffered, vec![vec![(0, 0), (2, 1)]], 4);
        let refs = producer.batch_refs();
        let truncated = &refs[..refs.len() - 1];
        let time = Time::default();
        let _ = producer.produce(truncated, 0, &time);
    }
}
