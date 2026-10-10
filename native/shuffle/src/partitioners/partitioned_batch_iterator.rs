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

use arrow::array::{cast::AsArray, Array, ArrayRef, BooleanArray, RecordBatch};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::{concat_batches, interleave, interleave_record_batch};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
#[cfg(test)]
use datafusion::common::utils::proxy::VecAllocExt;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::metrics::Time;
use std::sync::Arc;

/// A contiguous run of rows within one buffered batch, bound for one output partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BufferedRun {
    pub batch: u32,
    pub start: u32,
    pub len: u32,
}

/// Per-partition record of which buffered rows belong to that partition.
///
/// The two shapes are not interchangeable and a repartitioner picks one for its lifetime, from
/// the partitioning it was built with. Anything that places rows individually has to name them
/// individually; positional round robin places whole spans and can say so in a twelfth of the
/// space at a 64-row group, which matters because this list is charged against the spill
/// reservation. The payoff is at flush: a run is copied with one bulk copy per buffer, where a
/// row list has to be gathered a row at a time through `interleave_record_batch`, re-walking
/// every column and every nested child.
pub(crate) enum PartitionIndices {
    /// One `(batch, row)` pair per row, in the order the rows should be written.
    Rows(Vec<Vec<(u32, u32)>>),
    /// One `(batch, start, len)` run per contiguous span, in the order they should be written.
    Runs(Vec<Vec<BufferedRun>>),
}

impl PartitionIndices {
    /// An empty index of the same shape and partition count, for after the buffered batches it
    /// pointed into have drained.
    pub(crate) fn empty_like(&self) -> Self {
        let num_partitions = self.num_partitions();
        match self {
            Self::Rows(_) => Self::Rows(vec![vec![]; num_partitions]),
            Self::Runs(_) => Self::Runs(vec![vec![]; num_partitions]),
        }
    }

    pub(crate) fn num_partitions(&self) -> usize {
        match self {
            Self::Rows(indices) => indices.len(),
            Self::Runs(runs) => runs.len(),
        }
    }

    /// Bytes the per-partition lists have allocated, which the spill reservation is charged for.
    #[cfg(test)]
    pub(crate) fn allocated_size(&self) -> usize {
        match self {
            Self::Rows(indices) => indices.iter().map(|i| i.allocated_size()).sum(),
            Self::Runs(runs) => runs.iter().map(|r| r.allocated_size()).sum(),
        }
    }

    /// Number of entries recorded across all partitions: rows for [`Self::Rows`], runs for
    /// [`Self::Runs`].
    #[cfg(test)]
    pub(crate) fn entry_count(&self) -> usize {
        match self {
            Self::Rows(indices) => indices.iter().map(Vec::len).sum(),
            Self::Runs(runs) => runs.iter().map(Vec::len).sum(),
        }
    }
}

/// A helper struct to produce shuffled batches.
/// This struct takes ownership of the buffered batches and partition indices from the
/// ShuffleRepartitioner, and provides an iterator over the batches in the specified partitions.
pub(super) struct PartitionedBatchesProducer {
    buffered_batches: Vec<RecordBatch>,
    partition_indices: PartitionIndices,
    batch_size: usize,
    // Every partition borrows the same immutable schema. Check it once per flush.
    has_boolean_columns: bool,
}

impl PartitionedBatchesProducer {
    pub(super) fn new(
        buffered_batches: Vec<RecordBatch>,
        indices: PartitionIndices,
        batch_size: usize,
    ) -> Self {
        let has_boolean_columns = buffered_batches.first().is_some_and(|batch| {
            batch
                .schema_ref()
                .fields()
                .iter()
                .any(|field| field.data_type() == &DataType::Boolean)
        });
        Self {
            partition_indices: indices,
            buffered_batches,
            batch_size,
            has_boolean_columns,
        }
    }

    /// References to all buffered batches. Build this once per write cycle and share it
    /// across every partition's [`Self::produce`] call instead of rebuilding a fresh
    /// `Vec<&RecordBatch>` over all buffered batches for each partition.
    pub(super) fn batch_refs(&self) -> Vec<&RecordBatch> {
        self.buffered_batches.iter().collect()
    }

    /// `interleave_time` is the shuffle writer's `interleave_time` metric, which times the gather
    /// out of the partition index for either shape: `interleave_record_batch` over rows, or the
    /// slicing and concatenation of runs.
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
        match &self.partition_indices {
            PartitionIndices::Rows(indices) => PartitionedBatchIterator::Rows(RowIterator::new(
                &indices[partition_id],
                refs,
                self.batch_size,
                interleave_time,
                self.has_boolean_columns,
            )),
            PartitionIndices::Runs(runs) => PartitionedBatchIterator::Runs(RunIterator::new(
                &runs[partition_id],
                refs,
                self.batch_size,
                interleave_time,
            )),
        }
    }
}

/// Iterates over the shuffled record batches belonging to a single output partition.
///
/// One concrete type covering both index shapes, because [`crate::writers::PartitionWriter`] is
/// generic over a single iterator type rather than taking a trait object.
pub(crate) enum PartitionedBatchIterator<'a> {
    Rows(RowIterator<'a>),
    Runs(RunIterator<'a>),
}

impl Iterator for PartitionedBatchIterator<'_> {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Rows(iter) => iter.next(),
            Self::Runs(iter) => iter.next(),
        }
    }
}

/// Produces a partition's output by gathering individually named rows.
pub(crate) struct RowIterator<'a> {
    has_boolean_columns: bool,
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

impl<'a> RowIterator<'a> {
    fn new(
        indices: &'a [(u32, u32)],
        record_batches: &'a [&'a RecordBatch],
        batch_size: usize,
        interleave_time: &'a Time,
        has_boolean_columns: bool,
    ) -> Self {
        if indices.is_empty() {
            // Avoid unnecessary allocations when the partition is empty
            return Self {
                has_boolean_columns,
                record_batches: &[],
                batch_size,
                indices: &[],
                chunk_scratch: vec![],
                pos: 0,
                interleave_time,
            };
        }
        Self {
            has_boolean_columns,
            record_batches,
            batch_size,
            indices,
            chunk_scratch: Vec::with_capacity(batch_size.min(indices.len())),
            pos: 0,
            interleave_time,
        }
    }
}

impl Iterator for RowIterator<'_> {
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
        let result = if self.has_boolean_columns {
            interleave_shuffle_batches(self.record_batches, &self.chunk_scratch)
        } else {
            interleave_record_batch(self.record_batches, &self.chunk_scratch)
        };
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

/// Gather scattered Boolean state without rebuilding Arrow's MutableArrayData
/// descriptors for every reducer. Clustered selections retain Arrow's range copies.
/// Upstream tracking: https://github.com/apache/arrow-rs/issues/11379
/// The producer supplies matching immutable batches and valid row indices, just as
/// for the ordinary Arrow interleave path.
fn interleave_shuffle_batches(
    batches: &[&RecordBatch],
    indices: &[(usize, usize)],
) -> Result<RecordBatch, ArrowError> {
    let schema = batches[0].schema_ref();
    // Keep Arrow's efficient range copying for long clusters. Short hash runs still
    // benefit from direct Boolean gathering, especially with few output partitions.
    let clustered = has_long_contiguous_run(indices);
    if clustered {
        return interleave_record_batch(batches, indices);
    }
    let columns = (0..schema.fields().len())
        .map(|column| {
            if schema.field(column).data_type() != &DataType::Boolean {
                let arrays: Vec<&dyn Array> =
                    batches.iter().map(|b| b.column(column).as_ref()).collect();
                return interleave(&arrays, indices);
            }
            let booleans: Vec<_> = batches
                .iter()
                .map(|b| b.column(column).as_boolean())
                .collect();
            let values = BooleanBuffer::collect_bool(indices.len(), |i| {
                let (batch, row) = indices[i];
                booleans[batch].value(row)
            });
            let nulls = booleans.iter().any(|a| a.null_count() != 0).then(|| {
                NullBuffer::new(BooleanBuffer::collect_bool(indices.len(), |i| {
                    let (batch, row) = indices[i];
                    booleans[batch].is_valid(row)
                }))
            });
            Ok(Arc::new(BooleanArray::new(values, nulls)) as ArrayRef)
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns)
}

/// Sample at a 32-row stride and distance: every 64-row contiguous run is
/// detected regardless of alignment. False positives only choose Arrow's path.
fn has_long_contiguous_run(indices: &[(usize, usize)]) -> bool {
    (0..indices.len().saturating_sub(32)).step_by(32).any(|i| {
        indices[i].0 == indices[i + 32].0 && indices[i].1.checked_add(32) == Some(indices[i + 32].1)
    })
}

/// Produces a partition's output by copying contiguous runs of rows.
pub(crate) struct RunIterator<'a> {
    record_batches: &'a [&'a RecordBatch],
    batch_size: usize,
    runs: &'a [BufferedRun],
    /// Scratch for the slices making up the current output chunk.
    chunk_scratch: Vec<RecordBatch>,
    /// Index of the next run to consume.
    pos: usize,
    /// Rows already taken from `runs[pos]`, non-zero only when a run straddled a chunk boundary.
    consumed: u32,
    interleave_time: &'a Time,
}

impl<'a> RunIterator<'a> {
    fn new(
        runs: &'a [BufferedRun],
        record_batches: &'a [&'a RecordBatch],
        batch_size: usize,
        interleave_time: &'a Time,
    ) -> Self {
        Self {
            record_batches,
            batch_size,
            runs,
            chunk_scratch: Vec::with_capacity(runs.len().min(batch_size)),
            pos: 0,
            consumed: 0,
            interleave_time,
        }
    }
}

impl Iterator for RunIterator<'_> {
    type Item = datafusion::common::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.runs.len() {
            return None;
        }
        let mut timer = self.interleave_time.timer();

        // Zero-copy path: the next run is an entire buffered batch of exactly `batch_size` rows,
        // so it is already the chunk the copying path below would build, and can be handed
        // straight through. This is the case a group as large as the batch size is chosen to
        // hit. A buffered batch is never longer than `batch_size`, since `insert_batch` slices
        // its input to that, so both paths emit the same chunk sizes.
        if self.consumed == 0 {
            let run = self.runs[self.pos];
            let source = self.record_batches[run.batch as usize];
            if run.start == 0
                && run.len as usize == source.num_rows()
                && run.len as usize == self.batch_size
            {
                self.pos += 1;
                timer.stop();
                return Some(Ok((*source).clone()));
            }
        }

        // Otherwise accumulate whole runs until the chunk is full, splitting the run that
        // straddles the boundary, so that every chunk but a partition's last is `batch_size` rows
        // however long the runs happen to be.
        self.chunk_scratch.clear();
        let mut rows = 0usize;
        while self.pos < self.runs.len() && rows < self.batch_size {
            let run = self.runs[self.pos];
            let source = self.record_batches[run.batch as usize];
            let available = (run.len - self.consumed) as usize;
            let take = available.min(self.batch_size - rows);
            self.chunk_scratch
                .push(source.slice((run.start + self.consumed) as usize, take));
            rows += take;
            if take == available {
                self.pos += 1;
                self.consumed = 0;
            } else {
                self.consumed += take as u32;
            }
        }

        // `concat_batches` over a single slice returns the slice rather than compacting it
        // (arrow's `concat` short-circuits at one input), so skip the call and let the slice
        // through: the IPC writer truncates a sliced array's buffers per type, and the one
        // family it does not, the view types, cannot reach here (see `create_repartitioner`).
        let result = if self.chunk_scratch.len() == 1 {
            Ok(self.chunk_scratch.pop().expect("one slice"))
        } else {
            concat_batches(&self.chunk_scratch[0].schema(), self.chunk_scratch.iter())
        };
        timer.stop();
        match result {
            Ok(batch) => Some(Ok(batch)),
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
    use arrow::array::{Decimal128Array, Int32Array, Int64Array};
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

    #[test]
    fn boolean_gather_probe_preserves_short_hash_runs() {
        for run in [1, 2, 4, 8, 16] {
            let indices: Vec<_> = (0..8192)
                .map(|row| ((row / run) % 4, (row / (4 * run)) * run + row % run))
                .collect();
            assert!(!has_long_contiguous_run(&indices), "run={run}");
        }
        assert!(!has_long_contiguous_run(&[]));
        assert!(!has_long_contiguous_run(&[(0, 0); 32]));
        for offset in 0..32 {
            let mut indices = vec![(1, 0); offset];
            indices.extend((0..64).map(|row| (0, row)));
            assert!(has_long_contiguous_run(&indices), "offset={offset}");
        }
        let mut overflow = vec![(0, 0); 33];
        overflow[0] = (0, usize::MAX);
        overflow[32] = (0, 31);
        assert!(!has_long_contiguous_run(&overflow));
    }

    /// Compare mixed aggregate states against Arrow across non-byte-aligned slices, null
    /// patterns, contiguous and reordered rows, and chunks smaller than a bitmap word.
    #[test]
    fn boolean_interleave_matches_arrow_for_sliced_nullable_states() {
        for nullable in [false, true] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("sum", DataType::Decimal128(38, 2), true),
                Field::new("is_empty", DataType::Boolean, nullable),
                Field::new("all_null", DataType::Boolean, true),
            ]));
            let batches: Vec<_> = (0..4)
                .map(|source| {
                    let bools = BooleanArray::from(
                        (0..97)
                            .map(|row| {
                                if nullable && source % 2 == 0 && (row + source) % 5 == 0 {
                                    None
                                } else {
                                    Some((row + source) % 3 == 0)
                                }
                            })
                            .collect::<Vec<_>>(),
                    );
                    let decimals = Decimal128Array::from(
                        (0..97)
                            .map(|row| {
                                ((row + source) % 7 != 0).then_some((source * 100 + row) as i128)
                            })
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(38, 2)
                    .unwrap();
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from_iter_values(0..97)),
                            Arc::new(decimals),
                            Arc::new(bools),
                            Arc::new(BooleanArray::new_null(97)),
                        ],
                    )
                    .unwrap()
                    .slice(source + 1, 79)
                })
                .collect();
            let refs: Vec<_> = batches.iter().collect();
            let mut selections = vec![
                (0..257)
                    .map(|i| ((i * 3 % 4) as u32, (i * 17 % 79) as u32))
                    .collect::<Vec<_>>(),
                (0..79).map(|row| (1, row)).collect(),
                (0..4)
                    .flat_map(|batch| (3..20).chain(31..47).map(move |row| (batch, row)))
                    .collect(),
            ];
            // Stable hash partitioning can place scattered rows before eight-row runs.
            for prefix in [1, 4] {
                selections.push(
                    (0..2 * prefix)
                        .step_by(2)
                        .chain((2 * prefix..79).filter(|row| ((row - 2 * prefix) / 8) % 2 == 0))
                        .map(|row| (0, row))
                        .collect(),
                );
            }
            for indices in selections {
                for chunk_size in [1, 7, 64, 8192] {
                    let time = Time::default();
                    let producer = PartitionedBatchesProducer::new(
                        batches.clone(),
                        PartitionIndices::Rows(vec![indices.clone()]),
                        chunk_size,
                    );
                    let input_refs = producer.batch_refs();
                    let actual = producer
                        .produce(&input_refs, 0, &time)
                        .collect::<datafusion::common::Result<Vec<_>>>()
                        .unwrap();
                    for (actual, chunk) in actual.iter().zip(indices.chunks(chunk_size)) {
                        let selected: Vec<_> = chunk
                            .iter()
                            .map(|&(b, r)| (b as usize, r as usize))
                            .collect();
                        assert_eq!(*actual, interleave_record_batch(&refs, &selected).unwrap());
                    }
                    assert_eq!(
                        actual.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        indices.len()
                    );
                }
            }
        }
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
            PartitionIndices::Rows(vec![indices.clone(), Vec::new()]),
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
        let producer = PartitionedBatchesProducer::new(
            buffered,
            PartitionIndices::Rows(vec![vec![(0, 0), (2, 1)]]),
            4,
        );
        let refs = producer.batch_refs();
        let truncated = &refs[..refs.len() - 1];
        let time = Time::default();
        let _ = producer.produce(truncated, 0, &time);
    }

    fn run_values(runs: Vec<BufferedRun>, batch_size: usize) -> (Vec<Vec<i32>>, Vec<RecordBatch>) {
        let buffered = batches();
        let producer = PartitionedBatchesProducer::new(
            buffered,
            PartitionIndices::Runs(vec![runs]),
            batch_size,
        );
        let refs = producer.batch_refs();
        let time = Time::default();
        let produced: Vec<RecordBatch> = producer
            .produce(&refs, 0, &time)
            .collect::<datafusion::common::Result<_>>()
            .unwrap();
        let values = produced
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        (values, produced)
    }

    /// Runs are emitted in order, concatenated up to `batch_size`, with the run that straddles a
    /// chunk boundary split across the two chunks.
    #[test]
    fn runs_concatenate_into_fixed_size_chunks() {
        let runs = vec![
            BufferedRun {
                batch: 0,
                start: 1,
                len: 3,
            }, // 1, 2, 3
            BufferedRun {
                batch: 2,
                start: 0,
                len: 2,
            }, // 200, 201
            BufferedRun {
                batch: 1,
                start: 3,
                len: 2,
            }, // 103, 104
        ];
        let (values, _) = run_values(runs, 4);
        assert_eq!(values, vec![vec![1, 2, 3, 200], vec![201, 103, 104]]);
    }

    /// A run covering a whole buffered batch of `batch_size` rows is already a full chunk, so it
    /// is handed through without copying. Identity rather than equality, because the point is that the
    /// output shares the input's buffers.
    #[test]
    fn whole_batch_run_is_returned_without_copying() {
        let buffered = batches();
        let source_ptr = buffered[1].column(0).as_ref() as *const dyn arrow::array::Array;
        let producer = PartitionedBatchesProducer::new(
            buffered,
            PartitionIndices::Runs(vec![vec![BufferedRun {
                batch: 1,
                start: 0,
                len: 5,
            }]]),
            5,
        );
        let refs = producer.batch_refs();
        let time = Time::default();
        let produced: Vec<RecordBatch> = producer
            .produce(&refs, 0, &time)
            .collect::<datafusion::common::Result<_>>()
            .unwrap();
        assert_eq!(produced.len(), 1);
        assert!(std::ptr::addr_eq(
            produced[0].column(0).as_ref() as *const dyn arrow::array::Array,
            source_ptr
        ));
    }

    /// A single run that is a strict sub-range is sliced rather than copied, which is the cheap
    /// outcome and one the IPC writer truncates correctly for every non-view type.
    #[test]
    fn single_sub_range_run_stays_a_slice() {
        let buffered = batches();
        let source = buffered[2].column(0).to_data().buffers()[0].clone();
        let producer = PartitionedBatchesProducer::new(
            buffered,
            PartitionIndices::Runs(vec![vec![BufferedRun {
                batch: 2,
                start: 1,
                len: 3,
            }]]),
            8,
        );
        let refs = producer.batch_refs();
        let time = Time::default();
        let produced: Vec<RecordBatch> = producer
            .produce(&refs, 0, &time)
            .collect::<datafusion::common::Result<_>>()
            .unwrap();

        assert_eq!(
            produced[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec(),
            vec![201, 202, 203]
        );
        // Three i32s starting one element into the source allocation: `data_ptr` (the allocation)
        // is unchanged and `as_ptr` (the slice) has advanced, so nothing was copied.
        let produced_buffer = produced[0].column(0).to_data().buffers()[0].clone();
        assert_eq!(
            produced_buffer.data_ptr().as_ptr() as usize,
            source.data_ptr().as_ptr() as usize,
            "expected the source allocation, not a copy"
        );
        assert_eq!(
            produced_buffer.as_ptr() as usize,
            source.as_ptr() as usize + size_of::<i32>()
        );
    }

    #[test]
    fn empty_run_list_produces_nothing() {
        let (values, _) = run_values(vec![], 4);
        assert!(values.is_empty());
    }
}
