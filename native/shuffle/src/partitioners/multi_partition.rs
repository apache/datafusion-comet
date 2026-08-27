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

use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::partitioned_batch_iterator::PartitionedBatchesProducer;
use crate::partitioners::ShufflePartitioner;
use crate::writers::PartitionWriter;
use crate::{comet_partitioning, CometPartitioning, RoundRobinStrategy};
use arrow::array::{Array, ArrayData, ArrayRef, RecordBatch};
use datafusion::common::utils::proxy::VecAllocExt;
use datafusion::common::{DataFusionError, HashSet};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_comet_common::tracing::{with_trace, with_trace_async};
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use itertools::Itertools;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::time::Instant;

/// Reusable scratch buffers for computing row-to-partition assignments.
#[derive(Default)]
struct ScratchSpace {
    /// Hashes for each row in the current batch.
    hashes_buf: Vec<u32>,
    /// Partition ids for each row in the current batch.
    partition_ids: Vec<u32>,
    /// The row indices of the rows in each partition. This array is conceptually divided into
    /// partitions, where each partition contains the row indices of the rows in that partition.
    /// The length of this array is the same as the number of rows in the batch.
    partition_row_indices: Vec<u32>,
    /// The start indices of partitions in partition_row_indices. partition_starts[K] and
    /// partition_starts[K + 1] are the start and end indices of partition K in partition_row_indices.
    /// The length of this array is 1 + the number of partitions.
    partition_starts: Vec<u32>,
}

impl ScratchSpace {
    fn map_partition_ids_to_starts_and_indices(
        &mut self,
        num_output_partitions: usize,
        num_rows: usize,
    ) {
        let partition_ids = &mut self.partition_ids[..num_rows];

        // count each partition size, while leaving the last extra element as 0
        let partition_counters = &mut self.partition_starts;
        partition_counters.resize(num_output_partitions + 1, 0);
        partition_counters.fill(0);
        partition_ids
            .iter()
            .for_each(|partition_id| partition_counters[*partition_id as usize] += 1);

        // accumulate partition counters into partition ends
        // e.g. partition counter: [1, 3, 2, 1, 0] => [1, 4, 6, 7, 7]
        let partition_ends = partition_counters;
        let mut accum = 0;
        partition_ends.iter_mut().for_each(|v| {
            *v += accum;
            accum = *v;
        });

        // calculate partition row indices and partition starts
        // e.g. partition ids: [3, 1, 1, 1, 2, 2, 0] will produce the following partition_row_indices
        // and partition_starts arrays:
        //
        //  partition_row_indices: [6, 1, 2, 3, 4, 5, 0]
        //  partition_starts: [0, 1, 4, 6, 7]
        //
        // partition_starts conceptually splits partition_row_indices into smaller slices.
        // Each slice partition_row_indices[partition_starts[K]..partition_starts[K + 1]] contains the
        // row indices of the input batch that are partitioned into partition K. For example,
        // first partition 0 has one row index [6], partition 1 has row indices [1, 2, 3], etc.
        let partition_row_indices = &mut self.partition_row_indices;
        partition_row_indices.resize(num_rows, 0);
        for (index, partition_id) in partition_ids.iter().enumerate().rev() {
            partition_ends[*partition_id as usize] -= 1;
            let end = partition_ends[*partition_id as usize];
            partition_row_indices[end as usize] = index as u32;
        }

        // after calculating, partition ends become partition starts
    }
}

/// A partitioner that uses a hash function to partition data into multiple partitions
pub struct MultiPartitionShuffleRepartitioner<T: PartitionWriter> {
    buffered_batches: Vec<RecordBatch>,
    partition_indices: Vec<Vec<(u32, u32)>>,
    partition_writer: T,
    /// Partitioning scheme to use
    partitioning: CometPartitioning,
    metrics: ShufflePartitionerMetrics,
    /// Reused scratch space for computing partition indices
    scratch: ScratchSpace,
    /// The configured batch size
    batch_size: usize,
    /// Reservation for repartitioning
    reservation: MemoryReservation,
    /// Spill once the reservation reaches this many bytes, independently of whether the memory
    /// pool still has capacity. `None` disables the limit, leaving pool pressure as the only
    /// spill trigger.
    max_buffer_bytes: Option<usize>,
    tracing_enabled: bool,
    /// Start addresses (as `usize`, since raw pointers are not `Send`) of the backing buffers
    /// currently pinned by `buffered_batches`, so the spill reservation charges each distinct
    /// allocation once rather than once per slice that references it. Cleared whenever the
    /// buffered batches drain (spill / shuffle_write). See `count_new_buffers`.
    pinned_buffers: HashSet<usize>,
    /// Backing buffers already reported as spilled while slicing the current outer input batch.
    /// The outer batch keeps these allocations alive across spills, so clear this set only when
    /// that input batch finishes rather than whenever the repartitioner's buffers drain.
    spill_accounted_input_buffers: HashSet<usize>,
    /// Bytes in the currently buffered batches that were already counted by a previous spill of
    /// the same outer input batch. Partition-index allocations are never included here.
    repeated_spill_buffer_bytes: usize,
    /// Batch counter for the batch-granular RoundRobin path. Seeded with the input partition
    /// id so mappers with adjacent partition ids do not concentrate their first batches on the
    /// same output partition, then incremented once per input batch slice that reaches
    /// `partitioning_batch`. Unused by other strategies.
    round_robin_batch_seq: usize,
}

/// Sum of the capacities of the backing buffers reachable from `batch` whose start address is
/// not already in `seen` (recursing through child data: dictionary values, list children, and so
/// on). `seen` is kept across every buffered batch, so this returns the bytes a batch newly
/// pins, which is the memory the shuffle writer holds resident by buffering it. The second return
/// value contains the subset of those bytes whose buffers were already reported spilled while
/// processing the current outer input batch.
///
/// Cheaper measures do not match resident memory for the batches this writer sees. A partial
/// `HashAggregate` emits one group-values buffer sliced into batch_size chunks, and every
/// buffered chunk shares that one allocation:
///
///   * `RecordBatch::get_array_memory_size()` charges a buffer's capacity once per array that
///     references it, counting the shared allocation once per chunk and overstating memory by the
///     chunk count. Reserving against that figure trips the memory limit on nearly every batch
///     and spills spuriously.
///   * the sum of `ArrayData::get_slice_memory_size()` charges only the live rows of each slice,
///     but holding a slice pins its whole backing allocation. The group-values `Vec` rounds
///     capacity up to the next power of two, so that figure undercounts resident memory and lets
///     the writer hold well past its limit before spilling.
///
/// Counting each distinct allocation once, keyed by start address, is the measure that tracks
/// resident memory regardless of how arrays share or slice their buffers.
fn count_new_buffers(
    batch: &RecordBatch,
    seen: &mut HashSet<usize>,
    previously_spilled: Option<&HashSet<usize>>,
) -> (usize, usize) {
    fn visit(
        data: &ArrayData,
        seen: &mut HashSet<usize>,
        previously_spilled: Option<&HashSet<usize>>,
        total: &mut usize,
        repeated: &mut usize,
    ) {
        for buffer in data.buffers() {
            let address = buffer.data_ptr().as_ptr() as usize;
            if seen.insert(address) {
                *total += buffer.capacity();
                if previously_spilled.is_some_and(|buffers| buffers.contains(&address)) {
                    *repeated += buffer.capacity();
                }
            }
        }
        if let Some(nulls) = data.nulls() {
            let inner = nulls.inner().inner();
            let address = inner.data_ptr().as_ptr() as usize;
            if seen.insert(address) {
                *total += inner.capacity();
                if previously_spilled.is_some_and(|buffers| buffers.contains(&address)) {
                    *repeated += inner.capacity();
                }
            }
        }
        for child in data.child_data() {
            visit(child, seen, previously_spilled, total, repeated);
        }
    }
    let mut total = 0;
    let mut repeated = 0;
    for column in batch.columns() {
        visit(
            &column.to_data(),
            seen,
            previously_spilled,
            &mut total,
            &mut repeated,
        );
    }
    (total, repeated)
}

impl<T: PartitionWriter> MultiPartitionShuffleRepartitioner<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        partition: usize,
        partition_writer: T,
        partitioning: CometPartitioning,
        metrics: ShufflePartitionerMetrics,
        runtime: Arc<RuntimeEnv>,
        batch_size: usize,
        tracing_enabled: bool,
        max_buffer_bytes: Option<usize>,
    ) -> datafusion::common::Result<Self> {
        let num_output_partitions = partitioning.partition_count();
        assert_ne!(
            num_output_partitions, 1,
            "Use SinglePartitionShufflePartitioner for 1 output partition."
        );

        // Vectors in the scratch space will be filled with valid values before being used, this
        // initialization code is simply initializing the vectors to the desired size.
        // The initial values are not used.
        //
        // `partition_ids` and `partition_row_indices` are only touched by the row-level
        // strategies (`Hash`, `RangePartitioning`, and hash-all-columns RoundRobin). The
        // whole-batch RoundRobin path never inspects rows, so allocating them there would be
        // ~64 KB of dead scratch on every task.
        let needs_row_scratch = !matches!(
            &partitioning,
            CometPartitioning::SinglePartition
                | CometPartitioning::RoundRobin(_, RoundRobinStrategy::WholeBatch),
        );
        let scratch = ScratchSpace {
            hashes_buf: match &partitioning {
                // Allocate hashes_buf for hash and hash-all-columns round robin partitioning.
                // Whole-batch round robin does no per-row hashing.
                CometPartitioning::Hash(_, _)
                | CometPartitioning::RoundRobin(_, RoundRobinStrategy::HashAll { .. }) => {
                    vec![0; batch_size]
                }
                _ => vec![],
            },
            partition_ids: if needs_row_scratch {
                vec![0; batch_size]
            } else {
                vec![]
            },
            partition_row_indices: if needs_row_scratch {
                vec![0; batch_size]
            } else {
                vec![]
            },
            partition_starts: vec![0; num_output_partitions + 1],
        };

        let reservation = MemoryConsumer::new(format!("ShuffleRepartitioner[{partition}]"))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        Ok(Self {
            buffered_batches: vec![],
            partition_indices: vec![vec![]; num_output_partitions],
            partition_writer,
            partitioning,
            metrics,
            scratch,
            batch_size,
            reservation,
            max_buffer_bytes,
            tracing_enabled,
            pinned_buffers: HashSet::new(),
            spill_accounted_input_buffers: HashSet::new(),
            repeated_spill_buffer_bytes: 0,
            // Seed with the input partition id so batches from mapper i land on
            // partition (i + k) mod N on the k-th batch, spreading concurrent mappers' first
            // batches across distinct output partitions.
            round_robin_batch_seq: partition,
        })
    }

    /// Shuffles rows in input batch into corresponding partition buffer.
    /// This function first calculates hashes for rows and then takes rows in same
    /// partition as a record batch which is appended into partition buffer.
    /// This should not be called directly. Use `insert_batch` instead.
    async fn partitioning_batch(&mut self, input: RecordBatch) -> datafusion::common::Result<()> {
        if input.num_rows() == 0 {
            // skip empty batch
            return Ok(());
        }

        if input.num_rows() > self.batch_size {
            return Err(DataFusionError::Internal(
                "Input batch size exceeds configured batch size. Call `insert_batch` instead."
                    .to_string(),
            ));
        }

        // NOTE: in shuffle writer exec, the output_rows metrics represents the
        // number of rows those are written to output data file.
        self.metrics.baseline.record_output(input.num_rows());

        match &self.partitioning {
            CometPartitioning::Hash(exprs, num_output_partitions) => {
                let mut scratch = std::mem::take(&mut self.scratch);
                let (partition_starts, partition_row_indices): (&Vec<u32>, &Vec<u32>) = {
                    let mut timer = self.metrics.repart_time.timer();

                    // Evaluate partition expressions to get rows to apply partitioning scheme.
                    let arrays = exprs
                        .iter()
                        .map(|expr| expr.evaluate(&input)?.into_array(input.num_rows()))
                        .collect::<datafusion::common::Result<Vec<_>>>()?;

                    let num_rows = arrays[0].len();

                    // Use identical seed as Spark hash partitioning.
                    let hashes_buf = &mut scratch.hashes_buf[..num_rows];
                    hashes_buf.fill(42_u32);

                    // Generate partition ids for every row.
                    {
                        // Hash arrays and compute partition ids based on number of partitions.
                        let partition_ids = &mut scratch.partition_ids[..num_rows];
                        create_murmur3_hashes(&arrays, hashes_buf)?
                            .iter()
                            .enumerate()
                            .for_each(|(idx, hash)| {
                                partition_ids[idx] =
                                    comet_partitioning::pmod(*hash, *num_output_partitions) as u32;
                            });
                    }

                    // We now have partition ids for every input row, map that to partition starts
                    // and partition indices to eventually right these rows to partition buffers.
                    scratch
                        .map_partition_ids_to_starts_and_indices(*num_output_partitions, num_rows);

                    timer.stop();
                    Ok::<(&Vec<u32>, &Vec<u32>), DataFusionError>((
                        &scratch.partition_starts,
                        &scratch.partition_row_indices,
                    ))
                }?;

                self.buffer_partitioned_batch_may_spill(
                    input,
                    Some(partition_row_indices),
                    partition_starts,
                )
                .await?;
                self.scratch = scratch;
            }
            CometPartitioning::RangePartitioning(
                lex_ordering,
                num_output_partitions,
                row_converter,
                bounds,
            ) => {
                let mut scratch = std::mem::take(&mut self.scratch);
                let (partition_starts, partition_row_indices): (&Vec<u32>, &Vec<u32>) = {
                    let mut timer = self.metrics.repart_time.timer();

                    // Evaluate partition expressions for values to apply partitioning scheme on.
                    let arrays = lex_ordering
                        .iter()
                        .map(|expr| expr.expr.evaluate(&input)?.into_array(input.num_rows()))
                        .collect::<datafusion::common::Result<Vec<_>>>()?;

                    let num_rows = arrays[0].len();

                    // Generate partition ids for every row, first by converting the partition
                    // arrays to Rows, and then doing binary search for each Row against the
                    // bounds Rows.
                    {
                        let row_batch = row_converter.convert_columns(arrays.as_slice())?;
                        let partition_ids = &mut scratch.partition_ids[..num_rows];

                        row_batch.iter().enumerate().for_each(|(row_idx, row)| {
                            partition_ids[row_idx] = bounds
                                .as_slice()
                                .partition_point(|bound| bound.row() <= row)
                                as u32
                        });
                    }

                    // We now have partition ids for every input row, map that to partition starts
                    // and partition indices to eventually right these rows to partition buffers.
                    scratch
                        .map_partition_ids_to_starts_and_indices(*num_output_partitions, num_rows);

                    timer.stop();
                    Ok::<(&Vec<u32>, &Vec<u32>), DataFusionError>((
                        &scratch.partition_starts,
                        &scratch.partition_row_indices,
                    ))
                }?;

                self.buffer_partitioned_batch_may_spill(
                    input,
                    Some(partition_row_indices),
                    partition_starts,
                )
                .await?;
                self.scratch = scratch;
            }
            CometPartitioning::RoundRobin(num_output_partitions, strategy) => {
                // Two strategies share the scratch-take / timer / spill / scratch-restore
                // scaffold. Only the middle "fill partition_row_indices + partition_starts"
                // step differs. WholeBatch: assign the whole input batch to one partition and
                // pay no per-row cost. HashAll: hash every row (over up to max_hash_columns
                // columns) and route rows individually. WholeBatch is retry-safe only when the
                // upstream operator emits the same batches in the same order under retry
                // (Comet's `CometNativeScan` and other order-preserving operators do so).
                let mut scratch = std::mem::take(&mut self.scratch);
                let num_rows = input.num_rows();
                let partition_row_indices: Option<&[u32]> = {
                    let mut timer = self.metrics.repart_time.timer();

                    let indices: Option<&[u32]> = match strategy {
                        RoundRobinStrategy::WholeBatch => {
                            let target_idx = self.round_robin_batch_seq % *num_output_partitions;
                            self.round_robin_batch_seq = self.round_robin_batch_seq.wrapping_add(1);

                            // `partition_starts[k]..partition_starts[k+1]` is partition k's
                            // slice. Slots 0..=target_idx are 0; slots after `target_idx` are
                            // `num_rows`, so `target_idx` owns the whole [0..num_rows) range.
                            // `partition_row_indices` is left unmaterialized — the spill path
                            // treats the target partition's slice as an identity mapping.
                            let partition_starts = &mut scratch.partition_starts;
                            partition_starts.clear();
                            partition_starts.resize(target_idx + 1, 0);
                            partition_starts.resize(*num_output_partitions + 1, num_rows as u32);
                            None
                        }
                        RoundRobinStrategy::HashAll { max_hash_columns } => {
                            // Hash-partition rows into pmod(hash, N). This produces different
                            // partition assignments than Spark's round robin (which sorts by
                            // UnsafeRow binary representation before assigning partitions), but
                            // both approaches provide even distribution and determinism.
                            //
                            // max_hash_columns of 0 means no limit (hash all columns). Negative
                            // values are normalized to 0 in the planner.
                            let num_columns_to_hash = if *max_hash_columns == 0 {
                                input.num_columns()
                            } else {
                                (*max_hash_columns).min(input.num_columns())
                            };
                            let columns_to_hash: Vec<ArrayRef> = (0..num_columns_to_hash)
                                .map(|i| Arc::clone(input.column(i)))
                                .collect();

                            // Use identical seed as Spark hash partitioning.
                            let hashes_buf = &mut scratch.hashes_buf[..num_rows];
                            hashes_buf.fill(42_u32);
                            create_murmur3_hashes(&columns_to_hash, hashes_buf)?;

                            let partition_ids = &mut scratch.partition_ids[..num_rows];
                            hashes_buf.iter().enumerate().for_each(|(idx, hash)| {
                                partition_ids[idx] =
                                    comet_partitioning::pmod(*hash, *num_output_partitions) as u32;
                            });

                            scratch.map_partition_ids_to_starts_and_indices(
                                *num_output_partitions,
                                num_rows,
                            );
                            Some(scratch.partition_row_indices.as_slice())
                        }
                    };

                    timer.stop();
                    indices
                };

                self.buffer_partitioned_batch_may_spill(
                    input,
                    partition_row_indices,
                    &scratch.partition_starts,
                )
                .await?;
                self.scratch = scratch;
            }
            other => {
                // this should be unreachable as long as the validation logic
                // in the constructor is kept up-to-date
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported shuffle partitioning scheme {other:?}"
                )));
            }
        }
        Ok(())
    }

    async fn buffer_partitioned_batch_may_spill(
        &mut self,
        input: RecordBatch,
        partition_row_indices: Option<&[u32]>,
        partition_starts: &[u32],
    ) -> datafusion::common::Result<()> {
        // Charge both the reservation and the data_size metric for the buffers this batch newly
        // pins; `count_new_buffers` dedups buffers shared across already-buffered batches.
        let (new_buffer_bytes, repeated_buffer_bytes) = count_new_buffers(
            &input,
            &mut self.pinned_buffers,
            Some(&self.spill_accounted_input_buffers),
        );
        self.repeated_spill_buffer_bytes += repeated_buffer_bytes;
        self.metrics.data_size.add(new_buffer_bytes);
        let mut mem_growth: usize = new_buffer_bytes;
        let buffered_partition_idx = self.buffered_batches.len() as u32;
        self.buffered_batches.push(input);

        // `partition_starts` slices the input's rows into per-partition ranges: partition K
        // owns rows `partition_starts[K]..partition_starts[K + 1]`. When `partition_row_indices`
        // is `Some(indices)`, `indices[start..end]` are the source-row ids in the input for
        // partition K (see the hash arms). When it is `None`, the source rows are the identity
        // range `start..end` itself (whole-batch RoundRobin — one target partition owns all
        // input rows, and the mapping is trivial so we do not materialize it).
        for (partition_id, (&start, &end)) in partition_starts
            .iter()
            .tuple_windows()
            .enumerate()
            .filter(|(_, (start, end))| start < end)
        {
            let indices = &mut self.partition_indices[partition_id];
            let before_size = indices.allocated_size();
            match partition_row_indices {
                Some(row_indices) => indices.extend(
                    row_indices[start as usize..end as usize]
                        .iter()
                        .map(|&row_idx| (buffered_partition_idx, row_idx)),
                ),
                None => {
                    indices.extend((start..end).map(|row_idx| (buffered_partition_idx, row_idx)))
                }
            }
            let after_size = indices.allocated_size();
            mem_growth += after_size.saturating_sub(before_size);
        }

        // A rejected reservation does not include this batch's memory, even though the batch
        // and its partition indices have already been buffered and must be counted as spilled.
        let reservation_failed = self.reservation.try_grow(mem_growth).is_err();
        // Checking after buffering lets the writer overshoot the limit by at most one batch,
        // which is how the memory-pressure trigger already behaves.
        if reservation_failed
            || self
                .max_buffer_bytes
                .is_some_and(|limit| self.reservation.size() >= limit)
        {
            let unreserved_bytes = if reservation_failed { mem_growth } else { 0 };
            count_new_buffers(
                self.buffered_batches
                    .last()
                    .expect("the current input batch was buffered before spilling"),
                &mut self.spill_accounted_input_buffers,
                None,
            );
            self.spill(unreserved_bytes)?;
        }

        Ok(())
    }

    fn used(&self) -> usize {
        self.reservation.size()
    }

    fn spilled_bytes(&self) -> usize {
        self.metrics.spilled_bytes.value()
    }

    fn spill_count(&self) -> usize {
        self.metrics.spill_count.value()
    }

    fn data_size(&self) -> usize {
        self.metrics.data_size.value()
    }

    /// This function transfers the ownership of the buffered batches and partition indices from the
    /// ShuffleRepartitioner to a new PartitionedBatches struct. The returned PartitionedBatches struct
    /// can be used to produce shuffled batches.
    fn partitioned_batches(&mut self) -> PartitionedBatchesProducer {
        let num_output_partitions = self.partition_indices.len();
        let buffered_batches = std::mem::take(&mut self.buffered_batches);
        // let indices = std::mem::take(&mut self.partition_indices);
        let indices = std::mem::replace(
            &mut self.partition_indices,
            vec![vec![]; num_output_partitions],
        );
        PartitionedBatchesProducer::new(buffered_batches, indices, self.batch_size)
    }

    pub(crate) fn spill(&mut self, unreserved_bytes: usize) -> datafusion::common::Result<()> {
        log::info!(
            "ShuffleRepartitioner spilling {} bytes to its partition writer ({} previous spills)",
            self.used(),
            self.spill_count()
        );

        // we could always get a chance to free some memory as long as we are holding some
        if self.buffered_batches.is_empty() {
            return Ok(());
        }

        with_trace("shuffle_spill", self.tracing_enabled, || {
            let num_output_partitions = self.partition_indices.len();
            let write_result = {
                let mut partitioned_batches = self.partitioned_batches();
                (0..num_output_partitions).try_for_each(|partition_id| {
                    self.partition_writer.write(
                        partition_id,
                        &mut partitioned_batches
                            .produce(partition_id, &self.metrics.interleave_time),
                        &self.metrics,
                    )
                })
            };

            let memory_spilled_bytes = self
                .reservation
                .free()
                .saturating_add(unreserved_bytes)
                .saturating_sub(self.repeated_spill_buffer_bytes);
            self.metrics.memory_spilled_bytes.add(memory_spilled_bytes);
            self.pinned_buffers.clear();
            self.repeated_spill_buffer_bytes = 0;
            self.metrics.spill_count.add(1);
            write_result
        })
    }

    #[cfg(test)]
    pub(crate) fn partition_writer(&self) -> &T {
        &self.partition_writer
    }
}

#[async_trait::async_trait]
impl<T: PartitionWriter> ShufflePartitioner for MultiPartitionShuffleRepartitioner<T> {
    /// Shuffles rows in input batch into corresponding partition buffer.
    /// This function will slice input batch according to configured batch size and then
    /// shuffle rows into corresponding partition buffer.
    async fn insert_batch(&mut self, batch: RecordBatch) -> datafusion::common::Result<()> {
        self.spill_accounted_input_buffers.clear();
        let result = with_trace_async("shuffle_insert_batch", self.tracing_enabled, || async {
            let start_time = Instant::now();
            let mut start = 0;
            while start < batch.num_rows() {
                let end = (start + self.batch_size).min(batch.num_rows());
                let batch = batch.slice(start, end - start);
                self.partitioning_batch(batch).await?;
                start = end;
            }
            self.metrics.input_batches.add(1);
            self.metrics
                .baseline
                .elapsed_compute()
                .add_duration(start_time.elapsed());
            Ok(())
        })
        .await;
        self.spill_accounted_input_buffers.clear();
        result
    }

    /// Writes buffered shuffled record batches into Arrow IPC bytes.
    fn shuffle_write(&mut self) -> datafusion::common::Result<()> {
        with_trace("shuffle_write", self.tracing_enabled, || {
            let start_time = Instant::now();

            let mut partitioned_batches = self.partitioned_batches();
            self.pinned_buffers.clear();
            let num_output_partitions = self.partition_indices.len();

            #[allow(clippy::needless_range_loop)]
            for i in 0..num_output_partitions {
                self.partition_writer.finish_partition(
                    i,
                    &mut partitioned_batches.produce(i, &self.metrics.interleave_time),
                    &self.metrics,
                )?;
            }

            self.partition_writer.finish_all(&self.metrics)?;

            self.metrics
                .baseline
                .elapsed_compute()
                .add_duration(start_time.elapsed());
            Ok(())
        })
    }
}

impl<T: PartitionWriter> Debug for MultiPartitionShuffleRepartitioner<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShuffleRepartitioner")
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_count", &self.spill_count())
            .field("data_size", &self.data_size())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    #[derive(Default)]
    struct FailingPartitionWriter {
        fail: bool,
        write_calls: usize,
    }

    impl PartitionWriter for FailingPartitionWriter {
        fn write<I>(
            &mut self,
            _pid: usize,
            iter: &mut I,
            _metrics: &ShufflePartitionerMetrics,
        ) -> datafusion::common::Result<()>
        where
            I: Iterator<Item = datafusion::common::Result<RecordBatch>>,
        {
            self.write_calls += 1;
            if self.fail {
                return Err(DataFusionError::Execution(
                    "injected write failure".to_string(),
                ));
            }
            iter.try_for_each(|batch| batch.map(|_| ()))
        }

        fn finish_partition<I>(
            &mut self,
            pid: usize,
            iter: &mut I,
            metrics: &ShufflePartitionerMetrics,
        ) -> datafusion::common::Result<()>
        where
            I: Iterator<Item = datafusion::common::Result<RecordBatch>>,
        {
            self.write(pid, iter, metrics)
        }

        fn finish_all(
            &mut self,
            _metrics: &ShufflePartitionerMetrics,
        ) -> datafusion::common::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn spill_write_error_releases_buffered_memory() {
        let batch = RecordBatch::try_from_iter([(
            "a",
            Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        let buffer_bytes = batch.column(0).to_data().buffers()[0].capacity();
        let runtime = Arc::new(RuntimeEnv::default());
        let metrics_set = ExecutionPlanMetricsSet::new();
        let mut repartitioner = MultiPartitionShuffleRepartitioner::try_new(
            0,
            FailingPartitionWriter::default(),
            CometPartitioning::RoundRobin(2, 0),
            ShufflePartitionerMetrics::new(&metrics_set, 0),
            Arc::clone(&runtime),
            2,
            false,
            Some(1),
        )
        .unwrap();

        repartitioner
            .buffer_partitioned_batch_may_spill(batch.slice(0, 2), &[0, 1], &[0, 1, 2])
            .await
            .unwrap();
        assert_eq!(repartitioner.reservation.size(), 0);
        assert_eq!(runtime.memory_pool.reserved(), 0);
        assert!(repartitioner.pinned_buffers.is_empty());
        assert_eq!(repartitioner.spill_accounted_input_buffers.len(), 1);
        assert_eq!(repartitioner.repeated_spill_buffer_bytes, 0);
        assert!(repartitioner.buffered_batches.is_empty());
        assert!(repartitioner.partition_indices.iter().all(Vec::is_empty));
        assert_eq!(repartitioner.partition_writer.write_calls, 2);
        let successful_spill_bytes = repartitioner.metrics.memory_spilled_bytes.value();
        assert_eq!(repartitioner.spill_count(), 1);
        assert!(successful_spill_bytes > buffer_bytes);
        assert_eq!(repartitioner.spilled_bytes(), 0);
        assert_eq!(repartitioner.data_size(), buffer_bytes);

        repartitioner.max_buffer_bytes = None;
        repartitioner
            .buffer_partitioned_batch_may_spill(batch.slice(2, 2), &[0, 1], &[0, 1, 2])
            .await
            .unwrap();
        let reservation_before_failure = repartitioner.reservation.size();
        let repeated_before_failure = repartitioner.repeated_spill_buffer_bytes;
        let metrics_before_failure = (
            repartitioner.spill_count(),
            repartitioner.metrics.memory_spilled_bytes.value(),
            repartitioner.spilled_bytes(),
            repartitioner.data_size(),
        );
        assert_eq!(reservation_before_failure, successful_spill_bytes);
        assert_eq!(runtime.memory_pool.reserved(), reservation_before_failure);
        assert_eq!(repartitioner.pinned_buffers.len(), 1);
        assert_eq!(repartitioner.spill_accounted_input_buffers.len(), 1);
        assert_eq!(repeated_before_failure, buffer_bytes);
        assert_eq!(repartitioner.buffered_batches.len(), 1);
        assert_eq!(
            repartitioner
                .partition_indices
                .iter()
                .map(Vec::len)
                .sum::<usize>(),
            2
        );
        assert_eq!(repartitioner.partition_writer.write_calls, 2);
        assert_eq!(
            metrics_before_failure,
            (1, successful_spill_bytes, 0, buffer_bytes * 2)
        );

        repartitioner.partition_writer.fail = true;
        let error = repartitioner.spill(0).unwrap_err();
        assert!(matches!(
            error,
            DataFusionError::Execution(message) if message == "injected write failure"
        ));
        assert_eq!(repartitioner.reservation.size(), 0);
        assert_eq!(runtime.memory_pool.reserved(), 0);
        assert!(repartitioner.pinned_buffers.is_empty());
        assert_eq!(repartitioner.spill_accounted_input_buffers.len(), 1);
        assert_eq!(repartitioner.repeated_spill_buffer_bytes, 0);
        assert!(repartitioner.buffered_batches.is_empty());
        assert!(repartitioner.partition_indices.iter().all(Vec::is_empty));
        assert_eq!(repartitioner.partition_writer.write_calls, 3);
        assert_eq!(
            (
                repartitioner.spill_count(),
                repartitioner.metrics.memory_spilled_bytes.value(),
                repartitioner.spilled_bytes(),
                repartitioner.data_size(),
            ),
            (
                metrics_before_failure.0 + 1,
                metrics_before_failure.1 + reservation_before_failure - repeated_before_failure,
                metrics_before_failure.2,
                metrics_before_failure.3,
            )
        );
    }
}
