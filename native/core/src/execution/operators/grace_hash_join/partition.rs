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

//! Hash partitioning and per-partition state management for Grace Hash Join.
//!
//! Implements the prefix-sum hash-partitioning algorithm plus the Phase 1
//! (build-side) and Phase 2 (probe-side) streaming partitioners and their
//! spill bookkeeping. Also contains [`FinishedPartition`] and the
//! merge logic that runs between Phase 2 and Phase 3.

use std::sync::Arc;

use ahash::RandomState;
use arrow::array::UInt32Array;
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::StreamExt;
use log::info;

use super::metrics::GraceHashJoinMetrics;
use super::spill::SpillWriter;
use super::PROBE_PROGRESS_MILESTONE_ROWS;

/// Random state for hashing join keys into partitions. Uses fixed seeds
/// different from DataFusion's HashJoinExec to avoid correlation.
/// The `recursion_level` is XORed into the seed so that recursive
/// repartitioning uses different hash functions at each level.
fn partition_random_state(recursion_level: usize) -> RandomState {
    RandomState::with_seeds(
        0x517cc1b727220a95 ^ (recursion_level as u64),
        0x3a8b7c9d1e2f4056,
        0,
        0,
    )
}

// ---------------------------------------------------------------------------
// Per-partition state
// ---------------------------------------------------------------------------

/// Per-partition state tracking buffered data or spill writers.
pub(super) struct HashPartition {
    /// In-memory build-side batches for this partition.
    pub(super) build_batches: Vec<RecordBatch>,
    /// In-memory probe-side batches for this partition.
    pub(super) probe_batches: Vec<RecordBatch>,
    /// Incremental spill writer for build side (if spilling).
    pub(super) build_spill_writer: Option<SpillWriter>,
    /// Incremental spill writer for probe side (if spilling).
    pub(super) probe_spill_writer: Option<SpillWriter>,
    /// Approximate memory used by build-side batches in this partition.
    pub(super) build_mem_size: usize,
    /// Approximate memory used by probe-side batches in this partition.
    pub(super) probe_mem_size: usize,
}

impl HashPartition {
    pub(super) fn new() -> Self {
        Self {
            build_batches: Vec::new(),
            probe_batches: Vec::new(),
            build_spill_writer: None,
            probe_spill_writer: None,
            build_mem_size: 0,
            probe_mem_size: 0,
        }
    }

    /// Whether the build side has been spilled to disk.
    pub(super) fn build_spilled(&self) -> bool {
        self.build_spill_writer.is_some()
    }
}

// ---------------------------------------------------------------------------
// ScratchSpace: reusable buffers for efficient hash partitioning
// ---------------------------------------------------------------------------

/// Reusable scratch buffers for partitioning batches. Uses a prefix-sum
/// algorithm (borrowed from the shuffle `multi_partition.rs`) to compute
/// contiguous row-index regions per partition in a single pass, avoiding
/// N separate `take()` kernel calls.
#[derive(Default)]
pub(super) struct ScratchSpace {
    /// Hash values for each row.
    hashes: Vec<u64>,
    /// Partition id assigned to each row.
    partition_ids: Vec<u32>,
    /// Row indices reordered so that each partition's rows are contiguous.
    partition_row_indices: Vec<u32>,
    /// `partition_starts[k]..partition_starts[k+1]` gives the slice of
    /// `partition_row_indices` belonging to partition k.
    partition_starts: Vec<u32>,
}

impl ScratchSpace {
    /// Compute hashes and partition ids, then build the prefix-sum index
    /// structures for the given batch.
    pub(super) fn compute_partitions(
        &mut self,
        batch: &RecordBatch,
        keys: &[Arc<dyn PhysicalExpr>],
        num_partitions: usize,
        recursion_level: usize,
    ) -> DFResult<()> {
        let num_rows = batch.num_rows();

        // Evaluate key columns
        let key_columns: Vec<_> = keys
            .iter()
            .map(|expr| expr.evaluate(batch).and_then(|cv| cv.into_array(num_rows)))
            .collect::<DFResult<Vec<_>>>()?;

        // Hash. `create_hashes` XORs into the existing values, so the buffer
        // must be zeroed. `clear()` + `resize()` produces a fresh zeroed buffer
        // of the right length regardless of its previous size.
        self.hashes.clear();
        self.hashes.resize(num_rows, 0);
        let random_state = partition_random_state(recursion_level);
        create_hashes(&key_columns, &random_state, &mut self.hashes)?;

        // Assign partition ids
        self.partition_ids.resize(num_rows, 0);
        for (i, hash) in self.hashes[..num_rows].iter().enumerate() {
            self.partition_ids[i] = (*hash as u32) % (num_partitions as u32);
        }

        // Prefix-sum to get contiguous regions
        self.map_partition_ids_to_starts_and_indices(num_partitions, num_rows);

        Ok(())
    }

    /// Prefix-sum algorithm from `multi_partition.rs`.
    fn map_partition_ids_to_starts_and_indices(&mut self, num_partitions: usize, num_rows: usize) {
        let partition_ids = &self.partition_ids[..num_rows];

        // Count each partition size
        let partition_counters = &mut self.partition_starts;
        partition_counters.resize(num_partitions + 1, 0);
        partition_counters.fill(0);
        partition_ids
            .iter()
            .for_each(|pid| partition_counters[*pid as usize] += 1);

        // Accumulate into partition ends
        let mut accum = 0u32;
        for v in partition_counters.iter_mut() {
            *v += accum;
            accum = *v;
        }

        // Build partition_row_indices (iterate in reverse to turn ends into starts)
        self.partition_row_indices.resize(num_rows, 0);
        for (index, pid) in partition_ids.iter().enumerate().rev() {
            self.partition_starts[*pid as usize] -= 1;
            let pos = self.partition_starts[*pid as usize];
            self.partition_row_indices[pos as usize] = index as u32;
        }
    }

    /// Get the row index slice for a given partition.
    fn partition_slice(&self, partition_id: usize) -> &[u32] {
        let start = self.partition_starts[partition_id] as usize;
        let end = self.partition_starts[partition_id + 1] as usize;
        &self.partition_row_indices[start..end]
    }

    /// Number of rows in a given partition.
    pub(super) fn partition_len(&self, partition_id: usize) -> usize {
        (self.partition_starts[partition_id + 1] - self.partition_starts[partition_id]) as usize
    }

    pub(super) fn take_partition(
        &self,
        batch: &RecordBatch,
        partition_id: usize,
    ) -> DFResult<Option<RecordBatch>> {
        let row_indices = self.partition_slice(partition_id);
        if row_indices.is_empty() {
            return Ok(None);
        }
        let indices_array = UInt32Array::from(row_indices.to_vec());
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices_array, None))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(RecordBatch::try_new(batch.schema(), columns)?))
    }
}

// ---------------------------------------------------------------------------
// Optimistic build buffering (fast path)
// ---------------------------------------------------------------------------

/// Result of optimistic build-side buffering.
pub(super) enum BuildBufferResult {
    /// All build batches buffered successfully with memory tracking.
    Complete(Vec<RecordBatch>, usize),
    /// Memory pressure occurred — returns buffered batches and remaining stream.
    NeedPartition(Vec<RecordBatch>, SendableRecordBatchStream),
}

/// Buffer the build side without partitioning. Returns all batches and total bytes,
/// or signals memory pressure with the partially-buffered data and remaining stream.
pub(super) async fn buffer_build_optimistic(
    mut input: SendableRecordBatchStream,
    reservation: &mut MemoryReservation,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<BuildBufferResult> {
    let mut batches = Vec::new();
    let mut total_bytes = 0usize;

    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }

        metrics.build_input_batches.add(1);
        metrics.build_input_rows.add(batch.num_rows());

        let batch_size = batch.get_array_memory_size();

        if reservation.try_grow(batch_size).is_err() {
            // Memory pressure — return what we have and the remaining stream.
            // The caller will partition the buffered data and continue streaming.
            batches.push(batch);
            return Ok(BuildBufferResult::NeedPartition(batches, input));
        }

        total_bytes += batch_size;
        batches.push(batch);
    }

    Ok(BuildBufferResult::Complete(batches, total_bytes))
}

/// Partition already-buffered build batches into the partition structure.
/// Used when the optimistic fast path falls back to the slow path.
#[allow(clippy::too_many_arguments)]
pub(super) fn partition_from_buffer(
    batches: Vec<RecordBatch>,
    keys: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    schema: &SchemaRef,
    partitions: &mut [HashPartition],
    reservation: &mut MemoryReservation,
    context: &Arc<TaskContext>,
    metrics: &GraceHashJoinMetrics,
    scratch: &mut ScratchSpace,
) -> DFResult<()> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let total_rows = batch.num_rows();

        scratch.compute_partitions(&batch, keys, num_partitions, 0)?;

        #[allow(clippy::needless_range_loop)]
        for part_idx in 0..num_partitions {
            if scratch.partition_len(part_idx) == 0 {
                continue;
            }

            let sub_rows = scratch.partition_len(part_idx);
            let sub_batch = if sub_rows == total_rows {
                batch.clone()
            } else {
                scratch.take_partition(&batch, part_idx)?.unwrap()
            };
            let batch_size = sub_batch.get_array_memory_size();

            if partitions[part_idx].build_spilled() {
                if let Some(ref mut writer) = partitions[part_idx].build_spill_writer {
                    writer.write_batch(&sub_batch)?;
                }
            } else {
                if reservation.try_grow(batch_size).is_err() {
                    info!(
                        "GraceHashJoin: memory pressure during buffer partition, \
                         spilling largest partition"
                    );
                    spill_largest_partition(partitions, schema, context, reservation, metrics)?;

                    if reservation.try_grow(batch_size).is_err() {
                        spill_partition_build(
                            &mut partitions[part_idx],
                            schema,
                            context,
                            reservation,
                            metrics,
                        )?;
                        if let Some(ref mut writer) = partitions[part_idx].build_spill_writer {
                            writer.write_batch(&sub_batch)?;
                        }
                        continue;
                    }
                }

                partitions[part_idx].build_mem_size += batch_size;
                partitions[part_idx].build_batches.push(sub_batch);
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 1: Build-side partitioning
// ---------------------------------------------------------------------------

/// Phase 1: Read all build-side batches, hash-partition into N buckets.
/// Spills the largest partition when memory pressure is detected.
#[allow(clippy::too_many_arguments)]
pub(super) async fn partition_build_side(
    mut input: SendableRecordBatchStream,
    keys: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    schema: &SchemaRef,
    partitions: &mut [HashPartition],
    reservation: &mut MemoryReservation,
    context: &Arc<TaskContext>,
    metrics: &GraceHashJoinMetrics,
    scratch: &mut ScratchSpace,
) -> DFResult<()> {
    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }

        metrics.build_input_batches.add(1);
        metrics.build_input_rows.add(batch.num_rows());

        let total_rows = batch.num_rows();

        scratch.compute_partitions(&batch, keys, num_partitions, 0)?;

        #[allow(clippy::needless_range_loop)]
        for part_idx in 0..num_partitions {
            if scratch.partition_len(part_idx) == 0 {
                continue;
            }

            let sub_rows = scratch.partition_len(part_idx);
            let sub_batch = if sub_rows == total_rows {
                batch.clone()
            } else {
                scratch.take_partition(&batch, part_idx)?.unwrap()
            };
            let batch_size = sub_batch.get_array_memory_size();

            if partitions[part_idx].build_spilled() {
                // This partition is already spilled; append incrementally
                if let Some(ref mut writer) = partitions[part_idx].build_spill_writer {
                    writer.write_batch(&sub_batch)?;
                }
            } else {
                // Try to reserve memory
                if reservation.try_grow(batch_size).is_err() {
                    // Memory pressure: spill the largest in-memory partition
                    info!(
                        "GraceHashJoin: memory pressure during build, spilling largest partition"
                    );
                    spill_largest_partition(partitions, schema, context, reservation, metrics)?;

                    // Retry reservation after spilling
                    if reservation.try_grow(batch_size).is_err() {
                        // Still can't fit; spill this partition too
                        info!(
                            "GraceHashJoin: still under pressure, spilling partition {}",
                            part_idx
                        );
                        spill_partition_build(
                            &mut partitions[part_idx],
                            schema,
                            context,
                            reservation,
                            metrics,
                        )?;
                        if let Some(ref mut writer) = partitions[part_idx].build_spill_writer {
                            writer.write_batch(&sub_batch)?;
                        }
                        continue;
                    }
                }

                partitions[part_idx].build_mem_size += batch_size;
                partitions[part_idx].build_batches.push(sub_batch);
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Spill helpers
// ---------------------------------------------------------------------------

/// Spill the largest in-memory build partition to disk.
fn spill_largest_partition(
    partitions: &mut [HashPartition],
    schema: &SchemaRef,
    context: &Arc<TaskContext>,
    reservation: &mut MemoryReservation,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<()> {
    // Find the largest non-spilled partition
    let largest_idx = partitions
        .iter()
        .enumerate()
        .filter(|(_, p)| !p.build_spilled() && !p.build_batches.is_empty())
        .max_by_key(|(_, p)| p.build_mem_size)
        .map(|(idx, _)| idx);

    if let Some(idx) = largest_idx {
        info!(
            "GraceHashJoin: spilling partition {} ({} bytes, {} batches)",
            idx,
            partitions[idx].build_mem_size,
            partitions[idx].build_batches.len()
        );
        spill_partition_build(&mut partitions[idx], schema, context, reservation, metrics)?;
    }

    Ok(())
}

/// Spill a single partition's build-side data to disk using SpillWriter.
fn spill_partition_build(
    partition: &mut HashPartition,
    schema: &SchemaRef,
    context: &Arc<TaskContext>,
    reservation: &mut MemoryReservation,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<()> {
    let temp_file = context
        .runtime_env()
        .disk_manager
        .create_tmp_file("grace hash join build")?;

    let mut writer = SpillWriter::new(temp_file, schema)?;
    writer.write_batches(&partition.build_batches)?;

    // Free memory
    let freed = partition.build_mem_size;
    reservation.shrink(freed);

    metrics.spill_count.add(1);
    metrics.spilled_bytes.add(freed);

    partition.build_spill_writer = Some(writer);
    partition.build_batches.clear();
    partition.build_mem_size = 0;

    Ok(())
}

/// Spill a single partition's probe-side data to disk using SpillWriter.
fn spill_partition_probe(
    partition: &mut HashPartition,
    schema: &SchemaRef,
    context: &Arc<TaskContext>,
    reservation: &mut MemoryReservation,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<()> {
    if partition.probe_batches.is_empty() && partition.probe_spill_writer.is_some() {
        return Ok(());
    }

    let temp_file = context
        .runtime_env()
        .disk_manager
        .create_tmp_file("grace hash join probe")?;

    let mut writer = SpillWriter::new(temp_file, schema)?;
    writer.write_batches(&partition.probe_batches)?;

    let freed = partition.probe_mem_size;
    reservation.shrink(freed);

    metrics.spill_count.add(1);
    metrics.spilled_bytes.add(freed);

    partition.probe_spill_writer = Some(writer);
    partition.probe_batches.clear();
    partition.probe_mem_size = 0;

    Ok(())
}

/// Spill both build and probe sides of a partition to disk.
/// When spilling during the probe phase, both sides must be spilled so the
/// join phase reads both consistently from disk.
fn spill_partition_both_sides(
    partition: &mut HashPartition,
    probe_schema: &SchemaRef,
    build_schema: &SchemaRef,
    context: &Arc<TaskContext>,
    reservation: &mut MemoryReservation,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<()> {
    if !partition.build_spilled() {
        spill_partition_build(partition, build_schema, context, reservation, metrics)?;
    }
    if partition.probe_spill_writer.is_none() {
        spill_partition_probe(partition, probe_schema, context, reservation, metrics)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 2: Probe-side partitioning
// ---------------------------------------------------------------------------

/// Phase 2: Read all probe-side batches, route to in-memory buffers or spill files.
/// Tracks probe-side memory in the reservation and spills partitions when pressure
/// is detected, preventing OOM when the probe side is much larger than the build side.
#[allow(clippy::too_many_arguments)]
pub(super) async fn partition_probe_side(
    mut input: SendableRecordBatchStream,
    keys: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    schema: &SchemaRef,
    partitions: &mut [HashPartition],
    reservation: &mut MemoryReservation,
    build_schema: &SchemaRef,
    context: &Arc<TaskContext>,
    metrics: &GraceHashJoinMetrics,
    scratch: &mut ScratchSpace,
) -> DFResult<()> {
    let mut probe_rows_accumulated: usize = 0;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let prev_milestone = probe_rows_accumulated / PROBE_PROGRESS_MILESTONE_ROWS;
        probe_rows_accumulated += batch.num_rows();
        let new_milestone = probe_rows_accumulated / PROBE_PROGRESS_MILESTONE_ROWS;
        if new_milestone > prev_milestone {
            info!(
                "GraceHashJoin: probe accumulation progress: {} rows, \
                 reservation={}, pool reserved={}",
                probe_rows_accumulated,
                reservation.size(),
                context.runtime_env().memory_pool.reserved(),
            );
        }

        metrics.input_batches.add(1);
        metrics.input_rows.add(batch.num_rows());

        let total_rows = batch.num_rows();
        scratch.compute_partitions(&batch, keys, num_partitions, 0)?;

        #[allow(clippy::needless_range_loop)]
        for part_idx in 0..num_partitions {
            if scratch.partition_len(part_idx) == 0 {
                continue;
            }

            let sub_batch = if scratch.partition_len(part_idx) == total_rows {
                batch.clone()
            } else {
                scratch.take_partition(&batch, part_idx)?.unwrap()
            };

            if partitions[part_idx].build_spilled() {
                // Build side was spilled, so spill probe side too
                if partitions[part_idx].probe_spill_writer.is_none() {
                    let temp_file = context
                        .runtime_env()
                        .disk_manager
                        .create_tmp_file("grace hash join probe")?;
                    let mut writer = SpillWriter::new(temp_file, schema)?;
                    // Write any accumulated in-memory probe batches first
                    if !partitions[part_idx].probe_batches.is_empty() {
                        let freed = partitions[part_idx].probe_mem_size;
                        let batches = std::mem::take(&mut partitions[part_idx].probe_batches);
                        writer.write_batches(&batches)?;
                        partitions[part_idx].probe_mem_size = 0;
                        reservation.shrink(freed);
                    }
                    partitions[part_idx].probe_spill_writer = Some(writer);
                }
                if let Some(ref mut writer) = partitions[part_idx].probe_spill_writer {
                    writer.write_batch(&sub_batch)?;
                }
            } else {
                let batch_size = sub_batch.get_array_memory_size();
                if reservation.try_grow(batch_size).is_err() {
                    // Memory pressure: spill ALL non-spilled partitions.
                    // With multiple concurrent GHJ instances sharing the pool,
                    // partial spilling just lets data re-accumulate. Spilling
                    // everything ensures all subsequent probe data goes directly
                    // to disk, keeping in-memory footprint near zero.
                    let total_in_memory: usize = partitions
                        .iter()
                        .filter(|p| !p.build_spilled())
                        .map(|p| p.build_mem_size + p.probe_mem_size)
                        .sum();
                    let spillable_count = partitions.iter().filter(|p| !p.build_spilled()).count();

                    info!(
                        "GraceHashJoin: memory pressure during probe, \
                         spilling all {} non-spilled partitions ({} bytes)",
                        spillable_count, total_in_memory,
                    );

                    for i in 0..partitions.len() {
                        if !partitions[i].build_spilled() {
                            spill_partition_both_sides(
                                &mut partitions[i],
                                schema,
                                build_schema,
                                context,
                                reservation,
                                metrics,
                            )?;
                        }
                    }
                }

                if partitions[part_idx].build_spilled() {
                    // Partition was just spilled above — write to spill writer
                    if partitions[part_idx].probe_spill_writer.is_none() {
                        let temp_file = context
                            .runtime_env()
                            .disk_manager
                            .create_tmp_file("grace hash join probe")?;
                        partitions[part_idx].probe_spill_writer =
                            Some(SpillWriter::new(temp_file, schema)?);
                    }
                    if let Some(ref mut writer) = partitions[part_idx].probe_spill_writer {
                        writer.write_batch(&sub_batch)?;
                    }
                } else {
                    partitions[part_idx].probe_mem_size += batch_size;
                    partitions[part_idx].probe_batches.push(sub_batch);
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Finish spill writers + merge adjacent partitions
// ---------------------------------------------------------------------------

/// State of a finished partition ready for joining.
/// After merging, a partition may hold multiple spill files from adjacent
/// original partitions.
pub(super) struct FinishedPartition {
    pub(super) build_batches: Vec<RecordBatch>,
    pub(super) probe_batches: Vec<RecordBatch>,
    pub(super) build_spill_files: Vec<RefCountedTempFile>,
    pub(super) probe_spill_files: Vec<RefCountedTempFile>,
    /// Total build-side bytes (in-memory + spilled) for merge decisions.
    pub(super) build_bytes: usize,
}

/// Finish all open spill writers so files can be read back.
pub(super) fn finish_spill_writers(
    partitions: Vec<HashPartition>,
) -> DFResult<Vec<FinishedPartition>> {
    let mut finished = Vec::with_capacity(partitions.len());

    for partition in partitions {
        let (build_spill_files, spilled_build_bytes) =
            if let Some(writer) = partition.build_spill_writer {
                let (file, bytes) = writer.finish()?;
                (vec![file], bytes)
            } else {
                (vec![], 0)
            };

        let probe_spill_files = if let Some(writer) = partition.probe_spill_writer {
            let (file, _bytes) = writer.finish()?;
            vec![file]
        } else {
            vec![]
        };

        finished.push(FinishedPartition {
            build_bytes: partition.build_mem_size + spilled_build_bytes,
            build_batches: partition.build_batches,
            probe_batches: partition.probe_batches,
            build_spill_files,
            probe_spill_files,
        });
    }

    Ok(finished)
}

/// Merge adjacent finished partitions to reduce the number of per-partition
/// HashJoinExec calls. Groups adjacent partitions so each merged group has
/// roughly `TARGET_PARTITION_BUILD_SIZE` bytes of build data.
pub(super) fn merge_finished_partitions(
    partitions: Vec<FinishedPartition>,
    target_count: usize,
) -> Vec<FinishedPartition> {
    let original_count = partitions.len();
    if target_count >= original_count {
        return partitions;
    }

    // Divide original_count partitions into target_count groups as evenly as possible
    let base_group_size = original_count / target_count;
    let remainder = original_count % target_count;

    let mut merged = Vec::with_capacity(target_count);
    let mut iter = partitions.into_iter();

    for group_idx in 0..target_count {
        // First `remainder` groups get one extra partition
        let group_size = base_group_size + if group_idx < remainder { 1 } else { 0 };

        let mut build_batches = Vec::new();
        let mut probe_batches = Vec::new();
        let mut build_spill_files = Vec::new();
        let mut probe_spill_files = Vec::new();
        let mut build_bytes = 0usize;

        for _ in 0..group_size {
            if let Some(p) = iter.next() {
                build_batches.extend(p.build_batches);
                probe_batches.extend(p.probe_batches);
                build_spill_files.extend(p.build_spill_files);
                probe_spill_files.extend(p.probe_spill_files);
                build_bytes += p.build_bytes;
            }
        }

        merged.push(FinishedPartition {
            build_batches,
            probe_batches,
            build_spill_files,
            probe_spill_files,
            build_bytes,
        });
    }

    merged
}

// ---------------------------------------------------------------------------
// Recursive sub-partitioning (used by exec's repartition_and_join)
// ---------------------------------------------------------------------------

/// Distribute batches into sub-partitions by hashing key columns.
pub(super) fn sub_partition_batches(
    batches: &[RecordBatch],
    keys: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    recursion_level: usize,
    scratch: &mut ScratchSpace,
) -> DFResult<Vec<Vec<RecordBatch>>> {
    let mut result: Vec<Vec<RecordBatch>> = (0..num_partitions).map(|_| Vec::new()).collect();
    for batch in batches {
        let total_rows = batch.num_rows();
        scratch.compute_partitions(batch, keys, num_partitions, recursion_level)?;
        for (i, sub_vec) in result.iter_mut().enumerate() {
            if scratch.partition_len(i) == 0 {
                continue;
            }
            if scratch.partition_len(i) == total_rows {
                sub_vec.push(batch.clone());
            } else if let Some(sub) = scratch.take_partition(batch, i)? {
                sub_vec.push(sub);
            }
        }
    }
    Ok(result)
}
