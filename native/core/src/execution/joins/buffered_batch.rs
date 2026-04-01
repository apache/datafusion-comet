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

//! Buffered batch management for the sort merge join operator.
//!
//! [`BufferedMatchGroup`] holds all rows from the buffered (right) side that
//! share the current join key. When memory is tight, individual batches are
//! spilled to Arrow IPC files on disk and reloaded on demand.

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::utils::memory::get_record_batch_memory_size;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::spill::SpillManager;

use super::metrics::SortMergeJoinMetrics;

/// State of a single buffered batch — either held in memory or spilled to disk.
#[derive(Debug)]
enum BatchState {
    /// The batch is available in memory.
    InMemory(RecordBatch),
    /// The batch has been spilled to an Arrow IPC file.
    Spilled(RefCountedTempFile),
}

/// A single batch in a [`BufferedMatchGroup`].
///
/// Tracks the batch data (in-memory or spilled), pre-evaluated join key arrays,
/// row count, estimated memory size, and per-row match flags for outer joins.
#[derive(Debug)]
pub(super) struct BufferedBatch {
    /// The batch data, either in memory or spilled to disk.
    state: BatchState,
    /// Pre-evaluated join key column arrays. `None` when the batch has been spilled.
    join_arrays: Option<Vec<ArrayRef>>,
    /// Number of rows in this batch (cached so we don't need the batch to know).
    pub num_rows: usize,
    /// Estimated memory footprint in bytes (batch + join arrays).
    pub size_estimate: usize,
    /// For full/right outer joins: tracks which rows have been matched.
    /// `None` for inner/left joins where unmatched tracking is not needed.
    pub matched: Option<Vec<bool>>,
}

impl BufferedBatch {
    /// Create a new in-memory buffered batch.
    ///
    /// `full_outer` controls whether per-row match tracking is allocated.
    fn new_in_memory(
        batch: RecordBatch,
        join_arrays: Vec<ArrayRef>,
        full_outer: bool,
    ) -> Self {
        let num_rows = batch.num_rows();
        let mut size_estimate = get_record_batch_memory_size(&batch);
        for arr in &join_arrays {
            size_estimate += arr.get_array_memory_size();
        }
        let matched = if full_outer {
            Some(vec![false; num_rows])
        } else {
            None
        };
        Self {
            state: BatchState::InMemory(batch),
            join_arrays: Some(join_arrays),
            num_rows,
            size_estimate,
            matched,
        }
    }

    /// Return the batch. If it was spilled, read it back from disk via the spill manager.
    pub fn get_batch(&self, spill_manager: &SpillManager) -> Result<RecordBatch> {
        match &self.state {
            BatchState::InMemory(batch) => Ok(batch.clone()),
            BatchState::Spilled(file) => {
                let reader =
                    spill_manager.read_spill_as_stream(file.clone(), None)?;
                let rt = tokio::runtime::Handle::current();
                let batches = rt.block_on(async {
                    use futures::StreamExt;
                    let mut stream = reader;
                    let mut batches = Vec::new();
                    while let Some(batch) = stream.next().await {
                        batches.push(batch?);
                    }
                    Ok::<_, DataFusionError>(batches)
                })?;
                // A single batch was spilled per file, but concatenate just in case.
                if batches.len() == 1 {
                    Ok(batches.into_iter().next().unwrap())
                } else {
                    arrow::compute::concat_batches(
                        &spill_manager.schema().clone(),
                        &batches,
                    )
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                }
            }
        }
    }

    /// Return join key arrays. If in memory, returns the cached arrays directly.
    /// If spilled, deserializes the batch and re-evaluates the join expressions.
    pub fn get_join_arrays(
        &self,
        spill_manager: &SpillManager,
        join_exprs: &[PhysicalExprRef],
    ) -> Result<Vec<ArrayRef>> {
        if let Some(ref arrays) = self.join_arrays {
            return Ok(arrays.clone());
        }
        // Spilled — reload and re-evaluate
        let batch = self.get_batch(spill_manager)?;
        evaluate_join_keys(&batch, join_exprs)
    }
}

/// A group of buffered batches that share the same join key values.
///
/// Batches may be held in memory or spilled to disk when the memory reservation
/// cannot accommodate them. When spilled, they can be loaded back on demand via
/// the spill manager.
#[derive(Debug)]
pub(super) struct BufferedMatchGroup {
    /// All batches in this match group.
    pub batches: Vec<BufferedBatch>,
    /// Total number of rows across all batches.
    pub num_rows: usize,
    /// Total estimated memory usage of in-memory batches.
    pub memory_size: usize,
}

impl BufferedMatchGroup {
    /// Create a new empty match group.
    pub fn new() -> Self {
        Self {
            batches: Vec::new(),
            num_rows: 0,
            memory_size: 0,
        }
    }

    /// Add a batch to this match group.
    ///
    /// First attempts to grow the memory reservation to hold the batch in memory.
    /// If that fails, the batch is spilled to disk via the spill manager and the
    /// spill metrics are updated accordingly.
    pub fn add_batch(
        &mut self,
        batch: RecordBatch,
        join_arrays: Vec<ArrayRef>,
        full_outer: bool,
        reservation: &mut MemoryReservation,
        spill_manager: &SpillManager,
        metrics: &SortMergeJoinMetrics,
    ) -> Result<()> {
        let buffered = BufferedBatch::new_in_memory(batch.clone(), join_arrays, full_outer);
        let size = buffered.size_estimate;
        let num_rows = buffered.num_rows;

        if reservation.try_grow(size).is_ok() {
            // Fits in memory
            self.memory_size += size;
            self.num_rows += num_rows;
            self.batches.push(buffered);
        } else {
            // Spill to disk
            let spill_file = spill_manager
                .spill_record_batch_and_finish(&[batch], "SortMergeJoin buffered batch")?;
            match spill_file {
                Some(file) => {
                    metrics.spill_count.add(1);
                    metrics.spilled_bytes.add(
                        std::fs::metadata(file.path())
                            .map(|m| m.len() as usize)
                            .unwrap_or(0),
                    );
                    metrics.spilled_rows.add(num_rows);
                    let matched = if full_outer {
                        Some(vec![false; num_rows])
                    } else {
                        None
                    };
                    self.num_rows += num_rows;
                    self.batches.push(BufferedBatch {
                        state: BatchState::Spilled(file),
                        join_arrays: None,
                        num_rows,
                        size_estimate: 0, // not consuming memory
                        matched,
                    });
                }
                None => {
                    // Empty batch, nothing to do
                }
            }
        }
        Ok(())
    }

    /// Clear all batches and release the memory reservation.
    pub fn clear(&mut self, reservation: &mut MemoryReservation) {
        self.batches.clear();
        reservation.shrink(self.memory_size);
        self.num_rows = 0;
        self.memory_size = 0;
    }

    /// Get a batch by index. If the batch was spilled, it is read back from disk.
    pub fn get_batch(
        &self,
        batch_idx: usize,
        spill_manager: &SpillManager,
    ) -> Result<RecordBatch> {
        self.batches[batch_idx].get_batch(spill_manager)
    }

    /// Returns `true` if this group contains no batches.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// Evaluate join key physical expressions against a record batch and return the
/// resulting column arrays.
pub(super) fn evaluate_join_keys(
    batch: &RecordBatch,
    join_exprs: &[PhysicalExprRef],
) -> Result<Vec<ArrayRef>> {
    join_exprs
        .iter()
        .map(|expr| {
            expr.evaluate(batch)
                .and_then(|cv| cv.into_array(batch.num_rows()))
        })
        .collect()
}
