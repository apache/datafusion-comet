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

//! An immediate-mode shuffle partitioner that repartitions incoming batches and writes
//! per-partition data to in-memory buffers containing compressed IPC blocks. Unlike
//! `MultiPartitionShuffleRepartitioner`, this implementation does not buffer raw Arrow
//! `RecordBatch` objects — it uses Arrow `take` to extract per-partition slices and
//! serializes them immediately through per-partition `BufBatchWriter`s into `Vec<u8>`
//! buffers. Under memory pressure, buffers are spilled to per-partition temp files.
//! At `shuffle_write` time, spill files (if any) and remaining in-memory buffers are
//! concatenated into the final shuffle data file and index.

use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::scratch::ScratchSpace;
use crate::partitioners::ShufflePartitioner;
use crate::writers::BufBatchWriter;
use crate::{CometPartitioning, CompressionCodec, ShuffleBlockWriter};
use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_comet_common::tracing::{with_trace, with_trace_async};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, Seek, Write};
use std::sync::Arc;
use tokio::time::Instant;

/// Per-partition state: in-memory buffer for compressed IPC, plus an optional
/// spill file from prior memory-pressure events.
struct PartitionBuffer {
    writer: BufBatchWriter<ShuffleBlockWriter, Cursor<Vec<u8>>>,
    /// Spill file accumulating data from prior spill events. Created lazily
    /// on first spill. Subsequent spills append to the same file.
    spill_file: Option<SpillFile>,
}

struct SpillFile {
    _temp_file: RefCountedTempFile,
    file: File,
}

/// An immediate-mode shuffle partitioner with memory accounting and spilling.
/// Each incoming batch is repartitioned using `arrow::compute::take` and the
/// per-partition slices are serialized into per-partition `Vec<u8>` buffers
/// (compressed IPC). Under memory pressure, buffers are spilled to temp files.
pub(crate) struct ImmediateShufflePartitioner {
    output_data_file: String,
    output_index_file: String,
    partition_buffers: Vec<Option<PartitionBuffer>>,
    shuffle_block_writer: ShuffleBlockWriter,
    partitioning: CometPartitioning,
    runtime: Arc<RuntimeEnv>,
    metrics: ShufflePartitionerMetrics,
    scratch: ScratchSpace,
    batch_size: usize,
    reservation: MemoryReservation,
    tracing_enabled: bool,
    write_buffer_size: usize,
}

impl ImmediateShufflePartitioner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        partition: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: CometPartitioning,
        metrics: ShufflePartitionerMetrics,
        runtime: Arc<RuntimeEnv>,
        batch_size: usize,
        codec: CompressionCodec,
        tracing_enabled: bool,
        write_buffer_size: usize,
    ) -> datafusion::common::Result<Self> {
        let num_output_partitions = partitioning.partition_count();
        assert!(
            num_output_partitions > 1,
            "Use SinglePartitionShufflePartitioner for 1 output partition."
        );

        let scratch = ScratchSpace::new(&partitioning, batch_size, num_output_partitions);
        let shuffle_block_writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec)?;
        let partition_buffers = (0..num_output_partitions).map(|_| None).collect();

        let reservation = MemoryConsumer::new(format!("ImmediateShufflePartitioner[{partition}]"))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        Ok(Self {
            output_data_file,
            output_index_file,
            partition_buffers,
            shuffle_block_writer,
            partitioning,
            runtime,
            metrics,
            scratch,
            batch_size,
            reservation,
            tracing_enabled,
            write_buffer_size,
        })
    }

    fn ensure_partition_buffer(&mut self, partition_id: usize) {
        if self.partition_buffers[partition_id].is_none() {
            let writer = BufBatchWriter::new(
                self.shuffle_block_writer.clone(),
                Cursor::new(Vec::new()),
                self.write_buffer_size,
                self.batch_size,
            );
            self.partition_buffers[partition_id] = Some(PartitionBuffer {
                writer,
                spill_file: None,
            });
        }
    }

    /// Spill all in-memory partition buffers to disk.
    fn spill(&mut self) -> datafusion::common::Result<()> {
        let total_buffered: usize = self
            .partition_buffers
            .iter()
            .filter_map(|pb| pb.as_ref())
            .map(|pb| pb.writer.inner_buffer_len())
            .sum();

        if total_buffered == 0 {
            return Ok(());
        }

        log::info!(
            "ImmediateShufflePartitioner spilling {} to disk ({} time(s) so far)",
            total_buffered,
            self.metrics.spill_count.value()
        );

        let mut spilled_bytes = 0usize;

        for pb in self.partition_buffers.iter_mut().flatten() {
            // Flush the BufBatchWriter's coalescer into the Cursor<Vec<u8>>
            pb.writer
                .flush(&self.metrics.encode_time, &self.metrics.write_time)?;

            let buf = pb.writer.inner_mut().get_ref();
            if buf.is_empty() {
                continue;
            }

            // Ensure spill file exists
            if pb.spill_file.is_none() {
                let temp_file = self
                    .runtime
                    .disk_manager
                    .create_tmp_file("immediate_shuffle_spill")?;
                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(temp_file.path())
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Error creating spill file: {e}"))
                    })?;
                pb.spill_file = Some(SpillFile {
                    _temp_file: temp_file,
                    file,
                });
            }

            // Append buffer contents to spill file
            let spill = pb.spill_file.as_mut().unwrap();
            let mut write_timer = self.metrics.write_time.timer();
            spill.file.write_all(buf)?;
            write_timer.stop();
            spilled_bytes += buf.len();

            // Reset the in-memory buffer (keep allocated capacity for reuse)
            let cursor = pb.writer.inner_mut();
            cursor.get_mut().clear();
            cursor.set_position(0);
        }

        self.reservation.free();
        self.metrics.spill_count.add(1);
        self.metrics.spilled_bytes.add(spilled_bytes);

        Ok(())
    }

    /// Returns the total size of in-memory buffers across all partitions.
    /// Includes both the BufBatchWriter staging buffer and the Cursor<Vec<u8>> output.
    fn total_buffer_size(&self) -> usize {
        self.partition_buffers
            .iter()
            .filter_map(|pb| pb.as_ref())
            .map(|pb| pb.writer.inner_buffer_len() + pb.writer.inner_ref().get_ref().len())
            .sum()
    }

    fn partitioning_batch(&mut self, input: RecordBatch) -> datafusion::common::Result<()> {
        if input.num_rows() == 0 {
            return Ok(());
        }

        if input.num_rows() > self.batch_size {
            return Err(DataFusionError::Internal(
                "Input batch size exceeds configured batch size. Call `insert_batch` instead."
                    .to_string(),
            ));
        }

        self.metrics.data_size.add(input.get_array_memory_size());
        self.metrics.baseline.record_output(input.num_rows());

        let num_output_partitions = self.partitioning.partition_count();

        let mut scratch = std::mem::take(&mut self.scratch);
        {
            let mut timer = self.metrics.repart_time.timer();
            scratch.compute_partition_ids(&self.partitioning, &input)?;
            timer.stop();
        }

        // Single take per column to reorder entire batch by partition assignment,
        // then zero-copy slice per partition. This replaces P*C take calls with just C.
        let num_rows = input.num_rows();
        let all_indices = UInt32Array::from_iter_values(
            scratch.partition_row_indices[..num_rows].iter().copied(),
        );
        let sorted_columns: Vec<ArrayRef> = input
            .columns()
            .iter()
            .map(|col| {
                take(col, &all_indices, None)
                    .map_err(|e| DataFusionError::ArrowError(Box::from(e), None))
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        let sorted_batch = RecordBatch::try_new(input.schema(), sorted_columns)?;

        let size_before = self.total_buffer_size();

        for partition_id in 0..num_output_partitions {
            let start = scratch.partition_starts[partition_id] as usize;
            let end = scratch.partition_starts[partition_id + 1] as usize;
            if start == end {
                continue;
            }

            let partition_batch = sorted_batch.slice(start, end - start);

            self.ensure_partition_buffer(partition_id);
            let pb = self.partition_buffers[partition_id].as_mut().unwrap();
            pb.writer.write(
                &partition_batch,
                &self.metrics.encode_time,
                &self.metrics.write_time,
            )?;
        }

        let size_after = self.total_buffer_size();
        let mem_growth = size_after.saturating_sub(size_before);
        if mem_growth > 0 && self.reservation.try_grow(mem_growth).is_err() {
            self.spill()?;
        }

        self.scratch = scratch;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ShufflePartitioner for ImmediateShufflePartitioner {
    async fn insert_batch(&mut self, batch: RecordBatch) -> datafusion::common::Result<()> {
        with_trace_async(
            "immediate_shuffle_insert_batch",
            self.tracing_enabled,
            || async {
                let start_time = Instant::now();
                let mut start = 0;
                while start < batch.num_rows() {
                    let end = (start + self.batch_size).min(batch.num_rows());
                    let slice = batch.slice(start, end - start);
                    self.partitioning_batch(slice)?;
                    start = end;
                }
                self.metrics.input_batches.add(1);
                self.metrics
                    .baseline
                    .elapsed_compute()
                    .add_duration(start_time.elapsed());
                Ok(())
            },
        )
        .await
    }

    fn shuffle_write(&mut self) -> datafusion::common::Result<()> {
        with_trace("immediate_shuffle_write", self.tracing_enabled, || {
            let start_time = Instant::now();

            let num_output_partitions = self.partition_buffers.len();
            let mut offsets = vec![0u64; num_output_partitions + 1];

            let output_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&self.output_data_file)
                .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;

            let mut output_data = BufWriter::new(output_data);

            for (partition_id, pb_slot) in self.partition_buffers.iter_mut().enumerate() {
                offsets[partition_id] = output_data.stream_position()?;

                if let Some(mut pb) = pb_slot.take() {
                    // Copy spill file contents first (from prior spill events)
                    if let Some(mut spill) = pb.spill_file.take() {
                        spill.file.flush()?;
                        let spill_path = spill._temp_file.path().to_path_buf();
                        let mut spill_reader = File::open(&spill_path)?;
                        let mut write_timer = self.metrics.write_time.timer();
                        std::io::copy(&mut spill_reader, &mut output_data)?;
                        write_timer.stop();
                    }

                    // Flush and write remaining in-memory buffer
                    pb.writer
                        .flush(&self.metrics.encode_time, &self.metrics.write_time)?;

                    let buf = pb.writer.into_writer().into_inner();
                    if !buf.is_empty() {
                        let mut write_timer = self.metrics.write_time.timer();
                        output_data.write_all(&buf)?;
                        write_timer.stop();
                    }
                }
            }

            let mut write_timer = self.metrics.write_time.timer();
            output_data.flush()?;
            write_timer.stop();

            offsets[num_output_partitions] = output_data.stream_position()?;

            let mut write_timer = self.metrics.write_time.timer();
            let mut output_index =
                BufWriter::new(File::create(&self.output_index_file).map_err(|e| {
                    DataFusionError::Execution(format!("shuffle write error: {e:?}"))
                })?);
            for offset in offsets {
                output_index.write_all(&(offset as i64).to_le_bytes()[..])?;
            }
            output_index.flush()?;
            write_timer.stop();

            self.metrics
                .baseline
                .elapsed_compute()
                .add_duration(start_time.elapsed());

            Ok(())
        })
    }
}

impl Debug for ImmediateShufflePartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImmediateShufflePartitioner")
            .field("num_partitions", &self.partition_buffers.len())
            .field("memory_used", &self.reservation.size())
            .finish()
    }
}
