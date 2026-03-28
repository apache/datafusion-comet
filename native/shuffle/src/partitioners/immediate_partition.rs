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

//! Immediate-mode shuffle partitioner: repartitions each incoming batch via Arrow `take`,
//! serializes per-partition slices into in-memory compressed IPC buffers, and spills to
//! disk under memory pressure.

use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::scratch::ScratchSpace;
use crate::partitioners::ShufflePartitioner;
use crate::writers::{write_index_file, BufBatchWriter};
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

struct PartitionBuffer {
    writer: BufBatchWriter<ShuffleBlockWriter, Cursor<Vec<u8>>>,
    spill_file: Option<SpillFile>,
}

struct SpillFile {
    /// Kept alive to prevent temp file deletion (RAII guard).
    temp_file: RefCountedTempFile,
    file: File,
}

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

    fn spill(&mut self) -> datafusion::common::Result<()> {
        if self.partition_buffers.iter().all(|pb| pb.is_none()) {
            return Ok(());
        }

        log::info!(
            "ImmediateShufflePartitioner spilling to disk ({} time(s) so far)",
            self.metrics.spill_count.value()
        );

        let mut spilled_bytes = 0usize;

        for pb in self.partition_buffers.iter_mut().flatten() {
            pb.writer
                .flush(&self.metrics.encode_time, &self.metrics.write_time)?;

            let buf = pb.writer.output_bytes();
            if buf.is_empty() {
                continue;
            }

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
                pb.spill_file = Some(SpillFile { temp_file, file });
            }

            let spill = pb.spill_file.as_mut().unwrap();
            let mut write_timer = self.metrics.write_time.timer();
            spill.file.write_all(buf)?;
            write_timer.stop();
            spilled_bytes += buf.len();

            pb.writer.reset_output_buffer();
        }

        self.reservation.free();
        self.metrics.spill_count.add(1);
        self.metrics.spilled_bytes.add(spilled_bytes);

        Ok(())
    }

    fn total_buffer_size(&self) -> usize {
        self.partition_buffers
            .iter()
            .filter_map(|pb| pb.as_ref())
            .map(|pb| pb.writer.buffered_output_size())
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

        let mem_growth = self.total_buffer_size().saturating_sub(size_before);
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
                    if let Some(mut spill) = pb.spill_file.take() {
                        spill.file.flush()?;
                        let mut spill_reader = File::open(spill.temp_file.path())?;
                        let mut write_timer = self.metrics.write_time.timer();
                        std::io::copy(&mut spill_reader, &mut output_data)?;
                        write_timer.stop();
                    }

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
            write_index_file(&self.output_index_file, &offsets)?;
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
