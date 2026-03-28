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
//! buffers. At `shuffle_write` time, the buffers are concatenated into the final
//! shuffle data file and index.
//!
//! Because the buffers hold compressed IPC (typically 5-20% of raw data size), the
//! memory footprint is much lower than holding raw Arrow batches.

use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::scratch::ScratchSpace;
use crate::partitioners::ShufflePartitioner;
use crate::writers::BufBatchWriter;
use crate::{CometPartitioning, CompressionCodec, ShuffleBlockWriter};
use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion_comet_common::tracing::{with_trace, with_trace_async};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, Seek, Write};
use tokio::time::Instant;

/// Per-partition in-memory buffer holding compressed IPC blocks.
struct PartitionBuffer {
    writer: BufBatchWriter<ShuffleBlockWriter, Cursor<Vec<u8>>>,
}

/// An immediate-mode shuffle partitioner. Each incoming batch is repartitioned using
/// `arrow::compute::take` and the per-partition slices are serialized immediately into
/// per-partition `Vec<u8>` buffers (compressed IPC). No input `RecordBatch` objects are
/// retained in memory beyond the current one.
pub(crate) struct ImmediateShufflePartitioner {
    output_data_file: String,
    output_index_file: String,
    partition_buffers: Vec<Option<PartitionBuffer>>,
    shuffle_block_writer: ShuffleBlockWriter,
    partitioning: CometPartitioning,
    metrics: ShufflePartitionerMetrics,
    scratch: ScratchSpace,
    batch_size: usize,
    tracing_enabled: bool,
    write_buffer_size: usize,
}

impl ImmediateShufflePartitioner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: CometPartitioning,
        metrics: ShufflePartitionerMetrics,
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

        Ok(Self {
            output_data_file,
            output_index_file,
            partition_buffers,
            shuffle_block_writer,
            partitioning,
            metrics,
            scratch,
            batch_size,
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
            self.partition_buffers[partition_id] = Some(PartitionBuffer { writer });
        }
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
                    pb.writer
                        .flush(&self.metrics.encode_time, &self.metrics.write_time)?;

                    let buf = pb.writer.into_writer().into_inner();
                    let mut write_timer = self.metrics.write_time.timer();
                    output_data.write_all(&buf)?;
                    write_timer.stop();
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
            .finish()
    }
}
