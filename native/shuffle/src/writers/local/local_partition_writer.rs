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
use crate::writers::local::spill::SpillWriter;
use crate::writers::partition_writer::PartitionWriter;
use crate::writers::BufBatchWriter;
use crate::ShuffleBlockWriter;
use arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, Write};
use std::sync::Arc;

/// Output target for the shuffle data file.
///
/// The two shuffle modes drive the writer differently:
///
/// * Single-partition shuffles stream every batch through a single long-lived
///   [`BufBatchWriter`]. Keeping it alive across `write` calls preserves
///   cross-batch coalescing in the internal `BatchCoalescer` and limits
///   flushing (which also finalizes partially coalesced batches) to
///   [`PartitionWriter::finish_all`].
/// * Multi-partition shuffles finalize one partition at a time in
///   [`PartitionWriter::finish_partition`], each with its own short-lived
///   `BufBatchWriter`, so coalescing intentionally does not cross partition
///   boundaries. They hold the raw output writer and block writer directly.
enum DataOutput {
    /// Single-partition output: one long-lived writer streams all batches.
    Single(BufBatchWriter<ShuffleBlockWriter, File>),
    /// Multi-partition output: batches are staged per partition and merged into
    /// `output_writer` one partition at a time during `finish_partition`.
    Multi {
        output_writer: BufWriter<File>,
        shuffle_block_writer: ShuffleBlockWriter,
        /// One spill file per output partition, buffered until `finish_partition`
        /// merges them into the shuffle output.
        spill_writers: Vec<SpillWriter>,
        /// Runtime used to allocate the temporary spill files.
        runtime: Arc<RuntimeEnv>,
    },
}

/// Local file-based [`PartitionWriter`] implementation.
///
/// Writes shuffle output to a single data file plus an index file recording the
/// byte offset where each partition begins. See [`DataOutput`] for how the
/// single- and multi-partition modes differ.
pub(crate) struct LocalPartitionWriter {
    output_index_file: String,
    data_output: DataOutput,
    /// Start offset of each partition in the data file, plus a trailing entry
    /// with the total length so partition sizes are simple offset differences.
    /// Has `num_output_partitions + 1` elements.
    offsets: Vec<u64>,
    batch_size: usize,
    write_buffer_size: usize,
    num_output_partitions: usize,
    /// Id of the last partition passed to `finish_partition`, used to assert
    /// partitions are finalized in ascending order. `-1` before any call.
    last_finish_pid: i32,
}

impl LocalPartitionWriter {
    pub(crate) fn try_new(
        output_data_file: String,
        output_index_file: String,
        shuffle_block_writer: ShuffleBlockWriter,
        num_output_partitions: usize,
        batch_size: usize,
        write_buffer_size: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> datafusion::common::Result<Self> {
        let output_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_data_file.clone())
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;

        let data_output = if num_output_partitions == 1 {
            DataOutput::Single(BufBatchWriter::new(
                shuffle_block_writer,
                output_file,
                write_buffer_size,
                batch_size,
            ))
        } else {
            let output_writer = BufWriter::with_capacity(write_buffer_size, output_file);
            let spill_writers = (0..num_output_partitions)
                .map(|_| {
                    SpillWriter::try_new(
                        shuffle_block_writer.clone(),
                        write_buffer_size,
                        batch_size,
                    )
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            DataOutput::Multi {
                output_writer,
                shuffle_block_writer,
                spill_writers,
                runtime,
            }
        };
        Ok(Self {
            output_index_file,
            data_output,
            offsets: vec![0u64; num_output_partitions + 1],
            batch_size,
            write_buffer_size,
            num_output_partitions,
            last_finish_pid: -1,
        })
    }

    #[cfg(test)]
    pub(crate) fn get_spill_writers(&self) -> &Vec<SpillWriter> {
        match &self.data_output {
            DataOutput::Multi { spill_writers, .. } => spill_writers,
            DataOutput::Single(_) => panic!("single-partition output has no spill writers"),
        }
    }
}

impl PartitionWriter for LocalPartitionWriter {
    fn write<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()>
    where
        I: Iterator<Item = datafusion::common::Result<RecordBatch>>,
    {
        match &mut self.data_output {
            DataOutput::Single(writer) => {
                if pid != 0 {
                    return Err(DataFusionError::Execution(
                        "LocalPartitionWriter single-partition output only supports partition 0."
                            .to_string(),
                    ));
                }

                // Stream batches through the long-lived writer so small batches keep
                // coalescing across calls. Do not flush here: flushing also finalizes any
                // partially coalesced batch, which would defeat cross-call coalescing and
                // increase flush frequency. The single-partition writer is flushed once, in
                // `finish_all`.
                for batch in iter.by_ref() {
                    let batch = batch?;
                    writer.write(&batch, &metrics.encode_time, &metrics.write_time)?;
                }
            }
            DataOutput::Multi {
                spill_writers,
                runtime,
                ..
            } => {
                // Multi-partition output buffers each partition's batches into its own
                // spill file. `finish_partition` later merges the spill files (and any
                // remaining in-memory batches) into the shuffle output in partition order.
                spill_writers[pid].write(iter, runtime, metrics)?;
            }
        }

        Ok(())
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
        assert_eq!(
            pid as i32 - self.last_finish_pid,
            1,
            "LocalPartitionWriter::finish_partition must be called in order."
        );
        self.last_finish_pid = pid as i32;

        let write_buffer_size = self.write_buffer_size;
        let batch_size = self.batch_size;

        match &mut self.data_output {
            DataOutput::Single(writer) => {
                // Single-partition data was already streamed via `write`, starting at
                // offset 0 (already recorded in `self.offsets[0]`). Stream any trailing
                // batches (normally none) without flushing; the long-lived writer is
                // flushed once in `finish_all`.
                for batch in iter.by_ref() {
                    let batch = batch?;
                    writer.write(&batch, &metrics.encode_time, &metrics.write_time)?;
                }
            }
            DataOutput::Multi {
                output_writer,
                shuffle_block_writer,
                spill_writers,
                ..
            } => {
                self.offsets[pid] = output_writer.stream_position()?;

                // if we wrote a spill file for this partition then copy the
                // contents into the shuffle file
                if let Some(writer) = spill_writers.get(pid) {
                    if let Some(spill_path) = writer.path() {
                        // Use raw File handle (not BufReader) so that std::io::copy
                        // can use copy_file_range/sendfile for zero-copy on Linux.
                        let mut spill_file = File::open(spill_path)?;
                        let mut write_timer = metrics.write_time.timer();
                        std::io::copy(&mut spill_file, output_writer)?;
                        write_timer.stop();
                    }
                }

                // Write in memory batches to output data file. Each partition uses its
                // own writer so coalescing does not cross partition boundaries.
                let mut buf_batch_writer = BufBatchWriter::new(
                    shuffle_block_writer,
                    output_writer,
                    write_buffer_size,
                    batch_size,
                );
                for batch in iter.by_ref() {
                    let batch = batch?;
                    buf_batch_writer.write(&batch, &metrics.encode_time, &metrics.write_time)?;
                }
                buf_batch_writer.flush(&metrics.encode_time, &metrics.write_time)?;
            }
        }
        Ok(())
    }

    fn finish_all(
        &mut self,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()> {
        // Flush the data output and capture the final position. For the
        // single-partition writer this also finalizes the last coalesced batch.
        let final_offset = match &mut self.data_output {
            DataOutput::Single(writer) => {
                writer.flush(&metrics.encode_time, &metrics.write_time)?;
                writer.writer_stream_position()?
            }
            DataOutput::Multi { output_writer, .. } => {
                let mut write_timer = metrics.write_time.timer();
                output_writer.flush()?;
                let pos = output_writer.stream_position()?;
                write_timer.stop();
                pos
            }
        };

        // add one extra offset at last to ease partition length computation
        self.offsets[self.num_output_partitions] = final_offset;

        let mut write_timer = metrics.write_time.timer();
        let mut output_index = BufWriter::new(
            File::create(self.output_index_file.clone())
                .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?,
        );

        for offset in &self.offsets {
            let offset_i64 = i64::try_from(*offset).map_err(|_| {
                DataFusionError::Execution(format!(
                    "shuffle write error: offset overflow ({offset})"
                ))
            })?;
            output_index.write_all(&offset_i64.to_le_bytes())?;
        }
        output_index.flush()?;
        write_timer.stop();

        Ok(())
    }
}
