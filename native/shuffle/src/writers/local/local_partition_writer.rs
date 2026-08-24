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

use crate::codec_context::ShuffleCodecContext;
use crate::metrics::ShufflePartitionerMetrics;
use crate::writers::local::spill::PartitionedSpill;
use crate::writers::partition_writer::PartitionWriter;
use crate::writers::BufBatchWriter;
use crate::{PartitionOffsets, ShuffleBlockWriter};
use arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::os::unix::fs::FileExt;
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
#[allow(clippy::large_enum_variant)]
enum DataOutput {
    /// Single-partition output: one long-lived writer streams all batches.
    Single {
        writer: BufBatchWriter<ShuffleBlockWriter, File>,
        /// Task-scoped scratch byte buffer threaded through every call on the
        /// long-lived writer, which borrows rather than owns its serialization buffer.
        scratch: Vec<u8>,
    },
    /// Multi-partition output: batches are staged per partition and merged into
    /// `output_writer` one partition at a time during `finish_partition`.
    Multi {
        output_writer: BufWriter<File>,
        shuffle_block_writer: ShuffleBlockWriter,
        /// Spilled blocks for every partition, in one file.
        spill: PartitionedSpill,
        /// Read handle on the spill file and a write-buffer-sized scratch for its ranges,
        /// opened on first use.
        spill_reader: Option<(File, Vec<u8>)>,
        /// Runtime used to allocate the temporary spill file.
        runtime: Arc<RuntimeEnv>,
        /// Byte buffer recycled through the short-lived per-partition `BufBatchWriter`s.
        /// Partitions are written strictly one at a time, so a single buffer keeps its
        /// grown capacity across the whole task instead of every partition regrowing a
        /// fresh allocation toward the write buffer size.
        recycled_buffer: Vec<u8>,
    },
}

/// Local file-based [`PartitionWriter`] implementation.
///
/// Writes shuffle output to a single data file and publishes the byte offset where
/// each partition begins through [`PartitionOffsets`]. See [`DataOutput`] for how the
/// single- and multi-partition modes differ.
pub struct LocalPartitionWriter {
    partition_offsets: Arc<PartitionOffsets>,
    data_output: DataOutput,
    /// Compression state shared by every block this task writes; the per-partition
    /// `BufBatchWriter`s borrow it (see [`ShuffleCodecContext`]). Retention is bounded:
    /// released at spill/finish boundaries and whenever its workspace is oversized.
    codec_context: ShuffleCodecContext,
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
    pub fn try_new(
        output_data_file: String,
        partition_offsets: Arc<PartitionOffsets>,
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
            DataOutput::Single {
                writer: BufBatchWriter::new(
                    shuffle_block_writer,
                    output_file,
                    write_buffer_size,
                    batch_size,
                ),
                scratch: Vec::new(),
            }
        } else {
            let output_writer = BufWriter::with_capacity(write_buffer_size, output_file);
            let spill = PartitionedSpill::new(
                shuffle_block_writer.clone(),
                write_buffer_size,
                batch_size,
                num_output_partitions,
            );
            DataOutput::Multi {
                output_writer,
                shuffle_block_writer,
                spill,
                spill_reader: None,
                runtime,
                recycled_buffer: Vec::new(),
            }
        };
        Ok(Self {
            partition_offsets,
            data_output,
            codec_context: ShuffleCodecContext::default(),
            offsets: vec![0u64; num_output_partitions + 1],
            batch_size,
            write_buffer_size,
            num_output_partitions,
            last_finish_pid: -1,
        })
    }

    #[cfg(test)]
    pub(crate) fn holds_zstd_cctx(&self) -> bool {
        self.codec_context.holds_zstd_cctx()
    }

    #[cfg(test)]
    pub(crate) fn zstd_creation_count(&self) -> u32 {
        self.codec_context.creation_count()
    }

    #[cfg(test)]
    pub(crate) fn get_spill(&self) -> &PartitionedSpill {
        match &self.data_output {
            DataOutput::Multi { spill, .. } => spill,
            DataOutput::Single { .. } => panic!("single-partition output does not spill"),
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
            DataOutput::Single { writer, scratch } => {
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
                    writer.write(
                        &batch,
                        scratch,
                        &mut self.codec_context,
                        &metrics.encode_time,
                        &metrics.write_time,
                    )?;
                }
            }
            DataOutput::Multi {
                spill,
                runtime,
                recycled_buffer,
                ..
            } => {
                spill.write(
                    pid,
                    iter,
                    &mut self.codec_context,
                    runtime,
                    metrics,
                    recycled_buffer,
                )?;
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
        if pid as i32 - self.last_finish_pid != 1 {
            return Err(DataFusionError::Execution(
                "LocalPartitionWriter::finish_partition must be called in order.".to_string(),
            ));
        }
        self.last_finish_pid = pid as i32;

        let write_buffer_size = self.write_buffer_size;
        let batch_size = self.batch_size;

        match &mut self.data_output {
            DataOutput::Single { writer, scratch } => {
                // Single-partition data was already streamed via `write`, starting at
                // offset 0 (already recorded in `self.offsets[0]`). Stream any trailing
                // batches (normally none) without flushing; the long-lived writer is
                // flushed once in `finish_all`.
                for batch in iter.by_ref() {
                    let batch = batch?;
                    writer.write(
                        &batch,
                        scratch,
                        &mut self.codec_context,
                        &metrics.encode_time,
                        &metrics.write_time,
                    )?;
                }
            }
            DataOutput::Multi {
                output_writer,
                shuffle_block_writer,
                spill,
                spill_reader,
                recycled_buffer,
                ..
            } => {
                self.offsets[pid] = output_writer.stream_position()?;

                let mut flush_timer = metrics.write_time.timer();
                spill.flush()?;
                flush_timer.stop();
                let ranges = spill.ranges(pid)?;
                if !ranges.is_empty() {
                    if spill_reader.is_none() {
                        let path = spill.path()?.ok_or_else(|| {
                            DataFusionError::Internal(
                                "shuffle spill ranges recorded without a spill file".to_string(),
                            )
                        })?;
                        *spill_reader = Some((File::open(path)?, vec![0; write_buffer_size]));
                    }
                    let (spill_file, buffer) = spill_reader.as_mut().unwrap();
                    let mut write_timer = metrics.write_time.timer();
                    for range in ranges {
                        copy_spill_range(spill_file, buffer, range, output_writer)?;
                    }
                    write_timer.stop();
                }

                // Write in memory batches to output data file. Each partition uses its
                // own writer so coalescing does not cross partition boundaries, but the
                // scratch buffer is shared so its capacity carries over to the next one.
                let mut buf_batch_writer = BufBatchWriter::new(
                    shuffle_block_writer,
                    output_writer,
                    write_buffer_size,
                    batch_size,
                );
                let codec_context = &mut self.codec_context;
                let result: datafusion::common::Result<()> = (|| {
                    for batch in iter.by_ref() {
                        let batch = batch?;
                        buf_batch_writer.write(
                            &batch,
                            recycled_buffer,
                            codec_context,
                            &metrics.encode_time,
                            &metrics.write_time,
                        )?;
                    }
                    buf_batch_writer.flush(
                        recycled_buffer,
                        codec_context,
                        &metrics.encode_time,
                        &metrics.write_time,
                    )
                })();
                // An errored partition must hand back a drained buffer, or its bytes
                // leak into the next partition's block.
                result.inspect_err(|_| recycled_buffer.clear())?;
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
            DataOutput::Single { writer, scratch } => {
                writer.flush(
                    scratch,
                    &mut self.codec_context,
                    &metrics.encode_time,
                    &metrics.write_time,
                )?;
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

        let offsets = self
            .offsets
            .iter()
            .map(|offset| {
                i64::try_from(*offset).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "shuffle write error: offset overflow ({offset})"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.partition_offsets.set(offsets)?;

        // The shuffle output is complete; nothing else encodes through this context.
        self.codec_context.release_zstd();

        Ok(())
    }

    fn write_burst_complete(&mut self) {
        // A spill burst just ended and the next encode may be a long time coming; the zstd
        // workspace is native memory no reservation tracks, so don't sit on it.
        self.codec_context.release_zstd();
    }
}

/// Appends `range` of the spill file to `output`, reading it through `buffer` when it fits.
fn copy_spill_range(
    spill_file: &mut File,
    buffer: &mut [u8],
    range: &Range<u64>,
    output: &mut BufWriter<File>,
) -> datafusion::common::Result<()> {
    let len = range.end - range.start;
    let truncated = || {
        DataFusionError::Execution(format!(
            "shuffle spill file truncated: range {range:?} extends past its end"
        ))
    };
    match usize::try_from(len)
        .ok()
        .and_then(|len| buffer.get_mut(..len))
    {
        // one pread instead of io::copy's lseek, two statx and copy_file_range
        Some(chunk) => {
            spill_file
                .read_exact_at(chunk, range.start)
                .map_err(|e| match e.kind() {
                    ErrorKind::UnexpectedEof => truncated(),
                    _ => e.into(),
                })?;
            output.write_all(chunk)?;
        }
        None => {
            spill_file.seek(SeekFrom::Start(range.start))?;
            // raw File, not BufReader, so the copy can use copy_file_range on Linux
            let copied = std::io::copy(&mut Read::by_ref(spill_file).take(len), output)?;
            if copied != len {
                return Err(truncated());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writers::local::spill::pathless_backend;
    use crate::CompressionCodec;
    use arrow::array::Int64Array;
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..100))],
        )
        .unwrap()
    }

    fn partition_writer(
        batch: &RecordBatch,
        dir: &tempfile::TempDir,
        runtime: Arc<RuntimeEnv>,
    ) -> LocalPartitionWriter {
        partition_writer_with(batch, 2, 1 << 20, dir, runtime)
    }

    fn partition_writer_with(
        batch: &RecordBatch,
        num_partitions: usize,
        write_buffer_size: usize,
        dir: &tempfile::TempDir,
        runtime: Arc<RuntimeEnv>,
    ) -> LocalPartitionWriter {
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema_ref().as_ref(), CompressionCodec::None)
                .unwrap();
        LocalPartitionWriter::try_new(
            dir.path().join("data.out").to_str().unwrap().to_string(),
            Arc::new(PartitionOffsets::default()),
            block_writer,
            num_partitions,
            // batch_size below the row count so the write serializes into the scratch.
            10,
            write_buffer_size,
            runtime,
        )
        .unwrap()
    }

    /// A partition whose batch iterator fails after a batch was already encoded must hand
    /// back a drained scratch; leftover bytes would land in the next partition's block.
    #[test]
    fn finish_partition_error_drains_recycled_buffer() {
        let batch = test_batch();
        let dir = tempfile::tempdir().unwrap();
        let mut writer = partition_writer(&batch, &dir, Arc::new(RuntimeEnv::default()));

        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let mut iter = vec![
            Ok(batch),
            Err(DataFusionError::Execution("injected failure".to_string())),
        ]
        .into_iter();

        assert!(writer.finish_partition(0, &mut iter, &metrics).is_err());
        match &writer.data_output {
            DataOutput::Multi {
                recycled_buffer, ..
            } => assert!(
                recycled_buffer.is_empty(),
                "errored partition left {} bytes in the recycled buffer",
                recycled_buffer.len()
            ),
            DataOutput::Single { .. } => unreachable!("two partitions use the multi output"),
        }
    }

    /// Spilled bytes the writer cannot reach must fail the task. Skipping the copy the way
    /// an unspilled partition is skipped would leave the offsets claiming bytes that were
    /// never written, so the reader would decode the next partition's block as this one's.
    #[test]
    fn finish_partition_fails_when_spill_has_no_local_path() {
        let batch = test_batch();
        let dir = tempfile::tempdir().unwrap();
        let mut writer = partition_writer(&batch, &dir, Arc::new(pathless_backend::runtime()));
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);

        writer
            .write(0, &mut vec![Ok(batch)].into_iter(), &metrics)
            .unwrap();

        let err = writer
            .finish_partition(0, &mut std::iter::empty(), &metrics)
            .expect_err("unreachable spilled data must fail rather than truncate the partition");
        assert!(
            err.to_string().contains("no local path"),
            "unexpected error: {err}"
        );
    }

    fn int_batch(values: std::ops::Range<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(values))]).unwrap()
    }

    fn decode_blocks(bytes: &[u8]) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        let mut pos = 0;
        while pos < bytes.len() {
            let len = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
            batches.push(crate::read_ipc_compressed(&bytes[pos + 16..pos + 8 + len]).unwrap());
            pos += 8 + len;
        }
        batches
    }

    fn count_files(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    count_files(&path)
                } else {
                    1
                }
            })
            .sum()
    }

    /// Partition data spread over spill rounds written in different partition orders, plus a
    /// final in-memory batch, reads back per partition in write order, whether the spilled
    /// ranges are read through the scratch buffer or copied.
    #[test]
    fn spilled_partitions_read_back_in_write_order() {
        // a 64-byte write buffer is smaller than one block, so every range is copied
        for write_buffer_size in [1 << 20, 64] {
            let dir = tempfile::tempdir().unwrap();
            let schema = test_batch().schema();
            let mut writer = partition_writer_with(
                &test_batch(),
                4,
                write_buffer_size,
                &dir,
                Arc::new(RuntimeEnv::default()),
            );
            let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
            let mut expected: Vec<Vec<RecordBatch>> = vec![Vec::new(); 4];
            let mut next = 0i64;
            let mut batch = || {
                let batch = int_batch(next..next + 10);
                next += 10;
                batch
            };

            for order in [[3, 2, 1, 0], [0, 1, 2, 3]] {
                for pid in order {
                    let b = batch();
                    expected[pid].push(b.clone());
                    writer
                        .write(pid, &mut vec![Ok(b)].into_iter(), &metrics)
                        .unwrap();
                }
            }
            for (pid, rows) in expected.iter_mut().enumerate() {
                let b = batch();
                rows.push(b.clone());
                writer
                    .finish_partition(pid, &mut vec![Ok(b)].into_iter(), &metrics)
                    .unwrap();
            }
            writer.finish_all(&metrics).unwrap();

            let offsets = writer.partition_offsets.get().unwrap().to_vec();
            let data = std::fs::read(dir.path().join("data.out")).unwrap();
            for (pid, rows) in expected.iter().enumerate() {
                let bytes = &data[offsets[pid] as usize..offsets[pid + 1] as usize];
                let actual = concat_batches(&schema, &decode_blocks(bytes)).unwrap();
                assert_eq!(
                    actual,
                    concat_batches(&schema, rows).unwrap(),
                    "partition {pid}, write buffer {write_buffer_size}"
                );
            }
        }
    }

    /// A task spills to one file however many partitions it has.
    #[test]
    fn spilling_every_partition_creates_one_file() {
        let spill_dir = tempfile::tempdir().unwrap();
        let output_dir = tempfile::tempdir().unwrap();
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
                    DiskManagerMode::Directories(vec![spill_dir.path().to_path_buf()]),
                ))
                .build()
                .unwrap(),
        );
        let num_partitions = 64;
        let mut writer =
            partition_writer_with(&test_batch(), num_partitions, 1 << 20, &output_dir, runtime);
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);

        for _ in 0..3 {
            for pid in 0..num_partitions {
                writer
                    .write(pid, &mut vec![Ok(test_batch())].into_iter(), &metrics)
                    .unwrap();
            }
        }
        assert_eq!(count_files(spill_dir.path()), 1);
    }

    /// A spill file shorter than its recorded ranges fails the task instead of writing a short
    /// partition, whether the range is read through the scratch buffer or copied.
    #[test]
    fn finish_partition_fails_when_spill_file_is_truncated() {
        for write_buffer_size in [1 << 20, 64] {
            let dir = tempfile::tempdir().unwrap();
            let mut writer = partition_writer_with(
                &test_batch(),
                2,
                write_buffer_size,
                &dir,
                Arc::new(RuntimeEnv::default()),
            );
            let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
            for pid in 0..2 {
                writer
                    .write(pid, &mut vec![Ok(test_batch())].into_iter(), &metrics)
                    .unwrap();
            }
            // finishing partition 0 flushes the buffered spill bytes before the file is cut
            writer
                .finish_partition(0, &mut std::iter::empty(), &metrics)
                .unwrap();

            let path = writer.get_spill().path().unwrap().unwrap().to_path_buf();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(1)
                .unwrap();

            let err = writer
                .finish_partition(1, &mut std::iter::empty(), &metrics)
                .expect_err("a truncated spill file must fail the partition");
            assert!(
                err.to_string().contains("truncated"),
                "write buffer {write_buffer_size}: unexpected error: {err}"
            );
        }
    }
}
