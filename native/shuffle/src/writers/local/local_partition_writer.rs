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
use datafusion::execution::memory_pool::MemoryReservation;
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
        /// Unbuffered until the first `finish_partition` sizes it, since nothing reaches the
        /// data file before then. `None` only while it is rebuilt.
        output_writer: Option<BufWriter<File>>,
        shuffle_block_writer: ShuffleBlockWriter,
        /// Spilled blocks for every partition, in one file.
        spill: PartitionedSpill,
        /// Read handle on the spill file and a write-buffer-sized scratch for its ranges,
        /// opened by the first `finish_partition` if anything was spilled.
        spill_reader: Option<(File, Vec<u8>)>,
        /// Runtime used to allocate the temporary spill file.
        runtime: Arc<RuntimeEnv>,
        /// Byte buffer recycled through the short-lived per-partition `BufBatchWriter`s.
        /// Partitions are written strictly one at a time, so a single buffer keeps its
        /// grown capacity across the partitions of a spill or of the output instead of every
        /// partition regrowing a fresh allocation toward the write buffer size. Each spill
        /// releases it.
        recycled_buffer: Vec<u8>,
    },
}

/// Local file-based [`PartitionWriter`] implementation.
///
/// Writes shuffle output to a single data file and publishes the byte offset where
/// each partition begins through [`PartitionOffsets`]. See [`DataOutput`] for how the
/// single- and multi-partition modes differ.
pub(crate) struct LocalPartitionWriter {
    partition_offsets: Arc<PartitionOffsets>,
    data_output: DataOutput,
    /// Kept equal to [`Self::buffer_bytes_held`] at every call boundary. `None` for
    /// single-partition output.
    buffers: Option<MemoryReservation>,
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
    pub(crate) fn try_new(
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
            let output_writer = Some(BufWriter::with_capacity(0, output_file));
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
            buffers: None,
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

    pub(crate) fn buffer_bytes_held(&self) -> usize {
        match &self.data_output {
            DataOutput::Multi {
                output_writer,
                spill,
                spill_reader,
                recycled_buffer,
                ..
            } => {
                output_writer.as_ref().map_or(0, BufWriter::capacity)
                    + spill.buffer_capacity()
                    + spill_reader.as_ref().map_or(0, |(_, buffer)| buffer.len())
                    + recycled_buffer.capacity()
                    + self.codec_context.retained_bytes()
            }
            DataOutput::Single { .. } => 0,
        }
    }

    /// No pool call when nothing changed, so steady-state writes stay off the pool.
    fn sync_buffer_reservation(&self) {
        if let Some(buffers) = &self.buffers {
            buffers.resize(self.buffer_bytes_held());
        }
    }

    /// Runs before the first partition is finished. Nothing is spilled after that, so the
    /// spill file's write buffer goes back to the pool before the output's buffers are sized:
    /// the spill reader first, since it serves every spilled range, then the data file buffer,
    /// which blocks of the write buffer size bypass anyway.
    fn begin_output(
        &mut self,
        metrics: &ShufflePartitionerMetrics,
    ) -> datafusion::common::Result<()> {
        let DataOutput::Multi { spill, .. } = &mut self.data_output else {
            return Ok(());
        };
        let mut flush_timer = metrics.write_time.timer();
        spill.release_buffer()?;
        flush_timer.stop();
        self.sync_buffer_reservation();

        let DataOutput::Multi {
            output_writer,
            spill,
            spill_reader,
            ..
        } = &mut self.data_output
        else {
            return Ok(());
        };
        let write_buffer_size = self.write_buffer_size;
        let reserve = || match self.buffers.as_ref() {
            Some(buffers) => super::reserve_buffer(buffers, write_buffer_size),
            None => Ok(write_buffer_size),
        };
        if spill.has_ranges() {
            let path = spill.path()?.ok_or_else(|| {
                DataFusionError::Internal(
                    "shuffle spill ranges recorded without a spill file".to_string(),
                )
            })?;
            // Open first, so a failed open leaves nothing charged.
            let spill_file = File::open(path)?;
            *spill_reader = Some((spill_file, vec![0; reserve()?]));
        }
        let capacity = reserve()?;
        let writer = output_writer.take().ok_or_else(missing_output_writer)?;
        // An unbuffered writer holds no bytes, so only the file carries over.
        let (output_file, _) = writer.into_parts();
        *output_writer = Some(BufWriter::with_capacity(capacity, output_file));
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn buffer_reservation_size(&self) -> usize {
        self.buffers.as_ref().map_or(0, MemoryReservation::size)
    }

    #[cfg(test)]
    pub(crate) fn scratch_capacity(&self) -> usize {
        match &self.data_output {
            DataOutput::Multi {
                recycled_buffer, ..
            } => recycled_buffer.capacity(),
            DataOutput::Single { scratch, .. } => scratch.capacity(),
        }
    }

    #[cfg(test)]
    pub(crate) fn buffer_capacities(&self) -> (usize, usize, usize) {
        match &self.data_output {
            DataOutput::Multi {
                output_writer,
                spill,
                spill_reader,
                ..
            } => (
                output_writer.as_ref().map_or(0, BufWriter::capacity),
                spill.buffer_capacity(),
                spill_reader.as_ref().map_or(0, |(_, buffer)| buffer.len()),
            ),
            DataOutput::Single { .. } => panic!("single-partition output has no spill buffers"),
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
        // Synced on every exit, errors included, so it matches what a failed write left.
        let result: datafusion::common::Result<()> = (|| {
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
                        self.buffers.as_ref(),
                    )?;
                }
            }
            Ok(())
        })();
        self.sync_buffer_reservation();
        result
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
        let is_first = self.last_finish_pid == -1;
        self.last_finish_pid = pid as i32;

        let write_buffer_size = self.write_buffer_size;
        let batch_size = self.batch_size;

        // Synced on every exit, errors included, so it matches what a failed partition left.
        let result: datafusion::common::Result<()> = (|| {
            if is_first {
                self.begin_output(metrics)?;
            }
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
                    let output_writer = output_writer.as_mut().ok_or_else(missing_output_writer)?;
                    self.offsets[pid] = output_writer.stream_position()?;

                    let ranges = spill.ranges(pid)?;
                    if !ranges.is_empty() {
                        let (spill_file, buffer) = spill_reader.as_mut().ok_or_else(|| {
                            DataFusionError::Internal(
                                "shuffle spill ranges recorded without a spill reader".to_string(),
                            )
                        })?;
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
        })();
        self.sync_buffer_reservation();
        result
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
                let output_writer = output_writer.as_mut().ok_or_else(missing_output_writer)?;
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
        self.sync_buffer_reservation();

        Ok(())
    }

    fn write_burst_complete(&mut self, freed: usize) -> datafusion::common::Result<()> {
        // A spill burst just ended and the next encode may be a long time coming, so don't
        // sit on the zstd workspace, nor on the drained scratch, which one wide row can leave
        // larger than the pool.
        self.codec_context.release_zstd();
        if let DataOutput::Multi {
            recycled_buffer, ..
        } = &mut self.data_output
        {
            *recycled_buffer = Vec::new();
        }
        self.sync_buffer_reservation();
        // A pressure spill opened its file while the batches still held the pool; now they don't.
        // Half of what the spill freed stays with the input, so the next batch does not spill.
        let capacity = self.write_buffer_size.min(freed / 2);
        if let (Some(buffers), DataOutput::Multi { spill, .. }) =
            (&self.buffers, &mut self.data_output)
        {
            spill.restore_buffer(buffers, capacity)?;
        }
        Ok(())
    }

    fn attach_buffer_reservation(
        &mut self,
        reservation: MemoryReservation,
    ) -> datafusion::common::Result<()> {
        let DataOutput::Multi { .. } = &self.data_output else {
            return Ok(());
        };
        // Attached once, before any write. Each buffer is charged when it is allocated.
        debug_assert!(self.buffers.is_none());
        self.buffers = Some(reservation);
        self.sync_buffer_reservation();
        Ok(())
    }
}

fn missing_output_writer() -> DataFusionError {
    DataFusionError::Internal("shuffle data file writer missing".to_string())
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
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    fn test_batch() -> RecordBatch {
        test_batch_of(100)
    }

    fn test_batch_of(rows: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..rows))],
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
        partition_writer_with_codec(
            batch,
            num_partitions,
            write_buffer_size,
            CompressionCodec::None,
            dir,
            runtime,
        )
    }

    fn partition_writer_with_codec(
        batch: &RecordBatch,
        num_partitions: usize,
        write_buffer_size: usize,
        codec: CompressionCodec,
        dir: &tempfile::TempDir,
        runtime: Arc<RuntimeEnv>,
    ) -> LocalPartitionWriter {
        let block_writer = ShuffleBlockWriter::try_new(batch.schema_ref().as_ref(), codec).unwrap();
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

    fn greedy_pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(limit))
    }

    fn assert_buffers_charged(writer: &LocalPartitionWriter, pool: &Arc<dyn MemoryPool>) {
        assert_eq!(
            writer.buffer_reservation_size(),
            writer.buffer_bytes_held(),
            "reservation must equal the bytes held"
        );
        assert_eq!(pool.reserved(), writer.buffer_bytes_held());
    }

    /// Spills partition 0's 4096-row block and partition 1's 100-row block, then writes the
    /// output under a pool of `limit` bytes. Returns the buffer capacities, the output, and the
    /// encode scratch left by the spill.
    fn write_spilled_output(limit: usize) -> ((usize, usize, usize), Vec<u8>, usize) {
        let write_buffer_size = 1 << 20;
        let fallback = crate::writers::local::FALLBACK_BUFFER_SIZE;
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        // Partition 0's block exceeds the fallback buffer (`io::copy`), partition 1's fits (pread).
        let batch_for = |pid: usize| {
            if pid == 0 {
                test_batch_of(4096)
            } else {
                test_batch()
            }
        };
        let dir = tempfile::tempdir().unwrap();
        let pool = greedy_pool(limit);
        let mut writer = partition_writer_with(
            &test_batch(),
            2,
            write_buffer_size,
            &dir,
            Arc::new(RuntimeEnv::default()),
        );
        let reservation = MemoryConsumer::new("test").register(&pool);
        writer
            .attach_buffer_reservation(reservation.new_empty())
            .unwrap();
        for pid in 0..2 {
            writer
                .write(pid, &mut vec![Ok(batch_for(pid))].into_iter(), &metrics)
                .unwrap();
        }
        let scratch = writer.scratch_capacity();
        writer.write_burst_complete(0).unwrap();
        let range_len = |pid: usize| {
            let ranges = writer.get_spill().ranges(pid).unwrap();
            assert_eq!(ranges.len(), 1, "one spilled block for partition {pid}");
            (ranges[0].end - ranges[0].start) as usize
        };
        assert!(
            range_len(0) > fallback,
            "partition 0 must exceed the fallback buffer"
        );
        assert!(
            range_len(1) <= fallback,
            "partition 1 must fit the fallback buffer"
        );
        for pid in 0..2 {
            writer
                .finish_partition(pid, &mut vec![Ok(batch_for(pid))].into_iter(), &metrics)
                .unwrap();
        }
        writer.finish_all(&metrics).unwrap();
        assert_buffers_charged(&writer, &pool);
        (
            writer.buffer_capacities(),
            std::fs::read(dir.path().join("data.out")).unwrap(),
            scratch,
        )
    }

    /// The output buffers are sized once nothing more is spilled, after the spill file's write
    /// buffer is released, and the reader is sized before the data file buffer. A refused
    /// reader falls back to a small buffer and copies larger ranges with `io::copy`.
    #[test]
    fn output_buffers_are_sized_after_the_spill_buffer_is_released() {
        let write_buffer_size = 1 << 20;
        let fallback = crate::writers::local::FALLBACK_BUFFER_SIZE;
        let (capacities, baseline, scratch) = write_spilled_output(1 << 30);
        assert_eq!(capacities, (write_buffer_size, 0, write_buffer_size));
        assert!(!baseline.is_empty());
        for (limit, expected) in [
            // room for two full buffers only once the spill buffer is gone
            (
                2 * write_buffer_size + scratch + 64 * 1024,
                (write_buffer_size, 0, write_buffer_size),
            ),
            // room for one full buffer, which goes to the reader
            (
                write_buffer_size + scratch + 64 * 1024,
                (fallback, 0, write_buffer_size),
            ),
            (64 * 1024, (fallback, 0, fallback)),
        ] {
            let (capacities, output, _) = write_spilled_output(limit);
            assert_eq!(capacities, expected, "pool limit {limit}");
            assert_eq!(output, baseline, "pool limit {limit} changed the output");
        }
    }

    /// A partition that fails after the spill buffer was released still leaves the
    /// reservation equal to what the writer holds.
    #[test]
    fn failed_first_partition_keeps_the_reservation_in_sync() {
        let dir = tempfile::tempdir().unwrap();
        let pool = greedy_pool(1 << 30);
        let mut writer = partition_writer_with(
            &test_batch(),
            2,
            1 << 20,
            &dir,
            Arc::new(RuntimeEnv::default()),
        );
        let reservation = MemoryConsumer::new("test").register(&pool);
        writer
            .attach_buffer_reservation(reservation.new_empty())
            .unwrap();
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        writer
            .write(0, &mut vec![Ok(test_batch())].into_iter(), &metrics)
            .unwrap();
        assert_eq!(writer.buffer_capacities().1, 1 << 20);
        let scratch = writer.scratch_capacity();

        // The larger batch grows the scratch before the iterator fails.
        let mut iter = vec![
            Ok(test_batch_of(4096)),
            Err(DataFusionError::Execution("injected failure".to_string())),
        ]
        .into_iter();
        assert!(writer.finish_partition(0, &mut iter, &metrics).is_err());
        assert!(writer.scratch_capacity() > scratch);
        assert_eq!(writer.buffer_capacities().1, 0, "spill buffer released");
        assert_buffers_charged(&writer, &pool);
    }

    #[test]
    fn encode_scratch_growth_is_charged() {
        let dir = tempfile::tempdir().unwrap();
        let pool = greedy_pool(1 << 30);
        let mut writer = partition_writer_with(
            &test_batch(),
            2,
            1 << 20,
            &dir,
            Arc::new(RuntimeEnv::default()),
        );
        let reservation = MemoryConsumer::new("test").register(&pool);
        writer
            .attach_buffer_reservation(reservation.new_empty())
            .unwrap();
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);

        writer
            .write(0, &mut vec![Ok(test_batch())].into_iter(), &metrics)
            .unwrap();
        let (charged, scratch) = (writer.buffer_reservation_size(), writer.scratch_capacity());

        // A larger block grows only the scratch; the file buffers and codec are unchanged.
        writer
            .write(1, &mut vec![Ok(test_batch_of(4096))].into_iter(), &metrics)
            .unwrap();
        let scratch_delta = writer.scratch_capacity() - scratch;
        assert!(scratch_delta > 0, "the larger block must grow the scratch");
        assert_eq!(
            writer.buffer_reservation_size() - charged,
            scratch_delta,
            "the reservation must move by the scratch's growth"
        );
        assert_buffers_charged(&writer, &pool);
    }

    #[test]
    fn zstd_context_is_charged_while_retained() {
        let dir = tempfile::tempdir().unwrap();
        let pool = greedy_pool(1 << 30);
        let mut writer = partition_writer_with_codec(
            &test_batch(),
            2,
            1 << 20,
            CompressionCodec::Zstd(1),
            &dir,
            Arc::new(RuntimeEnv::default()),
        );
        let reservation = MemoryConsumer::new("test").register(&pool);
        writer
            .attach_buffer_reservation(reservation.new_empty())
            .unwrap();
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);

        writer
            .write(0, &mut vec![Ok(test_batch())].into_iter(), &metrics)
            .unwrap();
        let retained = writer.codec_context.retained_bytes();
        assert!(
            retained > 1 << 20,
            "level 1 context is over 1 MiB, got {retained}"
        );
        assert_buffers_charged(&writer, &pool);
        let after_write = writer.buffer_reservation_size();

        let scratch = writer.scratch_capacity();
        writer.write_burst_complete(0).unwrap();
        assert!(!writer.holds_zstd_cctx());
        assert_eq!(
            writer.buffer_reservation_size(),
            after_write - retained - scratch
        );
        assert_buffers_charged(&writer, &pool);

        for pid in 0..2 {
            writer
                .finish_partition(pid, &mut vec![Ok(test_batch())].into_iter(), &metrics)
                .unwrap();
        }
        let retained = writer.codec_context.retained_bytes();
        assert!(
            retained > 1 << 20,
            "finish_partition re-created the context"
        );
        assert_buffers_charged(&writer, &pool);
        let after_finish_partition = writer.buffer_reservation_size();

        writer.finish_all(&metrics).unwrap();
        assert!(!writer.holds_zstd_cctx());
        assert_eq!(
            writer.buffer_reservation_size(),
            after_finish_partition - retained
        );
        assert_buffers_charged(&writer, &pool);

        drop(writer);
        assert_eq!(pool.reserved(), 0);
    }
}
