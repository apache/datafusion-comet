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

use super::ShuffleBlockWriter;
use crate::codec_context::ShuffleCodecContext;
use arrow::array::RecordBatch;
use arrow::compute::kernels::coalesce::BatchCoalescer;
use datafusion::physical_plan::metrics::Time;
use std::borrow::Borrow;
use std::io::{Cursor, Seek, SeekFrom, Write};

/// Write batches to writer while using a buffer to avoid frequent system calls.
/// The record batches are first written by ShuffleBlockWriter into a caller-provided
/// scratch buffer. Once the scratch exceeds the max size, it is flushed to the writer.
///
/// The scratch buffer is borrowed per call rather than owned: task-scoped scratch is
/// threaded through every `write`/`flush`, so one buffer serves all the short-lived
/// writers of a task, and an error mid-partition cannot strand the buffer inside a
/// dropped writer and silently end recycling.
///
/// Small batches are coalesced using Arrow's [`BatchCoalescer`] before serialization, reducing
/// per-block IPC schema overhead. Output batches hold at least `batch_size` rows, apart from the
/// remainder emitted on flush. The coalescer is lazily initialized on the first write and
/// configured (via `biggest_coalesce_batch_size`) to pass batches that are already at least
/// `batch_size` rows straight through, verbatim and without copying them, so an oversized input
/// batch is written as a single oversized block.
///
/// Encoding methods borrow a [`ShuffleCodecContext`] rather than owning one: these writers
/// are created per output partition, and codec contexts must stay task-scoped.
pub(crate) struct BufBatchWriter<S: Borrow<ShuffleBlockWriter>, W: Write> {
    shuffle_block_writer: S,
    writer: W,
    buffer_max_size: usize,
    /// Coalesces small batches into target_batch_size before serialization.
    /// Lazily initialized on first write to capture the schema.
    coalescer: Option<BatchCoalescer>,
    /// Target batch size for coalescing
    batch_size: usize,
    /// Address of the scratch `Vec` seen on first use; every later call must pass the same
    /// one, or unflushed bytes in the other buffer would be silently abandoned.
    #[cfg(debug_assertions)]
    scratch_addr: Option<usize>,
    /// Running total of bytes serialized through this writer, used to report spilled bytes when
    /// the underlying writer does not implement [`Seek`] (e.g. a `Box<dyn SpillWriter>`).
    total_bytes_written: u64,
}

impl<S: Borrow<ShuffleBlockWriter>, W: Write> BufBatchWriter<S, W> {
    pub(crate) fn new(
        shuffle_block_writer: S,
        writer: W,
        buffer_max_size: usize,
        batch_size: usize,
    ) -> Self {
        Self {
            shuffle_block_writer,
            writer,
            buffer_max_size,
            coalescer: None,
            batch_size,
            #[cfg(debug_assertions)]
            scratch_addr: None,
            total_bytes_written: 0,
        }
    }

    /// A fresh writer must start from a drained scratch (stale bytes from a previous owner
    /// would be silently prepended to its first block), and every later call must pass the
    /// same scratch (bytes left unflushed in a swapped-out buffer would be silently lost).
    /// Identity is the `Vec`'s own address, which is stable for the caller-owned field the
    /// writer is used with, unlike the data pointer that moves on regrowth.
    #[allow(unused_variables)]
    #[allow(clippy::ptr_arg)] // identity check needs the Vec's own address, not a slice view
    fn check_scratch(&mut self, scratch: &Vec<u8>) {
        #[cfg(debug_assertions)]
        {
            let addr = scratch as *const Vec<u8> as usize;
            match self.scratch_addr {
                None => {
                    debug_assert!(
                        scratch.is_empty(),
                        "fresh BufBatchWriter handed a non-empty scratch buffer ({} bytes)",
                        scratch.len()
                    );
                    self.scratch_addr = Some(addr);
                }
                Some(previous) => debug_assert_eq!(
                    previous, addr,
                    "BufBatchWriter must receive the same scratch buffer on every call"
                ),
            }
        }
    }

    /// `scratch` is the caller-owned byte buffer to serialize into; threading the same
    /// buffer through every call reuses its capacity instead of regrowing a fresh
    /// allocation toward `buffer_max_size` for every writer.
    pub(crate) fn write(
        &mut self,
        batch: &RecordBatch,
        scratch: &mut Vec<u8>,
        codec_context: &mut ShuffleCodecContext,
        encode_time: &Time,
        write_time: &Time,
    ) -> datafusion::common::Result<usize> {
        self.check_scratch(scratch);
        let batch_size = self.batch_size;
        let coalescer = self.coalescer.get_or_insert_with(|| {
            // Enable BatchCoalescer's zero-copy passthrough for batches that are already big
            // enough, so we don't `copy_rows` the whole batch into the in-progress builders just
            // to re-emit a same-sized batch. The passthrough fires for batches strictly larger
            // than the limit, so set it to `batch_size - 1` to include batches of exactly
            // `batch_size`, which is what `PartitionedBatchIterator` emits (except the tail),
            // removing a full copy of the shuffle payload. Block boundaries are unchanged for
            // that iterator since it never emits more than `batch_size` rows. The
            // single-partition path routes `>= batch_size` batches here directly, so those now
            // form one oversized block rather than being split at `batch_size`. Rows are written
            // in the same order either way.
            BatchCoalescer::new(batch.schema(), batch_size)
                .with_biggest_coalesce_batch_size(Some(batch_size.saturating_sub(1)))
        });
        coalescer.push_batch(batch.clone())?;

        // Drain completed batches into a local vec so the coalescer borrow ends
        // before we call write_batch_to_buffer (which borrows &mut self).
        let mut completed = Vec::new();
        while let Some(batch) = coalescer.next_completed_batch() {
            completed.push(batch);
        }

        let mut bytes_written = 0;
        for batch in &completed {
            bytes_written +=
                self.write_batch_to_buffer(batch, scratch, codec_context, encode_time, write_time)?;
        }
        Ok(bytes_written)
    }

    /// Serialize a single batch into the scratch buffer, flushing to the writer if needed.
    fn write_batch_to_buffer(
        &mut self,
        batch: &RecordBatch,
        scratch: &mut Vec<u8>,
        codec_context: &mut ShuffleCodecContext,
        encode_time: &Time,
        write_time: &Time,
    ) -> datafusion::common::Result<usize> {
        let mut cursor = Cursor::new(&mut *scratch);
        cursor.seek(SeekFrom::End(0))?;
        let bytes_written = self.shuffle_block_writer.borrow().write_batch(
            batch,
            &mut cursor,
            codec_context,
            encode_time,
        )?;
        let pos = cursor.position();
        if pos >= self.buffer_max_size as u64 {
            let mut write_timer = write_time.timer();
            self.writer.write_all(scratch)?;
            write_timer.stop();
            scratch.clear();
        }
        self.total_bytes_written += bytes_written as u64;
        Ok(bytes_written)
    }

    /// Flushes buffered rows and bytes; `scratch` is left drained for the next writer.
    pub(crate) fn flush(
        &mut self,
        scratch: &mut Vec<u8>,
        codec_context: &mut ShuffleCodecContext,
        encode_time: &Time,
        write_time: &Time,
    ) -> datafusion::common::Result<()> {
        self.check_scratch(scratch);
        // Finish any remaining buffered rows in the coalescer
        let mut remaining = Vec::new();
        if let Some(coalescer) = &mut self.coalescer {
            coalescer.finish_buffered_batch()?;
            while let Some(batch) = coalescer.next_completed_batch() {
                remaining.push(batch);
            }
        }
        for batch in &remaining {
            self.write_batch_to_buffer(batch, scratch, codec_context, encode_time, write_time)?;
        }

        // Flush the scratch buffer to the underlying writer
        let mut write_timer = write_time.timer();
        if !scratch.is_empty() {
            self.writer.write_all(scratch)?;
        }
        self.writer.flush()?;
        write_timer.stop();
        scratch.clear();
        // The scratch's high-water mark can reach `buffer_max_size` plus the largest block
        // that crossed the threshold; keep only the configured buffer size across reuses.
        scratch.shrink_to(self.buffer_max_size);
        Ok(())
    }

    /// Total number of bytes serialized through this writer since it was created. Unlike
    /// [`Self::writer_stream_position`], this does not require the underlying writer to implement
    /// [`Seek`], so it is used to report spilled bytes when writing to a `Box<dyn SpillWriter>`.
    pub(crate) fn bytes_written(&self) -> u64 {
        self.total_bytes_written
    }
}

impl<S: Borrow<ShuffleBlockWriter>, W: Write + Seek> BufBatchWriter<S, W> {
    pub(crate) fn writer_stream_position(&mut self) -> datafusion::common::Result<u64> {
        self.writer.stream_position().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{read_ipc_compressed, CompressionCodec};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch(seed: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let values: Vec<i64> = (0..100).map(|i| seed * 1_000 + i).collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn write_one_partition(seed: i64, scratch: &mut Vec<u8>) -> Vec<u8> {
        let batch = test_batch(seed);
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::Zstd(1))
                .unwrap();
        let mut output = Vec::new();
        let time = Time::default();
        let mut codec_context = ShuffleCodecContext::default();
        let mut writer = BufBatchWriter::new(block_writer, &mut output, 1 << 20, 8192);
        writer
            .write(&batch, scratch, &mut codec_context, &time, &time)
            .unwrap();
        writer
            .flush(scratch, &mut codec_context, &time, &time)
            .unwrap();
        output
    }

    /// A scratch buffer recycled across partitions must produce byte-identical output to
    /// fresh per-partition buffers, come back drained, and keep its grown capacity.
    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call zstd's C FFI
    fn recycled_scratch_matches_fresh_buffers_and_keeps_capacity() {
        let fresh: Vec<Vec<u8>> = (0..3)
            .map(|p| write_one_partition(p, &mut Vec::new()))
            .collect();

        let mut scratch = Vec::new();
        let mut recycled = Vec::new();
        for p in 0..3 {
            let output = write_one_partition(p, &mut scratch);
            assert!(
                scratch.is_empty(),
                "recycled scratch must come back drained"
            );
            recycled.push(output);
        }

        assert_eq!(fresh, recycled);
        assert!(
            scratch.capacity() > 0,
            "capacity grown in one partition must survive into the next"
        );
        for output in &recycled {
            let decoded = read_ipc_compressed(&output[16..]).unwrap();
            assert_eq!(decoded.num_rows(), 100);
        }
    }

    /// Handing a non-empty scratch to a fresh writer would silently prepend stale bytes
    /// to the first block; debug builds must catch it.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "non-empty scratch")]
    fn fresh_writer_rejects_dirty_scratch() {
        let batch = test_batch(0);
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut output = Vec::new();
        let time = Time::default();
        let mut writer = BufBatchWriter::new(block_writer, &mut output, 1 << 20, 8192);
        let mut dirty = vec![0xAB, 0xCD];
        let mut codec_context = ShuffleCodecContext::default();
        let _ = writer.write(&batch, &mut dirty, &mut codec_context, &time, &time);
    }

    /// Swapping in a different scratch mid-writer would silently abandon any bytes still
    /// buffered in the first one; the identity check has to catch it in debug builds.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "same scratch buffer")]
    fn writer_rejects_swapped_scratch() {
        let batch = test_batch(0);
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut output = Vec::new();
        let time = Time::default();
        let mut writer = BufBatchWriter::new(block_writer, &mut output, 1 << 20, 8192);
        let mut codec_context = ShuffleCodecContext::default();
        let mut first = Vec::new();
        writer
            .write(&batch, &mut first, &mut codec_context, &time, &time)
            .unwrap();
        let mut second = Vec::new();
        let _ = writer.write(&batch, &mut second, &mut codec_context, &time, &time);
    }

    /// A block that crosses `buffer_max_size` grows the scratch past the cap; `flush`
    /// must shrink retained capacity back to the configured buffer size, while a
    /// normally-sized run keeps its (sub-cap) capacity untouched.
    #[test]
    fn flush_caps_retained_scratch_capacity() {
        let batch = test_batch(0); // 100 rows of Int64: block is far larger than 64 bytes
        let buffer_max_size = 64usize;
        // batch_size below the row count so the batch bypasses the coalescer and is
        // serialized into the scratch during `write`.
        let batch_size = 10usize;
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut output = Vec::new();
        let time = Time::default();
        let mut codec_context = ShuffleCodecContext::default();
        let mut scratch = Vec::new();
        let mut writer =
            BufBatchWriter::new(block_writer, &mut output, buffer_max_size, batch_size);
        writer
            .write(&batch, &mut scratch, &mut codec_context, &time, &time)
            .unwrap();
        assert!(
            scratch.capacity() > buffer_max_size,
            "oversized block must have grown the scratch past the cap"
        );
        writer
            .flush(&mut scratch, &mut codec_context, &time, &time)
            .unwrap();
        assert!(scratch.is_empty());
        assert!(
            scratch.capacity() <= buffer_max_size,
            "retained capacity {} exceeds cap {}",
            scratch.capacity(),
            buffer_max_size
        );

        // With a roomy cap the grown capacity is retained: `write` serializes into the
        // scratch (batch_size below the row count again), and `flush` must leave the
        // sub-cap capacity exactly unchanged rather than shrinking it.
        let large_cap = 1 << 20;
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut output = Vec::new();
        let mut scratch = Vec::new();
        let mut writer = BufBatchWriter::new(block_writer, &mut output, large_cap, batch_size);
        writer
            .write(&batch, &mut scratch, &mut codec_context, &time, &time)
            .unwrap();
        let cap_after_write = scratch.capacity();
        assert!(
            cap_after_write > 0 && cap_after_write <= large_cap,
            "write must have serialized the batch into the scratch"
        );
        writer
            .flush(&mut scratch, &mut codec_context, &time, &time)
            .unwrap();
        assert!(scratch.is_empty());
        assert_eq!(
            scratch.capacity(),
            cap_after_write,
            "flush must not shrink a scratch already under the cap"
        );
    }
}
