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
use arrow::array::RecordBatch;
use arrow::compute::kernels::coalesce::BatchCoalescer;
use arrow::ipc::writer::IpcWriteContext;
use datafusion::physical_plan::metrics::Time;
use std::borrow::Borrow;
use std::io::{Cursor, Seek, SeekFrom, Write};

/// Task-scoped serialization state threaded through every [`BufBatchWriter`] of a task.
///
/// A shuffle task creates one short-lived `BufBatchWriter` per output partition per spill or
/// finish cycle, so anything the writer owned would be rebuilt `partitions x cycles` times.
/// Both members here are cheap to keep and were previously rebuilt per writer:
///
/// * `buffer` is the byte buffer blocks are serialized into before being handed to the
///   underlying writer in `buffer_max_size` chunks. It is capped back to `buffer_max_size`
///   after every drain, so it never retains more than one block past that size.
/// * `ipc_context` holds arrow-ipc's flatbuffer builder for record-batch metadata. With
///   arrow's default `reserve_scratch = false`, which is what this uses, the context does not
///   retain the block body between encodes (each block body is a fresh `Vec` that is dropped
///   after the write), so sharing it costs one small metadata builder per task, not a block.
///
/// The struct is borrowed per call rather than owned, so an error mid-partition cannot strand it
/// inside a dropped writer and silently end recycling.
#[derive(Default)]
pub(crate) struct ShuffleScratch {
    pub(crate) buffer: Vec<u8>,
    pub(crate) ipc_context: IpcWriteContext,
}

impl ShuffleScratch {
    /// Drops any bytes left in the buffer. Used after an error so a failed partition's bytes
    /// cannot leak into the next partition's block; the IPC context holds no block data.
    pub(crate) fn clear(&mut self) {
        self.buffer.clear();
    }
}

/// Write batches to writer while using a buffer to avoid frequent system calls.
/// The record batches are first written by ShuffleBlockWriter into a caller-provided
/// scratch buffer. Once the scratch exceeds the max size, it is flushed to the writer.
///
/// The scratch is borrowed per call rather than owned: task-scoped [`ShuffleScratch`] is
/// threaded through every `write`/`flush`, so one buffer and one IPC context serve all the
/// short-lived writers of a task.
///
/// A writer either coalesces or passes batches through, chosen at construction:
///
/// * [`Self::new`]: small batches are coalesced using Arrow's [`BatchCoalescer`] before
///   serialization, reducing per-block IPC schema overhead. Output batches hold at least
///   `batch_size` rows, apart from the remainder emitted on flush. The coalescer is lazily
///   initialized on the first write and configured (via `biggest_coalesce_batch_size`) to pass
///   batches that are already at least `batch_size` rows straight through, verbatim and without
///   copying them, so an oversized input batch is written as a single oversized block. This is
///   the mode for the long-lived single-partition writer, whose inputs can genuinely be small.
/// * [`Self::new_passthrough`]: every batch is serialized as its own block, verbatim. This is
///   the mode for the per-partition writers of a multi-partition shuffle, whose input is a
///   `PartitionedBatchIterator` that already emits maximal `batch_size` chunks plus one tail:
///   there is never a second batch for the tail to coalesce with, so coalescing there would
///   only copy the tail's rows into builders and re-emit the same block.
pub(crate) struct BufBatchWriter<S: Borrow<ShuffleBlockWriter>, W: Write> {
    shuffle_block_writer: S,
    writer: W,
    buffer_max_size: usize,
    /// Coalesces small batches into target_batch_size before serialization.
    /// Lazily initialized on first write to capture the schema. Never set in passthrough mode.
    coalescer: Option<BatchCoalescer>,
    /// Target batch size for coalescing; `None` selects passthrough mode.
    coalesce_batch_size: Option<usize>,
    /// Address of the scratch seen on first use; every later call must pass the same one, or
    /// unflushed bytes in the other buffer would be silently abandoned.
    #[cfg(debug_assertions)]
    scratch_addr: Option<usize>,
    /// Running total of bytes serialized through this writer, used to report spilled bytes and
    /// to track output offsets without a `Seek` on the underlying writer.
    total_bytes_written: u64,
}

impl<S: Borrow<ShuffleBlockWriter>, W: Write> BufBatchWriter<S, W> {
    /// A coalescing writer; see the type-level docs for when to use which mode.
    pub(crate) fn new(
        shuffle_block_writer: S,
        writer: W,
        buffer_max_size: usize,
        batch_size: usize,
    ) -> Self {
        Self::with_mode(
            shuffle_block_writer,
            writer,
            buffer_max_size,
            Some(batch_size),
        )
    }

    /// A passthrough writer that serializes every batch as its own block, verbatim.
    pub(crate) fn new_passthrough(
        shuffle_block_writer: S,
        writer: W,
        buffer_max_size: usize,
    ) -> Self {
        Self::with_mode(shuffle_block_writer, writer, buffer_max_size, None)
    }

    fn with_mode(
        shuffle_block_writer: S,
        writer: W,
        buffer_max_size: usize,
        coalesce_batch_size: Option<usize>,
    ) -> Self {
        Self {
            shuffle_block_writer,
            writer,
            buffer_max_size,
            coalescer: None,
            coalesce_batch_size,
            #[cfg(debug_assertions)]
            scratch_addr: None,
            total_bytes_written: 0,
        }
    }

    /// A fresh writer must start from a drained scratch (stale bytes from a previous owner
    /// would be silently prepended to its first block), and every later call must pass the
    /// same scratch (bytes left unflushed in a swapped-out buffer would be silently lost).
    /// Identity is the scratch's own address, which is stable for the caller-owned field the
    /// writer is used with, unlike the data pointer that moves on regrowth.
    #[allow(unused_variables)]
    fn check_scratch(&mut self, scratch: &ShuffleScratch) {
        #[cfg(debug_assertions)]
        {
            let addr = scratch as *const ShuffleScratch as usize;
            match self.scratch_addr {
                None => {
                    debug_assert!(
                        scratch.buffer.is_empty(),
                        "fresh BufBatchWriter handed a non-empty scratch buffer ({} bytes)",
                        scratch.buffer.len()
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

    /// `scratch` is the caller-owned serialization state to encode into; threading the same
    /// one through every call reuses its capacity instead of regrowing a fresh allocation
    /// toward `buffer_max_size` for every writer.
    pub(crate) fn write(
        &mut self,
        batch: &RecordBatch,
        scratch: &mut ShuffleScratch,
        encode_time: &Time,
        write_time: &Time,
    ) -> datafusion::common::Result<usize> {
        self.check_scratch(scratch);
        let Some(batch_size) = self.coalesce_batch_size else {
            return self.write_batch_to_buffer(batch, scratch, encode_time, write_time);
        };
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
            bytes_written += self.write_batch_to_buffer(batch, scratch, encode_time, write_time)?;
        }
        Ok(bytes_written)
    }

    /// Serialize a single batch into the scratch buffer, flushing to the writer if needed.
    fn write_batch_to_buffer(
        &mut self,
        batch: &RecordBatch,
        scratch: &mut ShuffleScratch,
        encode_time: &Time,
        write_time: &Time,
    ) -> datafusion::common::Result<usize> {
        let ShuffleScratch {
            buffer,
            ipc_context,
        } = scratch;
        let mut cursor = Cursor::new(&mut *buffer);
        cursor.seek(SeekFrom::End(0))?;
        let bytes_written = self.shuffle_block_writer.borrow().write_batch(
            batch,
            &mut cursor,
            ipc_context,
            encode_time,
        )?;
        let pos = cursor.position();
        if pos >= self.buffer_max_size as u64 {
            let mut write_timer = write_time.timer();
            self.writer.write_all(buffer)?;
            write_timer.stop();
            buffer.clear();
        }
        self.total_bytes_written += bytes_written as u64;
        Ok(bytes_written)
    }

    /// Writes any rows still held by the coalescer and any bytes still in `scratch` to the
    /// underlying writer, without flushing that writer. `scratch` is left drained for the next
    /// writer.
    ///
    /// This is what a per-partition writer over a shared buffered output wants: the output's
    /// own buffer keeps accumulating across partitions and is flushed once at the end, instead
    /// of every partition ending in its own write syscall.
    pub(crate) fn drain(
        &mut self,
        scratch: &mut ShuffleScratch,
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
            self.write_batch_to_buffer(batch, scratch, encode_time, write_time)?;
        }

        // Hand the scratch buffer to the underlying writer
        let mut write_timer = write_time.timer();
        if !scratch.buffer.is_empty() {
            self.writer.write_all(&scratch.buffer)?;
        }
        write_timer.stop();
        scratch.buffer.clear();
        // The scratch's high-water mark can reach `buffer_max_size` plus the largest block
        // that crossed the threshold; keep only the configured buffer size across reuses.
        scratch.buffer.shrink_to(self.buffer_max_size);
        Ok(())
    }

    /// [`Self::drain`], then flushes the underlying writer.
    pub(crate) fn flush(
        &mut self,
        scratch: &mut ShuffleScratch,
        encode_time: &Time,
        write_time: &Time,
    ) -> datafusion::common::Result<()> {
        self.drain(scratch, encode_time, write_time)?;
        let mut write_timer = write_time.timer();
        self.writer.flush()?;
        write_timer.stop();
        Ok(())
    }

    /// Total number of bytes serialized through this writer since it was created. Unlike
    /// [`Self::writer_stream_position`], this does not require the underlying writer to implement
    /// [`Seek`], so it is used to report spilled bytes when writing to a `Box<dyn SpillWriter>`
    /// and to track output offsets without flushing a buffered output. After [`Self::drain`] or
    /// [`Self::flush`] every counted byte has been handed to the underlying writer.
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

    fn write_one_partition(seed: i64, scratch: &mut ShuffleScratch) -> Vec<u8> {
        let batch = test_batch(seed);
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::Zstd(1))
                .unwrap();
        let mut output = Vec::new();
        let time = Time::default();
        let mut writer = BufBatchWriter::new(block_writer, &mut output, 1 << 20, 8192);
        writer.write(&batch, scratch, &time, &time).unwrap();
        writer.flush(scratch, &time, &time).unwrap();
        output
    }

    /// A scratch buffer recycled across partitions must produce byte-identical output to
    /// fresh per-partition buffers, come back drained, and keep its grown capacity.
    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call zstd's C FFI
    fn recycled_scratch_matches_fresh_buffers_and_keeps_capacity() {
        let fresh: Vec<Vec<u8>> = (0..3)
            .map(|p| write_one_partition(p, &mut ShuffleScratch::default()))
            .collect();

        let mut scratch = ShuffleScratch::default();
        let mut recycled = Vec::new();
        for p in 0..3 {
            let output = write_one_partition(p, &mut scratch);
            assert!(
                scratch.buffer.is_empty(),
                "recycled scratch must come back drained"
            );
            recycled.push(output);
        }

        assert_eq!(fresh, recycled);
        assert!(
            scratch.buffer.capacity() > 0,
            "capacity grown in one partition must survive into the next"
        );
        for output in &recycled {
            let decoded = read_ipc_compressed(&output[16..]).unwrap();
            assert_eq!(decoded.num_rows(), 100);
        }
    }

    /// A passthrough writer must emit every input batch as its own block, in order, with no
    /// coalescing of a small tail into a following batch and no rows held back until `drain`.
    #[test]
    fn passthrough_writes_each_batch_as_its_own_block() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let make_batch = |start: i64, rows: i64| {
            let values: Vec<i64> = (start..start + rows).collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values))],
            )
            .unwrap()
        };
        // The shape a PartitionedBatchIterator produces: maximal chunks then one tail, but
        // also a tail followed by more batches, which a coalescer would merge.
        let inputs = [
            make_batch(0, 100),
            make_batch(100, 30),
            make_batch(130, 100),
            make_batch(230, 7),
        ];
        let block_writer =
            ShuffleBlockWriter::try_new(schema.as_ref(), CompressionCodec::None).unwrap();
        let mut output = Vec::new();
        let time = Time::default();
        let mut scratch = ShuffleScratch::default();
        let mut writer = BufBatchWriter::new_passthrough(block_writer, &mut output, 1 << 20);
        for batch in &inputs {
            writer.write(batch, &mut scratch, &time, &time).unwrap();
        }
        writer.drain(&mut scratch, &time, &time).unwrap();
        assert!(
            scratch.buffer.is_empty(),
            "drain must leave the scratch empty"
        );
        assert_eq!(writer.bytes_written() as usize, output.len());

        let mut block_rows = Vec::new();
        let mut next = 0i64;
        let mut pos = 0;
        while pos < output.len() {
            let len = u64::from_le_bytes(output[pos..pos + 8].try_into().unwrap()) as usize;
            let block = read_ipc_compressed(&output[pos + 16..pos + 8 + len]).unwrap();
            let values = block
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values.value(0), next, "rows must stay in input order");
            next += block.num_rows() as i64;
            block_rows.push(block.num_rows());
            pos += 8 + len;
        }
        assert_eq!(block_rows, vec![100, 30, 100, 7]);
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
        let mut dirty = ShuffleScratch {
            buffer: vec![0xAB, 0xCD],
            ..Default::default()
        };
        let _ = writer.write(&batch, &mut dirty, &time, &time);
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
        let mut first = ShuffleScratch::default();
        writer.write(&batch, &mut first, &time, &time).unwrap();
        let mut second = ShuffleScratch::default();
        let _ = writer.write(&batch, &mut second, &time, &time);
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
        let mut scratch = ShuffleScratch::default();
        let mut writer =
            BufBatchWriter::new(block_writer, &mut output, buffer_max_size, batch_size);
        writer.write(&batch, &mut scratch, &time, &time).unwrap();
        assert!(
            scratch.buffer.capacity() > buffer_max_size,
            "oversized block must have grown the scratch past the cap"
        );
        writer.flush(&mut scratch, &time, &time).unwrap();
        assert!(scratch.buffer.is_empty());
        assert!(
            scratch.buffer.capacity() <= buffer_max_size,
            "retained capacity {} exceeds cap {}",
            scratch.buffer.capacity(),
            buffer_max_size
        );

        // With a roomy cap the grown capacity is retained: `write` serializes into the
        // scratch (batch_size below the row count again), and `flush` must leave the
        // sub-cap capacity exactly unchanged rather than shrinking it.
        let large_cap = 1 << 20;
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut output = Vec::new();
        let mut scratch = ShuffleScratch::default();
        let mut writer = BufBatchWriter::new(block_writer, &mut output, large_cap, batch_size);
        writer.write(&batch, &mut scratch, &time, &time).unwrap();
        let cap_after_write = scratch.buffer.capacity();
        assert!(
            cap_after_write > 0 && cap_after_write <= large_cap,
            "write must have serialized the batch into the scratch"
        );
        writer.flush(&mut scratch, &time, &time).unwrap();
        assert!(scratch.buffer.is_empty());
        assert_eq!(
            scratch.buffer.capacity(),
            cap_after_write,
            "flush must not shrink a scratch already under the cap"
        );
    }
}
