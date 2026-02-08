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

use crate::execution::shuffle::ShuffleBlockWriter;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use datafusion::physical_plan::metrics::Time;
use std::borrow::Borrow;
use std::io::{Cursor, Seek, SeekFrom, Write};

/// Write batches to writer while using a buffer to avoid frequent system calls.
/// The record batches were first written by ShuffleBlockWriter into an internal buffer.
/// Once the buffer exceeds the max size, the buffer will be flushed to the writer.
///
/// When `batch_size > 1`, small batches are accumulated and coalesced via `concat_batches`
/// before serialization, reducing per-batch IPC schema overhead.
pub(crate) struct BufBatchWriter<S: Borrow<ShuffleBlockWriter>, W: Write> {
    shuffle_block_writer: S,
    writer: W,
    buffer: Vec<u8>,
    buffer_max_size: usize,
    /// Accumulated small batches waiting to be coalesced and serialized
    pending_batches: Vec<RecordBatch>,
    /// Total number of rows across pending_batches
    pending_rows: usize,
    /// Target batch size for coalescing; batches accumulate until this threshold is reached
    batch_size: usize,
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
            buffer: vec![],
            buffer_max_size,
            pending_batches: vec![],
            pending_rows: 0,
            batch_size,
        }
    }

    pub(crate) fn write(
        &mut self,
        batch: &RecordBatch,
        encode_time: &Time,
        write_time: &Time,
    ) -> datafusion::common::Result<usize> {
        self.pending_rows += batch.num_rows();
        self.pending_batches.push(batch.clone());
        if self.pending_rows >= self.batch_size {
            self.flush_pending(encode_time, write_time)
        } else {
            Ok(0)
        }
    }

    /// Coalesce pending batches and serialize to the byte buffer.
    fn flush_pending(
        &mut self,
        encode_time: &Time,
        write_time: &Time,
    ) -> datafusion::common::Result<usize> {
        if self.pending_batches.is_empty() {
            return Ok(0);
        }

        let coalesced = if self.pending_batches.len() == 1 {
            self.pending_batches.remove(0)
        } else {
            let schema = self.pending_batches[0].schema();
            concat_batches(&schema, self.pending_batches.iter())?
        };
        self.pending_batches.clear();
        self.pending_rows = 0;

        self.write_batch_to_buffer(&coalesced, encode_time, write_time)
    }

    /// Serialize a single batch into the byte buffer, flushing to the writer if needed.
    fn write_batch_to_buffer(
        &mut self,
        batch: &RecordBatch,
        encode_time: &Time,
        write_time: &Time,
    ) -> datafusion::common::Result<usize> {
        let mut cursor = Cursor::new(&mut self.buffer);
        cursor.seek(SeekFrom::End(0))?;
        let bytes_written =
            self.shuffle_block_writer
                .borrow()
                .write_batch(batch, &mut cursor, encode_time)?;
        let pos = cursor.position();
        if pos >= self.buffer_max_size as u64 {
            let mut write_timer = write_time.timer();
            self.writer.write_all(&self.buffer)?;
            write_timer.stop();
            self.buffer.clear();
        }
        Ok(bytes_written)
    }

    pub(crate) fn flush(
        &mut self,
        encode_time: &Time,
        write_time: &Time,
    ) -> datafusion::common::Result<()> {
        self.flush_pending(encode_time, write_time)?;
        let mut write_timer = write_time.timer();
        if !self.buffer.is_empty() {
            self.writer.write_all(&self.buffer)?;
        }
        self.writer.flush()?;
        write_timer.stop();
        self.buffer.clear();
        Ok(())
    }
}

impl<S: Borrow<ShuffleBlockWriter>, W: Write + Seek> BufBatchWriter<S, W> {
    pub(crate) fn writer_stream_position(&mut self) -> datafusion::common::Result<u64> {
        self.writer.stream_position().map_err(Into::into)
    }
}
