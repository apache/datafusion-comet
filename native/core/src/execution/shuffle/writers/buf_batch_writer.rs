use crate::execution::shuffle::ShuffleBlockWriter;
use arrow::array::RecordBatch;
use datafusion::physical_plan::metrics::Time;
use std::borrow::Borrow;
use std::io::{Cursor, Seek, SeekFrom, Write};

/// Write batches to writer while using a buffer to avoid frequent system calls.
/// The record batches were first written by ShuffleBlockWriter into an internal buffer.
/// Once the buffer exceeds the max size, the buffer will be flushed to the writer.
pub(crate) struct BufBatchWriter<S: Borrow<ShuffleBlockWriter>, W: Write> {
    shuffle_block_writer: S,
    writer: W,
    buffer: Vec<u8>,
    buffer_max_size: usize,
}

impl<S: Borrow<ShuffleBlockWriter>, W: Write> BufBatchWriter<S, W> {
    pub(crate) fn new(shuffle_block_writer: S, writer: W, buffer_max_size: usize) -> Self {
        Self {
            shuffle_block_writer,
            writer,
            buffer: vec![],
            buffer_max_size,
        }
    }

    pub(crate) fn write(
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

    pub(crate) fn flush(&mut self, write_time: &Time) -> datafusion::common::Result<()> {
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
