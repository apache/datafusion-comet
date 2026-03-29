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

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::CompressionType;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::physical_plan::metrics::Time;
use std::io::{Seek, SeekFrom, Write};

use super::CompressionCodec;

/// Maps a [`CompressionCodec`] to Arrow IPC [`IpcWriteOptions`].
///
/// Arrow IPC body compression supports LZ4_FRAME and ZSTD. Snappy is not
/// part of the Arrow IPC specification and will return an error.
fn ipc_write_options(codec: &CompressionCodec) -> Result<IpcWriteOptions> {
    match codec {
        CompressionCodec::None => Ok(IpcWriteOptions::default()),
        CompressionCodec::Lz4Frame => IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None)),
        CompressionCodec::Zstd(_) => IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None)),
        CompressionCodec::Snappy => Err(DataFusionError::NotImplemented(
            "Snappy compression is not supported for IPC stream format. \
             Use LZ4 or ZSTD instead."
                .to_string(),
        )),
    }
}

/// Writes record batches as a standard Arrow IPC stream.
///
/// Unlike [`super::ShuffleBlockWriter`] which writes each batch as a self-contained
/// block with a custom header (length prefix + field count + codec), this writer
/// produces a standard Arrow IPC stream where the schema is written once and
/// each batch is an IPC record batch message within the stream.
///
/// Benefits over the block-based format:
/// - Schema is written once per stream instead of once per batch
/// - Standard Arrow IPC format, readable by any Arrow-compatible tool
/// - Uses Arrow's built-in IPC body compression (LZ4_FRAME or ZSTD)
///
/// The writer is stateful: it must be created, used to write batches, then
/// finished. The schema is written on creation and the end-of-stream marker
/// is written on [`finish`](Self::finish).
///
/// # Example
///
/// ```ignore
/// let mut writer = IpcStreamWriter::try_new(file, &schema, CompressionCodec::Lz4Frame)?;
/// writer.write_batch(&batch1, &ipc_time)?;
/// writer.write_batch(&batch2, &ipc_time)?;
/// let file = writer.finish()?;
/// ```
pub struct IpcStreamWriter<W: Write> {
    writer: StreamWriter<W>,
}

impl<W: Write> IpcStreamWriter<W> {
    /// Creates a new IPC stream writer.
    ///
    /// Writes the IPC stream header (schema message) to the output immediately.
    pub fn try_new(output: W, schema: &Schema, codec: CompressionCodec) -> Result<Self> {
        let options = ipc_write_options(&codec)?;
        let writer = StreamWriter::try_new_with_options(output, schema, options)?;
        Ok(Self { writer })
    }

    /// Writes a record batch as an IPC message within the stream.
    ///
    /// Empty batches (0 rows) are skipped.
    pub fn write_batch(&mut self, batch: &RecordBatch, ipc_time: &Time) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut timer = ipc_time.timer();
        self.writer.write(batch)?;
        timer.stop();
        Ok(())
    }

    /// Writes the end-of-stream marker and returns the underlying writer.
    pub fn finish(mut self) -> Result<W> {
        self.writer.finish()?;
        self.writer.into_inner().map_err(Into::into)
    }
}

impl<W: Write + Seek> IpcStreamWriter<W> {
    /// Creates a new IPC stream writer with space reserved for an 8-byte length
    /// prefix. Call [`finish_length_prefixed`](Self::finish_length_prefixed)
    /// instead of `finish` to fill in the prefix.
    pub fn try_new_length_prefixed(
        mut output: W,
        schema: &Schema,
        codec: CompressionCodec,
    ) -> Result<Self> {
        // Reserve 8 bytes for the length prefix (filled in on finish)
        output.write_all(&[0u8; 8])?;
        Self::try_new(output, schema, codec)
    }

    /// Finishes the IPC stream and fills in the 8-byte length prefix that was
    /// reserved by [`try_new_length_prefixed`](Self::try_new_length_prefixed).
    ///
    /// The length prefix covers the IPC stream data only (not itself).
    pub fn finish_length_prefixed(self, start_pos: u64) -> Result<W> {
        let mut output = self.finish()?;
        let end_pos = output.stream_position()?;
        let ipc_length = end_pos - start_pos - 8;
        output.seek(SeekFrom::Start(start_pos))?;
        output.write_all(&ipc_length.to_le_bytes())?;
        output.seek(SeekFrom::Start(end_pos))?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    use std::sync::Arc;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ])
    }

    fn test_batch(schema: &Schema, n: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![n, n + 1, n + 2])),
                Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])),
            ],
        )
        .unwrap()
    }

    fn roundtrip(codec: CompressionCodec, num_batches: usize) {
        let schema = test_schema();
        let ipc_time = Time::default();

        let mut buf = Vec::new();
        {
            let cursor = Cursor::new(&mut buf);
            let mut writer = IpcStreamWriter::try_new(cursor, &schema, codec).unwrap();
            for i in 0..num_batches {
                let batch = test_batch(&schema, (i * 10) as i32);
                writer.write_batch(&batch, &ipc_time).unwrap();
            }
            writer.finish().unwrap();
        }

        // Read back
        let cursor = Cursor::new(&buf);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), num_batches);

        for (i, batch) in batches.iter().enumerate() {
            assert_eq!(batch.num_rows(), 3);
            let col_a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let base = (i * 10) as i32;
            assert_eq!(col_a.value(0), base);
            assert_eq!(col_a.value(1), base + 1);
            assert_eq!(col_a.value(2), base + 2);
        }
    }

    #[test]
    fn test_roundtrip_no_compression() {
        roundtrip(CompressionCodec::None, 3);
    }

    #[test]
    fn test_roundtrip_lz4() {
        roundtrip(CompressionCodec::Lz4Frame, 3);
    }

    #[test]
    fn test_roundtrip_zstd() {
        roundtrip(CompressionCodec::Zstd(1), 3);
    }

    #[test]
    fn test_empty_batch_skipped() {
        let schema = test_schema();
        let ipc_time = Time::default();

        let mut buf = Vec::new();
        {
            let cursor = Cursor::new(&mut buf);
            let mut writer =
                IpcStreamWriter::try_new(cursor, &schema, CompressionCodec::None).unwrap();

            // Write a real batch, an empty batch, then another real batch
            writer
                .write_batch(&test_batch(&schema, 0), &ipc_time)
                .unwrap();
            let empty = RecordBatch::new_empty(Arc::new(schema.clone()));
            writer.write_batch(&empty, &ipc_time).unwrap();
            writer
                .write_batch(&test_batch(&schema, 10), &ipc_time)
                .unwrap();
            writer.finish().unwrap();
        }

        let cursor = Cursor::new(&buf);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 2); // empty batch was skipped
    }

    #[test]
    fn test_snappy_unsupported() {
        let schema = test_schema();
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let result = IpcStreamWriter::try_new(cursor, &schema, CompressionCodec::Snappy);
        let err = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error for Snappy"),
        };
        assert!(err.contains("Snappy compression is not supported"));
    }

    #[test]
    fn test_single_batch() {
        roundtrip(CompressionCodec::None, 1);
    }
}
