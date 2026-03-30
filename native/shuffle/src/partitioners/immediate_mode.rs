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
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use crate::CompressionCodec;

/// Per-partition output stream that serializes Arrow IPC batches into an
/// in-memory buffer with compression. The block format matches
/// `ShuffleBlockWriter::write_batch` exactly:
///
/// - 8 bytes: payload length (u64 LE) — total bytes after this prefix
/// - 8 bytes: field_count (usize LE)
/// - 4 bytes: codec tag (b"SNAP", b"LZ4_", b"ZSTD", or b"NONE")
/// - N bytes: compressed Arrow IPC stream data
#[allow(dead_code)]
pub struct PartitionOutputStream {
    schema: SchemaRef,
    codec: CompressionCodec,
    buffer: Vec<u8>,
}

#[allow(dead_code)]
impl PartitionOutputStream {
    pub fn try_new(schema: SchemaRef, codec: CompressionCodec) -> Result<Self> {
        Ok(Self {
            schema,
            codec,
            buffer: Vec::with_capacity(1024 * 1024),
        })
    }

    /// Writes a record batch as a length-prefixed compressed IPC block.
    /// Returns the number of bytes written to the buffer.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<usize> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let start_pos = self.buffer.len();

        // Write 8-byte placeholder for length prefix
        self.buffer.extend_from_slice(&0u64.to_le_bytes());

        // Write field count (8 bytes, usize LE)
        let field_count = self.schema.fields().len();
        self.buffer
            .extend_from_slice(&(field_count as u64).to_le_bytes());

        // Write codec tag (4 bytes)
        let codec_tag: &[u8; 4] = match &self.codec {
            CompressionCodec::Snappy => b"SNAP",
            CompressionCodec::Lz4Frame => b"LZ4_",
            CompressionCodec::Zstd(_) => b"ZSTD",
            CompressionCodec::None => b"NONE",
        };
        self.buffer.extend_from_slice(codec_tag);

        // Write compressed IPC data
        match &self.codec {
            CompressionCodec::None => {
                let mut arrow_writer = StreamWriter::try_new(&mut self.buffer, &batch.schema())?;
                arrow_writer.write(batch)?;
                arrow_writer.finish()?;
                // StreamWriter::into_inner returns the inner writer; we don't need it
                // since we're writing directly to self.buffer
                arrow_writer.into_inner()?;
            }
            CompressionCodec::Lz4Frame => {
                let mut wtr = lz4_flex::frame::FrameEncoder::new(&mut self.buffer);
                let mut arrow_writer = StreamWriter::try_new(&mut wtr, &batch.schema())?;
                arrow_writer.write(batch)?;
                arrow_writer.finish()?;
                wtr.finish().map_err(|e| {
                    DataFusionError::Execution(format!("lz4 compression error: {e}"))
                })?;
            }
            CompressionCodec::Zstd(level) => {
                let encoder = zstd::Encoder::new(&mut self.buffer, *level)?;
                let mut arrow_writer = StreamWriter::try_new(encoder, &batch.schema())?;
                arrow_writer.write(batch)?;
                arrow_writer.finish()?;
                let zstd_encoder = arrow_writer.into_inner()?;
                zstd_encoder.finish()?;
            }
            CompressionCodec::Snappy => {
                let mut wtr = snap::write::FrameEncoder::new(&mut self.buffer);
                let mut arrow_writer = StreamWriter::try_new(&mut wtr, &batch.schema())?;
                arrow_writer.write(batch)?;
                arrow_writer.finish()?;
                wtr.into_inner().map_err(|e| {
                    DataFusionError::Execution(format!("snappy compression error: {e}"))
                })?;
            }
        }

        // Backfill length prefix: total bytes after the 8-byte length field
        let end_pos = self.buffer.len();
        let ipc_length = (end_pos - start_pos - 8) as u64;

        let max_size = i32::MAX as u64;
        if ipc_length > max_size {
            return Err(DataFusionError::Execution(format!(
                "Shuffle block size {ipc_length} exceeds maximum size of {max_size}. \
                Try reducing batch size or increasing compression level"
            )));
        }

        self.buffer[start_pos..start_pos + 8].copy_from_slice(&ipc_length.to_le_bytes());

        Ok(end_pos - start_pos)
    }

    /// Returns the number of bytes currently in the buffer.
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Takes the buffer contents, leaving the buffer empty.
    pub fn drain_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    /// Consumes self and returns the buffer.
    pub fn finish(self) -> Result<Vec<u8>> {
        Ok(self.buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_ipc_compressed;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let array = Int32Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_partition_output_stream_write_and_read() {
        let batch = make_test_batch(&[1, 2, 3, 4, 5]);
        let schema = batch.schema();

        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4Frame,
            CompressionCodec::Zstd(1),
            CompressionCodec::Snappy,
        ] {
            let mut stream =
                PartitionOutputStream::try_new(Arc::clone(&schema), codec).unwrap();
            let bytes_written = stream.write_batch(&batch).unwrap();
            assert!(bytes_written > 0);
            assert_eq!(stream.buffered_bytes(), bytes_written);

            let buf = stream.finish().unwrap();
            assert_eq!(buf.len(), bytes_written);

            // Parse the block: 8 bytes length, 8 bytes field_count, then codec+data
            let ipc_length =
                u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
            assert_eq!(ipc_length, buf.len() - 8);

            let field_count =
                usize::from_le_bytes(buf[8..16].try_into().unwrap());
            assert_eq!(field_count, 1); // one field "a"

            // read_ipc_compressed expects data starting at the codec tag
            let ipc_data = &buf[16..];
            let batch2 = read_ipc_compressed(ipc_data).unwrap();
            assert_eq!(batch, batch2);
        }
    }

    #[test]
    fn test_partition_output_stream_multiple_batches() {
        let batch1 = make_test_batch(&[1, 2, 3]);
        let batch2 = make_test_batch(&[4, 5, 6, 7]);
        let schema = batch1.schema();

        let mut stream =
            PartitionOutputStream::try_new(schema, CompressionCodec::None).unwrap();

        let bytes1 = stream.write_batch(&batch1).unwrap();
        assert!(bytes1 > 0);

        let bytes2 = stream.write_batch(&batch2).unwrap();
        assert!(bytes2 > 0);

        assert_eq!(stream.buffered_bytes(), bytes1 + bytes2);

        let buf = stream.finish().unwrap();

        // Read first block
        let len1 = u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
        let block1_end = 8 + len1;
        let ipc_data1 = &buf[16..block1_end];
        let decoded1 = read_ipc_compressed(ipc_data1).unwrap();
        assert_eq!(batch1, decoded1);

        // Read second block
        let len2 = u64::from_le_bytes(
            buf[block1_end..block1_end + 8].try_into().unwrap(),
        ) as usize;
        let block2_end = block1_end + 8 + len2;
        let ipc_data2 = &buf[block1_end + 16..block2_end];
        let decoded2 = read_ipc_compressed(ipc_data2).unwrap();
        assert_eq!(batch2, decoded2);

        assert_eq!(block2_end, buf.len());
    }

    #[test]
    fn test_partition_output_stream_empty_batch() {
        let batch = make_test_batch(&[]);
        let schema = batch.schema();

        let mut stream =
            PartitionOutputStream::try_new(schema, CompressionCodec::None).unwrap();
        let bytes_written = stream.write_batch(&batch).unwrap();
        assert_eq!(bytes_written, 0);
        assert_eq!(stream.buffered_bytes(), 0);
    }

    #[test]
    fn test_partition_output_stream_drain_buffer() {
        let batch = make_test_batch(&[1, 2, 3]);
        let schema = batch.schema();

        let mut stream =
            PartitionOutputStream::try_new(schema, CompressionCodec::None).unwrap();
        stream.write_batch(&batch).unwrap();
        assert!(stream.buffered_bytes() > 0);

        let drained = stream.drain_buffer();
        assert!(!drained.is_empty());
        assert_eq!(stream.buffered_bytes(), 0);

        // Can still write after drain
        stream.write_batch(&batch).unwrap();
        assert!(stream.buffered_bytes() > 0);
    }
}
