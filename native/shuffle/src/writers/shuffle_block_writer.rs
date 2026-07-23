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
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::ipc::writer::{
    write_message, CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
    StreamWriter,
};
use arrow::ipc::MetadataVersion;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::physical_plan::metrics::Time;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

/// Arrow IPC stream end-of-stream marker: the continuation marker (`0xFFFFFFFF`) followed by a
/// zero message length. This is what `StreamWriter::finish` emits for metadata version V5 with
/// non-legacy framing; a V4-legacy stream would instead emit four zero bytes with no continuation
/// marker. The fast path pins its `IpcWriteOptions` to V5 (see [`ShuffleBlockWriter::try_new`]), so
/// this constant is valid; if that assumption ever changes, this must be revisited.
const IPC_EOS: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00];

/// Compression algorithm applied to shuffle IPC blocks.
#[derive(Debug, Clone)]
pub enum CompressionCodec {
    None,
    Lz4Frame,
    Zstd(i32),
    Snappy,
}

/// How a writer encodes the Arrow IPC schema at the start of each block. The two arms are mutually
/// exclusive by schema shape, decided once in [`ShuffleBlockWriter::try_new`], so the retained
/// state never carries both a pre-encoded message and a schema at the same time.
#[derive(Clone)]
enum SchemaEncoding {
    /// Dictionary-free schema: the IPC schema message, pre-encoded once, is written verbatim at the
    /// start of every block instead of being re-serialized per block.
    Precoded(Vec<u8>),
    /// Schema containing dictionary types: the schema and record batch must share a dictionary
    /// tracker, so each block is encoded with `StreamWriter`, which re-serializes the schema.
    Fallback(SchemaRef),
}

/// Writes a record batch as a length-prefixed, compressed Arrow IPC block.
///
/// Each block is a self-contained Arrow IPC stream (schema message, dictionary messages, record
/// batch message, end-of-stream marker). For the common case of a schema with no dictionary types,
/// the schema flatbuffer is encoded once in [`Self::try_new`] and written verbatim at the start of
/// every block, rather than being re-serialized per block as `StreamWriter::try_new` would do.
/// Schemas that contain dictionary types fall back to `StreamWriter`, whose dictionary-id
/// bookkeeping ties schema and batch encoding together.
#[derive(Clone)]
pub struct ShuffleBlockWriter {
    codec: CompressionCodec,
    header_bytes: Vec<u8>,
    /// IPC options shared by the schema and record-batch encoders so the two can never diverge;
    /// pinned to metadata version V5, which [`IPC_EOS`] depends on.
    write_options: IpcWriteOptions,
    schema_encoding: SchemaEncoding,
}

impl ShuffleBlockWriter {
    pub fn try_new(schema: &Schema, codec: CompressionCodec) -> Result<Self> {
        // Header layout: 8-byte block length placeholder + 8-byte field count (usize) + 4-byte
        // codec tag = 20 bytes.
        let mut header_bytes = Vec::with_capacity(20);

        // leave space for compressed message length (filled in per block by write_batch)
        header_bytes.extend_from_slice(&[0u8; 8]);

        // write number of columns because JVM side needs to know how many addresses to allocate
        let field_count = schema.fields().len();
        header_bytes.extend_from_slice(&field_count.to_le_bytes());

        // write compression codec to header
        let codec_header: &[u8] = match &codec {
            CompressionCodec::Snappy => b"SNAP",
            CompressionCodec::Lz4Frame => b"LZ4_",
            CompressionCodec::Zstd(_) => b"ZSTD",
            CompressionCodec::None => b"NONE",
        };
        header_bytes.extend_from_slice(codec_header);

        // Shuffle blocks are always written with metadata version V5. Pin it explicitly rather than
        // relying on `IpcWriteOptions::default`, because IPC_EOS is only the correct end-of-stream
        // marker for V5. Alignment 64 and non-legacy framing match the arrow defaults.
        let write_options = IpcWriteOptions::try_new(64, false, MetadataVersion::V5)?;

        // `flattened_fields` walks the full nested field tree, so this catches dictionary types
        // nested under any container arrow itself recurses into when emitting dictionaries.
        let has_dictionaries = schema
            .flattened_fields()
            .iter()
            .any(|f| matches!(f.data_type(), DataType::Dictionary(_, _)));

        // For dictionary-free schemas, pre-encode the IPC schema message once so it does not have
        // to be re-serialized per block. Dictionary schemas use the `StreamWriter` fallback.
        let schema_encoding = if has_dictionaries {
            SchemaEncoding::Fallback(Arc::new(schema.clone()))
        } else {
            let data_gen = IpcDataGenerator::default();
            let mut dictionary_tracker = DictionaryTracker::new(true);
            let encoded_schema = data_gen.schema_to_bytes_with_dictionary_tracker(
                schema,
                &mut dictionary_tracker,
                &write_options,
            );
            let mut buf = Vec::new();
            write_message(&mut buf, encoded_schema, &write_options)?;
            SchemaEncoding::Precoded(buf)
        };

        Ok(Self {
            codec,
            header_bytes,
            write_options,
            schema_encoding,
        })
    }

    /// Serialize `batch` as a standalone Arrow IPC stream into `out`.
    fn encode_ipc_stream<W: Write>(&self, batch: &RecordBatch, out: &mut W) -> Result<()> {
        let schema_message = match &self.schema_encoding {
            SchemaEncoding::Fallback(schema) => {
                // Dictionary encoding requires the schema and record batch to share a dictionary
                // tracker, so `StreamWriter` (which re-encodes the schema per block) is used here.
                let mut stream_writer =
                    StreamWriter::try_new_with_options(out, schema, self.write_options.clone())?;
                stream_writer.write(batch)?;
                stream_writer.finish()?;
                return Ok(());
            }
            SchemaEncoding::Precoded(schema_message) => schema_message,
        };

        // Fast path: reuse the pre-encoded schema message and write the record batch manually.
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(true);
        let mut compression_context = CompressionContext::default();
        let (encoded_dictionaries, encoded_batch) = data_gen.encode(
            batch,
            &mut dictionary_tracker,
            &self.write_options,
            &mut compression_context,
        )?;
        debug_assert!(encoded_dictionaries.is_empty());

        out.write_all(schema_message)?;
        write_message(&mut *out, encoded_batch, &self.write_options)?;
        out.write_all(&IPC_EOS)?;
        Ok(())
    }

    /// Writes given record batch as Arrow IPC bytes into given writer.
    /// Returns number of bytes written.
    pub fn write_batch<W: Write + Seek>(
        &self,
        batch: &RecordBatch,
        output: &mut W,
        ipc_time: &Time,
    ) -> Result<usize> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let mut timer = ipc_time.timer();
        let start_pos = output.stream_position()?;

        // write header
        output.write_all(&self.header_bytes)?;

        match &self.codec {
            CompressionCodec::None => {
                self.encode_ipc_stream(batch, output)?;
            }
            CompressionCodec::Lz4Frame => {
                let mut wtr = lz4_flex::frame::FrameEncoder::new(&mut *output);
                self.encode_ipc_stream(batch, &mut wtr)?;
                wtr.finish().map_err(|e| {
                    DataFusionError::Execution(format!("lz4 compression error: {e}"))
                })?;
            }
            CompressionCodec::Snappy => {
                let mut wtr = snap::write::FrameEncoder::new(&mut *output);
                self.encode_ipc_stream(batch, &mut wtr)?;
                wtr.into_inner().map_err(|e| {
                    DataFusionError::Execution(format!("snappy compression error: {e}"))
                })?;
            }
            CompressionCodec::Zstd(level) => {
                let mut encoder = zstd::Encoder::new(&mut *output, *level)?;
                self.encode_ipc_stream(batch, &mut encoder)?;
                encoder.finish()?;
            }
        }

        // fill ipc length
        let end_pos = output.stream_position()?;
        let ipc_length = end_pos - start_pos - 8;
        let max_size = i32::MAX as u64;
        if ipc_length > max_size {
            return Err(DataFusionError::Execution(format!(
                "Shuffle block size {ipc_length} exceeds maximum size of {max_size}. \
                Try reducing batch size or increasing compression level"
            )));
        }

        output.seek(SeekFrom::Start(start_pos))?;
        output.write_all(&ipc_length.to_le_bytes())?;
        output.seek(SeekFrom::Start(end_pos))?;

        timer.stop();

        Ok((end_pos - start_pos) as usize)
    }
}
