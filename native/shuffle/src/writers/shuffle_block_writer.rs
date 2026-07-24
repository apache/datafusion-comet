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
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::physical_plan::metrics::Time;
use std::cell::RefCell;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

/// Arrow IPC stream end-of-stream marker: the continuation marker (`0xFFFFFFFF`) followed by a
/// zero message length, matching what `StreamWriter::finish` emits for metadata version V5.
const IPC_EOS: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00];

/// Compression algorithm applied to shuffle IPC blocks.
#[derive(Debug, Clone)]
pub enum CompressionCodec {
    None,
    Lz4Frame,
    Zstd(i32),
    Snappy,
}

/// Returns true if `data_type` is, or nests, a dictionary type.
fn contains_dictionary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _) => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _)
        | DataType::RunEndEncoded(_, f) => contains_dictionary(f.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| contains_dictionary(f.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, f)| contains_dictionary(f.data_type())),
        _ => false,
    }
}

/// Writes a record batch as a length-prefixed, compressed Arrow IPC block.
///
/// Each block is a self-contained Arrow IPC stream (schema message, dictionary messages, record
/// batch message, end-of-stream marker). For the common case of a schema with no dictionary types,
/// the schema flatbuffer is encoded once in [`Self::try_new`] and written verbatim at the start of
/// every block, rather than being re-serialized per block. Schemas that contain dictionary types
/// fall back to `StreamWriter`, whose dictionary-id bookkeeping ties schema and batch encoding
/// together.
#[derive(Clone)]
pub struct ShuffleBlockWriter {
    codec: CompressionCodec,
    header_bytes: Vec<u8>,
    schema: SchemaRef,
    /// Pre-encoded Arrow IPC schema message, written at the start of every block. Only used when
    /// the schema has no dictionary types.
    schema_message: Vec<u8>,
    /// Whether the schema contains any dictionary types (see [`Self::encode_ipc_stream`]).
    has_dictionaries: bool,
}

impl ShuffleBlockWriter {
    pub fn try_new(schema: &Schema, codec: CompressionCodec) -> Result<Self> {
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

        // Pre-encode the IPC schema message once so it does not have to be re-serialized per block.
        let options = IpcWriteOptions::default();
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(true);
        let encoded_schema = data_gen.schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut dictionary_tracker,
            &options,
        );
        let mut schema_message = Vec::new();
        write_message(&mut schema_message, encoded_schema, &options)?;

        let has_dictionaries = schema
            .fields()
            .iter()
            .any(|f| contains_dictionary(f.data_type()));

        Ok(Self {
            codec,
            header_bytes,
            schema: Arc::new(schema.clone()),
            schema_message,
            has_dictionaries,
        })
    }

    /// Serialize `batch` as a standalone Arrow IPC stream into `out`.
    fn encode_ipc_stream<W: Write + ?Sized>(&self, batch: &RecordBatch, out: &mut W) -> Result<()> {
        if self.has_dictionaries {
            // Dictionary encoding requires the schema and record batch to share a dictionary
            // tracker, so `StreamWriter` (which re-encodes the schema per block) is used here.
            let mut stream_writer = StreamWriter::try_new(out, &self.schema)?;
            stream_writer.write(batch)?;
            stream_writer.finish()?;
            return Ok(());
        }

        // Fast path: reuse the pre-encoded schema message and write the record batch manually.
        let options = IpcWriteOptions::default();
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(true);
        let mut compression_context = CompressionContext::default();
        let (encoded_dictionaries, encoded_batch) = data_gen.encode(
            batch,
            &mut dictionary_tracker,
            &options,
            &mut compression_context,
        )?;
        debug_assert!(encoded_dictionaries.is_empty());

        out.write_all(&self.schema_message)?;
        write_message(&mut *out, encoded_batch, &options)?;
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
                // Reuse the zstd compression context (ZSTD_CCtx + workspace) across blocks on this
                // thread instead of allocating a fresh one per block. The stream is compressed
                // incrementally (not buffered whole) so large blocks are not copied an extra time.
                ZSTD_ENCODER.with(|encoder| {
                    encoder
                        .borrow_mut()
                        .compress_stream(*level, output, |w| self.encode_ipc_stream(batch, w))
                })?;
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

/// Per-thread reusable zstd compression context, so the streaming shuffle writer does not allocate
/// a fresh `ZSTD_CCtx` (and workspace) for every block.
struct ZstdBlockEncoder {
    context: zstd::zstd_safe::CCtx<'static>,
    /// The compression level currently configured on the context; the level is (re)applied only
    /// when it changes.
    level: Option<i32>,
}

impl ZstdBlockEncoder {
    /// Compress the IPC stream produced by `build` into `output`, reusing this thread's
    /// `ZSTD_CCtx`. Compression is streamed incrementally, so the uncompressed stream is never
    /// fully buffered.
    fn compress_stream<W: Write>(
        &mut self,
        level: i32,
        output: &mut W,
        build: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<()> {
        if self.level != Some(level) {
            self.context
                .set_parameter(zstd::zstd_safe::CParameter::CompressionLevel(level))
                .map_err(|_| {
                    DataFusionError::Execution(format!("failed to set zstd level {level}"))
                })?;
            self.level = Some(level);
        }

        let mut encoder = zstd::stream::write::Encoder::with_context(output, &mut self.context);
        build(&mut encoder)?;
        encoder
            .finish()
            .map_err(|e| DataFusionError::Execution(format!("zstd compression error: {e}")))?;
        Ok(())
    }
}

thread_local! {
    static ZSTD_ENCODER: RefCell<ZstdBlockEncoder> = RefCell::new(ZstdBlockEncoder {
        context: zstd::zstd_safe::CCtx::create(),
        level: None,
    });
}
