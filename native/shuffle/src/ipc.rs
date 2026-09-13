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

use arrow::array::{ArrayRef, RecordBatch};
use arrow::buffer::MutableBuffer;
use arrow::datatypes::SchemaRef;
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::{read_dictionary_impl, RecordBatchDecoder};
use arrow::ipc::{root_as_message, MessageHeader};
use arrow_data::UnsafeFlag;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Read};
use std::sync::Arc;

/// Decode trusted local Comet output without revalidating every Arrow array value or offset.
///
/// Convenience wrapper around a throwaway [`ShuffleBlockDecoder`]; callers that decode more than
/// one block should hold a decoder so the schema message is parsed once rather than per block.
pub fn read_ipc_compressed(bytes: &[u8]) -> Result<RecordBatch> {
    ShuffleBlockDecoder::new().decode(bytes)
}

/// Decode remotely fetched Comet output, including Arrow buffer and offset validation.
///
/// See [`read_ipc_compressed`] for when to hold a [`ShuffleBlockDecoder`] instead.
pub fn read_ipc_compressed_validated(bytes: &[u8]) -> Result<RecordBatch> {
    ShuffleBlockDecoder::new().decode_validated(bytes)
}

/// The largest message metadata length a block may carry. Arrow encodes it as an `i32`, so
/// anything beyond this is corruption rather than a large message.
const MAX_METADATA_LEN: usize = i32::MAX as usize;

/// Decodes Comet shuffle blocks, caching the IPC schema across blocks.
///
/// Every block is a complete Arrow IPC stream: a schema message, any dictionary batches, one
/// record batch, and the end-of-stream marker. `ShuffleBlockWriter` pre-encodes the schema
/// message once and writes it verbatim into every block, so consecutive blocks from the same
/// writer carry byte-identical schema messages. Parsing that message per block (a flatbuffer
/// verification plus one `Arc<Field>` and `String` per column) is a fixed cost that dominates the
/// decode of small blocks, which is exactly what high partition counts produce.
///
/// The decoder keeps the raw bytes of the last schema message it parsed together with the parsed
/// [`SchemaRef`]. On each block it compares the incoming schema message bytes against the cached
/// ones and, on a match, reuses the schema without parsing. A mismatch (a different writer, a
/// different Comet version, or a block of a different shape) parses the new message and replaces
/// the cache, so correctness never depends on the cache: a block is decoded against the schema it
/// actually carries.
///
/// A decoder is meant to be held for the life of a reader (one per `ShuffleScanExec`, one per JNI
/// decoder handle) and is not thread-safe.
#[derive(Debug, Default)]
pub struct ShuffleBlockDecoder {
    /// Raw flatbuffer bytes of the last schema message and the schema parsed from them.
    cached_schema: Option<(Vec<u8>, SchemaRef)>,
    /// Scratch for message metadata so it is not reallocated per message.
    metadata: Vec<u8>,
    /// Number of schema messages actually parsed, i.e. cache misses. One per distinct schema
    /// encoding seen; exposed so tests and metrics can confirm the cache is doing its job.
    schema_parses: usize,
}

impl ShuffleBlockDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode a trusted local block without revalidating array values or offsets.
    pub fn decode(&mut self, bytes: &[u8]) -> Result<RecordBatch> {
        self.decode_impl(bytes, false)
    }

    /// Decode a remotely fetched block, including Arrow buffer and offset validation.
    pub fn decode_validated(&mut self, bytes: &[u8]) -> Result<RecordBatch> {
        self.decode_impl(bytes, true)
    }

    /// How many schema messages this decoder has parsed so far. Every block whose schema message
    /// is byte-identical to the previous block's reuses the cached schema and does not count.
    pub fn schema_parses(&self) -> usize {
        self.schema_parses
    }

    fn decode_impl(&mut self, bytes: &[u8], validate: bool) -> Result<RecordBatch> {
        let codec = bytes.get(..4).ok_or_else(|| {
            DataFusionError::Execution(
                "Failed to decode batch: truncated compression codec".to_owned(),
            )
        })?;
        let mut encoded = &bytes[4..];
        let batch = match codec {
            b"SNAP" => {
                self.read_single_batch(snap::read::FrameDecoder::new(&mut encoded), validate)?
            }
            b"LZ4_" => self.read_single_batch(
                lz4_flex::frame::FrameDecoder::new(RequireLz4EndMark(&mut encoded)),
                validate,
            )?,
            // The slice already implements BufRead. Adding another BufReader would let
            // read-ahead conceal compressed bytes left over after the decoder reaches its end
            // marker.
            b"ZSTD" => {
                self.read_single_batch(zstd::Decoder::with_buffer(&mut encoded)?, validate)?
            }
            b"NONE" => self.read_single_batch(&mut encoded, validate)?,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "Failed to decode batch: invalid compression codec: {other:?}"
                )))
            }
        };
        // LZ4 returns EOF at the end of one compressed frame without consuming the next one.
        // Check the encoded source as well as the decoded IPC tail so an oversized outer frame
        // cannot silently swallow another native frame's bytes.
        if !encoded.is_empty() {
            return Err(DataFusionError::Execution(
                "Failed to decode batch: trailing data after compressed stream".to_owned(),
            ));
        }
        Ok(batch)
    }

    /// Reads one complete IPC stream holding exactly one record batch, mirroring what
    /// `arrow::ipc::reader::StreamReader` does message by message but with the schema message
    /// served from the cache when its bytes match.
    fn read_single_batch<R: Read>(&mut self, mut input: R, validate: bool) -> Result<RecordBatch> {
        let mut skip_validation = UnsafeFlag::new();
        if !validate {
            // SAFETY: local blocks were written by this Comet version's ShuffleBlockWriter from
            // arrays that were valid when encoded, which is the same trust the previous
            // StreamReader-based path placed in them. Remote data keeps full validation.
            unsafe { skip_validation.set(true) };
        }

        // Schema message: served from the cache on a byte match, parsed otherwise.
        let schema = match self.read_metadata(&mut input)? {
            None => {
                return Err(DataFusionError::Execution(
                    "Failed to decode batch: empty IPC stream".to_owned(),
                ))
            }
            Some(()) => self.schema_for_current_metadata()?,
        };

        let mut dictionaries_by_id: HashMap<i64, ArrayRef> = HashMap::new();
        let mut decoded: Option<RecordBatch> = None;
        while self.read_metadata(&mut input)?.is_some() {
            let message = root_as_message(&self.metadata).map_err(|err| {
                DataFusionError::Execution(format!(
                    "Failed to decode batch: unable to get root as message: {err:?}"
                ))
            })?;
            let version = message.version();
            let body = read_body(&mut input, message.bodyLength())?;
            match message.header_type() {
                MessageHeader::DictionaryBatch => {
                    let dictionary = message.header_as_dictionary_batch().ok_or_else(|| {
                        DataFusionError::Execution(
                            "Failed to decode batch: unable to read dictionary batch".to_owned(),
                        )
                    })?;
                    read_dictionary_impl(
                        &body.into(),
                        dictionary,
                        &schema,
                        &mut dictionaries_by_id,
                        &version,
                        false,
                        skip_validation.clone(),
                    )?;
                }
                MessageHeader::RecordBatch => {
                    // Each Comet frame contains one complete IPC stream with exactly one record
                    // batch. Stopping after that batch would skip codec footer/checksum
                    // validation and could silently discard further frames swallowed by a
                    // corrupt outer length prefix, so keep reading until the end-of-stream
                    // marker and reject a second batch.
                    if decoded.is_some() {
                        return Err(DataFusionError::Execution(
                            "Failed to decode batch: multiple record batches in one shuffle frame"
                                .to_owned(),
                        ));
                    }
                    let batch = message.header_as_record_batch().ok_or_else(|| {
                        DataFusionError::Execution(
                            "Failed to decode batch: unable to read record batch".to_owned(),
                        )
                    })?;
                    let body = body.into();
                    decoded = Some(
                        RecordBatchDecoder::try_new(
                            &body,
                            batch,
                            Arc::clone(&schema),
                            &dictionaries_by_id,
                            &version,
                        )?
                        .with_require_alignment(false)
                        .with_skip_validation(skip_validation.clone())
                        .read_record_batch()?,
                    );
                }
                MessageHeader::Schema => {
                    return Err(DataFusionError::Execution(
                        "Failed to decode batch: expected a record batch, but found a schema"
                            .to_owned(),
                    ));
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to decode batch: unsupported message header type in IPC \
                         stream: '{other:?}'"
                    )));
                }
            }
        }

        let batch = decoded.ok_or_else(|| {
            DataFusionError::Execution("Failed to decode batch: empty IPC stream".to_owned())
        })?;
        if input.read(&mut [0])? != 0 {
            return Err(DataFusionError::Execution(
                "Failed to decode batch: trailing data after IPC stream".to_owned(),
            ));
        }
        Ok(batch)
    }

    /// Reads the next message's metadata length prefix and flatbuffer into `self.metadata`.
    /// Returns `None` at the end of the stream, whether marked (a zero length, optionally after
    /// a continuation marker) or a clean EOF before any length bytes.
    fn read_metadata<R: Read>(&mut self, input: &mut R) -> Result<Option<()>> {
        let mut prefix = [0u8; 4];
        match input.read_exact(&mut prefix) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        if prefix == [0xff; 4] {
            input.read_exact(&mut prefix)?;
        }
        let len = i32::from_le_bytes(prefix);
        if len == 0 {
            return Ok(None);
        }
        let len = usize::try_from(len)
            .ok()
            .filter(|len| *len <= MAX_METADATA_LEN)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Failed to decode batch: invalid metadata length: {len}"
                ))
            })?;
        self.metadata.resize(len, 0);
        input.read_exact(&mut self.metadata)?;
        Ok(Some(()))
    }

    /// Resolves the schema for the schema message currently in `self.metadata`, reusing the
    /// cached schema when the bytes match and parsing (and caching) otherwise. A schema message
    /// has no body, which the parse path checks, so a byte-identical message needs nothing read.
    fn schema_for_current_metadata(&mut self) -> Result<SchemaRef> {
        if let Some((cached_bytes, cached_schema)) = &self.cached_schema {
            if *cached_bytes == self.metadata {
                return Ok(Arc::clone(cached_schema));
            }
        }

        let message = root_as_message(&self.metadata).map_err(|err| {
            DataFusionError::Execution(format!(
                "Failed to decode batch: unable to get root as message: {err:?}"
            ))
        })?;
        if message.header_type() != MessageHeader::Schema {
            return Err(DataFusionError::Execution(format!(
                "Failed to decode batch: expected a schema as the first message in the \
                 stream, got: {:?}",
                message.header_type()
            )));
        }
        if message.bodyLength() != 0 {
            return Err(DataFusionError::Execution(
                "Failed to decode batch: schema message with a non-empty body".to_owned(),
            ));
        }
        let schema = message.header_as_schema().ok_or_else(|| {
            DataFusionError::Execution(
                "Failed to decode batch: failed to parse schema from message header".to_owned(),
            )
        })?;
        let schema = Arc::new(fb_to_schema(schema));
        self.schema_parses += 1;
        self.cached_schema = Some((self.metadata.clone(), Arc::clone(&schema)));
        Ok(schema)
    }
}

/// Reads a message body of `len` bytes into a fresh buffer, as `StreamReader` does.
fn read_body<R: Read>(input: &mut R, len: i64) -> Result<MutableBuffer> {
    let len = usize::try_from(len).map_err(|_| {
        DataFusionError::Execution(format!(
            "Failed to decode batch: invalid message body length: {len}"
        ))
    })?;
    let mut body = MutableBuffer::from_len_zeroed(len);
    input.read_exact(&mut body)?;
    Ok(body)
}

// lz4_flex treats physical EOF (including a partial block header) as a clean end of frame.
// Comet always writes an explicit LZ4 EndMark, so a decoder trying to read past the supplied
// bytes has encountered a truncated frame. InvalidData is deliberate: UnexpectedEof is swallowed
// by lz4_flex::frame::FrameDecoder::read_block.
struct RequireLz4EndMark<R>(R);

impl<R: Read> Read for RequireLz4EndMark<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = self.0.read(buffer)?;
        if count == 0 && !buffer.is_empty() {
            Err(Error::new(
                ErrorKind::InvalidData,
                "Failed to decode batch: truncated LZ4 shuffle frame",
            ))
        } else {
            Ok(count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{read_ipc_compressed, read_ipc_compressed_validated, ShuffleBlockDecoder};
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use std::io::Write;
    use std::sync::Arc;

    fn ipc_stream(batch_count: usize) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
        for _ in 0..batch_count {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
        bytes
    }

    fn encode(codec: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut bytes = codec.to_vec();
        match codec {
            b"NONE" => bytes.extend_from_slice(payload),
            b"SNAP" => {
                let mut writer = snap::write::FrameEncoder::new(&mut bytes);
                writer.write_all(payload).unwrap();
                writer.into_inner().unwrap();
            }
            b"LZ4_" => {
                let mut writer = lz4_flex::frame::FrameEncoder::new(&mut bytes);
                writer.write_all(payload).unwrap();
                writer.finish().unwrap();
            }
            b"ZSTD" => {
                let mut writer = zstd::Encoder::new(&mut bytes, 1).unwrap();
                writer.write_all(payload).unwrap();
                writer.finish().unwrap();
            }
            _ => unreachable!(),
        }
        bytes
    }

    /// Blocks that repeat the same schema message must be decoded against the cached schema
    /// (one parse for the whole run), and a block carrying a different schema must be decoded
    /// against its own schema and replace the cache, never against the stale one.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn schema_cache_hits_identical_messages_and_misses_different_ones() {
        let int_stream = ipc_stream(1);
        let utf8_stream = {
            let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(vec!["abc", "de"]))],
            )
            .unwrap();
            let mut bytes = Vec::new();
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            bytes
        };

        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            let int_frame = encode(codec, &int_stream);
            let utf8_frame = encode(codec, &utf8_stream);
            let fresh_int = read_ipc_compressed(&int_frame).unwrap();
            let fresh_utf8 = read_ipc_compressed(&utf8_frame).unwrap();

            for validate in [false, true] {
                let mut decoder = ShuffleBlockDecoder::new();
                let decode = |decoder: &mut ShuffleBlockDecoder, frame: &[u8]| {
                    if validate {
                        decoder.decode_validated(frame).unwrap()
                    } else {
                        decoder.decode(frame).unwrap()
                    }
                };

                for _ in 0..3 {
                    assert_eq!(decode(&mut decoder, &int_frame), fresh_int, "{codec:?}");
                }
                assert_eq!(
                    decoder.schema_parses(),
                    1,
                    "{codec:?}: repeats must hit the cache"
                );

                let utf8 = decode(&mut decoder, &utf8_frame);
                assert_eq!(utf8, fresh_utf8, "{codec:?}");
                assert_eq!(utf8.schema().field(0).data_type(), &DataType::Utf8);
                assert_eq!(
                    decoder.schema_parses(),
                    2,
                    "{codec:?}: new schema must parse"
                );

                assert_eq!(decode(&mut decoder, &int_frame), fresh_int, "{codec:?}");
                assert_eq!(
                    decoder.schema_parses(),
                    3,
                    "{codec:?}: switching back is a new encoding, not a stale hit"
                );
                assert_eq!(decode(&mut decoder, &int_frame), fresh_int, "{codec:?}");
                assert_eq!(decoder.schema_parses(), 3, "{codec:?}");
            }
        }
    }

    /// Dictionary-encoded columns arrive as a dictionary batch before the record batch, the
    /// layout the JVM columnar shuffle produces for strings. Both must decode through the
    /// cached-schema path, with dictionaries scoped to their own block.
    #[test]
    fn dictionary_blocks_decode_with_cached_schema() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let frame = |values: Vec<&str>| {
            let keys = Int32Array::from((0..values.len() as i32).collect::<Vec<_>>());
            let dictionary =
                DictionaryArray::<Int32Type>::try_new(keys, Arc::new(StringArray::from(values)))
                    .unwrap();
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dictionary)]).unwrap();
            let mut bytes = Vec::new();
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            encode(b"NONE", &bytes)
        };
        let first = frame(vec!["a", "b"]);
        let second = frame(vec!["x", "y", "z"]);

        let mut decoder = ShuffleBlockDecoder::new();
        for validate in [false, true] {
            for (block, expected) in [(&first, vec!["a", "b"]), (&second, vec!["x", "y", "z"])] {
                let batch = if validate {
                    decoder.decode_validated(block).unwrap()
                } else {
                    decoder.decode(block).unwrap()
                };
                let values = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
                let values = values.as_any().downcast_ref::<StringArray>().unwrap();
                let got: Vec<&str> = values.iter().map(|v| v.unwrap()).collect();
                assert_eq!(got, expected);
            }
        }
        assert_eq!(decoder.schema_parses(), 1);
    }

    #[test]
    fn malformed_codec_prefix_returns_error() {
        for prefix in [&b""[..], b"N", b"NO", b"NON", b"BAD!"] {
            assert!(read_ipc_compressed(prefix).is_err());
            assert!(read_ipc_compressed_validated(prefix).is_err());
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn empty_or_multiple_batch_stream_returns_error() {
        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            for batch_count in [0, 2] {
                let error = read_ipc_compressed(&encode(codec, &ipc_stream(batch_count)))
                    .unwrap_err()
                    .to_string();
                assert!(
                    error.contains(if batch_count == 0 {
                        "empty IPC stream"
                    } else {
                        "multiple record batches"
                    }),
                    "{codec:?}: {error}"
                );
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn trailing_data_after_ipc_stream_returns_error() {
        let mut payload = ipc_stream(1);
        payload.extend_from_slice(b"another shuffle frame");
        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            let error = read_ipc_compressed(&encode(codec, &payload))
                .unwrap_err()
                .to_string();
            assert!(error.contains("trailing data"), "{codec:?}: {error}");
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn trailing_data_after_compressed_stream_returns_error() {
        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            let mut frame = encode(codec, &ipc_stream(1));
            frame.extend_from_slice(&20_u64.to_le_bytes());
            frame.extend_from_slice(b"another native frame");
            assert!(read_ipc_compressed(&frame).is_err(), "{codec:?}");
        }
    }

    #[test]
    fn truncated_lz4_end_mark_returns_error() {
        let frame = encode(b"LZ4_", &ipc_stream(1));
        for truncated in 1..=4 {
            let error = read_ipc_compressed(&frame[..frame.len() - truncated])
                .unwrap_err()
                .to_string();
            assert!(error.contains("truncated LZ4"), "{truncated}: {error}");
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn invalid_array_offsets_return_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["abc", "def"]))],
        )
        .unwrap();
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let offsets: Vec<u8> = [0_i32, 3, 6]
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect();
        let positions: Vec<usize> = payload
            .windows(offsets.len())
            .enumerate()
            .filter_map(|(position, bytes)| (bytes == offsets).then_some(position))
            .collect();
        assert_eq!(positions.len(), 1);
        // Change [0, 3, 6] to [0, 3, 2]: the second string now has decreasing offsets.
        payload[positions[0] + 8..positions[0] + 12].copy_from_slice(&2_i32.to_le_bytes());
        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            assert!(read_ipc_compressed_validated(&encode(codec, &payload)).is_err());
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn valid_single_batch_frames_decode_with_all_codecs() {
        for codec in [b"NONE", b"SNAP", b"LZ4_", b"ZSTD"] {
            let frame = encode(codec, &ipc_stream(1));
            let batch = read_ipc_compressed(&frame).unwrap();
            let validated = read_ipc_compressed_validated(&frame).unwrap();
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch, validated);
        }
    }
}
