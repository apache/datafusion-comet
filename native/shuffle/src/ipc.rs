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
use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::{RecordBatchDecoder, StreamReader};
use arrow::ipc::{root_as_message, MessageHeader};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Cursor, Error, ErrorKind, Read};
use std::sync::Arc;

/// Decode trusted local Comet output without revalidating every Arrow array value or offset.
pub fn read_ipc_compressed(bytes: &[u8]) -> Result<RecordBatch> {
    read_ipc_compressed_impl(bytes, false)
}

/// Decode remotely fetched Comet output, including Arrow buffer and offset validation.
pub fn read_ipc_compressed_validated(bytes: &[u8]) -> Result<RecordBatch> {
    read_ipc_compressed_impl(bytes, true)
}

/// Arrow IPC continuation marker introducing a message length.
const CONTINUATION_MARKER: [u8; 4] = [0xff, 0xff, 0xff, 0xff];

/// Distinct schemas cached per thread.
///
/// One is enough for a single shuffle, but a reduce task can interleave blocks from more than one
/// shuffle (a join reading both of its sides, say), and a size-one cache would thrash between
/// them. The cache is keyed on the raw schema message rather than a parsed schema, so a hit costs
/// one memcmp.
const SCHEMA_CACHE_CAPACITY: usize = 4;

thread_local! {
    static SCHEMA_CACHE: RefCell<Vec<(Box<[u8]>, SchemaRef)>> =
        const { RefCell::new(Vec::new()) };
    /// Empty dictionary map handed to the fast path, which only runs for blocks that carry no
    /// dictionary messages.
    static NO_DICTIONARIES: HashMap<i64, ArrayRef> = HashMap::new();
}

fn cached_schema(schema_message: &[u8]) -> Option<SchemaRef> {
    SCHEMA_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let hit = cache
            .iter()
            .position(|(message, _)| message.as_ref() == schema_message)?;
        // Keep the most recently used entry first so an alternating pair stays resident.
        if hit != 0 {
            cache.swap(0, hit);
        }
        Some(Arc::clone(&cache[0].1))
    })
}

fn cache_schema(schema_message: &[u8], schema: SchemaRef) {
    SCHEMA_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache
            .iter()
            .any(|(message, _)| message.as_ref() == schema_message)
        {
            return;
        }
        if cache.len() == SCHEMA_CACHE_CAPACITY {
            cache.pop();
        }
        cache.insert(0, (schema_message.into(), schema));
    });
}

/// Empties this thread's schema cache, so the next decode re-parses its schema.
///
/// Exists so benchmarks can measure the cached and uncached decode paths against each other in a
/// single run, where machine drift affects both equally. Not part of the decode contract.
#[doc(hidden)]
pub fn reset_schema_cache() {
    SCHEMA_CACHE.with(|cache| cache.borrow_mut().clear());
}

/// One Arrow IPC message located inside a decoded block.
struct IpcMessage<'a> {
    /// The flatbuffer metadata, without the continuation marker or length prefix.
    metadata: &'a [u8],
    /// Offset of the message body within the block.
    body_start: usize,
    /// Offset just past this message, where the next one begins.
    end: usize,
}

/// Reads the message starting at `offset`, or `None` at a clean end of stream (an explicit
/// end-of-stream marker, or running out of bytes exactly on a message boundary).
///
/// Returns `Ok(None)` only for a well-formed end; anything truncated or inconsistent is an error,
/// so a corrupt block cannot be mistaken for a short one.
fn read_message(block: &[u8], offset: usize) -> Result<Option<IpcMessage<'_>>> {
    fn corrupt(what: &str) -> DataFusionError {
        DataFusionError::Execution(format!("Failed to decode batch: {what}"))
    }

    // Ending exactly on a message boundary is the legacy stream ending, which is valid.
    if offset == block.len() {
        return Ok(None);
    }

    let mut cursor = offset;
    let first = block
        .get(cursor..cursor + 4)
        .ok_or_else(|| corrupt("truncated IPC message length"))?;
    cursor += 4;

    let length_bytes = if first == CONTINUATION_MARKER {
        let bytes = block
            .get(cursor..cursor + 4)
            .ok_or_else(|| corrupt("truncated IPC message length"))?;
        cursor += 4;
        bytes
    } else {
        first
    };

    let metadata_len = i32::from_le_bytes(length_bytes.try_into().expect("four bytes"));
    if metadata_len == 0 {
        // End-of-stream marker.
        return Ok(None);
    }
    let metadata_len =
        usize::try_from(metadata_len).map_err(|_| corrupt("negative IPC metadata length"))?;

    let metadata_end = cursor
        .checked_add(metadata_len)
        .ok_or_else(|| corrupt("IPC metadata length overflows the block"))?;
    let metadata = block
        .get(cursor..metadata_end)
        .ok_or_else(|| corrupt("truncated IPC metadata"))?;

    let message = root_as_message(metadata)
        .map_err(|error| corrupt(&format!("invalid IPC metadata: {error}")))?;
    let body_len =
        usize::try_from(message.bodyLength()).map_err(|_| corrupt("negative IPC body length"))?;

    let body_start = metadata_end;
    let end = body_start
        .checked_add(body_len)
        .ok_or_else(|| corrupt("IPC body length overflows the block"))?;
    if end > block.len() {
        return Err(corrupt("truncated IPC body"));
    }

    Ok(Some(IpcMessage {
        metadata,
        body_start,
        end,
    }))
}

/// Confirms nothing follows the record batch but a well-formed end of stream.
///
/// `read_message` reports both an end-of-stream marker and a clean boundary as "no more
/// messages", which on its own would let trailing bytes after the marker pass unnoticed.
fn expect_end_of_stream(block: &[u8], offset: usize) -> Result<()> {
    let trailing = || {
        DataFusionError::Execution(
            "Failed to decode batch: trailing data after IPC stream".to_owned(),
        )
    };

    if offset == block.len() {
        return Ok(());
    }

    let mut cursor = offset;
    let first = block.get(cursor..cursor + 4).ok_or_else(trailing)?;
    cursor += 4;
    let length_bytes = if first == CONTINUATION_MARKER {
        let bytes = block.get(cursor..cursor + 4).ok_or_else(trailing)?;
        cursor += 4;
        bytes
    } else {
        first
    };

    if i32::from_le_bytes(length_bytes.try_into().expect("four bytes")) != 0 {
        return Err(trailing());
    }
    if cursor != block.len() {
        return Err(trailing());
    }
    Ok(())
}

/// Decodes a block whose schema is already known, avoiding a second parse of the schema
/// flatbuffer.
///
/// Returns `Ok(None)` when the block is not the simple `[schema][record batch][end]` shape the
/// fast path handles - a dictionary message, more than one record batch, or anything unexpected -
/// so the caller can fall back to the general decoder rather than this reimplementing its rules.
fn decode_with_known_schema(
    block: &Buffer,
    schema: SchemaRef,
    batch_message: &IpcMessage<'_>,
    validate: bool,
) -> Result<Option<RecordBatch>> {
    let message = root_as_message(batch_message.metadata).map_err(|error| {
        DataFusionError::Execution(format!(
            "Failed to decode batch: invalid IPC metadata: {error}"
        ))
    })?;
    let Some(record_batch) = message.header_as_record_batch() else {
        return Ok(None);
    };

    let body = block.slice_with_length(
        batch_message.body_start,
        batch_message.end - batch_message.body_start,
    );

    let version = message.version();
    let batch = NO_DICTIONARIES.with(|dictionaries| {
        let decoder =
            RecordBatchDecoder::try_new(&body, record_batch, schema, dictionaries, &version)?;
        let decoder = if validate {
            decoder
        } else {
            // Matches the trusted-local fast path taken by the general decoder below.
            let mut flag = arrow_data::UnsafeFlag::new();
            unsafe { flag.set(true) };
            decoder.with_skip_validation(flag)
        };
        decoder.read_record_batch()
    })?;

    Ok(Some(batch))
}

/// Decodes one decompressed block, reusing a cached schema when the block's schema message has
/// been seen before on this thread.
fn decode_block(block: Buffer, validate: bool) -> Result<RecordBatch> {
    if let Some(batch) = try_decode_with_cached_schema(&block, validate) {
        return Ok(batch);
    }

    // General path: unchanged behaviour, and the only path that parses a schema. Its parsed
    // schema is cached so later blocks carrying the same schema message take the fast path.
    let (batch, schema, schema_message) = read_single_batch_cached(block.as_slice(), validate)?;
    if let Some(schema_message) = schema_message {
        cache_schema(schema_message, schema);
    }
    Ok(batch)
}

/// Decodes a block against an already-parsed schema, or `None` if it cannot.
///
/// This never reports an error of its own. Anything it does not handle - a cache miss, a
/// dictionary message, more than one record batch, trailing bytes, or a block that fails to
/// decode - yields `None` so the general decoder runs instead. Validation behaviour and every
/// error message therefore stay exactly as they were, and the fast path is always safe to skip.
fn try_decode_with_cached_schema(block: &Buffer, validate: bool) -> Option<RecordBatch> {
    let bytes = block.as_slice();

    let schema_message = read_message(bytes, 0).ok()??;
    let is_schema = root_as_message(schema_message.metadata)
        .map(|message| message.header_type() == MessageHeader::Schema)
        .unwrap_or(false);
    if !is_schema {
        return None;
    }

    let schema = cached_schema(schema_message.metadata)?;

    // The record batch must be the message right after the schema, with nothing but an end of
    // stream behind it. A dictionary message lands here instead and takes the general path.
    let batch_message = read_message(bytes, schema_message.end).ok()??;
    expect_end_of_stream(bytes, batch_message.end).ok()?;

    decode_with_known_schema(block, schema, &batch_message, validate).ok()?
}

fn read_ipc_compressed_impl(bytes: &[u8], validate: bool) -> Result<RecordBatch> {
    let codec = bytes.get(..4).ok_or_else(|| {
        DataFusionError::Execution("Failed to decode batch: truncated compression codec".to_owned())
    })?;
    let mut encoded = &bytes[4..];
    // The block is materialized before decoding so its messages can be walked in place. The
    // decoded arrays borrow this buffer, so it is the same allocation the general decoder would
    // have made for the record batch body rather than an extra copy.
    let block = match codec {
        b"SNAP" => decompress(snap::read::FrameDecoder::new(&mut encoded))?,
        b"LZ4_" => decompress(lz4_flex::frame::FrameDecoder::new(RequireLz4EndMark(
            &mut encoded,
        )))?,
        // The slice already implements BufRead. Adding another BufReader would let read-ahead
        // conceal compressed bytes left over after the decoder reaches its end marker.
        b"ZSTD" => decompress(zstd::Decoder::with_buffer(&mut encoded)?)?,
        b"NONE" => {
            let block = Buffer::from(encoded);
            encoded = &[];
            block
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "Failed to decode batch: invalid compression codec: {other:?}"
            )))
        }
    };
    // LZ4 returns EOF at the end of one compressed frame without consuming the next one. Check
    // the encoded source as well as the decoded IPC tail so an oversized outer frame cannot
    // silently swallow another native frame's bytes.
    if !encoded.is_empty() {
        return Err(DataFusionError::Execution(
            "Failed to decode batch: trailing data after compressed stream".to_owned(),
        ));
    }
    decode_block(block, validate)
}

/// Reads a decompressor to the end, yielding the decoded block.
fn decompress<R: Read>(mut reader: R) -> Result<Buffer> {
    let mut decoded = Vec::new();
    reader.read_to_end(&mut decoded)?;
    Ok(Buffer::from_vec(decoded))
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

/// General decoder: the original `StreamReader` path, over the decoded block.
///
/// Also returns the parsed schema and the raw schema message it came from, so the caller can
/// cache them and let later blocks with the same schema skip this parse.
fn read_single_batch_cached(
    block: &[u8],
    validate: bool,
) -> Result<(RecordBatch, SchemaRef, Option<&[u8]>)> {
    let mut input = Cursor::new(block);
    let reader = StreamReader::try_new(&mut input, None)?;
    let mut reader = if validate {
        // Remote data must not escape as unchecked arrays and fail later in a native operator.
        reader
    } else {
        // Preserve the existing local-shuffle fast path for trusted Comet-written arrays.
        unsafe { reader.with_skip_validation(true) }
    };
    let schema = reader.schema();
    let batch = reader.next().transpose()?.ok_or_else(|| {
        DataFusionError::Execution("Failed to decode batch: empty IPC stream".to_owned())
    })?;

    // Each Comet frame contains one complete IPC stream with exactly one record batch.
    // Stopping after that batch would skip codec footer/checksum validation and could silently
    // discard further frames swallowed by a corrupt outer length prefix.
    if reader.next().transpose()?.is_some() {
        return Err(DataFusionError::Execution(
            "Failed to decode batch: multiple record batches in one shuffle frame".to_owned(),
        ));
    }
    if reader.get_mut().read(&mut [0])? != 0 {
        return Err(DataFusionError::Execution(
            "Failed to decode batch: trailing data after IPC stream".to_owned(),
        ));
    }

    // Only cache a leading schema message; anything else is not a key the fast path can match.
    let schema_message = read_message(block, 0)?.and_then(|message| {
        let is_schema = root_as_message(message.metadata)
            .map(|parsed| parsed.header_type() == MessageHeader::Schema)
            .unwrap_or(false);
        is_schema.then_some(message.metadata)
    });

    Ok((batch, schema, schema_message))
}

#[cfg(test)]
mod tests {
    use super::{read_ipc_compressed, read_ipc_compressed_validated};
    use arrow::array::{Array, Int32Array, RecordBatch, StringArray};
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

    /// Encodes one batch the way a Comet shuffle block carries it, without the outer 16-byte
    /// Comet header that `read_ipc_compressed` does not see.
    fn block_for(batch: &RecordBatch, codec: &[u8; 4]) -> Vec<u8> {
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, batch.schema_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        encode(codec, &payload)
    }

    fn mixed_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("a"), Some(""), None])),
                Arc::new(arrow::array::Float64Array::from(vec![1.5, -0.0, 2.25])),
            ],
        )
        .unwrap()
    }

    fn dictionary_batch() -> RecordBatch {
        let values = StringArray::from(vec!["x", "y"]);
        let keys = Int32Array::from(vec![0, 1, 0]);
        let dictionary = arrow::array::DictionaryArray::try_new(
            keys,
            Arc::new(values) as arrow::array::ArrayRef,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            dictionary.data_type().clone(),
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(dictionary)]).unwrap()
    }

    /// The second decode of a block reuses the cached schema. It has to produce exactly what the
    /// first one did, on every codec and on both the trusted and validated entry points.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn cached_schema_decode_matches_the_first_decode() {
        for batch in [mixed_batch(), dictionary_batch()] {
            for codec in [b"NONE", b"LZ4_", b"ZSTD", b"SNAP"] {
                let block = block_for(&batch, codec);

                let cold = read_ipc_compressed(&block).unwrap();
                let warm = read_ipc_compressed(&block).unwrap();
                assert_eq!(cold, batch, "cold decode differs, codec {codec:?}");
                assert_eq!(warm, batch, "warm decode differs, codec {codec:?}");
                assert_eq!(warm.schema(), batch.schema());

                let validated = read_ipc_compressed_validated(&block).unwrap();
                assert_eq!(
                    validated, batch,
                    "validated decode differs, codec {codec:?}"
                );
            }
        }
    }

    /// A dictionary-carrying block never takes the fast path, but must still decode correctly
    /// once its schema is cached by an earlier block.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn dictionary_blocks_keep_decoding_with_a_warm_cache() {
        let batch = dictionary_batch();
        let block = block_for(&batch, b"ZSTD");
        for _ in 0..3 {
            assert_eq!(read_ipc_compressed(&block).unwrap(), batch);
        }
    }

    /// Trailing bytes after the end-of-stream marker must stay an error once the schema is
    /// cached. A fast path that treated "no further message" as "clean end" would accept them.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn trailing_data_still_fails_with_a_warm_cache() {
        let batch = mixed_batch();
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, batch.schema_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        // Warm the cache with the well-formed block first.
        let good = encode(b"NONE", &payload);
        assert_eq!(read_ipc_compressed(&good).unwrap(), batch);

        let mut corrupted = payload.clone();
        corrupted.extend_from_slice(&[0u8; 8]);
        let error = read_ipc_compressed(&encode(b"NONE", &corrupted)).unwrap_err();
        assert!(
            error.to_string().contains("trailing data"),
            "unexpected error: {error}"
        );
    }

    /// A block truncated inside its record batch body must fail whether or not its schema is
    /// already cached. Dropping only the end-of-stream marker is not truncation: a stream ending
    /// on a message boundary is valid, and both paths accept it.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn truncated_block_fails_with_a_warm_cache() {
        let batch = mixed_batch();
        let block = block_for(&batch, b"NONE");

        // Cold, before anything is cached.
        let cut_into_body = &block[..block.len() - 24];
        assert!(read_ipc_compressed(cut_into_body).is_err());

        // Warm the cache, then the same truncation must still fail.
        assert_eq!(read_ipc_compressed(&block).unwrap(), batch);
        assert!(read_ipc_compressed(cut_into_body).is_err());

        // Dropping just the end-of-stream marker stays valid, as it was before.
        assert_eq!(
            read_ipc_compressed(&block[..block.len() - 8]).unwrap(),
            batch
        );
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
