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
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::{read_dictionary_impl, RecordBatchDecoder};
use arrow::ipc::{root_as_message, Message, MessageHeader};
use arrow_data::UnsafeFlag;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Read};
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
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// Distinct schemas cached per thread. More than one because a reduce task can interleave blocks
/// from several shuffles, and a single entry would thrash.
const SCHEMA_CACHE_CAPACITY: usize = 4;

/// Maximum estimated serialized-plus-parsed size of all cached schemas on a thread, excluding
/// allocator overhead. A shared 16 MiB budget lets all four slots hold wide schemas from
/// interleaved shuffles: 8,000 short-named Int32 fields need about 1.4 MiB with Arrow 59 on a
/// 64-bit target. This leaves headroom for wider schemas and names/metadata while bounding
/// estimated cache retention per decoding thread.
const SCHEMA_CACHE_RETAIN_LIMIT: usize = 16 << 20;

/// Metadata scratch larger than this is released after the block rather than kept for the thread.
/// This buffer-capacity limit is independent of the serialized-plus-parsed schema cache budget.
const SCRATCH_RETAIN_LIMIT: usize = 1 << 20;

struct CachedSchema {
    message: Box<[u8]>,
    schema: SchemaRef,
    /// Computed once on admission; cache hits and eviction do not walk the schema again.
    retained_size: usize,
}

/// Per-thread memoization of immutable schema metadata, not operator state. Moving execution to
/// another thread only loses cache hits. Dictionaries and batch data remain local to each call.
///
/// Every block is a complete IPC stream that opens with a schema message. `ShuffleBlockWriter`
/// encodes that message once and writes it verbatim into every block, so consecutive blocks carry
/// byte-identical schema messages. The cache is keyed on those bytes: a hit is one memcmp, and
/// the schema message is neither verified nor parsed.
#[derive(Default)]
struct DecoderState {
    /// Parsed schemas keyed on the raw schema message, most recently used first.
    schemas: Vec<CachedSchema>,
    /// Message metadata read from a decompressor lands here, so it is not reallocated per block.
    scratch: Vec<u8>,
    #[cfg(test)]
    stats: SchemaCacheStats,
}

thread_local! {
    static STATE: RefCell<DecoderState> = RefCell::new(DecoderState::default());
}

/// Empties this thread's schema cache, so the next decode re-parses its schema. For benchmarks
/// and tests comparing the cold and warm paths; not part of the decode contract.
#[doc(hidden)]
pub fn reset_schema_cache() {
    STATE.with_borrow_mut(|state| {
        state.schemas.clear();
        #[cfg(test)]
        {
            state.stats = SchemaCacheStats::default();
        }
    });
}

/// Schema cache hits and misses on this thread since the last [`reset_schema_cache`].
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SchemaCacheStats {
    hits: usize,
    misses: usize,
}

#[cfg(test)]
fn schema_cache_stats() -> SchemaCacheStats {
    STATE.with_borrow(|state| state.stats)
}

#[cfg(test)]
fn scratch_capacity() -> usize {
    STATE.with_borrow(|state| state.scratch.capacity())
}

fn cached_schema(schemas: &mut [CachedSchema], schema_message: &[u8]) -> Option<SchemaRef> {
    let hit = schemas
        .iter()
        .position(|entry| entry.message.as_ref() == schema_message)?;
    // Promote the hit without changing the relative recency of the other entries.
    if hit != 0 {
        schemas[..=hit].rotate_right(1);
    }
    Some(Arc::clone(&schemas[0].schema))
}

fn estimated_retained_size(schema_message: &[u8], schema: &Schema) -> usize {
    let mut retained_size = schema_message
        .len()
        .saturating_add(std::mem::size_of_val(schema))
        .saturating_add(schema.fields().size())
        .saturating_add(
            schema
                .metadata()
                .capacity()
                .saturating_mul(std::mem::size_of::<(String, String)>()),
        );
    for (key, value) in schema.metadata() {
        retained_size = retained_size
            .saturating_add(key.capacity())
            .saturating_add(value.capacity());
    }
    retained_size
}

fn cache_schema(schemas: &mut Vec<CachedSchema>, schema_message: &[u8], schema: SchemaRef) {
    // Reject entries larger than the whole budget before evicting or copying the key. Check the
    // serialized size first to avoid walking a schema that cannot fit even without its parsed copy.
    // Admission only affects reuse: oversized valid schemas still decode successfully.
    if schema_message.len() > SCHEMA_CACHE_RETAIN_LIMIT {
        return;
    }
    let retained_size = estimated_retained_size(schema_message, &schema);
    if retained_size > SCHEMA_CACHE_RETAIN_LIMIT {
        return;
    }
    // At most four stored estimates are summed on a miss. Keeping no separate total also means
    // clearing the cache cannot leave stale byte accounting behind.
    let mut cached_size: usize = schemas.iter().map(|entry| entry.retained_size).sum();
    while schemas.len() == SCHEMA_CACHE_CAPACITY
        || cached_size > SCHEMA_CACHE_RETAIN_LIMIT - retained_size
    {
        cached_size -= schemas.pop().unwrap().retained_size;
    }
    schemas.insert(
        0,
        CachedSchema {
            message: schema_message.into(),
            schema,
            retained_size,
        },
    );
}

fn decode_error(what: &str) -> DataFusionError {
    DataFusionError::Execution(format!("Failed to decode batch: {what}"))
}

fn parse_message(metadata: &[u8]) -> Result<Message<'_>> {
    root_as_message(metadata)
        .map_err(|error| decode_error(&format!("unable to get root as message: {error:?}")))
}

fn body_length(message: &Message<'_>) -> Result<usize> {
    usize::try_from(message.bodyLength()).map_err(|_| {
        decode_error(&format!(
            "invalid message body length: {}",
            message.bodyLength()
        ))
    })
}

fn read_ipc_compressed_impl(bytes: &[u8], validate: bool) -> Result<RecordBatch> {
    let codec = bytes
        .get(..4)
        .ok_or_else(|| decode_error("truncated compression codec"))?;
    let mut encoded = &bytes[4..];
    let batch = match codec {
        b"SNAP" => decode(
            Streamed(snap::read::FrameDecoder::new(&mut encoded)),
            validate,
        )?,
        b"LZ4_" => decode(
            Streamed(lz4_flex::frame::FrameDecoder::new(RequireLz4EndMark(
                &mut encoded,
            ))),
            validate,
        )?,
        // The slice already implements BufRead. Adding another BufReader would let read-ahead
        // conceal compressed bytes left over after the decoder reaches its end marker.
        b"ZSTD" => decode(
            Streamed(zstd::Decoder::with_buffer(&mut encoded)?),
            validate,
        )?,
        // Uncompressed messages are located in place, so only bodies are copied.
        b"NONE" => {
            let batch = decode(Sliced::new(encoded), validate)?;
            encoded = &[];
            batch
        }
        other => {
            return Err(decode_error(&format!(
                "invalid compression codec: {other:?}"
            )))
        }
    };
    // LZ4 returns EOF at the end of one compressed frame without consuming the next one. Check
    // the encoded source as well as the decoded IPC tail so an oversized outer frame cannot
    // silently swallow another native frame's bytes.
    if !encoded.is_empty() {
        return Err(decode_error("trailing data after compressed stream"));
    }
    Ok(batch)
}

fn decode<'b, S: BlockSource<'b>>(source: S, validate: bool) -> Result<RecordBatch> {
    STATE.with_borrow_mut(|state| {
        let batch = read_single_batch(state, source, validate);
        // a corrupt length can grow the scratch arbitrarily; do not pin that for the thread's life
        if state.scratch.capacity() > SCRATCH_RETAIN_LIMIT {
            state.scratch = Vec::new();
        }
        batch
    })
}

/// Reads one complete IPC stream holding exactly one record batch. Mirrors what
/// `arrow::ipc::reader::StreamReader` does message by message, except that the schema message is
/// served from the cache when its bytes match one already parsed.
fn read_single_batch<'b, S: BlockSource<'b>>(
    state: &mut DecoderState,
    mut source: S,
    validate: bool,
) -> Result<RecordBatch> {
    let DecoderState {
        schemas, scratch, ..
    } = state;

    let mut skip_validation = UnsafeFlag::new();
    if !validate {
        // SAFETY: local blocks were written by this Comet version's ShuffleBlockWriter from arrays
        // that were valid when encoded, the same trust the StreamReader path placed in them.
        // Remote blocks keep full validation.
        unsafe { skip_validation.set(true) };
    }

    let Some(metadata) = source.next_metadata(scratch)? else {
        return Err(decode_error("empty IPC stream"));
    };
    let schema = match cached_schema(schemas, metadata) {
        Some(schema) => {
            #[cfg(test)]
            {
                state.stats.hits += 1;
            }
            schema
        }
        None => {
            #[cfg(test)]
            {
                state.stats.misses += 1;
            }
            let message = parse_message(metadata)?;
            if message.header_type() != MessageHeader::Schema {
                return Err(decode_error(&format!(
                    "expected a schema as the first message in the stream, got: {:?}",
                    message.header_type()
                )));
            }
            let schema = message
                .header_as_schema()
                .ok_or_else(|| decode_error("failed to parse schema from message header"))?;
            let schema = Arc::new(fb_to_schema(schema));
            // A schema message has no body. Only bodiless ones are cached, so a hit never has a
            // body to skip; anything else is read past as StreamReader does, without caching.
            match body_length(&message)? {
                0 => cache_schema(schemas, metadata, Arc::clone(&schema)),
                len => {
                    source.body(len)?;
                }
            }
            schema
        }
    };

    // dictionaries belong to the block that carries them, never to the cached schema
    let mut dictionaries: HashMap<i64, ArrayRef> = HashMap::new();
    let mut batch = None;
    while let Some(metadata) = source.next_metadata(scratch)? {
        let message = parse_message(metadata)?;
        let version = message.version();
        let body_len = body_length(&message)?;
        match message.header_type() {
            MessageHeader::DictionaryBatch => {
                let dictionary = message
                    .header_as_dictionary_batch()
                    .ok_or_else(|| decode_error("unable to read dictionary batch"))?;
                let body = source.body(body_len)?;
                read_dictionary_impl(
                    &body,
                    dictionary,
                    &schema,
                    &mut dictionaries,
                    &version,
                    false,
                    skip_validation.clone(),
                )?;
            }
            MessageHeader::RecordBatch => {
                // Each Comet frame contains one complete IPC stream with exactly one record
                // batch. Stopping after that batch would skip codec footer/checksum validation
                // and could silently discard further frames swallowed by a corrupt outer length
                // prefix, so keep reading to the end-of-stream marker and reject a second batch.
                if batch.is_some() {
                    return Err(decode_error("multiple record batches in one shuffle frame"));
                }
                let record_batch = message
                    .header_as_record_batch()
                    .ok_or_else(|| decode_error("unable to read record batch"))?;
                let body = source.body(body_len)?;
                batch = Some(
                    RecordBatchDecoder::try_new(
                        &body,
                        record_batch,
                        Arc::clone(&schema),
                        &dictionaries,
                        &version,
                    )?
                    .with_require_alignment(false)
                    .with_skip_validation(skip_validation.clone())
                    .read_record_batch()?,
                );
            }
            MessageHeader::Schema => {
                return Err(decode_error("expected a record batch, but found a schema"));
            }
            other => {
                return Err(decode_error(&format!(
                    "unsupported message header type in IPC stream: '{other:?}'"
                )));
            }
        }
    }

    let batch = batch.ok_or_else(|| decode_error("empty IPC stream"))?;
    source.expect_exhausted()?;
    Ok(batch)
}

/// Where a block's IPC messages come from. Metadata is borrowed one message at a time; bodies
/// become exactly sized buffers that the decoded arrays keep.
///
/// `'b` is the lifetime of an in-memory block, so [`Sliced`] can hand out metadata without
/// copying it; a streamed source uses `'static` and copies metadata into the caller's scratch.
trait BlockSource<'b> {
    /// The next message's metadata, or `None` at the end of the stream: an explicit
    /// end-of-stream marker, or a clean EOF on a message boundary, which is the legacy ending.
    fn next_metadata<'a>(&mut self, scratch: &'a mut Vec<u8>) -> Result<Option<&'a [u8]>>
    where
        'b: 'a;

    /// The next message's body, `len` bytes long.
    fn body(&mut self, len: usize) -> Result<Buffer>;

    /// Errors unless every byte of the block has been consumed.
    fn expect_exhausted(&mut self) -> Result<()>;
}

/// Decodes the metadata length a message starts with, from its first four bytes and a reader for
/// four more should those be the continuation marker. `None` is the end-of-stream marker.
fn metadata_length(
    first: [u8; 4],
    next: impl FnOnce() -> Result<[u8; 4]>,
) -> Result<Option<usize>> {
    let length_bytes = if first == CONTINUATION_MARKER {
        next()?
    } else {
        first
    };
    match i32::from_le_bytes(length_bytes) {
        0 => Ok(None),
        len => usize::try_from(len)
            .map(Some)
            .map_err(|_| decode_error(&format!("invalid metadata length: {len}"))),
    }
}

/// A block read through a decompressor.
struct Streamed<R>(R);

impl<R: Read> Streamed<R> {
    fn read_exact(&mut self, buffer: &mut [u8], what: &str) -> Result<()> {
        self.0.read_exact(buffer).map_err(|error| {
            if error.kind() == ErrorKind::UnexpectedEof {
                decode_error(what)
            } else {
                error.into()
            }
        })
    }
}

impl<R: Read> BlockSource<'static> for Streamed<R> {
    fn next_metadata<'a>(&mut self, scratch: &'a mut Vec<u8>) -> Result<Option<&'a [u8]>>
    where
        'static: 'a,
    {
        let mut prefix = [0u8; 4];
        // EOF on a message boundary ends the stream; a partial length prefix does not
        if self.0.read(&mut prefix[..1])? == 0 {
            return Ok(None);
        }
        self.read_exact(&mut prefix[1..], "truncated IPC message length")?;
        let Some(len) = metadata_length(prefix, || {
            let mut bytes = [0u8; 4];
            self.read_exact(&mut bytes, "truncated IPC message length")?;
            Ok(bytes)
        })?
        else {
            return Ok(None);
        };
        scratch.resize(len, 0);
        self.read_exact(scratch, "truncated IPC metadata")?;
        Ok(Some(scratch.as_slice()))
    }

    fn body(&mut self, len: usize) -> Result<Buffer> {
        let mut body = MutableBuffer::from_len_zeroed(len);
        self.read_exact(&mut body, "truncated IPC body")?;
        Ok(body.into())
    }

    fn expect_exhausted(&mut self) -> Result<()> {
        if self.0.read(&mut [0])? != 0 {
            return Err(decode_error("trailing data after IPC stream"));
        }
        Ok(())
    }
}

/// An uncompressed block, walked in place.
struct Sliced<'b> {
    block: &'b [u8],
    offset: usize,
}

impl<'b> Sliced<'b> {
    fn new(block: &'b [u8]) -> Self {
        Self { block, offset: 0 }
    }

    fn take(&mut self, len: usize, what: &str) -> Result<&'b [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.block.len())
            .ok_or_else(|| decode_error(what))?;
        let bytes = &self.block[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

impl<'b> BlockSource<'b> for Sliced<'b> {
    fn next_metadata<'a>(&mut self, _scratch: &'a mut Vec<u8>) -> Result<Option<&'a [u8]>>
    where
        'b: 'a,
    {
        if self.offset == self.block.len() {
            return Ok(None);
        }
        let first = self.take(4, "truncated IPC message length")?;
        let Some(len) = metadata_length(first.try_into().expect("four bytes"), || {
            let bytes = self.take(4, "truncated IPC message length")?;
            Ok(bytes.try_into().expect("four bytes"))
        })?
        else {
            return Ok(None);
        };
        Ok(Some(self.take(len, "truncated IPC metadata")?))
    }

    fn body(&mut self, len: usize) -> Result<Buffer> {
        // an exactly sized copy, with no zero fill before it
        Ok(Buffer::from(self.take(len, "truncated IPC body")?))
    }

    fn expect_exhausted(&mut self) -> Result<()> {
        if self.offset != self.block.len() {
            return Err(decode_error("trailing data after IPC stream"));
        }
        Ok(())
    }
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
    use super::{
        read_ipc_compressed, read_ipc_compressed_validated, reset_schema_cache, schema_cache_stats,
        scratch_capacity, RequireLz4EndMark, SchemaCacheStats, SCHEMA_CACHE_CAPACITY,
        SCHEMA_CACHE_RETAIN_LIMIT, SCRATCH_RETAIN_LIMIT,
    };
    use crate::writers::rss::tests::allocations;
    use arrow::array::{Array, DictionaryArray, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;
    use std::collections::HashMap;
    use std::io::{Cursor, Read, Write};
    use std::sync::Arc;

    const CODECS: [&[u8; 4]; 4] = [b"NONE", b"LZ4_", b"ZSTD", b"SNAP"];

    fn stats(hits: usize, misses: usize) -> SchemaCacheStats {
        SchemaCacheStats { hits, misses }
    }

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

    /// One batch as a complete IPC stream.
    fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, batch.schema_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        payload
    }

    /// One encoded block, without the 16-byte Comet header.
    fn block_for(batch: &RecordBatch, codec: &[u8; 4]) -> Vec<u8> {
        encode(codec, &ipc_bytes(batch))
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

    /// One dictionary-encoded string column; every call shares the same schema, so blocks built
    /// from different values share a schema message but carry their own dictionary batch.
    fn dictionary_batch(values: &[&str]) -> RecordBatch {
        let dictionary: DictionaryArray<Int32Type> = values.iter().copied().collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            dictionary.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(dictionary)]).unwrap()
    }

    fn strings(batch: &RecordBatch) -> Vec<String> {
        let values = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        values.iter().map(|v| v.unwrap().to_owned()).collect()
    }

    fn n_column_batch(num_columns: usize) -> RecordBatch {
        let fields = (0..num_columns)
            .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
            .collect::<Vec<_>>();
        let columns = (0..num_columns)
            .map(|_| Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef)
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    /// After a cold decode, the same schema is served from the cache by both entry points, and
    /// the warm decodes equal the cold one on every codec.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn warm_decodes_hit_the_cache_and_match_the_cold_one() {
        for batch in [mixed_batch(), dictionary_batch(&["x", "y", "x"])] {
            for codec in CODECS {
                let block = block_for(&batch, codec);
                reset_schema_cache();

                let cold = read_ipc_compressed(&block).unwrap();
                assert_eq!(schema_cache_stats(), stats(0, 1), "codec {codec:?}");
                let warm = read_ipc_compressed(&block).unwrap();
                assert_eq!(schema_cache_stats(), stats(1, 1), "codec {codec:?}");
                let validated = read_ipc_compressed_validated(&block).unwrap();
                assert_eq!(schema_cache_stats(), stats(2, 1), "codec {codec:?}");

                for decoded in [&cold, &warm, &validated] {
                    assert_eq!(decoded, &batch, "codec {codec:?}");
                    assert_eq!(decoded.schema(), batch.schema(), "codec {codec:?}");
                }
            }
        }
    }

    /// Blocks that share a schema each carry their own dictionary batch. With the schema served
    /// from the cache, a record batch must still be decoded against the dictionary in its own
    /// block, never against a previous block's.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn dictionaries_are_scoped_to_their_block_under_a_cached_schema() {
        let first = dictionary_batch(&["a", "b", "a"]);
        let second = dictionary_batch(&["x", "y", "z"]);
        assert_eq!(first.schema(), second.schema());

        for codec in CODECS {
            for validate in [false, true] {
                let decode = |block: &[u8]| {
                    if validate {
                        read_ipc_compressed_validated(block).unwrap()
                    } else {
                        read_ipc_compressed(block).unwrap()
                    }
                };
                reset_schema_cache();
                assert_eq!(strings(&decode(&block_for(&first, codec))), ["a", "b", "a"]);
                assert_eq!(
                    strings(&decode(&block_for(&second, codec))),
                    ["x", "y", "z"]
                );
                assert_eq!(strings(&decode(&block_for(&first, codec))), ["a", "b", "a"]);
                assert_eq!(
                    schema_cache_stats(),
                    stats(2, 1),
                    "codec {codec:?}, validate {validate}"
                );
            }
        }
    }

    /// Each distinct schema misses once. The cache keeps several, so blocks from two shuffles
    /// can alternate without evicting each other, and only the least recently used one goes
    /// when the capacity is exceeded.
    #[test]
    fn distinct_schemas_miss_once_and_recent_ones_stay_cached() {
        let blocks: Vec<Vec<u8>> = (1..=SCHEMA_CACHE_CAPACITY + 1)
            .map(|num_columns| block_for(&n_column_batch(num_columns), b"NONE"))
            .collect();
        let decode = |block: &[u8]| read_ipc_compressed(block).unwrap();

        reset_schema_cache();
        decode(&blocks[0]);
        decode(&blocks[1]);
        decode(&blocks[0]);
        decode(&blocks[1]);
        assert_eq!(schema_cache_stats(), stats(2, 2));

        // one more schema than the capacity evicts the least recently used one
        for block in &blocks {
            decode(block);
        }
        assert_eq!(schema_cache_stats(), stats(4, 5));
        decode(&blocks[0]);
        assert_eq!(schema_cache_stats(), stats(4, 6), "evicted");
        decode(&blocks[SCHEMA_CACHE_CAPACITY]);
        assert_eq!(schema_cache_stats(), stats(5, 6), "most recent stays");
    }

    #[test]
    fn promoting_a_schema_preserves_eviction_order() {
        let blocks: Vec<_> = (1..=5)
            .map(|columns| block_for(&n_column_batch(columns), b"NONE"))
            .collect();
        reset_schema_cache();
        // A B C D A E D: promoting A must keep D newer than B and C, so E evicts B.
        for index in [0, 1, 2, 3, 0, 4, 3] {
            assert_eq!(
                read_ipc_compressed(&blocks[index]).unwrap(),
                n_column_batch(index + 1)
            );
        }
        assert_eq!(schema_cache_stats(), stats(2, 5));
        read_ipc_compressed(&blocks[1]).unwrap();
        assert_eq!(schema_cache_stats(), stats(2, 6));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn oversized_schemas_decode_without_retention_or_eviction() {
        let normal_blocks: Vec<_> = (1..=SCHEMA_CACHE_CAPACITY)
            .map(|columns| block_for(&n_column_batch(columns), b"NONE"))
            .collect();
        let schemas = [
            (
                "oversized serialized schema",
                false,
                Schema::new(vec![Field::new(
                    "x".repeat(SCHEMA_CACHE_RETAIN_LIMIT + 1),
                    DataType::Int32,
                    false,
                )]),
            ),
            // These wire messages fit the budget, but their parsed copies push retention over it.
            (
                "oversized parsed field name",
                true,
                Schema::new(vec![Field::new(
                    "x".repeat(SCHEMA_CACHE_RETAIN_LIMIT / 2),
                    DataType::Int32,
                    false,
                )]),
            ),
            (
                "oversized parsed schema metadata",
                true,
                Schema::new(vec![Field::new("c", DataType::Int32, false)]).with_metadata(
                    HashMap::from([("key".into(), "x".repeat(SCHEMA_CACHE_RETAIN_LIMIT / 2))]),
                ),
            ),
        ];
        for (case, wire_fits, schema) in schemas {
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap_or_else(|error| panic!("{case}: {error}"));
            let ipc = ipc_bytes(&batch);
            assert_eq!(
                ipc.len() < SCHEMA_CACHE_RETAIN_LIMIT,
                wire_fits,
                "{case}: IPC length {}",
                ipc.len()
            );
            for codec in CODECS {
                let codec_name = std::str::from_utf8(codec).expect("codecs are ASCII");
                let context = format!("{case}, codec {codec_name}");
                reset_schema_cache();
                for (index, block) in normal_blocks.iter().enumerate() {
                    read_ipc_compressed(block).unwrap_or_else(|error| {
                        panic!("{context}: cache prefill schema {index}: {error}")
                    });
                }
                let block = encode(codec, &ipc);
                for validate in [false, true] {
                    let context = format!("{context}, validate {validate}");
                    let decoded = if validate {
                        read_ipc_compressed_validated(&block)
                    } else {
                        read_ipc_compressed(&block)
                    }
                    .unwrap_or_else(|error| panic!("{context}: {error}"));
                    assert_eq!(decoded, batch, "{context}");
                    let schema_ref = Arc::downgrade(&decoded.schema());
                    drop(decoded);
                    assert!(schema_ref.upgrade().is_none(), "{context}: schema retained");
                }
                assert_eq!(
                    schema_cache_stats(),
                    stats(0, SCHEMA_CACHE_CAPACITY + 2),
                    "{context}: oversized schemas must miss"
                );
                for (index, block) in normal_blocks.iter().enumerate() {
                    read_ipc_compressed(block).unwrap_or_else(|error| {
                        panic!("{context}: cached schema {index}: {error}")
                    });
                }
                assert_eq!(
                    schema_cache_stats(),
                    stats(SCHEMA_CACHE_CAPACITY, SCHEMA_CACHE_CAPACITY + 2),
                    "{context}: normal schemas must stay cached"
                );
                assert!(
                    scratch_capacity() <= SCRATCH_RETAIN_LIMIT,
                    "{context}: scratch retained {} bytes",
                    scratch_capacity()
                );
            }
        }
    }

    /// Realistic wide schemas still benefit from repeated decode, and reset releases the
    /// parsed schema once no decoded batch owns it.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn wide_schemas_hit_the_cache_and_reset_releases_them() {
        let batch = n_column_batch(8_000);
        let ipc = ipc_bytes(&batch);
        for codec in CODECS {
            let block = encode(codec, &ipc);
            for validate in [false, true] {
                let decode = |block: &[u8]| {
                    if validate {
                        read_ipc_compressed_validated(block)
                    } else {
                        read_ipc_compressed(block)
                    }
                };
                reset_schema_cache();
                let cold = decode(&block).unwrap_or_else(|error| {
                    panic!("cold codec {codec:?}, validate {validate}: {error}")
                });
                let schema = Arc::downgrade(cold.schema_ref());
                let warm = decode(&block).unwrap_or_else(|error| {
                    panic!("warm codec {codec:?}, validate {validate}: {error}")
                });
                assert_eq!(cold, batch, "codec {codec:?}, validate {validate}");
                assert_eq!(warm, batch, "codec {codec:?}, validate {validate}");
                assert_eq!(
                    schema_cache_stats(),
                    stats(1, 1),
                    "codec {codec:?}, validate {validate}"
                );
                drop(cold);
                drop(warm);
                assert!(
                    schema.upgrade().is_some(),
                    "codec {codec:?}, validate {validate}: schema remains cached"
                );
                reset_schema_cache();
                assert!(
                    schema.upgrade().is_none(),
                    "codec {codec:?}, validate {validate}: reset releases cached schemas"
                );
                assert_eq!(schema_cache_stats(), stats(0, 0));
            }
        }
    }

    /// Interleaved shuffles must retain all four wide schemas after the first decode round.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn interleaved_wide_schemas_hit_the_cache_after_the_first_round() {
        let batch = n_column_batch(8_000);
        let batches: Vec<_> = (0..4)
            .map(|schema_id| {
                let schema = batch
                    .schema_ref()
                    .as_ref()
                    .clone()
                    .with_metadata(HashMap::from([("shuffle".into(), schema_id.to_string())]));
                RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec()).unwrap()
            })
            .collect();
        for codec in CODECS {
            let codec_name = std::str::from_utf8(codec).expect("codecs are ASCII");
            let blocks: Vec<_> = batches
                .iter()
                .map(|batch| block_for(batch, codec))
                .collect();
            for validate in [false, true] {
                reset_schema_cache();
                for round in 0..10 {
                    let context = format!(
                        "round {}, codec {codec_name}, validate {validate}",
                        round + 1
                    );
                    for (schema_id, (block, batch)) in blocks.iter().zip(&batches).enumerate() {
                        let decoded = if validate {
                            read_ipc_compressed_validated(block)
                        } else {
                            read_ipc_compressed(block)
                        }
                        .unwrap_or_else(|error| panic!("{context}, schema {schema_id}: {error}"));
                        assert_eq!(&decoded, batch, "{context}, schema {schema_id}");
                    }
                    assert_eq!(schema_cache_stats(), stats(round * 4, 4), "{context}");
                }
            }
        }
    }

    #[test]
    fn retained_size_counts_nested_and_schema_metadata_capacity() {
        let metadata = || {
            let mut value = String::with_capacity(SCHEMA_CACHE_RETAIN_LIMIT);
            value.push('x');
            HashMap::from([("key".into(), value)])
        };
        let nested = Schema::new(vec![Field::new(
            "outer",
            DataType::Struct(
                vec![Field::new("inner", DataType::Int32, false).with_metadata(metadata())].into(),
            ),
            false,
        )]);
        for (case, schema) in [
            ("nested field metadata", nested),
            ("schema metadata", Schema::empty().with_metadata(metadata())),
        ] {
            // The strings contain one byte but retain an allocation as large as the budget.
            // This also verifies that field sizing recurses through a struct's children.
            assert!(
                super::estimated_retained_size(&[], &schema) > SCHEMA_CACHE_RETAIN_LIMIT,
                "{case} capacity must count toward retention"
            );
        }
    }

    /// A byte-budget eviction can remove multiple entries even before the entry-count cap
    /// is reached. A promoted entry must outlive both less-recent entries.
    #[test]
    fn byte_budget_evicts_multiple_least_recent_schemas() {
        // This helper exercises admission directly; the messages need only be distinct
        // cache keys because IPC validation happens before cache_schema is called.
        fn insert(
            schemas: &mut Vec<super::CachedSchema>,
            key: u8,
            retained_size: usize,
        ) -> (Vec<u8>, std::sync::Weak<Schema>) {
            let schema = Arc::new(Schema::empty());
            let parsed_size = super::estimated_retained_size(&[], schema.as_ref());
            let message = vec![key; retained_size - parsed_size];
            assert_eq!(
                super::estimated_retained_size(&message, schema.as_ref()),
                retained_size
            );
            let weak = Arc::downgrade(&schema);
            super::cache_schema(schemas, &message, schema);
            (message, weak)
        }

        let budget = super::SCHEMA_CACHE_RETAIN_LIMIT;
        let mut schemas = Vec::new();
        let (a, a_schema) = insert(&mut schemas, 1, budget / 4);
        let (b, b_schema) = insert(&mut schemas, 2, budget / 4);
        let (c, c_schema) = insert(&mut schemas, 3, budget / 4);
        assert_eq!(schemas.len(), 3);
        assert!(super::cached_schema(&mut schemas, &a).is_some());

        let (d, d_schema) = insert(&mut schemas, 4, budget * 5 / 8);
        assert_eq!(schemas.len(), 2);
        assert_eq!(schemas[0].message.as_ref(), d);
        assert_eq!(schemas[1].message.as_ref(), a);
        assert!(
            schemas
                .iter()
                .map(|entry| entry.retained_size)
                .sum::<usize>()
                <= budget
        );
        assert!(super::cached_schema(&mut schemas, &b).is_none());
        assert!(super::cached_schema(&mut schemas, &c).is_none());
        assert!(b_schema.upgrade().is_none(), "oldest schema was released");
        assert!(
            c_schema.upgrade().is_none(),
            "next-oldest schema was released"
        );
        assert!(
            a_schema.upgrade().is_some(),
            "promoted schema remains cached"
        );
        assert!(d_schema.upgrade().is_some(), "new schema remains cached");
    }

    #[test]
    fn exact_budget_schema_is_cached_and_oversized_schema_does_not_evict_it() {
        let budget = super::SCHEMA_CACHE_RETAIN_LIMIT;
        let mut schemas = Vec::new();
        let schema = Arc::new(Schema::empty());
        let parsed_size = super::estimated_retained_size(&[], schema.as_ref());
        let message = vec![1; budget - parsed_size];
        let cached = Arc::downgrade(&schema);
        super::cache_schema(&mut schemas, &message, schema);
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].retained_size, budget);
        assert!(super::cached_schema(&mut schemas, &message).is_some());

        let schema = Arc::new(Schema::empty());
        let oversized = Arc::downgrade(&schema);
        let too_large = vec![2; budget + 1 - parsed_size];
        super::cache_schema(&mut schemas, &too_large, schema);
        assert_eq!(schemas.len(), 1);
        assert!(super::cached_schema(&mut schemas, &message).is_some());
        assert!(super::cached_schema(&mut schemas, &too_large).is_none());
        assert!(cached.upgrade().is_some(), "existing schema remains cached");
        assert!(
            oversized.upgrade().is_none(),
            "oversized schema was released"
        );
    }

    /// An `Int32` and a `Utf8` column, `num_rows` long.
    fn wide_batch(num_rows: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new((0..num_rows).collect::<Int32Array>()),
                Arc::new(
                    (0..num_rows)
                        .map(|i| Some(format!("value_{i}")))
                        .collect::<StringArray>(),
                ),
            ],
        )
        .unwrap()
    }

    /// The reader this change replaced: a `StreamReader` per block over the decompressor,
    /// exactly one batch, then the end of the stream.
    fn stream_reader_decode(block: &[u8]) -> RecordBatch {
        fn read<R: Read>(input: R) -> RecordBatch {
            let mut reader = unsafe {
                StreamReader::try_new(input, None)
                    .unwrap()
                    .with_skip_validation(true)
            };
            let batch = reader.next().unwrap().unwrap();
            assert!(reader.next().is_none());
            batch
        }
        let mut encoded = &block[4..];
        match &block[..4] {
            b"NONE" => read(&mut encoded),
            b"LZ4_" => read(lz4_flex::frame::FrameDecoder::new(RequireLz4EndMark(
                &mut encoded,
            ))),
            b"ZSTD" => read(zstd::Decoder::with_buffer(&mut encoded).unwrap()),
            b"SNAP" => read(snap::read::FrameDecoder::new(&mut encoded)),
            _ => unreachable!(),
        }
    }

    /// With the schema cached, a decode allocates no more than the `StreamReader` path did:
    /// no more allocations, no more bytes, and no higher peak, on every codec, for a tiny block
    /// and a typical one.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn warm_decode_allocates_no_more_than_stream_reader() {
        /// (allocations, bytes requested, peak live bytes) of one decode
        fn probe(
            decode: impl FnOnce() -> RecordBatch,
            expected: &RecordBatch,
        ) -> (usize, usize, usize) {
            let ((batch, (allocations, bytes)), peak) = allocations::measure(|| {
                let batch = decode();
                (batch, allocations::totals())
            });
            assert_eq!(&batch, expected);
            (allocations, bytes, peak)
        }

        for (shape, batch) in [("3 rows", mixed_batch()), ("8192 rows", wide_batch(8192))] {
            for codec in CODECS {
                let block = block_for(&batch, codec);
                reset_schema_cache();
                assert_eq!(read_ipc_compressed(&block).unwrap(), batch);

                let old = probe(|| stream_reader_decode(&block), &batch);
                let new = probe(|| read_ipc_compressed(&block).unwrap(), &batch);
                assert_eq!(schema_cache_stats(), stats(1, 1));

                let codec = std::str::from_utf8(codec).unwrap();
                println!(
                    "{shape} {codec}: stream reader (allocations, bytes, peak) {old:?}, \
                     cached {new:?}"
                );
                assert!(
                    new.0 <= old.0 && new.1 <= old.1 && new.2 <= old.2,
                    "{shape} {codec}: {old:?} -> {new:?}"
                );
            }
        }
    }

    /// Bodies read from a decompressor are allocated at exactly their length, as `StreamReader`
    /// allocates them, so the arrays carry no growth slack and report the same memory size.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn decoded_arrays_report_the_same_memory_size_as_stream_reader() {
        let batch = wide_batch(100_000);
        let ipc = ipc_bytes(&batch);
        let via_stream_reader = StreamReader::try_new(Cursor::new(&ipc), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        for codec in CODECS {
            reset_schema_cache();
            // cold, then warm
            for _ in 0..2 {
                let decoded = read_ipc_compressed(&encode(codec, &ipc)).unwrap();
                assert_eq!(decoded, batch);
                assert_eq!(
                    decoded.get_array_memory_size(),
                    via_stream_reader.get_array_memory_size(),
                    "codec {codec:?}"
                );
            }
        }
    }

    /// Trailing bytes after the end-of-stream marker must stay an error with a warm cache.
    #[test]
    fn trailing_data_still_fails_with_a_warm_cache() {
        let batch = mixed_batch();
        let payload = ipc_bytes(&batch);

        reset_schema_cache();
        assert_eq!(
            read_ipc_compressed(&encode(b"NONE", &payload)).unwrap(),
            batch
        );

        let mut corrupted = payload.clone();
        corrupted.extend_from_slice(&[0u8; 8]);
        let error = read_ipc_compressed(&encode(b"NONE", &corrupted)).unwrap_err();
        assert!(
            error.to_string().contains("trailing data"),
            "unexpected error: {error}"
        );
        assert_eq!(schema_cache_stats(), stats(1, 1), "failed on the warm path");
    }

    /// A block truncated inside its body must fail cold and warm. Dropping only the
    /// end-of-stream marker is not truncation: a stream ending on a message boundary is valid.
    #[test]
    fn truncated_block_fails_with_a_warm_cache() {
        let batch = mixed_batch();
        let block = block_for(&batch, b"NONE");
        reset_schema_cache();

        // cold: the schema parses and is cached before the truncation is reached
        let cut_into_body = &block[..block.len() - 24];
        assert!(read_ipc_compressed(cut_into_body).is_err());
        assert_eq!(schema_cache_stats(), stats(0, 1));

        // warm, and the same truncation must still fail
        assert_eq!(read_ipc_compressed(&block).unwrap(), batch);
        assert!(read_ipc_compressed(cut_into_body).is_err());
        assert_eq!(schema_cache_stats(), stats(2, 1));

        // dropping just the end-of-stream marker stays valid
        assert_eq!(
            read_ipc_compressed(&block[..block.len() - 8]).unwrap(),
            batch
        );
    }

    /// A partial message length after the record batch is an error on every codec, whether it
    /// follows the end-of-stream marker or stands in for it.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn partial_length_prefix_is_an_error() {
        let payload = ipc_stream(1);
        for codec in CODECS {
            let mut after_marker = payload.clone();
            after_marker.extend_from_slice(&[0, 0]);
            let error = read_ipc_compressed(&encode(codec, &after_marker))
                .unwrap_err()
                .to_string();
            assert!(error.contains("trailing data"), "{codec:?}: {error}");

            let mut instead_of_marker = payload[..payload.len() - 8].to_vec();
            instead_of_marker.extend_from_slice(&[0, 0]);
            let error = read_ipc_compressed(&encode(codec, &instead_of_marker))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("truncated IPC message length"),
                "{codec:?}: {error}"
            );
        }
    }

    /// A corrupt metadata length makes the streamed reader grow its scratch before the read
    /// fails. That growth must not stay pinned in the thread-local state afterwards.
    #[test]
    fn oversized_metadata_length_is_an_error_and_releases_the_scratch() {
        let mut payload = ipc_stream(1);
        // the record batch message follows the schema message: continuation marker, length, body
        let schema_len = i32::from_le_bytes(payload[4..8].try_into().unwrap()) as usize;
        let batch_message = 8 + schema_len;
        assert_eq!(payload[batch_message..batch_message + 4], [0xff; 4]);
        let forged = (2 * SCRATCH_RETAIN_LIMIT) as i32;
        payload[batch_message + 4..batch_message + 8].copy_from_slice(&forged.to_le_bytes());

        let error = read_ipc_compressed(&encode(b"LZ4_", &payload))
            .unwrap_err()
            .to_string();
        assert!(error.contains("truncated IPC metadata"), "{error}");
        assert!(scratch_capacity() <= SCRATCH_RETAIN_LIMIT);

        // the in-place reader rejects the same length without allocating anything
        let error = read_ipc_compressed(&encode(b"NONE", &payload))
            .unwrap_err()
            .to_string();
        assert!(error.contains("truncated IPC metadata"), "{error}");
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
        for codec in CODECS {
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
        for codec in CODECS {
            let error = read_ipc_compressed(&encode(codec, &payload))
                .unwrap_err()
                .to_string();
            assert!(error.contains("trailing data"), "{codec:?}: {error}");
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn trailing_data_after_compressed_stream_returns_error() {
        for codec in CODECS {
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

    /// Validation must reject a corrupt array whether the schema is parsed for this block or
    /// served from the cache by an earlier valid block of the same schema.
    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn invalid_array_offsets_fail_validation_cold_and_warm() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["abc", "def"]))],
        )
        .unwrap();
        let mut payload = ipc_bytes(&batch);

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
        let valid = ipc_bytes(&batch);
        for codec in CODECS {
            reset_schema_cache();
            assert!(read_ipc_compressed_validated(&encode(codec, &payload)).is_err());
            assert_eq!(
                read_ipc_compressed_validated(&encode(codec, &valid)).unwrap(),
                batch
            );
            assert!(
                read_ipc_compressed_validated(&encode(codec, &payload)).is_err(),
                "{codec:?}: warm"
            );
            assert_eq!(schema_cache_stats(), stats(2, 1), "{codec:?}");
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call Zstd's C FFI.
    fn valid_single_batch_frames_decode_with_all_codecs() {
        for codec in CODECS {
            let frame = encode(codec, &ipc_stream(1));
            let batch = read_ipc_compressed(&frame).unwrap();
            let validated = read_ipc_compressed_validated(&frame).unwrap();
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch, validated);
        }
    }
}
