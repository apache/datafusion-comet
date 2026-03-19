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

use crate::errors::{CometError, CometResult};
use arrow::array::{make_array, Array, ArrayRef, MutableArrayData, RecordBatch};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatchOptions;
use bytes::Buf;
use crc32fast::Hasher;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::physical_plan::metrics::Time;
use simd_adler32::Adler32;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum CompressionCodec {
    None,
    Lz4Frame,
    Zstd(i32),
    Snappy,
}

#[derive(Clone)]
pub struct ShuffleBlockWriter {
    codec: CompressionCodec,
    header_bytes: Vec<u8>,
}

/// Recursively writes raw Arrow ArrayData buffers to the given writer.
/// Arrays must be normalized to zero offset before calling this function.
fn write_array_data<W: Write>(data: &arrow::array::ArrayData, writer: &mut W) -> Result<()> {
    debug_assert_eq!(data.offset(), 0, "shuffle arrays must have offset 0");

    // Write null_count
    let null_count = data.null_count() as u32;
    writer.write_all(&null_count.to_le_bytes())?;

    // Write validity bitmap
    if null_count > 0 {
        if let Some(bitmap) = data.nulls() {
            debug_assert_eq!(bitmap.offset(), 0, "null bitmap must have offset 0");
            let bitmap_bytes = bitmap.buffer().as_slice();
            let len = bitmap_bytes.len() as u32;
            writer.write_all(&len.to_le_bytes())?;
            writer.write_all(bitmap_bytes)?;
        } else {
            writer.write_all(&0u32.to_le_bytes())?;
        }
    } else {
        writer.write_all(&0u32.to_le_bytes())?;
    }

    // Write buffers
    let num_buffers = data.buffers().len() as u32;
    writer.write_all(&num_buffers.to_le_bytes())?;
    for buffer in data.buffers() {
        let len: u32 = buffer.len().try_into().map_err(|_| {
            DataFusionError::Execution(format!("Buffer length {} exceeds u32::MAX", buffer.len()))
        })?;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(buffer.as_slice())?;
    }

    // Write children
    let num_children = data.child_data().len() as u32;
    writer.write_all(&num_children.to_le_bytes())?;
    for child in data.child_data() {
        let child_num_rows = child.len() as u32;
        writer.write_all(&child_num_rows.to_le_bytes())?;
        write_array_data(child, writer)?;
    }

    Ok(())
}

/// Ensures an array has zero offset in both its data and null buffer by
/// producing a physical copy when necessary. This is required because our
/// raw buffer format writes buffers verbatim and assumes offset 0.
fn normalize_array(col: &ArrayRef) -> Result<ArrayRef> {
    let needs_copy = col.offset() != 0 || col.nulls().is_some_and(|nulls| nulls.offset() != 0);
    if needs_copy {
        // Use MutableArrayData::extend for a direct memcpy rather than
        // take() which builds an index array and does per-element lookups.
        let data = col.to_data();
        let mut mutable = MutableArrayData::new(vec![&data], false, col.len());
        mutable.extend(0, 0, col.len());
        Ok(make_array(mutable.freeze()))
    } else {
        Ok(Arc::clone(col))
    }
}

// Column encoding tags for the raw shuffle format.
const COL_TAG_PLAIN: u8 = 0;
const COL_TAG_DICTIONARY: u8 = 1;

/// Encode a dictionary key DataType as a single byte.
fn encode_key_type(dt: &DataType) -> Result<u8> {
    match dt {
        DataType::Int8 => Ok(0),
        DataType::Int16 => Ok(1),
        DataType::Int32 => Ok(2),
        DataType::Int64 => Ok(3),
        DataType::UInt8 => Ok(4),
        DataType::UInt16 => Ok(5),
        DataType::UInt32 => Ok(6),
        DataType::UInt64 => Ok(7),
        _ => Err(DataFusionError::Execution(format!(
            "unsupported dictionary key type: {dt:?}"
        ))),
    }
}

/// Decode a dictionary key DataType from the byte written by encode_key_type.
fn decode_key_type(tag: u8) -> Result<DataType> {
    match tag {
        0 => Ok(DataType::Int8),
        1 => Ok(DataType::Int16),
        2 => Ok(DataType::Int32),
        3 => Ok(DataType::Int64),
        4 => Ok(DataType::UInt8),
        5 => Ok(DataType::UInt16),
        6 => Ok(DataType::UInt32),
        7 => Ok(DataType::UInt64),
        _ => Err(DataFusionError::Execution(format!(
            "unknown dictionary key type tag: {tag}"
        ))),
    }
}

/// Writes a RecordBatch in raw buffer format. Dictionary arrays are preserved
/// (not cast to value type) so they compress better. Each column is prefixed
/// with a tag byte indicating plain (0) or dictionary (1) encoding.
fn write_raw_batch<W: Write>(batch: &RecordBatch, writer: &mut W) -> Result<()> {
    let num_rows = batch.num_rows() as u32;
    writer.write_all(&num_rows.to_le_bytes())?;

    for col in batch.columns() {
        let col = normalize_array(col)?;
        match col.data_type() {
            DataType::Dictionary(key_type, _) => {
                writer.write_all(&[COL_TAG_DICTIONARY])?;
                writer.write_all(&[encode_key_type(key_type)?])?;
            }
            _ => {
                writer.write_all(&[COL_TAG_PLAIN])?;
            }
        }
        write_array_data(&col.to_data(), writer)?;
    }

    Ok(())
}

impl ShuffleBlockWriter {
    pub fn try_new(schema: &Schema, codec: CompressionCodec) -> Result<Self> {
        let header_bytes = Vec::with_capacity(20);
        let mut cursor = Cursor::new(header_bytes);

        // leave space for compressed message length
        cursor.seek_relative(8)?;

        // write number of columns because JVM side needs to know how many addresses to allocate
        let field_count = schema.fields().len();
        cursor.write_all(&field_count.to_le_bytes())?;

        // write compression codec to header
        let codec_header = match &codec {
            CompressionCodec::Snappy => b"SNAP",
            CompressionCodec::Lz4Frame => b"LZ4_",
            CompressionCodec::Zstd(_) => b"ZSTD",
            CompressionCodec::None => b"NONE",
        };
        cursor.write_all(codec_header)?;

        let header_bytes = cursor.into_inner();

        Ok(Self {
            codec,
            header_bytes,
        })
    }

    /// Writes given record batch in raw buffer format into given writer.
    /// Returns number of bytes written.
    ///
    /// The batch is first serialized to an intermediate buffer, then compressed
    /// in one shot. This avoids creating a streaming compression encoder per
    /// batch (expensive for Zstd ~128KB state) and gives us the uncompressed
    /// size for a pre-allocation hint on the read side.
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

        // Serialize raw batch to intermediate buffer
        let mut raw_buf = Vec::new();
        write_raw_batch(batch, &mut raw_buf)?;

        // Write uncompressed size hint (u32), then compressed data
        let uncompressed_len = raw_buf.len() as u32;
        output.write_all(&uncompressed_len.to_le_bytes())?;

        match &self.codec {
            CompressionCodec::None => {
                output.write_all(&raw_buf)?;
            }
            CompressionCodec::Lz4Frame => {
                let compressed = lz4_flex::compress(&raw_buf);
                output.write_all(&compressed)?;
            }
            CompressionCodec::Zstd(level) => {
                let compressed = zstd::bulk::compress(&raw_buf, *level)?;
                output.write_all(&compressed)?;
            }
            CompressionCodec::Snappy => {
                let mut wtr = snap::write::FrameEncoder::new(output.by_ref());
                wtr.write_all(&raw_buf)?;
                wtr.into_inner().map_err(|e| {
                    DataFusionError::Execution(format!("snappy compression error: {e}"))
                })?;
            }
        };

        // fill block length
        let end_pos = output.stream_position()?;
        let block_length = end_pos - start_pos - 8;
        let max_size = i32::MAX as u64;
        if block_length > max_size {
            return Err(DataFusionError::Execution(format!(
                "Shuffle block size {block_length} exceeds maximum size of {max_size}. \
                Try reducing batch size or increasing compression level"
            )));
        }

        // fill block length
        output.seek(SeekFrom::Start(start_pos))?;
        output.write_all(&block_length.to_le_bytes())?;
        output.seek(SeekFrom::Start(end_pos))?;

        timer.stop();

        Ok((end_pos - start_pos) as usize)
    }
}

// ---------------------------------------------------------------------------
// Read-side helpers
// ---------------------------------------------------------------------------

fn read_u32(cursor: &mut &[u8]) -> Result<u32> {
    if cursor.len() < 4 {
        return Err(DataFusionError::Execution(
            "unexpected end of shuffle block data".to_string(),
        ));
    }
    let (bytes, rest) = cursor.split_at(4);
    *cursor = rest;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_bytes<'a>(cursor: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if cursor.len() < len {
        return Err(DataFusionError::Execution(
            "unexpected end of shuffle block data".to_string(),
        ));
    }
    let (bytes, rest) = cursor.split_at(len);
    *cursor = rest;
    Ok(bytes)
}

/// Returns child data types for nested Arrow types.
fn get_child_types(data_type: &DataType) -> Vec<DataType> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            vec![field.data_type().clone()]
        }
        DataType::Map(field, _) => {
            // Map's single child is a struct with key/value fields
            vec![field.data_type().clone()]
        }
        DataType::Struct(fields) => fields.iter().map(|f| f.data_type().clone()).collect(),
        DataType::Dictionary(_, value_type) => vec![value_type.as_ref().clone()],
        _ => vec![],
    }
}

/// Reconstructs ArrayData from raw buffer format (reverse of write_array_data).
fn read_array_data(
    cursor: &mut &[u8],
    data_type: &DataType,
    num_rows: usize,
) -> Result<arrow::array::ArrayData> {
    let null_count = read_u32(cursor)? as usize;

    // Read validity bitmap
    let bitmap_len = read_u32(cursor)? as usize;
    let null_buffer = if bitmap_len > 0 {
        let bytes = read_bytes(cursor, bitmap_len)?;
        Some(Buffer::from(bytes))
    } else {
        None
    };

    // Read buffers
    let num_buffers = read_u32(cursor)? as usize;
    let mut buffers = Vec::with_capacity(num_buffers);
    for _ in 0..num_buffers {
        let buf_len = read_u32(cursor)? as usize;
        let bytes = read_bytes(cursor, buf_len)?;
        buffers.push(Buffer::from(bytes));
    }

    // Read children
    let num_children = read_u32(cursor)? as usize;
    let child_types = get_child_types(data_type);
    let mut child_data = Vec::with_capacity(num_children);
    for i in 0..num_children {
        let child_num_rows = read_u32(cursor)? as usize;
        let child_type = child_types.get(i).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "unexpected child index {i} for data type {data_type:?}"
            ))
        })?;
        child_data.push(read_array_data(cursor, child_type, child_num_rows)?);
    }

    // Build ArrayData without validation (data came from our own writer)
    let mut builder = arrow::array::ArrayData::builder(data_type.clone())
        .len(num_rows)
        .null_count(null_count);

    if let Some(nb) = null_buffer {
        builder = builder.null_bit_buffer(Some(nb));
    }

    for buf in buffers {
        builder = builder.add_buffer(buf);
    }

    for child in child_data {
        builder = builder.add_child_data(child);
    }

    // SAFETY: data was written by write_array_data from valid Arrow arrays
    Ok(unsafe { builder.build_unchecked() })
}

/// Read a raw batch from decompressed bytes, given the expected schema.
/// Columns that were written as dictionary-encoded are reconstructed as
/// DictionaryArray. The returned batch schema may differ from the input
/// schema (Dictionary vs plain types) — callers that need plain types
/// should cast dictionary columns afterward.
fn read_raw_batch(bytes: &[u8], schema: &Arc<Schema>) -> Result<RecordBatch> {
    let mut cursor = bytes;

    let num_rows = read_u32(&mut cursor)? as usize;

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let mut fields: Vec<arrow::datatypes::FieldRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        // Read per-column tag
        let tag = read_bytes(&mut cursor, 1)?[0];
        let data_type = match tag {
            COL_TAG_PLAIN => field.data_type().clone(),
            COL_TAG_DICTIONARY => {
                let key_tag = read_bytes(&mut cursor, 1)?[0];
                let key_type = decode_key_type(key_tag)?;
                DataType::Dictionary(Box::new(key_type), Box::new(field.data_type().clone()))
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "unknown column tag: {tag}"
                )));
            }
        };
        let array_data = read_array_data(&mut cursor, &data_type, num_rows)?;
        columns.push(make_array(array_data));
        fields.push(Arc::new(arrow::datatypes::Field::new(
            field.name(),
            data_type,
            field.is_nullable(),
        )));
    }

    let actual_schema = Arc::new(Schema::new(fields));
    let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
    let batch = RecordBatch::try_new_with_options(actual_schema, columns, &options)?;
    Ok(batch)
}

/// Reads and decompresses a shuffle block written in raw buffer format.
/// The `bytes` slice starts at the codec tag (after the 8-byte length and
/// 8-byte field_count header that the JVM reads).
///
/// Format: `[codec_tag: 4 bytes][uncompressed_len: u32][compressed_data...]`
pub fn read_shuffle_block(bytes: &[u8], schema: &Arc<Schema>) -> Result<RecordBatch> {
    let codec_tag = &bytes[0..4];
    // Read uncompressed size hint for pre-allocation
    let uncompressed_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let data = &bytes[8..];

    match codec_tag {
        b"SNAP" => {
            let decoder = snap::read::FrameDecoder::new(data);
            let decompressed = read_all_with_capacity(decoder, uncompressed_len)?;
            read_raw_batch(&decompressed, schema)
        }
        b"LZ4_" => {
            let decompressed = lz4_flex::decompress(data, uncompressed_len)
                .map_err(|e| DataFusionError::Execution(format!("lz4 decompression error: {e}")))?;
            read_raw_batch(&decompressed, schema)
        }
        b"ZSTD" => {
            let decompressed = zstd::bulk::decompress(data, uncompressed_len)?;
            read_raw_batch(&decompressed, schema)
        }
        b"NONE" => read_raw_batch(data, schema),
        other => Err(DataFusionError::Execution(format!(
            "Failed to decode batch: invalid compression codec: {other:?}"
        ))),
    }
}

/// Read all bytes from a reader into a pre-allocated Vec.
fn read_all_with_capacity<R: Read>(mut reader: R, capacity: usize) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(capacity);
    reader.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Checksum algorithms for writing shuffle bytes.
#[derive(Clone)]
pub(crate) enum Checksum {
    /// CRC32 checksum algorithm.
    CRC32(Hasher),
    /// Adler32 checksum algorithm.
    Adler32(Adler32),
}

impl Checksum {
    pub(crate) fn try_new(algo: i32, initial_opt: Option<u32>) -> CometResult<Self> {
        match algo {
            0 => {
                let hasher = if let Some(initial) = initial_opt {
                    Hasher::new_with_initial(initial)
                } else {
                    Hasher::new()
                };
                Ok(Checksum::CRC32(hasher))
            }
            1 => {
                let hasher = if let Some(initial) = initial_opt {
                    // Note that Adler32 initial state is not zero.
                    // i.e., `Adler32::from_checksum(0)` is not the same as `Adler32::new()`.
                    Adler32::from_checksum(initial)
                } else {
                    Adler32::new()
                };
                Ok(Checksum::Adler32(hasher))
            }
            _ => Err(CometError::Internal(
                "Unsupported checksum algorithm".to_string(),
            )),
        }
    }

    pub(crate) fn update(&mut self, cursor: &mut Cursor<&mut Vec<u8>>) -> CometResult<()> {
        match self {
            Checksum::CRC32(hasher) => {
                std::io::Seek::seek(cursor, SeekFrom::Start(0))?;
                hasher.update(cursor.chunk());
                Ok(())
            }
            Checksum::Adler32(hasher) => {
                std::io::Seek::seek(cursor, SeekFrom::Start(0))?;
                hasher.write(cursor.chunk());
                Ok(())
            }
        }
    }

    pub(crate) fn finalize(self) -> u32 {
        match self {
            Checksum::CRC32(hasher) => hasher.finalize(),
            Checksum::Adler32(hasher) => hasher.finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::metrics::Time;
    use std::io::Cursor;
    use std::sync::Arc;

    fn make_test_batch() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn roundtrip(codec: CompressionCodec) {
        let (schema, batch) = make_test_batch();
        let writer = ShuffleBlockWriter::try_new(&schema, codec).unwrap();
        let mut buf = Cursor::new(Vec::new());
        let ipc_time = Time::new();
        writer.write_batch(&batch, &mut buf, &ipc_time).unwrap();

        let bytes = buf.into_inner();
        // Skip 16-byte header: 8 compressed_length + 8 field_count
        let body = &bytes[16..];

        let decoded = read_shuffle_block(body, &schema).unwrap();
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 2);

        // Verify Int32 column (nullable)
        let col0 = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col0.value(0), 1);
        assert!(col0.is_null(1));
        assert_eq!(col0.value(2), 3);

        // Verify Float64 column (non-nullable)
        let col1 = decoded
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(col1.value(0), 1.0);
        assert_eq!(col1.value(1), 2.0);
        assert_eq!(col1.value(2), 3.0);
    }

    #[test]
    fn test_raw_roundtrip_primitives_none() {
        roundtrip(CompressionCodec::None);
    }

    #[test]
    fn test_raw_roundtrip_primitives_lz4() {
        roundtrip(CompressionCodec::Lz4Frame);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_raw_roundtrip_primitives_zstd() {
        roundtrip(CompressionCodec::Zstd(1));
    }

    #[test]
    fn test_raw_roundtrip_primitives_snappy() {
        roundtrip(CompressionCodec::Snappy);
    }

    /// Generic roundtrip helper: writes a batch with ShuffleBlockWriter,
    /// reads it back with read_shuffle_block, and asserts equality for all
    /// four compression codecs.
    fn roundtrip_test(schema: Arc<Schema>, batch: &RecordBatch) {
        let codecs = vec![
            CompressionCodec::None,
            CompressionCodec::Lz4Frame,
            CompressionCodec::Zstd(1),
            CompressionCodec::Snappy,
        ];
        for codec in codecs {
            let writer = ShuffleBlockWriter::try_new(&schema, codec.clone()).unwrap();
            let mut buf = Cursor::new(Vec::new());
            let ipc_time = Time::new();
            writer.write_batch(batch, &mut buf, &ipc_time).unwrap();

            let bytes = buf.into_inner();
            let body = &bytes[16..];

            let decoded = read_shuffle_block(body, &schema).unwrap();
            assert_eq!(decoded.num_rows(), batch.num_rows());
            assert_eq!(decoded.num_columns(), batch.num_columns());
            for i in 0..batch.num_columns() {
                assert_eq!(
                    batch.column(i).as_ref(),
                    decoded.column(i).as_ref(),
                    "column {i} mismatch with codec {:?}",
                    codec
                );
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_roundtrip_string_and_binary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("hello"),
                    None,
                    Some("world"),
                    Some(""),
                ])),
                Arc::new(BinaryArray::from(vec![
                    Some(b"abc" as &[u8]),
                    Some(b"\x00\x01\x02"),
                    None,
                    Some(b""),
                ])),
            ],
        )
        .unwrap();
        roundtrip_test(schema, &batch);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_roundtrip_boolean_and_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bool", DataType::Boolean, true),
            Field::new("n", DataType::Null, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                ])),
                Arc::new(NullArray::new(4)),
            ],
        )
        .unwrap();
        roundtrip_test(schema, &batch);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_roundtrip_decimal_date_timestamp() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dec", DataType::Decimal128(18, 3), true),
            Field::new("date", DataType::Date32, true),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    Decimal128Array::from(vec![Some(12345_i128), None, Some(-99999)])
                        .with_precision_and_scale(18, 3)
                        .unwrap(),
                ),
                Arc::new(Date32Array::from(vec![Some(18000), None, Some(19000)])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_000_000),
                    None,
                    Some(2_000_000),
                ])),
            ],
        )
        .unwrap();
        roundtrip_test(schema, &batch);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_roundtrip_nested_types() {
        // List<Int32> with nulls at the list level
        let list_field = Field::new_list("l", Field::new("item", DataType::Int32, true), true);

        // Struct<Int32, Utf8> with nulls
        let struct_field = Field::new(
            "st",
            DataType::Struct(
                vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new("y", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );

        // Map<Utf8, Int32>
        let map_field = Field::new(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );

        let schema = Arc::new(Schema::new(vec![list_field, struct_field, map_field]));

        // Build List<Int32>
        let list_arr = {
            let mut builder = ListBuilder::new(Int32Builder::new());
            builder.values().append_value(1);
            builder.values().append_value(2);
            builder.append(true);
            builder.append(false); // null list
            builder.values().append_value(3);
            builder.append(true);
            builder.finish()
        };

        // Build Struct<Int32, Utf8>
        let struct_arr = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("y", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
            ),
        ]);
        // Apply null at row 1
        let struct_arr = StructArray::try_new(
            struct_arr.fields().clone(),
            struct_arr.columns().to_vec(),
            Some(arrow::buffer::NullBuffer::from(vec![true, false, true])),
        )
        .unwrap();

        // Build Map<Utf8, Int32>
        let map_arr = {
            let key_builder = StringBuilder::new();
            let value_builder = Int32Builder::new();
            let mut builder = MapBuilder::new(None, key_builder, value_builder);
            builder.keys().append_value("k1");
            builder.values().append_value(100);
            builder.append(true).unwrap();
            builder.append(false).unwrap(); // null map entry
            builder.keys().append_value("k2");
            builder.values().append_value(200);
            builder.append(true).unwrap();
            builder.finish()
        };

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(list_arr), Arc::new(struct_arr), Arc::new(map_arr)],
        )
        .unwrap();

        roundtrip_test(schema, &batch);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_roundtrip_dictionary_preserved() {
        // Dictionary<Int32, Utf8> should be preserved through write/read
        let dict_schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));

        let keys = Int32Array::from(vec![Some(0), Some(1), None, Some(0)]);
        let values = StringArray::from(vec!["foo", "bar"]);
        let dict_arr = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();

        let batch =
            RecordBatch::try_new(Arc::clone(&dict_schema), vec![Arc::new(dict_arr)]).unwrap();

        // The read schema uses the value type (Utf8) since that's what Spark knows about.
        // The reader reconstructs a Dictionary type from the per-column tag.
        let read_schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Utf8, true)]));

        let codecs = vec![
            CompressionCodec::None,
            CompressionCodec::Lz4Frame,
            CompressionCodec::Zstd(1),
            CompressionCodec::Snappy,
        ];
        for codec in codecs {
            let writer = ShuffleBlockWriter::try_new(&dict_schema, codec.clone()).unwrap();
            let mut buf = Cursor::new(Vec::new());
            let ipc_time = Time::new();
            writer.write_batch(&batch, &mut buf, &ipc_time).unwrap();

            let bytes = buf.into_inner();
            let body = &bytes[16..];

            let decoded = read_shuffle_block(body, &read_schema).unwrap();
            assert_eq!(decoded.num_rows(), 4);

            // Result should be a DictionaryArray (preserved, not cast)
            let col = decoded
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("expected DictionaryArray to be preserved");
            // Verify values by casting to string for comparison
            let cast_col = arrow::compute::cast(col, &DataType::Utf8).expect("cast dict to utf8");
            let str_col = cast_col.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(str_col.value(0), "foo");
            assert_eq!(str_col.value(1), "bar");
            assert!(str_col.is_null(2));
            assert_eq!(str_col.value(3), "foo");
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_roundtrip_sliced_batch() {
        // Test that arrays with non-zero offsets (from slicing) roundtrip correctly.
        // This is important because the shuffle writer uses debug_assert for offset==0,
        // but in release builds sliced arrays could silently produce wrong results.
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let full_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    None,
                    Some(3),
                    Some(4),
                    None,
                    Some(6),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("bb"),
                    None,
                    Some("dddd"),
                    Some("eeeee"),
                    None,
                ])),
            ],
        )
        .unwrap();

        // Slice the batch to get arrays with non-zero offset
        let sliced = full_batch.slice(2, 3); // rows: [Some(3), Some(4), None] and [None, Some("dddd"), Some("eeeee")]
        assert_eq!(sliced.num_rows(), 3);
        roundtrip_test(schema, &sliced);
    }

    #[test]
    fn test_empty_batch_returns_zero() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 0);

        let writer = ShuffleBlockWriter::try_new(&schema, CompressionCodec::None).unwrap();
        let mut buf = Cursor::new(Vec::new());
        let ipc_time = Time::new();
        let bytes_written = writer.write_batch(&batch, &mut buf, &ipc_time).unwrap();
        assert_eq!(bytes_written, 0);
    }
}
