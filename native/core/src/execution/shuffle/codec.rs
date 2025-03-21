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
use crate::parquet::data_type::AsBytes;
use arrow::array::cast::AsArray;
use arrow::array::types::Int32Type;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch,
    RecordBatchOptions, StringArray, TimestampMicrosecondArray,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use bytes::Buf;
use crc32fast::Hasher;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::physical_plan::metrics::Time;
use simd_adler32::Adler32;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

pub fn fast_codec_supports_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Utf8
        | DataType::Binary => true,
        DataType::Decimal128(_, s) if *s >= 0 => true,
        DataType::Dictionary(k, v) if **k == DataType::Int32 => fast_codec_supports_type(v),
        _ => false,
    }
}

enum DataTypeId {
    Boolean = 0,
    Int8,
    Int16,
    Int32,
    Int64,
    Date32,
    Timestamp,
    TimestampNtz,
    Decimal128,
    Float32,
    Float64,
    Utf8,
    Binary,
    Dictionary,
}

pub struct BatchWriter<W: Write> {
    inner: W,
}

impl<W: Write> BatchWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    /// Encode the schema (just column names because data types can vary per batch)
    pub fn write_partial_schema(&mut self, schema: &Schema) -> Result<(), DataFusionError> {
        let schema_len = schema.fields().len();
        let mut null_bytes = Vec::with_capacity(schema_len);
        self.inner.write_all(&schema_len.to_le_bytes())?;
        for field in schema.fields() {
            // field name
            let field_name = field.name();
            self.inner.write_all(&field_name.len().to_le_bytes())?;
            self.inner.write_all(field_name.as_str().as_bytes())?;
            // nullable
            null_bytes.push(field.is_nullable() as u8);
        }
        self.inner.write_all(null_bytes.as_bytes())?;
        Ok(())
    }

    fn write_data_type(&mut self, data_type: &DataType) -> Result<(), DataFusionError> {
        match data_type {
            DataType::Boolean => {
                self.inner.write_all(&[DataTypeId::Boolean as u8])?;
            }
            DataType::Int8 => {
                self.inner.write_all(&[DataTypeId::Int8 as u8])?;
            }
            DataType::Int16 => {
                self.inner.write_all(&[DataTypeId::Int16 as u8])?;
            }
            DataType::Int32 => {
                self.inner.write_all(&[DataTypeId::Int32 as u8])?;
            }
            DataType::Int64 => {
                self.inner.write_all(&[DataTypeId::Int64 as u8])?;
            }
            DataType::Float32 => {
                self.inner.write_all(&[DataTypeId::Float32 as u8])?;
            }
            DataType::Float64 => {
                self.inner.write_all(&[DataTypeId::Float64 as u8])?;
            }
            DataType::Date32 => {
                self.inner.write_all(&[DataTypeId::Date32 as u8])?;
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                self.inner.write_all(&[DataTypeId::TimestampNtz as u8])?;
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                self.inner.write_all(&[DataTypeId::Timestamp as u8])?;
                let tz_bytes = tz.as_bytes();
                self.inner.write_all(&tz_bytes.len().to_le_bytes())?;
                self.inner.write_all(tz_bytes)?;
            }
            DataType::Utf8 => {
                self.inner.write_all(&[DataTypeId::Utf8 as u8])?;
            }
            DataType::Binary => {
                self.inner.write_all(&[DataTypeId::Binary as u8])?;
            }
            DataType::Decimal128(p, s) if *s >= 0 => {
                self.inner
                    .write_all(&[DataTypeId::Decimal128 as u8, *p, *s as u8])?;
            }
            DataType::Dictionary(k, v) => {
                self.inner.write_all(&[DataTypeId::Dictionary as u8])?;
                self.write_data_type(k)?;
                self.write_data_type(v)?;
            }
            other => {
                return Err(DataFusionError::Internal(format!(
                    "unsupported type in fast writer {other}"
                )))
            }
        }
        Ok(())
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(bytes)
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        self.write_all(&batch.num_rows().to_le_bytes())?;
        for i in 0..batch.num_columns() {
            self.write_array(batch.column(i))?;
        }
        Ok(())
    }

    fn write_array(&mut self, col: &dyn Array) -> Result<(), DataFusionError> {
        // data type
        self.write_data_type(col.data_type())?;
        // array contents
        match col.data_type() {
            DataType::Boolean => {
                let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                // boolean array is the only type we write the array length because it cannot
                // be determined from the data buffer size (length is in bits rather than bytes)
                self.write_all(&arr.len().to_le_bytes())?;
                // write data buffer
                self.write_boolean_buffer(arr.values())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Int8 => {
                let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
                // write data buffer
                self.write_buffer(arr.values().inner())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Int16 => {
                let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                // write data buffer
                self.write_buffer(arr.values().inner())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Int32 => {
                let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                // write data buffer
                self.write_buffer(arr.values().inner())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                // write data buffer
                self.write_buffer(arr.values().inner())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Float32 => {
                let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                // write data buffer
                self.write_buffer(arr.values().inner())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                // write data buffer
                self.write_buffer(arr.values().inner())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Date32 => {
                let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                // write data buffer
                self.write_buffer(arr.values().inner())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                // write data buffer
                self.write_buffer(arr.values().inner())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Decimal128(_, _) => {
                let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                // write data buffer
                self.write_buffer(arr.values().inner())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                // write data buffer
                self.write_buffer(arr.values())?;
                // write offset buffer
                let offsets = arr.offsets();
                let scalar_buffer = offsets.inner();
                self.write_buffer(scalar_buffer.inner())?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Binary => {
                let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                // write data buffer
                self.write_buffer(arr.values())?;
                // write offset buffer
                let offsets = arr.offsets();
                let scalar_buffer = offsets.inner();
                let buffer = scalar_buffer.inner();
                self.write_buffer(buffer)?;
                // write null buffer
                self.write_null_buffer(arr.nulls())?;
            }
            DataType::Dictionary(k, _) if **k == DataType::Int32 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .unwrap();
                self.write_array(arr.keys())?;
                self.write_array(arr.values())?;
            }
            other => {
                return Err(DataFusionError::Internal(format!(
                    "unsupported type {other}"
                )))
            }
        }
        Ok(())
    }

    fn write_null_buffer(
        &mut self,
        null_buffer: Option<&NullBuffer>,
    ) -> Result<(), DataFusionError> {
        if let Some(nulls) = null_buffer {
            let buffer = nulls.inner();
            // write null buffer length in bits
            self.write_all(&buffer.len().to_le_bytes())?;
            // write null buffer
            self.write_boolean_buffer(buffer)?;
        } else {
            self.inner.write_all(&0_usize.to_le_bytes())?;
        }
        Ok(())
    }

    fn write_buffer(&mut self, buffer: &Buffer) -> std::io::Result<()> {
        // write buffer length
        self.inner.write_all(&buffer.len().to_le_bytes())?;
        // write buffer data
        self.inner.write_all(buffer.as_slice())
    }

    fn write_boolean_buffer(&mut self, buffer: &BooleanBuffer) -> std::io::Result<()> {
        let inner_buffer = buffer.inner();
        if buffer.offset() == 0 && buffer.len() == inner_buffer.len() {
            // Not a sliced buffer, write the inner buffer directly
            self.write_buffer(inner_buffer)?;
        } else {
            // Sliced buffer, create and write the sliced buffer
            let buffer = buffer.sliced();
            self.write_buffer(&buffer)?;
        }
        Ok(())
    }

    pub fn inner(self) -> W {
        self.inner
    }
}

pub struct BatchReader<'a> {
    input: &'a [u8],
    offset: usize,
    /// buffer for reading usize
    length: [u8; 8],
}

impl<'a> BatchReader<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            offset: 0,
            length: [0; 8],
        }
    }

    pub fn read_batch(&mut self) -> Result<RecordBatch, DataFusionError> {
        let mut length = [0; 8];
        length.copy_from_slice(&self.input[0..8]);
        self.offset += 8;
        let schema_len = usize::from_le_bytes(length);

        let mut field_names: Vec<String> = Vec::with_capacity(schema_len);
        let mut nullable: Vec<bool> = Vec::with_capacity(schema_len);
        for _ in 0..schema_len {
            field_names.push(self.read_string());
        }
        for _ in 0..schema_len {
            nullable.push(self.read_bool());
        }

        length.copy_from_slice(&self.input[self.offset..self.offset + 8]);
        self.offset += 8;
        let num_rows = usize::from_le_bytes(length);

        let mut fields: Vec<Arc<Field>> = Vec::with_capacity(schema_len);
        let mut arrays = Vec::with_capacity(schema_len);
        for (name, nullable) in field_names.into_iter().zip(&nullable) {
            let array = self.read_array()?;
            let field = Arc::new(Field::new(name, array.data_type().clone(), *nullable));
            arrays.push(array);
            fields.push(field);
        }
        let schema = Arc::new(Schema::new(fields));
        Ok(RecordBatch::try_new_with_options(
            schema,
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
        .unwrap())
    }

    fn read_array(&mut self) -> Result<ArrayRef, DataFusionError> {
        // read data type
        let data_type = self.read_data_type()?;
        Ok(match data_type {
            DataType::Boolean => {
                // read array length (number of bits)
                let mut length = [0; 8];
                length.copy_from_slice(&self.input[self.offset..self.offset + 8]);
                self.offset += 8;
                let array_len = usize::from_le_bytes(length);
                let buffer = self.read_buffer();
                let data_buffer = BooleanBuffer::new(buffer, 0, array_len);
                let null_buffer = self.read_null_buffer();
                Arc::new(BooleanArray::new(data_buffer, null_buffer))
            }
            DataType::Int8 => {
                let buffer = self.read_buffer();
                let data_buffer = ScalarBuffer::<i8>::from(buffer);
                let null_buffer = self.read_null_buffer();
                Arc::new(Int8Array::try_new(data_buffer, null_buffer)?)
            }
            DataType::Int16 => {
                let buffer = self.read_buffer();
                let data_buffer = ScalarBuffer::<i16>::from(buffer);
                let null_buffer = self.read_null_buffer();
                Arc::new(Int16Array::try_new(data_buffer, null_buffer)?)
            }
            DataType::Int32 => {
                let buffer = self.read_buffer();
                let data_buffer = ScalarBuffer::<i32>::from(buffer);
                let null_buffer = self.read_null_buffer();
                Arc::new(Int32Array::try_new(data_buffer, null_buffer)?)
            }
            DataType::Int64 => {
                let buffer = self.read_buffer();
                let data_buffer = ScalarBuffer::<i64>::from(buffer);
                let null_buffer = self.read_null_buffer();
                Arc::new(Int64Array::try_new(data_buffer, null_buffer)?)
            }
            DataType::Float32 => {
                let buffer = self.read_buffer();
                let data_buffer = ScalarBuffer::<f32>::from(buffer);
                let null_buffer = self.read_null_buffer();
                Arc::new(Float32Array::try_new(data_buffer, null_buffer)?)
            }
            DataType::Float64 => {
                let buffer = self.read_buffer();
                let data_buffer = ScalarBuffer::<f64>::from(buffer);
                let null_buffer = self.read_null_buffer();
                Arc::new(Float64Array::try_new(data_buffer, null_buffer)?)
            }
            DataType::Date32 => {
                let buffer = self.read_buffer();
                let data_buffer = ScalarBuffer::<i32>::from(buffer);
                let null_buffer = self.read_null_buffer();
                Arc::new(Date32Array::try_new(data_buffer, null_buffer)?)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let buffer = self.read_buffer();
                let data_buffer = ScalarBuffer::<i64>::from(buffer);
                let null_buffer = self.read_null_buffer();
                Arc::new(TimestampMicrosecondArray::try_new(
                    data_buffer,
                    null_buffer,
                )?)
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                let buffer = self.read_buffer();
                let data_buffer = ScalarBuffer::<i64>::from(buffer);
                let null_buffer = self.read_null_buffer();
                Arc::new(
                    TimestampMicrosecondArray::try_new(data_buffer, null_buffer)?.with_timezone(tz),
                )
            }
            DataType::Decimal128(p, s) => {
                let buffer = self.read_buffer();
                let data_buffer = ScalarBuffer::<i128>::from(buffer);
                let null_buffer = self.read_null_buffer();
                Arc::new(
                    Decimal128Array::try_new(data_buffer, null_buffer)?
                        .with_precision_and_scale(p, s)?,
                )
            }
            DataType::Utf8 => {
                let buffer = self.read_buffer();
                let offset_buffer = self.read_offset_buffer();
                let null_buffer = self.read_null_buffer();
                Arc::new(StringArray::try_new(offset_buffer, buffer, null_buffer)?)
            }
            DataType::Binary => {
                let buffer = self.read_buffer();
                let offset_buffer = self.read_offset_buffer();
                let null_buffer = self.read_null_buffer();
                Arc::new(BinaryArray::try_new(offset_buffer, buffer, null_buffer)?)
            }
            DataType::Dictionary(k, _) if *k == DataType::Int32 => {
                let k = self.read_array()?;
                let v = self.read_array()?;
                Arc::new(DictionaryArray::try_new(
                    k.as_primitive::<Int32Type>().to_owned(),
                    v,
                )?)
            }
            other => {
                return Err(DataFusionError::Internal(format!(
                    "unsupported type in fast reader {other}"
                )))
            }
        })
    }

    fn read_data_type(&mut self) -> Result<DataType, DataFusionError> {
        let type_id = self.input[self.offset] as i32;
        let data_type = match type_id {
            x if x == DataTypeId::Boolean as i32 => DataType::Boolean,
            x if x == DataTypeId::Int8 as i32 => DataType::Int8,
            x if x == DataTypeId::Int16 as i32 => DataType::Int16,
            x if x == DataTypeId::Int32 as i32 => DataType::Int32,
            x if x == DataTypeId::Int64 as i32 => DataType::Int64,
            x if x == DataTypeId::Float32 as i32 => DataType::Float32,
            x if x == DataTypeId::Float64 as i32 => DataType::Float64,
            x if x == DataTypeId::Date32 as i32 => DataType::Date32,
            x if x == DataTypeId::TimestampNtz as i32 => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            x if x == DataTypeId::Timestamp as i32 => {
                self.offset += 1;
                let tz = self.read_string();
                let tz: Arc<str> = Arc::from(tz.into_boxed_str());
                DataType::Timestamp(TimeUnit::Microsecond, Some(tz))
            }
            x if x == DataTypeId::Utf8 as i32 => DataType::Utf8,
            x if x == DataTypeId::Binary as i32 => DataType::Binary,
            x if x == DataTypeId::Dictionary as i32 => {
                self.offset += 1;
                DataType::Dictionary(
                    Box::new(self.read_data_type()?),
                    Box::new(self.read_data_type()?),
                )
            }
            x if x == DataTypeId::Decimal128 as i32 => DataType::Decimal128(
                self.input[self.offset + 1],
                self.input[self.offset + 2] as i8,
            ),
            other => {
                return Err(DataFusionError::Internal(format!(
                    "unsupported type {other}"
                )))
            }
        };
        match data_type {
            DataType::Dictionary(_, _) | DataType::Timestamp(_, Some(_)) => {
                // no need to increment
            }
            DataType::Decimal128(_, _) => self.offset += 3,
            _ => self.offset += 1,
        }
        Ok(data_type)
    }

    fn read_bool(&mut self) -> bool {
        let value = self.input[self.offset] != 0;
        self.offset += 1;
        value
    }

    fn read_string(&mut self) -> String {
        // read field name length
        self.length
            .copy_from_slice(&self.input[self.offset..self.offset + 8]);
        let field_name_len = usize::from_le_bytes(self.length);
        self.offset += 8;

        // read field name
        let field_name_bytes = &self.input[self.offset..self.offset + field_name_len];
        let str = unsafe { String::from_utf8_unchecked(field_name_bytes.into()) };
        self.offset += field_name_len;
        str
    }

    fn read_offset_buffer(&mut self) -> OffsetBuffer<i32> {
        let offset_buffer = self.read_buffer();
        let offset_buffer: ScalarBuffer<i32> = ScalarBuffer::from(offset_buffer);
        OffsetBuffer::new(offset_buffer)
    }

    fn read_buffer(&mut self) -> Buffer {
        // read data buffer length
        let mut length = [0; 8];
        length.copy_from_slice(&self.input[self.offset..self.offset + 8]);
        let buffer_len = usize::from_le_bytes(length);
        self.offset += 8;

        // read data buffer
        let buffer = Buffer::from(&self.input[self.offset..self.offset + buffer_len]);
        self.offset += buffer_len;
        buffer
    }

    fn read_null_buffer(&mut self) -> Option<NullBuffer> {
        // read null buffer length in bits
        let mut length = [0; 8];
        length.copy_from_slice(&self.input[self.offset..self.offset + 8]);
        let length_bits = usize::from_le_bytes(length);
        self.offset += 8;
        if length_bits == 0 {
            return None;
        }

        // read buffer length in bytes
        length.copy_from_slice(&self.input[self.offset..self.offset + 8]);
        let null_buffer_length = usize::from_le_bytes(length);
        self.offset += 8;

        let null_buffer = if null_buffer_length != 0 {
            let null_buffer = &self.input[self.offset..self.offset + null_buffer_length];
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(null_buffer),
                0,
                length_bits,
            )))
        } else {
            None
        };
        self.offset += null_buffer_length;
        null_buffer
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::builder::*;
    use std::sync::Arc;

    #[test]
    fn roundtrip() {
        let batch = create_batch(8192, true);
        let buffer = Vec::new();
        let mut writer = BatchWriter::new(buffer);
        writer.write_partial_schema(&batch.schema()).unwrap();
        writer.write_batch(&batch).unwrap();
        let buffer = writer.inner();

        let mut reader = BatchReader::new(&buffer);
        let batch2 = reader.read_batch().unwrap();
        assert_eq!(batch, batch2);
    }

    #[test]
    fn roundtrip_sliced() {
        let batch = create_batch(8192, true);

        let mut start = 0;
        let batch_size = 128;
        while start < batch.num_rows() {
            let end = (start + batch_size).min(batch.num_rows());
            let sliced_batch = batch.slice(start, end - start);
            let buffer = Vec::new();
            let mut writer = BatchWriter::new(buffer);
            writer.write_partial_schema(&sliced_batch.schema()).unwrap();
            writer.write_batch(&sliced_batch).unwrap();
            let buffer = writer.inner();

            let mut reader = BatchReader::new(&buffer);
            let batch2 = reader.read_batch().unwrap();
            assert_eq!(sliced_batch, batch2);

            start = end;
        }
    }

    fn create_batch(num_rows: usize, allow_nulls: bool) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bool", DataType::Boolean, true),
            Field::new("int8", DataType::Int8, true),
            Field::new("int16", DataType::Int16, true),
            Field::new("int32", DataType::Int32, true),
            Field::new("int64", DataType::Int64, true),
            Field::new("float32", DataType::Float32, true),
            Field::new("float64", DataType::Float64, true),
            Field::new("binary", DataType::Binary, true),
            Field::new("utf8", DataType::Utf8, true),
            Field::new(
                "utf8_dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("date32", DataType::Date32, true),
            Field::new("decimal128", DataType::Decimal128(11, 2), true),
            Field::new(
                "timestamp_ntz",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        let mut col_bool = BooleanBuilder::with_capacity(num_rows);
        let mut col_i8 = Int8Builder::new();
        let mut col_i16 = Int16Builder::new();
        let mut col_i32 = Int32Builder::new();
        let mut col_i64 = Int64Builder::new();
        let mut col_f32 = Float32Builder::new();
        let mut col_f64 = Float64Builder::new();
        let mut col_binary = BinaryBuilder::new();
        let mut col_utf8 = StringBuilder::new();
        let mut col_utf8_dict: StringDictionaryBuilder<Int32Type> = StringDictionaryBuilder::new();
        let mut col_date32 = Date32Builder::new();
        let mut col_decimal128 = Decimal128Builder::new()
            .with_precision_and_scale(11, 2)
            .unwrap();
        let mut col_timestamp_ntz = TimestampMicrosecondBuilder::with_capacity(num_rows);
        let mut col_timestamp =
            TimestampMicrosecondBuilder::with_capacity(num_rows).with_timezone("UTC");
        for i in 0..num_rows {
            col_i8.append_value(i as i8);
            col_i16.append_value(i as i16);
            col_i32.append_value(i as i32);
            col_i64.append_value(i as i64);
            col_f32.append_value(i as f32 * 1.23_f32);
            col_f64.append_value(i as f64 * 1.23_f64);
            col_date32.append_value(i as i32);
            col_decimal128.append_value((i * 1000000) as i128);
            col_binary.append_value(format!("{i}").as_bytes());
            if allow_nulls && i % 10 == 0 {
                col_utf8.append_null();
                col_utf8_dict.append_null();
                col_bool.append_null();
                col_timestamp_ntz.append_null();
                col_timestamp.append_null();
            } else {
                // test for dictionary-encoded strings
                col_utf8.append_value(format!("this is string {i}"));
                col_utf8_dict.append_value("this string is repeated a lot");
                col_bool.append_value(i % 2 == 0);
                col_timestamp_ntz.append_value((i * 100000000) as i64);
                col_timestamp.append_value((i * 100000000) as i64);
            }
        }
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(col_bool.finish()),
                Arc::new(col_i8.finish()),
                Arc::new(col_i16.finish()),
                Arc::new(col_i32.finish()),
                Arc::new(col_i64.finish()),
                Arc::new(col_f32.finish()),
                Arc::new(col_f64.finish()),
                Arc::new(col_binary.finish()),
                Arc::new(col_utf8.finish()),
                Arc::new(col_utf8_dict.finish()),
                Arc::new(col_date32.finish()),
                Arc::new(col_decimal128.finish()),
                Arc::new(col_timestamp_ntz.finish()),
                Arc::new(col_timestamp.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Debug, Clone, Default)]
pub enum CompressionCodec {
    #[default]
    None,
    Lz4Frame,
    Zstd(i32),
    Snappy,
}

#[derive(Debug, Clone, Default)]
pub struct ShuffleBlockWriter {
    fast_encoding: bool,
    codec: CompressionCodec,
    encoded_schema: Vec<u8>,
    header_bytes: Vec<u8>,
}

impl ShuffleBlockWriter {
    pub fn try_new(
        schema: &Schema,
        enable_fast_encoding: bool,
        codec: CompressionCodec,
    ) -> Result<Self> {
        let mut encoded_schema = vec![];

        let enable_fast_encoding = enable_fast_encoding
            && schema
                .fields()
                .iter()
                .all(|f| fast_codec_supports_type(f.data_type()));

        // encode the schema once and then reuse the encoded bytes for each batch
        if enable_fast_encoding {
            let mut w = BatchWriter::new(&mut encoded_schema);
            w.write_partial_schema(schema)?;
        }

        let header_bytes = Vec::with_capacity(24);
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

        // write encoding scheme
        if enable_fast_encoding {
            cursor.write_all(b"FAST")?;
        } else {
            cursor.write_all(b"AIPC")?;
        }

        let header_bytes = cursor.into_inner();

        Ok(Self {
            fast_encoding: enable_fast_encoding,
            codec,
            encoded_schema,
            header_bytes,
        })
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

        let output = if self.fast_encoding {
            match &self.codec {
                CompressionCodec::None => {
                    let mut fast_writer = BatchWriter::new(&mut *output);
                    fast_writer.write_all(&self.encoded_schema)?;
                    fast_writer.write_batch(batch)?;
                    output
                }
                CompressionCodec::Lz4Frame => {
                    let mut wtr = lz4_flex::frame::FrameEncoder::new(output);
                    let mut fast_writer = BatchWriter::new(&mut wtr);
                    fast_writer.write_all(&self.encoded_schema)?;
                    fast_writer.write_batch(batch)?;
                    wtr.finish().map_err(|e| {
                        DataFusionError::Execution(format!("lz4 compression error: {}", e))
                    })?
                }
                CompressionCodec::Zstd(level) => {
                    let mut encoder = zstd::Encoder::new(output, *level)?;
                    let mut fast_writer = BatchWriter::new(&mut encoder);
                    fast_writer.write_all(&self.encoded_schema)?;
                    fast_writer.write_batch(batch)?;
                    encoder.finish()?
                }
                CompressionCodec::Snappy => {
                    let mut encoder = snap::write::FrameEncoder::new(output);
                    let mut fast_writer = BatchWriter::new(&mut encoder);
                    fast_writer.write_all(&self.encoded_schema)?;
                    fast_writer.write_batch(batch)?;
                    encoder.into_inner().map_err(|e| {
                        DataFusionError::Execution(format!("snappy compression error: {}", e))
                    })?
                }
            }
        } else {
            match &self.codec {
                CompressionCodec::None => {
                    let mut arrow_writer = StreamWriter::try_new(output, &batch.schema())?;
                    arrow_writer.write(batch)?;
                    arrow_writer.finish()?;
                    arrow_writer.into_inner()?
                }
                CompressionCodec::Lz4Frame => {
                    let mut wtr = lz4_flex::frame::FrameEncoder::new(output);
                    let mut arrow_writer = StreamWriter::try_new(&mut wtr, &batch.schema())?;
                    arrow_writer.write(batch)?;
                    arrow_writer.finish()?;
                    wtr.finish().map_err(|e| {
                        DataFusionError::Execution(format!("lz4 compression error: {}", e))
                    })?
                }

                CompressionCodec::Zstd(level) => {
                    let encoder = zstd::Encoder::new(output, *level)?;
                    let mut arrow_writer = StreamWriter::try_new(encoder, &batch.schema())?;
                    arrow_writer.write(batch)?;
                    arrow_writer.finish()?;
                    let zstd_encoder = arrow_writer.into_inner()?;
                    zstd_encoder.finish()?
                }

                CompressionCodec::Snappy => {
                    let mut wtr = snap::write::FrameEncoder::new(output);
                    let mut arrow_writer = StreamWriter::try_new(&mut wtr, &batch.schema())?;
                    arrow_writer.write(batch)?;
                    arrow_writer.finish()?;
                    wtr.into_inner().map_err(|e| {
                        DataFusionError::Execution(format!("snappy compression error: {}", e))
                    })?
                }
            }
        };

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

        // fill ipc length
        output.seek(SeekFrom::Start(start_pos))?;
        output.write_all(&ipc_length.to_le_bytes())?;
        output.seek(SeekFrom::Start(end_pos))?;

        timer.stop();

        Ok((end_pos - start_pos) as usize)
    }
}

pub fn read_ipc_compressed(bytes: &[u8]) -> Result<RecordBatch> {
    let fast_encoding = match &bytes[4..8] {
        b"AIPC" => false,
        b"FAST" => true,
        other => {
            return Err(DataFusionError::Internal(format!(
                "invalid encoding schema: {other:?}"
            )))
        }
    };
    match &bytes[0..4] {
        b"SNAP" => {
            let mut decoder = snap::read::FrameDecoder::new(&bytes[8..]);
            if fast_encoding {
                // TODO avoid reading bytes into interim buffer
                let mut buffer = vec![];
                decoder.read_to_end(&mut buffer)?;
                let mut reader = BatchReader::new(&buffer);
                reader.read_batch()
            } else {
                let mut reader = StreamReader::try_new(decoder, None)?;
                reader.next().unwrap().map_err(|e| e.into())
            }
        }
        b"LZ4_" => {
            let mut decoder = lz4_flex::frame::FrameDecoder::new(&bytes[8..]);
            if fast_encoding {
                // TODO avoid reading bytes into interim buffer
                let mut buffer = vec![];
                decoder.read_to_end(&mut buffer)?;
                let mut reader = BatchReader::new(&buffer);
                reader.read_batch()
            } else {
                let mut reader = StreamReader::try_new(decoder, None)?;
                reader.next().unwrap().map_err(|e| e.into())
            }
        }
        b"ZSTD" => {
            let mut decoder = zstd::Decoder::new(&bytes[8..])?;
            if fast_encoding {
                // TODO avoid reading bytes into interim buffer
                let mut buffer = vec![];
                decoder.read_to_end(&mut buffer)?;
                let mut reader = BatchReader::new(&buffer);
                reader.read_batch()
            } else {
                let mut reader = StreamReader::try_new(decoder, None)?;
                reader.next().unwrap().map_err(|e| e.into())
            }
        }
        b"NONE" => {
            if fast_encoding {
                let mut reader = BatchReader::new(&bytes[8..]);
                reader.read_batch()
            } else {
                let mut reader = StreamReader::try_new(&bytes[8..], None)?;
                reader.next().unwrap().map_err(|e| e.into())
            }
        }
        other => Err(DataFusionError::Execution(format!(
            "Failed to decode batch: invalid compression codec: {other:?}"
        ))),
    }
}

/// Checksum algorithms for writing IPC bytes.
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
