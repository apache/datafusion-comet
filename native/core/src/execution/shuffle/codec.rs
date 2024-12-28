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

use arrow_array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::DataFusionError;
use std::io::Write;
use std::sync::Arc;

pub fn fast_codec_supports_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Int32 | DataType::Int64 | DataType::Date32 | DataType::Utf8 => true,
        DataType::Decimal128(_, s) if *s >= 0 => true,
        _ => false,
    }
}

enum DataTypeId {
    // Int8 = 0,
    // Int16 = 1,
    Int32 = 2,
    Int64 = 3,
    Date32 = 4,
    Decimal128 = 5,
    Utf8 = 6,
}

pub struct BatchWriter<W: Write> {
    inner: W,
}

impl<W: Write> BatchWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        // write schema
        let schema_len = batch.schema().fields().len();
        self.inner.write_all(&schema_len.to_le_bytes())?;

        for field in batch.schema().fields() {
            // field name
            let field_name = field.name();
            self.inner.write_all(&field_name.len().to_le_bytes())?;
            self.inner.write_all(field_name.as_str().as_bytes())?;
            // data type
            match field.data_type() {
                DataType::Int32 => {
                    self.inner.write_all(&[DataTypeId::Int32 as u8])?;
                }
                DataType::Int64 => {
                    self.inner.write_all(&[DataTypeId::Int64 as u8])?;
                }
                DataType::Date32 => {
                    self.inner.write_all(&[DataTypeId::Date32 as u8])?;
                }
                DataType::Utf8 => {
                    self.inner.write_all(&[DataTypeId::Utf8 as u8])?;
                }
                DataType::Decimal128(p, s) if *s >= 0 => {
                    self.inner
                        .write_all(&[DataTypeId::Decimal128 as u8, *p, *s as u8])?;
                }
                _ => todo!(),
            }
            // TODO nullable - assume all nullable for now
        }

        // write row count
        // self.inner.write_all(&batch.num_rows().to_le_bytes())?;

        for i in 0..batch.num_columns() {
            let col = batch.column(i);
            match col.data_type() {
                DataType::Int32 => {
                    let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();

                    // write data buffer
                    let buffer = arr.values();
                    let buffer = buffer.inner();
                    self.write_buffer(buffer)?;

                    if let Some(nulls) = arr.nulls() {
                        let buffer = nulls.inner();
                        let buffer = buffer.inner();
                        self.write_buffer(buffer)?;
                    } else {
                        self.inner.write_all(&0_usize.to_le_bytes())?;
                    }
                }
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();

                    // write data buffer
                    let buffer = arr.values();
                    let buffer = buffer.inner();
                    self.write_buffer(buffer)?;

                    if let Some(nulls) = arr.nulls() {
                        let buffer = nulls.inner();
                        let buffer = buffer.inner();
                        self.write_buffer(buffer)?;
                    } else {
                        self.inner.write_all(&0_usize.to_le_bytes())?;
                    }
                }
                DataType::Date32 => {
                    let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();

                    // write data buffer
                    let buffer = arr.values();
                    let buffer = buffer.inner();
                    self.write_buffer(buffer)?;

                    if let Some(nulls) = arr.nulls() {
                        let buffer = nulls.inner();
                        let buffer = buffer.inner();
                        self.write_buffer(buffer)?;
                    } else {
                        self.inner.write_all(&0_usize.to_le_bytes())?;
                    }
                }
                DataType::Decimal128(_, _) => {
                    let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();

                    // write data buffer
                    let buffer = arr.values();
                    let buffer = buffer.inner();
                    self.write_buffer(buffer)?;

                    if let Some(nulls) = arr.nulls() {
                        let buffer = nulls.inner();
                        let buffer = buffer.inner();
                        self.write_buffer(buffer)?;
                    } else {
                        self.inner.write_all(&0_usize.to_le_bytes())?;
                    }
                }
                DataType::Utf8 => {
                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();

                    // write data buffer
                    let buffer = arr.values();
                    self.write_buffer(buffer)?;

                    // write offset buffer
                    let offsets = arr.offsets();
                    let scalar_buffer = offsets.inner();
                    let buffer = scalar_buffer.inner();
                    self.write_buffer(buffer)?;

                    if let Some(nulls) = arr.nulls() {
                        let buffer = nulls.inner();
                        let buffer = buffer.inner();
                        self.write_buffer(buffer)?;
                    } else {
                        self.inner.write_all(&0_usize.to_le_bytes())?;
                    }
                }
                _ => todo!(),
            }
        }

        Ok(())
    }

    pub fn inner(self) -> W {
        self.inner
    }

    fn write_buffer(&mut self, buffer: &Buffer) -> std::io::Result<()> {
        // write buffer length
        self.inner.write_all(&buffer.len().to_le_bytes())?;
        // write buffer data
        self.inner.write_all(buffer.as_slice())
    }
}

pub struct BatchReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> BatchReader<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    pub fn read_batch(&mut self) -> Result<RecordBatch, DataFusionError> {
        // use provided schema, or read schema from bytes
        let mut length = [0; 8];
        length.copy_from_slice(&self.input[0..8]);
        self.offset += 8;
        let schema_len = usize::from_le_bytes(length);

        let mut fields: Vec<Arc<Field>> = Vec::with_capacity(schema_len);
        for _ in 0..schema_len {
            // read field name length
            length.copy_from_slice(&self.input[self.offset..self.offset + 8]);
            let field_name_len = usize::from_le_bytes(length);
            self.offset += 8;

            // read field name
            let field_name_bytes = &self.input[self.offset..self.offset + field_name_len];
            self.offset += field_name_bytes.len();
            let field_name = unsafe { String::from_utf8_unchecked(field_name_bytes.into()) };

            // read data type
            let type_id = self.input[self.offset] as i32;
            let data_type = match type_id {
                x if x == DataTypeId::Int32 as i32 => DataType::Int32,
                x if x == DataTypeId::Int64 as i32 => DataType::Int64,
                x if x == DataTypeId::Date32 as i32 => DataType::Date32,
                x if x == DataTypeId::Utf8 as i32 => DataType::Utf8,
                x if x == DataTypeId::Decimal128 as i32 => DataType::Decimal128(
                    self.input[self.offset + 1],
                    self.input[self.offset + 2] as i8,
                ),
                _ => unreachable!(),
            };
            self.offset += 1;
            if matches!(data_type, DataType::Decimal128(_, _)) {
                self.offset += 2;
            }

            // create field
            let field = Arc::new(Field::new(field_name, data_type, true));
            fields.push(field);
        }
        let schema = Arc::new(Schema::new(fields));

        let mut arrays = Vec::with_capacity(schema.fields().len());
        for i in 0..schema.fields().len() {
            let buffer = self.read_buffer();

            match schema.field(i).data_type() {
                DataType::Int32 => {
                    // create array
                    let data_buffer = ScalarBuffer::<i32>::from(buffer);

                    // read null buffer
                    let null_buffer = self.read_null_buffer();

                    let array: ArrayRef = Arc::new(Int32Array::try_new(data_buffer, null_buffer)?);
                    arrays.push(array);
                }
                DataType::Int64 => {
                    // create array
                    let data_buffer = ScalarBuffer::<i64>::from(buffer);

                    // read null buffer
                    let null_buffer = self.read_null_buffer();

                    let array: ArrayRef = Arc::new(Int64Array::try_new(data_buffer, null_buffer)?);
                    arrays.push(array);
                }
                DataType::Date32 => {
                    // create array
                    let data_buffer = ScalarBuffer::<i32>::from(buffer);

                    // read null buffer
                    let null_buffer = self.read_null_buffer();

                    let array: ArrayRef = Arc::new(Date32Array::try_new(data_buffer, null_buffer)?);
                    arrays.push(array);
                }
                DataType::Decimal128(p, s) => {
                    // create array
                    let data_buffer = ScalarBuffer::<i128>::from(buffer);

                    // read null buffer
                    let null_buffer = self.read_null_buffer();

                    let array: ArrayRef = Arc::new(
                        Decimal128Array::try_new(data_buffer, null_buffer)?
                            .with_precision_and_scale(*p, *s)?,
                    );
                    arrays.push(array);
                }
                DataType::Utf8 => {
                    // read offset buffer
                    let offset_buffer = self.read_offset_buffer();

                    // read null buffer
                    let null_buffer = self.read_null_buffer();

                    // create array
                    let array: ArrayRef =
                        Arc::new(StringArray::try_new(offset_buffer, buffer, null_buffer)?);
                    arrays.push(array);
                }
                _ => todo!(),
            }
        }

        assert_eq!(schema.fields().len(), arrays.len());
        for i in 0..arrays.len() {
            println!("{} length = {}", schema.field(i).name(), arrays[i].len());
        }

        Ok(RecordBatch::try_new(schema, arrays).unwrap())
    }

    fn read_offset_buffer(&mut self) -> OffsetBuffer<i32> {
        let offset_buffer = self.read_buffer();
        let offset_buffer: ScalarBuffer<i32> = ScalarBuffer::from(offset_buffer);
        let offset_buffer = OffsetBuffer::new(offset_buffer);
        offset_buffer
    }

    fn read_buffer(&mut self) -> Buffer {
        // read data buffer length
        let mut length = [0; 8];
        length.copy_from_slice(&self.input[self.offset..self.offset + 8]);
        let buffer_len = usize::from_le_bytes(length);
        self.offset += 8;

        // read data buffer
        // println!("reading data buffer with {buffer_len} bytes");
        let buffer = Buffer::from(&self.input[self.offset..self.offset + buffer_len]);
        self.offset += buffer_len;
        buffer
    }

    fn read_null_buffer(&mut self) -> Option<NullBuffer> {
        let mut length = [0; 8];
        length.copy_from_slice(&self.input[self.offset..self.offset + 8]);
        let null_buffer_length = usize::from_le_bytes(length);
        self.offset += 8;
        let null_buffer = if null_buffer_length != 0 {
            let null_buffer = &self.input[self.offset..self.offset + null_buffer_length];
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(null_buffer),
                0,
                null_buffer.len() * 8,
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
    use arrow_array::builder::{Date32Builder, Decimal128Builder, Int32Builder, StringBuilder};
    use std::sync::Arc;

    #[test]
    fn roundtrip() {
        let batch = create_batch(8192, true);
        let buffer = Vec::new();
        let mut writer = BatchWriter::new(buffer);
        writer.write_batch(&batch).unwrap();
        let buffer = writer.inner();
        //assert_eq!(257315, buffer.len());

        let mut reader = BatchReader::new(&buffer);
        let batch2 = reader.read_batch().unwrap();
        assert_eq!(batch, batch2);
    }

    fn create_batch(num_rows: usize, allow_nulls: bool) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Int32, true),
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Date32, true),
            Field::new("c3", DataType::Decimal128(11, 2), true),
        ]));
        let mut a = Int32Builder::new();
        let mut b = StringBuilder::new();
        let mut c = Date32Builder::new();
        let mut d = Decimal128Builder::new()
            .with_precision_and_scale(11, 2)
            .unwrap();
        for i in 0..num_rows {
            a.append_value(i as i32);
            c.append_value(i as i32);
            d.append_value((i * 1000000) as i128);
            if allow_nulls && i % 10 == 0 {
                b.append_null();
            } else {
                b.append_value(format!("this is string number {i}"));
            }
        }
        let a = a.finish();
        let b = b.finish();
        let c = c.finish();
        let d = d.finish();
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(d)],
        )
        .unwrap()
    }
}
