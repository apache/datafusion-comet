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

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::DataFusionError;
use std::io::Write;
use std::sync::Arc;

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
                    self.inner.write_all(&[0_u8])?;
                }
                DataType::Utf8 => {
                    self.inner.write_all(&[1_u8])?;
                }
                _ => todo!(),
            }
            // TODO nullable
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

pub fn read_batch_fast(input: &[u8]) -> Result<RecordBatch, DataFusionError> {
    let mut offset = 0;

    // use provided schema, or read schema from bytes
    let mut length = [0; 8];
    length.copy_from_slice(&input[0..8]);
    offset += 8;
    let schema_len = usize::from_le_bytes(length);

    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(schema_len);
    for _ in 0..schema_len {
        // read field name length
        length.copy_from_slice(&input[offset..offset + 8]);
        let field_name_len = usize::from_le_bytes(length);
        offset += 8;

        // read field name
        let field_name_bytes = &input[offset..offset + field_name_len];
        offset += field_name_bytes.len();
        let field_name = unsafe { String::from_utf8_unchecked(field_name_bytes.into()) };

        // TODO read field data type
        let data_type = match &input[offset] {
            0_u8 => DataType::Int32,
            1_u8 => DataType::Utf8,
            _ => todo!(),
        };
        offset += 1;

        // create field
        let field = Arc::new(Field::new(field_name, data_type, true));
        fields.push(field);
    }
    let schema = Arc::new(Schema::new(fields));

    let mut arrays = Vec::with_capacity(schema.fields().len());
    for i in 0..schema.fields().len() {
        // read data buffer length
        length.copy_from_slice(&input[offset..offset + 8]);
        let buffer_len = usize::from_le_bytes(length);
        offset += 8;

        // read data buffer
        // println!("reading data buffer with {buffer_len} bytes");
        let offset_segment_start = offset + buffer_len;
        let buffer = Buffer::from(&input[offset..offset_segment_start]);
        offset += buffer_len;

        match schema.field(i).data_type() {
            DataType::Int32 => {
                // create array
                let data_buffer = ScalarBuffer::<i32>::from(buffer);

                // read null buffer
                length.copy_from_slice(&input[offset..offset + 8]);
                let null_buffer_length = usize::from_le_bytes(length);
                offset += 8;
                let null_buffer = if null_buffer_length != 0 {
                    let null_buffer = &input[offset..offset + null_buffer_length];
                    Some(NullBuffer::new(BooleanBuffer::new(
                        Buffer::from(null_buffer),
                        0,
                        null_buffer.len() * 8,
                    )))
                } else {
                    None
                };
                offset += null_buffer_length;

                let array: ArrayRef = Arc::new(Int32Array::try_new(data_buffer, null_buffer)?);
                arrays.push(array);
            }
            DataType::Utf8 => {
                // read offset buffer length
                length.copy_from_slice(&input[offset_segment_start..offset_segment_start + 8]);
                let offset_buffer_len = usize::from_le_bytes(length);
                offset += 8;

                // read offset buffer
                // println!("reading offset buffer with {offset_buffer_len} bytes");
                let offset_buffer = Buffer::from(&input[offset..offset + offset_buffer_len]);
                offset += offset_buffer_len;
                let scalar_buffer: ScalarBuffer<i32> = ScalarBuffer::from(offset_buffer);
                let offset_buffer = OffsetBuffer::new(scalar_buffer);

                // read null buffer
                length.copy_from_slice(&input[offset..offset + 8]);
                let null_buffer_length = usize::from_le_bytes(length);
                offset += 8;
                let null_buffer = if null_buffer_length != 0 {
                    let null_buffer = &input[offset..offset + null_buffer_length];
                    Some(NullBuffer::new(BooleanBuffer::new(
                        Buffer::from(null_buffer),
                        0,
                        null_buffer.len() * 8,
                    )))
                } else {
                    None
                };
                offset += null_buffer_length;

                // create array
                let array: ArrayRef =
                    Arc::new(StringArray::try_new(offset_buffer, buffer, null_buffer)?);
                arrays.push(array);
            }
            _ => todo!(),
        }
    }

    Ok(RecordBatch::try_new(schema, arrays).unwrap())
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_array::builder::{Int32Builder, StringBuilder};
    use std::sync::Arc;

    #[test]
    fn roundtrip() {
        let batch = create_batch(8192, true);
        let buffer = Vec::new();
        let mut writer = BatchWriter::new(buffer);
        writer.write_batch(&batch).unwrap();
        let buffer = writer.inner();
        //assert_eq!(257315, buffer.len());

        let batch2 = read_batch_fast(&buffer).unwrap();
        assert_eq!(batch, batch2);
    }

    fn create_batch(num_rows: usize, allow_nulls: bool) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Int32, true),
            Field::new("c1", DataType::Utf8, true),
        ]));
        let mut a = Int32Builder::new();
        let mut b = StringBuilder::new();
        for i in 0..num_rows {
            a.append_value(i as i32);
            if allow_nulls && i % 10 == 0 {
                b.append_null();
            } else {
                b.append_value(format!("this is string number {i}"));
            }
        }
        let a = a.finish();
        let b = b.finish();
        RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap()
    }
}
