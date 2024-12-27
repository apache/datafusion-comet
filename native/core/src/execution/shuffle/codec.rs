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

use std::io::Write;
use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_buffer::Buffer;
use arrow_schema::DataType;
use datafusion_common::DataFusionError;

pub struct BatchWriter<W: Write> {
    inner: W,
}

impl<W: Write> BatchWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner
        }
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
                _ => todo!()
            }
            // TODO nullable
        }

        // write row count
        self.inner.write_all(&batch.num_rows().to_le_bytes())?;

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

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use arrow_array::builder::{Int32Builder, StringBuilder};
    use super::*;
    use arrow_schema::*;

    #[test]
    fn roundtrip() {
        let batch = create_batch(8192, true);
        let buffer = Vec::new();
        let mut writer = BatchWriter::new(buffer);
        writer.write_batch(&batch).unwrap();
        let buffer = writer.inner();
        assert_eq!(257307, buffer.len());

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