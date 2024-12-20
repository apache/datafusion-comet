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

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::DataFusionError;
use std::io::Write;
use std::sync::Arc;

pub fn write_batch_fast(
    batch: &RecordBatch,
    output: &mut Vec<u8>,
    write_schema: bool,
) -> Result<(), DataFusionError> {
    if write_schema {
        // write schema
        let schema_len = batch.schema().fields().len();
        output.write_all(&schema_len.to_le_bytes()[..])?;

        for field in batch.schema().fields() {
            let field_name = field.name();
            let field_name_len = field_name.len();
            output.write_all(&field_name_len.to_le_bytes()[..])?;
            output.write_all(field_name.as_str().as_bytes())?;

            // TODO write field type using FFI_ArrowSchema encoding, assume string for now
        }
    }

    // for each array
    for i in 0..batch.schema().fields.len() {
        match batch.schema().field(i).data_type() {
            DataType::Utf8 => {
                let string_array = batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();

                // write data buffer length
                let buffer = string_array.values().as_slice();
                let buffer_len = buffer.len();
                output.write_all(&buffer_len.to_le_bytes()[..])?;

                // write data buffer
                // println!("writing data buffer with {buffer_len} bytes");
                output.write_all(buffer)?;

                // write offset buffer length
                let offsets = string_array.offsets();
                let scalar_buffer = offsets.inner();
                let buffer = scalar_buffer.inner();
                let offset_buffer_len = buffer.len();
                output.write_all(&offset_buffer_len.to_le_bytes()[..])?;

                // write offset buffer
                let offset_buffer = scalar_buffer.inner().as_slice();
                // println!("writing offset buffer with {offset_buffer_len} bytes");
                output.write_all(offset_buffer)?;
            }
            _ => todo!(),
        }
    }

    Ok(())
}

pub fn read_batch_fast(
    input: &[u8],
    schema: Option<SchemaRef>,
) -> Result<RecordBatch, DataFusionError> {
    let mut offset = 0;

    // use provided schema, or read schema from bytes
    let schema: Arc<Schema> = schema.unwrap_or_else(|| {
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

            // create field
            let field = Arc::new(Field::new(field_name, DataType::Utf8, false));
            fields.push(field);
        }
        Arc::new(Schema::new(fields))
    });

    let mut length = [0; 8];
    let mut arrays = Vec::with_capacity(schema.fields().len());
    for _ in 0..schema.fields().len() {
        // read data buffer length
        length.copy_from_slice(&input[offset..offset + 8]);
        let buffer_len = usize::from_le_bytes(length);
        offset += 8;

        // read data buffer
        // println!("reading data buffer with {buffer_len} bytes");
        let offset_segment_start = offset + buffer_len;
        let buffer = Buffer::from(&input[offset..offset_segment_start]);
        offset += buffer_len;

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

        // create array
        let array: ArrayRef = Arc::new(StringArray::try_new(offset_buffer, buffer, None)?);
        arrays.push(array);
    }

    Ok(RecordBatch::try_new(schema, arrays).unwrap())
}

pub fn write_batch_ipc(batch: &RecordBatch, output: &mut Vec<u8>) -> Result<(), DataFusionError> {
    let mut arrow_writer = StreamWriter::try_new(output, &batch.schema())?;
    arrow_writer.write(batch)?;
    arrow_writer.finish().unwrap();
    Ok(())
}

pub fn read_batch_ipc(input: &[u8]) -> Result<RecordBatch, DataFusionError> {
    let mut arrow_reader = StreamReader::try_new(input, None).unwrap();
    Ok(arrow_reader.next().unwrap().unwrap())
}

pub fn write_batch_ipc_data_only(batch: &RecordBatch, output: &mut Vec<u8>) -> Result<(), DataFusionError> {
    // Error of dictionary ids are replaced.
    let error_on_replacement = true;
    let options = IpcWriteOptions::default();
    let mut dictionary_tracker = DictionaryTracker::new(error_on_replacement);

    // encode the batch into zero or more encoded dictionaries
    // and the data for the actual array.
    let data_gen = IpcDataGenerator::default();
    let (encoded_dictionaries, encoded_message) = data_gen
        .encoded_batch(&batch, &mut dictionary_tracker, &options)
        .unwrap();
    for dict in &encoded_dictionaries {
        output.write_all(&dict.ipc_message)?;
        output.write_all(&dict.arrow_data)?;
    }
    output.write_all(&encoded_message.ipc_message)?;
    output.write_all(&encoded_message.arrow_data)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_array::builder::StringBuilder;
    use arrow_array::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn roundtrip_batch_fast_with_schema() {
        let batch = create_batch();
        let mut output = Vec::new();

        write_batch_fast(&batch, &mut output, true).unwrap();
        assert_eq!(193376, output.len());

        let batch2 = read_batch_fast(&output, None).unwrap();

        assert_batches_eq(batch, batch2);
    }

    #[test]
    fn roundtrip_batch_fast_no_schema() {
        let batch = create_batch();
        let mut output = Vec::new();

        write_batch_fast(&batch, &mut output, false).unwrap();
        assert_eq!(193338, output.len());

        let batch2 = read_batch_fast(&output, Some(batch.schema())).unwrap();

        assert_batches_eq(batch, batch2);
    }

    #[test]
    fn roundtrip_batch_ipc() {
        let batch = create_batch();
        let mut output = Vec::new();

        write_batch_ipc(&batch, &mut output).unwrap();
        assert_eq!(197192, output.len());

        let batch2 = read_batch_ipc(&output).unwrap();

        assert_batches_eq(batch, batch2);
    }

    fn create_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Utf8, false),
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Utf8, false),
        ]));
        let mut b = StringBuilder::new();
        for i in 0..8192 {
            b.append_value(format!("{i}"));
        }
        let array: ArrayRef = Arc::new(b.finish());
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::clone(&array), Arc::clone(&array), array],
        )
        .unwrap()
    }

    fn assert_batches_eq(batch: RecordBatch, batch2: RecordBatch) {
        assert_eq!(batch.schema(), batch2.schema());
        assert_eq!(batch.num_rows(), batch2.num_rows());

        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let b = batch2
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(a.value(i), b.value(i));
        }
    }
}
