use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::DataFusionError;
use std::io::Write;
use std::sync::Arc;

pub fn write_batch(batch: &RecordBatch, output: &mut Vec<u8>) -> Result<(), DataFusionError> {
    // write schema
    let schema_len = batch.schema().fields().len();
    output.write_all(&schema_len.to_le_bytes()[..])?;

    for field in batch.schema().fields() {
        let field_name = field.name();
        let field_name_len = field_name.len();
        output.write_all(&field_name_len.to_le_bytes()[..])?;
        output.write(field_name.as_str().as_bytes())?;

        // TODO write field type using FFI_ArrowSchema encoding, assume string for now
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
                output.write(buffer)?;

                // write offset buffer length
                let offsets = string_array.offsets();
                let scalar_buffer = offsets.inner();
                let buffer = scalar_buffer.inner();
                let offset_buffer_len = buffer.len();
                output.write_all(&offset_buffer_len.to_le_bytes()[..])?;

                // write offset buffer
                let offset_buffer = scalar_buffer.inner().as_slice();
                // println!("writing offset buffer with {offset_buffer_len} bytes");
                output.write(offset_buffer)?;
            }
            _ => todo!(),
        }
    }

    Ok(())
}

pub fn read_batch(input: &[u8]) -> Result<RecordBatch, DataFusionError> {
    let mut length = [0; 8];
    length.copy_from_slice(&input[0..8]);
    let schema_len = usize::from_le_bytes(length);

    let mut offset = 8;
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
    let schema = Arc::new(Schema::new(fields));

    let mut arrays = Vec::with_capacity(schema.fields().len());
    for _ in 0..schema_len {
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

#[cfg(test)]
mod test {
    use super::*;
    use arrow_array::builder::StringBuilder;
    use arrow_array::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn roundtrip_batch_fast() {
        let batch = create_batch();
        let mut output = Vec::new();

        write_batch(&batch, &mut output).unwrap();
        // assert_eq!(64463, output.len());

        let batch2 = read_batch(&output).unwrap();

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

    fn create_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let mut b = StringBuilder::new();
        for i in 0..8192 {
            b.append_value(format!("{i}"));
        }
        let array = b.finish();
        RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
    }
}
