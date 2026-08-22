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

/// Utils for array vector, etc.
use crate::execution::operators::ExecutionError;
use arrow::{
    array::ArrayData,
    datatypes::Field,
    error::ArrowError,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
};

fn ffi_schema_for_field(field: &Field) -> Result<FFI_ArrowSchema, ArrowError> {
    if field.name().contains('\0') {
        // Spark keeps Parquet field names as strings, while ArrowSchema exports names as C strings:
        // https://github.com/apache/spark/blob/v4.1.3/sql/api/src/main/scala/org/apache/spark/sql/types/StructField.scala#L32-L51
        // https://github.com/apache/spark/blob/v4.1.3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetSchemaConverter.scala#L576-L647
        // https://github.com/apache/arrow-rs/blob/58.4.0/arrow-schema/src/ffi.rs#L168-L175
        // The logical output name is owned by Spark's plan, so substitute only at this boundary.
        let field = field
            .clone()
            .with_name(field.name().replace('\0', "\u{fffd}"));
        FFI_ArrowSchema::try_from(&field)
    } else {
        FFI_ArrowSchema::try_from(field)
    }
}

pub trait SparkArrowConvert {
    /// Move Arrow Arrays to C data interface.
    fn move_to_spark(&self, field: &Field, array: i64, schema: i64) -> Result<(), ExecutionError>;
}

impl SparkArrowConvert for ArrayData {
    /// Move this ArrowData to pointers of Arrow C data interface.
    fn move_to_spark(&self, field: &Field, array: i64, schema: i64) -> Result<(), ExecutionError> {
        let array_ptr = array as *mut FFI_ArrowArray;
        let schema_ptr = schema as *mut FFI_ArrowSchema;

        let array_align = std::mem::align_of::<FFI_ArrowArray>();
        let schema_align = std::mem::align_of::<FFI_ArrowSchema>();
        let ffi_array = FFI_ArrowArray::new(self);
        let ffi_schema = ffi_schema_for_field(field)?;

        // Check if the pointer alignment is correct.
        if array_ptr.align_offset(array_align) != 0 || schema_ptr.align_offset(schema_align) != 0 {
            unsafe {
                std::ptr::write_unaligned(array_ptr, ffi_array);
                std::ptr::write_unaligned(schema_ptr, ffi_schema);
            }
        } else {
            // SAFETY: `array_ptr` and `schema_ptr` are aligned correctly.
            debug_assert_eq!(
                array_ptr.align_offset(array_align),
                0,
                "move_to_spark: array_ptr not aligned"
            );
            debug_assert_eq!(
                schema_ptr.align_offset(schema_align),
                0,
                "move_to_spark: schema_ptr not aligned"
            );
            unsafe {
                std::ptr::write(array_ptr, ffi_array);
                std::ptr::write(schema_ptr, ffi_schema);
            }
        }

        Ok(())
    }
}

pub use datafusion_comet_common::bytes_to_i128;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    #[test]
    fn test_ffi_schema_sanitizes_nul_name_and_preserves_metadata() {
        let field = Field::new("v\0tail", DataType::Int32, true).with_metadata(HashMap::from([(
            "ARROW:extension:name".to_string(),
            "arrow.parquet.variant".to_string(),
        )]));

        let ffi_schema = ffi_schema_for_field(&field).unwrap();
        let exported = Field::try_from(&ffi_schema).unwrap();

        assert_eq!(exported.name(), "v\u{fffd}tail");
        assert_eq!(exported.data_type(), field.data_type());
        assert_eq!(exported.is_nullable(), field.is_nullable());
        assert_eq!(exported.metadata(), field.metadata());
    }
}
