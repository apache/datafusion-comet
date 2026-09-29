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
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
};
use datafusion_comet_common::zero_offsets;

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
        // Arrow Java ignores `ArrowArray.offset` on import, so every level has to start at 0.
        let data = zero_offsets(self)?;
        let ffi_array = FFI_ArrowArray::new(&data);
        // Spark owns the top-level name and nullability. Preserve the existing anonymous schema
        // shape while carrying logical extension metadata from the RecordBatch field.
        let ffi_schema =
            FFI_ArrowSchema::try_from(self.data_type())?.with_metadata(field.metadata())?;

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
    use arrow::{
        array::{Array, ArrayRef, BooleanArray, Int32Array, StructArray},
        datatypes::DataType,
        ffi::from_ffi,
    };
    use std::{collections::HashMap, mem::MaybeUninit, sync::Arc};

    #[test]
    fn test_move_to_spark_preserves_field_metadata() {
        let field = Field::new("v", DataType::Int32, true).with_metadata(HashMap::from([(
            "ARROW:extension:name".to_string(),
            "example.logical-type".to_string(),
        )]));
        let data = Int32Array::from(vec![Some(1), None]).into_data();
        let mut ffi_array = MaybeUninit::<FFI_ArrowArray>::uninit();
        let mut ffi_schema = MaybeUninit::<FFI_ArrowSchema>::uninit();

        data.move_to_spark(
            &field,
            ffi_array.as_mut_ptr() as i64,
            ffi_schema.as_mut_ptr() as i64,
        )
        .unwrap();

        let ffi_array = unsafe { ffi_array.assume_init() };
        let ffi_schema = unsafe { ffi_schema.assume_init() };
        let exported = Field::try_from(&ffi_schema).unwrap();

        assert_eq!(exported.name(), "");
        assert_eq!(exported.data_type(), field.data_type());
        assert!(!exported.is_nullable());
        assert_eq!(exported.metadata(), field.metadata());

        drop(ffi_array);
        drop(ffi_schema);
    }

    /// Arrow Java ignores `ArrowArray.offset`, so a boolean child sliced along with its struct
    /// has to be exported at offset 0 (https://github.com/apache/datafusion-comet/issues/6288).
    #[test]
    fn test_move_to_spark_zeroes_nested_boolean_offsets() {
        let booleans: BooleanArray = (0..64)
            .map(|i| (i % 5 != 0).then_some(i % 3 == 0))
            .collect();
        let array = StructArray::from(vec![(
            Arc::new(Field::new("b", DataType::Boolean, true)),
            Arc::new(booleans) as ArrayRef,
        )])
        .slice(17, 20);
        let data = array.to_data();
        assert_eq!(data.child_data()[0].offset(), 17);
        let field = Field::new("s", data.data_type().clone(), true);
        let mut ffi_array = MaybeUninit::<FFI_ArrowArray>::uninit();
        let mut ffi_schema = MaybeUninit::<FFI_ArrowSchema>::uninit();

        data.move_to_spark(
            &field,
            ffi_array.as_mut_ptr() as i64,
            ffi_schema.as_mut_ptr() as i64,
        )
        .unwrap();

        let ffi_array = unsafe { ffi_array.assume_init() };
        let ffi_schema = unsafe { ffi_schema.assume_init() };
        assert_eq!(ffi_array.offset(), 0);
        assert_eq!(ffi_array.child(0).offset(), 0);
        let imported = unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap();
        assert_eq!(imported, data);
    }
}
