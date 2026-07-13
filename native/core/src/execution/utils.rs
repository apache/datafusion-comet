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
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
};

pub trait SparkArrowConvert {
    /// Move Arrow Arrays to C data interface.
    fn move_to_spark(&self, array: i64, schema: i64) -> Result<(), ExecutionError>;
}

impl SparkArrowConvert for ArrayData {
    /// Move this ArrowData to pointers of Arrow C data interface.
    fn move_to_spark(&self, array: i64, schema: i64) -> Result<(), ExecutionError> {
        let array_ptr = array as *mut FFI_ArrowArray;
        let schema_ptr = schema as *mut FFI_ArrowSchema;

        let array_align = std::mem::align_of::<FFI_ArrowArray>();
        let schema_align = std::mem::align_of::<FFI_ArrowSchema>();

        // Check if the pointer alignment is correct.
        if array_ptr.align_offset(array_align) != 0 || schema_ptr.align_offset(schema_align) != 0 {
            unsafe {
                std::ptr::write_unaligned(array_ptr, FFI_ArrowArray::new(self));
                std::ptr::write_unaligned(schema_ptr, FFI_ArrowSchema::try_from(self.data_type())?);
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
                std::ptr::write(array_ptr, FFI_ArrowArray::new(self));
                std::ptr::write(schema_ptr, FFI_ArrowSchema::try_from(self.data_type())?);
            }
        }

        Ok(())
    }
}

pub use datafusion_comet_common::bytes_to_i128;
