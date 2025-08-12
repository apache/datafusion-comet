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
use crate::errors::ExpressionError;
use crate::execution::operators::ExecutionError;
use arrow::datatypes::{ArrowNativeType, DataType};
use arrow::{
    array::ArrayData,
    error::ArrowError,
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};

impl From<ArrowError> for ExecutionError {
    fn from(error: ArrowError) -> ExecutionError {
        ExecutionError::ArrowError(error.to_string())
    }
}

impl From<ArrowError> for ExpressionError {
    fn from(error: ArrowError) -> ExpressionError {
        ExpressionError::ArrowError(error.to_string())
    }
}

impl From<ExpressionError> for ArrowError {
    fn from(error: ExpressionError) -> ArrowError {
        ArrowError::ComputeError(error.to_string())
    }
}

pub trait SparkArrowConvert {
    /// Build Arrow Arrays from C data interface passed from Spark.
    /// It accepts a tuple (ArrowArray address, ArrowSchema address).
    fn from_spark(addresses: (i64, i64)) -> Result<Self, ExecutionError>
    where
        Self: Sized;

    /// Move Arrow Arrays to C data interface.
    fn move_to_spark(&self, array: i64, schema: i64) -> Result<(), ExecutionError>;
}

impl SparkArrowConvert for ArrayData {
    fn from_spark(addresses: (i64, i64)) -> Result<Self, ExecutionError> {
        let (array_ptr, schema_ptr) = addresses;

        let array_ptr = array_ptr as *mut FFI_ArrowArray;
        let schema_ptr = schema_ptr as *mut FFI_ArrowSchema;

        if array_ptr.is_null() || schema_ptr.is_null() {
            return Err(ExecutionError::ArrowError(
                "At least one of passed pointers is null".to_string(),
            ));
        };

        // `ArrowArray` will convert raw pointers back to `Arc`. No worries
        // about memory leak.
        let mut ffi_array = unsafe {
            let array_data = std::ptr::replace(array_ptr, FFI_ArrowArray::empty());
            let schema_data = std::ptr::replace(schema_ptr, FFI_ArrowSchema::empty());

            from_ffi(array_data, &schema_data)?
        };

        // Align imported buffers from Java.
        ffi_array.align_buffers();

        Ok(ffi_array)
    }

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
            unsafe {
                std::ptr::write(array_ptr, FFI_ArrowArray::new(self));
                std::ptr::write(schema_ptr, FFI_ArrowSchema::try_from(self.data_type())?);
            }
        }

        Ok(())
    }
}

/// Converts a slice of bytes to i128. The bytes are serialized in big-endian order by
/// `BigInteger.toByteArray()` in Java.
pub fn bytes_to_i128(slice: &[u8]) -> i128 {
    let mut bytes = [0; 16];
    let mut i = 0;
    while i != 16 && i != slice.len() {
        bytes[i] = slice[slice.len() - 1 - i];
        i += 1;
    }

    // if the decimal is negative, we need to flip all the bits
    if (slice[0] as i8) < 0 {
        while i < 16 {
            bytes[i] = !bytes[i];
            i += 1;
        }
    }

    i128::from_le_bytes(bytes)
}

pub(crate) fn validate_array_data(array: &ArrayData) -> Result<(), ArrowError> {
    array.validate_full()
    // match array.data_type() {
    //     DataType::Utf8 | DataType::Binary => {
    //         let buffer = &array.buffers()[1];
    //         validate_offsets::<i32>(buffer.typed_data(), array.len())
    //     }
    //     DataType::LargeUtf8 | DataType::LargeBinary => {
    //         let buffer = &array.buffers()[1];
    //         validate_offsets::<i64>(buffer.typed_data(), array.len())
    //     }
    //     _ => Ok(()),
    // }
}

// fn validate_offsets<T: ArrowNativeType + num::Num + std::fmt::Display>(
//     offsets: &[T],
//     values_length: usize,
// ) -> Result<(), ArrowError> {
//     for i in 0..values_length - 1 {
//         let current_offset = offsets[i];
//         let next_offset = offsets[i + 1];
//         if (current_offset > next_offset) || current_offset < T::zero() || next_offset < T::zero() {
//             return Err(ArrowError::MemoryError(format!(
//                 "corrupt offsets [{i}] {current_offset}, [{}] {next_offset}",
//                 i + 1
//             )));
//         }
//     }
//     Ok(())
// }
