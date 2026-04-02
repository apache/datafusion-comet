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

use super::list::SparkUnsafeArray;
use super::map::SparkUnsafeMap;
use super::row::SparkUnsafeRow;
use datafusion_comet_common::bytes_to_i128;
use std::str::from_utf8;

const MAX_LONG_DIGITS: u8 = 18;

/// A common trait for Spark Unsafe classes that can be used to access the underlying data,
/// e.g., `UnsafeRow` and `UnsafeArray`. This defines a set of methods that can be used to
/// access the underlying data with index.
///
/// # Safety
///
/// Implementations must ensure that:
/// - `get_row_addr()` returns a valid pointer to JVM-allocated memory
/// - `get_element_offset()` returns a valid pointer within the row/array data region
/// - The memory layout follows Spark's UnsafeRow/UnsafeArray format
/// - The memory remains valid for the lifetime of the object (guaranteed by JVM ownership)
///
/// All accessor methods (get_boolean, get_int, etc.) use unsafe pointer operations but are
/// safe to call as long as:
/// - The index is within bounds (caller's responsibility)
/// - The object was constructed from valid Spark UnsafeRow/UnsafeArray data
///
/// # Alignment
///
/// Primitive accessor methods are implemented separately for each type because they have
/// different alignment guarantees:
/// - `SparkUnsafeRow`: All field offsets are 8-byte aligned (bitset width is a multiple of 8,
///   and each field slot is 8 bytes), so accessors use aligned `ptr::read()`.
/// - `SparkUnsafeArray`: The array base address may be unaligned when nested within a row's
///   variable-length region, so accessors use `ptr::read_unaligned()`.
pub trait SparkUnsafeObject {
    /// Returns the address of the row.
    fn get_row_addr(&self) -> i64;

    /// Returns the offset of the element at the given index.
    fn get_element_offset(&self, index: usize, element_size: usize) -> *const u8;

    fn get_boolean(&self, index: usize) -> bool;
    fn get_byte(&self, index: usize) -> i8;
    fn get_short(&self, index: usize) -> i16;
    fn get_int(&self, index: usize) -> i32;
    fn get_long(&self, index: usize) -> i64;
    fn get_float(&self, index: usize) -> f32;
    fn get_double(&self, index: usize) -> f64;
    fn get_date(&self, index: usize) -> i32;
    fn get_timestamp(&self, index: usize) -> i64;

    /// Returns the offset and length of the element at the given index.
    #[inline]
    fn get_offset_and_len(&self, index: usize) -> (i32, i32) {
        let offset_and_size = self.get_long(index);
        let offset = (offset_and_size >> 32) as i32;
        let len = offset_and_size as i32;
        (offset, len)
    }

    /// Returns string value at the given index of the object.
    fn get_string(&self, index: usize) -> &str {
        let (offset, len) = self.get_offset_and_len(index);
        let addr = self.get_row_addr() + offset as i64;
        // SAFETY: addr points to valid UTF-8 string data within the variable-length region.
        // Offset and length are read from the fixed-length portion of the row/array.
        debug_assert!(addr != 0, "get_string: null address at index {index}");
        debug_assert!(
            len >= 0,
            "get_string: negative length {len} at index {index}"
        );
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr as *const u8, len as usize) };

        from_utf8(slice).unwrap()
    }

    /// Returns binary value at the given index of the object.
    fn get_binary(&self, index: usize) -> &[u8] {
        let (offset, len) = self.get_offset_and_len(index);
        let addr = self.get_row_addr() + offset as i64;
        // SAFETY: addr points to valid binary data within the variable-length region.
        // Offset and length are read from the fixed-length portion of the row/array.
        debug_assert!(addr != 0, "get_binary: null address at index {index}");
        debug_assert!(
            len >= 0,
            "get_binary: negative length {len} at index {index}"
        );
        unsafe { std::slice::from_raw_parts(addr as *const u8, len as usize) }
    }

    /// Returns decimal value at the given index of the object.
    fn get_decimal(&self, index: usize, precision: u8) -> i128 {
        if precision <= MAX_LONG_DIGITS {
            self.get_long(index) as i128
        } else {
            let slice = self.get_binary(index);
            bytes_to_i128(slice)
        }
    }

    /// Returns struct value at the given index of the object.
    fn get_struct(&self, index: usize, num_fields: usize) -> SparkUnsafeRow {
        let (offset, len) = self.get_offset_and_len(index);
        let mut row = SparkUnsafeRow::new_with_num_fields(num_fields);
        row.point_to(self.get_row_addr() + offset as i64, len);

        row
    }

    /// Returns array value at the given index of the object.
    fn get_array(&self, index: usize) -> SparkUnsafeArray {
        let (offset, _) = self.get_offset_and_len(index);
        SparkUnsafeArray::new(self.get_row_addr() + offset as i64)
    }

    fn get_map(&self, index: usize) -> SparkUnsafeMap {
        let (offset, len) = self.get_offset_and_len(index);
        SparkUnsafeMap::new(self.get_row_addr() + offset as i64, len)
    }
}

/// Generates primitive accessor implementations for `SparkUnsafeObject`.
///
/// Uses `$read_method` to read typed values from raw pointers:
/// - `read` for aligned access (SparkUnsafeRow — all offsets are 8-byte aligned)
/// - `read_unaligned` for potentially unaligned access (SparkUnsafeArray)
macro_rules! impl_primitive_accessors {
    ($read_method:ident) => {
        #[inline]
        fn get_boolean(&self, index: usize) -> bool {
            let addr = self.get_element_offset(index, 1);
            debug_assert!(
                !addr.is_null(),
                "get_boolean: null pointer at index {index}"
            );
            // SAFETY: addr points to valid element data within the row/array region.
            unsafe { *addr != 0 }
        }

        #[inline]
        fn get_byte(&self, index: usize) -> i8 {
            let addr = self.get_element_offset(index, 1);
            debug_assert!(!addr.is_null(), "get_byte: null pointer at index {index}");
            // SAFETY: addr points to valid element data (1 byte) within the row/array region.
            unsafe { *(addr as *const i8) }
        }

        #[inline]
        fn get_short(&self, index: usize) -> i16 {
            let addr = self.get_element_offset(index, 2) as *const i16;
            debug_assert!(!addr.is_null(), "get_short: null pointer at index {index}");
            // SAFETY: addr points to valid element data (2 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_int(&self, index: usize) -> i32 {
            let addr = self.get_element_offset(index, 4) as *const i32;
            debug_assert!(!addr.is_null(), "get_int: null pointer at index {index}");
            // SAFETY: addr points to valid element data (4 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_long(&self, index: usize) -> i64 {
            let addr = self.get_element_offset(index, 8) as *const i64;
            debug_assert!(!addr.is_null(), "get_long: null pointer at index {index}");
            // SAFETY: addr points to valid element data (8 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_float(&self, index: usize) -> f32 {
            let addr = self.get_element_offset(index, 4) as *const f32;
            debug_assert!(!addr.is_null(), "get_float: null pointer at index {index}");
            // SAFETY: addr points to valid element data (4 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_double(&self, index: usize) -> f64 {
            let addr = self.get_element_offset(index, 8) as *const f64;
            debug_assert!(!addr.is_null(), "get_double: null pointer at index {index}");
            // SAFETY: addr points to valid element data (8 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_date(&self, index: usize) -> i32 {
            let addr = self.get_element_offset(index, 4) as *const i32;
            debug_assert!(!addr.is_null(), "get_date: null pointer at index {index}");
            // SAFETY: addr points to valid element data (4 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }

        #[inline]
        fn get_timestamp(&self, index: usize) -> i64 {
            let addr = self.get_element_offset(index, 8) as *const i64;
            debug_assert!(
                !addr.is_null(),
                "get_timestamp: null pointer at index {index}"
            );
            // SAFETY: addr points to valid element data (8 bytes) within the row/array region.
            unsafe { addr.$read_method() }
        }
    };
}
pub(crate) use impl_primitive_accessors;
