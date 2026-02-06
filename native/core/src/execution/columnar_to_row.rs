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

//! Native implementation of columnar to row conversion for Spark UnsafeRow format.
//!
//! This module converts Arrow columnar data to Spark's UnsafeRow format, which is used
//! for row-based operations in Spark. The conversion is done in native code for better
//! performance compared to the JVM implementation.
//!
//! # UnsafeRow Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │ Null Bitset: ((numFields + 63) / 64) * 8 bytes             │
//! ├─────────────────────────────────────────────────────────────┤
//! │ Fixed-width portion: 8 bytes per field                      │
//! │ - Primitives: value stored directly (in lowest bytes)       │
//! │ - Variable-length: (offset << 32) | length                  │
//! ├─────────────────────────────────────────────────────────────┤
//! │ Variable-length data: 8-byte aligned                        │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use crate::errors::{CometError, CometResult};
use arrow::array::types::{
    ArrowDictionaryKeyType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow::array::*;
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{ArrowNativeType, DataType, TimeUnit};
use std::sync::Arc;

/// Maximum digits for decimal that can fit in a long (8 bytes).
const MAX_LONG_DIGITS: u8 = 18;

/// Helper macro for downcasting arrays with consistent error messages.
macro_rules! downcast_array {
    ($array:expr, $array_type:ty) => {
        $array
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to downcast to {}, actual type: {:?}",
                    stringify!($array_type),
                    $array.data_type()
                ))
            })
    };
}

/// Macro to implement is_null for typed array enums.
/// Generates a complete match expression for all variants that have an array as first field.
macro_rules! impl_is_null {
    ($self:expr, $row_idx:expr, [$($variant:ident),+ $(,)?]) => {
        match $self {
            $(Self::$variant(arr, ..) => arr.is_null($row_idx),)+
        }
    };
    // Version with special handling for Null variant
    ($self:expr, $row_idx:expr, null_always_true, [$($variant:ident),+ $(,)?]) => {
        match $self {
            Self::Null => true,
            $(Self::$variant(arr, ..) => arr.is_null($row_idx),)+
        }
    };
}

/// Macro to generate TypedElements::from_array match arms for primitive types.
macro_rules! typed_elements_from_primitive {
    ($array:expr, $element_type:expr, $(($dt:pat, $variant:ident, $arr_type:ty)),+ $(,)?) => {
        match $element_type {
            $(
                $dt => {
                    if let Some(arr) = $array.as_any().downcast_ref::<$arr_type>() {
                        return TypedElements::$variant(arr);
                    }
                }
            )+
            _ => {}
        }
    };
}

/// Macro for write_column_fixed_width arms - handles downcast + loop pattern.
macro_rules! write_fixed_column_primitive {
    ($self:expr, $array:expr, $row_size:expr, $field_offset:expr, $num_rows:expr,
     $arr_type:ty, $to_i64:expr) => {{
        let arr = downcast_array!($array, $arr_type)?;
        for row_idx in 0..$num_rows {
            if !arr.is_null(row_idx) {
                let offset = row_idx * $row_size + $field_offset;
                let value: i64 = $to_i64(arr.value(row_idx));
                $self.buffer[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            }
        }
        Ok(())
    }};
}

/// Macro for get_field_value arms - handles downcast + value extraction.
macro_rules! get_field_value_primitive {
    ($array:expr, $row_idx:expr, $arr_type:ty, $to_i64:expr) => {{
        let arr = downcast_array!($array, $arr_type)?;
        Ok($to_i64(arr.value($row_idx)))
    }};
}

/// Macro for write_struct_to_buffer fixed-width field extraction.
macro_rules! extract_fixed_value {
    ($column:expr, $row_idx:expr, $(($dt:pat, $arr_type:ty, $to_i64:expr)),+ $(,)?) => {
        match $column.data_type() {
            $(
                $dt => {
                    let arr = downcast_array!($column, $arr_type)?;
                    Some($to_i64(arr.value($row_idx)))
                }
            )+
            _ => None,
        }
    };
}

/// Writes bytes to buffer with 8-byte alignment padding.
/// Returns the unpadded length.
#[inline]
fn write_bytes_padded(buffer: &mut Vec<u8>, bytes: &[u8]) -> usize {
    let len = bytes.len();
    buffer.extend_from_slice(bytes);
    let padding = round_up_to_8(len) - len;
    buffer.extend(std::iter::repeat_n(0u8, padding));
    len
}

/// Pre-downcast array reference to avoid type dispatch in inner loops.
/// This enum holds references to concrete array types, allowing direct access
/// without repeated downcast_ref calls.
enum TypedArray<'a> {
    Null,
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Date32(&'a Date32Array),
    TimestampMicro(&'a TimestampMicrosecondArray),
    Decimal128(&'a Decimal128Array, u8), // array + precision
    String(&'a StringArray),
    LargeString(&'a LargeStringArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    FixedSizeBinary(&'a FixedSizeBinaryArray),
    Struct(
        &'a StructArray,
        arrow::datatypes::Fields,
        Vec<TypedElements<'a>>,
    ),
    List(&'a ListArray, arrow::datatypes::FieldRef),
    LargeList(&'a LargeListArray, arrow::datatypes::FieldRef),
    Map(&'a MapArray, arrow::datatypes::FieldRef),
    Dictionary(&'a ArrayRef, DataType), // fallback for dictionary types
}

impl<'a> TypedArray<'a> {
    /// Pre-downcast an ArrayRef to a TypedArray.
    fn from_array(array: &'a ArrayRef) -> CometResult<Self> {
        let actual_type = array.data_type();
        match actual_type {
            DataType::Null => {
                // Verify the array is actually a NullArray, but we don't need to store the reference
                // since all values are null by definition
                downcast_array!(array, NullArray)?;
                Ok(TypedArray::Null)
            }
            DataType::Boolean => Ok(TypedArray::Boolean(downcast_array!(array, BooleanArray)?)),
            DataType::Int8 => Ok(TypedArray::Int8(downcast_array!(array, Int8Array)?)),
            DataType::Int16 => Ok(TypedArray::Int16(downcast_array!(array, Int16Array)?)),
            DataType::Int32 => Ok(TypedArray::Int32(downcast_array!(array, Int32Array)?)),
            DataType::Int64 => Ok(TypedArray::Int64(downcast_array!(array, Int64Array)?)),
            DataType::Float32 => Ok(TypedArray::Float32(downcast_array!(array, Float32Array)?)),
            DataType::Float64 => Ok(TypedArray::Float64(downcast_array!(array, Float64Array)?)),
            DataType::Date32 => Ok(TypedArray::Date32(downcast_array!(array, Date32Array)?)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(TypedArray::TimestampMicro(
                downcast_array!(array, TimestampMicrosecondArray)?,
            )),
            DataType::Decimal128(p, _) => Ok(TypedArray::Decimal128(
                downcast_array!(array, Decimal128Array)?,
                *p,
            )),
            DataType::Utf8 => Ok(TypedArray::String(downcast_array!(array, StringArray)?)),
            DataType::LargeUtf8 => Ok(TypedArray::LargeString(downcast_array!(
                array,
                LargeStringArray
            )?)),
            DataType::Binary => Ok(TypedArray::Binary(downcast_array!(array, BinaryArray)?)),
            DataType::LargeBinary => Ok(TypedArray::LargeBinary(downcast_array!(
                array,
                LargeBinaryArray
            )?)),
            DataType::FixedSizeBinary(_) => Ok(TypedArray::FixedSizeBinary(downcast_array!(
                array,
                FixedSizeBinaryArray
            )?)),
            DataType::Struct(fields) => {
                let struct_arr = downcast_array!(array, StructArray)?;
                // Pre-downcast all struct fields once
                let typed_fields: Vec<TypedElements> = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| {
                        TypedElements::from_array(struct_arr.column(idx), field.data_type())
                    })
                    .collect();
                Ok(TypedArray::Struct(struct_arr, fields.clone(), typed_fields))
            }
            DataType::List(field) => Ok(TypedArray::List(
                downcast_array!(array, ListArray)?,
                Arc::clone(field),
            )),
            DataType::LargeList(field) => Ok(TypedArray::LargeList(
                downcast_array!(array, LargeListArray)?,
                Arc::clone(field),
            )),
            DataType::Map(field, _) => Ok(TypedArray::Map(
                downcast_array!(array, MapArray)?,
                Arc::clone(field),
            )),
            DataType::Dictionary(_, _) => Ok(TypedArray::Dictionary(array, actual_type.clone())),
            _ => Err(CometError::Internal(format!(
                "Unsupported data type for pre-downcast: {:?}",
                actual_type
            ))),
        }
    }

    /// Check if the value at the given index is null.
    #[inline]
    fn is_null(&self, row_idx: usize) -> bool {
        impl_is_null!(
            self,
            row_idx,
            null_always_true,
            [
                Boolean,
                Int8,
                Int16,
                Int32,
                Int64,
                Float32,
                Float64,
                Date32,
                TimestampMicro,
                Decimal128,
                String,
                LargeString,
                Binary,
                LargeBinary,
                FixedSizeBinary,
                Struct,
                List,
                LargeList,
                Map,
                Dictionary
            ]
        )
    }

    /// Get the fixed-width value as i64 (for types that fit in 8 bytes).
    #[inline]
    fn get_fixed_value(&self, row_idx: usize) -> i64 {
        match self {
            TypedArray::Boolean(arr) => {
                if arr.value(row_idx) {
                    1i64
                } else {
                    0i64
                }
            }
            TypedArray::Int8(arr) => arr.value(row_idx) as i64,
            TypedArray::Int16(arr) => arr.value(row_idx) as i64,
            TypedArray::Int32(arr) => arr.value(row_idx) as i64,
            TypedArray::Int64(arr) => arr.value(row_idx),
            TypedArray::Float32(arr) => arr.value(row_idx).to_bits() as i64,
            TypedArray::Float64(arr) => arr.value(row_idx).to_bits() as i64,
            TypedArray::Date32(arr) => arr.value(row_idx) as i64,
            TypedArray::TimestampMicro(arr) => arr.value(row_idx),
            TypedArray::Decimal128(arr, precision) => {
                if *precision <= MAX_LONG_DIGITS {
                    arr.value(row_idx) as i64
                } else {
                    0 // Variable-length decimal, handled elsewhere
                }
            }
            // Variable-length types return 0, actual value written separately
            _ => 0,
        }
    }

    /// Check if this is a variable-length type.
    #[inline]
    fn is_variable_length(&self) -> bool {
        match self {
            TypedArray::Null
            | TypedArray::Boolean(_)
            | TypedArray::Int8(_)
            | TypedArray::Int16(_)
            | TypedArray::Int32(_)
            | TypedArray::Int64(_)
            | TypedArray::Float32(_)
            | TypedArray::Float64(_)
            | TypedArray::Date32(_)
            | TypedArray::TimestampMicro(_) => false,
            TypedArray::Decimal128(_, precision) => *precision > MAX_LONG_DIGITS,
            _ => true,
        }
    }

    /// Write variable-length data to buffer. Returns actual length (0 if not variable-length).
    fn write_variable_to_buffer(&self, buffer: &mut Vec<u8>, row_idx: usize) -> CometResult<usize> {
        match self {
            TypedArray::String(arr) => {
                Ok(write_bytes_padded(buffer, arr.value(row_idx).as_bytes()))
            }
            TypedArray::LargeString(arr) => {
                Ok(write_bytes_padded(buffer, arr.value(row_idx).as_bytes()))
            }
            TypedArray::Binary(arr) => Ok(write_bytes_padded(buffer, arr.value(row_idx))),
            TypedArray::LargeBinary(arr) => Ok(write_bytes_padded(buffer, arr.value(row_idx))),
            TypedArray::FixedSizeBinary(arr) => Ok(write_bytes_padded(buffer, arr.value(row_idx))),
            TypedArray::Decimal128(arr, precision) if *precision > MAX_LONG_DIGITS => {
                let bytes = i128_to_spark_decimal_bytes(arr.value(row_idx));
                Ok(write_bytes_padded(buffer, &bytes))
            }
            TypedArray::Struct(arr, fields, typed_fields) => {
                write_struct_to_buffer_typed(buffer, arr, row_idx, fields, typed_fields)
            }
            TypedArray::List(arr, field) => write_list_to_buffer(buffer, arr, row_idx, field),
            TypedArray::LargeList(arr, field) => {
                write_large_list_to_buffer(buffer, arr, row_idx, field)
            }
            TypedArray::Map(arr, field) => write_map_to_buffer(buffer, arr, row_idx, field),
            TypedArray::Dictionary(arr, schema_type) => {
                if let DataType::Dictionary(key_type, value_type) = schema_type {
                    write_dictionary_to_buffer(
                        buffer,
                        arr,
                        row_idx,
                        key_type.as_ref(),
                        value_type.as_ref(),
                    )
                } else {
                    Err(CometError::Internal(format!(
                        "Expected Dictionary type but got {:?}",
                        schema_type
                    )))
                }
            }
            _ => Ok(0), // Fixed-width types
        }
    }
}

/// Pre-downcast element array for list/array types.
/// This allows direct access to element values without per-row allocation.
enum TypedElements<'a> {
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Date32(&'a Date32Array),
    TimestampMicro(&'a TimestampMicrosecondArray),
    Decimal128(&'a Decimal128Array, u8),
    String(&'a StringArray),
    LargeString(&'a LargeStringArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    FixedSizeBinary(&'a FixedSizeBinaryArray),
    // For nested types, fall back to ArrayRef
    Other(&'a ArrayRef, DataType),
}

impl<'a> TypedElements<'a> {
    /// Create from an ArrayRef and element type.
    fn from_array(array: &'a ArrayRef, element_type: &DataType) -> Self {
        // Try primitive types first using macro
        typed_elements_from_primitive!(
            array,
            element_type,
            (DataType::Boolean, Boolean, BooleanArray),
            (DataType::Int8, Int8, Int8Array),
            (DataType::Int16, Int16, Int16Array),
            (DataType::Int32, Int32, Int32Array),
            (DataType::Int64, Int64, Int64Array),
            (DataType::Float32, Float32, Float32Array),
            (DataType::Float64, Float64, Float64Array),
            (DataType::Date32, Date32, Date32Array),
            (DataType::Utf8, String, StringArray),
            (DataType::LargeUtf8, LargeString, LargeStringArray),
            (DataType::Binary, Binary, BinaryArray),
            (DataType::LargeBinary, LargeBinary, LargeBinaryArray),
        );

        // Handle special cases that need extra processing
        match element_type {
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    return TypedElements::TimestampMicro(arr);
                }
            }
            DataType::Decimal128(p, _) => {
                if let Some(arr) = array.as_any().downcast_ref::<Decimal128Array>() {
                    return TypedElements::Decimal128(arr, *p);
                }
            }
            DataType::FixedSizeBinary(_) => {
                if let Some(arr) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    return TypedElements::FixedSizeBinary(arr);
                }
            }
            _ => {}
        }
        TypedElements::Other(array, element_type.clone())
    }

    /// Get element size for UnsafeArrayData format.
    fn element_size(&self) -> usize {
        match self {
            TypedElements::Boolean(_) => 1,
            TypedElements::Int8(_) => 1,
            TypedElements::Int16(_) => 2,
            TypedElements::Int32(_) | TypedElements::Date32(_) | TypedElements::Float32(_) => 4,
            TypedElements::Int64(_)
            | TypedElements::TimestampMicro(_)
            | TypedElements::Float64(_) => 8,
            TypedElements::Decimal128(_, p) if *p <= MAX_LONG_DIGITS => 8,
            _ => 8, // Variable-length uses 8 bytes for offset+length
        }
    }

    /// Check if this is a fixed-width primitive type that supports bulk copy.
    fn supports_bulk_copy(&self) -> bool {
        matches!(
            self,
            TypedElements::Int8(_)
                | TypedElements::Int16(_)
                | TypedElements::Int32(_)
                | TypedElements::Int64(_)
                | TypedElements::Float32(_)
                | TypedElements::Float64(_)
                | TypedElements::Date32(_)
                | TypedElements::TimestampMicro(_)
        )
    }

    /// Check if value at given index is null.
    #[inline]
    fn is_null_at(&self, idx: usize) -> bool {
        impl_is_null!(
            self,
            idx,
            [
                Boolean,
                Int8,
                Int16,
                Int32,
                Int64,
                Float32,
                Float64,
                Date32,
                TimestampMicro,
                Decimal128,
                String,
                LargeString,
                Binary,
                LargeBinary,
                FixedSizeBinary,
                Other
            ]
        )
    }

    /// Check if this is a fixed-width type (value fits in 8-byte slot).
    #[inline]
    fn is_fixed_width(&self) -> bool {
        match self {
            TypedElements::Boolean(_)
            | TypedElements::Int8(_)
            | TypedElements::Int16(_)
            | TypedElements::Int32(_)
            | TypedElements::Int64(_)
            | TypedElements::Float32(_)
            | TypedElements::Float64(_)
            | TypedElements::Date32(_)
            | TypedElements::TimestampMicro(_) => true,
            TypedElements::Decimal128(_, p) => *p <= MAX_LONG_DIGITS,
            _ => false,
        }
    }

    /// Get fixed-width value as i64 for the 8-byte field slot.
    #[inline]
    fn get_fixed_value(&self, idx: usize) -> i64 {
        match self {
            TypedElements::Boolean(arr) => {
                if arr.value(idx) {
                    1
                } else {
                    0
                }
            }
            TypedElements::Int8(arr) => arr.value(idx) as i64,
            TypedElements::Int16(arr) => arr.value(idx) as i64,
            TypedElements::Int32(arr) => arr.value(idx) as i64,
            TypedElements::Int64(arr) => arr.value(idx),
            TypedElements::Float32(arr) => (arr.value(idx).to_bits() as i32) as i64,
            TypedElements::Float64(arr) => arr.value(idx).to_bits() as i64,
            TypedElements::Date32(arr) => arr.value(idx) as i64,
            TypedElements::TimestampMicro(arr) => arr.value(idx),
            TypedElements::Decimal128(arr, _) => arr.value(idx) as i64,
            _ => 0, // Should not be called for variable-length types
        }
    }

    /// Write variable-length data to buffer. Returns length written (0 for fixed-width).
    fn write_variable_value(&self, buffer: &mut Vec<u8>, idx: usize) -> CometResult<usize> {
        match self {
            TypedElements::String(arr) => Ok(write_bytes_padded(buffer, arr.value(idx).as_bytes())),
            TypedElements::LargeString(arr) => {
                Ok(write_bytes_padded(buffer, arr.value(idx).as_bytes()))
            }
            TypedElements::Binary(arr) => Ok(write_bytes_padded(buffer, arr.value(idx))),
            TypedElements::LargeBinary(arr) => Ok(write_bytes_padded(buffer, arr.value(idx))),
            TypedElements::FixedSizeBinary(arr) => Ok(write_bytes_padded(buffer, arr.value(idx))),
            TypedElements::Decimal128(arr, precision) if *precision > MAX_LONG_DIGITS => {
                let bytes = i128_to_spark_decimal_bytes(arr.value(idx));
                Ok(write_bytes_padded(buffer, &bytes))
            }
            TypedElements::Other(arr, element_type) => {
                write_nested_variable_to_buffer(buffer, element_type, arr, idx)
            }
            _ => Ok(0), // Fixed-width types
        }
    }

    /// Write a range of elements to buffer in UnsafeArrayData format.
    /// Returns the total bytes written (including header).
    fn write_range_to_buffer(
        &self,
        buffer: &mut Vec<u8>,
        start_idx: usize,
        num_elements: usize,
    ) -> CometResult<usize> {
        let element_size = self.element_size();
        let array_start = buffer.len();
        let element_bitset_width = ColumnarToRowContext::calculate_bitset_width(num_elements);

        // Write number of elements
        buffer.extend_from_slice(&(num_elements as i64).to_le_bytes());

        // Reserve space for null bitset
        let null_bitset_start = buffer.len();
        buffer.resize(null_bitset_start + element_bitset_width, 0);

        // Reserve space for element values
        let elements_start = buffer.len();
        let elements_total_size = round_up_to_8(num_elements * element_size);
        buffer.resize(elements_start + elements_total_size, 0);

        // Try bulk copy for primitive types
        if self.supports_bulk_copy() {
            self.bulk_copy_range(
                buffer,
                null_bitset_start,
                elements_start,
                start_idx,
                num_elements,
            );
            return Ok(buffer.len() - array_start);
        }

        // Handle other types element by element
        self.write_elements_slow(
            buffer,
            array_start,
            null_bitset_start,
            elements_start,
            element_size,
            start_idx,
            num_elements,
        )
    }

    /// Bulk copy primitive values from a range.
    fn bulk_copy_range(
        &self,
        buffer: &mut [u8],
        null_bitset_start: usize,
        elements_start: usize,
        start_idx: usize,
        num_elements: usize,
    ) {
        macro_rules! bulk_copy_range {
            ($arr:expr, $elem_size:expr) => {{
                let values_slice = $arr.values();
                let byte_len = num_elements * $elem_size;
                let src_start = start_idx * $elem_size;
                let src_bytes = unsafe {
                    std::slice::from_raw_parts(
                        (values_slice.as_ptr() as *const u8).add(src_start),
                        byte_len,
                    )
                };
                buffer[elements_start..elements_start + byte_len].copy_from_slice(src_bytes);

                // Set null bits
                if $arr.null_count() > 0 {
                    for i in 0..num_elements {
                        if $arr.is_null(start_idx + i) {
                            let word_idx = i / 64;
                            let bit_idx = i % 64;
                            let word_offset = null_bitset_start + word_idx * 8;
                            let mut word = i64::from_le_bytes(
                                buffer[word_offset..word_offset + 8].try_into().unwrap(),
                            );
                            word |= 1i64 << bit_idx;
                            buffer[word_offset..word_offset + 8]
                                .copy_from_slice(&word.to_le_bytes());
                        }
                    }
                }
            }};
        }

        match self {
            TypedElements::Int8(arr) => bulk_copy_range!(arr, 1),
            TypedElements::Int16(arr) => bulk_copy_range!(arr, 2),
            TypedElements::Int32(arr) => bulk_copy_range!(arr, 4),
            TypedElements::Int64(arr) => bulk_copy_range!(arr, 8),
            TypedElements::Float32(arr) => bulk_copy_range!(arr, 4),
            TypedElements::Float64(arr) => bulk_copy_range!(arr, 8),
            TypedElements::Date32(arr) => bulk_copy_range!(arr, 4),
            TypedElements::TimestampMicro(arr) => bulk_copy_range!(arr, 8),
            _ => {} // Should not reach here due to supports_bulk_copy check
        }
    }

    /// Slow path for non-bulk-copyable types.
    #[allow(clippy::too_many_arguments)]
    fn write_elements_slow(
        &self,
        buffer: &mut Vec<u8>,
        array_start: usize,
        null_bitset_start: usize,
        elements_start: usize,
        element_size: usize,
        start_idx: usize,
        num_elements: usize,
    ) -> CometResult<usize> {
        match self {
            TypedElements::Boolean(arr) => {
                for i in 0..num_elements {
                    let src_idx = start_idx + i;
                    if arr.is_null(src_idx) {
                        set_null_bit(buffer, null_bitset_start, i);
                    } else {
                        buffer[elements_start + i] = if arr.value(src_idx) { 1 } else { 0 };
                    }
                }
            }
            TypedElements::Decimal128(arr, precision) if *precision <= MAX_LONG_DIGITS => {
                for i in 0..num_elements {
                    let src_idx = start_idx + i;
                    if arr.is_null(src_idx) {
                        set_null_bit(buffer, null_bitset_start, i);
                    } else {
                        let slot_offset = elements_start + i * 8;
                        let value = arr.value(src_idx) as i64;
                        buffer[slot_offset..slot_offset + 8].copy_from_slice(&value.to_le_bytes());
                    }
                }
            }
            TypedElements::Decimal128(arr, _) => {
                // Large decimal - variable length
                for i in 0..num_elements {
                    let src_idx = start_idx + i;
                    if arr.is_null(src_idx) {
                        set_null_bit(buffer, null_bitset_start, i);
                    } else {
                        let bytes = i128_to_spark_decimal_bytes(arr.value(src_idx));
                        let len = write_bytes_padded(buffer, &bytes);
                        let data_offset = buffer.len() - round_up_to_8(len) - array_start;
                        let offset_and_len = ((data_offset as i64) << 32) | (len as i64);
                        let slot_offset = elements_start + i * 8;
                        buffer[slot_offset..slot_offset + 8]
                            .copy_from_slice(&offset_and_len.to_le_bytes());
                    }
                }
            }
            TypedElements::String(arr) => {
                for i in 0..num_elements {
                    let src_idx = start_idx + i;
                    if arr.is_null(src_idx) {
                        set_null_bit(buffer, null_bitset_start, i);
                    } else {
                        let len = write_bytes_padded(buffer, arr.value(src_idx).as_bytes());
                        let data_offset = buffer.len() - round_up_to_8(len) - array_start;
                        let offset_and_len = ((data_offset as i64) << 32) | (len as i64);
                        let slot_offset = elements_start + i * 8;
                        buffer[slot_offset..slot_offset + 8]
                            .copy_from_slice(&offset_and_len.to_le_bytes());
                    }
                }
            }
            TypedElements::LargeString(arr) => {
                for i in 0..num_elements {
                    let src_idx = start_idx + i;
                    if arr.is_null(src_idx) {
                        set_null_bit(buffer, null_bitset_start, i);
                    } else {
                        let len = write_bytes_padded(buffer, arr.value(src_idx).as_bytes());
                        let data_offset = buffer.len() - round_up_to_8(len) - array_start;
                        let offset_and_len = ((data_offset as i64) << 32) | (len as i64);
                        let slot_offset = elements_start + i * 8;
                        buffer[slot_offset..slot_offset + 8]
                            .copy_from_slice(&offset_and_len.to_le_bytes());
                    }
                }
            }
            TypedElements::Binary(arr) => {
                for i in 0..num_elements {
                    let src_idx = start_idx + i;
                    if arr.is_null(src_idx) {
                        set_null_bit(buffer, null_bitset_start, i);
                    } else {
                        let len = write_bytes_padded(buffer, arr.value(src_idx));
                        let data_offset = buffer.len() - round_up_to_8(len) - array_start;
                        let offset_and_len = ((data_offset as i64) << 32) | (len as i64);
                        let slot_offset = elements_start + i * 8;
                        buffer[slot_offset..slot_offset + 8]
                            .copy_from_slice(&offset_and_len.to_le_bytes());
                    }
                }
            }
            TypedElements::LargeBinary(arr) => {
                for i in 0..num_elements {
                    let src_idx = start_idx + i;
                    if arr.is_null(src_idx) {
                        set_null_bit(buffer, null_bitset_start, i);
                    } else {
                        let len = write_bytes_padded(buffer, arr.value(src_idx));
                        let data_offset = buffer.len() - round_up_to_8(len) - array_start;
                        let offset_and_len = ((data_offset as i64) << 32) | (len as i64);
                        let slot_offset = elements_start + i * 8;
                        buffer[slot_offset..slot_offset + 8]
                            .copy_from_slice(&offset_and_len.to_le_bytes());
                    }
                }
            }
            TypedElements::Other(arr, element_type) => {
                // Fall back to old method for nested types
                for i in 0..num_elements {
                    let src_idx = start_idx + i;
                    if arr.is_null(src_idx) {
                        set_null_bit(buffer, null_bitset_start, i);
                    } else {
                        let slot_offset = elements_start + i * element_size;
                        let var_len =
                            write_nested_variable_to_buffer(buffer, element_type, arr, src_idx)?;

                        if var_len > 0 {
                            let padded_len = round_up_to_8(var_len);
                            let data_offset = buffer.len() - padded_len - array_start;
                            let offset_and_len = ((data_offset as i64) << 32) | (var_len as i64);
                            buffer[slot_offset..slot_offset + 8]
                                .copy_from_slice(&offset_and_len.to_le_bytes());
                        } else {
                            let value = get_field_value(element_type, arr, src_idx)?;
                            write_array_element(buffer, element_type, value, slot_offset);
                        }
                    }
                }
            }
            _ => {
                // Should not reach here - all cases covered above
            }
        }
        Ok(buffer.len() - array_start)
    }
}

/// Helper to set a null bit in the buffer.
#[inline]
fn set_null_bit(buffer: &mut [u8], null_bitset_start: usize, idx: usize) {
    let word_idx = idx / 64;
    let bit_idx = idx % 64;
    let word_offset = null_bitset_start + word_idx * 8;
    let mut word = i64::from_le_bytes(buffer[word_offset..word_offset + 8].try_into().unwrap());
    word |= 1i64 << bit_idx;
    buffer[word_offset..word_offset + 8].copy_from_slice(&word.to_le_bytes());
}

/// Check if a data type is fixed-width for UnsafeRow purposes.
/// Fixed-width types are stored directly in the 8-byte field slot.
#[inline]
fn is_fixed_width(data_type: &DataType) -> bool {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Microsecond, _) => true,
        DataType::Decimal128(p, _) => *p <= MAX_LONG_DIGITS,
        _ => false,
    }
}

/// Check if all columns in a schema are fixed-width.
#[inline]
fn is_all_fixed_width(schema: &[DataType]) -> bool {
    schema.iter().all(is_fixed_width)
}

/// Context for columnar to row conversion.
///
/// This struct maintains the output buffer and schema information needed for
/// converting Arrow columnar data to Spark UnsafeRow format. The buffer is
/// reused across multiple `convert` calls to minimize allocations.
pub struct ColumnarToRowContext {
    /// The Arrow data types for each column.
    schema: Vec<DataType>,
    /// The output buffer containing converted rows.
    /// Layout: [Row0][Row1]...[RowN] where each row is an UnsafeRow.
    buffer: Vec<u8>,
    /// Byte offset where each row starts in the buffer.
    offsets: Vec<i32>,
    /// Byte length of each row.
    lengths: Vec<i32>,
    /// Pre-calculated null bitset width in bytes.
    null_bitset_width: usize,
    /// Pre-calculated fixed-width portion size in bytes (null bitset + 8 bytes per field).
    fixed_width_size: usize,
    /// Maximum batch size for pre-allocation.
    _batch_size: usize,
    /// Whether all columns are fixed-width (enables fast path).
    all_fixed_width: bool,
}

impl ColumnarToRowContext {
    /// Creates a new ColumnarToRowContext with the given schema.
    ///
    /// # Arguments
    ///
    /// * `schema` - The Arrow data types for each column.
    /// * `batch_size` - Maximum number of rows expected per batch (for pre-allocation).
    pub fn new(schema: Vec<DataType>, batch_size: usize) -> Self {
        let num_fields = schema.len();
        let null_bitset_width = Self::calculate_bitset_width(num_fields);
        let fixed_width_size = null_bitset_width + num_fields * 8;
        let all_fixed_width = is_all_fixed_width(&schema);

        // Pre-allocate buffer for maximum batch size
        // For fixed-width schemas, we know exact size; otherwise estimate
        let estimated_row_size = if all_fixed_width {
            fixed_width_size
        } else {
            fixed_width_size + 64 // Conservative estimate for variable-length data
        };
        let initial_capacity = batch_size * estimated_row_size;

        Self {
            schema,
            buffer: Vec::with_capacity(initial_capacity),
            offsets: Vec::with_capacity(batch_size),
            lengths: Vec::with_capacity(batch_size),
            null_bitset_width,
            fixed_width_size,
            _batch_size: batch_size,
            all_fixed_width,
        }
    }

    /// Calculate the width of the null bitset in bytes.
    /// This matches Spark's `UnsafeRow.calculateBitSetWidthInBytes`.
    #[inline]
    pub const fn calculate_bitset_width(num_fields: usize) -> usize {
        num_fields.div_ceil(64) * 8
    }

    /// Round up to the nearest multiple of 8 for alignment.
    #[inline]
    const fn round_up_to_8(value: usize) -> usize {
        value.div_ceil(8) * 8
    }

    /// Converts Arrow arrays to Spark UnsafeRow format.
    ///
    /// # Arguments
    ///
    /// * `arrays` - The Arrow arrays to convert, one per column.
    /// * `num_rows` - The number of rows to convert.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - A pointer to the output buffer
    /// - A reference to the offsets array
    /// - A reference to the lengths array
    pub fn convert(
        &mut self,
        arrays: &[ArrayRef],
        num_rows: usize,
    ) -> CometResult<(*const u8, &[i32], &[i32])> {
        if arrays.len() != self.schema.len() {
            return Err(CometError::Internal(format!(
                "Column count mismatch: expected {}, got {}",
                self.schema.len(),
                arrays.len()
            )));
        }

        // Unpack any dictionary arrays to their underlying value type
        // This is needed because Parquet may return dictionary-encoded arrays
        // even when the schema expects a specific type like Decimal128
        let arrays: Vec<ArrayRef> = arrays
            .iter()
            .zip(self.schema.iter())
            .map(|(arr, schema_type)| Self::maybe_cast_to_schema_type(arr, schema_type))
            .collect::<CometResult<Vec<_>>>()?;
        let arrays = arrays.as_slice();

        // Clear previous data
        self.buffer.clear();
        self.offsets.clear();
        self.lengths.clear();

        // Reserve space for offsets and lengths
        self.offsets.reserve(num_rows);
        self.lengths.reserve(num_rows);

        // Use fast path for fixed-width-only schemas
        if self.all_fixed_width {
            return self.convert_fixed_width(arrays, num_rows);
        }

        // Pre-downcast all arrays to avoid type dispatch in inner loop
        let typed_arrays: Vec<TypedArray> = arrays
            .iter()
            .map(TypedArray::from_array)
            .collect::<CometResult<Vec<_>>>()?;

        // Pre-compute variable-length column indices (once per batch, not per row)
        let var_len_indices: Vec<usize> = typed_arrays
            .iter()
            .enumerate()
            .filter(|(_, arr)| arr.is_variable_length())
            .map(|(idx, _)| idx)
            .collect();

        // Process each row (general path for variable-length data)
        for row_idx in 0..num_rows {
            let row_start = self.buffer.len();
            self.offsets.push(row_start as i32);

            // Write fixed-width portion (null bitset + field values)
            self.write_row_typed(&typed_arrays, &var_len_indices, row_idx)?;

            let row_end = self.buffer.len();
            self.lengths.push((row_end - row_start) as i32);
        }

        Ok((self.buffer.as_ptr(), &self.offsets, &self.lengths))
    }

    /// Casts an array to match the expected schema type if needed.
    /// This handles cases where:
    /// 1. Parquet returns dictionary-encoded arrays but the schema expects a non-dictionary type
    /// 2. Parquet returns NullArray when all values are null, but the schema expects a typed array
    /// 3. Parquet returns Int32/Int64 for small-precision decimals but schema expects Decimal128
    fn maybe_cast_to_schema_type(
        array: &ArrayRef,
        schema_type: &DataType,
    ) -> CometResult<ArrayRef> {
        let actual_type = array.data_type();

        // If types already match, no cast needed
        if actual_type == schema_type {
            return Ok(Arc::clone(array));
        }

        match (actual_type, schema_type) {
            (DataType::Dictionary(_, _), schema)
                if !matches!(schema, DataType::Dictionary(_, _)) =>
            {
                // Unpack dictionary if the schema type is not a dictionary
                let options = CastOptions::default();
                cast_with_options(array, schema_type, &options).map_err(|e| {
                    CometError::Internal(format!(
                        "Failed to unpack dictionary array from {:?} to {:?}: {}",
                        actual_type, schema_type, e
                    ))
                })
            }
            (DataType::Null, _) => {
                // Cast NullArray to the expected schema type
                // This happens when all values in a column are null
                let options = CastOptions::default();
                cast_with_options(array, schema_type, &options).map_err(|e| {
                    CometError::Internal(format!(
                        "Failed to cast NullArray to {:?}: {}",
                        schema_type, e
                    ))
                })
            }
            (DataType::Int32, DataType::Decimal128(precision, scale)) => {
                // Parquet stores small-precision decimals as Int32 for efficiency.
                // When COMET_USE_DECIMAL_128 is false, BatchReader produces these types.
                // The Int32 value is already scaled (e.g., -1 means -0.01 for scale 2).
                // We need to reinterpret (not cast) to Decimal128 preserving the value.
                let int_array = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    CometError::Internal("Failed to downcast to Int32Array".to_string())
                })?;
                let decimal_array: Decimal128Array = int_array
                    .iter()
                    .map(|v| v.map(|x| x as i128))
                    .collect::<Decimal128Array>()
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| {
                        CometError::Internal(format!("Invalid decimal precision/scale: {}", e))
                    })?;
                Ok(Arc::new(decimal_array))
            }
            (DataType::Int64, DataType::Decimal128(precision, scale)) => {
                // Same as Int32 but for medium-precision decimals stored as Int64.
                let int_array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    CometError::Internal("Failed to downcast to Int64Array".to_string())
                })?;
                let decimal_array: Decimal128Array = int_array
                    .iter()
                    .map(|v| v.map(|x| x as i128))
                    .collect::<Decimal128Array>()
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| {
                        CometError::Internal(format!("Invalid decimal precision/scale: {}", e))
                    })?;
                Ok(Arc::new(decimal_array))
            }
            _ => Ok(Arc::clone(array)),
        }
    }

    /// Fast path for schemas with only fixed-width columns.
    /// Pre-allocates entire buffer and processes more efficiently.
    fn convert_fixed_width(
        &mut self,
        arrays: &[ArrayRef],
        num_rows: usize,
    ) -> CometResult<(*const u8, &[i32], &[i32])> {
        let row_size = self.fixed_width_size;
        let total_size = row_size * num_rows;
        let null_bitset_width = self.null_bitset_width;

        // Pre-allocate entire buffer at once (all zeros)
        self.buffer.resize(total_size, 0);

        // Pre-fill offsets and lengths (constant for fixed-width)
        let row_size_i32 = row_size as i32;
        for row_idx in 0..num_rows {
            self.offsets.push((row_idx * row_size) as i32);
            self.lengths.push(row_size_i32);
        }

        // Process column by column for better cache locality
        for (col_idx, array) in arrays.iter().enumerate() {
            let field_offset_in_row = null_bitset_width + col_idx * 8;

            // Write values for all rows in this column
            self.write_column_fixed_width(
                array,
                &self.schema[col_idx].clone(),
                col_idx,
                field_offset_in_row,
                row_size,
                num_rows,
            )?;
        }

        Ok((self.buffer.as_ptr(), &self.offsets, &self.lengths))
    }

    /// Write a fixed-width column's values for all rows.
    /// Processes column-by-column for better cache locality.
    fn write_column_fixed_width(
        &mut self,
        array: &ArrayRef,
        data_type: &DataType,
        col_idx: usize,
        field_offset_in_row: usize,
        row_size: usize,
        num_rows: usize,
    ) -> CometResult<()> {
        // Handle nulls first - set null bits
        if array.null_count() > 0 {
            let word_idx = col_idx / 64;
            let bit_idx = col_idx % 64;
            let bit_mask = 1i64 << bit_idx;

            for row_idx in 0..num_rows {
                if array.is_null(row_idx) {
                    let row_start = row_idx * row_size;
                    let word_offset = row_start + word_idx * 8;
                    let mut word = i64::from_le_bytes(
                        self.buffer[word_offset..word_offset + 8]
                            .try_into()
                            .unwrap(),
                    );
                    word |= bit_mask;
                    self.buffer[word_offset..word_offset + 8].copy_from_slice(&word.to_le_bytes());
                }
            }
        }

        // Write non-null values using type-specific fast paths
        match data_type {
            DataType::Boolean => {
                // Boolean is special: writes single byte, not 8-byte i64
                let arr = downcast_array!(array, BooleanArray)?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset] = if arr.value(row_idx) { 1 } else { 0 };
                    }
                }
                Ok(())
            }
            DataType::Int8 => write_fixed_column_primitive!(
                self,
                array,
                row_size,
                field_offset_in_row,
                num_rows,
                Int8Array,
                |v: i8| v as i64
            ),
            DataType::Int16 => write_fixed_column_primitive!(
                self,
                array,
                row_size,
                field_offset_in_row,
                num_rows,
                Int16Array,
                |v: i16| v as i64
            ),
            DataType::Int32 => write_fixed_column_primitive!(
                self,
                array,
                row_size,
                field_offset_in_row,
                num_rows,
                Int32Array,
                |v: i32| v as i64
            ),
            DataType::Int64 => write_fixed_column_primitive!(
                self,
                array,
                row_size,
                field_offset_in_row,
                num_rows,
                Int64Array,
                |v: i64| v
            ),
            DataType::Float32 => write_fixed_column_primitive!(
                self,
                array,
                row_size,
                field_offset_in_row,
                num_rows,
                Float32Array,
                |v: f32| v.to_bits() as i64
            ),
            DataType::Float64 => write_fixed_column_primitive!(
                self,
                array,
                row_size,
                field_offset_in_row,
                num_rows,
                Float64Array,
                |v: f64| v.to_bits() as i64
            ),
            DataType::Date32 => write_fixed_column_primitive!(
                self,
                array,
                row_size,
                field_offset_in_row,
                num_rows,
                Date32Array,
                |v: i32| v as i64
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => write_fixed_column_primitive!(
                self,
                array,
                row_size,
                field_offset_in_row,
                num_rows,
                TimestampMicrosecondArray,
                |v: i64| v
            ),
            DataType::Decimal128(precision, _) if *precision <= MAX_LONG_DIGITS => {
                write_fixed_column_primitive!(
                    self,
                    array,
                    row_size,
                    field_offset_in_row,
                    num_rows,
                    Decimal128Array,
                    |v: i128| v as i64
                )
            }
            _ => Err(CometError::Internal(format!(
                "Unexpected non-fixed-width type in fast path: {:?}",
                data_type
            ))),
        }
    }

    /// Writes a complete row using pre-downcast TypedArrays.
    /// This avoids type dispatch overhead in the inner loop.
    fn write_row_typed(
        &mut self,
        typed_arrays: &[TypedArray],
        var_len_indices: &[usize],
        row_idx: usize,
    ) -> CometResult<()> {
        let row_start = self.buffer.len();
        let null_bitset_width = self.null_bitset_width;
        let fixed_width_size = self.fixed_width_size;

        // Extend buffer for fixed-width portion
        self.buffer.resize(row_start + fixed_width_size, 0);

        // First pass: write null bits and fixed-width values
        for (col_idx, typed_arr) in typed_arrays.iter().enumerate() {
            let is_null = typed_arr.is_null(row_idx);

            if is_null {
                // Set null bit
                let word_idx = col_idx / 64;
                let bit_idx = col_idx % 64;
                let word_offset = row_start + word_idx * 8;

                let mut word = i64::from_le_bytes(
                    self.buffer[word_offset..word_offset + 8]
                        .try_into()
                        .unwrap(),
                );
                word |= 1i64 << bit_idx;
                self.buffer[word_offset..word_offset + 8].copy_from_slice(&word.to_le_bytes());
            } else if !typed_arr.is_variable_length() {
                // Write fixed-width field value (skip variable-length, they're handled in pass 2)
                let field_offset = row_start + null_bitset_width + col_idx * 8;
                let value = typed_arr.get_fixed_value(row_idx);
                self.buffer[field_offset..field_offset + 8].copy_from_slice(&value.to_le_bytes());
            }
        }

        // Second pass: write variable-length data (only iterate over var-len columns)
        for &col_idx in var_len_indices {
            let typed_arr = &typed_arrays[col_idx];
            if typed_arr.is_null(row_idx) {
                continue;
            }

            // Write variable-length data directly to buffer
            let actual_len = typed_arr.write_variable_to_buffer(&mut self.buffer, row_idx)?;
            if actual_len > 0 {
                // Calculate offset: buffer grew by padded_len, but we need offset to start of data
                let padded_len = Self::round_up_to_8(actual_len);
                let current_offset = self.buffer.len() - padded_len - row_start;

                // Update the field slot with (offset << 32) | length
                let offset_and_len = ((current_offset as i64) << 32) | (actual_len as i64);
                let field_offset = row_start + null_bitset_width + col_idx * 8;
                self.buffer[field_offset..field_offset + 8]
                    .copy_from_slice(&offset_and_len.to_le_bytes());
            }
        }

        Ok(())
    }

    /// Returns a pointer to the buffer.
    pub fn buffer_ptr(&self) -> *const u8 {
        self.buffer.as_ptr()
    }

    /// Returns the schema.
    pub fn schema(&self) -> &[DataType] {
        &self.schema
    }
}

/// Gets the fixed-width value for a field as i64.
#[inline]
fn get_field_value(data_type: &DataType, array: &ArrayRef, row_idx: usize) -> CometResult<i64> {
    // Use the actual array type for dispatching to handle type mismatches
    let actual_type = array.data_type();

    match actual_type {
        DataType::Boolean => {
            let arr = downcast_array!(array, BooleanArray)?;
            Ok(if arr.value(row_idx) { 1i64 } else { 0i64 })
        }
        DataType::Int8 => get_field_value_primitive!(array, row_idx, Int8Array, |v: i8| v as i64),
        DataType::Int16 => {
            get_field_value_primitive!(array, row_idx, Int16Array, |v: i16| v as i64)
        }
        DataType::Int32 => {
            get_field_value_primitive!(array, row_idx, Int32Array, |v: i32| v as i64)
        }
        DataType::Int64 => get_field_value_primitive!(array, row_idx, Int64Array, |v: i64| v),
        DataType::Float32 => {
            get_field_value_primitive!(array, row_idx, Float32Array, |v: f32| v.to_bits() as i64)
        }
        DataType::Float64 => {
            get_field_value_primitive!(array, row_idx, Float64Array, |v: f64| v.to_bits() as i64)
        }
        DataType::Date32 => {
            get_field_value_primitive!(array, row_idx, Date32Array, |v: i32| v as i64)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            get_field_value_primitive!(array, row_idx, TimestampMicrosecondArray, |v: i64| v)
        }
        DataType::Decimal128(precision, _) if *precision <= MAX_LONG_DIGITS => {
            get_field_value_primitive!(array, row_idx, Decimal128Array, |v: i128| v as i64)
        }
        // Variable-length types use placeholder (will be overwritten by get_variable_length_data)
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Decimal128(_, _)
        | DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::Map(_, _)
        | DataType::Dictionary(_, _) => Ok(0i64),
        _ => {
            // Check if the schema type is a known type that we should handle
            match data_type {
                DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Date32
                | DataType::Timestamp(TimeUnit::Microsecond, _)
                | DataType::Decimal128(_, _) => Err(CometError::Internal(format!(
                    "Type mismatch in get_field_value: schema expects {:?} but actual array type is {:?}",
                    data_type, actual_type
                ))),
                // If schema is also a variable-length type, return placeholder
                DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Struct(_)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::Map(_, _)
                | DataType::Dictionary(_, _) => Ok(0i64),
                _ => Err(CometError::Internal(format!(
                    "Unsupported data type for columnar to row conversion: schema={:?}, actual={:?}",
                    data_type, actual_type
                ))),
            }
        }
    }
}

/// Writes dictionary-encoded value directly to buffer.
#[inline]
fn write_dictionary_to_buffer(
    buffer: &mut Vec<u8>,
    array: &ArrayRef,
    row_idx: usize,
    key_type: &DataType,
    value_type: &DataType,
) -> CometResult<usize> {
    match key_type {
        DataType::Int8 => {
            write_dictionary_to_buffer_with_key::<Int8Type>(buffer, array, row_idx, value_type)
        }
        DataType::Int16 => {
            write_dictionary_to_buffer_with_key::<Int16Type>(buffer, array, row_idx, value_type)
        }
        DataType::Int32 => {
            write_dictionary_to_buffer_with_key::<Int32Type>(buffer, array, row_idx, value_type)
        }
        DataType::Int64 => {
            write_dictionary_to_buffer_with_key::<Int64Type>(buffer, array, row_idx, value_type)
        }
        DataType::UInt8 => {
            write_dictionary_to_buffer_with_key::<UInt8Type>(buffer, array, row_idx, value_type)
        }
        DataType::UInt16 => {
            write_dictionary_to_buffer_with_key::<UInt16Type>(buffer, array, row_idx, value_type)
        }
        DataType::UInt32 => {
            write_dictionary_to_buffer_with_key::<UInt32Type>(buffer, array, row_idx, value_type)
        }
        DataType::UInt64 => {
            write_dictionary_to_buffer_with_key::<UInt64Type>(buffer, array, row_idx, value_type)
        }
        _ => Err(CometError::Internal(format!(
            "Unsupported dictionary key type: {:?}",
            key_type
        ))),
    }
}

/// Writes dictionary value directly to buffer with specific key type.
#[inline]
fn write_dictionary_to_buffer_with_key<K: ArrowDictionaryKeyType>(
    buffer: &mut Vec<u8>,
    array: &ArrayRef,
    row_idx: usize,
    value_type: &DataType,
) -> CometResult<usize> {
    let dict_array = array
        .as_any()
        .downcast_ref::<DictionaryArray<K>>()
        .ok_or_else(|| {
            CometError::Internal(format!(
                "Failed to downcast to DictionaryArray<{:?}>",
                std::any::type_name::<K>()
            ))
        })?;

    let values = dict_array.values();
    let key_idx = dict_array.keys().value(row_idx).to_usize().ok_or_else(|| {
        CometError::Internal("Dictionary key index out of usize range".to_string())
    })?;

    match value_type {
        DataType::Utf8 => {
            let string_values = downcast_array!(values, StringArray)?;
            Ok(write_bytes_padded(
                buffer,
                string_values.value(key_idx).as_bytes(),
            ))
        }
        DataType::LargeUtf8 => {
            let string_values = downcast_array!(values, LargeStringArray)?;
            Ok(write_bytes_padded(
                buffer,
                string_values.value(key_idx).as_bytes(),
            ))
        }
        DataType::Binary => {
            let binary_values = downcast_array!(values, BinaryArray)?;
            Ok(write_bytes_padded(buffer, binary_values.value(key_idx)))
        }
        DataType::LargeBinary => {
            let binary_values = downcast_array!(values, LargeBinaryArray)?;
            Ok(write_bytes_padded(buffer, binary_values.value(key_idx)))
        }
        _ => Err(CometError::Internal(format!(
            "Unsupported dictionary value type for direct buffer write: {:?}",
            value_type
        ))),
    }
}

/// Converts i128 to Spark's big-endian decimal byte format.
fn i128_to_spark_decimal_bytes(value: i128) -> Vec<u8> {
    // Spark uses big-endian format for large decimals
    let bytes = value.to_be_bytes();

    // Find the minimum number of bytes needed (excluding leading sign-extension bytes)
    let is_negative = value < 0;
    let sign_byte = if is_negative { 0xFF } else { 0x00 };

    let mut start = 0;
    while start < 15 && bytes[start] == sign_byte {
        // Check if the next byte's sign bit matches
        let next_byte = bytes[start + 1];
        let has_correct_sign = if is_negative {
            (next_byte & 0x80) != 0
        } else {
            (next_byte & 0x80) == 0
        };
        if has_correct_sign {
            start += 1;
        } else {
            break;
        }
    }

    bytes[start..].to_vec()
}

/// Round up to the nearest multiple of 8 for alignment.
#[inline]
const fn round_up_to_8(value: usize) -> usize {
    value.div_ceil(8) * 8
}

/// Writes a primitive value with the correct size for UnsafeArrayData.
#[inline]
fn write_array_element(buffer: &mut [u8], data_type: &DataType, value: i64, offset: usize) {
    match data_type {
        DataType::Boolean => {
            buffer[offset] = if value != 0 { 1 } else { 0 };
        }
        DataType::Int8 => {
            buffer[offset] = value as u8;
        }
        DataType::Int16 => {
            buffer[offset..offset + 2].copy_from_slice(&(value as i16).to_le_bytes());
        }
        DataType::Int32 | DataType::Date32 => {
            buffer[offset..offset + 4].copy_from_slice(&(value as i32).to_le_bytes());
        }
        DataType::Float32 => {
            buffer[offset..offset + 4].copy_from_slice(&(value as u32).to_le_bytes());
        }
        // All 8-byte types
        _ => {
            buffer[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        }
    }
}

// =============================================================================
// Optimized direct-write functions for complex types
// These write directly to the output buffer to avoid intermediate allocations.
// =============================================================================

/// Writes a struct value directly to the buffer using pre-downcast typed fields.
/// Returns the unpadded length written.
///
/// This version uses pre-downcast TypedElements for each field, eliminating
/// per-row type dispatch overhead.
#[inline]
fn write_struct_to_buffer_typed(
    buffer: &mut Vec<u8>,
    _struct_array: &StructArray,
    row_idx: usize,
    _fields: &arrow::datatypes::Fields,
    typed_fields: &[TypedElements],
) -> CometResult<usize> {
    let num_fields = typed_fields.len();
    let nested_bitset_width = ColumnarToRowContext::calculate_bitset_width(num_fields);
    let nested_fixed_size = nested_bitset_width + num_fields * 8;

    // Remember where this struct starts in the buffer
    let struct_start = buffer.len();

    // Reserve space for fixed-width portion (zeros for null bits and field slots)
    buffer.resize(struct_start + nested_fixed_size, 0);

    // Write each field using pre-downcast types
    for (field_idx, typed_field) in typed_fields.iter().enumerate() {
        if typed_field.is_null_at(row_idx) {
            // Set null bit in nested struct
            set_null_bit(buffer, struct_start, field_idx);
        } else {
            let field_offset = struct_start + nested_bitset_width + field_idx * 8;

            if typed_field.is_fixed_width() {
                // Fixed-width field - use pre-downcast accessor
                let value = typed_field.get_fixed_value(row_idx);
                buffer[field_offset..field_offset + 8].copy_from_slice(&value.to_le_bytes());
            } else {
                // Variable-length field - use pre-downcast writer
                let var_len = typed_field.write_variable_value(buffer, row_idx)?;
                if var_len > 0 {
                    let padded_len = round_up_to_8(var_len);
                    let data_offset = buffer.len() - padded_len - struct_start;
                    let offset_and_len = ((data_offset as i64) << 32) | (var_len as i64);
                    buffer[field_offset..field_offset + 8]
                        .copy_from_slice(&offset_and_len.to_le_bytes());
                }
            }
        }
    }

    Ok(buffer.len() - struct_start)
}

/// Writes a struct value directly to the buffer.
/// Returns the unpadded length written.
///
/// Processes each field using inline type dispatch to avoid allocation overhead.
/// This is used for nested structs where we don't have pre-downcast fields.
fn write_struct_to_buffer(
    buffer: &mut Vec<u8>,
    struct_array: &StructArray,
    row_idx: usize,
    fields: &arrow::datatypes::Fields,
) -> CometResult<usize> {
    let num_fields = fields.len();
    let nested_bitset_width = ColumnarToRowContext::calculate_bitset_width(num_fields);
    let nested_fixed_size = nested_bitset_width + num_fields * 8;

    // Remember where this struct starts in the buffer
    let struct_start = buffer.len();

    // Reserve space for fixed-width portion (zeros for null bits and field slots)
    buffer.resize(struct_start + nested_fixed_size, 0);

    // Write each field with inline type handling (no allocation)
    for (field_idx, field) in fields.iter().enumerate() {
        let column = struct_array.column(field_idx);
        let data_type = field.data_type();

        if column.is_null(row_idx) {
            // Set null bit in nested struct
            set_null_bit(buffer, struct_start, field_idx);
        } else {
            let field_offset = struct_start + nested_bitset_width + field_idx * 8;

            // Inline type dispatch for fixed-width types (most common case)
            let value: Option<i64> = extract_fixed_value!(
                column,
                row_idx,
                (DataType::Boolean, BooleanArray, |v: bool| if v {
                    1i64
                } else {
                    0i64
                }),
                (DataType::Int8, Int8Array, |v: i8| v as i64),
                (DataType::Int16, Int16Array, |v: i16| v as i64),
                (DataType::Int32, Int32Array, |v: i32| v as i64),
                (DataType::Int64, Int64Array, |v: i64| v),
                (DataType::Float32, Float32Array, |v: f32| v.to_bits() as i64),
                (DataType::Float64, Float64Array, |v: f64| v.to_bits() as i64),
                (DataType::Date32, Date32Array, |v: i32| v as i64),
                (
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                    TimestampMicrosecondArray,
                    |v: i64| v
                ),
            );
            // Handle Decimal128 with precision guard separately
            let value: Option<i64> = match (value, data_type) {
                (Some(v), _) => Some(v),
                (None, DataType::Decimal128(p, _)) if *p <= MAX_LONG_DIGITS => {
                    let arr = downcast_array!(column, Decimal128Array)?;
                    Some(arr.value(row_idx) as i64)
                }
                _ => None,
            };

            if let Some(v) = value {
                // Fixed-width field
                buffer[field_offset..field_offset + 8].copy_from_slice(&v.to_le_bytes());
            } else {
                // Variable-length field
                let var_len = write_nested_variable_to_buffer(buffer, data_type, column, row_idx)?;
                if var_len > 0 {
                    let padded_len = round_up_to_8(var_len);
                    let data_offset = buffer.len() - padded_len - struct_start;
                    let offset_and_len = ((data_offset as i64) << 32) | (var_len as i64);
                    buffer[field_offset..field_offset + 8]
                        .copy_from_slice(&offset_and_len.to_le_bytes());
                }
            }
        }
    }

    Ok(buffer.len() - struct_start)
}

/// Writes a list value directly to the buffer in UnsafeArrayData format.
/// Returns the unpadded length written.
///
/// This uses offsets directly to avoid per-row ArrayRef allocation.
#[inline]
fn write_list_to_buffer(
    buffer: &mut Vec<u8>,
    list_array: &ListArray,
    row_idx: usize,
    element_field: &arrow::datatypes::FieldRef,
) -> CometResult<usize> {
    // Get offsets directly to avoid creating a sliced ArrayRef
    let offsets = list_array.value_offsets();
    let start_offset = offsets[row_idx] as usize;
    let end_offset = offsets[row_idx + 1] as usize;
    let num_elements = end_offset - start_offset;

    // Pre-downcast the element array once
    let element_array = list_array.values();
    let element_type = element_field.data_type();
    let typed_elements = TypedElements::from_array(element_array, element_type);

    // Write the range of elements
    typed_elements.write_range_to_buffer(buffer, start_offset, num_elements)
}

/// Writes a large list value directly to the buffer in UnsafeArrayData format.
/// Returns the unpadded length written.
///
/// This uses offsets directly to avoid per-row ArrayRef allocation.
#[inline]
fn write_large_list_to_buffer(
    buffer: &mut Vec<u8>,
    list_array: &LargeListArray,
    row_idx: usize,
    element_field: &arrow::datatypes::FieldRef,
) -> CometResult<usize> {
    // Get offsets directly to avoid creating a sliced ArrayRef
    let offsets = list_array.value_offsets();
    let start_offset = offsets[row_idx] as usize;
    let end_offset = offsets[row_idx + 1] as usize;
    let num_elements = end_offset - start_offset;

    // Pre-downcast the element array once
    let element_array = list_array.values();
    let element_type = element_field.data_type();
    let typed_elements = TypedElements::from_array(element_array, element_type);

    // Write the range of elements
    typed_elements.write_range_to_buffer(buffer, start_offset, num_elements)
}

/// Writes a map value directly to the buffer in UnsafeMapData format.
/// Returns the unpadded length written.
///
/// This uses offsets directly to avoid per-row ArrayRef allocation.
fn write_map_to_buffer(
    buffer: &mut Vec<u8>,
    map_array: &MapArray,
    row_idx: usize,
    entries_field: &arrow::datatypes::FieldRef,
) -> CometResult<usize> {
    // UnsafeMapData format:
    // [key array size: 8 bytes][key array data][value array data]
    let map_start = buffer.len();

    // Get offsets directly to avoid creating a sliced ArrayRef
    let offsets = map_array.value_offsets();
    let start_offset = offsets[row_idx] as usize;
    let end_offset = offsets[row_idx + 1] as usize;
    let num_entries = end_offset - start_offset;

    // Get keys and values from the underlying entries struct
    let entries_array = map_array.entries();
    let keys = entries_array.column(0);
    let values = entries_array.column(1);

    let (key_type, value_type) = if let DataType::Struct(fields) = entries_field.data_type() {
        (fields[0].data_type().clone(), fields[1].data_type().clone())
    } else {
        return Err(CometError::Internal(format!(
            "Map entries field is not a struct: {:?}",
            entries_field.data_type()
        )));
    };

    // Pre-downcast keys and values once
    let typed_keys = TypedElements::from_array(keys, &key_type);
    let typed_values = TypedElements::from_array(values, &value_type);

    // Placeholder for key array size
    let key_size_offset = buffer.len();
    buffer.extend_from_slice(&0i64.to_le_bytes());

    // Write key array using range
    let key_array_start = buffer.len();
    typed_keys.write_range_to_buffer(buffer, start_offset, num_entries)?;
    let key_array_size = (buffer.len() - key_array_start) as i64;
    buffer[key_size_offset..key_size_offset + 8].copy_from_slice(&key_array_size.to_le_bytes());

    // Write value array using range
    typed_values.write_range_to_buffer(buffer, start_offset, num_entries)?;

    Ok(buffer.len() - map_start)
}

/// Writes variable-length data for a nested field directly to buffer.
/// Used by struct, list, and map writers for their nested elements.
/// Returns the unpadded length written (0 if not variable-length).
fn write_nested_variable_to_buffer(
    buffer: &mut Vec<u8>,
    data_type: &DataType,
    array: &ArrayRef,
    row_idx: usize,
) -> CometResult<usize> {
    let actual_type = array.data_type();

    match actual_type {
        DataType::Utf8 => {
            let arr = downcast_array!(array, StringArray)?;
            Ok(write_bytes_padded(buffer, arr.value(row_idx).as_bytes()))
        }
        DataType::LargeUtf8 => {
            let arr = downcast_array!(array, LargeStringArray)?;
            Ok(write_bytes_padded(buffer, arr.value(row_idx).as_bytes()))
        }
        DataType::Binary => {
            let arr = downcast_array!(array, BinaryArray)?;
            Ok(write_bytes_padded(buffer, arr.value(row_idx)))
        }
        DataType::LargeBinary => {
            let arr = downcast_array!(array, LargeBinaryArray)?;
            Ok(write_bytes_padded(buffer, arr.value(row_idx)))
        }
        DataType::Decimal128(precision, _) if *precision > MAX_LONG_DIGITS => {
            let arr = downcast_array!(array, Decimal128Array)?;
            let bytes = i128_to_spark_decimal_bytes(arr.value(row_idx));
            Ok(write_bytes_padded(buffer, &bytes))
        }
        DataType::Struct(fields) => {
            let struct_array = downcast_array!(array, StructArray)?;
            write_struct_to_buffer(buffer, struct_array, row_idx, fields)
        }
        DataType::List(field) => {
            let list_array = downcast_array!(array, ListArray)?;
            write_list_to_buffer(buffer, list_array, row_idx, field)
        }
        DataType::LargeList(field) => {
            let list_array = downcast_array!(array, LargeListArray)?;
            write_large_list_to_buffer(buffer, list_array, row_idx, field)
        }
        DataType::Map(field, _) => {
            let map_array = downcast_array!(array, MapArray)?;
            write_map_to_buffer(buffer, map_array, row_idx, field)
        }
        DataType::Dictionary(key_type, value_type) => {
            write_dictionary_to_buffer(buffer, array, row_idx, key_type, value_type)
        }
        // Check if schema type expects variable-length but actual type doesn't match
        _ => match data_type {
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Map(_, _) => Err(CometError::Internal(format!(
                "Type mismatch in nested write: schema expects {:?} but actual array type is {:?}",
                data_type, actual_type
            ))),
            DataType::Decimal128(precision, _) if *precision > MAX_LONG_DIGITS => {
                Err(CometError::Internal(format!(
                    "Type mismatch for large decimal: schema expects {:?} but actual is {:?}",
                    data_type, actual_type
                )))
            }
            _ => Ok(0), // Not a variable-length type
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_bitset_width_calculation() {
        assert_eq!(ColumnarToRowContext::calculate_bitset_width(0), 0);
        assert_eq!(ColumnarToRowContext::calculate_bitset_width(1), 8);
        assert_eq!(ColumnarToRowContext::calculate_bitset_width(64), 8);
        assert_eq!(ColumnarToRowContext::calculate_bitset_width(65), 16);
        assert_eq!(ColumnarToRowContext::calculate_bitset_width(128), 16);
        assert_eq!(ColumnarToRowContext::calculate_bitset_width(129), 24);
    }

    #[test]
    fn test_round_up_to_8() {
        assert_eq!(ColumnarToRowContext::round_up_to_8(0), 0);
        assert_eq!(ColumnarToRowContext::round_up_to_8(1), 8);
        assert_eq!(ColumnarToRowContext::round_up_to_8(7), 8);
        assert_eq!(ColumnarToRowContext::round_up_to_8(8), 8);
        assert_eq!(ColumnarToRowContext::round_up_to_8(9), 16);
    }

    #[test]
    fn test_convert_int_array() {
        let schema = vec![DataType::Int32];
        let mut ctx = ColumnarToRowContext::new(schema, 100);

        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));
        let arrays = vec![array];

        let (ptr, offsets, lengths) = ctx.convert(&arrays, 4).unwrap();

        assert!(!ptr.is_null());
        assert_eq!(offsets.len(), 4);
        assert_eq!(lengths.len(), 4);

        // Each row should have: 8 bytes null bitset + 8 bytes for one field = 16 bytes
        for len in lengths {
            assert_eq!(*len, 16);
        }
    }

    #[test]
    fn test_convert_multiple_columns() {
        let schema = vec![DataType::Int32, DataType::Int64, DataType::Float64];
        let mut ctx = ColumnarToRowContext::new(schema, 100);

        let array1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let array2: ArrayRef = Arc::new(Int64Array::from(vec![100i64, 200, 300]));
        let array3: ArrayRef = Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3]));
        let arrays = vec![array1, array2, array3];

        let (ptr, offsets, lengths) = ctx.convert(&arrays, 3).unwrap();

        assert!(!ptr.is_null());
        assert_eq!(offsets.len(), 3);
        assert_eq!(lengths.len(), 3);

        // Each row should have: 8 bytes null bitset + 24 bytes for three fields = 32 bytes
        for len in lengths {
            assert_eq!(*len, 32);
        }
    }

    #[test]
    fn test_fixed_width_fast_path() {
        // Test that the fixed-width fast path produces correct results
        let schema = vec![DataType::Int32, DataType::Int64, DataType::Float64];
        let mut ctx = ColumnarToRowContext::new(schema.clone(), 100);

        // Verify that the context detects this as all fixed-width
        assert!(
            ctx.all_fixed_width,
            "Schema should be detected as all fixed-width"
        );

        let array1: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let array2: ArrayRef = Arc::new(Int64Array::from(vec![Some(100i64), Some(200), None]));
        let array3: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5]));
        let arrays = vec![array1, array2, array3];

        let (ptr, offsets, lengths) = ctx.convert(&arrays, 3).unwrap();

        assert!(!ptr.is_null());
        assert_eq!(offsets.len(), 3);
        assert_eq!(lengths.len(), 3);

        // Each row: 8 bytes null bitset + 24 bytes for three fields = 32 bytes
        let row_size = 32;
        for (i, len) in lengths.iter().enumerate() {
            assert_eq!(
                *len, row_size as i32,
                "Row {} should be {} bytes",
                i, row_size
            );
        }

        // Verify the actual data
        let buffer = unsafe { std::slice::from_raw_parts(ptr, row_size * 3) };

        // Row 0: int32=1 (not null), int64=100 (not null), float64=1.5 (not null)
        let null_bitset_0 = i64::from_le_bytes(buffer[0..8].try_into().unwrap());
        assert_eq!(null_bitset_0, 0, "Row 0 should have no nulls");
        let val0_0 = i64::from_le_bytes(buffer[8..16].try_into().unwrap());
        assert_eq!(val0_0, 1, "Row 0, col 0 should be 1");
        let val0_1 = i64::from_le_bytes(buffer[16..24].try_into().unwrap());
        assert_eq!(val0_1, 100, "Row 0, col 1 should be 100");
        let val0_2 = f64::from_bits(u64::from_le_bytes(buffer[24..32].try_into().unwrap()));
        assert!((val0_2 - 1.5).abs() < 0.001, "Row 0, col 2 should be 1.5");

        // Row 1: int32=null, int64=200 (not null), float64=2.5 (not null)
        let null_bitset_1 = i64::from_le_bytes(buffer[32..40].try_into().unwrap());
        assert_eq!(null_bitset_1 & 1, 1, "Row 1, col 0 should be null");
        let val1_1 = i64::from_le_bytes(buffer[48..56].try_into().unwrap());
        assert_eq!(val1_1, 200, "Row 1, col 1 should be 200");

        // Row 2: int32=3 (not null), int64=null, float64=3.5 (not null)
        let null_bitset_2 = i64::from_le_bytes(buffer[64..72].try_into().unwrap());
        assert_eq!(null_bitset_2 & 2, 2, "Row 2, col 1 should be null");
        let val2_0 = i64::from_le_bytes(buffer[72..80].try_into().unwrap());
        assert_eq!(val2_0, 3, "Row 2, col 0 should be 3");
    }

    #[test]
    fn test_mixed_schema_uses_general_path() {
        // Test that schemas with variable-length types use the general path
        let schema = vec![DataType::Int32, DataType::Utf8];
        let ctx = ColumnarToRowContext::new(schema, 100);

        // Should NOT be detected as all fixed-width
        assert!(
            !ctx.all_fixed_width,
            "Schema with Utf8 should not be all fixed-width"
        );
    }

    #[test]
    fn test_convert_string_array() {
        let schema = vec![DataType::Utf8];
        let mut ctx = ColumnarToRowContext::new(schema, 100);

        let array: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));
        let arrays = vec![array];

        let (ptr, offsets, lengths) = ctx.convert(&arrays, 2).unwrap();

        assert!(!ptr.is_null());
        assert_eq!(offsets.len(), 2);
        assert_eq!(lengths.len(), 2);

        // Row 0: 8 (bitset) + 8 (field slot) + 8 (aligned "hello") = 24
        // Row 1: 8 (bitset) + 8 (field slot) + 8 (aligned "world") = 24
        assert_eq!(lengths[0], 24);
        assert_eq!(lengths[1], 24);
    }

    #[test]
    fn test_i128_to_spark_decimal_bytes() {
        // Test positive number
        let bytes = i128_to_spark_decimal_bytes(12345);
        assert!(bytes.len() <= 16);

        // Test negative number
        let bytes = i128_to_spark_decimal_bytes(-12345);
        assert!(bytes.len() <= 16);

        // Test zero
        let bytes = i128_to_spark_decimal_bytes(0);
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_list_data_conversion() {
        use arrow::datatypes::Field;

        // Create a list with elements [0, 1, 2, 3, 4]
        let values = Int32Array::from(vec![0, 1, 2, 3, 4]);
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0, 5].into());

        let list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_array = ListArray::new(Arc::clone(&list_field), offsets, Arc::new(values), None);

        // Convert the list for row 0 using the new direct-write function
        let mut buffer = Vec::new();
        write_list_to_buffer(&mut buffer, &list_array, 0, &list_field).expect("conversion failed");
        let result = &buffer;

        // UnsafeArrayData format for Int32:
        // [0..8]: numElements = 5
        // [8..16]: null bitset (8 bytes for up to 64 elements)
        // [16..20]: element 0 (4 bytes for Int32)
        // [20..24]: element 1 (4 bytes for Int32)
        // ... (total 20 bytes for 5 elements, rounded up to 24 for 8-byte alignment)

        let num_elements = i64::from_le_bytes(result[0..8].try_into().unwrap());
        assert_eq!(num_elements, 5, "should have 5 elements");

        let bitset_width = ColumnarToRowContext::calculate_bitset_width(5);
        assert_eq!(bitset_width, 8);

        // Read each element value (Int32 uses 4 bytes per element)
        let element_size = 4; // Int32
        for i in 0..5 {
            let slot_offset = 8 + bitset_width + i * element_size;
            let value =
                i32::from_le_bytes(result[slot_offset..slot_offset + 4].try_into().unwrap());
            assert_eq!(value, i as i32, "element {} should be {}", i, i);
        }
    }

    #[test]
    fn test_list_data_conversion_multiple_rows() {
        use arrow::datatypes::Field;

        // Create multiple lists:
        // Row 0: [0]
        // Row 1: [0, 1]
        // Row 2: [0, 1, 2]
        let values = Int32Array::from(vec![
            0, // row 0
            0, 1, // row 1
            0, 1, 2, // row 2
        ]);
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0, 1, 3, 6].into());

        let list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_array = ListArray::new(Arc::clone(&list_field), offsets, Arc::new(values), None);

        // Test row 1 which has elements [0, 1]
        let mut buffer = Vec::new();
        write_list_to_buffer(&mut buffer, &list_array, 1, &list_field).expect("conversion failed");
        let result = &buffer;

        let num_elements = i64::from_le_bytes(result[0..8].try_into().unwrap());
        assert_eq!(num_elements, 2, "row 1 should have 2 elements");

        // Int32 uses 4 bytes per element
        let element_size = 4;
        let bitset_width = ColumnarToRowContext::calculate_bitset_width(2);
        let slot0_offset = 8 + bitset_width;
        let slot1_offset = slot0_offset + element_size;

        let value0 = i32::from_le_bytes(result[slot0_offset..slot0_offset + 4].try_into().unwrap());
        let value1 = i32::from_le_bytes(result[slot1_offset..slot1_offset + 4].try_into().unwrap());

        assert_eq!(value0, 0, "row 1, element 0 should be 0");
        assert_eq!(value1, 1, "row 1, element 1 should be 1");

        // Also verify that list slicing is working correctly
        let list_values = list_array.value(1);
        assert_eq!(list_values.len(), 2, "row 1 should have 2 elements");
        let int_arr = list_values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_arr.value(0), 0, "row 1 value[0] via Arrow should be 0");
        assert_eq!(int_arr.value(1), 1, "row 1 value[1] via Arrow should be 1");
    }

    #[test]
    fn test_map_data_conversion() {
        use arrow::datatypes::{Field, Fields};

        // Create a map with 3 entries: {"key_0": 0, "key_1": 10, "key_2": 20}
        let keys = StringArray::from(vec!["key_0", "key_1", "key_2"]);
        let values = Int32Array::from(vec![0, 10, 20]);

        let entries_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        );

        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, true)),
                Arc::new(values) as ArrayRef,
            ),
        ]);

        let map_array = MapArray::new(
            Arc::new(entries_field.clone()),
            arrow::buffer::OffsetBuffer::new(vec![0, 3].into()),
            entries,
            None,
            false,
        );

        // Convert the map for row 0
        let mut buffer = Vec::new();
        write_map_to_buffer(&mut buffer, &map_array, 0, &Arc::new(entries_field))
            .expect("conversion failed");
        let result = &buffer;

        // Verify the structure:
        // - [0..8]: key array size
        // - [8..key_end]: key array data (UnsafeArrayData format)
        // - [key_end..]: value array data (UnsafeArrayData format)

        let key_array_size = i64::from_le_bytes(result[0..8].try_into().unwrap());
        assert!(key_array_size > 0, "key array size should be positive");

        let value_array_start = (8 + key_array_size) as usize;
        assert!(
            value_array_start < result.len(),
            "value array should start within buffer"
        );

        // Read value array
        let value_num_elements = i64::from_le_bytes(
            result[value_array_start..value_array_start + 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(value_num_elements, 3, "should have 3 values");

        // Value array layout for Int32 (4 bytes per element):
        // [0..8]: numElements = 3
        // [8..16]: null bitset (8 bytes for up to 64 elements)
        // [16..20]: element 0 (4 bytes)
        // [20..24]: element 1 (4 bytes)
        // [24..28]: element 2 (4 bytes)
        let value_bitset_width = ColumnarToRowContext::calculate_bitset_width(3);
        assert_eq!(value_bitset_width, 8);

        let element_size = 4; // Int32
        let slot0_offset = value_array_start + 8 + value_bitset_width;
        let slot1_offset = slot0_offset + element_size;
        let slot2_offset = slot1_offset + element_size;

        let value0 = i32::from_le_bytes(result[slot0_offset..slot0_offset + 4].try_into().unwrap());
        let value1 = i32::from_le_bytes(result[slot1_offset..slot1_offset + 4].try_into().unwrap());
        let value2 = i32::from_le_bytes(result[slot2_offset..slot2_offset + 4].try_into().unwrap());

        assert_eq!(value0, 0, "first value should be 0");
        assert_eq!(value1, 10, "second value should be 10");
        assert_eq!(value2, 20, "third value should be 20");
    }

    #[test]
    fn test_map_data_conversion_multiple_rows() {
        use arrow::datatypes::{Field, Fields};

        // Create multiple maps:
        // Row 0: {"key_0": 0}
        // Row 1: {"key_0": 0, "key_1": 10}
        // Row 2: {"key_0": 0, "key_1": 10, "key_2": 20}
        // All entries are concatenated in the underlying arrays
        let keys = StringArray::from(vec![
            "key_0", // row 0
            "key_0", "key_1", // row 1
            "key_0", "key_1", "key_2", // row 2
        ]);
        let values = Int32Array::from(vec![
            0, // row 0
            0, 10, // row 1
            0, 10, 20, // row 2
        ]);

        let entries_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        );

        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, true)),
                Arc::new(values) as ArrayRef,
            ),
        ]);

        // Offsets: row 0 has 1 entry, row 1 has 2 entries, row 2 has 3 entries
        let map_array = MapArray::new(
            Arc::new(entries_field.clone()),
            arrow::buffer::OffsetBuffer::new(vec![0, 1, 3, 6].into()),
            entries,
            None,
            false,
        );

        // Test row 1 which has 2 entries
        let mut buffer = Vec::new();
        write_map_to_buffer(&mut buffer, &map_array, 1, &Arc::new(entries_field.clone()))
            .expect("conversion failed");
        let result = &buffer;

        let key_array_size = i64::from_le_bytes(result[0..8].try_into().unwrap());
        let value_array_start = (8 + key_array_size) as usize;

        let value_num_elements = i64::from_le_bytes(
            result[value_array_start..value_array_start + 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(value_num_elements, 2, "row 1 should have 2 values");

        // Int32 uses 4 bytes per element
        let element_size = 4;
        let value_bitset_width = ColumnarToRowContext::calculate_bitset_width(2);
        let slot0_offset = value_array_start + 8 + value_bitset_width;
        let slot1_offset = slot0_offset + element_size;

        let value0 = i32::from_le_bytes(result[slot0_offset..slot0_offset + 4].try_into().unwrap());
        let value1 = i32::from_le_bytes(result[slot1_offset..slot1_offset + 4].try_into().unwrap());

        assert_eq!(value0, 0, "row 1, first value should be 0");
        assert_eq!(value1, 10, "row 1, second value should be 10");

        // Also verify that entries slicing is working correctly
        let entries_row1 = map_array.value(1);
        assert_eq!(entries_row1.len(), 2, "row 1 should have 2 entries");

        let entries_values = entries_row1.column(1);
        assert_eq!(
            entries_values.len(),
            2,
            "row 1 values should have 2 elements"
        );

        // Check the actual values from the sliced array
        let values_arr = entries_values
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            values_arr.value(0),
            0,
            "row 1 value[0] via Arrow should be 0"
        );
        assert_eq!(
            values_arr.value(1),
            10,
            "row 1 value[1] via Arrow should be 10"
        );
    }

    /// Test map conversion with a sliced MapArray to simulate FFI import behavior.
    /// When data comes from FFI, the MapArray might be a slice of a larger array,
    /// and the entries' child arrays might have offsets that don't start at 0.
    #[test]
    fn test_map_data_conversion_sliced_maparray() {
        use arrow::datatypes::{Field, Fields};

        // Create multiple maps (same as above)
        let keys = StringArray::from(vec![
            "key_0", // row 0
            "key_0", "key_1", // row 1
            "key_0", "key_1", "key_2", // row 2
        ]);
        let values = Int32Array::from(vec![
            0, // row 0
            0, 10, // row 1
            0, 10, 20, // row 2
        ]);

        let entries_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        );

        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, true)),
                Arc::new(values) as ArrayRef,
            ),
        ]);

        let map_array = MapArray::new(
            Arc::new(entries_field.clone()),
            arrow::buffer::OffsetBuffer::new(vec![0, 1, 3, 6].into()),
            entries,
            None,
            false,
        );

        // Slice the MapArray to skip row 0 - this simulates what might happen with FFI
        let sliced_map = map_array.slice(1, 2);
        let sliced_map_array = sliced_map.as_any().downcast_ref::<MapArray>().unwrap();

        // Now test row 0 of the sliced array (which is row 1 of the original)
        let mut buffer = Vec::new();
        write_map_to_buffer(
            &mut buffer,
            sliced_map_array,
            0,
            &Arc::new(entries_field.clone()),
        )
        .expect("conversion failed");
        let result = &buffer;

        let key_array_size = i64::from_le_bytes(result[0..8].try_into().unwrap());
        let value_array_start = (8 + key_array_size) as usize;

        let value_num_elements = i64::from_le_bytes(
            result[value_array_start..value_array_start + 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(value_num_elements, 2, "sliced row 0 should have 2 values");

        let value_bitset_width = ColumnarToRowContext::calculate_bitset_width(2);
        let slot0_offset = value_array_start + 8 + value_bitset_width;
        let slot1_offset = slot0_offset + 4; // Int32 uses 4 bytes

        let value0 = i32::from_le_bytes(result[slot0_offset..slot0_offset + 4].try_into().unwrap());
        let value1 = i32::from_le_bytes(result[slot1_offset..slot1_offset + 4].try_into().unwrap());

        assert_eq!(value0, 0, "sliced row 0, first value should be 0");
        assert_eq!(value1, 10, "sliced row 0, second value should be 10");
    }

    #[test]
    fn test_large_list_data_conversion() {
        use arrow::datatypes::Field;

        // Create a large list with elements [0, 1, 2, 3, 4]
        // LargeListArray uses i64 offsets instead of i32
        let values = Int32Array::from(vec![0, 1, 2, 3, 4]);
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i64, 5].into());

        let list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_array =
            LargeListArray::new(Arc::clone(&list_field), offsets, Arc::new(values), None);

        // Convert the list for row 0
        let mut buffer = Vec::new();
        write_large_list_to_buffer(&mut buffer, &list_array, 0, &list_field)
            .expect("conversion failed");
        let result = &buffer;

        // UnsafeArrayData format for Int32:
        // [0..8]: numElements = 5
        // [8..16]: null bitset (8 bytes for up to 64 elements)
        // [16..20]: element 0 (4 bytes for Int32)
        // ... (total 20 bytes for 5 elements, rounded up to 24 for 8-byte alignment)

        let num_elements = i64::from_le_bytes(result[0..8].try_into().unwrap());
        assert_eq!(num_elements, 5, "should have 5 elements");

        let bitset_width = ColumnarToRowContext::calculate_bitset_width(5);
        assert_eq!(bitset_width, 8);

        // Read each element value (Int32 uses 4 bytes per element)
        let element_size = 4; // Int32
        for i in 0..5 {
            let slot_offset = 8 + bitset_width + i * element_size;
            let value =
                i32::from_le_bytes(result[slot_offset..slot_offset + 4].try_into().unwrap());
            assert_eq!(value, i as i32, "element {} should be {}", i, i);
        }
    }

    #[test]
    fn test_convert_fixed_size_binary_array() {
        // FixedSizeBinary(3) - each value is exactly 3 bytes
        let schema = vec![DataType::FixedSizeBinary(3)];
        let mut ctx = ColumnarToRowContext::new(schema, 100);

        let array: ArrayRef = Arc::new(FixedSizeBinaryArray::from(vec![
            Some(&[1u8, 2, 3][..]),
            Some(&[4u8, 5, 6][..]),
            None, // Test null handling
        ]));
        let arrays = vec![array];

        let (ptr, offsets, lengths) = ctx.convert(&arrays, 3).unwrap();

        assert!(!ptr.is_null());
        assert_eq!(offsets.len(), 3);
        assert_eq!(lengths.len(), 3);

        // Row 0: 8 (bitset) + 8 (field slot) + 8 (aligned 3-byte data) = 24
        // Row 1: 8 (bitset) + 8 (field slot) + 8 (aligned 3-byte data) = 24
        // Row 2: 8 (bitset) + 8 (field slot) = 16 (null, no variable data)
        assert_eq!(lengths[0], 24);
        assert_eq!(lengths[1], 24);
        assert_eq!(lengths[2], 16);

        // Verify the data is correct for non-null rows
        unsafe {
            let row0 =
                std::slice::from_raw_parts(ptr.add(offsets[0] as usize), lengths[0] as usize);
            // Variable data starts at offset 16 (8 bitset + 8 field slot)
            assert_eq!(&row0[16..19], &[1u8, 2, 3]);

            let row1 =
                std::slice::from_raw_parts(ptr.add(offsets[1] as usize), lengths[1] as usize);
            assert_eq!(&row1[16..19], &[4u8, 5, 6]);
        }
    }

    #[test]
    fn test_convert_dictionary_decimal_array() {
        // Test that dictionary-encoded decimals are correctly unpacked and converted
        // This tests the fix for casting to schema_type instead of value_type
        use arrow::datatypes::Int8Type;

        // Create a dictionary array with Decimal128 values
        // Values: [-0.01, -0.02, -0.03] represented as [-1, -2, -3] with scale 2
        let values = Decimal128Array::from(vec![-1i128, -2, -3])
            .with_precision_and_scale(5, 2)
            .unwrap();

        // Keys: [0, 1, 2, 0, 1, 2] - each value appears twice
        let keys = Int8Array::from(vec![0i8, 1, 2, 0, 1, 2]);

        let dict_array: ArrayRef =
            Arc::new(DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap());

        // Schema expects Decimal128(5, 2) - not a dictionary type
        let schema = vec![DataType::Decimal128(5, 2)];
        let mut ctx = ColumnarToRowContext::new(schema, 100);

        let arrays = vec![dict_array];
        let (ptr, offsets, lengths) = ctx.convert(&arrays, 6).unwrap();

        assert!(!ptr.is_null());
        assert_eq!(offsets.len(), 6);
        assert_eq!(lengths.len(), 6);

        // Verify the decimal values are correct (not doubled or otherwise corrupted)
        // Fixed-width decimal is stored directly in the 8-byte field slot
        unsafe {
            for (i, expected) in [-1i64, -2, -3, -1, -2, -3].iter().enumerate() {
                let row =
                    std::slice::from_raw_parts(ptr.add(offsets[i] as usize), lengths[i] as usize);
                // Field value starts at offset 8 (after null bitset)
                let value = i64::from_le_bytes(row[8..16].try_into().unwrap());
                assert_eq!(
                    value, *expected,
                    "Row {} should have value {}, got {}",
                    i, expected, value
                );
            }
        }
    }

    #[test]
    fn test_convert_int32_to_decimal128() {
        // Test that Int32 arrays are correctly cast to Decimal128 when schema expects Decimal128.
        // This can happen when COMET_USE_DECIMAL_128 is false and the parquet reader produces
        // Int32 for small-precision decimals.

        // Create an Int32 array representing decimals: [-1, -2, -3] which at scale 2 means
        // [-0.01, -0.02, -0.03]
        let int_array: ArrayRef = Arc::new(Int32Array::from(vec![-1i32, -2, -3]));

        // Schema expects Decimal128(5, 2)
        let schema = vec![DataType::Decimal128(5, 2)];
        let mut ctx = ColumnarToRowContext::new(schema, 100);

        let arrays = vec![int_array];
        let (ptr, offsets, lengths) = ctx.convert(&arrays, 3).unwrap();

        assert!(!ptr.is_null());
        assert_eq!(offsets.len(), 3);
        assert_eq!(lengths.len(), 3);

        // Verify the decimal values are correct after casting
        // Fixed-width decimal is stored directly in the 8-byte field slot
        unsafe {
            for (i, expected) in [-1i64, -2, -3].iter().enumerate() {
                let row =
                    std::slice::from_raw_parts(ptr.add(offsets[i] as usize), lengths[i] as usize);
                // Field value starts at offset 8 (after null bitset)
                let value = i64::from_le_bytes(row[8..16].try_into().unwrap());
                assert_eq!(
                    value, *expected,
                    "Row {} should have value {}, got {}",
                    i, expected, value
                );
            }
        }
    }

    #[test]
    fn test_convert_int64_to_decimal128() {
        // Test that Int64 arrays are correctly cast to Decimal128 when schema expects Decimal128.
        // This can happen when COMET_USE_DECIMAL_128 is false and the parquet reader produces
        // Int64 for medium-precision decimals.

        // Create an Int64 array representing decimals
        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![-100i64, -200, -300]));

        // Schema expects Decimal128(10, 2)
        let schema = vec![DataType::Decimal128(10, 2)];
        let mut ctx = ColumnarToRowContext::new(schema, 100);

        let arrays = vec![int_array];
        let (ptr, offsets, lengths) = ctx.convert(&arrays, 3).unwrap();

        assert!(!ptr.is_null());
        assert_eq!(offsets.len(), 3);
        assert_eq!(lengths.len(), 3);

        // Verify the decimal values are correct after casting
        unsafe {
            for (i, expected) in [-100i64, -200, -300].iter().enumerate() {
                let row =
                    std::slice::from_raw_parts(ptr.add(offsets[i] as usize), lengths[i] as usize);
                // Field value starts at offset 8 (after null bitset)
                let value = i64::from_le_bytes(row[8..16].try_into().unwrap());
                assert_eq!(
                    value, *expected,
                    "Row {} should have value {}, got {}",
                    i, expected, value
                );
            }
        }
    }
}
