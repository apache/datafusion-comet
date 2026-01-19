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
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};

/// Maximum digits for decimal that can fit in a long (8 bytes).
const MAX_LONG_DIGITS: u8 = 18;

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

        // Pre-allocate buffer for maximum batch size
        // Estimate: fixed_width_size per row + some extra for variable-length data
        let estimated_row_size = fixed_width_size + 64; // Conservative estimate
        let initial_capacity = batch_size * estimated_row_size;

        Self {
            schema,
            buffer: Vec::with_capacity(initial_capacity),
            offsets: Vec::with_capacity(batch_size),
            lengths: Vec::with_capacity(batch_size),
            null_bitset_width,
            fixed_width_size,
            _batch_size: batch_size,
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

        // Clear previous data
        self.buffer.clear();
        self.offsets.clear();
        self.lengths.clear();

        // Reserve space for offsets and lengths
        self.offsets.reserve(num_rows);
        self.lengths.reserve(num_rows);

        // Process each row
        for row_idx in 0..num_rows {
            let row_start = self.buffer.len();
            self.offsets.push(row_start as i32);

            // Write fixed-width portion (null bitset + field values)
            self.write_row(arrays, row_idx)?;

            let row_end = self.buffer.len();
            self.lengths.push((row_end - row_start) as i32);
        }

        Ok((self.buffer.as_ptr(), &self.offsets, &self.lengths))
    }

    /// Writes a complete row including fixed-width and variable-length portions.
    fn write_row(&mut self, arrays: &[ArrayRef], row_idx: usize) -> CometResult<()> {
        let row_start = self.buffer.len();
        let null_bitset_width = self.null_bitset_width;
        let fixed_width_size = self.fixed_width_size;

        // Extend buffer for fixed-width portion
        self.buffer.resize(row_start + fixed_width_size, 0);

        // First pass: write null bits and fixed-width values
        for (col_idx, array) in arrays.iter().enumerate() {
            let is_null = array.is_null(row_idx);

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
            } else {
                // Write field value at the correct offset
                let field_offset = row_start + null_bitset_width + col_idx * 8;
                let value = get_field_value(&self.schema[col_idx], array, row_idx)?;
                self.buffer[field_offset..field_offset + 8].copy_from_slice(&value.to_le_bytes());
            }
        }

        // Second pass: write variable-length data
        for (col_idx, array) in arrays.iter().enumerate() {
            if array.is_null(row_idx) {
                continue;
            }

            let data_type = &self.schema[col_idx];
            if let Some(var_data) = get_variable_length_data(data_type, array, row_idx)? {
                let current_offset = self.buffer.len() - row_start;
                let len = var_data.len();

                // Write the data
                self.buffer.extend_from_slice(&var_data);

                // Pad to 8-byte alignment
                let padding = Self::round_up_to_8(len) - len;
                self.buffer.extend(std::iter::repeat_n(0u8, padding));

                // Update the field slot with (offset << 32) | length
                let offset_and_len = ((current_offset as i64) << 32) | (len as i64);
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
fn get_field_value(data_type: &DataType, array: &ArrayRef, row_idx: usize) -> CometResult<i64> {
    match data_type {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(if arr.value(row_idx) { 1i64 } else { 0i64 })
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(arr.value(row_idx) as i64)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(arr.value(row_idx) as i64)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(arr.value(row_idx) as i64)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.value(row_idx))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(arr.value(row_idx).to_bits() as i64)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(arr.value(row_idx).to_bits() as i64)
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(arr.value(row_idx) as i64)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(arr.value(row_idx))
        }
        DataType::Decimal128(precision, _) if *precision <= MAX_LONG_DIGITS => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(arr.value(row_idx) as i64)
        }
        // Variable-length types use placeholder (will be overwritten)
        DataType::Utf8
        | DataType::Binary
        | DataType::Decimal128(_, _)
        | DataType::Struct(_)
        | DataType::List(_)
        | DataType::Map(_, _) => Ok(0i64),
        dt => Err(CometError::Internal(format!(
            "Unsupported data type for columnar to row conversion: {:?}",
            dt
        ))),
    }
}

/// Gets variable-length data for a field, if applicable.
fn get_variable_length_data(
    data_type: &DataType,
    array: &ArrayRef,
    row_idx: usize,
) -> CometResult<Option<Vec<u8>>> {
    match data_type {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(Some(arr.value(row_idx).as_bytes().to_vec()))
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Ok(Some(arr.value(row_idx).to_vec()))
        }
        DataType::Decimal128(precision, _) if *precision > MAX_LONG_DIGITS => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(Some(i128_to_spark_decimal_bytes(arr.value(row_idx))))
        }
        DataType::Struct(fields) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            Ok(Some(write_nested_struct(struct_array, row_idx, fields)?))
        }
        DataType::List(field) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            Ok(Some(write_list_data(list_array, row_idx, field)?))
        }
        DataType::Map(field, _) => {
            let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();
            Ok(Some(write_map_data(map_array, row_idx, field)?))
        }
        _ => Ok(None),
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

/// Writes a nested struct value to bytes.
fn write_nested_struct(
    struct_array: &StructArray,
    row_idx: usize,
    fields: &arrow::datatypes::Fields,
) -> CometResult<Vec<u8>> {
    let num_fields = fields.len();
    let nested_bitset_width = ColumnarToRowContext::calculate_bitset_width(num_fields);
    let nested_fixed_size = nested_bitset_width + num_fields * 8;

    let mut buffer = vec![0u8; nested_fixed_size];

    // Write each field of the struct
    for (field_idx, field) in fields.iter().enumerate() {
        let column = struct_array.column(field_idx);
        let is_null = column.is_null(row_idx);

        if is_null {
            // Set null bit in nested struct
            let word_idx = field_idx / 64;
            let bit_idx = field_idx % 64;
            let word_offset = word_idx * 8;
            let mut word =
                i64::from_le_bytes(buffer[word_offset..word_offset + 8].try_into().unwrap());
            word |= 1i64 << bit_idx;
            buffer[word_offset..word_offset + 8].copy_from_slice(&word.to_le_bytes());
        } else {
            // Write field value
            let field_offset = nested_bitset_width + field_idx * 8;
            let value = get_field_value(field.data_type(), column, row_idx)?;
            buffer[field_offset..field_offset + 8].copy_from_slice(&value.to_le_bytes());

            // Handle variable-length nested data
            if let Some(var_data) = get_variable_length_data(field.data_type(), column, row_idx)? {
                let current_offset = buffer.len();
                let len = var_data.len();

                buffer.extend_from_slice(&var_data);
                let padding = round_up_to_8(len) - len;
                buffer.extend(std::iter::repeat_n(0u8, padding));

                let offset_and_len = ((current_offset as i64) << 32) | (len as i64);
                buffer[field_offset..field_offset + 8]
                    .copy_from_slice(&offset_and_len.to_le_bytes());
            }
        }
    }

    Ok(buffer)
}

/// Writes a list (array) value in UnsafeArrayData format.
fn write_list_data(
    list_array: &ListArray,
    row_idx: usize,
    element_field: &arrow::datatypes::FieldRef,
) -> CometResult<Vec<u8>> {
    let values = list_array.value(row_idx);
    let num_elements = values.len();

    // UnsafeArrayData format:
    // [numElements: 8 bytes][null bitset][element offsets or values]

    let element_bitset_width = ColumnarToRowContext::calculate_bitset_width(num_elements);
    let mut buffer = Vec::new();

    // Write number of elements
    buffer.extend_from_slice(&(num_elements as i64).to_le_bytes());

    // Write null bitset for elements
    let null_bitset_start = buffer.len();
    buffer.resize(null_bitset_start + element_bitset_width, 0);

    for i in 0..num_elements {
        if values.is_null(i) {
            let word_idx = i / 64;
            let bit_idx = i % 64;
            let word_offset = null_bitset_start + word_idx * 8;
            let mut word =
                i64::from_le_bytes(buffer[word_offset..word_offset + 8].try_into().unwrap());
            word |= 1i64 << bit_idx;
            buffer[word_offset..word_offset + 8].copy_from_slice(&word.to_le_bytes());
        }
    }

    // Write element values (8 bytes each for fixed-width or offset+length for variable)
    let elements_start = buffer.len();
    buffer.resize(elements_start + num_elements * 8, 0);

    for i in 0..num_elements {
        if !values.is_null(i) {
            let slot_offset = elements_start + i * 8;
            let value = get_field_value(element_field.data_type(), &values, i)?;
            buffer[slot_offset..slot_offset + 8].copy_from_slice(&value.to_le_bytes());

            // Handle variable-length element data
            if let Some(var_data) = get_variable_length_data(element_field.data_type(), &values, i)?
            {
                let current_offset = buffer.len();
                let len = var_data.len();

                buffer.extend_from_slice(&var_data);
                let padding = round_up_to_8(len) - len;
                buffer.extend(std::iter::repeat_n(0u8, padding));

                let offset_and_len = ((current_offset as i64) << 32) | (len as i64);
                buffer[slot_offset..slot_offset + 8].copy_from_slice(&offset_and_len.to_le_bytes());
            }
        }
    }

    Ok(buffer)
}

/// Writes a map value in UnsafeMapData format.
fn write_map_data(
    map_array: &MapArray,
    row_idx: usize,
    entries_field: &arrow::datatypes::FieldRef,
) -> CometResult<Vec<u8>> {
    let entries = map_array.value(row_idx);

    // UnsafeMapData format:
    // [key array size: 8 bytes][key array data][value array data]

    // Get keys and values from the struct array entries
    let keys = entries.column(0);
    let values = entries.column(1);
    let num_entries = keys.len();

    let mut buffer = Vec::new();

    // Placeholder for key array size (will be filled in later)
    let key_size_offset = buffer.len();
    buffer.extend_from_slice(&0i64.to_le_bytes());

    // Write key array (as UnsafeArrayData)
    let key_array_start = buffer.len();
    buffer.extend_from_slice(&(num_entries as i64).to_le_bytes());

    let key_bitset_width = ColumnarToRowContext::calculate_bitset_width(num_entries);
    let key_null_start = buffer.len();
    buffer.resize(key_null_start + key_bitset_width, 0);

    // Map keys are not nullable in Spark, but we write the bitset anyway
    let key_elements_start = buffer.len();
    buffer.resize(key_elements_start + num_entries * 8, 0);

    if let DataType::Struct(fields) = entries_field.data_type() {
        let key_type = fields[0].data_type();
        for i in 0..num_entries {
            let slot_offset = key_elements_start + i * 8;
            let value = get_field_value(key_type, keys, i)?;
            buffer[slot_offset..slot_offset + 8].copy_from_slice(&value.to_le_bytes());

            // Handle variable-length key data
            if let Some(var_data) = get_variable_length_data(key_type, keys, i)? {
                let current_offset = buffer.len();
                let len = var_data.len();

                buffer.extend_from_slice(&var_data);
                let padding = round_up_to_8(len) - len;
                buffer.extend(std::iter::repeat_n(0u8, padding));

                let offset_and_len = ((current_offset as i64) << 32) | (len as i64);
                buffer[slot_offset..slot_offset + 8].copy_from_slice(&offset_and_len.to_le_bytes());
            }
        }
    }

    let key_array_size = (buffer.len() - key_array_start) as i64;
    buffer[key_size_offset..key_size_offset + 8].copy_from_slice(&key_array_size.to_le_bytes());

    // Write value array
    buffer.extend_from_slice(&(num_entries as i64).to_le_bytes());

    let value_bitset_width = ColumnarToRowContext::calculate_bitset_width(num_entries);
    let value_null_start = buffer.len();
    buffer.resize(value_null_start + value_bitset_width, 0);

    for i in 0..num_entries {
        if values.is_null(i) {
            let word_idx = i / 64;
            let bit_idx = i % 64;
            let word_offset = value_null_start + word_idx * 8;
            let mut word =
                i64::from_le_bytes(buffer[word_offset..word_offset + 8].try_into().unwrap());
            word |= 1i64 << bit_idx;
            buffer[word_offset..word_offset + 8].copy_from_slice(&word.to_le_bytes());
        }
    }

    let value_elements_start = buffer.len();
    buffer.resize(value_elements_start + num_entries * 8, 0);

    if let DataType::Struct(fields) = entries_field.data_type() {
        let value_type = fields[1].data_type();
        for i in 0..num_entries {
            if !values.is_null(i) {
                let slot_offset = value_elements_start + i * 8;
                let value = get_field_value(value_type, values, i)?;
                buffer[slot_offset..slot_offset + 8].copy_from_slice(&value.to_le_bytes());

                // Handle variable-length value data
                if let Some(var_data) = get_variable_length_data(value_type, values, i)? {
                    let current_offset = buffer.len();
                    let len = var_data.len();

                    buffer.extend_from_slice(&var_data);
                    let padding = round_up_to_8(len) - len;
                    buffer.extend(std::iter::repeat_n(0u8, padding));

                    let offset_and_len = ((current_offset as i64) << 32) | (len as i64);
                    buffer[slot_offset..slot_offset + 8]
                        .copy_from_slice(&offset_and_len.to_le_bytes());
                }
            }
        }
    }

    Ok(buffer)
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
}
