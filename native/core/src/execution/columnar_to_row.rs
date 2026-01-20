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
use arrow::datatypes::{ArrowNativeType, DataType, TimeUnit};
use std::sync::Arc;

/// Maximum digits for decimal that can fit in a long (8 bytes).
const MAX_LONG_DIGITS: u8 = 18;

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

        // Process each row (general path for variable-length data)
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
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        CometError::Internal("Failed to downcast to BooleanArray".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset] = if arr.value(row_idx) { 1 } else { 0 };
                    }
                }
            }
            DataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                    CometError::Internal("Failed to downcast to Int8Array".to_string())
                })?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset..offset + 8]
                            .copy_from_slice(&(arr.value(row_idx) as i64).to_le_bytes());
                    }
                }
            }
            DataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                    CometError::Internal("Failed to downcast to Int16Array".to_string())
                })?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset..offset + 8]
                            .copy_from_slice(&(arr.value(row_idx) as i64).to_le_bytes());
                    }
                }
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    CometError::Internal("Failed to downcast to Int32Array".to_string())
                })?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset..offset + 8]
                            .copy_from_slice(&(arr.value(row_idx) as i64).to_le_bytes());
                    }
                }
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    CometError::Internal("Failed to downcast to Int64Array".to_string())
                })?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset..offset + 8]
                            .copy_from_slice(&arr.value(row_idx).to_le_bytes());
                    }
                }
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        CometError::Internal("Failed to downcast to Float32Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset..offset + 8]
                            .copy_from_slice(&(arr.value(row_idx).to_bits() as i64).to_le_bytes());
                    }
                }
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        CometError::Internal("Failed to downcast to Float64Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset..offset + 8]
                            .copy_from_slice(&(arr.value(row_idx).to_bits() as i64).to_le_bytes());
                    }
                }
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        CometError::Internal("Failed to downcast to Date32Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset..offset + 8]
                            .copy_from_slice(&(arr.value(row_idx) as i64).to_le_bytes());
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        CometError::Internal(
                            "Failed to downcast to TimestampMicrosecondArray".to_string(),
                        )
                    })?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset..offset + 8]
                            .copy_from_slice(&arr.value(row_idx).to_le_bytes());
                    }
                }
            }
            DataType::Decimal128(precision, _) if *precision <= MAX_LONG_DIGITS => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        CometError::Internal("Failed to downcast to Decimal128Array".to_string())
                    })?;
                for row_idx in 0..num_rows {
                    if !arr.is_null(row_idx) {
                        let offset = row_idx * row_size + field_offset_in_row;
                        self.buffer[offset..offset + 8]
                            .copy_from_slice(&(arr.value(row_idx) as i64).to_le_bytes());
                    }
                }
            }
            _ => {
                return Err(CometError::Internal(format!(
                    "Unexpected non-fixed-width type in fast path: {:?}",
                    data_type
                )));
            }
        }

        Ok(())
    }

    /// Writes a complete row including fixed-width and variable-length portions.
    /// Optimized to write directly to the buffer without intermediate allocations.
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

        // Second pass: write variable-length data directly to buffer
        for (col_idx, array) in arrays.iter().enumerate() {
            if array.is_null(row_idx) {
                continue;
            }

            // Write variable-length data directly to buffer, returns actual length (0 if not variable-length)
            let actual_len = write_variable_length_to_buffer(&mut self.buffer, array, row_idx)?;
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
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to BooleanArray for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(if arr.value(row_idx) { 1i64 } else { 0i64 })
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to downcast to Int8Array for type {:?}",
                    actual_type
                ))
            })?;
            Ok(arr.value(row_idx) as i64)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to downcast to Int16Array for type {:?}",
                    actual_type
                ))
            })?;
            Ok(arr.value(row_idx) as i64)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to downcast to Int32Array for type {:?}",
                    actual_type
                ))
            })?;
            Ok(arr.value(row_idx) as i64)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to downcast to Int64Array for type {:?}",
                    actual_type
                ))
            })?;
            Ok(arr.value(row_idx))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to Float32Array for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(arr.value(row_idx).to_bits() as i64)
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to Float64Array for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(arr.value(row_idx).to_bits() as i64)
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to Date32Array for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(arr.value(row_idx) as i64)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to TimestampMicrosecondArray for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(arr.value(row_idx))
        }
        DataType::Decimal128(precision, _) if *precision <= MAX_LONG_DIGITS => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to Decimal128Array for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(arr.value(row_idx) as i64)
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

/// Gets variable-length data for a field, if applicable.
fn get_variable_length_data(
    data_type: &DataType,
    array: &ArrayRef,
    row_idx: usize,
) -> CometResult<Option<Vec<u8>>> {
    // Use the actual array type for dispatching to handle type mismatches
    // between the serialized schema and the actual Arrow array (e.g., List vs LargeList)
    let actual_type = array.data_type();

    match actual_type {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to StringArray for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(Some(arr.value(row_idx).as_bytes().to_vec()))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to LargeStringArray for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(Some(arr.value(row_idx).as_bytes().to_vec()))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to BinaryArray for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(Some(arr.value(row_idx).to_vec()))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to LargeBinaryArray for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(Some(arr.value(row_idx).to_vec()))
        }
        DataType::Decimal128(precision, _) if *precision > MAX_LONG_DIGITS => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to Decimal128Array for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(Some(i128_to_spark_decimal_bytes(arr.value(row_idx))))
        }
        DataType::Struct(fields) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to StructArray for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(Some(write_nested_struct(struct_array, row_idx, fields)?))
        }
        DataType::List(field) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to downcast to ListArray for type {:?}",
                    actual_type
                ))
            })?;
            Ok(Some(write_list_data(list_array, row_idx, field)?))
        }
        DataType::LargeList(field) => {
            let list_array = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to LargeListArray for type {:?}",
                        actual_type
                    ))
                })?;
            Ok(Some(write_large_list_data(list_array, row_idx, field)?))
        }
        DataType::Map(field, _) => {
            let map_array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to downcast to MapArray for type {:?}",
                    actual_type
                ))
            })?;
            Ok(Some(write_map_data(map_array, row_idx, field)?))
        }
        // Handle Dictionary-encoded arrays by extracting the actual value
        DataType::Dictionary(key_type, value_type) => {
            get_dictionary_value(array, row_idx, key_type, value_type)
        }
        // For types not in the match, check if the schema type expects variable-length
        _ => {
            match data_type {
                DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Struct(_)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::Map(_, _) => Err(CometError::Internal(format!(
                    "Type mismatch in columnar to row: schema expects {:?} but actual array type is {:?}",
                    data_type, actual_type
                ))),
                DataType::Decimal128(precision, _) if *precision > MAX_LONG_DIGITS => {
                    Err(CometError::Internal(format!(
                        "Type mismatch for large decimal: schema expects {:?} but actual array type is {:?}",
                        data_type, actual_type
                    )))
                }
                _ => Ok(None),
            }
        }
    }
}

/// Gets the value from a dictionary-encoded array.
fn get_dictionary_value(
    array: &ArrayRef,
    row_idx: usize,
    key_type: &DataType,
    value_type: &DataType,
) -> CometResult<Option<Vec<u8>>> {
    // Handle different key types (Int8, Int16, Int32, Int64 are common)
    match key_type {
        DataType::Int8 => get_dictionary_value_with_key::<Int8Type>(array, row_idx, value_type),
        DataType::Int16 => get_dictionary_value_with_key::<Int16Type>(array, row_idx, value_type),
        DataType::Int32 => get_dictionary_value_with_key::<Int32Type>(array, row_idx, value_type),
        DataType::Int64 => get_dictionary_value_with_key::<Int64Type>(array, row_idx, value_type),
        DataType::UInt8 => get_dictionary_value_with_key::<UInt8Type>(array, row_idx, value_type),
        DataType::UInt16 => get_dictionary_value_with_key::<UInt16Type>(array, row_idx, value_type),
        DataType::UInt32 => get_dictionary_value_with_key::<UInt32Type>(array, row_idx, value_type),
        DataType::UInt64 => get_dictionary_value_with_key::<UInt64Type>(array, row_idx, value_type),
        _ => Err(CometError::Internal(format!(
            "Unsupported dictionary key type: {:?}",
            key_type
        ))),
    }
}

/// Gets the value from a dictionary array with a specific key type.
fn get_dictionary_value_with_key<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    row_idx: usize,
    value_type: &DataType,
) -> CometResult<Option<Vec<u8>>> {
    let dict_array = array
        .as_any()
        .downcast_ref::<DictionaryArray<K>>()
        .ok_or_else(|| {
            CometError::Internal(format!(
                "Failed to downcast to DictionaryArray<{:?}>",
                std::any::type_name::<K>()
            ))
        })?;

    // Get the values array (the dictionary)
    let values = dict_array.values();

    // Get the key for this row (index into the dictionary)
    let key_idx = dict_array.keys().value(row_idx).to_usize().ok_or_else(|| {
        CometError::Internal("Dictionary key index out of usize range".to_string())
    })?;

    // Extract the value based on the value type
    match value_type {
        DataType::Utf8 => {
            let string_values = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast dictionary values to StringArray, actual type: {:?}",
                        values.data_type()
                    ))
                })?;
            Ok(Some(string_values.value(key_idx).as_bytes().to_vec()))
        }
        DataType::LargeUtf8 => {
            let string_values = values
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast dictionary values to LargeStringArray, actual type: {:?}",
                        values.data_type()
                    ))
                })?;
            Ok(Some(string_values.value(key_idx).as_bytes().to_vec()))
        }
        DataType::Binary => {
            let binary_values = values
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast dictionary values to BinaryArray, actual type: {:?}",
                        values.data_type()
                    ))
                })?;
            Ok(Some(binary_values.value(key_idx).to_vec()))
        }
        DataType::LargeBinary => {
            let binary_values = values
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast dictionary values to LargeBinaryArray, actual type: {:?}",
                        values.data_type()
                    ))
                })?;
            Ok(Some(binary_values.value(key_idx).to_vec()))
        }
        _ => Err(CometError::Internal(format!(
            "Unsupported dictionary value type for variable-length data: {:?}",
            value_type
        ))),
    }
}

/// Writes variable-length data directly to buffer without intermediate allocations.
/// Returns the actual (unpadded) length of the data written, or 0 if not a variable-length type.
/// The buffer is extended with data followed by padding to 8-byte alignment.
#[inline]
fn write_variable_length_to_buffer(
    buffer: &mut Vec<u8>,
    array: &ArrayRef,
    row_idx: usize,
) -> CometResult<usize> {
    let actual_type = array.data_type();

    match actual_type {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to StringArray for type {:?}",
                        actual_type
                    ))
                })?;
            let bytes = arr.value(row_idx).as_bytes();
            let len = bytes.len();
            buffer.extend_from_slice(bytes);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to LargeStringArray for type {:?}",
                        actual_type
                    ))
                })?;
            let bytes = arr.value(row_idx).as_bytes();
            let len = bytes.len();
            buffer.extend_from_slice(bytes);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to BinaryArray for type {:?}",
                        actual_type
                    ))
                })?;
            let bytes = arr.value(row_idx);
            let len = bytes.len();
            buffer.extend_from_slice(bytes);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to LargeBinaryArray for type {:?}",
                        actual_type
                    ))
                })?;
            let bytes = arr.value(row_idx);
            let len = bytes.len();
            buffer.extend_from_slice(bytes);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::Decimal128(precision, _) if *precision > MAX_LONG_DIGITS => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to Decimal128Array for type {:?}",
                        actual_type
                    ))
                })?;
            // For large decimals, we still need to convert to Spark format
            let bytes = i128_to_spark_decimal_bytes(arr.value(row_idx));
            let len = bytes.len();
            buffer.extend_from_slice(&bytes);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::Struct(fields) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to StructArray for type {:?}",
                        actual_type
                    ))
                })?;
            // For complex types, use the existing functions for now
            // These can be further optimized to write directly in the future
            let data = write_nested_struct(struct_array, row_idx, fields)?;
            let len = data.len();
            buffer.extend_from_slice(&data);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::List(field) => {
            let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to downcast to ListArray for type {:?}",
                    actual_type
                ))
            })?;
            let data = write_list_data(list_array, row_idx, field)?;
            let len = data.len();
            buffer.extend_from_slice(&data);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::LargeList(field) => {
            let list_array = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast to LargeListArray for type {:?}",
                        actual_type
                    ))
                })?;
            let data = write_large_list_data(list_array, row_idx, field)?;
            let len = data.len();
            buffer.extend_from_slice(&data);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::Map(field, _) => {
            let map_array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to downcast to MapArray for type {:?}",
                    actual_type
                ))
            })?;
            let data = write_map_data(map_array, row_idx, field)?;
            let len = data.len();
            buffer.extend_from_slice(&data);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::Dictionary(key_type, value_type) => {
            // For dictionary-encoded arrays, extract the value and write it
            write_dictionary_to_buffer(buffer, array, row_idx, key_type, value_type)
        }
        // Not a variable-length type
        _ => Ok(0),
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
            let string_values = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast dictionary values to StringArray, actual type: {:?}",
                        values.data_type()
                    ))
                })?;
            let bytes = string_values.value(key_idx).as_bytes();
            let len = bytes.len();
            buffer.extend_from_slice(bytes);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::LargeUtf8 => {
            let string_values = values
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast dictionary values to LargeStringArray, actual type: {:?}",
                        values.data_type()
                    ))
                })?;
            let bytes = string_values.value(key_idx).as_bytes();
            let len = bytes.len();
            buffer.extend_from_slice(bytes);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::Binary => {
            let binary_values = values
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast dictionary values to BinaryArray, actual type: {:?}",
                        values.data_type()
                    ))
                })?;
            let bytes = binary_values.value(key_idx);
            let len = bytes.len();
            buffer.extend_from_slice(bytes);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
        }
        DataType::LargeBinary => {
            let binary_values = values
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    CometError::Internal(format!(
                        "Failed to downcast dictionary values to LargeBinaryArray, actual type: {:?}",
                        values.data_type()
                    ))
                })?;
            let bytes = binary_values.value(key_idx);
            let len = bytes.len();
            buffer.extend_from_slice(bytes);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));
            Ok(len)
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

/// Gets the element size in bytes for UnsafeArrayData.
/// Unlike UnsafeRow fields which are always 8 bytes, UnsafeArrayData uses
/// the actual primitive size for fixed-width types.
#[inline]
fn get_element_size(data_type: &DataType) -> usize {
    match data_type {
        DataType::Boolean => 1,
        DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 4,
        DataType::Int64 => 8,
        DataType::Float32 => 4,
        DataType::Float64 => 8,
        DataType::Date32 => 4,
        DataType::Timestamp(_, _) => 8,
        DataType::Decimal128(precision, _) if *precision <= MAX_LONG_DIGITS => 8,
        // Variable-length types use 8 bytes for offset+length
        _ => 8,
    }
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
    let element_type = element_field.data_type();
    let element_size = get_element_size(element_type);

    // UnsafeArrayData format:
    // [numElements: 8 bytes][null bitset][elements with type-specific size]
    // The null bitset is aligned to 8 bytes.
    // For primitive types, elements use their natural size (e.g., 4 bytes for INT).
    // For variable-length types, elements use 8 bytes (offset + length).

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

    // Write element values using type-specific element size
    let elements_start = buffer.len();
    let elements_total_size = round_up_to_8(num_elements * element_size);
    buffer.resize(elements_start + elements_total_size, 0);

    for i in 0..num_elements {
        if !values.is_null(i) {
            let slot_offset = elements_start + i * element_size;
            let value = get_field_value(element_type, &values, i)?;
            write_array_element(&mut buffer, element_type, value, slot_offset);

            // Handle variable-length element data
            if let Some(var_data) = get_variable_length_data(element_type, &values, i)? {
                // Offset is relative to the array base (buffer position 0 since this is a fresh Vec)
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

/// Writes a large list (array) value in UnsafeArrayData format.
/// This is the same as write_list_data but for LargeListArray (64-bit offsets).
fn write_large_list_data(
    list_array: &LargeListArray,
    row_idx: usize,
    element_field: &arrow::datatypes::FieldRef,
) -> CometResult<Vec<u8>> {
    let values = list_array.value(row_idx);
    let num_elements = values.len();
    let element_type = element_field.data_type();
    let element_size = get_element_size(element_type);

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

    // Write element values using type-specific element size
    let elements_start = buffer.len();
    let elements_total_size = round_up_to_8(num_elements * element_size);
    buffer.resize(elements_start + elements_total_size, 0);

    for i in 0..num_elements {
        if !values.is_null(i) {
            let slot_offset = elements_start + i * element_size;
            let value = get_field_value(element_type, &values, i)?;
            write_array_element(&mut buffer, element_type, value, slot_offset);

            // Handle variable-length element data
            if let Some(var_data) = get_variable_length_data(element_type, &values, i)? {
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
    // UnsafeMapData format:
    // [key array size: 8 bytes][key array data][value array data]

    // Use map_array.value() to get the entries for this row.
    // This properly handles any offset in the MapArray (e.g., from slicing or FFI).
    let entries = map_array.value(row_idx);
    let num_entries = entries.len();

    // Get the key and value columns from the entries StructArray.
    // entries.column() returns &ArrayRef, so we clone to get owned ArrayRef
    // for easier manipulation.
    let keys = Arc::clone(entries.column(0));
    let values = Arc::clone(entries.column(1));

    // Check if the column lengths match. If they don't, we may have an FFI issue
    // where the StructArray's columns weren't properly sliced.
    if keys.len() != num_entries || values.len() != num_entries {
        // The columns have different lengths than the entries, which suggests
        // they weren't properly sliced when the StructArray was created.
        // This can happen with FFI-imported data. We need to manually slice.
        return Err(CometError::Internal(format!(
            "Map entries column length mismatch: entries.len()={}, keys.len()={}, values.len()={}",
            num_entries,
            keys.len(),
            values.len()
        )));
    }

    // Get the key and value types from the entries struct field
    let (key_type, value_type) = if let DataType::Struct(fields) = entries_field.data_type() {
        (fields[0].data_type().clone(), fields[1].data_type().clone())
    } else {
        return Err(CometError::Internal(format!(
            "Map entries field is not a struct: {:?}",
            entries_field.data_type()
        )));
    };

    let key_element_size = get_element_size(&key_type);
    let value_element_size = get_element_size(&value_type);

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
    let key_elements_size = round_up_to_8(num_entries * key_element_size);
    buffer.resize(key_elements_start + key_elements_size, 0);

    for i in 0..num_entries {
        let slot_offset = key_elements_start + i * key_element_size;
        let value = get_field_value(&key_type, &keys, i)?;
        write_array_element(&mut buffer, &key_type, value, slot_offset);

        // Handle variable-length key data
        if let Some(var_data) = get_variable_length_data(&key_type, &keys, i)? {
            // Offset must be relative to the key array base (where numElements is)
            let current_offset = buffer.len() - key_array_start;
            let len = var_data.len();

            buffer.extend_from_slice(&var_data);
            let padding = round_up_to_8(len) - len;
            buffer.extend(std::iter::repeat_n(0u8, padding));

            let offset_and_len = ((current_offset as i64) << 32) | (len as i64);
            buffer[slot_offset..slot_offset + 8].copy_from_slice(&offset_and_len.to_le_bytes());
        }
    }

    let key_array_size = (buffer.len() - key_array_start) as i64;
    buffer[key_size_offset..key_size_offset + 8].copy_from_slice(&key_array_size.to_le_bytes());

    // Write value array
    let value_array_start = buffer.len();
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
    let value_elements_size = round_up_to_8(num_entries * value_element_size);
    buffer.resize(value_elements_start + value_elements_size, 0);

    for i in 0..num_entries {
        if !values.is_null(i) {
            let slot_offset = value_elements_start + i * value_element_size;
            let value = get_field_value(&value_type, &values, i)?;
            write_array_element(&mut buffer, &value_type, value, slot_offset);

            // Handle variable-length value data
            if let Some(var_data) = get_variable_length_data(&value_type, &values, i)? {
                // Offset must be relative to the value array base (where numElements is)
                let current_offset = buffer.len() - value_array_start;
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
        let list_array = ListArray::new(list_field.clone(), offsets, Arc::new(values), None);

        // Convert the list for row 0
        let result = write_list_data(&list_array, 0, &list_field).expect("conversion failed");

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
        let list_array = ListArray::new(list_field.clone(), offsets, Arc::new(values), None);

        // Test row 1 which has elements [0, 1]
        let result = write_list_data(&list_array, 1, &list_field).expect("conversion failed");

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
        let result =
            write_map_data(&map_array, 0, &Arc::new(entries_field)).expect("conversion failed");

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
        let result = write_map_data(&map_array, 1, &Arc::new(entries_field.clone()))
            .expect("conversion failed");

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
        let result = write_map_data(sliced_map_array, 0, &Arc::new(entries_field.clone()))
            .expect("conversion failed");

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
        let list_array = LargeListArray::new(list_field.clone(), offsets, Arc::new(values), None);

        // Convert the list for row 0
        let result = write_large_list_data(&list_array, 0, &list_field).expect("conversion failed");

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
    fn test_get_variable_length_data_with_large_list() {
        use arrow::datatypes::Field;

        // Create a LargeListArray and pass it to get_variable_length_data
        // This tests that the function correctly dispatches based on the actual array type
        let values = Int32Array::from(vec![10, 20, 30]);
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i64, 3].into());

        let list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_array = LargeListArray::new(list_field.clone(), offsets, Arc::new(values), None);
        let array_ref: ArrayRef = Arc::new(list_array);

        // Even if we pass a List schema type, the function should handle LargeList correctly
        // because it now uses the actual array type for dispatching
        let list_schema_type = DataType::List(list_field);

        let result = get_variable_length_data(&list_schema_type, &array_ref, 0)
            .expect("conversion failed")
            .expect("should have data");

        // Verify the result
        let num_elements = i64::from_le_bytes(result[0..8].try_into().unwrap());
        assert_eq!(num_elements, 3, "should have 3 elements");
    }

    #[test]
    fn test_dictionary_encoded_string_array() {
        // Create a dictionary-encoded string array
        // This simulates what Spark/Parquet might send through FFI for optimized string columns
        let keys = Int32Array::from(vec![0, 1, 2, 0, 1]); // indices into dictionary
        let values = StringArray::from(vec!["hello", "world", "test"]);

        let dict_array: DictionaryArray<Int32Type> =
            DictionaryArray::try_new(keys, Arc::new(values)).expect("failed to create dict array");
        let array_ref: ArrayRef = Arc::new(dict_array);

        // Test that we can extract values correctly even when schema says Utf8
        let schema_type = DataType::Utf8;

        // Row 0 should be "hello" (key=0)
        let result = get_variable_length_data(&schema_type, &array_ref, 0)
            .expect("conversion failed")
            .expect("should have data");
        assert_eq!(result, b"hello", "row 0 should be 'hello'");

        // Row 1 should be "world" (key=1)
        let result = get_variable_length_data(&schema_type, &array_ref, 1)
            .expect("conversion failed")
            .expect("should have data");
        assert_eq!(result, b"world", "row 1 should be 'world'");

        // Row 2 should be "test" (key=2)
        let result = get_variable_length_data(&schema_type, &array_ref, 2)
            .expect("conversion failed")
            .expect("should have data");
        assert_eq!(result, b"test", "row 2 should be 'test'");

        // Row 3 should be "hello" again (key=0)
        let result = get_variable_length_data(&schema_type, &array_ref, 3)
            .expect("conversion failed")
            .expect("should have data");
        assert_eq!(result, b"hello", "row 3 should be 'hello'");
    }

    #[test]
    fn test_get_field_value_with_dictionary() {
        // Test that get_field_value returns 0 (placeholder) for dictionary types
        let keys = Int32Array::from(vec![0, 1]);
        let values = StringArray::from(vec!["a", "b"]);

        let dict_array: DictionaryArray<Int32Type> =
            DictionaryArray::try_new(keys, Arc::new(values)).expect("failed to create dict array");
        let array_ref: ArrayRef = Arc::new(dict_array);

        let schema_type = DataType::Utf8;

        // Should return 0 as placeholder for variable-length type
        let result = get_field_value(&schema_type, &array_ref, 0).expect("should not fail");
        assert_eq!(result, 0, "dictionary type should return 0 placeholder");
    }
}
