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

//! Utils for supporting native sort-based columnar shuffle.

use crate::{
    errors::CometError,
    execution::{
        shuffle::{
            codec::{Checksum, ShuffleBlockWriter},
            spark_unsafe::{
                list::{append_list_element, SparkUnsafeArray},
                map::{append_map_elements, get_map_key_value_fields, SparkUnsafeMap},
            },
        },
        utils::bytes_to_i128,
    },
};
use arrow::array::{
    builder::{
        ArrayBuilder, BinaryBuilder, BinaryDictionaryBuilder, BooleanBuilder, Date32Builder,
        Decimal128Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
        Int64Builder, Int8Builder, ListBuilder, MapBuilder, StringBuilder, StringDictionaryBuilder,
        StructBuilder, TimestampMicrosecondBuilder,
    },
    types::Int32Type,
    Array, ArrayRef, RecordBatch, RecordBatchOptions,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use datafusion::physical_plan::metrics::Time;
use jni::sys::{jint, jlong};
use std::{
    fs::OpenOptions,
    io::{Cursor, Write},
    str::from_utf8,
    sync::Arc,
};

const MAX_LONG_DIGITS: u8 = 18;
const NESTED_TYPE_BUILDER_CAPACITY: usize = 100;

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
pub trait SparkUnsafeObject {
    /// Returns the address of the row.
    fn get_row_addr(&self) -> i64;

    /// Returns the offset of the element at the given index.
    fn get_element_offset(&self, index: usize, element_size: usize) -> *const u8;

    /// Returns the offset and length of the element at the given index.
    #[inline]
    fn get_offset_and_len(&self, index: usize) -> (i32, i32) {
        let offset_and_size = self.get_long(index);
        let offset = (offset_and_size >> 32) as i32;
        let len = offset_and_size as i32;
        (offset, len)
    }

    /// Returns boolean value at the given index of the object.
    #[inline]
    fn get_boolean(&self, index: usize) -> bool {
        let addr = self.get_element_offset(index, 1);
        unsafe { *addr != 0 }
    }

    /// Returns byte value at the given index of the object.
    #[inline]
    fn get_byte(&self, index: usize) -> i8 {
        let addr = self.get_element_offset(index, 1);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 1) };
        i8::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns short value at the given index of the object.
    #[inline]
    fn get_short(&self, index: usize) -> i16 {
        let addr = self.get_element_offset(index, 2);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 2) };
        i16::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns integer value at the given index of the object.
    #[inline]
    fn get_int(&self, index: usize) -> i32 {
        let addr = self.get_element_offset(index, 4);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 4) };
        i32::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns long value at the given index of the object.
    #[inline]
    fn get_long(&self, index: usize) -> i64 {
        let addr = self.get_element_offset(index, 8);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 8) };
        i64::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns float value at the given index of the object.
    #[inline]
    fn get_float(&self, index: usize) -> f32 {
        let addr = self.get_element_offset(index, 4);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 4) };
        f32::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns double value at the given index of the object.
    #[inline]
    fn get_double(&self, index: usize) -> f64 {
        let addr = self.get_element_offset(index, 8);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 8) };
        f64::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns string value at the given index of the object.
    fn get_string(&self, index: usize) -> &str {
        let (offset, len) = self.get_offset_and_len(index);
        let addr = self.get_row_addr() + offset as i64;
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr as *const u8, len as usize) };

        from_utf8(slice).unwrap()
    }

    /// Returns binary value at the given index of the object.
    fn get_binary(&self, index: usize) -> &[u8] {
        let (offset, len) = self.get_offset_and_len(index);
        let addr = self.get_row_addr() + offset as i64;
        unsafe { std::slice::from_raw_parts(addr as *const u8, len as usize) }
    }

    /// Returns date value at the given index of the object.
    #[inline]
    fn get_date(&self, index: usize) -> i32 {
        let addr = self.get_element_offset(index, 4);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 4) };
        i32::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns timestamp value at the given index of the object.
    #[inline]
    fn get_timestamp(&self, index: usize) -> i64 {
        let addr = self.get_element_offset(index, 8);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 8) };
        i64::from_le_bytes(slice.try_into().unwrap())
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

pub struct SparkUnsafeRow {
    row_addr: i64,
    row_size: i32,
    row_bitset_width: i64,
}

impl SparkUnsafeObject for SparkUnsafeRow {
    fn get_row_addr(&self) -> i64 {
        self.row_addr
    }

    fn get_element_offset(&self, index: usize, _: usize) -> *const u8 {
        (self.row_addr + self.row_bitset_width + (index * 8) as i64) as *const u8
    }
}

impl Default for SparkUnsafeRow {
    fn default() -> Self {
        Self {
            row_addr: -1,
            row_size: -1,
            row_bitset_width: -1,
        }
    }
}

impl SparkUnsafeRow {
    fn new(schema: &[DataType]) -> Self {
        Self {
            row_addr: -1,
            row_size: -1,
            row_bitset_width: Self::get_row_bitset_width(schema.len()) as i64,
        }
    }

    /// Returns true if the row is a null row.
    pub fn is_null_row(&self) -> bool {
        self.row_addr == -1 && self.row_size == -1 && self.row_bitset_width == -1
    }

    /// Calculate the width of the bitset for the row in bytes.
    /// The logic is from Spark `UnsafeRow.calculateBitSetWidthInBytes`.
    #[inline]
    pub const fn get_row_bitset_width(num_fields: usize) -> usize {
        num_fields.div_ceil(64) * 8
    }

    pub fn new_with_num_fields(num_fields: usize) -> Self {
        Self {
            row_addr: -1,
            row_size: -1,
            row_bitset_width: Self::get_row_bitset_width(num_fields) as i64,
        }
    }

    /// Points the row to the given slice.
    pub fn point_to_slice(&mut self, slice: &[u8]) {
        self.row_addr = slice.as_ptr() as i64;
        self.row_size = slice.len() as i32;
    }

    /// Points the row to the given address with specified row size.
    fn point_to(&mut self, row_addr: i64, row_size: i32) {
        self.row_addr = row_addr;
        self.row_size = row_size;
    }

    pub fn get_row_size(&self) -> i32 {
        self.row_size
    }

    /// Returns true if the null bit at the given index of the row is set.
    #[inline]
    pub(crate) fn is_null_at(&self, index: usize) -> bool {
        // SAFETY: row_addr points to valid Spark UnsafeRow data with at least
        // ceil(num_fields/64) * 8 bytes of null bitset. The caller ensures index < num_fields.
        // word_offset is within the bitset region since (index >> 6) << 3 < bitset size.
        unsafe {
            let mask: i64 = 1i64 << (index & 0x3f);
            let word_offset = (self.row_addr + (((index >> 6) as i64) << 3)) as *const i64;
            let word: i64 = *word_offset;
            (word & mask) != 0
        }
    }

    /// Unsets the null bit at the given index of the row, i.e., set the bit to 0 (not null).
    pub fn set_not_null_at(&mut self, index: usize) {
        // SAFETY: row_addr points to valid Spark UnsafeRow data with at least
        // ceil(num_fields/64) * 8 bytes of null bitset. The caller ensures index < num_fields.
        // word_offset is within the bitset region since (index >> 6) << 3 < bitset size.
        // Writing is safe because we have mutable access and the memory is owned by the JVM.
        unsafe {
            let mask: i64 = 1i64 << (index & 0x3f);
            let word_offset = (self.row_addr + (((index >> 6) as i64) << 3)) as *mut i64;
            let word: i64 = *word_offset;
            *word_offset = word & !mask;
        }
    }
}

macro_rules! downcast_builder_ref {
    ($builder_type:ty, $builder:expr) => {{
        let actual_type_id = $builder.as_any().type_id();
        $builder
            .as_any_mut()
            .downcast_mut::<$builder_type>()
            .ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to downcast builder: expected {}, got {:?}",
                    stringify!($builder_type),
                    actual_type_id
                ))
            })?
    }};
}

macro_rules! get_field_builder {
    ($struct_builder:expr, $builder_type:ty, $idx:expr) => {
        $struct_builder
            .field_builder::<$builder_type>($idx)
            .ok_or_else(|| {
                CometError::Internal(format!(
                    "Failed to get field builder at index {}: expected {}",
                    $idx,
                    stringify!($builder_type)
                ))
            })?
    };
}

// Expose the macro for other modules.
use crate::execution::shuffle::CompressionCodec;
pub(crate) use downcast_builder_ref;

/// Appends field of row to the given struct builder. `dt` is the data type of the field.
/// `struct_builder` is the struct builder of the row. `row` is the row that contains the field.
/// `idx` is the index of the field in the row. The caller is responsible for ensuring that the
/// `struct_builder.append` is called before/after calling this function to append the null buffer
/// of the struct array.
#[allow(clippy::redundant_closure_call)]
pub(super) fn append_field(
    dt: &DataType,
    struct_builder: &mut StructBuilder,
    row: &SparkUnsafeRow,
    idx: usize,
) -> Result<(), CometError> {
    /// A macro for generating code of appending value into field builder of Arrow struct builder.
    macro_rules! append_field_to_builder {
        ($builder_type:ty, $accessor:expr) => {{
            let field_builder = get_field_builder!(struct_builder, $builder_type, idx);

            if row.is_null_row() {
                // The row is null.
                field_builder.append_null();
            } else {
                let is_null = row.is_null_at(idx);

                if is_null {
                    // The field in the row is null.
                    // Append a null value to the field builder.
                    field_builder.append_null();
                } else {
                    $accessor(field_builder);
                }
            }
        }};
    }

    match dt {
        DataType::Boolean => {
            append_field_to_builder!(BooleanBuilder, |builder: &mut BooleanBuilder| builder
                .append_value(row.get_boolean(idx)));
        }
        DataType::Int8 => {
            append_field_to_builder!(Int8Builder, |builder: &mut Int8Builder| builder
                .append_value(row.get_byte(idx)));
        }
        DataType::Int16 => {
            append_field_to_builder!(Int16Builder, |builder: &mut Int16Builder| builder
                .append_value(row.get_short(idx)));
        }
        DataType::Int32 => {
            append_field_to_builder!(Int32Builder, |builder: &mut Int32Builder| builder
                .append_value(row.get_int(idx)));
        }
        DataType::Int64 => {
            append_field_to_builder!(Int64Builder, |builder: &mut Int64Builder| builder
                .append_value(row.get_long(idx)));
        }
        DataType::Float32 => {
            append_field_to_builder!(Float32Builder, |builder: &mut Float32Builder| builder
                .append_value(row.get_float(idx)));
        }
        DataType::Float64 => {
            append_field_to_builder!(Float64Builder, |builder: &mut Float64Builder| builder
                .append_value(row.get_double(idx)));
        }
        DataType::Date32 => {
            append_field_to_builder!(Date32Builder, |builder: &mut Date32Builder| builder
                .append_value(row.get_date(idx)));
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            append_field_to_builder!(
                TimestampMicrosecondBuilder,
                |builder: &mut TimestampMicrosecondBuilder| builder
                    .append_value(row.get_timestamp(idx))
            );
        }
        DataType::Binary => {
            append_field_to_builder!(BinaryBuilder, |builder: &mut BinaryBuilder| builder
                .append_value(row.get_binary(idx)));
        }
        DataType::Utf8 => {
            append_field_to_builder!(StringBuilder, |builder: &mut StringBuilder| builder
                .append_value(row.get_string(idx)));
        }
        DataType::Decimal128(p, _) => {
            append_field_to_builder!(Decimal128Builder, |builder: &mut Decimal128Builder| builder
                .append_value(row.get_decimal(idx, *p)));
        }
        DataType::Struct(fields) => {
            // Appending value into struct field builder of Arrow struct builder.
            let field_builder = get_field_builder!(struct_builder, StructBuilder, idx);

            let nested_row = if row.is_null_row() || row.is_null_at(idx) {
                // The row is null, or the field in the row is null, i.e., a null nested row.
                // Append a null value to the row builder.
                field_builder.append_null();
                SparkUnsafeRow::default()
            } else {
                field_builder.append(true);
                row.get_struct(idx, fields.len())
            };

            for (field_idx, field) in fields.into_iter().enumerate() {
                append_field(field.data_type(), field_builder, &nested_row, field_idx)?;
            }
        }
        DataType::Map(field, _) => {
            let field_builder = get_field_builder!(
                struct_builder,
                MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
                idx
            );

            if row.is_null_row() {
                // The row is null.
                field_builder.append(false)?;
            } else {
                let is_null = row.is_null_at(idx);

                if is_null {
                    // The field in the row is null.
                    // Append a null value to the map builder.
                    field_builder.append(false)?;
                } else {
                    append_map_elements(field, field_builder, &row.get_map(idx))?;
                }
            }
        }
        DataType::List(field) => {
            let field_builder =
                get_field_builder!(struct_builder, ListBuilder<Box<dyn ArrayBuilder>>, idx);

            if row.is_null_row() {
                // The row is null.
                field_builder.append_null();
            } else {
                let is_null = row.is_null_at(idx);

                if is_null {
                    // The field in the row is null.
                    // Append a null value to the list builder.
                    field_builder.append_null();
                } else {
                    append_list_element(field.data_type(), field_builder, &row.get_array(idx))?
                }
            }
        }
        _ => {
            unreachable!("Unsupported data type of struct field: {:?}", dt)
        }
    }

    Ok(())
}

/// Appends nested struct fields to the struct builder using field-major order.
/// This is a helper function for processing nested struct fields recursively.
///
/// Unlike `append_struct_fields_field_major`, this function takes slices of row addresses,
/// sizes, and null flags directly, without needing to navigate from a parent row.
#[allow(clippy::redundant_closure_call)]
fn append_nested_struct_fields_field_major(
    row_addresses: &[jlong],
    row_sizes: &[jint],
    struct_is_null: &[bool],
    struct_builder: &mut StructBuilder,
    fields: &arrow::datatypes::Fields,
) -> Result<(), CometError> {
    let num_rows = row_addresses.len();
    let mut row = SparkUnsafeRow::new_with_num_fields(fields.len());

    // Helper macro for processing primitive fields
    macro_rules! process_field {
        ($builder_type:ty, $field_idx:expr, $get_value:expr) => {{
            let field_builder = get_field_builder!(struct_builder, $builder_type, $field_idx);

            for row_idx in 0..num_rows {
                if struct_is_null[row_idx] {
                    // Struct is null, field is also null
                    field_builder.append_null();
                } else {
                    let row_addr = row_addresses[row_idx];
                    let row_size = row_sizes[row_idx];
                    row.point_to(row_addr, row_size);

                    if row.is_null_at($field_idx) {
                        field_builder.append_null();
                    } else {
                        field_builder.append_value($get_value(&row, $field_idx));
                    }
                }
            }
        }};
    }

    // Process each field across all rows
    for (field_idx, field) in fields.iter().enumerate() {
        match field.data_type() {
            DataType::Boolean => {
                process_field!(BooleanBuilder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_boolean(idx));
            }
            DataType::Int8 => {
                process_field!(Int8Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_byte(idx));
            }
            DataType::Int16 => {
                process_field!(Int16Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_short(idx));
            }
            DataType::Int32 => {
                process_field!(Int32Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_int(idx));
            }
            DataType::Int64 => {
                process_field!(Int64Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_long(idx));
            }
            DataType::Float32 => {
                process_field!(Float32Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_float(idx));
            }
            DataType::Float64 => {
                process_field!(Float64Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_double(idx));
            }
            DataType::Date32 => {
                process_field!(Date32Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_date(idx));
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                process_field!(
                    TimestampMicrosecondBuilder,
                    field_idx,
                    |row: &SparkUnsafeRow, idx| row.get_timestamp(idx)
                );
            }
            DataType::Binary => {
                let field_builder = get_field_builder!(struct_builder, BinaryBuilder, field_idx);

                for row_idx in 0..num_rows {
                    if struct_is_null[row_idx] {
                        field_builder.append_null();
                    } else {
                        let row_addr = row_addresses[row_idx];
                        let row_size = row_sizes[row_idx];
                        row.point_to(row_addr, row_size);

                        if row.is_null_at(field_idx) {
                            field_builder.append_null();
                        } else {
                            field_builder.append_value(row.get_binary(field_idx));
                        }
                    }
                }
            }
            DataType::Utf8 => {
                let field_builder = get_field_builder!(struct_builder, StringBuilder, field_idx);

                for row_idx in 0..num_rows {
                    if struct_is_null[row_idx] {
                        field_builder.append_null();
                    } else {
                        let row_addr = row_addresses[row_idx];
                        let row_size = row_sizes[row_idx];
                        row.point_to(row_addr, row_size);

                        if row.is_null_at(field_idx) {
                            field_builder.append_null();
                        } else {
                            field_builder.append_value(row.get_string(field_idx));
                        }
                    }
                }
            }
            DataType::Decimal128(p, _) => {
                let p = *p;
                let field_builder =
                    get_field_builder!(struct_builder, Decimal128Builder, field_idx);

                for row_idx in 0..num_rows {
                    if struct_is_null[row_idx] {
                        field_builder.append_null();
                    } else {
                        let row_addr = row_addresses[row_idx];
                        let row_size = row_sizes[row_idx];
                        row.point_to(row_addr, row_size);

                        if row.is_null_at(field_idx) {
                            field_builder.append_null();
                        } else {
                            field_builder.append_value(row.get_decimal(field_idx, p));
                        }
                    }
                }
            }
            DataType::Struct(nested_fields) => {
                let nested_builder = get_field_builder!(struct_builder, StructBuilder, field_idx);

                // Collect nested struct addresses and sizes in one pass, building validity
                let mut nested_addresses: Vec<jlong> = Vec::with_capacity(num_rows);
                let mut nested_sizes: Vec<jint> = Vec::with_capacity(num_rows);
                let mut nested_is_null: Vec<bool> = Vec::with_capacity(num_rows);

                for row_idx in 0..num_rows {
                    if struct_is_null[row_idx] {
                        // Parent struct is null, nested struct is also null
                        nested_builder.append_null();
                        nested_is_null.push(true);
                        nested_addresses.push(0);
                        nested_sizes.push(0);
                    } else {
                        let row_addr = row_addresses[row_idx];
                        let row_size = row_sizes[row_idx];
                        row.point_to(row_addr, row_size);

                        if row.is_null_at(field_idx) {
                            nested_builder.append_null();
                            nested_is_null.push(true);
                            nested_addresses.push(0);
                            nested_sizes.push(0);
                        } else {
                            nested_builder.append(true);
                            nested_is_null.push(false);
                            // Get nested struct address and size
                            let nested_row = row.get_struct(field_idx, nested_fields.len());
                            nested_addresses.push(nested_row.get_row_addr());
                            nested_sizes.push(nested_row.get_row_size());
                        }
                    }
                }

                // Recursively process nested struct fields in field-major order
                append_nested_struct_fields_field_major(
                    &nested_addresses,
                    &nested_sizes,
                    &nested_is_null,
                    nested_builder,
                    nested_fields,
                )?;
            }
            // For list and map, fall back to append_field since they have variable-length elements
            dt @ (DataType::List(_) | DataType::Map(_, _)) => {
                for row_idx in 0..num_rows {
                    if struct_is_null[row_idx] {
                        let null_row = SparkUnsafeRow::default();
                        append_field(dt, struct_builder, &null_row, field_idx)?;
                    } else {
                        let row_addr = row_addresses[row_idx];
                        let row_size = row_sizes[row_idx];
                        row.point_to(row_addr, row_size);
                        append_field(dt, struct_builder, &row, field_idx)?;
                    }
                }
            }
            _ => {
                unreachable!(
                    "Unsupported data type of struct field: {:?}",
                    field.data_type()
                )
            }
        }
    }

    Ok(())
}

/// Reads row address and size from JVM-provided pointer arrays and points the row to that data.
///
/// # Safety
/// Caller must ensure row_addresses_ptr and row_sizes_ptr are valid for index i.
/// This is guaranteed when called from append_columns with indices in [row_start, row_end).
macro_rules! read_row_at {
    ($row:expr, $row_addresses_ptr:expr, $row_sizes_ptr:expr, $i:expr) => {{
        // SAFETY: Caller guarantees pointers are valid for this index (see macro doc)
        let row_addr = unsafe { *$row_addresses_ptr.add($i) };
        let row_size = unsafe { *$row_sizes_ptr.add($i) };
        $row.point_to(row_addr, row_size);
    }};
}

/// Appends a batch of list values to the list builder with a single type dispatch.
/// This moves type dispatch from O(rows) to O(1), significantly improving performance
/// for large batches.
#[allow(clippy::too_many_arguments)]
fn append_list_column_batch(
    row_addresses_ptr: *mut jlong,
    row_sizes_ptr: *mut jint,
    row_start: usize,
    row_end: usize,
    schema: &[DataType],
    column_idx: usize,
    element_type: &DataType,
    list_builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
) -> Result<(), CometError> {
    let mut row = SparkUnsafeRow::new(schema);

    // Helper macro for primitive element types - gets builder fresh each iteration
    // to avoid borrow conflicts with list_builder.append()
    macro_rules! process_primitive_lists {
        ($builder_type:ty, $append_fn:ident) => {{
            for i in row_start..row_end {
                read_row_at!(row, row_addresses_ptr, row_sizes_ptr, i);

                if row.is_null_at(column_idx) {
                    list_builder.append_null();
                } else {
                    let array = row.get_array(column_idx);
                    // Get values builder fresh each iteration to avoid borrow conflict
                    let values_builder = list_builder
                        .values()
                        .as_any_mut()
                        .downcast_mut::<$builder_type>()
                        .expect(stringify!($builder_type));
                    array.$append_fn::<true>(values_builder);
                    list_builder.append(true);
                }
            }
        }};
    }

    match element_type {
        DataType::Boolean => {
            process_primitive_lists!(BooleanBuilder, append_booleans_to_builder);
        }
        DataType::Int8 => {
            process_primitive_lists!(Int8Builder, append_bytes_to_builder);
        }
        DataType::Int16 => {
            process_primitive_lists!(Int16Builder, append_shorts_to_builder);
        }
        DataType::Int32 => {
            process_primitive_lists!(Int32Builder, append_ints_to_builder);
        }
        DataType::Int64 => {
            process_primitive_lists!(Int64Builder, append_longs_to_builder);
        }
        DataType::Float32 => {
            process_primitive_lists!(Float32Builder, append_floats_to_builder);
        }
        DataType::Float64 => {
            process_primitive_lists!(Float64Builder, append_doubles_to_builder);
        }
        DataType::Date32 => {
            process_primitive_lists!(Date32Builder, append_dates_to_builder);
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            process_primitive_lists!(TimestampMicrosecondBuilder, append_timestamps_to_builder);
        }
        // For complex element types, fall back to per-row dispatch
        _ => {
            for i in row_start..row_end {
                read_row_at!(row, row_addresses_ptr, row_sizes_ptr, i);

                if row.is_null_at(column_idx) {
                    list_builder.append_null();
                } else {
                    append_list_element(element_type, list_builder, &row.get_array(column_idx))?;
                }
            }
        }
    }

    Ok(())
}

/// Appends a batch of map values to the map builder with a single type dispatch.
/// This moves type dispatch from O(rows × 2) to O(2), improving performance for maps.
#[allow(clippy::too_many_arguments)]
fn append_map_column_batch(
    row_addresses_ptr: *mut jlong,
    row_sizes_ptr: *mut jint,
    row_start: usize,
    row_end: usize,
    schema: &[DataType],
    column_idx: usize,
    field: &arrow::datatypes::FieldRef,
    map_builder: &mut MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
) -> Result<(), CometError> {
    let mut row = SparkUnsafeRow::new(schema);
    let (key_field, value_field, _) = get_map_key_value_fields(field)?;
    let key_type = key_field.data_type();
    let value_type = value_field.data_type();

    // Helper macro for processing maps with primitive key/value types
    // Uses scoped borrows to avoid borrow checker conflicts
    macro_rules! process_primitive_maps {
        ($key_builder:ty, $key_append:ident, $val_builder:ty, $val_append:ident) => {{
            for i in row_start..row_end {
                read_row_at!(row, row_addresses_ptr, row_sizes_ptr, i);

                if row.is_null_at(column_idx) {
                    map_builder.append(false)?;
                } else {
                    let map = row.get_map(column_idx);
                    // Process keys in a scope so borrow ends
                    {
                        let keys_builder = map_builder
                            .keys()
                            .as_any_mut()
                            .downcast_mut::<$key_builder>()
                            .expect(stringify!($key_builder));
                        map.keys.$key_append::<false>(keys_builder);
                    }
                    // Process values in a scope so borrow ends
                    {
                        let values_builder = map_builder
                            .values()
                            .as_any_mut()
                            .downcast_mut::<$val_builder>()
                            .expect(stringify!($val_builder));
                        map.values.$val_append::<true>(values_builder);
                    }
                    map_builder.append(true)?;
                }
            }
        }};
    }

    // Optimize common map type combinations
    match (key_type, value_type) {
        // Map<Int64, Int64>
        (DataType::Int64, DataType::Int64) => {
            process_primitive_maps!(
                Int64Builder,
                append_longs_to_builder,
                Int64Builder,
                append_longs_to_builder
            );
        }
        // Map<Int64, Float64>
        (DataType::Int64, DataType::Float64) => {
            process_primitive_maps!(
                Int64Builder,
                append_longs_to_builder,
                Float64Builder,
                append_doubles_to_builder
            );
        }
        // Map<Int32, Int32>
        (DataType::Int32, DataType::Int32) => {
            process_primitive_maps!(
                Int32Builder,
                append_ints_to_builder,
                Int32Builder,
                append_ints_to_builder
            );
        }
        // Map<Int32, Int64>
        (DataType::Int32, DataType::Int64) => {
            process_primitive_maps!(
                Int32Builder,
                append_ints_to_builder,
                Int64Builder,
                append_longs_to_builder
            );
        }
        // For other types, fall back to per-row dispatch
        _ => {
            for i in row_start..row_end {
                read_row_at!(row, row_addresses_ptr, row_sizes_ptr, i);

                if row.is_null_at(column_idx) {
                    map_builder.append(false)?;
                } else {
                    append_map_elements(field, map_builder, &row.get_map(column_idx))?;
                }
            }
        }
    }

    Ok(())
}

/// Appends struct fields to the struct builder using field-major order.
/// This processes one field at a time across all rows, which moves type dispatch
/// outside the row loop (O(fields) dispatches instead of O(rows × fields)).
#[allow(clippy::redundant_closure_call, clippy::too_many_arguments)]
fn append_struct_fields_field_major(
    row_addresses_ptr: *mut jlong,
    row_sizes_ptr: *mut jint,
    row_start: usize,
    row_end: usize,
    parent_row: &mut SparkUnsafeRow,
    column_idx: usize,
    struct_builder: &mut StructBuilder,
    fields: &arrow::datatypes::Fields,
) -> Result<(), CometError> {
    let num_rows = row_end - row_start;
    let num_fields = fields.len();

    // First pass: Build struct validity and collect which structs are null
    // We use a Vec<bool> for simplicity; could use a bitset for better memory
    let mut struct_is_null = Vec::with_capacity(num_rows);

    for i in row_start..row_end {
        read_row_at!(parent_row, row_addresses_ptr, row_sizes_ptr, i);

        let is_null = parent_row.is_null_at(column_idx);
        struct_is_null.push(is_null);

        if is_null {
            struct_builder.append_null();
        } else {
            struct_builder.append(true);
        }
    }

    // Helper macro for processing primitive fields
    macro_rules! process_field {
        ($builder_type:ty, $field_idx:expr, $get_value:expr) => {{
            let field_builder = get_field_builder!(struct_builder, $builder_type, $field_idx);

            for (row_idx, i) in (row_start..row_end).enumerate() {
                if struct_is_null[row_idx] {
                    // Struct is null, field is also null
                    field_builder.append_null();
                } else {
                    read_row_at!(parent_row, row_addresses_ptr, row_sizes_ptr, i);
                    let nested_row = parent_row.get_struct(column_idx, num_fields);

                    if nested_row.is_null_at($field_idx) {
                        field_builder.append_null();
                    } else {
                        field_builder.append_value($get_value(&nested_row, $field_idx));
                    }
                }
            }
        }};
    }

    // Second pass: Process each field across all rows
    for (field_idx, field) in fields.iter().enumerate() {
        match field.data_type() {
            DataType::Boolean => {
                process_field!(BooleanBuilder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_boolean(idx));
            }
            DataType::Int8 => {
                process_field!(Int8Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_byte(idx));
            }
            DataType::Int16 => {
                process_field!(Int16Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_short(idx));
            }
            DataType::Int32 => {
                process_field!(Int32Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_int(idx));
            }
            DataType::Int64 => {
                process_field!(Int64Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_long(idx));
            }
            DataType::Float32 => {
                process_field!(Float32Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_float(idx));
            }
            DataType::Float64 => {
                process_field!(Float64Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_double(idx));
            }
            DataType::Date32 => {
                process_field!(Date32Builder, field_idx, |row: &SparkUnsafeRow, idx| row
                    .get_date(idx));
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                process_field!(
                    TimestampMicrosecondBuilder,
                    field_idx,
                    |row: &SparkUnsafeRow, idx| row.get_timestamp(idx)
                );
            }
            DataType::Binary => {
                let field_builder = get_field_builder!(struct_builder, BinaryBuilder, field_idx);

                for (row_idx, i) in (row_start..row_end).enumerate() {
                    if struct_is_null[row_idx] {
                        field_builder.append_null();
                    } else {
                        read_row_at!(parent_row, row_addresses_ptr, row_sizes_ptr, i);
                        let nested_row = parent_row.get_struct(column_idx, num_fields);

                        if nested_row.is_null_at(field_idx) {
                            field_builder.append_null();
                        } else {
                            field_builder.append_value(nested_row.get_binary(field_idx));
                        }
                    }
                }
            }
            DataType::Utf8 => {
                let field_builder = get_field_builder!(struct_builder, StringBuilder, field_idx);

                for (row_idx, i) in (row_start..row_end).enumerate() {
                    if struct_is_null[row_idx] {
                        field_builder.append_null();
                    } else {
                        read_row_at!(parent_row, row_addresses_ptr, row_sizes_ptr, i);
                        let nested_row = parent_row.get_struct(column_idx, num_fields);

                        if nested_row.is_null_at(field_idx) {
                            field_builder.append_null();
                        } else {
                            field_builder.append_value(nested_row.get_string(field_idx));
                        }
                    }
                }
            }
            DataType::Decimal128(p, _) => {
                let p = *p;
                let field_builder =
                    get_field_builder!(struct_builder, Decimal128Builder, field_idx);

                for (row_idx, i) in (row_start..row_end).enumerate() {
                    if struct_is_null[row_idx] {
                        field_builder.append_null();
                    } else {
                        read_row_at!(parent_row, row_addresses_ptr, row_sizes_ptr, i);
                        let nested_row = parent_row.get_struct(column_idx, num_fields);

                        if nested_row.is_null_at(field_idx) {
                            field_builder.append_null();
                        } else {
                            field_builder.append_value(nested_row.get_decimal(field_idx, p));
                        }
                    }
                }
            }
            // For nested structs, apply field-major processing recursively
            DataType::Struct(nested_fields) => {
                let nested_builder = get_field_builder!(struct_builder, StructBuilder, field_idx);

                // Collect nested struct addresses and sizes in one pass, building validity
                let mut nested_addresses: Vec<jlong> = Vec::with_capacity(num_rows);
                let mut nested_sizes: Vec<jint> = Vec::with_capacity(num_rows);
                let mut nested_is_null: Vec<bool> = Vec::with_capacity(num_rows);

                for (row_idx, i) in (row_start..row_end).enumerate() {
                    if struct_is_null[row_idx] {
                        // Parent struct is null, nested struct is also null
                        nested_builder.append_null();
                        nested_is_null.push(true);
                        nested_addresses.push(0);
                        nested_sizes.push(0);
                    } else {
                        read_row_at!(parent_row, row_addresses_ptr, row_sizes_ptr, i);
                        let parent_struct = parent_row.get_struct(column_idx, num_fields);

                        if parent_struct.is_null_at(field_idx) {
                            nested_builder.append_null();
                            nested_is_null.push(true);
                            nested_addresses.push(0);
                            nested_sizes.push(0);
                        } else {
                            nested_builder.append(true);
                            nested_is_null.push(false);
                            // Get nested struct address and size
                            let nested_row =
                                parent_struct.get_struct(field_idx, nested_fields.len());
                            nested_addresses.push(nested_row.get_row_addr());
                            nested_sizes.push(nested_row.get_row_size());
                        }
                    }
                }

                // Recursively process nested struct fields in field-major order
                append_nested_struct_fields_field_major(
                    &nested_addresses,
                    &nested_sizes,
                    &nested_is_null,
                    nested_builder,
                    nested_fields,
                )?;
            }
            // For list and map, fall back to append_field since they have variable-length elements
            dt @ (DataType::List(_) | DataType::Map(_, _)) => {
                for (row_idx, i) in (row_start..row_end).enumerate() {
                    if struct_is_null[row_idx] {
                        let null_row = SparkUnsafeRow::default();
                        append_field(dt, struct_builder, &null_row, field_idx)?;
                    } else {
                        read_row_at!(parent_row, row_addresses_ptr, row_sizes_ptr, i);
                        let nested_row = parent_row.get_struct(column_idx, num_fields);
                        append_field(dt, struct_builder, &nested_row, field_idx)?;
                    }
                }
            }
            _ => {
                unreachable!(
                    "Unsupported data type of struct field: {:?}",
                    field.data_type()
                )
            }
        }
    }

    Ok(())
}

/// Appends column of top rows to the given array builder.
///
/// # Safety
///
/// The caller must ensure:
/// - `row_addresses_ptr` points to an array of at least `row_end` jlong values
/// - `row_sizes_ptr` points to an array of at least `row_end` jint values
/// - Each address in `row_addresses_ptr[row_start..row_end]` points to valid Spark UnsafeRow data
/// - The memory remains valid for the duration of this function call
///
/// These invariants are guaranteed when called from JNI with arrays provided by the JVM.
#[allow(clippy::redundant_closure_call, clippy::too_many_arguments)]
fn append_columns(
    row_addresses_ptr: *mut jlong,
    row_sizes_ptr: *mut jint,
    row_start: usize,
    row_end: usize,
    schema: &[DataType],
    column_idx: usize,
    builder: &mut Box<dyn ArrayBuilder>,
    prefer_dictionary_ratio: f64,
) -> Result<(), CometError> {
    /// A macro for generating code of appending values into Arrow array builders.
    macro_rules! append_column_to_builder {
        ($builder_type:ty, $accessor:expr) => {{
            let element_builder = builder
                .as_any_mut()
                .downcast_mut::<$builder_type>()
                .expect(stringify!($builder_type));
            let mut row = SparkUnsafeRow::new(schema);

            for i in row_start..row_end {
                read_row_at!(row, row_addresses_ptr, row_sizes_ptr, i);

                let is_null = row.is_null_at(column_idx);

                if is_null {
                    // The element value is null.
                    // Append a null value to the element builder.
                    element_builder.append_null();
                } else {
                    $accessor(element_builder, &row, column_idx);
                }
            }
        }};
    }

    let dt = &schema[column_idx];

    match dt {
        DataType::Boolean => {
            append_column_to_builder!(
                BooleanBuilder,
                |builder: &mut BooleanBuilder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_boolean(idx))
            );
        }
        DataType::Int8 => {
            append_column_to_builder!(
                Int8Builder,
                |builder: &mut Int8Builder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_byte(idx))
            );
        }
        DataType::Int16 => {
            append_column_to_builder!(
                Int16Builder,
                |builder: &mut Int16Builder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_short(idx))
            );
        }
        DataType::Int32 => {
            append_column_to_builder!(
                Int32Builder,
                |builder: &mut Int32Builder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_int(idx))
            );
        }
        DataType::Int64 => {
            append_column_to_builder!(
                Int64Builder,
                |builder: &mut Int64Builder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_long(idx))
            );
        }
        DataType::Float32 => {
            append_column_to_builder!(
                Float32Builder,
                |builder: &mut Float32Builder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_float(idx))
            );
        }
        DataType::Float64 => {
            append_column_to_builder!(
                Float64Builder,
                |builder: &mut Float64Builder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_double(idx))
            );
        }
        DataType::Decimal128(p, _) => {
            append_column_to_builder!(
                Decimal128Builder,
                |builder: &mut Decimal128Builder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_decimal(idx, *p))
            );
        }
        DataType::Utf8 => {
            if prefer_dictionary_ratio > 1.0 {
                append_column_to_builder!(
                    StringDictionaryBuilder<Int32Type>,
                    |builder: &mut StringDictionaryBuilder<Int32Type>,
                     row: &SparkUnsafeRow,
                     idx| builder.append_value(row.get_string(idx))
                );
            } else {
                append_column_to_builder!(
                    StringBuilder,
                    |builder: &mut StringBuilder, row: &SparkUnsafeRow, idx| builder
                        .append_value(row.get_string(idx))
                );
            }
        }
        DataType::Binary => {
            if prefer_dictionary_ratio > 1.0 {
                append_column_to_builder!(
                    BinaryDictionaryBuilder<Int32Type>,
                    |builder: &mut BinaryDictionaryBuilder<Int32Type>,
                     row: &SparkUnsafeRow,
                     idx| builder.append_value(row.get_binary(idx))
                );
            } else {
                append_column_to_builder!(
                    BinaryBuilder,
                    |builder: &mut BinaryBuilder, row: &SparkUnsafeRow, idx| builder
                        .append_value(row.get_binary(idx))
                );
            }
        }
        DataType::Date32 => {
            append_column_to_builder!(
                Date32Builder,
                |builder: &mut Date32Builder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_date(idx))
            );
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            append_column_to_builder!(
                TimestampMicrosecondBuilder,
                |builder: &mut TimestampMicrosecondBuilder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_timestamp(idx))
            );
        }
        DataType::Map(field, _) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
                builder
            );
            // Use batched processing for better performance
            append_map_column_batch(
                row_addresses_ptr,
                row_sizes_ptr,
                row_start,
                row_end,
                schema,
                column_idx,
                field,
                map_builder,
            )?;
        }
        DataType::List(field) => {
            let list_builder = downcast_builder_ref!(ListBuilder<Box<dyn ArrayBuilder>>, builder);
            // Use batched processing for better performance
            append_list_column_batch(
                row_addresses_ptr,
                row_sizes_ptr,
                row_start,
                row_end,
                schema,
                column_idx,
                field.data_type(),
                list_builder,
            )?;
        }
        DataType::Struct(fields) => {
            let struct_builder = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .expect("StructBuilder");
            let mut row = SparkUnsafeRow::new(schema);

            // Use field-major processing to avoid per-row type dispatch
            append_struct_fields_field_major(
                row_addresses_ptr,
                row_sizes_ptr,
                row_start,
                row_end,
                &mut row,
                column_idx,
                struct_builder,
                fields,
            )?;
        }
        _ => {
            unreachable!("Unsupported data type of column: {:?}", dt)
        }
    }

    Ok(())
}

fn make_builders(
    dt: &DataType,
    row_num: usize,
    prefer_dictionary_ratio: f64,
) -> Result<Box<dyn ArrayBuilder>, CometError> {
    let builder: Box<dyn ArrayBuilder> = match dt {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(row_num)),
        DataType::Int8 => Box::new(Int8Builder::with_capacity(row_num)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(row_num)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(row_num)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(row_num)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(row_num)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(row_num)),
        DataType::Decimal128(_, _) => {
            Box::new(Decimal128Builder::with_capacity(row_num).with_data_type(dt.clone()))
        }
        DataType::Utf8 => {
            if prefer_dictionary_ratio > 1.0 {
                Box::new(StringDictionaryBuilder::<Int32Type>::with_capacity(
                    row_num / 2,
                    row_num,
                    1024,
                ))
            } else {
                Box::new(StringBuilder::with_capacity(row_num, 1024))
            }
        }
        DataType::Binary => {
            if prefer_dictionary_ratio > 1.0 {
                Box::new(BinaryDictionaryBuilder::<Int32Type>::with_capacity(
                    row_num / 2,
                    row_num,
                    1024,
                ))
            } else {
                Box::new(BinaryBuilder::with_capacity(row_num, 1024))
            }
        }
        DataType::Date32 => Box::new(Date32Builder::with_capacity(row_num)),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::with_capacity(row_num).with_data_type(dt.clone()))
        }
        DataType::Map(field, _) => {
            let (key_field, value_field, map_field_names) = get_map_key_value_fields(field)?;
            let key_dt = key_field.data_type();
            let value_dt = value_field.data_type();
            let key_builder = make_builders(key_dt, NESTED_TYPE_BUILDER_CAPACITY, 1.0)?;
            let value_builder = make_builders(value_dt, NESTED_TYPE_BUILDER_CAPACITY, 1.0)?;

            Box::new(
                MapBuilder::new(Some(map_field_names), key_builder, value_builder)
                    .with_values_field(Arc::clone(value_field)),
            )
        }
        DataType::List(field) => {
            // Disable dictionary encoding for array element
            let value_builder =
                make_builders(field.data_type(), NESTED_TYPE_BUILDER_CAPACITY, 1.0)?;

            // Needed to overwrite default ListBuilder creation having the incoming field schema to be driving
            let value_field = Arc::clone(field);

            Box::new(ListBuilder::new(value_builder).with_field(value_field))
        }
        DataType::Struct(fields) => {
            let field_builders = fields
                .iter()
                // Disable dictionary encoding for struct fields
                .map(|field| make_builders(field.data_type(), row_num, 1.0))
                .collect::<Result<Vec<_>, _>>()?;

            Box::new(StructBuilder::new(fields.clone(), field_builders))
        }
        _ => return Err(CometError::Internal(format!("Unsupported type: {dt:?}"))),
    };

    Ok(builder)
}

/// Processes a sorted row partition and writes the result to the given output path.
#[allow(clippy::too_many_arguments)]
pub fn process_sorted_row_partition(
    row_num: usize,
    batch_size: usize,
    row_addresses_ptr: *mut jlong,
    row_sizes_ptr: *mut jint,
    schema: &[DataType],
    output_path: String,
    prefer_dictionary_ratio: f64,
    checksum_enabled: bool,
    checksum_algo: i32,
    // This is the checksum value passed in from Spark side, and is getting updated for
    // each shuffle partition Spark processes. It is called "initial" here to indicate
    // this is the initial checksum for this method, as it also gets updated iteratively
    // inside the loop within the method across batches.
    initial_checksum: Option<u32>,
    codec: &CompressionCodec,
) -> Result<(i64, Option<u32>), CometError> {
    // The current row number we are reading
    let mut current_row = 0;
    // Total number of bytes written
    let mut written = 0;
    // The current checksum value. This is updated incrementally in the following loop.
    let mut current_checksum = if checksum_enabled {
        Some(Checksum::try_new(checksum_algo, initial_checksum)?)
    } else {
        None
    };

    // Create builders once and reuse them across batches.
    // After finish() is called, builders are reset and can be reused.
    let mut data_builders: Vec<Box<dyn ArrayBuilder>> = vec![];
    schema.iter().try_for_each(|dt| {
        make_builders(dt, batch_size, prefer_dictionary_ratio)
            .map(|builder| data_builders.push(builder))?;
        Ok::<(), CometError>(())
    })?;

    // Open the output file once and reuse it across batches
    let mut output_data = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&output_path)?;

    // Reusable buffer for serialized batch data
    let mut frozen: Vec<u8> = Vec::new();

    while current_row < row_num {
        let n = std::cmp::min(batch_size, row_num - current_row);

        // Appends rows to the array builders.
        // For each column, iterating over rows and appending values to corresponding array
        // builder.
        for (idx, builder) in data_builders.iter_mut().enumerate() {
            append_columns(
                row_addresses_ptr,
                row_sizes_ptr,
                current_row,
                current_row + n,
                schema,
                idx,
                builder,
                prefer_dictionary_ratio,
            )?;
        }

        // Writes a record batch generated from the array builders to the output file.
        // Note: builder_to_array calls finish() which resets the builder, making it reusable for the next batch.
        let array_refs: Result<Vec<ArrayRef>, _> = data_builders
            .iter_mut()
            .zip(schema.iter())
            .map(|(builder, datatype)| builder_to_array(builder, datatype, prefer_dictionary_ratio))
            .collect();
        let batch = make_batch(array_refs?, n)?;

        frozen.clear();
        let mut cursor = Cursor::new(&mut frozen);

        // we do not collect metrics in Native_writeSortedFileNative
        let ipc_time = Time::default();
        let block_writer = ShuffleBlockWriter::try_new(batch.schema().as_ref(), codec.clone())?;
        written += block_writer.write_batch(&batch, &mut cursor, &ipc_time)?;

        if let Some(checksum) = &mut current_checksum {
            checksum.update(&mut cursor)?;
        }

        output_data.write_all(&frozen)?;
        current_row += n;
    }

    Ok((written as i64, current_checksum.map(|c| c.finalize())))
}

fn builder_to_array(
    builder: &mut Box<dyn ArrayBuilder>,
    datatype: &DataType,
    prefer_dictionary_ratio: f64,
) -> Result<ArrayRef, CometError> {
    match datatype {
        // We don't have redundant dictionary values which are not referenced by any key.
        // So the reasonable ratio must be larger than 1.0.
        DataType::Utf8 if prefer_dictionary_ratio > 1.0 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<StringDictionaryBuilder<Int32Type>>()
                .expect("StringDictionaryBuilder<Int32Type>");

            let dict_array = builder.finish();
            let num_keys = dict_array.keys().len();
            let num_values = dict_array.values().len();

            if num_keys as f64 > num_values as f64 * prefer_dictionary_ratio {
                // The number of keys in the dictionary is less than a ratio of the number of
                // values. The dictionary is efficient, so we return it directly.
                Ok(Arc::new(dict_array))
            } else {
                // If the dictionary is not efficient, we convert it to a plain string array.
                Ok(cast(&dict_array, &DataType::Utf8)?)
            }
        }
        DataType::Binary if prefer_dictionary_ratio > 1.0 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<BinaryDictionaryBuilder<Int32Type>>()
                .expect("BinaryDictionaryBuilder<Int32Type>");

            let dict_array = builder.finish();
            let num_keys = dict_array.keys().len();
            let num_values = dict_array.values().len();

            if num_keys as f64 > num_values as f64 * prefer_dictionary_ratio {
                // The number of keys in the dictionary is less than a ratio of the number of
                // values. The dictionary is efficient, so we return it directly.
                Ok(Arc::new(dict_array))
            } else {
                // If the dictionary is not efficient, we convert it to a plain string array.
                Ok(cast(&dict_array, &DataType::Binary)?)
            }
        }
        _ => Ok(builder.finish()),
    }
}

fn make_batch(arrays: Vec<ArrayRef>, row_count: usize) -> Result<RecordBatch, ArrowError> {
    let fields = arrays
        .iter()
        .enumerate()
        .map(|(i, array)| Field::new(format!("c{i}"), array.data_type().clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let options = RecordBatchOptions::new().with_row_count(Option::from(row_count));
    RecordBatch::try_new_with_options(schema, arrays, &options)
}

#[cfg(test)]
mod test {
    use arrow::datatypes::Fields;

    use super::*;

    #[test]
    fn test_append_null_row_to_struct_builder() {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]));
        let fields = Fields::from(vec![Field::new("st", data_type.clone(), true)]);
        let mut struct_builder = StructBuilder::from_fields(fields, 1);
        let row = SparkUnsafeRow::default();
        append_field(&data_type, &mut struct_builder, &row, 0).expect("append field");
        struct_builder.append_null();
        let struct_array = struct_builder.finish();
        assert_eq!(struct_array.len(), 1);
        assert!(struct_array.is_null(0));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Unaligned memory access in SparkUnsafeRow
    fn test_append_null_struct_field_to_struct_builder() {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]));
        let fields = Fields::from(vec![Field::new("st", data_type.clone(), true)]);
        let mut struct_builder = StructBuilder::from_fields(fields, 1);
        let mut row = SparkUnsafeRow::new_with_num_fields(1);
        let data = [0; 8];
        row.point_to_slice(&data);
        append_field(&data_type, &mut struct_builder, &row, 0).expect("append field");
        struct_builder.append_null();
        let struct_array = struct_builder.finish();
        assert_eq!(struct_array.len(), 1);
        assert!(struct_array.is_null(0));
    }
}
