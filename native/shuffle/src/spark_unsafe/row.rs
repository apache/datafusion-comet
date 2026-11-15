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

use crate::codec_context::ShuffleCodecContext;
use crate::spark_unsafe::unsafe_object::{impl_primitive_accessors, SparkUnsafeObject};
use crate::spark_unsafe::{
    list::append_list_element,
    map::{append_map_elements, get_map_key_value_fields, map_key_value_fields},
};
use crate::writers::Checksum;
use crate::writers::ShuffleBlockWriter;
use arrow::array::{
    builder::{
        ArrayBuilder, BinaryBuilder, BinaryDictionaryBuilder, BooleanBuilder, Date32Builder,
        Decimal128Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
        Int64Builder, Int8Builder, ListBuilder, MapBuilder, NullBuilder, StringBuilder,
        StringDictionaryBuilder, StructBuilder, Time64NanosecondBuilder,
        TimestampMicrosecondBuilder,
    },
    types::Int32Type,
    Array, ArrayRef, RecordBatch, RecordBatchOptions,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::physical_plan::metrics::Time;
use datafusion_comet_jni_bridge::errors::CometError;
use jni::sys::{jint, jlong};
use std::{
    fs::{File, OpenOptions},
    io::{Cursor, Write},
    sync::Arc,
};

const NESTED_TYPE_BUILDER_CAPACITY: usize = 100;

/// A Spark `UnsafeRow` backed by JVM-allocated memory, providing field access by index.
pub struct SparkUnsafeRow {
    row_addr: i64,
    row_size: i32,
    row_bitset_width: i64,
}

impl SparkUnsafeObject for SparkUnsafeRow {
    fn get_row_addr(&self) -> i64 {
        self.row_addr
    }

    fn get_element_offset(&self, index: usize, element_size: usize) -> *const u8 {
        let offset = self.row_bitset_width + (index * 8) as i64;
        debug_assert!(
            self.row_size >= 0 && offset + element_size as i64 <= self.row_size as i64,
            "get_element_offset: access at offset {offset} with size {element_size} \
             exceeds row_size {} for index {index}",
            self.row_size
        );
        (self.row_addr + offset) as *const u8
    }

    // SparkUnsafeRow field offsets are always 8-byte aligned: the base address is 8-byte
    // aligned (JVM guarantee), bitset_width is a multiple of 8, and each field slot is
    // 8 bytes. This means we can safely use aligned ptr::read() for all typed accesses.
    impl_primitive_accessors!(read);
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
    pub(crate) fn point_to(&mut self, row_addr: i64, row_size: i32) {
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
        // The bitset starts at row_addr (8-byte aligned) and each word is at offset 8*k,
        // so word_offset is always 8-byte aligned — we can use aligned ptr::read().
        debug_assert!(self.row_addr != -1, "is_null_at: row not initialized");
        unsafe {
            let mask: i64 = 1i64 << (index & 0x3f);
            let word_offset = (self.row_addr + (((index >> 6) as i64) << 3)) as *const i64;
            let word: i64 = word_offset.read();
            (word & mask) != 0
        }
    }

    /// Unsets the null bit at the given index of the row, i.e., set the bit to 0 (not null).
    pub fn set_not_null_at(&mut self, index: usize) {
        // SAFETY: row_addr points to valid Spark UnsafeRow data with at least
        // ceil(num_fields/64) * 8 bytes of null bitset. The caller ensures index < num_fields.
        // word_offset is within the bitset region since (index >> 6) << 3 < bitset size.
        // Writing is safe because we have mutable access and the memory is owned by the JVM.
        // The bitset is always 8-byte aligned — we can use aligned ptr::read()/write().
        debug_assert!(self.row_addr != -1, "set_not_null_at: row not initialized");
        unsafe {
            let mask: i64 = 1i64 << (index & 0x3f);
            let word_offset = (self.row_addr + (((index >> 6) as i64) << 3)) as *mut i64;
            let word: i64 = word_offset.read();
            word_offset.write(word & !mask);
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
use crate::CompressionCodec;
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
        DataType::Null => {
            let field_builder = get_field_builder!(struct_builder, NullBuilder, idx);
            field_builder.append_null();
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            append_field_to_builder!(
                TimestampMicrosecondBuilder,
                |builder: &mut TimestampMicrosecondBuilder| builder
                    .append_value(row.get_timestamp(idx))
            );
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            append_field_to_builder!(
                Time64NanosecondBuilder,
                |builder: &mut Time64NanosecondBuilder| builder.append_value(row.get_long(idx))
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
    // Reused by every nested struct field. The recursive call only borrows them.
    let mut nested_addresses: Vec<jlong> = Vec::new();
    let mut nested_sizes: Vec<jint> = Vec::new();
    let mut nested_is_null: Vec<bool> = Vec::new();

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
            DataType::Time64(TimeUnit::Nanosecond) => {
                process_field!(
                    Time64NanosecondBuilder,
                    field_idx,
                    |row: &SparkUnsafeRow, idx| row.get_long(idx)
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
                nested_addresses.clear();
                nested_sizes.clear();
                nested_is_null.clear();
                nested_addresses.reserve(num_rows);
                nested_sizes.reserve(num_rows);
                nested_is_null.reserve(num_rows);

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
            // Every value of a NullType field is null, so there is nothing to read from the row.
            DataType::Null => {
                let field_builder = get_field_builder!(struct_builder, NullBuilder, field_idx);
                for _ in 0..num_rows {
                    field_builder.append_null();
                }
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
        debug_assert!(
            !$row_addresses_ptr.is_null(),
            "read_row_at: null row_addresses_ptr"
        );
        debug_assert!(!$row_sizes_ptr.is_null(), "read_row_at: null row_sizes_ptr");
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
        ($builder_type:ty, $append_fn:ident $(, $extra:expr)*) => {{
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
                    array.$append_fn::<true>(values_builder $(, $extra)*);
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
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            process_primitive_lists!(
                TimestampMicrosecondBuilder,
                append_timestamps_to_builder,
                tz
            );
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            process_primitive_lists!(Time64NanosecondBuilder, append_time64s_to_builder);
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
    let (key_field, value_field) = map_key_value_fields(field)?;
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

    // Reused by every nested struct field. The recursive call only borrows them.
    let mut nested_addresses: Vec<jlong> = Vec::new();
    let mut nested_sizes: Vec<jint> = Vec::new();
    let mut nested_is_null: Vec<bool> = Vec::new();

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
            DataType::Time64(TimeUnit::Nanosecond) => {
                process_field!(
                    Time64NanosecondBuilder,
                    field_idx,
                    |row: &SparkUnsafeRow, idx| row.get_long(idx)
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
                nested_addresses.clear();
                nested_sizes.clear();
                nested_is_null.clear();
                nested_addresses.reserve(num_rows);
                nested_sizes.reserve(num_rows);
                nested_is_null.reserve(num_rows);

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
            // Every value of a NullType field is null, so there is nothing to read from the row.
            DataType::Null => {
                let field_builder = get_field_builder!(struct_builder, NullBuilder, field_idx);
                for _ in row_start..row_end {
                    field_builder.append_null();
                }
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
        DataType::Null => {
            let null_builder = downcast_builder_ref!(NullBuilder, builder);
            for _ in row_start..row_end {
                null_builder.append_null();
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            append_column_to_builder!(
                TimestampMicrosecondBuilder,
                |builder: &mut TimestampMicrosecondBuilder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_timestamp(idx))
            );
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            append_column_to_builder!(
                Time64NanosecondBuilder,
                |builder: &mut Time64NanosecondBuilder, row: &SparkUnsafeRow, idx| builder
                    .append_value(row.get_long(idx))
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
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::with_capacity(row_num).with_data_type(dt.clone()))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Box::new(Time64NanosecondBuilder::with_capacity(row_num))
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

/// Bits of Spark's `PackedRecordPointer`: the partition id in the upper 24 bits, then the page
/// number in 13 bits and the offset in the page in the lower 27.
const PACKED_PARTITION_ID_SHIFT: u32 = 40;
const PACKED_PAGE_NUMBER_SHIFT: u32 = 27;
const PACKED_PAGE_NUMBER_MASK: u64 = (1 << 13) - 1;
const PACKED_OFFSET_MASK: u64 = (1 << 27) - 1;

/// Bytes `UnsafeRowSerializer` writes before each row for the row's size.
const SERIALIZED_ROW_SIZE_BYTES: i64 = 4;

/// Encoded blocks are buffered until they reach this many bytes, so that a spill of many small
/// partitions is not written one partition at a time.
const OUTPUT_BUFFER_BYTES: usize = 1 << 20;

/// Converts batches of Spark `UnsafeRow`s to Arrow and appends each batch to a file as a shuffle
/// block. It holds the state that does not depend on the rows, so one encoder serves every
/// partition of a spill: the array builders, the block writer with its pre-encoded schema, the
/// compression context and the open file.
struct SortedRowEncoder<'a> {
    schema: &'a [DataType],
    prefer_dictionary_ratio: f64,
    codec: &'a CompressionCodec,
    offsets_as_lengths: bool,
    /// `builder_to_array` finishes every builder, which resets it for the next batch.
    data_builders: Vec<Box<dyn ArrayBuilder>>,
    /// The schema of the last batch and the block writer that pre-encodes it, kept while the
    /// columns keep their types.
    block_writer: Option<(SchemaRef, ShuffleBlockWriter)>,
    codec_context: ShuffleCodecContext,
    output: File,
    /// Encoded blocks not yet written to `output`. Native memory that no memory pool reserves, like
    /// the builders. Its contents are written out as soon as they reach `OUTPUT_BUFFER_BYTES`, so
    /// they never exceed that size plus one block. The `Vec` keeps the capacity it grew to.
    buffer: Vec<u8>,
    /// Encode and compression time across all batches.
    ipc_time: Time,
}

impl<'a> SortedRowEncoder<'a> {
    /// Creates an encoder that appends to the file at `output_path`, with builders sized for
    /// `capacity` rows.
    fn try_new(
        schema: &'a [DataType],
        capacity: usize,
        prefer_dictionary_ratio: f64,
        codec: &'a CompressionCodec,
        offsets_as_lengths: bool,
        output_path: &str,
    ) -> Result<Self, CometError> {
        let data_builders = schema
            .iter()
            .map(|dt| make_builders(dt, capacity, prefer_dictionary_ratio))
            .collect::<Result<Vec<_>, _>>()?;
        let output = OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_path)?;
        Ok(Self {
            schema,
            prefer_dictionary_ratio,
            codec,
            offsets_as_lengths,
            data_builders,
            block_writer: None,
            codec_context: ShuffleCodecContext::default(),
            output,
            buffer: Vec::new(),
            ipc_time: Time::default(),
        })
    }

    /// Converts rows `row_start..row_end` of the rows at `row_addresses_ptr` and `row_sizes_ptr`
    /// to one batch and buffers its shuffle block, adding the block to `checksum`. Writes the
    /// buffered blocks once they reach `OUTPUT_BUFFER_BYTES`. Returns the block's size.
    ///
    /// # Safety
    ///
    /// As for `append_columns`: both pointers must point to at least `row_end` values, and each
    /// address to a readable row of the corresponding size.
    fn encode_batch(
        &mut self,
        row_addresses_ptr: *mut jlong,
        row_sizes_ptr: *mut jint,
        row_start: usize,
        row_end: usize,
        checksum: Option<&mut Checksum>,
    ) -> Result<usize, CometError> {
        for (idx, builder) in self.data_builders.iter_mut().enumerate() {
            append_columns(
                row_addresses_ptr,
                row_sizes_ptr,
                row_start,
                row_end,
                self.schema,
                idx,
                builder,
                self.prefer_dictionary_ratio,
            )?;
        }
        let arrays = self
            .data_builders
            .iter_mut()
            .zip(self.schema.iter())
            .map(|(builder, datatype)| {
                builder_to_array(builder, datatype, self.prefer_dictionary_ratio)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // The schema changes between batches when a dictionary-encoded column falls back to a
        // plain array (see `builder_to_array`).
        let reusable = self.block_writer.as_ref().is_some_and(|(schema, _)| {
            schema
                .fields()
                .iter()
                .zip(&arrays)
                .all(|(field, array)| field.data_type() == array.data_type())
        });
        if !reusable {
            let fields = arrays
                .iter()
                .enumerate()
                .map(|(i, array)| Field::new(format!("c{i}"), array.data_type().clone(), true))
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(fields));
            let writer = ShuffleBlockWriter::try_new_with_offsets_as_lengths(
                &schema,
                self.codec.clone(),
                self.offsets_as_lengths,
            )?;
            self.block_writer = Some((schema, writer));
        }
        let (schema, writer) = self
            .block_writer
            .as_ref()
            .expect("block writer is set above");
        let options = RecordBatchOptions::new().with_row_count(Some(row_end - row_start));
        let batch = RecordBatch::try_new_with_options(Arc::clone(schema), arrays, &options)?;

        let start = self.buffer.len();
        let mut cursor = Cursor::new(&mut self.buffer);
        cursor.set_position(start as u64);
        let written =
            writer.write_batch(&batch, &mut cursor, &mut self.codec_context, &self.ipc_time)?;
        if let Some(checksum) = checksum {
            checksum.update(&self.buffer[start..]);
        }
        if self.buffer.len() >= OUTPUT_BUFFER_BYTES {
            self.write_buffered()?;
        }
        Ok(written)
    }

    /// Writes the blocks still buffered and returns the encode and compression time across all
    /// batches, in nanoseconds.
    fn finish(mut self) -> Result<i64, CometError> {
        self.write_buffered()?;
        Ok(self.ipc_time.value() as i64)
    }

    fn write_buffered(&mut self) -> Result<(), CometError> {
        self.output.write_all(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }
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
    offsets_as_lengths: bool,
) -> Result<(i64, Option<u32>, i64), CometError> {
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

    let mut encoder = SortedRowEncoder::try_new(
        schema,
        batch_size.min(row_num),
        prefer_dictionary_ratio,
        codec,
        offsets_as_lengths,
        &output_path,
    )?;

    while current_row < row_num {
        let n = std::cmp::min(batch_size, row_num - current_row);
        written += encoder.encode_batch(
            row_addresses_ptr,
            row_sizes_ptr,
            current_row,
            current_row + n,
            current_checksum.as_mut(),
        )?;
        current_row += n;
    }
    let encode_nanos = encoder.finish()?;

    Ok((
        written as i64,
        current_checksum.map(|c| c.finalize()),
        encode_nanos,
    ))
}

/// Processes one sorted spill of the sort-based JVM shuffle writer: converts the rows of every
/// partition in it and appends the partitions' blocks to `output_path`, in partition order.
///
/// `record_pointers` are Spark `PackedRecordPointer`s sorted by partition id, as
/// `ShuffleInMemorySorter` holds them. The page number in a pointer indexes `page_addresses`,
/// the base address of each page the sorter holds, and its offset locates the record in that
/// page. A record is stored as its length (`uao_size` bytes, written by `UnsafeAlignedOffset`),
/// then the row's size (4 bytes, written by `UnsafeRowSerializer`), then the row.
///
/// Sets `partition_lengths[p]` to the bytes written for partition `p`. With a `checksum_algo`,
/// it also continues `partition_checksums[p]` over those bytes, where `i64::MIN` is a checksum not
/// yet started. Partitions without rows keep their values. Returns the encode and compression
/// time in nanoseconds.
///
/// # Safety
/// Every pointer must resolve to a record in one of the pages, holding a readable row.
#[allow(clippy::too_many_arguments)]
pub unsafe fn process_sorted_partitions(
    record_pointers: &[i64],
    page_addresses: &[i64],
    uao_size: usize,
    schema: &[DataType],
    output_path: &str,
    prefer_dictionary_ratio: f64,
    batch_size: usize,
    checksum_algo: Option<i32>,
    partition_checksums: &mut [i64],
    partition_lengths: &mut [i64],
    codec: &CompressionCodec,
    offsets_as_lengths: bool,
) -> Result<i64, CometError> {
    if uao_size != 4 && uao_size != 8 {
        return Err(CometError::Internal(format!(
            "Unsupported record length size {uao_size}"
        )));
    }
    if batch_size == 0 {
        return Err(CometError::Internal(
            "Batch size must be positive".to_string(),
        ));
    }
    let num_partitions = partition_lengths.len();
    if checksum_algo.is_some() && partition_checksums.len() != num_partitions {
        return Err(CometError::Internal(format!(
            "{} partition checksums for {num_partitions} partitions",
            partition_checksums.len()
        )));
    }

    // No batch holds more rows than this.
    let capacity = batch_size.min(record_pointers.len());
    let mut encoder = SortedRowEncoder::try_new(
        schema,
        capacity,
        prefer_dictionary_ratio,
        codec,
        offsets_as_lengths,
        output_path,
    )?;

    // The rows of one batch, resolved from their record pointers.
    let mut row_addresses: Vec<jlong> = Vec::with_capacity(capacity);
    let mut row_sizes: Vec<jint> = Vec::with_capacity(capacity);

    // Each run of pointers with one partition id holds that partition's records.
    let mut next_partition = 0;
    for run in record_pointers.chunk_by(|&a, &b| packed_partition_id(a) == packed_partition_id(b)) {
        let partition = packed_partition_id(run[0]);
        if partition >= num_partitions {
            return Err(CometError::Internal(format!(
                "Shuffle record for partition {partition} of {num_partitions}"
            )));
        }
        if partition < next_partition {
            return Err(CometError::Internal(
                "Shuffle records are not sorted by partition".to_string(),
            ));
        }
        next_partition = partition + 1;

        let mut checksum = checksum_algo
            .map(|algo| {
                let current = partition_checksums[partition];
                Checksum::try_new(algo, (current != i64::MIN).then_some(current as u32))
            })
            .transpose()?;
        let mut written = 0;
        for pointers in run.chunks(batch_size) {
            row_addresses.clear();
            row_sizes.clear();
            for &pointer in pointers {
                let (address, size) = resolve_record(pointer, page_addresses, uao_size)?;
                row_addresses.push(address);
                row_sizes.push(size);
            }
            written += encoder.encode_batch(
                row_addresses.as_mut_ptr(),
                row_sizes.as_mut_ptr(),
                0,
                pointers.len(),
                checksum.as_mut(),
            )?;
        }

        partition_lengths[partition] = written as i64;
        if let Some(checksum) = checksum {
            partition_checksums[partition] = checksum.finalize() as i64;
        }
    }
    encoder.finish()
}

fn packed_partition_id(pointer: i64) -> usize {
    (pointer as u64 >> PACKED_PARTITION_ID_SHIFT) as usize
}

/// Returns the address and size of the row the packed record pointer `pointer` points to.
///
/// # Safety
/// If the pointer's page is in `page_addresses`, its record must be readable.
unsafe fn resolve_record(
    pointer: i64,
    page_addresses: &[i64],
    uao_size: usize,
) -> Result<(jlong, jint), CometError> {
    let pointer = pointer as u64;
    let page_number = ((pointer >> PACKED_PAGE_NUMBER_SHIFT) & PACKED_PAGE_NUMBER_MASK) as usize;
    let page_address = page_addresses
        .get(page_number)
        .copied()
        .filter(|&address| address != 0)
        .ok_or_else(|| {
            CometError::Internal(format!(
                "Shuffle record in page {page_number}, which the sorter does not hold"
            ))
        })?;
    let record_address = page_address + (pointer & PACKED_OFFSET_MASK) as i64;
    let record_length = if uao_size == 4 {
        (record_address as *const i32).read_unaligned()
    } else {
        (record_address as *const i64).read_unaligned() as i32
    };
    let row_size = record_length - SERIALIZED_ROW_SIZE_BYTES as i32;
    if row_size < 0 {
        return Err(CometError::Internal(format!(
            "Shuffle record of {record_length} bytes is too short to hold a row"
        )));
    }
    Ok((
        record_address + uao_size as i64 + SERIALIZED_ROW_SIZE_BYTES,
        row_size,
    ))
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

#[cfg(test)]
mod test {
    use arrow::array::AsArray;
    use arrow::datatypes::{Fields, TimestampMicrosecondType};

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
    fn test_append_null_struct_field_to_struct_builder() {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]));
        let fields = Fields::from(vec![Field::new("st", data_type.clone(), true)]);
        let mut struct_builder = StructBuilder::from_fields(fields, 1);
        let mut row = SparkUnsafeRow::new_with_num_fields(1);
        // 8 bytes null bitset + 8 bytes field value = 16 bytes
        // Set bit 0 in the null bitset to mark field 0 as null
        // Use aligned buffer to match real Spark UnsafeRow layout (8-byte aligned)
        #[repr(align(8))]
        struct Aligned([u8; 16]);
        let mut data = Aligned([0u8; 16]);
        data.0[0] = 1;
        row.point_to_slice(&data.0);
        append_field(&data_type, &mut struct_builder, &row, 0).expect("append field");
        struct_builder.append_null();
        let struct_array = struct_builder.finish();
        assert_eq!(struct_array.len(), 1);
        assert!(struct_array.is_null(0));
    }

    // Spark's `UnsafeRow.getUTF8String` performs no UTF-8 validation, and
    // `cast(BinaryType -> StringType)` is a zero-copy reinterpret -- so a StringType field can
    // hold arbitrary non-UTF-8 bytes. `get_string` must not panic on those; it should decode
    // lossily, matching Spark treating the bytes as opaque.
    #[test]
    fn get_string_tolerates_non_utf8_bytes() {
        // One string field. Row layout: 8-byte null bitset + an 8-byte (offset<<32 | len) slot,
        // then the variable-length region. 8-byte aligned to match a real Spark UnsafeRow.
        #[repr(align(8))]
        struct Aligned([u8; 24]);
        let mut data = Aligned([0u8; 24]);
        // Invalid UTF-8 bytes at offset 16: 0xFF, 0xFE, then ASCII 'A'.
        data.0[16] = 0xFF;
        data.0[17] = 0xFE;
        data.0[18] = b'A';
        // Field 0 slot: offset = 16, len = 3.
        let offset_and_len: i64 = (16i64 << 32) | 3;
        data.0[8..16].copy_from_slice(&offset_and_len.to_ne_bytes());

        let mut row = SparkUnsafeRow::new_with_num_fields(1);
        row.point_to_slice(&data.0);

        // Strict `from_utf8(..).unwrap()` panics here; lossy decode replaces each invalid byte
        // with U+FFFD. `&*` works whether get_string returns `&str` or `Cow<str>`.
        assert_eq!(&*row.get_string(0), "\u{FFFD}\u{FFFD}A");
    }

    /// Lays out an 8-byte-aligned `UnsafeRow` with one array field of 8-byte elements: the null
    /// bitset, the field's offset and size, then the `UnsafeArrayData`. `None` is a null field.
    fn unsafe_row_with_array(values: Option<&[Option<i64>]>) -> Vec<u64> {
        let Some(values) = values else {
            return vec![1, 0];
        };
        let mut null_bitset = vec![0u64; values.len().div_ceil(64)];
        let mut elements = Vec::with_capacity(values.len());
        for (i, value) in values.iter().enumerate() {
            if value.is_none() {
                null_bitset[i / 64] |= 1 << (i % 64);
            }
            elements.push(value.unwrap_or(0) as u64);
        }
        let array_size = 8 * (1 + null_bitset.len() + elements.len()) as u64;
        let mut row = vec![0, (16 << 32) | array_size, values.len() as u64];
        row.extend(null_bitset);
        row.extend(elements);
        row
    }

    #[test]
    fn timestamp_list_column_keeps_its_timezone() {
        // An array of at least MIN_BULK_NULLABLE_APPEND_ELEMENTS elements that holds a null is
        // appended with `append_array`, which panics unless the array has the builder's timezone.
        let data_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )));
        let with_null: Vec<Option<i64>> = (0..70).map(|v| (v != 3).then_some(v << 20)).collect();
        let without_null: Vec<Option<i64>> = (0..70).map(|v| Some(-v)).collect();
        let short = vec![Some(1), None];
        let lists = [
            Some(with_null.as_slice()),
            None,
            Some(short.as_slice()),
            Some(without_null.as_slice()),
        ];
        let rows: Vec<Vec<u64>> = lists.iter().map(|l| unsafe_row_with_array(*l)).collect();
        let mut row_addresses: Vec<jlong> = rows.iter().map(|r| r.as_ptr() as jlong).collect();
        let mut row_sizes: Vec<jint> = rows.iter().map(|r| (r.len() * 8) as jint).collect();

        let schema = [data_type.clone()];
        let mut builder = make_builders(&data_type, rows.len(), 1.0).unwrap();
        append_columns(
            row_addresses.as_mut_ptr(),
            row_sizes.as_mut_ptr(),
            0,
            rows.len(),
            &schema,
            0,
            &mut builder,
            1.0,
        )
        .unwrap();
        let array = builder.finish();

        assert_eq!(array.data_type(), &data_type);
        let appended: Vec<Option<Vec<Option<i64>>>> = array
            .as_list::<i32>()
            .iter()
            .map(|values| {
                values.map(|values| {
                    values
                        .as_primitive::<TimestampMicrosecondType>()
                        .iter()
                        .collect()
                })
            })
            .collect();
        let expected: Vec<Option<Vec<Option<i64>>>> =
            lists.iter().map(|l| l.map(<[_]>::to_vec)).collect();
        assert_eq!(appended, expected);
    }

    #[test]
    fn resolve_record_finds_the_row_a_packed_pointer_points_to() {
        // A page holding two records the way Spark's sorter stores them: the record's length in
        // `uao_size` bytes, then the row's 4-byte size, then a 16-byte row.
        for uao_size in [4_usize, 8] {
            let record_len = uao_size + 4 + 16;
            let mut page = vec![0_u64; (2 * record_len).div_ceil(8)];
            let base = page.as_mut_ptr() as *mut u8;
            for record in 0..2 {
                let offset = record * record_len;
                // SAFETY: both records lie inside `page`.
                unsafe {
                    let length = base.add(offset);
                    if uao_size == 4 {
                        (length as *mut i32).write_unaligned(4 + 16);
                    } else {
                        (length as *mut i64).write_unaligned(4 + 16);
                    }
                    (base.add(offset + uao_size) as *mut i32).write_unaligned(16_i32.to_be());
                }
            }
            // The page is page number 1; page 0 is not held.
            let page_addresses = [0, base as i64];

            let pointer = (5_i64 << 40) | (1 << 27) | record_len as i64;
            assert_eq!(packed_partition_id(pointer), 5);
            let (address, size) = unsafe { resolve_record(pointer, &page_addresses, uao_size) }
                .expect("record in a held page");
            assert_eq!(address, base as i64 + (record_len + uao_size + 4) as i64);
            assert_eq!(size, 16);

            for missing_page in [0_i64, 2] {
                let pointer = (5_i64 << 40) | (missing_page << 27);
                let error = unsafe { resolve_record(pointer, &page_addresses, uao_size) }
                    .unwrap_err()
                    .to_string();
                assert!(error.contains(&format!("page {missing_page}")), "{error}");
            }
        }
    }

    /// Lays out an 8-byte-aligned `UnsafeRow` with one struct field: the null bitset, the field's
    /// offset and size, then the nested `UnsafeRow` holding `fields`. A one-word field is stored
    /// in its slot, a longer one in the variable-length region, and `None` is written as null, as
    /// Spark writes every `NullType` value. `None` for `fields` makes the struct itself null.
    fn unsafe_row_with_struct(fields: Option<&[Option<Vec<u64>>]>) -> Vec<u64> {
        let Some(fields) = fields else {
            return vec![1, 0];
        };
        assert!(fields.len() <= 64);
        let mut nested = vec![0u64; 1 + fields.len()];
        let mut variable: Vec<u64> = Vec::new();
        for (i, field) in fields.iter().enumerate() {
            match field {
                None => nested[0] |= 1 << i,
                Some(value) if value.len() == 1 => nested[1 + i] = value[0],
                Some(value) => {
                    let offset = 8 * (nested.len() + variable.len()) as u64;
                    nested[1 + i] = (offset << 32) | (8 * value.len()) as u64;
                    variable.extend(value);
                }
            }
        }
        nested.extend(variable);
        let mut row = vec![0, (16 << 32) | (8 * nested.len()) as u64];
        row.extend(nested);
        row
    }

    fn struct_column(data_type: &DataType, rows: &[Vec<u64>]) -> ArrayRef {
        let mut row_addresses: Vec<jlong> = rows.iter().map(|r| r.as_ptr() as jlong).collect();
        let mut row_sizes: Vec<jint> = rows.iter().map(|r| (r.len() * 8) as jint).collect();
        let schema = [data_type.clone()];
        let mut builder = make_builders(data_type, rows.len(), 1.0).unwrap();
        append_columns(
            row_addresses.as_mut_ptr(),
            row_sizes.as_mut_ptr(),
            0,
            rows.len(),
            &schema,
            0,
            &mut builder,
            1.0,
        )
        .unwrap();
        builder.finish()
    }

    // `named_struct('x', a, 'n', null)` reaches the field-major struct conversion with a
    // `DataType::Null` field, which it has to accept like `append_field` does.
    #[test]
    fn struct_column_with_null_type_field() {
        let fields = Fields::from(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("n", DataType::Null, true),
        ]);
        let data_type = DataType::Struct(fields.clone());
        let rows = [
            unsafe_row_with_struct(Some(&[Some(vec![7]), None])),
            unsafe_row_with_struct(None),
            unsafe_row_with_struct(Some(&[None, None])),
        ];

        let array = struct_column(&data_type, &rows);

        assert_eq!(array.data_type(), &data_type);
        let st = array.as_struct();
        assert_eq!(st.len(), 3);
        assert_eq!(st.null_count(), 1);
        assert!(st.is_null(1));
        let x: Vec<Option<i32>> = st.column(0).as_primitive::<Int32Type>().iter().collect();
        assert_eq!(x, vec![Some(7), None, None]);
        let n = st.column(1);
        assert_eq!(n.data_type(), &DataType::Null);
        assert_eq!(n.len(), 3);
        assert_eq!(n.logical_null_count(), 3);
    }

    // `named_struct('s', named_struct('x', a, 'n', null))` reaches the nested variant.
    #[test]
    fn nested_struct_column_with_null_type_field() {
        let inner_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("n", DataType::Null, true),
        ]);
        let fields = Fields::from(vec![Field::new(
            "s",
            DataType::Struct(inner_fields.clone()),
            true,
        )]);
        let data_type = DataType::Struct(fields.clone());
        let inner = |x: Option<u64>| {
            // Drop the outer row's bitset and slot: the nested row starts at its own bitset.
            unsafe_row_with_struct(Some(&[x.map(|x| vec![x]), None]))[2..].to_vec()
        };
        let rows = [
            unsafe_row_with_struct(Some(&[Some(inner(Some(7)))])),
            unsafe_row_with_struct(Some(&[None])),
            unsafe_row_with_struct(None),
            unsafe_row_with_struct(Some(&[Some(inner(None))])),
        ];

        let array = struct_column(&data_type, &rows);

        assert_eq!(array.data_type(), &data_type);
        let st = array.as_struct();
        assert_eq!(st.len(), 4);
        assert!(st.is_null(2));
        let s = st.column(0).as_struct();
        assert_eq!(s.null_count(), 2);
        assert!(s.is_null(1) && s.is_null(2));
        let x: Vec<Option<i32>> = s.column(0).as_primitive::<Int32Type>().iter().collect();
        assert_eq!(x, vec![Some(7), None, None, None]);
        let n = s.column(1);
        assert_eq!(n.data_type(), &DataType::Null);
        assert_eq!(n.len(), 4);
        assert_eq!(n.logical_null_count(), 4);
    }
}
