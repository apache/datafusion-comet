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
            list::{append_list_element, SparkUnsafeArray},
            map::{append_map_elements, get_map_key_value_dt, SparkUnsafeMap},
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
    io::{Cursor, Seek, SeekFrom, Write},
    str::from_utf8,
    sync::Arc,
};

const MAX_LONG_DIGITS: u8 = 18;
const NESTED_TYPE_BUILDER_CAPACITY: usize = 100;

/// A common trait for Spark Unsafe classes that can be used to access the underlying data,
/// e.g., `UnsafeRow` and `UnsafeArray`. This defines a set of methods that can be used to
/// access the underlying data with index.
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
    fn get_boolean(&self, index: usize) -> bool {
        let addr = self.get_element_offset(index, 1);
        unsafe { *addr != 0 }
    }

    /// Returns byte value at the given index of the object.
    fn get_byte(&self, index: usize) -> i8 {
        let addr = self.get_element_offset(index, 1);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 1) };
        i8::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns short value at the given index of the object.
    fn get_short(&self, index: usize) -> i16 {
        let addr = self.get_element_offset(index, 2);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 2) };
        i16::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns integer value at the given index of the object.
    fn get_int(&self, index: usize) -> i32 {
        let addr = self.get_element_offset(index, 4);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 4) };
        i32::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns long value at the given index of the object.
    fn get_long(&self, index: usize) -> i64 {
        let addr = self.get_element_offset(index, 8);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 8) };
        i64::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns float value at the given index of the object.
    fn get_float(&self, index: usize) -> f32 {
        let addr = self.get_element_offset(index, 4);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 4) };
        f32::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns double value at the given index of the object.
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
    fn get_date(&self, index: usize) -> i32 {
        let addr = self.get_element_offset(index, 4);
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr, 4) };
        i32::from_le_bytes(slice.try_into().unwrap())
    }

    /// Returns timestamp value at the given index of the object.
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
        ((num_fields + 63) / 64) * 8
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
        unsafe {
            let mask: i64 = 1i64 << (index & 0x3f);
            let word_offset = (self.row_addr + (((index >> 6) as i64) << 3)) as *const i64;
            let word: i64 = *word_offset;
            (word & mask) != 0
        }
    }

    /// Unsets the null bit at the given index of the row, i.e., set the bit to 0 (not null).
    pub fn set_not_null_at(&mut self, index: usize) {
        unsafe {
            let mask: i64 = 1i64 << (index & 0x3f);
            let word_offset = (self.row_addr + (((index >> 6) as i64) << 3)) as *mut i64;
            let word: i64 = *word_offset;
            *word_offset = word & !mask;
        }
    }
}

macro_rules! downcast_builder {
    ($builder_type:ty, $builder:expr) => {
        $builder
            .into_box_any()
            .downcast::<$builder_type>()
            .expect(stringify!($builder_type))
    };
}

macro_rules! downcast_builder_ref {
    ($builder_type:ty, $builder:expr) => {
        $builder
            .as_any_mut()
            .downcast_mut::<$builder_type>()
            .expect(stringify!($builder_type))
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
pub(crate) fn append_field(
    dt: &DataType,
    struct_builder: &mut StructBuilder,
    row: &SparkUnsafeRow,
    idx: usize,
) -> Result<(), CometError> {
    /// A macro for generating code of appending value into field builder of Arrow struct builder.
    macro_rules! append_field_to_builder {
        ($builder_type:ty, $accessor:expr) => {{
            let field_builder = struct_builder.field_builder::<$builder_type>(idx).unwrap();

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

    /// The macros for generating code of appending value into Map field builder of Arrow struct
    /// builder.
    macro_rules! append_map_element {
        ($key_builder_type:ty, $value_builder_type:ty, $field:expr) => {{
            let field_builder = struct_builder
                .field_builder::<MapBuilder<$key_builder_type, $value_builder_type>>(idx)
                .unwrap();

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
                    append_map_elements::<$key_builder_type, $value_builder_type>(
                        $field,
                        field_builder,
                        &row.get_map(idx),
                    )?;
                }
            }
        }};
    }

    /// A macro for generating code of appending value into list field builder of Arrow struct
    /// builder.
    macro_rules! append_list_field_to_builder {
        ($builder_type:ty, $element_dt:expr) => {{
            let field_builder = struct_builder
                .field_builder::<ListBuilder<$builder_type>>(idx)
                .unwrap();

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
                    append_list_element::<$builder_type>(
                        $element_dt,
                        field_builder,
                        &row.get_array(idx),
                    )?
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
            let field_builder = struct_builder.field_builder::<StructBuilder>(idx).unwrap();

            if row.is_null_row() {
                // The row is null.
                field_builder.append_null();
            } else {
                let is_null = row.is_null_at(idx);

                let nested_row = if is_null {
                    // The field in the row is null, i.e., a null nested row.
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
        }
        DataType::Map(field, _) => {
            let (key_dt, value_dt, _) = get_map_key_value_dt(field).unwrap();

            // macro cannot expand to match arm
            match (key_dt, value_dt) {
                (DataType::Boolean, DataType::Boolean) => {
                    append_map_element!(BooleanBuilder, BooleanBuilder, field);
                }
                (DataType::Boolean, DataType::Int8) => {
                    append_map_element!(BooleanBuilder, Int8Builder, field);
                }
                (DataType::Boolean, DataType::Int16) => {
                    append_map_element!(BooleanBuilder, Int16Builder, field);
                }
                (DataType::Boolean, DataType::Int32) => {
                    append_map_element!(BooleanBuilder, Int32Builder, field);
                }
                (DataType::Boolean, DataType::Int64) => {
                    append_map_element!(BooleanBuilder, Int64Builder, field);
                }
                (DataType::Boolean, DataType::Float32) => {
                    append_map_element!(BooleanBuilder, Float32Builder, field);
                }
                (DataType::Boolean, DataType::Float64) => {
                    append_map_element!(BooleanBuilder, Float64Builder, field);
                }
                (DataType::Boolean, DataType::Date32) => {
                    append_map_element!(BooleanBuilder, Date32Builder, field);
                }
                (DataType::Boolean, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(BooleanBuilder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Boolean, DataType::Binary) => {
                    append_map_element!(BooleanBuilder, BinaryBuilder, field);
                }
                (DataType::Boolean, DataType::Utf8) => {
                    append_map_element!(BooleanBuilder, StringBuilder, field);
                }
                (DataType::Boolean, DataType::Decimal128(_, _)) => {
                    append_map_element!(BooleanBuilder, Decimal128Builder, field);
                }
                (DataType::Boolean, DataType::Struct(_)) => {
                    append_map_element!(BooleanBuilder, StructBuilder, field);
                }
                (DataType::Int8, DataType::Boolean) => {
                    append_map_element!(Int8Builder, BooleanBuilder, field);
                }
                (DataType::Int8, DataType::Int8) => {
                    append_map_element!(Int8Builder, Int8Builder, field);
                }
                (DataType::Int8, DataType::Int16) => {
                    append_map_element!(Int8Builder, Int16Builder, field);
                }
                (DataType::Int8, DataType::Int32) => {
                    append_map_element!(Int8Builder, Int32Builder, field);
                }
                (DataType::Int8, DataType::Int64) => {
                    append_map_element!(Int8Builder, Int64Builder, field);
                }
                (DataType::Int8, DataType::Float32) => {
                    append_map_element!(Int8Builder, Float32Builder, field);
                }
                (DataType::Int8, DataType::Float64) => {
                    append_map_element!(Int8Builder, Float64Builder, field);
                }
                (DataType::Int8, DataType::Date32) => {
                    append_map_element!(Int8Builder, Date32Builder, field);
                }
                (DataType::Int8, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(Int8Builder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Int8, DataType::Binary) => {
                    append_map_element!(Int8Builder, BinaryBuilder, field);
                }
                (DataType::Int8, DataType::Utf8) => {
                    append_map_element!(Int8Builder, StringBuilder, field);
                }
                (DataType::Int8, DataType::Decimal128(_, _)) => {
                    append_map_element!(Int8Builder, Decimal128Builder, field);
                }
                (DataType::Int8, DataType::Struct(_)) => {
                    append_map_element!(Int8Builder, StructBuilder, field);
                }
                (DataType::Int16, DataType::Boolean) => {
                    append_map_element!(Int16Builder, BooleanBuilder, field);
                }
                (DataType::Int16, DataType::Int8) => {
                    append_map_element!(Int16Builder, Int8Builder, field);
                }
                (DataType::Int16, DataType::Int16) => {
                    append_map_element!(Int16Builder, Int16Builder, field);
                }
                (DataType::Int16, DataType::Int32) => {
                    append_map_element!(Int16Builder, Int32Builder, field);
                }
                (DataType::Int16, DataType::Int64) => {
                    append_map_element!(Int16Builder, Int64Builder, field);
                }
                (DataType::Int16, DataType::Float32) => {
                    append_map_element!(Int16Builder, Float32Builder, field);
                }
                (DataType::Int16, DataType::Float64) => {
                    append_map_element!(Int16Builder, Float64Builder, field);
                }
                (DataType::Int16, DataType::Date32) => {
                    append_map_element!(Int16Builder, Date32Builder, field);
                }
                (DataType::Int16, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(Int16Builder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Int16, DataType::Binary) => {
                    append_map_element!(Int16Builder, BinaryBuilder, field);
                }
                (DataType::Int16, DataType::Utf8) => {
                    append_map_element!(Int16Builder, StringBuilder, field);
                }
                (DataType::Int16, DataType::Decimal128(_, _)) => {
                    append_map_element!(Int16Builder, Decimal128Builder, field);
                }
                (DataType::Int16, DataType::Struct(_)) => {
                    append_map_element!(Int16Builder, StructBuilder, field);
                }
                (DataType::Int32, DataType::Boolean) => {
                    append_map_element!(Int32Builder, BooleanBuilder, field);
                }
                (DataType::Int32, DataType::Int8) => {
                    append_map_element!(Int32Builder, Int8Builder, field);
                }
                (DataType::Int32, DataType::Int16) => {
                    append_map_element!(Int32Builder, Int16Builder, field);
                }
                (DataType::Int32, DataType::Int32) => {
                    append_map_element!(Int32Builder, Int32Builder, field);
                }
                (DataType::Int32, DataType::Int64) => {
                    append_map_element!(Int32Builder, Int64Builder, field);
                }
                (DataType::Int32, DataType::Float32) => {
                    append_map_element!(Int32Builder, Float32Builder, field);
                }
                (DataType::Int32, DataType::Float64) => {
                    append_map_element!(Int32Builder, Float64Builder, field);
                }
                (DataType::Int32, DataType::Date32) => {
                    append_map_element!(Int32Builder, Date32Builder, field);
                }
                (DataType::Int32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(Int32Builder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Int32, DataType::Binary) => {
                    append_map_element!(Int32Builder, BinaryBuilder, field);
                }
                (DataType::Int32, DataType::Utf8) => {
                    append_map_element!(Int32Builder, StringBuilder, field);
                }
                (DataType::Int32, DataType::Decimal128(_, _)) => {
                    append_map_element!(Int32Builder, Decimal128Builder, field);
                }
                (DataType::Int32, DataType::Struct(_)) => {
                    append_map_element!(Int32Builder, StructBuilder, field);
                }
                (DataType::Int64, DataType::Boolean) => {
                    append_map_element!(Int64Builder, BooleanBuilder, field);
                }
                (DataType::Int64, DataType::Int8) => {
                    append_map_element!(Int64Builder, Int8Builder, field);
                }
                (DataType::Int64, DataType::Int16) => {
                    append_map_element!(Int64Builder, Int16Builder, field);
                }
                (DataType::Int64, DataType::Int32) => {
                    append_map_element!(Int64Builder, Int32Builder, field);
                }
                (DataType::Int64, DataType::Int64) => {
                    append_map_element!(Int64Builder, Int64Builder, field);
                }
                (DataType::Int64, DataType::Float32) => {
                    append_map_element!(Int64Builder, Float32Builder, field);
                }
                (DataType::Int64, DataType::Float64) => {
                    append_map_element!(Int64Builder, Float64Builder, field);
                }
                (DataType::Int64, DataType::Date32) => {
                    append_map_element!(Int64Builder, Date32Builder, field);
                }
                (DataType::Int64, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(Int64Builder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Int64, DataType::Binary) => {
                    append_map_element!(Int64Builder, BinaryBuilder, field);
                }
                (DataType::Int64, DataType::Utf8) => {
                    append_map_element!(Int64Builder, StringBuilder, field);
                }
                (DataType::Int64, DataType::Decimal128(_, _)) => {
                    append_map_element!(Int64Builder, Decimal128Builder, field);
                }
                (DataType::Int64, DataType::Struct(_)) => {
                    append_map_element!(Int64Builder, StructBuilder, field);
                }
                (DataType::Float32, DataType::Boolean) => {
                    append_map_element!(Float32Builder, BooleanBuilder, field);
                }
                (DataType::Float32, DataType::Int8) => {
                    append_map_element!(Float32Builder, Int8Builder, field);
                }
                (DataType::Float32, DataType::Int16) => {
                    append_map_element!(Float32Builder, Int16Builder, field);
                }
                (DataType::Float32, DataType::Int32) => {
                    append_map_element!(Float32Builder, Int32Builder, field);
                }
                (DataType::Float32, DataType::Int64) => {
                    append_map_element!(Float32Builder, Int64Builder, field);
                }
                (DataType::Float32, DataType::Float32) => {
                    append_map_element!(Float32Builder, Float32Builder, field);
                }
                (DataType::Float32, DataType::Float64) => {
                    append_map_element!(Float32Builder, Float64Builder, field);
                }
                (DataType::Float32, DataType::Date32) => {
                    append_map_element!(Float32Builder, Date32Builder, field);
                }
                (DataType::Float32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(Float32Builder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Float32, DataType::Binary) => {
                    append_map_element!(Float32Builder, BinaryBuilder, field);
                }
                (DataType::Float32, DataType::Utf8) => {
                    append_map_element!(Float32Builder, StringBuilder, field);
                }
                (DataType::Float32, DataType::Decimal128(_, _)) => {
                    append_map_element!(Float32Builder, Decimal128Builder, field);
                }
                (DataType::Float32, DataType::Struct(_)) => {
                    append_map_element!(Float32Builder, StructBuilder, field);
                }
                (DataType::Float64, DataType::Boolean) => {
                    append_map_element!(Float64Builder, BooleanBuilder, field);
                }
                (DataType::Float64, DataType::Int8) => {
                    append_map_element!(Float64Builder, Int8Builder, field);
                }
                (DataType::Float64, DataType::Int16) => {
                    append_map_element!(Float64Builder, Int16Builder, field);
                }
                (DataType::Float64, DataType::Int32) => {
                    append_map_element!(Float64Builder, Int32Builder, field);
                }
                (DataType::Float64, DataType::Int64) => {
                    append_map_element!(Float64Builder, Int64Builder, field);
                }
                (DataType::Float64, DataType::Float32) => {
                    append_map_element!(Float64Builder, Float32Builder, field);
                }
                (DataType::Float64, DataType::Float64) => {
                    append_map_element!(Float64Builder, Float64Builder, field);
                }
                (DataType::Float64, DataType::Date32) => {
                    append_map_element!(Float64Builder, Date32Builder, field);
                }
                (DataType::Float64, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(Float64Builder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Float64, DataType::Binary) => {
                    append_map_element!(Float64Builder, BinaryBuilder, field);
                }
                (DataType::Float64, DataType::Utf8) => {
                    append_map_element!(Float64Builder, StringBuilder, field);
                }
                (DataType::Float64, DataType::Decimal128(_, _)) => {
                    append_map_element!(Float64Builder, Decimal128Builder, field);
                }
                (DataType::Float64, DataType::Struct(_)) => {
                    append_map_element!(Float64Builder, StructBuilder, field);
                }
                (DataType::Date32, DataType::Boolean) => {
                    append_map_element!(Date32Builder, BooleanBuilder, field);
                }
                (DataType::Date32, DataType::Int8) => {
                    append_map_element!(Date32Builder, Int8Builder, field);
                }
                (DataType::Date32, DataType::Int16) => {
                    append_map_element!(Date32Builder, Int16Builder, field);
                }
                (DataType::Date32, DataType::Int32) => {
                    append_map_element!(Date32Builder, Int32Builder, field);
                }
                (DataType::Date32, DataType::Int64) => {
                    append_map_element!(Date32Builder, Int64Builder, field);
                }
                (DataType::Date32, DataType::Float32) => {
                    append_map_element!(Date32Builder, Float32Builder, field);
                }
                (DataType::Date32, DataType::Float64) => {
                    append_map_element!(Date32Builder, Float64Builder, field);
                }
                (DataType::Date32, DataType::Date32) => {
                    append_map_element!(Date32Builder, Date32Builder, field);
                }
                (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(Date32Builder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Date32, DataType::Binary) => {
                    append_map_element!(Date32Builder, BinaryBuilder, field);
                }
                (DataType::Date32, DataType::Utf8) => {
                    append_map_element!(Date32Builder, StringBuilder, field);
                }
                (DataType::Date32, DataType::Decimal128(_, _)) => {
                    append_map_element!(Date32Builder, Decimal128Builder, field);
                }
                (DataType::Date32, DataType::Struct(_)) => {
                    append_map_element!(Date32Builder, StructBuilder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Boolean) => {
                    append_map_element!(TimestampMicrosecondBuilder, BooleanBuilder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int8) => {
                    append_map_element!(TimestampMicrosecondBuilder, Int8Builder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int16) => {
                    append_map_element!(TimestampMicrosecondBuilder, Int16Builder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int32) => {
                    append_map_element!(TimestampMicrosecondBuilder, Int32Builder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int64) => {
                    append_map_element!(TimestampMicrosecondBuilder, Int64Builder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Float32) => {
                    append_map_element!(TimestampMicrosecondBuilder, Float32Builder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Float64) => {
                    append_map_element!(TimestampMicrosecondBuilder, Float64Builder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Date32) => {
                    append_map_element!(TimestampMicrosecondBuilder, Date32Builder, field);
                }
                (
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                ) => {
                    append_map_element!(
                        TimestampMicrosecondBuilder,
                        TimestampMicrosecondBuilder,
                        field
                    );
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Binary) => {
                    append_map_element!(TimestampMicrosecondBuilder, BinaryBuilder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Utf8) => {
                    append_map_element!(TimestampMicrosecondBuilder, StringBuilder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Decimal128(_, _)) => {
                    append_map_element!(TimestampMicrosecondBuilder, Decimal128Builder, field);
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Struct(_)) => {
                    append_map_element!(TimestampMicrosecondBuilder, StructBuilder, field);
                }
                (DataType::Binary, DataType::Boolean) => {
                    append_map_element!(BinaryBuilder, BooleanBuilder, field);
                }
                (DataType::Binary, DataType::Int8) => {
                    append_map_element!(BinaryBuilder, Int8Builder, field);
                }
                (DataType::Binary, DataType::Int16) => {
                    append_map_element!(BinaryBuilder, Int16Builder, field);
                }
                (DataType::Binary, DataType::Int32) => {
                    append_map_element!(BinaryBuilder, Int32Builder, field);
                }
                (DataType::Binary, DataType::Int64) => {
                    append_map_element!(BinaryBuilder, Int64Builder, field);
                }
                (DataType::Binary, DataType::Float32) => {
                    append_map_element!(BinaryBuilder, Float32Builder, field);
                }
                (DataType::Binary, DataType::Float64) => {
                    append_map_element!(BinaryBuilder, Float64Builder, field);
                }
                (DataType::Binary, DataType::Date32) => {
                    append_map_element!(BinaryBuilder, Date32Builder, field);
                }
                (DataType::Binary, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(BinaryBuilder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Binary, DataType::Binary) => {
                    append_map_element!(BinaryBuilder, BinaryBuilder, field);
                }
                (DataType::Binary, DataType::Utf8) => {
                    append_map_element!(BinaryBuilder, StringBuilder, field);
                }
                (DataType::Binary, DataType::Decimal128(_, _)) => {
                    append_map_element!(BinaryBuilder, Decimal128Builder, field);
                }
                (DataType::Binary, DataType::Struct(_)) => {
                    append_map_element!(BinaryBuilder, StructBuilder, field);
                }
                (DataType::Utf8, DataType::Boolean) => {
                    append_map_element!(StringBuilder, BooleanBuilder, field);
                }
                (DataType::Utf8, DataType::Int8) => {
                    append_map_element!(StringBuilder, Int8Builder, field);
                }
                (DataType::Utf8, DataType::Int16) => {
                    append_map_element!(StringBuilder, Int16Builder, field);
                }
                (DataType::Utf8, DataType::Int32) => {
                    append_map_element!(StringBuilder, Int32Builder, field);
                }
                (DataType::Utf8, DataType::Int64) => {
                    append_map_element!(StringBuilder, Int64Builder, field);
                }
                (DataType::Utf8, DataType::Float32) => {
                    append_map_element!(StringBuilder, Float32Builder, field);
                }
                (DataType::Utf8, DataType::Float64) => {
                    append_map_element!(StringBuilder, Float64Builder, field);
                }
                (DataType::Utf8, DataType::Date32) => {
                    append_map_element!(StringBuilder, Date32Builder, field);
                }
                (DataType::Utf8, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(StringBuilder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Utf8, DataType::Binary) => {
                    append_map_element!(StringBuilder, BinaryBuilder, field);
                }
                (DataType::Utf8, DataType::Utf8) => {
                    append_map_element!(StringBuilder, StringBuilder, field);
                }
                (DataType::Utf8, DataType::Decimal128(_, _)) => {
                    append_map_element!(StringBuilder, Decimal128Builder, field);
                }
                (DataType::Utf8, DataType::Struct(_)) => {
                    append_map_element!(StringBuilder, StructBuilder, field);
                }
                (DataType::Decimal128(_, _), DataType::Boolean) => {
                    append_map_element!(Decimal128Builder, BooleanBuilder, field);
                }
                (DataType::Decimal128(_, _), DataType::Int8) => {
                    append_map_element!(Decimal128Builder, Int8Builder, field);
                }
                (DataType::Decimal128(_, _), DataType::Int16) => {
                    append_map_element!(Decimal128Builder, Int16Builder, field);
                }
                (DataType::Decimal128(_, _), DataType::Int32) => {
                    append_map_element!(Decimal128Builder, Int32Builder, field);
                }
                (DataType::Decimal128(_, _), DataType::Int64) => {
                    append_map_element!(Decimal128Builder, Int64Builder, field);
                }
                (DataType::Decimal128(_, _), DataType::Float32) => {
                    append_map_element!(Decimal128Builder, Float32Builder, field);
                }
                (DataType::Decimal128(_, _), DataType::Float64) => {
                    append_map_element!(Decimal128Builder, Float64Builder, field);
                }
                (DataType::Decimal128(_, _), DataType::Date32) => {
                    append_map_element!(Decimal128Builder, Date32Builder, field);
                }
                (DataType::Decimal128(_, _), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(Decimal128Builder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Decimal128(_, _), DataType::Binary) => {
                    append_map_element!(Decimal128Builder, BinaryBuilder, field);
                }
                (DataType::Decimal128(_, _), DataType::Utf8) => {
                    append_map_element!(Decimal128Builder, StringBuilder, field);
                }
                (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
                    append_map_element!(Decimal128Builder, Decimal128Builder, field);
                }
                (DataType::Decimal128(_, _), DataType::Struct(_)) => {
                    append_map_element!(Decimal128Builder, StructBuilder, field);
                }
                (DataType::Struct(_), DataType::Boolean) => {
                    append_map_element!(StructBuilder, BooleanBuilder, field);
                }
                (DataType::Struct(_), DataType::Int8) => {
                    append_map_element!(StructBuilder, Int8Builder, field);
                }
                (DataType::Struct(_), DataType::Int16) => {
                    append_map_element!(StructBuilder, Int16Builder, field);
                }
                (DataType::Struct(_), DataType::Int32) => {
                    append_map_element!(StructBuilder, Int32Builder, field);
                }
                (DataType::Struct(_), DataType::Int64) => {
                    append_map_element!(StructBuilder, Int64Builder, field);
                }
                (DataType::Struct(_), DataType::Float32) => {
                    append_map_element!(StructBuilder, Float32Builder, field);
                }
                (DataType::Struct(_), DataType::Float64) => {
                    append_map_element!(StructBuilder, Float64Builder, field);
                }
                (DataType::Struct(_), DataType::Date32) => {
                    append_map_element!(StructBuilder, Date32Builder, field);
                }
                (DataType::Struct(_), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_map_element!(StructBuilder, TimestampMicrosecondBuilder, field);
                }
                (DataType::Struct(_), DataType::Binary) => {
                    append_map_element!(StructBuilder, BinaryBuilder, field);
                }
                (DataType::Struct(_), DataType::Utf8) => {
                    append_map_element!(StructBuilder, StringBuilder, field);
                }
                (DataType::Struct(_), DataType::Decimal128(_, _)) => {
                    append_map_element!(StructBuilder, Decimal128Builder, field);
                }
                (DataType::Struct(_), DataType::Struct(_)) => {
                    append_map_element!(StructBuilder, StructBuilder, field);
                }
                _ => {
                    unreachable!(
                        "Unsupported data type of map key and value: {:?} and {:?}",
                        key_dt, value_dt
                    )
                }
            }
        }
        DataType::List(field) => match field.data_type() {
            DataType::Boolean => {
                append_list_field_to_builder!(BooleanBuilder, field.data_type());
            }
            DataType::Int8 => {
                append_list_field_to_builder!(Int8Builder, field.data_type());
            }
            DataType::Int16 => {
                append_list_field_to_builder!(Int16Builder, field.data_type());
            }
            DataType::Int32 => {
                append_list_field_to_builder!(Int32Builder, field.data_type());
            }
            DataType::Int64 => {
                append_list_field_to_builder!(Int64Builder, field.data_type());
            }
            DataType::Float32 => {
                append_list_field_to_builder!(Float32Builder, field.data_type());
            }
            DataType::Float64 => {
                append_list_field_to_builder!(Float64Builder, field.data_type());
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                append_list_field_to_builder!(TimestampMicrosecondBuilder, field.data_type());
            }
            DataType::Date32 => {
                append_list_field_to_builder!(Date32Builder, field.data_type());
            }
            DataType::Binary => {
                append_list_field_to_builder!(BinaryBuilder, field.data_type());
            }
            DataType::Utf8 => {
                append_list_field_to_builder!(StringBuilder, field.data_type());
            }
            DataType::Struct(_) => {
                append_list_field_to_builder!(StructBuilder, field.data_type());
            }
            DataType::Decimal128(_, _) => {
                append_list_field_to_builder!(Decimal128Builder, field.data_type());
            }
            _ => unreachable!("Unsupported data type of struct field: {:?}", dt),
        },
        _ => {
            unreachable!("Unsupported data type of struct field: {:?}", dt)
        }
    }

    Ok(())
}

/// Appends column of top rows to the given array builder.
#[allow(clippy::redundant_closure_call, clippy::too_many_arguments)]
pub(crate) fn append_columns(
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
                let row_addr = unsafe { *row_addresses_ptr.add(i) };
                let row_size = unsafe { *row_sizes_ptr.add(i) };
                row.point_to(row_addr, row_size);

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

    /// A macro for generating code of appending values into Arrow `ListBuilder`.
    macro_rules! append_column_to_list_builder {
        ($builder_type:ty, $element_dt:expr) => {{
            let list_builder = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<$builder_type>>()
                .expect(stringify!($builder_type));
            let mut row = SparkUnsafeRow::new(schema);

            for i in row_start..row_end {
                let row_addr = unsafe { *row_addresses_ptr.add(i) };
                let row_size = unsafe { *row_sizes_ptr.add(i) };
                row.point_to(row_addr, row_size);

                let is_null = row.is_null_at(column_idx);

                if is_null {
                    // The list is null.
                    // Append a null value to the list builder.
                    list_builder.append_null();
                } else {
                    append_list_element::<$builder_type>(
                        $element_dt,
                        list_builder,
                        &row.get_array(column_idx),
                    )?
                }
            }
        }};
    }

    /// A macro for generating code of appending values into Arrow `MapBuilder`.
    macro_rules! append_column_to_map_builder {
        ($key_builder_type:ty, $value_builder_type:ty, $field:expr) => {{
            let map_builder = builder
                .as_any_mut()
                .downcast_mut::<MapBuilder<$key_builder_type, $value_builder_type>>()
                .expect(&format!(
                    "MapBuilder<{},{}>",
                    stringify!($key_builder_type),
                    stringify!($value_builder_type)
                ));
            let mut row = SparkUnsafeRow::new(schema);

            for i in row_start..row_end {
                let row_addr = unsafe { *row_addresses_ptr.add(i) };
                let row_size = unsafe { *row_sizes_ptr.add(i) };
                row.point_to(row_addr, row_size);

                let is_null = row.is_null_at(column_idx);

                if is_null {
                    // The map is null.
                    // Append a null value to the map builder.
                    map_builder.append(false)?;
                } else {
                    append_map_elements::<$key_builder_type, $value_builder_type>(
                        $field,
                        map_builder,
                        &row.get_map(column_idx),
                    )?
                }
            }
        }};
    }

    /// A macro for generating code of appending values into Arrow `StructBuilder`.
    macro_rules! append_column_to_struct_builder {
        ($fields:expr) => {{
            let struct_builder = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .expect("StructBuilder");
            let mut row = SparkUnsafeRow::new(schema);

            for i in row_start..row_end {
                let row_addr = unsafe { *row_addresses_ptr.add(i) };
                let row_size = unsafe { *row_sizes_ptr.add(i) };
                row.point_to(row_addr, row_size);

                let is_null = row.is_null_at(column_idx);

                let nested_row = if is_null {
                    // The struct is null.
                    // Append a null value to the struct builder and field builders.
                    struct_builder.append_null();
                    SparkUnsafeRow::default()
                } else {
                    struct_builder.append(true);
                    row.get_struct(column_idx, $fields.len())
                };

                for (idx, field) in $fields.into_iter().enumerate() {
                    append_field(field.data_type(), struct_builder, &nested_row, idx)?;
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
            let (key_dt, value_dt, _) = get_map_key_value_dt(field)?;

            // macro cannot expand to match arm
            match (key_dt, value_dt) {
                (DataType::Boolean, DataType::Boolean) => {
                    append_column_to_map_builder!(BooleanBuilder, BooleanBuilder, field)
                }
                (DataType::Boolean, DataType::Int8) => {
                    append_column_to_map_builder!(BooleanBuilder, Int8Builder, field)
                }
                (DataType::Boolean, DataType::Int16) => {
                    append_column_to_map_builder!(BooleanBuilder, Int16Builder, field)
                }
                (DataType::Boolean, DataType::Int32) => {
                    append_column_to_map_builder!(BooleanBuilder, Int32Builder, field)
                }
                (DataType::Boolean, DataType::Int64) => {
                    append_column_to_map_builder!(BooleanBuilder, Int64Builder, field)
                }
                (DataType::Boolean, DataType::Float32) => {
                    append_column_to_map_builder!(BooleanBuilder, Float32Builder, field)
                }
                (DataType::Boolean, DataType::Float64) => {
                    append_column_to_map_builder!(BooleanBuilder, Float64Builder, field)
                }
                (DataType::Boolean, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(
                        BooleanBuilder,
                        TimestampMicrosecondBuilder,
                        field
                    )
                }
                (DataType::Boolean, DataType::Date32) => {
                    append_column_to_map_builder!(BooleanBuilder, Date32Builder, field)
                }
                (DataType::Boolean, DataType::Binary) => {
                    append_column_to_map_builder!(BooleanBuilder, BinaryBuilder, field)
                }
                (DataType::Boolean, DataType::Utf8) => {
                    append_column_to_map_builder!(BooleanBuilder, StringBuilder, field)
                }
                (DataType::Boolean, DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(BooleanBuilder, Decimal128Builder, field)
                }
                (DataType::Int8, DataType::Boolean) => {
                    append_column_to_map_builder!(Int8Builder, BooleanBuilder, field)
                }
                (DataType::Int8, DataType::Int8) => {
                    append_column_to_map_builder!(Int8Builder, Int8Builder, field)
                }
                (DataType::Int8, DataType::Int16) => {
                    append_column_to_map_builder!(Int8Builder, Int16Builder, field)
                }
                (DataType::Int8, DataType::Int32) => {
                    append_column_to_map_builder!(Int8Builder, Int32Builder, field)
                }
                (DataType::Int8, DataType::Int64) => {
                    append_column_to_map_builder!(Int8Builder, Int64Builder, field)
                }
                (DataType::Int8, DataType::Float32) => {
                    append_column_to_map_builder!(Int8Builder, Float32Builder, field)
                }
                (DataType::Int8, DataType::Float64) => {
                    append_column_to_map_builder!(Int8Builder, Float64Builder, field)
                }
                (DataType::Int8, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(Int8Builder, TimestampMicrosecondBuilder, field)
                }
                (DataType::Int8, DataType::Date32) => {
                    append_column_to_map_builder!(Int8Builder, Date32Builder, field)
                }
                (DataType::Int8, DataType::Binary) => {
                    append_column_to_map_builder!(Int8Builder, BinaryBuilder, field)
                }
                (DataType::Int8, DataType::Utf8) => {
                    append_column_to_map_builder!(Int8Builder, StringBuilder, field)
                }
                (DataType::Int8, DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(Int8Builder, Decimal128Builder, field)
                }
                (DataType::Int16, DataType::Boolean) => {
                    append_column_to_map_builder!(Int16Builder, BooleanBuilder, field)
                }
                (DataType::Int16, DataType::Int8) => {
                    append_column_to_map_builder!(Int16Builder, Int8Builder, field)
                }
                (DataType::Int16, DataType::Int16) => {
                    append_column_to_map_builder!(Int16Builder, Int16Builder, field)
                }
                (DataType::Int16, DataType::Int32) => {
                    append_column_to_map_builder!(Int16Builder, Int32Builder, field)
                }
                (DataType::Int16, DataType::Int64) => {
                    append_column_to_map_builder!(Int16Builder, Int64Builder, field)
                }
                (DataType::Int16, DataType::Float32) => {
                    append_column_to_map_builder!(Int16Builder, Float32Builder, field)
                }
                (DataType::Int16, DataType::Float64) => {
                    append_column_to_map_builder!(Int16Builder, Float64Builder, field)
                }
                (DataType::Int16, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(Int16Builder, TimestampMicrosecondBuilder, field)
                }
                (DataType::Int16, DataType::Date32) => {
                    append_column_to_map_builder!(Int16Builder, Date32Builder, field)
                }
                (DataType::Int16, DataType::Binary) => {
                    append_column_to_map_builder!(Int16Builder, BinaryBuilder, field)
                }
                (DataType::Int16, DataType::Utf8) => {
                    append_column_to_map_builder!(Int16Builder, StringBuilder, field)
                }
                (DataType::Int16, DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(Int16Builder, Decimal128Builder, field)
                }
                (DataType::Int32, DataType::Boolean) => {
                    append_column_to_map_builder!(Int32Builder, BooleanBuilder, field)
                }
                (DataType::Int32, DataType::Int8) => {
                    append_column_to_map_builder!(Int32Builder, Int8Builder, field)
                }
                (DataType::Int32, DataType::Int16) => {
                    append_column_to_map_builder!(Int32Builder, Int16Builder, field)
                }
                (DataType::Int32, DataType::Int32) => {
                    append_column_to_map_builder!(Int32Builder, Int32Builder, field)
                }
                (DataType::Int32, DataType::Int64) => {
                    append_column_to_map_builder!(Int32Builder, Int64Builder, field)
                }
                (DataType::Int32, DataType::Float32) => {
                    append_column_to_map_builder!(Int32Builder, Float32Builder, field)
                }
                (DataType::Int32, DataType::Float64) => {
                    append_column_to_map_builder!(Int32Builder, Float64Builder, field)
                }
                (DataType::Int32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(Int32Builder, TimestampMicrosecondBuilder, field)
                }
                (DataType::Int32, DataType::Date32) => {
                    append_column_to_map_builder!(Int32Builder, Date32Builder, field)
                }
                (DataType::Int32, DataType::Binary) => {
                    append_column_to_map_builder!(Int32Builder, BinaryBuilder, field)
                }
                (DataType::Int32, DataType::Utf8) => {
                    append_column_to_map_builder!(Int32Builder, StringBuilder, field)
                }
                (DataType::Int32, DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(Int32Builder, Decimal128Builder, field)
                }
                (DataType::Int64, DataType::Boolean) => {
                    append_column_to_map_builder!(Int64Builder, BooleanBuilder, field)
                }
                (DataType::Int64, DataType::Int8) => {
                    append_column_to_map_builder!(Int64Builder, Int8Builder, field)
                }
                (DataType::Int64, DataType::Int16) => {
                    append_column_to_map_builder!(Int64Builder, Int16Builder, field)
                }
                (DataType::Int64, DataType::Int32) => {
                    append_column_to_map_builder!(Int64Builder, Int32Builder, field)
                }
                (DataType::Int64, DataType::Int64) => {
                    append_column_to_map_builder!(Int64Builder, Int64Builder, field)
                }
                (DataType::Int64, DataType::Float32) => {
                    append_column_to_map_builder!(Int64Builder, Float32Builder, field)
                }
                (DataType::Int64, DataType::Float64) => {
                    append_column_to_map_builder!(Int64Builder, Float64Builder, field)
                }
                (DataType::Int64, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(Int64Builder, TimestampMicrosecondBuilder, field)
                }
                (DataType::Int64, DataType::Date32) => {
                    append_column_to_map_builder!(Int64Builder, Date32Builder, field)
                }
                (DataType::Int64, DataType::Binary) => {
                    append_column_to_map_builder!(Int64Builder, BinaryBuilder, field)
                }
                (DataType::Int64, DataType::Utf8) => {
                    append_column_to_map_builder!(Int64Builder, StringBuilder, field)
                }
                (DataType::Int64, DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(Int64Builder, Decimal128Builder, field)
                }
                (DataType::Float32, DataType::Boolean) => {
                    append_column_to_map_builder!(Float32Builder, BooleanBuilder, field)
                }
                (DataType::Float32, DataType::Int8) => {
                    append_column_to_map_builder!(Float32Builder, Int8Builder, field)
                }
                (DataType::Float32, DataType::Int16) => {
                    append_column_to_map_builder!(Float32Builder, Int16Builder, field)
                }
                (DataType::Float32, DataType::Int32) => {
                    append_column_to_map_builder!(Float32Builder, Int32Builder, field)
                }
                (DataType::Float32, DataType::Int64) => {
                    append_column_to_map_builder!(Float32Builder, Int64Builder, field)
                }
                (DataType::Float32, DataType::Float32) => {
                    append_column_to_map_builder!(Float32Builder, Float32Builder, field)
                }
                (DataType::Float32, DataType::Float64) => {
                    append_column_to_map_builder!(Float32Builder, Float64Builder, field)
                }
                (DataType::Float32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(
                        Float32Builder,
                        TimestampMicrosecondBuilder,
                        field
                    )
                }
                (DataType::Float32, DataType::Date32) => {
                    append_column_to_map_builder!(Float32Builder, Date32Builder, field)
                }
                (DataType::Float32, DataType::Binary) => {
                    append_column_to_map_builder!(Float32Builder, BinaryBuilder, field)
                }
                (DataType::Float32, DataType::Utf8) => {
                    append_column_to_map_builder!(Float32Builder, StringBuilder, field)
                }
                (DataType::Float32, DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(Float32Builder, Decimal128Builder, field)
                }
                (DataType::Float64, DataType::Boolean) => {
                    append_column_to_map_builder!(Float64Builder, BooleanBuilder, field)
                }
                (DataType::Float64, DataType::Int8) => {
                    append_column_to_map_builder!(Float64Builder, Int8Builder, field)
                }
                (DataType::Float64, DataType::Int16) => {
                    append_column_to_map_builder!(Float64Builder, Int16Builder, field)
                }
                (DataType::Float64, DataType::Int32) => {
                    append_column_to_map_builder!(Float64Builder, Int32Builder, field)
                }
                (DataType::Float64, DataType::Int64) => {
                    append_column_to_map_builder!(Float64Builder, Int64Builder, field)
                }
                (DataType::Float64, DataType::Float32) => {
                    append_column_to_map_builder!(Float64Builder, Float32Builder, field)
                }
                (DataType::Float64, DataType::Float64) => {
                    append_column_to_map_builder!(Float64Builder, Float64Builder, field)
                }
                (DataType::Float64, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(
                        Float64Builder,
                        TimestampMicrosecondBuilder,
                        field
                    )
                }
                (DataType::Float64, DataType::Date32) => {
                    append_column_to_map_builder!(Float64Builder, Date32Builder, field)
                }
                (DataType::Float64, DataType::Binary) => {
                    append_column_to_map_builder!(Float64Builder, BinaryBuilder, field)
                }
                (DataType::Float64, DataType::Utf8) => {
                    append_column_to_map_builder!(Float64Builder, StringBuilder, field)
                }
                (DataType::Float64, DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(Float64Builder, Decimal128Builder, field)
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Boolean) => {
                    append_column_to_map_builder!(
                        TimestampMicrosecondBuilder,
                        BooleanBuilder,
                        field
                    )
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int8) => {
                    append_column_to_map_builder!(TimestampMicrosecondBuilder, Int8Builder, field)
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int16) => {
                    append_column_to_map_builder!(TimestampMicrosecondBuilder, Int16Builder, field)
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int32) => {
                    append_column_to_map_builder!(TimestampMicrosecondBuilder, Int32Builder, field)
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int64) => {
                    append_column_to_map_builder!(TimestampMicrosecondBuilder, Int64Builder, field)
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Float32) => {
                    append_column_to_map_builder!(
                        TimestampMicrosecondBuilder,
                        Float32Builder,
                        field
                    )
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Float64) => {
                    append_column_to_map_builder!(
                        TimestampMicrosecondBuilder,
                        Float64Builder,
                        field
                    )
                }
                (
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                ) => {
                    append_column_to_map_builder!(
                        TimestampMicrosecondBuilder,
                        TimestampMicrosecondBuilder,
                        field
                    )
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Date32) => {
                    append_column_to_map_builder!(TimestampMicrosecondBuilder, Date32Builder, field)
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Binary) => {
                    append_column_to_map_builder!(TimestampMicrosecondBuilder, BinaryBuilder, field)
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Utf8) => {
                    append_column_to_map_builder!(TimestampMicrosecondBuilder, StringBuilder, field)
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(
                        TimestampMicrosecondBuilder,
                        Decimal128Builder,
                        field
                    )
                }
                (DataType::Date32, DataType::Boolean) => {
                    append_column_to_map_builder!(Date32Builder, BooleanBuilder, field)
                }
                (DataType::Date32, DataType::Int8) => {
                    append_column_to_map_builder!(Date32Builder, Int8Builder, field)
                }
                (DataType::Date32, DataType::Int16) => {
                    append_column_to_map_builder!(Date32Builder, Int16Builder, field)
                }
                (DataType::Date32, DataType::Int32) => {
                    append_column_to_map_builder!(Date32Builder, Int32Builder, field)
                }
                (DataType::Date32, DataType::Int64) => {
                    append_column_to_map_builder!(Date32Builder, Int64Builder, field)
                }
                (DataType::Date32, DataType::Float32) => {
                    append_column_to_map_builder!(Date32Builder, Float32Builder, field)
                }
                (DataType::Date32, DataType::Float64) => {
                    append_column_to_map_builder!(Date32Builder, Float64Builder, field)
                }
                (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(Date32Builder, TimestampMicrosecondBuilder, field)
                }
                (DataType::Date32, DataType::Date32) => {
                    append_column_to_map_builder!(Date32Builder, Date32Builder, field)
                }
                (DataType::Date32, DataType::Binary) => {
                    append_column_to_map_builder!(Date32Builder, BinaryBuilder, field)
                }
                (DataType::Date32, DataType::Utf8) => {
                    append_column_to_map_builder!(Date32Builder, StringBuilder, field)
                }
                (DataType::Date32, DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(Date32Builder, Decimal128Builder, field)
                }
                (DataType::Binary, DataType::Boolean) => {
                    append_column_to_map_builder!(BinaryBuilder, BooleanBuilder, field)
                }
                (DataType::Binary, DataType::Int8) => {
                    append_column_to_map_builder!(BinaryBuilder, Int8Builder, field)
                }
                (DataType::Binary, DataType::Int16) => {
                    append_column_to_map_builder!(BinaryBuilder, Int16Builder, field)
                }
                (DataType::Binary, DataType::Int32) => {
                    append_column_to_map_builder!(BinaryBuilder, Int32Builder, field)
                }
                (DataType::Binary, DataType::Int64) => {
                    append_column_to_map_builder!(BinaryBuilder, Int64Builder, field)
                }
                (DataType::Binary, DataType::Float32) => {
                    append_column_to_map_builder!(BinaryBuilder, Float32Builder, field)
                }
                (DataType::Binary, DataType::Float64) => {
                    append_column_to_map_builder!(BinaryBuilder, Float64Builder, field)
                }
                (DataType::Binary, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(BinaryBuilder, TimestampMicrosecondBuilder, field)
                }
                (DataType::Binary, DataType::Date32) => {
                    append_column_to_map_builder!(BinaryBuilder, Date32Builder, field)
                }
                (DataType::Binary, DataType::Binary) => {
                    append_column_to_map_builder!(BinaryBuilder, BinaryBuilder, field)
                }
                (DataType::Binary, DataType::Utf8) => {
                    append_column_to_map_builder!(BinaryBuilder, StringBuilder, field)
                }
                (DataType::Binary, DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(BinaryBuilder, Decimal128Builder, field)
                }
                (DataType::Utf8, DataType::Boolean) => {
                    append_column_to_map_builder!(StringBuilder, BooleanBuilder, field)
                }
                (DataType::Utf8, DataType::Int8) => {
                    append_column_to_map_builder!(StringBuilder, Int8Builder, field)
                }
                (DataType::Utf8, DataType::Int16) => {
                    append_column_to_map_builder!(StringBuilder, Int16Builder, field)
                }
                (DataType::Utf8, DataType::Int32) => {
                    append_column_to_map_builder!(StringBuilder, Int32Builder, field)
                }
                (DataType::Utf8, DataType::Int64) => {
                    append_column_to_map_builder!(StringBuilder, Int64Builder, field)
                }
                (DataType::Utf8, DataType::Float32) => {
                    append_column_to_map_builder!(StringBuilder, Float32Builder, field)
                }
                (DataType::Utf8, DataType::Float64) => {
                    append_column_to_map_builder!(StringBuilder, Float64Builder, field)
                }
                (DataType::Utf8, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(StringBuilder, TimestampMicrosecondBuilder, field)
                }
                (DataType::Utf8, DataType::Date32) => {
                    append_column_to_map_builder!(StringBuilder, Date32Builder, field)
                }
                (DataType::Utf8, DataType::Binary) => {
                    append_column_to_map_builder!(StringBuilder, BinaryBuilder, field)
                }
                (DataType::Utf8, DataType::Utf8) => {
                    append_column_to_map_builder!(StringBuilder, StringBuilder, field)
                }
                (DataType::Utf8, DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(StringBuilder, Decimal128Builder, field)
                }
                (DataType::Decimal128(_, _), DataType::Boolean) => {
                    append_column_to_map_builder!(Decimal128Builder, BooleanBuilder, field)
                }
                (DataType::Decimal128(_, _), DataType::Int8) => {
                    append_column_to_map_builder!(Decimal128Builder, Int8Builder, field)
                }
                (DataType::Decimal128(_, _), DataType::Int16) => {
                    append_column_to_map_builder!(Decimal128Builder, Int16Builder, field)
                }
                (DataType::Decimal128(_, _), DataType::Int32) => {
                    append_column_to_map_builder!(Decimal128Builder, Int32Builder, field)
                }
                (DataType::Decimal128(_, _), DataType::Int64) => {
                    append_column_to_map_builder!(Decimal128Builder, Int64Builder, field)
                }
                (DataType::Decimal128(_, _), DataType::Float32) => {
                    append_column_to_map_builder!(Decimal128Builder, Float32Builder, field)
                }
                (DataType::Decimal128(_, _), DataType::Float64) => {
                    append_column_to_map_builder!(Decimal128Builder, Float64Builder, field)
                }
                (DataType::Decimal128(_, _), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    append_column_to_map_builder!(
                        Decimal128Builder,
                        TimestampMicrosecondBuilder,
                        field
                    )
                }
                (DataType::Decimal128(_, _), DataType::Date32) => {
                    append_column_to_map_builder!(Decimal128Builder, Date32Builder, field)
                }
                (DataType::Decimal128(_, _), DataType::Binary) => {
                    append_column_to_map_builder!(Decimal128Builder, BinaryBuilder, field)
                }
                (DataType::Decimal128(_, _), DataType::Utf8) => {
                    append_column_to_map_builder!(Decimal128Builder, StringBuilder, field)
                }
                (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
                    append_column_to_map_builder!(Decimal128Builder, Decimal128Builder, field)
                }
                _ => {
                    return Err(CometError::Internal(format!(
                        "Unsupported map type: {:?}",
                        field.data_type()
                    )))
                }
            }
        }
        DataType::List(field) => match field.data_type() {
            DataType::Boolean => {
                append_column_to_list_builder!(BooleanBuilder, field.data_type());
            }
            DataType::Int8 => {
                append_column_to_list_builder!(Int8Builder, field.data_type());
            }
            DataType::Int16 => {
                append_column_to_list_builder!(Int16Builder, field.data_type());
            }
            DataType::Int32 => {
                append_column_to_list_builder!(Int32Builder, field.data_type());
            }
            DataType::Int64 => {
                append_column_to_list_builder!(Int64Builder, field.data_type());
            }
            DataType::Float32 => {
                append_column_to_list_builder!(Float32Builder, field.data_type());
            }
            DataType::Float64 => {
                append_column_to_list_builder!(Float64Builder, field.data_type());
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                append_column_to_list_builder!(TimestampMicrosecondBuilder, field.data_type());
            }
            DataType::Date32 => {
                append_column_to_list_builder!(Date32Builder, field.data_type());
            }
            DataType::Binary => {
                append_column_to_list_builder!(BinaryBuilder, field.data_type());
            }
            DataType::Utf8 => {
                append_column_to_list_builder!(StringBuilder, field.data_type());
            }
            DataType::Struct(_) => {
                append_column_to_list_builder!(StructBuilder, field.data_type());
            }
            DataType::Decimal128(_, _) => {
                append_column_to_list_builder!(Decimal128Builder, field.data_type());
            }
            _ => unreachable!("Unsupported data type of list element: {:?}", dt),
        },
        DataType::Struct(fields) => {
            append_column_to_struct_builder!(fields);
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
            let (key_dt, value_dt, map_fieldnames) = get_map_key_value_dt(field)?;

            let key_builder = make_builders(key_dt, NESTED_TYPE_BUILDER_CAPACITY, 1.0)?;
            let value_builder = make_builders(value_dt, NESTED_TYPE_BUILDER_CAPACITY, 1.0)?;

            // TODO: support other types of map after new release of Arrow. In new API, `MapBuilder`
            // can take general `Box<dyn ArrayBuilder>` as key/value builder.
            match (key_dt, value_dt) {
                (DataType::Boolean, DataType::Boolean) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Int8) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Int16) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Int32) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Int64) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Float32) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Float64) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Date32) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Binary) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Utf8) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Boolean, DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(BooleanBuilder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Boolean) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Int8) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Int16) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Int32) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Int64) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Float32) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Float64) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Date32) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Binary) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Utf8) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int8, DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(Int8Builder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Boolean) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Int8) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Int16) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Int32) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Int64) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Float32) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Float64) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Date32) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Binary) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Utf8) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int16, DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(Int16Builder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Boolean) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Int8) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Int16) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Int32) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Int64) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Float32) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Float64) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Date32) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Binary) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Utf8) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int32, DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(Int32Builder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Boolean) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Int8) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Int16) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Int32) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Int64) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Float32) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Float64) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Date32) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Binary) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Utf8) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Int64, DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(Int64Builder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Boolean) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Int8) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Int16) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Int32) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Int64) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Float32) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Float64) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Date32) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Binary) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Utf8) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float32, DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(Float32Builder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Boolean) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Int8) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Int16) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Int32) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Int64) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Float32) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Float64) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Date32) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Binary) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Utf8) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Float64, DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(Float64Builder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Boolean) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Int8) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Int16) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Int32) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Int64) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Float32) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Float64) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Date32) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Binary) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Utf8) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Date32, DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(Date32Builder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Boolean) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int8) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int16) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int32) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int64) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Float32) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Float64) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                    DataType::Timestamp(TimeUnit::Microsecond, _),
                ) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Date32) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Binary) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Utf8) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(TimestampMicrosecondBuilder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Boolean) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Int8) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Int16) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Int32) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Int64) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Float32) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Float64) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Date32) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Binary) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Utf8) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Binary, DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(BinaryBuilder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Boolean) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Int8) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Int16) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Int32) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Int64) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Float32) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Float64) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Date32) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Binary) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Utf8) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Utf8, DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(StringBuilder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Boolean) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Int8) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Int16) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Int32) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Int64) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Float32) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Float64) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder =
                        downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Date32) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Binary) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Utf8) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }
                (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
                    let key_builder = downcast_builder!(Decimal128Builder, key_builder);
                    let value_builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(MapBuilder::new(
                        Some(map_fieldnames),
                        *key_builder,
                        *value_builder,
                    ))
                }

                _ => {
                    return Err(CometError::Internal(format!(
                        "Unsupported map type: {:?}",
                        field.data_type()
                    )))
                }
            }
        }
        DataType::List(field) => {
            // Disable dictionary encoding for array element
            let value_builder =
                make_builders(field.data_type(), NESTED_TYPE_BUILDER_CAPACITY, 1.0)?;

            // Needed to overwrite default ListBuilder creation having the incoming field schema to be driving
            let value_field = Arc::clone(field);

            match field.data_type() {
                DataType::Boolean => {
                    let builder = downcast_builder!(BooleanBuilder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Int8 => {
                    let builder = downcast_builder!(Int8Builder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Int16 => {
                    let builder = downcast_builder!(Int16Builder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Int32 => {
                    let builder = downcast_builder!(Int32Builder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Int64 => {
                    let builder = downcast_builder!(Int64Builder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Float32 => {
                    let builder = downcast_builder!(Float32Builder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Float64 => {
                    let builder = downcast_builder!(Float64Builder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Decimal128(_, _) => {
                    let builder = downcast_builder!(Decimal128Builder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    let builder = downcast_builder!(TimestampMicrosecondBuilder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Date32 => {
                    let builder = downcast_builder!(Date32Builder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Binary => {
                    let builder = downcast_builder!(BinaryBuilder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Utf8 => {
                    let builder = downcast_builder!(StringBuilder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                DataType::Struct(_) => {
                    let builder = downcast_builder!(StructBuilder, value_builder);
                    Box::new(ListBuilder::new(*builder).with_field(value_field))
                }
                // TODO: nested list is not supported. Due to the design of `ListBuilder`, it has
                // a `T: ArrayBuilder` as type parameter. It makes hard to construct an arbitrarily
                // nested `ListBuilder`.
                DataType::List(_) => {
                    return Err(CometError::Internal(
                        "list of list is not supported type".to_string(),
                    ))
                }
                _ => {
                    return Err(CometError::Internal(format!(
                        "Unsupported list type: {:?}",
                        field.data_type()
                    )))
                }
            }
        }
        DataType::Struct(fields) => {
            let field_builders = fields
                .iter()
                // Disable dictionary encoding for struct fields
                .map(|field| make_builders(field.data_type(), row_num, 1.0))
                .collect::<Result<Vec<_>, _>>()?;

            dbg!(fields);
            Box::new(StructBuilder::new(fields.clone(), field_builders))
        }
        _ => return Err(CometError::Internal(format!("Unsupported type: {:?}", dt))),
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
    enable_fast_encoding: bool,
) -> Result<(i64, Option<u32>), CometError> {
    // TODO: We can tune this parameter automatically based on row size and cache size.
    let row_step = 10;

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

    while current_row < row_num {
        let n = std::cmp::min(batch_size, row_num - current_row);

        let mut data_builders: Vec<Box<dyn ArrayBuilder>> = vec![];
        schema.iter().try_for_each(|dt| {
            make_builders(dt, n, prefer_dictionary_ratio)
                .map(|builder| data_builders.push(builder))?;
            Ok::<(), CometError>(())
        })?;

        // Appends rows to the array builders.
        let mut row_start: usize = current_row;
        while row_start < current_row + n {
            let row_end = std::cmp::min(row_start + row_step, current_row + n);

            // For each column, iterating over rows and appending values to corresponding array
            // builder.
            for (idx, builder) in data_builders.iter_mut().enumerate() {
                append_columns(
                    row_addresses_ptr,
                    row_sizes_ptr,
                    row_start,
                    row_end,
                    schema,
                    idx,
                    builder,
                    prefer_dictionary_ratio,
                )?;
            }

            row_start = row_end;
        }

        // Writes a record batch generated from the array builders to the output file.
        let array_refs: Result<Vec<ArrayRef>, _> = data_builders
            .iter_mut()
            .zip(schema.iter())
            .map(|(builder, datatype)| builder_to_array(builder, datatype, prefer_dictionary_ratio))
            .collect();
        let batch = make_batch(array_refs?, n)?;

        let mut frozen: Vec<u8> = vec![];
        let mut cursor = Cursor::new(&mut frozen);
        cursor.seek(SeekFrom::End(0))?;

        // we do not collect metrics in Native_writeSortedFileNative
        let ipc_time = Time::default();
        let block_writer = ShuffleBlockWriter::try_new(
            batch.schema().as_ref(),
            enable_fast_encoding,
            codec.clone(),
        )?;
        written += block_writer.write_batch(&batch, &mut cursor, &ipc_time)?;

        if let Some(checksum) = &mut current_checksum {
            checksum.update(&mut cursor)?;
        }

        let mut output_data = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&output_path)?;

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
        .map(|(i, array)| Field::new(format!("c{}", i), array.data_type().clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let options = RecordBatchOptions::new().with_row_count(Option::from(row_count));
    RecordBatch::try_new_with_options(schema, arrays, &options)
}
