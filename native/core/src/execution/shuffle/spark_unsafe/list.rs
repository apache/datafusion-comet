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

use crate::{
    errors::CometError,
    execution::shuffle::spark_unsafe::{
        map::append_map_elements,
        row::{append_field, downcast_builder_ref, SparkUnsafeObject, SparkUnsafeRow},
    },
};
use arrow::array::{
    builder::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
        ListBuilder, StringBuilder, StructBuilder, TimestampMicrosecondBuilder,
    },
    MapBuilder,
};
use arrow::datatypes::{DataType, TimeUnit};

/// Generates bulk append methods for primitive types in SparkUnsafeArray.
///
/// # Safety invariants for all generated methods:
/// - `element_offset` points to contiguous element data of length `num_elements`
/// - `null_bitset_ptr()` returns a pointer to `ceil(num_elements/64)` i64 words
/// - These invariants are guaranteed by the SparkUnsafeArray layout from the JVM
macro_rules! impl_append_to_builder {
    ($method_name:ident, $builder_type:ty, $element_type:ty) => {
        pub(crate) fn $method_name<const NULLABLE: bool>(&self, builder: &mut $builder_type) {
            let num_elements = self.num_elements;
            if num_elements == 0 {
                return;
            }

            if NULLABLE {
                let mut ptr = self.element_offset as *const $element_type;
                let null_words = self.null_bitset_ptr();
                for idx in 0..num_elements {
                    let word_idx = idx >> 6;
                    let bit_idx = idx & 0x3f;
                    // SAFETY: word_idx < ceil(num_elements/64) since idx < num_elements
                    let is_null = unsafe { (*null_words.add(word_idx) & (1i64 << bit_idx)) != 0 };

                    if is_null {
                        builder.append_null();
                    } else {
                        // SAFETY: ptr is within element data bounds
                        builder.append_value(unsafe { *ptr });
                    }
                    // SAFETY: ptr stays within bounds, iterating num_elements times
                    ptr = unsafe { ptr.add(1) };
                }
            } else {
                // SAFETY: element_offset points to contiguous data of length num_elements
                let slice = unsafe {
                    std::slice::from_raw_parts(
                        self.element_offset as *const $element_type,
                        num_elements,
                    )
                };
                builder.append_slice(slice);
            }
        }
    };
}

pub struct SparkUnsafeArray {
    row_addr: i64,
    num_elements: usize,
    element_offset: i64,
}

impl SparkUnsafeObject for SparkUnsafeArray {
    #[inline]
    fn get_row_addr(&self) -> i64 {
        self.row_addr
    }

    #[inline]
    fn get_element_offset(&self, index: usize, element_size: usize) -> *const u8 {
        (self.element_offset + (index * element_size) as i64) as *const u8
    }
}

impl SparkUnsafeArray {
    /// Creates a `SparkUnsafeArray` which points to the given address and size in bytes.
    pub fn new(addr: i64) -> Self {
        // SAFETY: addr points to valid Spark UnsafeArray data from the JVM.
        // The first 8 bytes contain the element count as a little-endian i64.
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr as *const u8, 8) };
        let num_elements = i64::from_le_bytes(slice.try_into().unwrap());

        if num_elements < 0 {
            panic!("Negative number of elements: {num_elements}");
        }

        if num_elements > i32::MAX as i64 {
            panic!("Number of elements should <= i32::MAX: {num_elements}");
        }

        Self {
            row_addr: addr,
            num_elements: num_elements as usize,
            element_offset: addr + Self::get_header_portion_in_bytes(num_elements),
        }
    }

    pub(crate) fn get_num_elements(&self) -> usize {
        self.num_elements
    }

    /// Returns the size of array header in bytes.
    #[inline]
    const fn get_header_portion_in_bytes(num_fields: i64) -> i64 {
        8 + ((num_fields + 63) / 64) * 8
    }

    /// Returns true if the null bit at the given index of the array is set.
    #[inline]
    pub(crate) fn is_null_at(&self, index: usize) -> bool {
        // SAFETY: row_addr points to valid Spark UnsafeArray data. The null bitset starts
        // at offset 8 and contains ceil(num_elements/64) * 8 bytes. The caller ensures
        // index < num_elements, so word_offset is within the bitset region.
        unsafe {
            let mask: i64 = 1i64 << (index & 0x3f);
            let word_offset = (self.row_addr + 8 + (((index >> 6) as i64) << 3)) as *const i64;
            let word: i64 = *word_offset;
            (word & mask) != 0
        }
    }

    /// Returns the null bitset pointer (starts at row_addr + 8).
    #[inline]
    fn null_bitset_ptr(&self) -> *const i64 {
        (self.row_addr + 8) as *const i64
    }

    impl_append_to_builder!(append_ints_to_builder, Int32Builder, i32);
    impl_append_to_builder!(append_longs_to_builder, Int64Builder, i64);
    impl_append_to_builder!(append_shorts_to_builder, Int16Builder, i16);
    impl_append_to_builder!(append_bytes_to_builder, Int8Builder, i8);
    impl_append_to_builder!(append_floats_to_builder, Float32Builder, f32);
    impl_append_to_builder!(append_doubles_to_builder, Float64Builder, f64);

    /// Bulk append boolean values to builder.
    /// Booleans are stored as 1 byte each in SparkUnsafeArray, requiring special handling.
    pub(crate) fn append_booleans_to_builder<const NULLABLE: bool>(
        &self,
        builder: &mut BooleanBuilder,
    ) {
        let num_elements = self.num_elements;
        if num_elements == 0 {
            return;
        }

        let mut ptr = self.element_offset as *const u8;

        if NULLABLE {
            let null_words = self.null_bitset_ptr();
            for idx in 0..num_elements {
                let word_idx = idx >> 6;
                let bit_idx = idx & 0x3f;
                // SAFETY: word_idx < ceil(num_elements/64) since idx < num_elements
                let is_null = unsafe { (*null_words.add(word_idx) & (1i64 << bit_idx)) != 0 };

                if is_null {
                    builder.append_null();
                } else {
                    // SAFETY: ptr is within element data bounds
                    builder.append_value(unsafe { *ptr != 0 });
                }
                // SAFETY: ptr stays within bounds, iterating num_elements times
                ptr = unsafe { ptr.add(1) };
            }
        } else {
            for _ in 0..num_elements {
                // SAFETY: ptr is within element data bounds
                builder.append_value(unsafe { *ptr != 0 });
                ptr = unsafe { ptr.add(1) };
            }
        }
    }

    /// Bulk append timestamp values to builder (stored as i64 microseconds).
    pub(crate) fn append_timestamps_to_builder<const NULLABLE: bool>(
        &self,
        builder: &mut TimestampMicrosecondBuilder,
    ) {
        let num_elements = self.num_elements;
        if num_elements == 0 {
            return;
        }

        if NULLABLE {
            let mut ptr = self.element_offset as *const i64;
            let null_words = self.null_bitset_ptr();
            for idx in 0..num_elements {
                let word_idx = idx >> 6;
                let bit_idx = idx & 0x3f;
                // SAFETY: word_idx < ceil(num_elements/64) since idx < num_elements
                let is_null = unsafe { (*null_words.add(word_idx) & (1i64 << bit_idx)) != 0 };

                if is_null {
                    builder.append_null();
                } else {
                    // SAFETY: ptr is within element data bounds
                    builder.append_value(unsafe { *ptr });
                }
                // SAFETY: ptr stays within bounds, iterating num_elements times
                ptr = unsafe { ptr.add(1) };
            }
        } else {
            // SAFETY: element_offset points to contiguous i64 data of length num_elements
            let slice = unsafe {
                std::slice::from_raw_parts(self.element_offset as *const i64, num_elements)
            };
            builder.append_slice(slice);
        }
    }

    /// Bulk append date values to builder (stored as i32 days since epoch).
    pub(crate) fn append_dates_to_builder<const NULLABLE: bool>(
        &self,
        builder: &mut Date32Builder,
    ) {
        let num_elements = self.num_elements;
        if num_elements == 0 {
            return;
        }

        if NULLABLE {
            let mut ptr = self.element_offset as *const i32;
            let null_words = self.null_bitset_ptr();
            for idx in 0..num_elements {
                let word_idx = idx >> 6;
                let bit_idx = idx & 0x3f;
                // SAFETY: word_idx < ceil(num_elements/64) since idx < num_elements
                let is_null = unsafe { (*null_words.add(word_idx) & (1i64 << bit_idx)) != 0 };

                if is_null {
                    builder.append_null();
                } else {
                    // SAFETY: ptr is within element data bounds
                    builder.append_value(unsafe { *ptr });
                }
                // SAFETY: ptr stays within bounds, iterating num_elements times
                ptr = unsafe { ptr.add(1) };
            }
        } else {
            // SAFETY: element_offset points to contiguous i32 data of length num_elements
            let slice = unsafe {
                std::slice::from_raw_parts(self.element_offset as *const i32, num_elements)
            };
            builder.append_slice(slice);
        }
    }
}

pub fn append_to_builder<const NULLABLE: bool>(
    data_type: &DataType,
    builder: &mut dyn ArrayBuilder,
    array: &SparkUnsafeArray,
) -> Result<(), CometError> {
    macro_rules! add_values {
        ($builder_type:ty, $add_value:expr, $add_null:expr) => {
            let builder = downcast_builder_ref!($builder_type, builder);
            for idx in 0..array.get_num_elements() {
                if NULLABLE && array.is_null_at(idx) {
                    $add_null(builder);
                } else {
                    $add_value(builder, array, idx);
                }
            }
        };
    }

    match data_type {
        DataType::Boolean => {
            let builder = downcast_builder_ref!(BooleanBuilder, builder);
            array.append_booleans_to_builder::<NULLABLE>(builder);
        }
        DataType::Int8 => {
            let builder = downcast_builder_ref!(Int8Builder, builder);
            array.append_bytes_to_builder::<NULLABLE>(builder);
        }
        DataType::Int16 => {
            let builder = downcast_builder_ref!(Int16Builder, builder);
            array.append_shorts_to_builder::<NULLABLE>(builder);
        }
        DataType::Int32 => {
            let builder = downcast_builder_ref!(Int32Builder, builder);
            array.append_ints_to_builder::<NULLABLE>(builder);
        }
        DataType::Int64 => {
            let builder = downcast_builder_ref!(Int64Builder, builder);
            array.append_longs_to_builder::<NULLABLE>(builder);
        }
        DataType::Float32 => {
            let builder = downcast_builder_ref!(Float32Builder, builder);
            array.append_floats_to_builder::<NULLABLE>(builder);
        }
        DataType::Float64 => {
            let builder = downcast_builder_ref!(Float64Builder, builder);
            array.append_doubles_to_builder::<NULLABLE>(builder);
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let builder = downcast_builder_ref!(TimestampMicrosecondBuilder, builder);
            array.append_timestamps_to_builder::<NULLABLE>(builder);
        }
        DataType::Date32 => {
            let builder = downcast_builder_ref!(Date32Builder, builder);
            array.append_dates_to_builder::<NULLABLE>(builder);
        }
        DataType::Binary => {
            add_values!(
                BinaryBuilder,
                |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_binary(idx)),
                |builder: &mut BinaryBuilder| builder.append_null()
            );
        }
        DataType::Utf8 => {
            add_values!(
                StringBuilder,
                |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_string(idx)),
                |builder: &mut StringBuilder| builder.append_null()
            );
        }
        DataType::List(field) => {
            let builder = downcast_builder_ref!(ListBuilder<Box<dyn ArrayBuilder>>, builder);
            for idx in 0..array.get_num_elements() {
                if NULLABLE && array.is_null_at(idx) {
                    builder.append_null();
                } else {
                    let nested_array = array.get_array(idx);
                    append_list_element(field.data_type(), builder, &nested_array)?;
                };
            }
        }
        DataType::Struct(fields) => {
            let builder = downcast_builder_ref!(StructBuilder, builder);
            for idx in 0..array.get_num_elements() {
                let nested_row = if NULLABLE && array.is_null_at(idx) {
                    builder.append_null();
                    SparkUnsafeRow::default()
                } else {
                    builder.append(true);
                    array.get_struct(idx, fields.len())
                };

                for (field_idx, field) in fields.into_iter().enumerate() {
                    append_field(field.data_type(), builder, &nested_row, field_idx)?;
                }
            }
        }
        DataType::Decimal128(p, _) => {
            add_values!(
                Decimal128Builder,
                |builder: &mut Decimal128Builder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_decimal(idx, *p)),
                |builder: &mut Decimal128Builder| builder.append_null()
            );
        }
        DataType::Map(field, _) => {
            let builder = downcast_builder_ref!(
                MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
                builder
            );
            for idx in 0..array.get_num_elements() {
                if NULLABLE && array.is_null_at(idx) {
                    builder.append(false)?;
                } else {
                    let nested_map = array.get_map(idx);
                    append_map_elements(field, builder, &nested_map)?;
                };
            }
        }
        _ => {
            return Err(CometError::Internal(format!(
                "Unsupported map data type: {:?}",
                data_type
            )))
        }
    }

    Ok(())
}

/// Appending the given list stored in `SparkUnsafeArray` into `ListBuilder`.
/// `element_dt` is the data type of the list element. `list_builder` is the list builder.
/// `list` is the list stored in `SparkUnsafeArray`.
pub fn append_list_element(
    element_dt: &DataType,
    list_builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
    list: &SparkUnsafeArray,
) -> Result<(), CometError> {
    append_to_builder::<true>(element_dt, list_builder.values(), list)?;
    list_builder.append(true);

    Ok(())
}
