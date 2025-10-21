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
    execution::shuffle::{
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

pub struct SparkUnsafeArray {
    row_addr: i64,
    num_elements: usize,
    element_offset: i64,
}

impl SparkUnsafeObject for SparkUnsafeArray {
    fn get_row_addr(&self) -> i64 {
        self.row_addr
    }

    fn get_element_offset(&self, index: usize, element_size: usize) -> *const u8 {
        (self.element_offset + (index * element_size) as i64) as *const u8
    }
}

impl SparkUnsafeArray {
    /// Creates a `SparkUnsafeArray` which points to the given address and size in bytes.
    pub fn new(addr: i64) -> Self {
        // Read the number of elements from the first 8 bytes.
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
        unsafe {
            let mask: i64 = 1i64 << (index & 0x3f);
            let word_offset = (self.row_addr + 8 + (((index >> 6) as i64) << 3)) as *const i64;
            let word: i64 = *word_offset;
            (word & mask) != 0
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
            add_values!(
                BooleanBuilder,
                |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_boolean(idx)),
                |builder: &mut BooleanBuilder| builder.append_null()
            );
        }
        DataType::Int8 => {
            add_values!(
                Int8Builder,
                |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_byte(idx)),
                |builder: &mut Int8Builder| builder.append_null()
            );
        }
        DataType::Int16 => {
            add_values!(
                Int16Builder,
                |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_short(idx)),
                |builder: &mut Int16Builder| builder.append_null()
            );
        }
        DataType::Int32 => {
            add_values!(
                Int32Builder,
                |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_int(idx)),
                |builder: &mut Int32Builder| builder.append_null()
            );
        }
        DataType::Int64 => {
            add_values!(
                Int64Builder,
                |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_long(idx)),
                |builder: &mut Int64Builder| builder.append_null()
            );
        }
        DataType::Float32 => {
            add_values!(
                Float32Builder,
                |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_float(idx)),
                |builder: &mut Float32Builder| builder.append_null()
            );
        }
        DataType::Float64 => {
            add_values!(
                Float64Builder,
                |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_double(idx)),
                |builder: &mut Float64Builder| builder.append_null()
            );
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            add_values!(
                TimestampMicrosecondBuilder,
                |builder: &mut TimestampMicrosecondBuilder,
                 values: &SparkUnsafeArray,
                 idx: usize| builder.append_value(values.get_timestamp(idx)),
                |builder: &mut TimestampMicrosecondBuilder| builder.append_null()
            );
        }
        DataType::Date32 => {
            add_values!(
                Date32Builder,
                |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
                    .append_value(values.get_date(idx)),
                |builder: &mut Date32Builder| builder.append_null()
            );
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
