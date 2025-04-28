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
    execution::shuffle::row::{append_field, SparkUnsafeObject, SparkUnsafeRow},
};
use arrow::array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder,
    StringBuilder, StructBuilder, TimestampMicrosecondBuilder,
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
            panic!("Negative number of elements: {}", num_elements);
        }

        if num_elements > i32::MAX as i64 {
            panic!("Number of elements should <= i32::MAX: {}", num_elements);
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

/// A macro defines a function that appends the given list stored in `SparkUnsafeArray` into
/// `ListBuilder`.
macro_rules! define_append_element {
    ($func:ident, $builder_type:ty, $accessor:expr) => {
        #[allow(clippy::redundant_closure_call)]
        fn $func(
            list_builder: &mut ListBuilder<$builder_type>,
            list: &SparkUnsafeArray,
            idx: usize,
        ) {
            let element_builder: &mut $builder_type = list_builder.values();
            let is_null = list.is_null_at(idx);

            if is_null {
                // Append a null value to the element builder.
                element_builder.append_null();
            } else {
                $accessor(element_builder, list, idx);
            }
        }
    };
}

define_append_element!(
    append_boolean_element,
    BooleanBuilder,
    |builder: &mut BooleanBuilder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_boolean(idx))
);
define_append_element!(
    append_int8_element,
    Int8Builder,
    |builder: &mut Int8Builder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_byte(idx))
);
define_append_element!(
    append_int16_element,
    Int16Builder,
    |builder: &mut Int16Builder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_short(idx))
);
define_append_element!(
    append_int32_element,
    Int32Builder,
    |builder: &mut Int32Builder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_int(idx))
);
define_append_element!(
    append_int64_element,
    Int64Builder,
    |builder: &mut Int64Builder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_long(idx))
);
define_append_element!(
    append_float32_element,
    Float32Builder,
    |builder: &mut Float32Builder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_float(idx))
);
define_append_element!(
    append_float64_element,
    Float64Builder,
    |builder: &mut Float64Builder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_double(idx))
);
define_append_element!(
    append_date32_element,
    Date32Builder,
    |builder: &mut Date32Builder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_date(idx))
);
define_append_element!(
    append_timestamp_element,
    TimestampMicrosecondBuilder,
    |builder: &mut TimestampMicrosecondBuilder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_timestamp(idx))
);
define_append_element!(
    append_binary_element,
    BinaryBuilder,
    |builder: &mut BinaryBuilder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_binary(idx))
);
define_append_element!(
    append_string_element,
    StringBuilder,
    |builder: &mut StringBuilder, list: &SparkUnsafeArray, idx: usize| builder
        .append_value(list.get_string(idx))
);

/// Appending the given list stored in `SparkUnsafeArray` into `ListBuilder`.
/// `element_dt` is the data type of the list element. `list_builder` is the list builder.
/// `list` is the list stored in `SparkUnsafeArray`.
pub fn append_list_element<T: ArrayBuilder>(
    element_dt: &DataType,
    list_builder: &mut ListBuilder<T>,
    list: &SparkUnsafeArray,
) -> Result<(), CometError> {
    for idx in 0..list.get_num_elements() {
        match element_dt {
            DataType::Boolean => append_boolean_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<BooleanBuilder>>()
                    .expect("ListBuilder<BooleanBuilder>"),
                list,
                idx,
            ),
            DataType::Int8 => append_int8_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Int8Builder>>()
                    .expect("ListBuilder<Int8Builder>"),
                list,
                idx,
            ),
            DataType::Int16 => append_int16_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Int16Builder>>()
                    .expect("ListBuilder<Int16Builder>"),
                list,
                idx,
            ),
            DataType::Int32 => append_int32_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Int32Builder>>()
                    .expect("ListBuilder<Int32Builder>"),
                list,
                idx,
            ),
            DataType::Int64 => append_int64_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Int64Builder>>()
                    .expect("ListBuilder<Int64Builder>"),
                list,
                idx,
            ),
            DataType::Float32 => append_float32_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Float32Builder>>()
                    .expect("ListBuilder<Float32Builder>"),
                list,
                idx,
            ),
            DataType::Float64 => append_float64_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Float64Builder>>()
                    .expect("ListBuilder<Float64Builder>"),
                list,
                idx,
            ),
            DataType::Date32 => append_date32_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<Date32Builder>>()
                    .expect("ListBuilder<Date32Builder>"),
                list,
                idx,
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => append_timestamp_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<TimestampMicrosecondBuilder>>()
                    .expect("ListBuilder<TimestampMicrosecondBuilder>"),
                list,
                idx,
            ),
            DataType::Binary => append_binary_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<BinaryBuilder>>()
                    .expect("ListBuilder<BinaryBuilder>"),
                list,
                idx,
            ),
            DataType::Utf8 => append_string_element(
                list_builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<StringBuilder>>()
                    .expect("ListBuilder<StringBuilder>"),
                list,
                idx,
            ),
            DataType::Decimal128(p, _) => {
                let element_builder: &mut Decimal128Builder = list_builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<Decimal128Builder>()
                    .expect("ListBuilder<Decimal128Builder>");
                let is_null = list.is_null_at(idx);

                if is_null {
                    // Append a null value to element builder.
                    element_builder.append_null();
                } else {
                    element_builder.append_value(list.get_decimal(idx, *p))
                }
            }
            // TODO: support nested list
            // If the element is a list, we need to get the nested list builder by
            // `list_builder.values()` and downcast to correct type, i.e., ListBuilder<U>.
            // But we don't know the type `U` so we cannot downcast to correct type
            // and recursively call `append_list_element`. Later once we upgrade to
            // latest Arrow, the `T` of `ListBuilder<T>` could be `Box<dyn ArrowBuilder>`
            // which erase the deep type of the builder.
            /*
            DataType::List(field) => {
                let element_builder: &mut ListBuilder<_> = list_builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<_>>()
                    .unwrap();
                let is_null = list.is_null_at(idx);

                if is_null {
                    element_builder.append_null();
                } else {
                    append_list_element(field.data_type(), element_builder, list);
                }
            }
             */
            DataType::Struct(fields) => {
                let struct_builder: &mut StructBuilder = list_builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<StructBuilder>()
                    .expect("StructBuilder");
                let is_null = list.is_null_at(idx);

                let nested_row = if is_null {
                    SparkUnsafeRow::default()
                } else {
                    list.get_struct(idx, fields.len())
                };

                struct_builder.append(!is_null);
                for (field_idx, field) in fields.into_iter().enumerate() {
                    append_field(field.data_type(), struct_builder, &nested_row, field_idx)?;
                }
            }
            _ => {
                return Err(CometError::Internal(format!(
                    "Unsupported data type in list element: {:?}",
                    element_dt
                )))
            }
        }
    }
    list_builder.append(true);

    Ok(())
}
