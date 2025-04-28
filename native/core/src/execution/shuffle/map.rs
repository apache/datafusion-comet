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
        list::SparkUnsafeArray,
        row::{append_field, downcast_builder_ref, SparkUnsafeObject, SparkUnsafeRow},
    },
};
use arrow::array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, MapBuilder,
    MapFieldNames, StringBuilder, StructBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, FieldRef, Fields, TimeUnit};

pub struct SparkUnsafeMap {
    pub(crate) keys: SparkUnsafeArray,
    pub(crate) values: SparkUnsafeArray,
}

impl SparkUnsafeMap {
    /// Creates a `SparkUnsafeMap` which points to the given address and size in bytes.
    pub(crate) fn new(addr: i64, size: i32) -> Self {
        // Read the number of bytes of key array from the first 8 bytes.
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(addr as *const u8, 8) };
        let key_array_size = i64::from_le_bytes(slice.try_into().unwrap());

        if key_array_size < 0 {
            panic!("Negative key size in bytes of map: {}", key_array_size);
        }

        if key_array_size > i32::MAX as i64 {
            panic!(
                "Number of key size in bytes should <= i32::MAX: {}",
                key_array_size
            );
        }

        let value_array_size = size - key_array_size as i32 - 8;
        if value_array_size < 0 {
            panic!("Negative value size in bytes of map: {}", value_array_size);
        }

        let keys = SparkUnsafeArray::new(addr + 8);
        let values = SparkUnsafeArray::new(addr + 8 + key_array_size);

        if keys.get_num_elements() != values.get_num_elements() {
            panic!(
                "Number of elements of keys and values should be the same: {} vs {}",
                keys.get_num_elements(),
                values.get_num_elements()
            );
        }

        Self { keys, values }
    }
}

/// A macro defining a function to append elements of a map to a map builder with given key builder
/// and given value builder.
macro_rules! define_append_map_element {
    ($func:ident, $key_builder_type:ty, $value_builder_type:ty, $key_accessor:expr, $value_accessor:expr) => {
        #[allow(clippy::redundant_closure_call)]
        fn $func(
            map_builder: &mut MapBuilder<$key_builder_type, $value_builder_type>,
            map: &SparkUnsafeMap,
        ) -> Result<(), CometError> {
            let keys = &map.keys;
            let values = &map.values;

            for idx in 0..keys.get_num_elements() {
                // Map key cannot be null
                // TODO: Getting key/value builders outside loop when new API is available
                // from upstream release
                let key_builder = downcast_builder_ref!($key_builder_type, map_builder.keys());
                $key_accessor(key_builder, keys, idx);

                let value_builder =
                    downcast_builder_ref!($value_builder_type, map_builder.values());
                if values.is_null_at(idx) {
                    value_builder.append_null();
                } else {
                    $value_accessor(value_builder, values, idx);
                }
            }
            map_builder.append(true)?;

            Ok(())
        }
    };
}

/// A macro defining a function to append elements of a map to a map builder with given key builder
/// and struct builder as value builder.
macro_rules! define_append_map_struct_value_element {
    ($func:ident, $key_builder_type:ty, $key_accessor:expr) => {
        #[allow(clippy::redundant_closure_call)]
        fn $func(
            map_builder: &mut MapBuilder<$key_builder_type, StructBuilder>,
            map: &SparkUnsafeMap,
            fields: &Fields,
        ) -> Result<(), CometError> {
            let keys = &map.keys;
            let values = &map.values;

            for idx in 0..keys.get_num_elements() {
                // Map key cannot be null
                // TODO: Getting key/value builders outside loop when new API is available
                // from upstream release
                let key_builder = downcast_builder_ref!($key_builder_type, map_builder.keys());
                $key_accessor(key_builder, keys, idx);

                let value_builder = downcast_builder_ref!(StructBuilder, map_builder.values());
                let nested_row = if values.is_null_at(idx) {
                    value_builder.append_null();
                    SparkUnsafeRow::default()
                } else {
                    value_builder.append(true);
                    values.get_struct(idx, fields.len())
                };

                for (field_idx, field) in fields.into_iter().enumerate() {
                    append_field(field.data_type(), value_builder, &nested_row, field_idx)?;
                }
            }
            map_builder.append(true)?;

            Ok(())
        }
    };
}

/// A macro defining a function to append elements of a map to a map builder with struct builder as
/// key builder and given value builder.
macro_rules! define_append_map_struct_key_element {
    ($func:ident, $value_builder_type:ty, $value_accessor:expr) => {
        #[allow(clippy::redundant_closure_call)]
        fn $func(
            map_builder: &mut MapBuilder<StructBuilder, $value_builder_type>,
            map: &SparkUnsafeMap,
            fields: &Fields,
        ) -> Result<(), CometError> {
            let keys = &map.keys;
            let values = &map.values;

            for idx in 0..keys.get_num_elements() {
                // Map key cannot be null
                // TODO: Getting key/value builders outside loop when new API is available
                // from upstream release
                let key_builder = downcast_builder_ref!(StructBuilder, map_builder.keys());
                let nested_row = keys.get_struct(idx, fields.len());
                key_builder.append(true);
                for (field_idx, field) in fields.into_iter().enumerate() {
                    append_field(field.data_type(), key_builder, &nested_row, field_idx)?;
                }

                let value_builder =
                    downcast_builder_ref!($value_builder_type, map_builder.values());
                if values.is_null_at(idx) {
                    value_builder.append_null();
                } else {
                    $value_accessor(value_builder, values, idx);
                }
            }
            map_builder.append(true)?;

            Ok(())
        }
    };
}

fn append_map_struct_struct_element(
    map_builder: &mut MapBuilder<StructBuilder, StructBuilder>,
    map: &SparkUnsafeMap,
    key_fields: &Fields,
    value_fields: &Fields,
) -> Result<(), CometError> {
    let keys = &map.keys;
    let values = &map.values;

    for idx in 0..keys.get_num_elements() {
        // Map key cannot be null
        // TODO: Getting key/value builders outside loop when new API is available
        // from upstream release
        let key_builder = downcast_builder_ref!(StructBuilder, map_builder.keys());
        let nested_row = keys.get_struct(idx, key_fields.len());
        key_builder.append(true);
        for (field_idx, field) in key_fields.into_iter().enumerate() {
            append_field(field.data_type(), key_builder, &nested_row, field_idx)?;
        }

        let value_builder = downcast_builder_ref!(StructBuilder, map_builder.values());
        let nested_row = if values.is_null_at(idx) {
            value_builder.append_null();
            SparkUnsafeRow::default()
        } else {
            value_builder.append(true);
            values.get_struct(idx, value_fields.len())
        };

        for (field_idx, field) in value_fields.into_iter().enumerate() {
            append_field(field.data_type(), value_builder, &nested_row, field_idx)?;
        }
    }
    map_builder.append(true)?;

    Ok(())
}

/// A macro defining a function to append elements of a map to a map builder with given key builder
/// and decimal builder as value builder.
macro_rules! define_append_map_decimal_value_element {
    ($func:ident, $key_builder_type:ty, $key_accessor:expr) => {
        #[allow(clippy::redundant_closure_call)]
        fn $func(
            map_builder: &mut MapBuilder<$key_builder_type, Decimal128Builder>,
            map: &SparkUnsafeMap,
            precision: u8,
        ) -> Result<(), CometError> {
            let keys = &map.keys;
            let values = &map.values;

            for idx in 0..keys.get_num_elements() {
                // Map key cannot be null
                // TODO: Getting key/value builders outside loop when new API is available
                // from upstream release
                let key_builder = downcast_builder_ref!($key_builder_type, map_builder.keys());
                $key_accessor(key_builder, keys, idx);

                let value_builder = downcast_builder_ref!(Decimal128Builder, map_builder.values());
                if values.is_null_at(idx) {
                    value_builder.append_null();
                } else {
                    value_builder.append_value(values.get_decimal(idx, precision));
                }
            }
            map_builder.append(true)?;

            Ok(())
        }
    };
}

/// A macro defining a function to append elements of a map to a map builder with decimal builder as
/// key builder and given value builder.
macro_rules! define_append_map_decimal_key_element {
    ($func:ident, $value_builder_type:ty, $value_accessor:expr) => {
        #[allow(clippy::redundant_closure_call)]
        fn $func(
            map_builder: &mut MapBuilder<Decimal128Builder, $value_builder_type>,
            map: &SparkUnsafeMap,
            precision: u8,
        ) -> Result<(), CometError> {
            let keys = &map.keys;
            let values = &map.values;

            for idx in 0..keys.get_num_elements() {
                // Map key cannot be null
                // TODO: Getting key/value builders outside loop when new API is available
                // from upstream release
                let key_builder = downcast_builder_ref!(Decimal128Builder, map_builder.keys());
                key_builder.append_value(keys.get_decimal(idx, precision));

                let value_builder =
                    downcast_builder_ref!($value_builder_type, map_builder.values());
                if values.is_null_at(idx) {
                    value_builder.append_null();
                } else {
                    $value_accessor(value_builder, values, idx);
                }
            }
            map_builder.append(true)?;

            Ok(())
        }
    };
}

fn append_map_decimal_decimal_element(
    map_builder: &mut MapBuilder<Decimal128Builder, Decimal128Builder>,
    map: &SparkUnsafeMap,
    key_precision: u8,
    value_precision: u8,
) -> Result<(), CometError> {
    let keys = &map.keys;
    let values = &map.values;

    for idx in 0..keys.get_num_elements() {
        // Map key cannot be null
        // TODO: Getting key/value builders outside loop when new API is available
        // from upstream release
        let key_builder = downcast_builder_ref!(Decimal128Builder, map_builder.keys());
        key_builder.append_value(keys.get_decimal(idx, key_precision));

        let value_builder = downcast_builder_ref!(Decimal128Builder, map_builder.values());
        if values.is_null_at(idx) {
            value_builder.append_null();
        } else {
            value_builder.append_value(values.get_decimal(idx, value_precision));
        }
    }
    map_builder.append(true)?;

    Ok(())
}

fn append_map_decimal_struct_element(
    map_builder: &mut MapBuilder<Decimal128Builder, StructBuilder>,
    map: &SparkUnsafeMap,
    precision: u8,
    fields: &Fields,
) -> Result<(), CometError> {
    let keys = &map.keys;
    let values = &map.values;

    for idx in 0..keys.get_num_elements() {
        // Map key cannot be null
        // TODO: Getting key/value builders outside loop when new API is available
        // from upstream release
        let key_builder = downcast_builder_ref!(Decimal128Builder, map_builder.keys());
        key_builder.append_value(keys.get_decimal(idx, precision));

        let value_builder = downcast_builder_ref!(StructBuilder, map_builder.values());
        let nested_row = if values.is_null_at(idx) {
            value_builder.append_null();
            SparkUnsafeRow::default()
        } else {
            value_builder.append(true);
            values.get_struct(idx, fields.len())
        };

        for (field_idx, field) in fields.into_iter().enumerate() {
            append_field(field.data_type(), value_builder, &nested_row, field_idx)?;
        }
    }
    map_builder.append(true)?;

    Ok(())
}

fn append_map_struct_decimal_element(
    map_builder: &mut MapBuilder<StructBuilder, Decimal128Builder>,
    map: &SparkUnsafeMap,
    precision: u8,
    fields: &Fields,
) -> Result<(), CometError> {
    let keys = &map.keys;
    let values = &map.values;

    for idx in 0..keys.get_num_elements() {
        // Map key cannot be null
        // TODO: Getting key/value builders outside loop when new API is available
        // from upstream release
        let key_builder = downcast_builder_ref!(StructBuilder, map_builder.keys());
        let nested_row = values.get_struct(idx, fields.len());
        key_builder.append(true);
        for (field_idx, field) in fields.into_iter().enumerate() {
            append_field(field.data_type(), key_builder, &nested_row, field_idx)?;
        }

        let value_builder = downcast_builder_ref!(Decimal128Builder, map_builder.values());
        if values.is_null_at(idx) {
            value_builder.append_null();
        } else {
            value_builder.append_value(values.get_decimal(idx, precision));
        }
    }
    map_builder.append(true)?;

    Ok(())
}

// Boolean key
define_append_map_element!(
    append_map_boolean_boolean_element,
    BooleanBuilder,
    BooleanBuilder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_boolean_int8_element,
    BooleanBuilder,
    Int8Builder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_boolean_int16_element,
    BooleanBuilder,
    Int16Builder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_boolean_int32_element,
    BooleanBuilder,
    Int32Builder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_boolean_int64_element,
    BooleanBuilder,
    Int64Builder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_boolean_float32_element,
    BooleanBuilder,
    Float32Builder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_boolean_float64_element,
    BooleanBuilder,
    Float64Builder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_boolean_date32_element,
    BooleanBuilder,
    Date32Builder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_boolean_timestamp_element,
    BooleanBuilder,
    TimestampMicrosecondBuilder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_boolean_binary_element,
    BooleanBuilder,
    BinaryBuilder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_boolean_string_element,
    BooleanBuilder,
    StringBuilder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_boolean_decimal_element,
    BooleanBuilder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx))
);

define_append_map_struct_value_element!(
    append_map_boolean_struct_element,
    BooleanBuilder,
    |builder: &mut BooleanBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_boolean(idx))
);

// Int8 key
define_append_map_element!(
    append_map_int8_boolean_element,
    Int8Builder,
    BooleanBuilder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_int8_int8_element,
    Int8Builder,
    Int8Builder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_int8_int16_element,
    Int8Builder,
    Int16Builder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_int8_int32_element,
    Int8Builder,
    Int32Builder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_int8_int64_element,
    Int8Builder,
    Int64Builder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_int8_float32_element,
    Int8Builder,
    Float32Builder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_int8_float64_element,
    Int8Builder,
    Float64Builder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_int8_date32_element,
    Int8Builder,
    Date32Builder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_int8_timestamp_element,
    Int8Builder,
    TimestampMicrosecondBuilder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_int8_binary_element,
    Int8Builder,
    BinaryBuilder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_int8_string_element,
    Int8Builder,
    StringBuilder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_int8_decimal_element,
    Int8Builder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx))
);

define_append_map_struct_value_element!(
    append_map_int8_struct_element,
    Int8Builder,
    |builder: &mut Int8Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_byte(idx))
);

// Int16 key
define_append_map_element!(
    append_map_int16_boolean_element,
    Int16Builder,
    BooleanBuilder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_int16_int8_element,
    Int16Builder,
    Int8Builder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_int16_int16_element,
    Int16Builder,
    Int16Builder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_int16_int32_element,
    Int16Builder,
    Int32Builder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_int16_int64_element,
    Int16Builder,
    Int64Builder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_int16_float32_element,
    Int16Builder,
    Float32Builder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_int16_float64_element,
    Int16Builder,
    Float64Builder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_int16_date32_element,
    Int16Builder,
    Date32Builder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_int16_timestamp_element,
    Int16Builder,
    TimestampMicrosecondBuilder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_int16_binary_element,
    Int16Builder,
    BinaryBuilder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_int16_string_element,
    Int16Builder,
    StringBuilder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_int16_decimal_element,
    Int16Builder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx))
);

define_append_map_struct_value_element!(
    append_map_int16_struct_element,
    Int16Builder,
    |builder: &mut Int16Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_short(idx))
);

// Int32 key
define_append_map_element!(
    append_map_int32_boolean_element,
    Int32Builder,
    BooleanBuilder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_int32_int8_element,
    Int32Builder,
    Int8Builder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_int32_int16_element,
    Int32Builder,
    Int16Builder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_int32_int32_element,
    Int32Builder,
    Int32Builder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_int32_int64_element,
    Int32Builder,
    Int64Builder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_int32_float32_element,
    Int32Builder,
    Float32Builder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_int32_float64_element,
    Int32Builder,
    Float64Builder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_int32_date32_element,
    Int32Builder,
    Date32Builder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_int32_timestamp_element,
    Int32Builder,
    TimestampMicrosecondBuilder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_int32_binary_element,
    Int32Builder,
    BinaryBuilder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_int32_string_element,
    Int32Builder,
    StringBuilder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_int32_decimal_element,
    Int32Builder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx))
);

define_append_map_struct_value_element!(
    append_map_int32_struct_element,
    Int32Builder,
    |builder: &mut Int32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_int(idx))
);

// Int64 key
define_append_map_element!(
    append_map_int64_boolean_element,
    Int64Builder,
    BooleanBuilder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_int64_int8_element,
    Int64Builder,
    Int8Builder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_int64_int16_element,
    Int64Builder,
    Int16Builder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_int64_int32_element,
    Int64Builder,
    Int32Builder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_int64_int64_element,
    Int64Builder,
    Int64Builder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_int64_float32_element,
    Int64Builder,
    Float32Builder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_int64_float64_element,
    Int64Builder,
    Float64Builder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_int64_date32_element,
    Int64Builder,
    Date32Builder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_int64_timestamp_element,
    Int64Builder,
    TimestampMicrosecondBuilder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_int64_binary_element,
    Int64Builder,
    BinaryBuilder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_int64_string_element,
    Int64Builder,
    StringBuilder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_int64_decimal_element,
    Int64Builder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx))
);

define_append_map_struct_value_element!(
    append_map_int64_struct_element,
    Int64Builder,
    |builder: &mut Int64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_long(idx))
);

// Float32 key
define_append_map_element!(
    append_map_float32_boolean_element,
    Float32Builder,
    BooleanBuilder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_float32_int8_element,
    Float32Builder,
    Int8Builder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_float32_int16_element,
    Float32Builder,
    Int16Builder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_float32_int32_element,
    Float32Builder,
    Int32Builder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_float32_int64_element,
    Float32Builder,
    Int64Builder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_float32_float32_element,
    Float32Builder,
    Float32Builder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_float32_float64_element,
    Float32Builder,
    Float64Builder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_float32_date32_element,
    Float32Builder,
    Date32Builder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_float32_timestamp_element,
    Float32Builder,
    TimestampMicrosecondBuilder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_float32_binary_element,
    Float32Builder,
    BinaryBuilder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_float32_string_element,
    Float32Builder,
    StringBuilder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_float32_decimal_element,
    Float32Builder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx))
);

define_append_map_struct_value_element!(
    append_map_float32_struct_element,
    Float32Builder,
    |builder: &mut Float32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_float(idx))
);

// Float64 key
define_append_map_element!(
    append_map_float64_boolean_element,
    Float64Builder,
    BooleanBuilder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_float64_int8_element,
    Float64Builder,
    Int8Builder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_float64_int16_element,
    Float64Builder,
    Int16Builder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_float64_int32_element,
    Float64Builder,
    Int32Builder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_float64_int64_element,
    Float64Builder,
    Int64Builder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_float64_float32_element,
    Float64Builder,
    Float32Builder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_float64_float64_element,
    Float64Builder,
    Float64Builder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_float64_date32_element,
    Float64Builder,
    Date32Builder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_float64_timestamp_element,
    Float64Builder,
    TimestampMicrosecondBuilder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_float64_binary_element,
    Float64Builder,
    BinaryBuilder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_float64_string_element,
    Float64Builder,
    StringBuilder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_float64_decimal_element,
    Float64Builder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx))
);

define_append_map_struct_value_element!(
    append_map_float64_struct_element,
    Float64Builder,
    |builder: &mut Float64Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_double(idx))
);

// Date32 key
define_append_map_element!(
    append_map_date32_boolean_element,
    Date32Builder,
    BooleanBuilder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_date32_int8_element,
    Date32Builder,
    Int8Builder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_date32_int16_element,
    Date32Builder,
    Int16Builder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_date32_int32_element,
    Date32Builder,
    Int32Builder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_date32_int64_element,
    Date32Builder,
    Int64Builder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_date32_float32_element,
    Date32Builder,
    Float32Builder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_date32_float64_element,
    Date32Builder,
    Float64Builder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_date32_date32_element,
    Date32Builder,
    Date32Builder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_date32_timestamp_element,
    Date32Builder,
    TimestampMicrosecondBuilder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_date32_binary_element,
    Date32Builder,
    BinaryBuilder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_date32_string_element,
    Date32Builder,
    StringBuilder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_date32_decimal_element,
    Date32Builder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx))
);

define_append_map_struct_value_element!(
    append_map_date32_struct_element,
    Date32Builder,
    |builder: &mut Date32Builder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_date(idx))
);

// Timestamp key
define_append_map_element!(
    append_map_timestamp_boolean_element,
    TimestampMicrosecondBuilder,
    BooleanBuilder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_timestamp_int8_element,
    TimestampMicrosecondBuilder,
    Int8Builder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_timestamp_int16_element,
    TimestampMicrosecondBuilder,
    Int16Builder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_timestamp_int32_element,
    TimestampMicrosecondBuilder,
    Int32Builder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_timestamp_int64_element,
    TimestampMicrosecondBuilder,
    Int64Builder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_timestamp_float32_element,
    TimestampMicrosecondBuilder,
    Float32Builder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_timestamp_float64_element,
    TimestampMicrosecondBuilder,
    Float64Builder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_timestamp_date32_element,
    TimestampMicrosecondBuilder,
    Date32Builder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_timestamp_timestamp_element,
    TimestampMicrosecondBuilder,
    TimestampMicrosecondBuilder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_timestamp_binary_element,
    TimestampMicrosecondBuilder,
    BinaryBuilder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_timestamp_string_element,
    TimestampMicrosecondBuilder,
    StringBuilder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_timestamp_decimal_element,
    TimestampMicrosecondBuilder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx))
);

define_append_map_struct_value_element!(
    append_map_timestamp_struct_element,
    TimestampMicrosecondBuilder,
    |builder: &mut TimestampMicrosecondBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_timestamp(idx))
);

// Binary key
define_append_map_element!(
    append_map_binary_boolean_element,
    BinaryBuilder,
    BooleanBuilder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_binary_int8_element,
    BinaryBuilder,
    Int8Builder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_binary_int16_element,
    BinaryBuilder,
    Int16Builder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_binary_int32_element,
    BinaryBuilder,
    Int32Builder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_binary_int64_element,
    BinaryBuilder,
    Int64Builder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_binary_float32_element,
    BinaryBuilder,
    Float32Builder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_binary_float64_element,
    BinaryBuilder,
    Float64Builder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_binary_date32_element,
    BinaryBuilder,
    Date32Builder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_binary_timestamp_element,
    BinaryBuilder,
    TimestampMicrosecondBuilder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_binary_binary_element,
    BinaryBuilder,
    BinaryBuilder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_binary_string_element,
    BinaryBuilder,
    StringBuilder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_binary_decimal_element,
    BinaryBuilder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx))
);

define_append_map_struct_value_element!(
    append_map_binary_struct_element,
    BinaryBuilder,
    |builder: &mut BinaryBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_binary(idx))
);

// String key
define_append_map_element!(
    append_map_string_boolean_element,
    StringBuilder,
    BooleanBuilder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_element!(
    append_map_string_int8_element,
    StringBuilder,
    Int8Builder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_element!(
    append_map_string_int16_element,
    StringBuilder,
    Int16Builder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_element!(
    append_map_string_int32_element,
    StringBuilder,
    Int32Builder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_element!(
    append_map_string_int64_element,
    StringBuilder,
    Int64Builder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_element!(
    append_map_string_float32_element,
    StringBuilder,
    Float32Builder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_element!(
    append_map_string_float64_element,
    StringBuilder,
    Float64Builder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_element!(
    append_map_string_date32_element,
    StringBuilder,
    Date32Builder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_element!(
    append_map_string_timestamp_element,
    StringBuilder,
    TimestampMicrosecondBuilder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_element!(
    append_map_string_binary_element,
    StringBuilder,
    BinaryBuilder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_element!(
    append_map_string_string_element,
    StringBuilder,
    StringBuilder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx)),
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

define_append_map_decimal_value_element!(
    append_map_string_decimal_element,
    StringBuilder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx))
);

define_append_map_struct_value_element!(
    append_map_string_struct_element,
    StringBuilder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx))
);

// Decimal key
define_append_map_decimal_key_element!(
    append_map_decimal_boolean_element,
    BooleanBuilder,
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_decimal_key_element!(
    append_map_decimal_int8_element,
    Int8Builder,
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_decimal_key_element!(
    append_map_decimal_int16_element,
    Int16Builder,
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_decimal_key_element!(
    append_map_decimal_int32_element,
    Int32Builder,
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_decimal_key_element!(
    append_map_decimal_int64_element,
    Int64Builder,
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_decimal_key_element!(
    append_map_decimal_float32_element,
    Float32Builder,
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_decimal_key_element!(
    append_map_decimal_float64_element,
    Float64Builder,
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_decimal_key_element!(
    append_map_decimal_date32_element,
    Date32Builder,
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_decimal_key_element!(
    append_map_decimal_timestamp_element,
    TimestampMicrosecondBuilder,
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_decimal_key_element!(
    append_map_decimal_binary_element,
    BinaryBuilder,
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_decimal_key_element!(
    append_map_decimal_string_element,
    StringBuilder,
    |builder: &mut StringBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_string(idx))
);

// Struct key
define_append_map_struct_key_element!(
    append_map_struct_boolean_element,
    BooleanBuilder,
    |builder: &mut BooleanBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_boolean(idx))
);

define_append_map_struct_key_element!(
    append_map_struct_int8_element,
    Int8Builder,
    |builder: &mut Int8Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_byte(idx))
);

define_append_map_struct_key_element!(
    append_map_struct_int16_element,
    Int16Builder,
    |builder: &mut Int16Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_short(idx))
);

define_append_map_struct_key_element!(
    append_map_struct_int32_element,
    Int32Builder,
    |builder: &mut Int32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_int(idx))
);

define_append_map_struct_key_element!(
    append_map_struct_int64_element,
    Int64Builder,
    |builder: &mut Int64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_long(idx))
);

define_append_map_struct_key_element!(
    append_map_struct_float32_element,
    Float32Builder,
    |builder: &mut Float32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_float(idx))
);

define_append_map_struct_key_element!(
    append_map_struct_float64_element,
    Float64Builder,
    |builder: &mut Float64Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_double(idx))
);

define_append_map_struct_key_element!(
    append_map_struct_date32_element,
    Date32Builder,
    |builder: &mut Date32Builder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_date(idx))
);

define_append_map_struct_key_element!(
    append_map_struct_timestamp_element,
    TimestampMicrosecondBuilder,
    |builder: &mut TimestampMicrosecondBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_timestamp(idx))
);

define_append_map_struct_key_element!(
    append_map_struct_binary_element,
    BinaryBuilder,
    |builder: &mut BinaryBuilder, values: &SparkUnsafeArray, idx: usize| builder
        .append_value(values.get_binary(idx))
);

define_append_map_struct_key_element!(
    append_map_struct_string_element,
    StringBuilder,
    |builder: &mut StringBuilder, keys: &SparkUnsafeArray, idx: usize| builder
        .append_value(keys.get_string(idx))
);

/// Appending the given map stored in `SparkUnsafeMap` into `MapBuilder`.
/// `field` includes data types of the map element. `map_builder` is the map builder.
/// `map` is the map stored in `SparkUnsafeMap`.
pub fn append_map_elements<K: ArrayBuilder, V: ArrayBuilder>(
    field: &FieldRef,
    map_builder: &mut MapBuilder<K, V>,
    map: &SparkUnsafeMap,
) -> Result<(), CometError> {
    let (key_field, value_field, _) = get_map_key_value_fields(field)?;

    // macro cannot expand to match arm
    match (key_field.data_type(), value_field.data_type()) {
        (DataType::Boolean, DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, BooleanBuilder>, map_builder);
            append_map_boolean_boolean_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, Int8Builder>, map_builder);
            append_map_boolean_int8_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, Int16Builder>, map_builder);
            append_map_boolean_int16_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, Int32Builder>, map_builder);
            append_map_boolean_int32_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, Int64Builder>, map_builder);
            append_map_boolean_int64_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, Float32Builder>, map_builder);
            append_map_boolean_float32_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, Float64Builder>, map_builder);
            append_map_boolean_float64_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, Date32Builder>, map_builder);
            append_map_boolean_date32_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<BooleanBuilder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_boolean_timestamp_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, BinaryBuilder>, map_builder);
            append_map_boolean_binary_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, StringBuilder>, map_builder);
            append_map_boolean_string_element(map_builder, map)?;
        }
        (DataType::Boolean, DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, Decimal128Builder>, map_builder);
            append_map_boolean_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Boolean, DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BooleanBuilder, StructBuilder>, map_builder);
            append_map_boolean_struct_element(map_builder, map, fields)?;
        }
        (DataType::Int8, DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, BooleanBuilder>, map_builder);
            append_map_int8_boolean_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, Int8Builder>, map_builder);
            append_map_int8_int8_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, Int16Builder>, map_builder);
            append_map_int8_int16_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, Int32Builder>, map_builder);
            append_map_int8_int32_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, Int64Builder>, map_builder);
            append_map_int8_int64_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, Float32Builder>, map_builder);
            append_map_int8_float32_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, Float64Builder>, map_builder);
            append_map_int8_float64_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, Date32Builder>, map_builder);
            append_map_int8_date32_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<Int8Builder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_int8_timestamp_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, BinaryBuilder>, map_builder);
            append_map_int8_binary_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, StringBuilder>, map_builder);
            append_map_int8_string_element(map_builder, map)?;
        }
        (DataType::Int8, DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, Decimal128Builder>, map_builder);
            append_map_int8_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Int8, DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int8Builder, StructBuilder>, map_builder);
            append_map_int8_struct_element(map_builder, map, fields)?;
        }
        (DataType::Int16, DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, BooleanBuilder>, map_builder);
            append_map_int16_boolean_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, Int8Builder>, map_builder);
            append_map_int16_int8_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, Int16Builder>, map_builder);
            append_map_int16_int16_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, Int32Builder>, map_builder);
            append_map_int16_int32_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, Int64Builder>, map_builder);
            append_map_int16_int64_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, Float32Builder>, map_builder);
            append_map_int16_float32_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, Float64Builder>, map_builder);
            append_map_int16_float64_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, Date32Builder>, map_builder);
            append_map_int16_date32_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<Int16Builder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_int16_timestamp_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, BinaryBuilder>, map_builder);
            append_map_int16_binary_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, StringBuilder>, map_builder);
            append_map_int16_string_element(map_builder, map)?;
        }
        (DataType::Int16, DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, Decimal128Builder>, map_builder);
            append_map_int16_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Int16, DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int16Builder, StructBuilder>, map_builder);
            append_map_int16_struct_element(map_builder, map, fields)?;
        }
        (DataType::Int32, DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, BooleanBuilder>, map_builder);
            append_map_int32_boolean_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, Int8Builder>, map_builder);
            append_map_int32_int8_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, Int16Builder>, map_builder);
            append_map_int32_int16_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, Int32Builder>, map_builder);
            append_map_int32_int32_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, Int64Builder>, map_builder);
            append_map_int32_int64_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, Float32Builder>, map_builder);
            append_map_int32_float32_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, Float64Builder>, map_builder);
            append_map_int32_float64_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, Date32Builder>, map_builder);
            append_map_int32_date32_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<Int32Builder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_int32_timestamp_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, BinaryBuilder>, map_builder);
            append_map_int32_binary_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, StringBuilder>, map_builder);
            append_map_int32_string_element(map_builder, map)?;
        }
        (DataType::Int32, DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, Decimal128Builder>, map_builder);
            append_map_int32_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Int32, DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int32Builder, StructBuilder>, map_builder);
            append_map_int32_struct_element(map_builder, map, fields)?;
        }
        (DataType::Int64, DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, BooleanBuilder>, map_builder);
            append_map_int64_boolean_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, Int8Builder>, map_builder);
            append_map_int64_int8_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, Int16Builder>, map_builder);
            append_map_int64_int16_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, Int32Builder>, map_builder);
            append_map_int64_int32_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, Int64Builder>, map_builder);
            append_map_int64_int64_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, Float32Builder>, map_builder);
            append_map_int64_float32_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, Float64Builder>, map_builder);
            append_map_int64_float64_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, Date32Builder>, map_builder);
            append_map_int64_date32_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<Int64Builder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_int64_timestamp_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, BinaryBuilder>, map_builder);
            append_map_int64_binary_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, StringBuilder>, map_builder);
            append_map_int64_string_element(map_builder, map)?;
        }
        (DataType::Int64, DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, Decimal128Builder>, map_builder);
            append_map_int64_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Int64, DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Int64Builder, StructBuilder>, map_builder);
            append_map_int64_struct_element(map_builder, map, fields)?;
        }
        (DataType::Float32, DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, BooleanBuilder>, map_builder);
            append_map_float32_boolean_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, Int8Builder>, map_builder);
            append_map_float32_int8_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, Int16Builder>, map_builder);
            append_map_float32_int16_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, Int32Builder>, map_builder);
            append_map_float32_int32_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, Int64Builder>, map_builder);
            append_map_float32_int64_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, Float32Builder>, map_builder);
            append_map_float32_float32_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, Float64Builder>, map_builder);
            append_map_float32_float64_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, Date32Builder>, map_builder);
            append_map_float32_date32_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<Float32Builder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_float32_timestamp_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, BinaryBuilder>, map_builder);
            append_map_float32_binary_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, StringBuilder>, map_builder);
            append_map_float32_string_element(map_builder, map)?;
        }
        (DataType::Float32, DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, Decimal128Builder>, map_builder);
            append_map_float32_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Float32, DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float32Builder, StructBuilder>, map_builder);
            append_map_float32_struct_element(map_builder, map, fields)?;
        }
        (DataType::Float64, DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, BooleanBuilder>, map_builder);
            append_map_float64_boolean_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, Int8Builder>, map_builder);
            append_map_float64_int8_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, Int16Builder>, map_builder);
            append_map_float64_int16_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, Int32Builder>, map_builder);
            append_map_float64_int32_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, Int64Builder>, map_builder);
            append_map_float64_int64_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, Float32Builder>, map_builder);
            append_map_float64_float32_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, Float64Builder>, map_builder);
            append_map_float64_float64_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, Date32Builder>, map_builder);
            append_map_float64_date32_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<Float64Builder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_float64_timestamp_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, BinaryBuilder>, map_builder);
            append_map_float64_binary_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, StringBuilder>, map_builder);
            append_map_float64_string_element(map_builder, map)?;
        }
        (DataType::Float64, DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, Decimal128Builder>, map_builder);
            append_map_float64_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Float64, DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Float64Builder, StructBuilder>, map_builder);
            append_map_float64_struct_element(map_builder, map, fields)?;
        }
        (DataType::Date32, DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, BooleanBuilder>, map_builder);
            append_map_date32_boolean_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, Int8Builder>, map_builder);
            append_map_date32_int8_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, Int16Builder>, map_builder);
            append_map_date32_int16_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, Int32Builder>, map_builder);
            append_map_date32_int32_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, Int64Builder>, map_builder);
            append_map_date32_int64_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, Float32Builder>, map_builder);
            append_map_date32_float32_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, Float64Builder>, map_builder);
            append_map_date32_float64_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, Date32Builder>, map_builder);
            append_map_date32_date32_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<Date32Builder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_date32_timestamp_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, BinaryBuilder>, map_builder);
            append_map_date32_binary_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, StringBuilder>, map_builder);
            append_map_date32_string_element(map_builder, map)?;
        }
        (DataType::Date32, DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, Decimal128Builder>, map_builder);
            append_map_date32_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Date32, DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Date32Builder, StructBuilder>, map_builder);
            append_map_date32_struct_element(map_builder, map, fields)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Boolean) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, BooleanBuilder>,
                map_builder
            );
            append_map_timestamp_boolean_element(map_builder, map)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int8) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, Int8Builder>,
                map_builder
            );
            append_map_timestamp_int8_element(map_builder, map)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int16) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, Int16Builder>,
                map_builder
            );
            append_map_timestamp_int16_element(map_builder, map)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int32) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, Int32Builder>,
                map_builder
            );
            append_map_timestamp_int32_element(map_builder, map)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Int64) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, Int64Builder>,
                map_builder
            );
            append_map_timestamp_int64_element(map_builder, map)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Float32) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, Float32Builder>,
                map_builder
            );
            append_map_timestamp_float32_element(map_builder, map)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Float64) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, Float64Builder>,
                map_builder
            );
            append_map_timestamp_float64_element(map_builder, map)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Date32) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, Date32Builder>,
                map_builder
            );
            append_map_timestamp_date32_element(map_builder, map)?;
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, _),
        ) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_timestamp_timestamp_element(map_builder, map)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Binary) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, BinaryBuilder>,
                map_builder
            );
            append_map_timestamp_binary_element(map_builder, map)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Utf8) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<TimestampMicrosecondBuilder, StringBuilder>,
                map_builder
            );
            append_map_timestamp_string_element(map_builder, map)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Decimal128(p, _)) => {
            let map_builder = downcast_builder_ref!(MapBuilder<TimestampMicrosecondBuilder, Decimal128Builder>, map_builder);
            append_map_timestamp_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Struct(fields)) => {
            let map_builder = downcast_builder_ref!(MapBuilder<TimestampMicrosecondBuilder, StructBuilder>, map_builder);
            append_map_timestamp_struct_element(map_builder, map, fields)?;
        }
        (DataType::Binary, DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, BooleanBuilder>, map_builder);
            append_map_binary_boolean_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, Int8Builder>, map_builder);
            append_map_binary_int8_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, Int16Builder>, map_builder);
            append_map_binary_int16_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, Int32Builder>, map_builder);
            append_map_binary_int32_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, Int64Builder>, map_builder);
            append_map_binary_int64_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, Float32Builder>, map_builder);
            append_map_binary_float32_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, Float64Builder>, map_builder);
            append_map_binary_float64_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, Date32Builder>, map_builder);
            append_map_binary_date32_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<BinaryBuilder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_binary_timestamp_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, BinaryBuilder>, map_builder);
            append_map_binary_binary_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, StringBuilder>, map_builder);
            append_map_binary_string_element(map_builder, map)?;
        }
        (DataType::Binary, DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, Decimal128Builder>, map_builder);
            append_map_binary_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Binary, DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<BinaryBuilder, StructBuilder>, map_builder);
            append_map_binary_struct_element(map_builder, map, fields)?;
        }
        (DataType::Utf8, DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, BooleanBuilder>, map_builder);
            append_map_string_boolean_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, Int8Builder>, map_builder);
            append_map_string_int8_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, Int16Builder>, map_builder);
            append_map_string_int16_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, Int32Builder>, map_builder);
            append_map_string_int32_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, Int64Builder>, map_builder);
            append_map_string_int64_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, Float32Builder>, map_builder);
            append_map_string_float32_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, Float64Builder>, map_builder);
            append_map_string_float64_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, Date32Builder>, map_builder);
            append_map_string_date32_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<StringBuilder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_string_timestamp_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, BinaryBuilder>, map_builder);
            append_map_string_binary_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, StringBuilder>, map_builder);
            append_map_string_string_element(map_builder, map)?;
        }
        (DataType::Utf8, DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, Decimal128Builder>, map_builder);
            append_map_string_decimal_element(map_builder, map, *p)?;
        }
        (DataType::Utf8, DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StringBuilder, StructBuilder>, map_builder);
            append_map_string_struct_element(map_builder, map, fields)?;
        }
        (DataType::Decimal128(p, _), DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, BooleanBuilder>, map_builder);
            append_map_decimal_boolean_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p, _), DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, Int8Builder>, map_builder);
            append_map_decimal_int8_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p, _), DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, Int16Builder>, map_builder);
            append_map_decimal_int16_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p, _), DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, Int32Builder>, map_builder);
            append_map_decimal_int32_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p, _), DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, Int64Builder>, map_builder);
            append_map_decimal_int64_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p, _), DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, Float32Builder>, map_builder);
            append_map_decimal_float32_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p, _), DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, Float64Builder>, map_builder);
            append_map_decimal_float64_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p, _), DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, Date32Builder>, map_builder);
            append_map_decimal_date32_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p, _), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<Decimal128Builder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_decimal_timestamp_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p, _), DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, BinaryBuilder>, map_builder);
            append_map_decimal_binary_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p, _), DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, StringBuilder>, map_builder);
            append_map_decimal_string_element(map_builder, map, *p)?;
        }
        (DataType::Decimal128(p1, _), DataType::Decimal128(p2, _)) => {
            let map_builder = downcast_builder_ref!(MapBuilder<Decimal128Builder, Decimal128Builder>, map_builder);
            append_map_decimal_decimal_element(map_builder, map, *p1, *p2)?;
        }
        (DataType::Decimal128(p, _), DataType::Struct(fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<Decimal128Builder, StructBuilder>, map_builder);
            append_map_decimal_struct_element(map_builder, map, *p, fields)?;
        }
        (DataType::Struct(fields), DataType::Boolean) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, BooleanBuilder>, map_builder);
            append_map_struct_boolean_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Int8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, Int8Builder>, map_builder);
            append_map_struct_int8_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Int16) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, Int16Builder>, map_builder);
            append_map_struct_int16_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Int32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, Int32Builder>, map_builder);
            append_map_struct_int32_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Int64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, Int64Builder>, map_builder);
            append_map_struct_int64_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Float32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, Float32Builder>, map_builder);
            append_map_struct_float32_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Float64) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, Float64Builder>, map_builder);
            append_map_struct_float64_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Date32) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, Date32Builder>, map_builder);
            append_map_struct_date32_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            let map_builder = downcast_builder_ref!(
                MapBuilder<StructBuilder, TimestampMicrosecondBuilder>,
                map_builder
            );
            append_map_struct_timestamp_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Binary) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, BinaryBuilder>, map_builder);
            append_map_struct_binary_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Utf8) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, StringBuilder>, map_builder);
            append_map_struct_string_element(map_builder, map, fields)?;
        }
        (DataType::Struct(fields), DataType::Decimal128(p, _)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, Decimal128Builder>, map_builder);
            append_map_struct_decimal_element(map_builder, map, *p, fields)?;
        }
        (DataType::Struct(key_fields), DataType::Struct(value_fields)) => {
            let map_builder =
                downcast_builder_ref!(MapBuilder<StructBuilder, StructBuilder>, map_builder);
            append_map_struct_struct_element(map_builder, map, key_fields, value_fields)?;
        }
        _ => {
            return Err(CometError::Internal(format!(
                "Unsupported map key/value data type: {:?}/{:?}",
                key_field.data_type(),
                value_field.data_type()
            )))
        }
    }

    Ok(())
}

#[allow(clippy::field_reassign_with_default)]
pub fn get_map_key_value_fields(
    field: &FieldRef,
) -> Result<(&FieldRef, &FieldRef, MapFieldNames), CometError> {
    let mut map_fieldnames = MapFieldNames::default();
    map_fieldnames.entry = field.name().to_string();

    let (key_field, value_field) = match field.data_type() {
        DataType::Struct(fields) => {
            if fields.len() != 2 {
                return Err(CometError::Internal(format!(
                    "Map field should have 2 fields, but got {}",
                    fields.len()
                )));
            }

            let key = &fields[0];
            let value = &fields[1];

            map_fieldnames.key = key.name().to_string();
            map_fieldnames.value = value.name().to_string();

            (key, value)
        }
        _ => {
            return Err(CometError::Internal(format!(
                "Map field should be a struct, but got {:?}",
                field.data_type()
            )));
        }
    };

    Ok((key_field, value_field, map_fieldnames))
}
