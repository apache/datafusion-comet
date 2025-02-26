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

use arrow_array::builder::{make_builder, ArrayBuilder};
use std::sync::Arc;

use crate::common::bit::ceil;
use arrow::datatypes::*;
use datafusion::arrow::{
    array::*,
    datatypes::{DataType, SchemaRef, TimeUnit},
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};

pub(crate) fn new_array_builders(
    schema: &SchemaRef,
    batch_size: usize,
) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let dt = field.data_type();
            if matches!(dt, DataType::Dictionary(_, _)) {
                make_dict_builder(dt, batch_size)
            } else {
                make_builder(dt, batch_size)
            }
        })
        .collect::<Vec<_>>()
}

macro_rules! primitive_dict_builder_inner_helper {
    ($kt:ty, $vt:ty, $capacity:ident) => {
        Box::new(PrimitiveDictionaryBuilder::<$kt, $vt>::with_capacity(
            $capacity,
            $capacity / 100,
        ))
    };
}

macro_rules! primitive_dict_builder_helper {
    ($kt:ty, $vt:ident, $capacity:ident) => {
        match $vt.as_ref() {
            DataType::Int8 => {
                primitive_dict_builder_inner_helper!($kt, Int8Type, $capacity)
            }
            DataType::Int16 => {
                primitive_dict_builder_inner_helper!($kt, Int16Type, $capacity)
            }
            DataType::Int32 => {
                primitive_dict_builder_inner_helper!($kt, Int32Type, $capacity)
            }
            DataType::Int64 => {
                primitive_dict_builder_inner_helper!($kt, Int64Type, $capacity)
            }
            DataType::UInt8 => {
                primitive_dict_builder_inner_helper!($kt, UInt8Type, $capacity)
            }
            DataType::UInt16 => {
                primitive_dict_builder_inner_helper!($kt, UInt16Type, $capacity)
            }
            DataType::UInt32 => {
                primitive_dict_builder_inner_helper!($kt, UInt32Type, $capacity)
            }
            DataType::UInt64 => {
                primitive_dict_builder_inner_helper!($kt, UInt64Type, $capacity)
            }
            DataType::Float32 => {
                primitive_dict_builder_inner_helper!($kt, Float32Type, $capacity)
            }
            DataType::Float64 => {
                primitive_dict_builder_inner_helper!($kt, Float64Type, $capacity)
            }
            DataType::Decimal128(p, s) => {
                let keys_builder = PrimitiveBuilder::<$kt>::new();
                let values_builder =
                    Decimal128Builder::new().with_data_type(DataType::Decimal128(*p, *s));
                Box::new(
                    PrimitiveDictionaryBuilder::<$kt, Decimal128Type>::new_from_empty_builders(
                        keys_builder,
                        values_builder,
                    ),
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                let keys_builder = PrimitiveBuilder::<$kt>::new();
                let values_builder = TimestampMicrosecondBuilder::new()
                    .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()));
                Box::new(
                    PrimitiveDictionaryBuilder::<$kt, TimestampMicrosecondType>::new_from_empty_builders(
                        keys_builder,
                        values_builder,
                    ),
                )
            }
            DataType::Date32 => {
                primitive_dict_builder_inner_helper!($kt, Date32Type, $capacity)
            }
            DataType::Date64 => {
                primitive_dict_builder_inner_helper!($kt, Date64Type, $capacity)
            }
            t => unimplemented!("{:?} is not supported", t),
        }
    };
}

macro_rules! byte_dict_builder_inner_helper {
    ($kt:ty, $capacity:ident, $builder:ident) => {
        Box::new($builder::<$kt>::with_capacity(
            $capacity,
            $capacity / 100,
            $capacity,
        ))
    };
}

/// Returns a dictionary array builder with capacity `capacity` that corresponds to the datatype
/// `DataType` This function is useful to construct arrays from an arbitrary vectors with
/// known/expected schema.
/// TODO: move this to the upstream.
fn make_dict_builder(datatype: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match datatype {
        DataType::Dictionary(key_type, value_type) if value_type.is_primitive() => {
            match key_type.as_ref() {
                DataType::Int8 => primitive_dict_builder_helper!(Int8Type, value_type, capacity),
                DataType::Int16 => primitive_dict_builder_helper!(Int16Type, value_type, capacity),
                DataType::Int32 => primitive_dict_builder_helper!(Int32Type, value_type, capacity),
                DataType::Int64 => primitive_dict_builder_helper!(Int64Type, value_type, capacity),
                DataType::UInt8 => primitive_dict_builder_helper!(UInt8Type, value_type, capacity),
                DataType::UInt16 => {
                    primitive_dict_builder_helper!(UInt16Type, value_type, capacity)
                }
                DataType::UInt32 => {
                    primitive_dict_builder_helper!(UInt32Type, value_type, capacity)
                }
                DataType::UInt64 => {
                    primitive_dict_builder_helper!(UInt64Type, value_type, capacity)
                }
                _ => unreachable!(""),
            }
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::Utf8) =>
        {
            match key_type.as_ref() {
                DataType::Int8 => {
                    byte_dict_builder_inner_helper!(Int8Type, capacity, StringDictionaryBuilder)
                }
                DataType::Int16 => {
                    byte_dict_builder_inner_helper!(Int16Type, capacity, StringDictionaryBuilder)
                }
                DataType::Int32 => {
                    byte_dict_builder_inner_helper!(Int32Type, capacity, StringDictionaryBuilder)
                }
                DataType::Int64 => {
                    byte_dict_builder_inner_helper!(Int64Type, capacity, StringDictionaryBuilder)
                }
                DataType::UInt8 => {
                    byte_dict_builder_inner_helper!(UInt8Type, capacity, StringDictionaryBuilder)
                }
                DataType::UInt16 => {
                    byte_dict_builder_inner_helper!(UInt16Type, capacity, StringDictionaryBuilder)
                }
                DataType::UInt32 => {
                    byte_dict_builder_inner_helper!(UInt32Type, capacity, StringDictionaryBuilder)
                }
                DataType::UInt64 => {
                    byte_dict_builder_inner_helper!(UInt64Type, capacity, StringDictionaryBuilder)
                }
                _ => unreachable!(""),
            }
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::LargeUtf8) =>
        {
            match key_type.as_ref() {
                DataType::Int8 => byte_dict_builder_inner_helper!(
                    Int8Type,
                    capacity,
                    LargeStringDictionaryBuilder
                ),
                DataType::Int16 => byte_dict_builder_inner_helper!(
                    Int16Type,
                    capacity,
                    LargeStringDictionaryBuilder
                ),
                DataType::Int32 => byte_dict_builder_inner_helper!(
                    Int32Type,
                    capacity,
                    LargeStringDictionaryBuilder
                ),
                DataType::Int64 => byte_dict_builder_inner_helper!(
                    Int64Type,
                    capacity,
                    LargeStringDictionaryBuilder
                ),
                DataType::UInt8 => byte_dict_builder_inner_helper!(
                    UInt8Type,
                    capacity,
                    LargeStringDictionaryBuilder
                ),
                DataType::UInt16 => {
                    byte_dict_builder_inner_helper!(
                        UInt16Type,
                        capacity,
                        LargeStringDictionaryBuilder
                    )
                }
                DataType::UInt32 => {
                    byte_dict_builder_inner_helper!(
                        UInt32Type,
                        capacity,
                        LargeStringDictionaryBuilder
                    )
                }
                DataType::UInt64 => {
                    byte_dict_builder_inner_helper!(
                        UInt64Type,
                        capacity,
                        LargeStringDictionaryBuilder
                    )
                }
                _ => unreachable!(""),
            }
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::Binary) =>
        {
            match key_type.as_ref() {
                DataType::Int8 => {
                    byte_dict_builder_inner_helper!(Int8Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::Int16 => {
                    byte_dict_builder_inner_helper!(Int16Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::Int32 => {
                    byte_dict_builder_inner_helper!(Int32Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::Int64 => {
                    byte_dict_builder_inner_helper!(Int64Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::UInt8 => {
                    byte_dict_builder_inner_helper!(UInt8Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::UInt16 => {
                    byte_dict_builder_inner_helper!(UInt16Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::UInt32 => {
                    byte_dict_builder_inner_helper!(UInt32Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::UInt64 => {
                    byte_dict_builder_inner_helper!(UInt64Type, capacity, BinaryDictionaryBuilder)
                }
                _ => unreachable!(""),
            }
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::LargeBinary) =>
        {
            match key_type.as_ref() {
                DataType::Int8 => byte_dict_builder_inner_helper!(
                    Int8Type,
                    capacity,
                    LargeBinaryDictionaryBuilder
                ),
                DataType::Int16 => byte_dict_builder_inner_helper!(
                    Int16Type,
                    capacity,
                    LargeBinaryDictionaryBuilder
                ),
                DataType::Int32 => byte_dict_builder_inner_helper!(
                    Int32Type,
                    capacity,
                    LargeBinaryDictionaryBuilder
                ),
                DataType::Int64 => byte_dict_builder_inner_helper!(
                    Int64Type,
                    capacity,
                    LargeBinaryDictionaryBuilder
                ),
                DataType::UInt8 => byte_dict_builder_inner_helper!(
                    UInt8Type,
                    capacity,
                    LargeBinaryDictionaryBuilder
                ),
                DataType::UInt16 => {
                    byte_dict_builder_inner_helper!(
                        UInt16Type,
                        capacity,
                        LargeBinaryDictionaryBuilder
                    )
                }
                DataType::UInt32 => {
                    byte_dict_builder_inner_helper!(
                        UInt32Type,
                        capacity,
                        LargeBinaryDictionaryBuilder
                    )
                }
                DataType::UInt64 => {
                    byte_dict_builder_inner_helper!(
                        UInt64Type,
                        capacity,
                        LargeBinaryDictionaryBuilder
                    )
                }
                _ => unreachable!(""),
            }
        }
        t => panic!("Data type {t:?} is not currently supported"),
    }
}

pub(crate) fn slot_size(len: usize, data_type: &DataType) -> usize {
    match data_type {
        DataType::Boolean => ceil(len, 8),
        DataType::Int8 => len,
        DataType::Int16 => len * 2,
        DataType::Int32 => len * 4,
        DataType::Int64 => len * 8,
        DataType::UInt8 => len,
        DataType::UInt16 => len * 2,
        DataType::UInt32 => len * 4,
        DataType::UInt64 => len * 8,
        DataType::Float32 => len * 4,
        DataType::Float64 => len * 8,
        DataType::Date32 => len * 4,
        DataType::Date64 => len * 8,
        DataType::Time32(TimeUnit::Second) => len * 4,
        DataType::Time32(TimeUnit::Millisecond) => len * 4,
        DataType::Time64(TimeUnit::Microsecond) => len * 8,
        DataType::Time64(TimeUnit::Nanosecond) => len * 8,
        // TODO: this is not accurate, but should be good enough for now
        DataType::Utf8 => len * 100 + len * 4,
        DataType::LargeUtf8 => len * 100 + len * 8,
        DataType::Decimal128(_, _) => len * 16,
        DataType::Dictionary(key_type, value_type) => {
            // TODO: this is not accurate, but should be good enough for now
            slot_size(len, key_type.as_ref()) + slot_size(len / 10, value_type.as_ref())
        }
        // TODO: this is not accurate, but should be good enough for now
        DataType::Binary => len * 100 + len * 4,
        DataType::LargeBinary => len * 100 + len * 8,
        DataType::FixedSizeBinary(s) => len * (*s as usize),
        DataType::Timestamp(_, _) => len * 8,
        dt => unimplemented!(
            "{}",
            format!("data type {dt} not supported in shuffle write")
        ),
    }
}

pub(crate) fn append_columns(
    to: &mut Box<dyn ArrayBuilder>,
    from: &Arc<dyn Array>,
    indices: &[usize],
    data_type: &DataType,
) {
    /// Append values from `from` to `to` using `indices`.
    macro_rules! append {
        ($arrowty:ident) => {{
            type B = paste::paste! {[< $arrowty Builder >]};
            type A = paste::paste! {[< $arrowty Array >]};
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            let f = from.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append_value(f.value(i));
                } else {
                    t.append_null();
                }
            }
        }};
    }

    /// Some array builder (e.g. `FixedSizeBinary`) its `append_value` method returning
    /// a `Result`.
    macro_rules! append_unwrap {
        ($arrowty:ident) => {{
            type B = paste::paste! {[< $arrowty Builder >]};
            type A = paste::paste! {[< $arrowty Array >]};
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            let f = from.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append_value(f.value(i)).unwrap();
                } else {
                    t.append_null();
                }
            }
        }};
    }

    /// Appends values from a dictionary array to a dictionary builder.
    macro_rules! append_dict {
        ($kt:ty, $builder:ty, $dict_array:ty) => {{
            let t = to.as_any_mut().downcast_mut::<$builder>().unwrap();
            let f = from
                .as_any()
                .downcast_ref::<DictionaryArray<$kt>>()
                .unwrap()
                .downcast_dict::<$dict_array>()
                .unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append_value(f.value(i));
                } else {
                    t.append_null();
                }
            }
        }};
    }

    macro_rules! append_dict_helper {
        ($kt:ident, $ty:ty, $dict_array:ty) => {{
            match $kt.as_ref() {
                DataType::Int8 => append_dict!(Int8Type, PrimitiveDictionaryBuilder<Int8Type, $ty>, $dict_array),
                DataType::Int16 => append_dict!(Int16Type, PrimitiveDictionaryBuilder<Int16Type, $ty>, $dict_array),
                DataType::Int32 => append_dict!(Int32Type, PrimitiveDictionaryBuilder<Int32Type, $ty>, $dict_array),
                DataType::Int64 => append_dict!(Int64Type, PrimitiveDictionaryBuilder<Int64Type, $ty>, $dict_array),
                DataType::UInt8 => append_dict!(UInt8Type, PrimitiveDictionaryBuilder<UInt8Type, $ty>, $dict_array),
                DataType::UInt16 => {
                    append_dict!(UInt16Type, PrimitiveDictionaryBuilder<UInt16Type, $ty>, $dict_array)
                }
                DataType::UInt32 => {
                    append_dict!(UInt32Type, PrimitiveDictionaryBuilder<UInt32Type, $ty>, $dict_array)
                }
                DataType::UInt64 => {
                    append_dict!(UInt64Type, PrimitiveDictionaryBuilder<UInt64Type, $ty>, $dict_array)
                }
                _ => unreachable!("Unknown key type for dictionary"),
            }
        }};
    }

    macro_rules! primitive_append_dict_helper {
        ($kt:ident, $vt:ident) => {
            match $vt.as_ref() {
                DataType::Int8 => {
                    append_dict_helper!($kt, Int8Type, Int8Array)
                }
                DataType::Int16 => {
                    append_dict_helper!($kt, Int16Type, Int16Array)
                }
                DataType::Int32 => {
                    append_dict_helper!($kt, Int32Type, Int32Array)
                }
                DataType::Int64 => {
                    append_dict_helper!($kt, Int64Type, Int64Array)
                }
                DataType::UInt8 => {
                    append_dict_helper!($kt, UInt8Type, UInt8Array)
                }
                DataType::UInt16 => {
                    append_dict_helper!($kt, UInt16Type, UInt16Array)
                }
                DataType::UInt32 => {
                    append_dict_helper!($kt, UInt32Type, UInt32Array)
                }
                DataType::UInt64 => {
                    append_dict_helper!($kt, UInt64Type, UInt64Array)
                }
                DataType::Float32 => {
                    append_dict_helper!($kt, Float32Type, Float32Array)
                }
                DataType::Float64 => {
                    append_dict_helper!($kt, Float64Type, Float64Array)
                }
                DataType::Decimal128(_, _) => {
                    append_dict_helper!($kt, Decimal128Type, Decimal128Array)
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    append_dict_helper!($kt, TimestampMicrosecondType, TimestampMicrosecondArray)
                }
                DataType::Date32 => {
                    append_dict_helper!($kt, Date32Type, Date32Array)
                }
                DataType::Date64 => {
                    append_dict_helper!($kt, Date64Type, Date64Array)
                }
                t => unimplemented!("{:?} is not supported for appending dictionary builder", t),
            }
        };
    }

    macro_rules! append_byte_dict {
        ($kt:ident, $byte_type:ty, $array_type:ty) => {{
            match $kt.as_ref() {
                DataType::Int8 => {
                    append_dict!(Int8Type, GenericByteDictionaryBuilder<Int8Type, $byte_type>, $array_type)
                }
                DataType::Int16 => {
                    append_dict!(Int16Type,  GenericByteDictionaryBuilder<Int16Type, $byte_type>, $array_type)
                }
                DataType::Int32 => {
                    append_dict!(Int32Type,  GenericByteDictionaryBuilder<Int32Type, $byte_type>, $array_type)
                }
                DataType::Int64 => {
                    append_dict!(Int64Type,  GenericByteDictionaryBuilder<Int64Type, $byte_type>, $array_type)
                }
                DataType::UInt8 => {
                    append_dict!(UInt8Type,  GenericByteDictionaryBuilder<UInt8Type, $byte_type>, $array_type)
                }
                DataType::UInt16 => {
                    append_dict!(UInt16Type, GenericByteDictionaryBuilder<UInt16Type, $byte_type>, $array_type)
                }
                DataType::UInt32 => {
                    append_dict!(UInt32Type, GenericByteDictionaryBuilder<UInt32Type, $byte_type>, $array_type)
                }
                DataType::UInt64 => {
                    append_dict!(UInt64Type, GenericByteDictionaryBuilder<UInt64Type, $byte_type>, $array_type)
                }
                _ => unreachable!("Unknown key type for dictionary"),
            }
        }};
    }

    match data_type {
        DataType::Boolean => append!(Boolean),
        DataType::Int8 => append!(Int8),
        DataType::Int16 => append!(Int16),
        DataType::Int32 => append!(Int32),
        DataType::Int64 => append!(Int64),
        DataType::UInt8 => append!(UInt8),
        DataType::UInt16 => append!(UInt16),
        DataType::UInt32 => append!(UInt32),
        DataType::UInt64 => append!(UInt64),
        DataType::Float32 => append!(Float32),
        DataType::Float64 => append!(Float64),
        DataType::Date32 => append!(Date32),
        DataType::Date64 => append!(Date64),
        DataType::Time32(TimeUnit::Second) => append!(Time32Second),
        DataType::Time32(TimeUnit::Millisecond) => append!(Time32Millisecond),
        DataType::Time64(TimeUnit::Microsecond) => append!(Time64Microsecond),
        DataType::Time64(TimeUnit::Nanosecond) => append!(Time64Nanosecond),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            append!(TimestampMicrosecond)
        }
        DataType::Utf8 => append!(String),
        DataType::LargeUtf8 => append!(LargeString),
        DataType::Decimal128(_, _) => append!(Decimal128),
        DataType::Dictionary(key_type, value_type) if value_type.is_primitive() => {
            primitive_append_dict_helper!(key_type, value_type)
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::Utf8) =>
        {
            append_byte_dict!(key_type, GenericStringType<i32>, StringArray)
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::LargeUtf8) =>
        {
            append_byte_dict!(key_type, GenericStringType<i64>, LargeStringArray)
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::Binary) =>
        {
            append_byte_dict!(key_type, GenericBinaryType<i32>, BinaryArray)
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::LargeBinary) =>
        {
            append_byte_dict!(key_type, GenericBinaryType<i64>, LargeBinaryArray)
        }
        DataType::Binary => append!(Binary),
        DataType::LargeBinary => append!(LargeBinary),
        DataType::FixedSizeBinary(_) => append_unwrap!(FixedSizeBinary),
        t => unimplemented!(
            "{}",
            format!("data type {} not supported in shuffle write", t)
        ),
    }
}

pub(crate) fn make_batch(
    schema: SchemaRef,
    mut arrays: Vec<Box<dyn ArrayBuilder>>,
    row_count: usize,
) -> ArrowResult<RecordBatch> {
    let columns = arrays.iter_mut().map(|array| array.finish()).collect();
    let options = RecordBatchOptions::new().with_row_count(Option::from(row_count));
    RecordBatch::try_new_with_options(schema, columns, &options)
}
