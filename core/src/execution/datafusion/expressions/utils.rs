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

use crate::execution::timezone::Tz;
use arrow::{
    array::{
        as_dictionary_array, as_primitive_array, Array, ArrayRef, GenericStringArray,
        PrimitiveArray,
    },
    compute::unary,
    datatypes::{Int32Type, Int64Type, TimestampMicrosecondType},
    error::ArrowError,
    temporal_conversions::as_datetime,
};
use arrow_array::{cast::AsArray, types::ArrowPrimitiveType};
use arrow_schema::DataType;
use chrono::{DateTime, Offset, TimeZone};
use datafusion_common::cast::as_generic_string_array;
use datafusion_physical_expr::PhysicalExpr;
use std::{any::Any, sync::Arc};

/// An utility function from DataFusion. It is not exposed by DataFusion.
pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn PhysicalExpr>>() {
        any.downcast_ref::<Arc<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else if any.is::<Box<dyn PhysicalExpr>>() {
        any.downcast_ref::<Box<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}

/// Preprocesses input arrays to add timezone information from Spark to Arrow array datatype or
/// to apply timezone offset.
//
//  We consider the following cases:
//
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Conversion            | Input array  | Timezone          | Output array                     |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp ->          | Array in UTC | Timezone of input | A timestamp with the timezone    |
//  |  Utf8 or Date32       |              |                   | offset applied and timezone      |
//  |                       |              |                   | removed                          |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp ->          | Array in UTC | Timezone of input | Same as input array              |
//  |  Timestamp  w/Timezone|              |                   |                                  |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp_ntz ->      | Array in     | Timezone of input | Same as input array              |
//  |   Utf8 or Date32      | timezone     |                   |                                  |
//  |                       | session local|                   |                                  |
//  |                       | timezone     |                   |                                  |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp_ntz ->      | Array in     | Timezone of input |  Array in UTC and timezone       |
//  |  Timestamp w/Timezone | session local|                   |  specified in input              |
//  |                       | timezone     |                   |                                  |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp(_ntz) ->    |                                                                     |
//  |        Any other type |              Not Supported                                          |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//
pub fn array_with_timezone(
    array: ArrayRef,
    timezone: String,
    to_type: Option<&DataType>,
) -> ArrayRef {
    match array.data_type() {
        DataType::Timestamp(_, None) => {
            assert!(!timezone.is_empty());
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => array,
                Some(DataType::Timestamp(_, Some(_))) => {
                    timestamp_ntz_to_timestamp(array, timezone.as_str(), Some(timezone.as_str()))
                }
                _ => {
                    // Not supported
                    panic!(
                        "Cannot convert from {:?} to {:?}",
                        array.data_type(),
                        to_type.unwrap()
                    )
                }
            }
        }
        DataType::Timestamp(_, Some(_)) => {
            assert!(!timezone.is_empty());
            let array = as_primitive_array::<TimestampMicrosecondType>(&array);
            let array_with_timezone = array.clone().with_timezone(timezone.clone());
            let array = Arc::new(array_with_timezone) as ArrayRef;
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => {
                    pre_timestamp_cast(array, timezone)
                }
                _ => array,
            }
        }
        DataType::Dictionary(_, value_type)
            if matches!(value_type.as_ref(), &DataType::Timestamp(_, _)) =>
        {
            let dict = as_dictionary_array::<Int32Type>(&array);
            let array = as_primitive_array::<TimestampMicrosecondType>(dict.values());
            let array_with_timezone =
                array_with_timezone(Arc::new(array.clone()) as ArrayRef, timezone, to_type);
            let dict = dict.with_values(array_with_timezone);
            Arc::new(dict) as ArrayRef
        }
        _ => array,
    }
}

/// Takes in a Timestamp(Microsecond, None) array and a timezone id, and returns
/// a Timestamp(Microsecond, Some<_>) array.
/// The understanding is that the input array has time in the timezone specified in the second
/// argument.
/// Parameters:
///     array - input array of timestamp without timezone
///     tz - timezone of the values in the input array
///     to_timezone - timezone to change the input values to
fn timestamp_ntz_to_timestamp(array: ArrayRef, tz: &str, to_timezone: Option<&str>) -> ArrayRef {
    assert!(!tz.is_empty());
    match array.data_type() {
        DataType::Timestamp(_, None) => {
            let array = as_primitive_array::<TimestampMicrosecondType>(&array);
            let tz: Tz = tz.parse().unwrap();
            let values = array.iter().map(|v| {
                v.map(|value| {
                    let local_datetime = as_datetime::<TimestampMicrosecondType>(value).unwrap();
                    let datetime: DateTime<Tz> = tz.from_local_datetime(&local_datetime).unwrap();
                    datetime.timestamp_micros()
                })
            });
            let mut array: PrimitiveArray<TimestampMicrosecondType> =
                unsafe { PrimitiveArray::from_trusted_len_iter(values) };
            array = if let Some(to_tz) = to_timezone {
                array.with_timezone(to_tz)
            } else {
                array
            };
            Arc::new(array) as ArrayRef
        }
        _ => array,
    }
}

const MICROS_PER_SECOND: i64 = 1000000;

/// This takes for special pre-casting cases of Spark. E.g., Timestamp to String.
fn pre_timestamp_cast(array: ArrayRef, timezone: String) -> ArrayRef {
    assert!(!timezone.is_empty());
    match array.data_type() {
        DataType::Timestamp(_, _) => {
            // Spark doesn't output timezone while casting timestamp to string, but arrow's cast
            // kernel does if timezone exists. So we need to apply offset of timezone to array
            // timestamp value and remove timezone from array datatype.
            let array = as_primitive_array::<TimestampMicrosecondType>(&array);

            let tz: Tz = timezone.parse().unwrap();
            let values = array.iter().map(|v| {
                v.map(|value| {
                    let datetime = as_datetime::<TimestampMicrosecondType>(value).unwrap();
                    let offset = tz.offset_from_utc_datetime(&datetime).fix();
                    let datetime = datetime + offset;
                    datetime.timestamp_micros()
                })
            });

            let array: PrimitiveArray<TimestampMicrosecondType> =
                unsafe { PrimitiveArray::from_trusted_len_iter(values) };
            Arc::new(array) as ArrayRef
        }
        _ => array,
    }
}

/// This takes for special casting cases of Spark. E.g., Timestamp to Long.
/// This function runs as a post process of the DataFusion cast(). By the time it arrives here,
/// Dictionary arrays are already unpacked by the DataFusion cast() since Spark cannot specify
/// Dictionary as to_type. The from_type is taken before the DataFusion cast() runs in
/// expressions/cast.rs, so it can be still Dictionary.
pub(crate) fn spark_cast(array: ArrayRef, from_type: &DataType, to_type: &DataType) -> ArrayRef {
    match (from_type, to_type) {
        (DataType::Timestamp(_, _), DataType::Int64) => {
            // See Spark's `Cast` expression
            unary_dyn::<_, Int64Type>(&array, |v| v.div_floor(MICROS_PER_SECOND)).unwrap()
        }
        (DataType::Dictionary(_, value_type), DataType::Int64)
            if matches!(value_type.as_ref(), &DataType::Timestamp(_, _)) =>
        {
            // See Spark's `Cast` expression
            unary_dyn::<_, Int64Type>(&array, |v| v.div_floor(MICROS_PER_SECOND)).unwrap()
        }
        (DataType::Timestamp(_, _), DataType::Utf8) => remove_trailing_zeroes(array),
        (DataType::Dictionary(_, value_type), DataType::Utf8)
            if matches!(value_type.as_ref(), &DataType::Timestamp(_, _)) =>
        {
            remove_trailing_zeroes(array)
        }
        _ => array,
    }
}

/// A fork & modified version of Arrow's `unary_dyn` which is being deprecated
fn unary_dyn<F, T>(array: &ArrayRef, op: F) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> T::Native,
{
    if let Some(d) = array.as_any_dictionary_opt() {
        let new_values = unary_dyn::<F, T>(d.values(), op)?;
        return Ok(Arc::new(d.with_values(Arc::new(new_values))));
    }

    match array.as_primitive_opt::<T>() {
        Some(a) if PrimitiveArray::<T>::is_compatible(a.data_type()) => {
            Ok(Arc::new(unary::<T, F, T>(
                array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap(),
                op,
            )))
        }
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Cannot perform unary operation of type {} on array of type {}",
            T::DATA_TYPE,
            array.data_type()
        ))),
    }
}

/// Remove any trailing zeroes in the string if they occur after in the fractional seconds,
/// to match Spark behavior
/// example:
/// "1970-01-01 05:29:59.900" => "1970-01-01 05:29:59.9"
/// "1970-01-01 05:29:59.990" => "1970-01-01 05:29:59.99"
/// "1970-01-01 05:29:59.999" => "1970-01-01 05:29:59.999"
/// "1970-01-01 05:30:00"     => "1970-01-01 05:30:00"
/// "1970-01-01 05:30:00.001" => "1970-01-01 05:30:00.001"
fn remove_trailing_zeroes(array: ArrayRef) -> ArrayRef {
    let string_array = as_generic_string_array::<i32>(&array).unwrap();
    let result = string_array
        .iter()
        .map(|s| s.map(trim_end))
        .collect::<GenericStringArray<i32>>();
    Arc::new(result) as ArrayRef
}

fn trim_end(s: &str) -> &str {
    if s.rfind('.').is_some() {
        s.trim_end_matches('0')
    } else {
        s
    }
}
