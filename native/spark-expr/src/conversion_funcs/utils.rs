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

use crate::SparkError;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, GenericStringArray, PrimitiveArray,
};
use arrow::compute::unary;
use arrow::datatypes::{DataType, Int64Type};
use arrow::error::ArrowError;
use datafusion::common::cast::as_generic_string_array;
use num::integer::div_floor;
use std::sync::Arc;

const MICROS_PER_SECOND: i64 = 1000000;
/// A fork & modified version of Arrow's `unary_dyn` which is being deprecated
pub fn unary_dyn<F, T>(array: &ArrayRef, op: F) -> Result<ArrayRef, ArrowError>
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

/// This takes for special casting cases of Spark. E.g., Timestamp to Long.
/// This function runs as a post process of the DataFusion cast(). By the time it arrives here,
/// Dictionary arrays are already unpacked by the DataFusion cast() since Spark cannot specify
/// Dictionary as to_type. The from_type is taken before the DataFusion cast() runs in
/// expressions/cast.rs, so it can be still Dictionary.
pub fn spark_cast_postprocess(
    array: ArrayRef,
    from_type: &DataType,
    to_type: &DataType,
) -> ArrayRef {
    match (from_type, to_type) {
        (DataType::Timestamp(_, _), DataType::Int64) => {
            // See Spark's `Cast` expression
            unary_dyn::<_, Int64Type>(&array, |v| div_floor(v, MICROS_PER_SECOND)).unwrap()
        }
        (DataType::Dictionary(_, value_type), DataType::Int64)
            if matches!(value_type.as_ref(), &DataType::Timestamp(_, _)) =>
        {
            // See Spark's `Cast` expression
            unary_dyn::<_, Int64Type>(&array, |v| div_floor(v, MICROS_PER_SECOND)).unwrap()
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

#[inline]
pub fn is_identity_cast(from_type: &DataType, to_type: &DataType) -> bool {
    from_type == to_type
}

#[inline]
pub fn cast_overflow(value: &str, from_type: &str, to_type: &str) -> SparkError {
    SparkError::CastOverFlow {
        value: value.to_string(),
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
    }
}

#[inline]
pub fn invalid_value(value: &str, from_type: &str, to_type: &str) -> SparkError {
    SparkError::CastInvalidValue {
        value: value.to_string(),
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
    }
}
