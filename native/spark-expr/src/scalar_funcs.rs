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

use arrow::array::{ArrayRef, AsArray, Float32Array, Float64Array, OffsetSizeTrait};
use arrow::compute::kernels::numeric::{add, sub};
use arrow::datatypes::IntervalDayTime;
use arrow_array::builder::{GenericStringBuilder, IntervalDayTimeBuilder};
use arrow_array::types::{Int16Type, Int32Type, Int8Type};
use arrow_array::{Array, BooleanArray, Datum};
use arrow_schema::{ArrowError, DataType};
use datafusion::physical_expr_common::datum;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::{cast::as_generic_string_array, DataFusionError, ScalarValue};
use std::fmt::Write;
use std::sync::Arc;

mod chr;
pub use chr::SparkChrFunc;

pub mod hash_expressions;
// exposed for benchmark only
pub use hash_expressions::{spark_murmur3_hash, spark_xxhash64};

/// Similar to DataFusion `rpad`, but not to truncate when the string is already longer than length
pub fn spark_read_side_padding(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    match args {
        [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(Some(length)))] => {
            match array.data_type() {
                DataType::Utf8 => spark_read_side_padding_internal::<i32>(array, *length),
                DataType::LargeUtf8 => spark_read_side_padding_internal::<i64>(array, *length),
                // TODO: handle Dictionary types
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for function read_side_padding",
                ))),
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported arguments {other:?} for function read_side_padding",
        ))),
    }
}

fn spark_read_side_padding_internal<T: OffsetSizeTrait>(
    array: &ArrayRef,
    length: i32,
) -> Result<ColumnarValue, DataFusionError> {
    let string_array = as_generic_string_array::<T>(array)?;
    let length = 0.max(length) as usize;
    let space_string = " ".repeat(length);

    let mut builder =
        GenericStringBuilder::<T>::with_capacity(string_array.len(), string_array.len() * length);

    for string in string_array.iter() {
        match string {
            Some(string) => {
                // It looks Spark's UTF8String is closer to chars rather than graphemes
                // https://stackoverflow.com/a/46290728
                let char_len = string.chars().count();
                if length <= char_len {
                    builder.append_value(string);
                } else {
                    // write_str updates only the value buffer, not null nor offset buffer
                    // This is convenient for concatenating str(s)
                    builder.write_str(string)?;
                    builder.append_value(&space_string[char_len..]);
                }
            }
            _ => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

/// Spark-compatible `isnan` expression
pub fn spark_isnan(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    fn set_nulls_to_false(is_nan: BooleanArray) -> ColumnarValue {
        match is_nan.nulls() {
            Some(nulls) => {
                let is_not_null = nulls.inner();
                ColumnarValue::Array(Arc::new(BooleanArray::new(
                    is_nan.values() & is_not_null,
                    None,
                )))
            }
            None => ColumnarValue::Array(Arc::new(is_nan)),
        }
    }
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let is_nan = BooleanArray::from_unary(array, |x| x.is_nan());
                Ok(set_nulls_to_false(is_nan))
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let is_nan = BooleanArray::from_unary(array, |x| x.is_nan());
                Ok(set_nulls_to_false(is_nan))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function isnan",
                other,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                a.map(|x| x.is_nan()).unwrap_or(false),
            )))),
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                a.map(|x| x.is_nan()).unwrap_or(false),
            )))),
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function isnan",
                value.data_type(),
            ))),
        },
    }
}

macro_rules! scalar_date_arithmetic {
    ($start:expr, $days:expr, $op:expr) => {{
        let interval = IntervalDayTime::new(*$days as i32, 0);
        let interval_cv = ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(interval)));
        datum::apply($start, &interval_cv, $op)
    }};
}
macro_rules! array_date_arithmetic {
    ($days:expr, $interval_builder:expr, $intType:ty) => {{
        for day in $days.as_primitive::<$intType>().into_iter() {
            if let Some(non_null_day) = day {
                $interval_builder.append_value(IntervalDayTime::new(non_null_day as i32, 0));
            } else {
                $interval_builder.append_null();
            }
        }
    }};
}

/// Spark-compatible `date_add` and `date_sub` expressions, which assumes days for the second
/// argument, but we cannot directly add that to a Date32. We generate an IntervalDayTime from the
/// second argument and use DataFusion's interface to apply Arrow's operators.
fn spark_date_arithmetic(
    args: &[ColumnarValue],
    op: impl Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>,
) -> Result<ColumnarValue, DataFusionError> {
    let start = &args[0];
    match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Int8(Some(days))) => {
            scalar_date_arithmetic!(start, days, op)
        }
        ColumnarValue::Scalar(ScalarValue::Int16(Some(days))) => {
            scalar_date_arithmetic!(start, days, op)
        }
        ColumnarValue::Scalar(ScalarValue::Int32(Some(days))) => {
            scalar_date_arithmetic!(start, days, op)
        }
        ColumnarValue::Array(days) => {
            let mut interval_builder = IntervalDayTimeBuilder::with_capacity(days.len());
            match days.data_type() {
                DataType::Int8 => {
                    array_date_arithmetic!(days, interval_builder, Int8Type)
                }
                DataType::Int16 => {
                    array_date_arithmetic!(days, interval_builder, Int16Type)
                }
                DataType::Int32 => {
                    array_date_arithmetic!(days, interval_builder, Int32Type)
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data types {:?} for date arithmetic.",
                        args,
                    )))
                }
            }
            let interval_cv = ColumnarValue::Array(Arc::new(interval_builder.finish()));
            datum::apply(start, &interval_cv, op)
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported data types {:?} for date arithmetic.",
            args,
        ))),
    }
}
pub fn spark_date_add(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_date_arithmetic(args, add)
}

pub fn spark_date_sub(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_date_arithmetic(args, sub)
}
