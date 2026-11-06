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

use arrow::array::cast::AsArray;
use arrow::array::types::{Decimal128Type, Float32Type, Float64Type};
use arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, Decimal128Array, Int64Array};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use num::integer::{div_ceil, div_floor};
use std::sync::Arc;

/// Evaluates `$body` with `$exp` bound as a compile-time constant equal to `$scale`, for every
/// decimal scale whose divisor `10^scale` fits in an `i64`. Larger scales evaluate `$fallback`,
/// which must derive the divisor at runtime.
///
/// Specializing on the scale lets the divisor be folded into the generated code, turning the
/// division by `10^scale` into a multiply-and-shift.
macro_rules! dispatch_pow10 {
    ($scale:expr, $exp:ident => $body:expr, $fallback:expr) => {
        match $scale {
            1 => {
                const $exp: u32 = 1;
                $body
            }
            2 => {
                const $exp: u32 = 2;
                $body
            }
            3 => {
                const $exp: u32 = 3;
                $body
            }
            4 => {
                const $exp: u32 = 4;
                $body
            }
            5 => {
                const $exp: u32 = 5;
                $body
            }
            6 => {
                const $exp: u32 = 6;
                $body
            }
            7 => {
                const $exp: u32 = 7;
                $body
            }
            8 => {
                const $exp: u32 = 8;
                $body
            }
            9 => {
                const $exp: u32 = 9;
                $body
            }
            10 => {
                const $exp: u32 = 10;
                $body
            }
            11 => {
                const $exp: u32 = 11;
                $body
            }
            12 => {
                const $exp: u32 = 12;
                $body
            }
            13 => {
                const $exp: u32 = 13;
                $body
            }
            14 => {
                const $exp: u32 = 14;
                $body
            }
            15 => {
                const $exp: u32 = 15;
                $body
            }
            16 => {
                const $exp: u32 = 16;
                $body
            }
            17 => {
                const $exp: u32 = 17;
                $body
            }
            18 => {
                const $exp: u32 = 18;
                $body
            }
            _ => $fallback,
        }
    };
}

#[inline]
pub(crate) fn make_decimal_scalar(
    a: &Option<i128>,
    precision: u8,
    scale: i8,
    f: impl Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let result = ScalarValue::Decimal128(a.map(f), precision, scale);
    Ok(ColumnarValue::Scalar(result))
}

/// Generic over the operation so that it is monomorphized into the element loop rather than
/// called through a vtable for every value.
#[inline]
pub(crate) fn make_decimal_array(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
    f: impl Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let array = array.as_primitive::<Decimal128Type>();
    let result: Decimal128Array = arrow::compute::kernels::arity::unary(array, f);
    let result = result.with_data_type(DataType::Decimal128(precision, scale));
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[inline]
pub(crate) fn get_precision_scale(data_type: &DataType) -> (u8, i8) {
    let DataType::Decimal128(precision, scale) = data_type else {
        unreachable!()
    };
    (*precision, *scale)
}

/// The rounding direction that distinguishes `ceil` from `floor`.
///
/// Implemented by marker types so that each direction is monomorphized into the element loop
/// rather than dispatched for every value.
pub(crate) trait RoundingMode {
    const NAME: &'static str;

    fn round_f32(x: f32) -> f32;
    fn round_f64(x: f64) -> f64;
    fn div_i64(x: i64, div: i64) -> i64;
    fn div_i128(x: i128, div: i128) -> i128;
}

pub(crate) struct Ceil;

impl RoundingMode for Ceil {
    const NAME: &'static str = "ceil";

    #[inline]
    fn round_f32(x: f32) -> f32 {
        x.ceil()
    }

    #[inline]
    fn round_f64(x: f64) -> f64 {
        x.ceil()
    }

    #[inline]
    fn div_i64(x: i64, div: i64) -> i64 {
        div_ceil(x, div)
    }

    #[inline]
    fn div_i128(x: i128, div: i128) -> i128 {
        div_ceil(x, div)
    }
}

pub(crate) struct Floor;

impl RoundingMode for Floor {
    const NAME: &'static str = "floor";

    #[inline]
    fn round_f32(x: f32) -> f32 {
        x.floor()
    }

    #[inline]
    fn round_f64(x: f64) -> f64 {
        x.floor()
    }

    #[inline]
    fn div_i64(x: i64, div: i64) -> i64 {
        div_floor(x, div)
    }

    #[inline]
    fn div_i128(x: i128, div: i128) -> i128 {
        div_floor(x, div)
    }
}

/// Shared implementation of Spark's `ceil` and `floor` expressions.
pub(crate) fn spark_ceil_or_floor<M: RoundingMode>(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let result: Int64Array = arrow::compute::kernels::arity::unary(
                    array.as_primitive::<Float32Type>(),
                    |x| M::round_f32(x) as i64,
                );
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Float64 => {
                let result: Int64Array = arrow::compute::kernels::arity::unary(
                    array.as_primitive::<Float64Type>(),
                    |x| M::round_f64(x) as i64,
                );
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int64 => {
                let result = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(ColumnarValue::Array(Arc::new(result.clone())))
            }
            DataType::Decimal128(_, input_scale) if *input_scale > 0 => {
                let (precision, scale) = get_precision_scale(data_type);
                dispatch_pow10!(
                    *input_scale,
                    EXP => make_decimal_array(array, precision, scale, decimal_round_pow10::<M, EXP>),
                    make_decimal_array(array, precision, scale, decimal_round_f::<M>(*input_scale))
                )
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {other:?} for function {}",
                M::NAME,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| M::round_f32(x) as i64),
            ))),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| M::round_f64(x) as i64),
            ))),
            ScalarValue::Int64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(a.map(|x| x)))),
            ScalarValue::Decimal128(a, _, input_scale) if *input_scale > 0 => {
                let f = decimal_round_f::<M>(*input_scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, f)
            }
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                value.data_type(),
                M::NAME,
            ))),
        },
    }
}

#[inline]
fn decimal_round_f<M: RoundingMode>(scale: i8) -> impl Fn(i128) -> i128 {
    let div = 10_i128.pow_wrapping(scale as u32);
    move |x: i128| M::div_i128(x, div)
}

/// Rounds an unscaled decimal divided by `10^EXP` in the direction of `M`.
///
/// `EXP` is a compile-time constant so that the divisor is folded in and the division lowered to a
/// multiply-and-shift. A 128-bit division is always a libcall, even by a constant, so values that
/// fit in 64 bits take a 64-bit path; unscaled decimals rarely exceed that range.
#[inline]
fn decimal_round_pow10<M: RoundingMode, const EXP: u32>(x: i128) -> i128 {
    match i64::try_from(x) {
        Ok(x) => M::div_i64(x, const { 10_i64.pow(EXP) }) as i128,
        Err(_) => decimal_round_wide::<M>(x, const { 10_i128.pow(EXP) }),
    }
}

/// Kept out of line so that the libcall and its stack frame stay out of the loop body of every
/// [`decimal_round_pow10`] instantiation.
#[cold]
#[inline(never)]
fn decimal_round_wide<M: RoundingMode>(x: i128, div: i128) -> i128 {
    M::div_i128(x, div)
}
