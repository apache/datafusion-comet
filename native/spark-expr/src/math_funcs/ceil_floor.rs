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

use crate::math_funcs::utils::{get_precision_scale, make_decimal_array, make_decimal_scalar};
use arrow::array::cast::AsArray;
use arrow::array::types::{Float32Type, Float64Type};
use arrow::array::{Array, ArrowNativeTypeOp, Int64Array};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use num::integer::{div_ceil, div_floor, Integer};
use num::Float;
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

/// `ceil` function that simulates Spark `ceil` expression
pub fn spark_ceil(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    spark_ceil_or_floor::<true>(args, data_type)
}

/// `floor` function that simulates Spark `floor` expression
pub fn spark_floor(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    spark_ceil_or_floor::<false>(args, data_type)
}

#[inline]
fn round_float<const CEIL: bool, T: Float>(x: T) -> T {
    if CEIL {
        x.ceil()
    } else {
        x.floor()
    }
}

#[inline]
fn div_round<const CEIL: bool, T: Integer>(x: T, div: T) -> T {
    if CEIL {
        div_ceil(x, div)
    } else {
        div_floor(x, div)
    }
}

/// Shared implementation of Spark's `ceil` and `floor` expressions.
///
/// `CEIL` selects the rounding direction. It is a const parameter so that each direction is
/// monomorphized into the element loop rather than branched on for every value.
fn spark_ceil_or_floor<const CEIL: bool>(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let name = if CEIL { "ceil" } else { "floor" };
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let result: Int64Array = arrow::compute::kernels::arity::unary(
                    array.as_primitive::<Float32Type>(),
                    |x| round_float::<CEIL, _>(x) as i64,
                );
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Float64 => {
                let result: Int64Array = arrow::compute::kernels::arity::unary(
                    array.as_primitive::<Float64Type>(),
                    |x| round_float::<CEIL, _>(x) as i64,
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
                    EXP => make_decimal_array(array, precision, scale, decimal_round_pow10::<CEIL, EXP>),
                    make_decimal_array(array, precision, scale, decimal_round_f::<CEIL>(*input_scale))
                )
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {other:?} for function {}",
                name,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| round_float::<CEIL, _>(x) as i64),
            ))),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| round_float::<CEIL, _>(x) as i64),
            ))),
            ScalarValue::Int64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(a.map(|x| x)))),
            ScalarValue::Decimal128(a, _, input_scale) if *input_scale > 0 => {
                let f = decimal_round_f::<CEIL>(*input_scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, f)
            }
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                value.data_type(),
                name,
            ))),
        },
    }
}

#[inline]
fn decimal_round_f<const CEIL: bool>(scale: i8) -> impl Fn(i128) -> i128 {
    let div = 10_i128.pow_wrapping(scale as u32);
    move |x: i128| div_round::<CEIL, _>(x, div)
}

/// Rounds an unscaled decimal divided by `10^EXP` up if `CEIL` is set, down otherwise.
///
/// `EXP` is a compile-time constant so that the divisor is folded in and the division lowered to a
/// multiply-and-shift. A 128-bit division is always a libcall, even by a constant, so values that
/// fit in 64 bits take a 64-bit path; unscaled decimals rarely exceed that range.
#[inline]
fn decimal_round_pow10<const CEIL: bool, const EXP: u32>(x: i128) -> i128 {
    match i64::try_from(x) {
        Ok(x) => div_round::<CEIL, _>(x, const { 10_i64.pow(EXP) }) as i128,
        Err(_) => decimal_round_wide::<CEIL>(x, const { 10_i128.pow(EXP) }),
    }
}

/// Kept out of line so that the libcall and its stack frame stay out of the loop body of every
/// [`decimal_round_pow10`] instantiation.
#[cold]
#[inline(never)]
fn decimal_round_wide<const CEIL: bool>(x: i128, div: i128) -> i128 {
    div_round::<CEIL, _>(x, div)
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{ArrayRef, Decimal128Array, Float32Array, Float64Array};
    use datafusion::common::cast::as_int64_array;
    use datafusion::common::Result;

    type RoundFn = fn(&[ColumnarValue], &DataType) -> Result<ColumnarValue>;

    fn check_int64_array(
        round: RoundFn,
        input: ArrayRef,
        expected: Vec<Option<i64>>,
    ) -> Result<()> {
        let data_type = input.data_type().clone();
        let ColumnarValue::Array(result) = round(&[ColumnarValue::Array(input)], &data_type)?
        else {
            unreachable!()
        };
        assert_eq!(as_int64_array(&result)?, &Int64Array::from(expected));
        Ok(())
    }

    fn check_decimal_array(
        round: RoundFn,
        input: &[Option<i128>],
        (precision, scale): (u8, i8),
        output_precision: u8,
        expected: Vec<Option<i128>>,
    ) -> Result<()> {
        let array =
            Decimal128Array::from(input.to_vec()).with_precision_and_scale(precision, scale)?;
        let args = [ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) =
            round(&args, &DataType::Decimal128(output_precision, 0))?
        else {
            unreachable!()
        };
        let expected =
            Decimal128Array::from(expected).with_precision_and_scale(output_precision, 0)?;
        let actual = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(actual, &expected);
        Ok(())
    }

    fn check_scalar(
        round: RoundFn,
        input: ScalarValue,
        data_type: &DataType,
        expected: ScalarValue,
    ) -> Result<()> {
        let ColumnarValue::Scalar(actual) = round(&[ColumnarValue::Scalar(input)], data_type)?
        else {
            unreachable!()
        };
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn test_f32_array() -> Result<()> {
        let ceil_input = [125.2345, 15.0001, 0.1, -0.9, -1.1, 123.0].map(Some);
        let floor_input = [125.9345, 15.9999, 0.9, -0.1, -1.999, 123.0].map(Some);
        let with_null = |values: [Option<f32>; 6]| {
            let values: Vec<_> = values.into_iter().chain([None]).collect();
            Arc::new(Float32Array::from(values)) as ArrayRef
        };
        check_int64_array(
            spark_ceil,
            with_null(ceil_input),
            vec![
                Some(126),
                Some(16),
                Some(1),
                Some(0),
                Some(-1),
                Some(123),
                None,
            ],
        )?;
        check_int64_array(
            spark_floor,
            with_null(floor_input),
            vec![
                Some(125),
                Some(15),
                Some(0),
                Some(-1),
                Some(-2),
                Some(123),
                None,
            ],
        )
    }

    #[test]
    fn test_f64_array() -> Result<()> {
        let ceil_input = [125.2345, 15.0001, 0.1, -0.9, -1.1, 123.0].map(Some);
        let floor_input = [125.9345, 15.9999, 0.9, -0.1, -1.999, 123.0].map(Some);
        let with_null = |values: [Option<f64>; 6]| {
            let values: Vec<_> = values.into_iter().chain([None]).collect();
            Arc::new(Float64Array::from(values)) as ArrayRef
        };
        check_int64_array(
            spark_ceil,
            with_null(ceil_input),
            vec![
                Some(126),
                Some(16),
                Some(1),
                Some(0),
                Some(-1),
                Some(123),
                None,
            ],
        )?;
        check_int64_array(
            spark_floor,
            with_null(floor_input),
            vec![
                Some(125),
                Some(15),
                Some(0),
                Some(-1),
                Some(-2),
                Some(123),
                None,
            ],
        )
    }

    #[test]
    fn test_i64_array() -> Result<()> {
        for round in [spark_ceil as RoundFn, spark_floor] {
            let values = vec![Some(-1), Some(0), Some(1), None];
            let input = Arc::new(Int64Array::from(values.clone())) as ArrayRef;
            check_int64_array(round, input, values)?;
        }
        Ok(())
    }

    #[test]
    fn test_decimal128_array() -> Result<()> {
        let input = [
            Some(12345),  // 123.45
            Some(12500),  // 125.00
            Some(-12999), // -129.99
            None,
        ];
        check_decimal_array(
            spark_ceil,
            &input,
            (5, 2),
            4,
            vec![Some(124), Some(125), Some(-129), None],
        )?;
        check_decimal_array(
            spark_floor,
            &input,
            (5, 2),
            4,
            vec![Some(123), Some(125), Some(-130), None],
        )
    }

    #[test]
    fn test_decimal128_wide_array() -> Result<()> {
        // Unscaled values above i64::MAX (~9.22e18) take the wide fallback in decimal_round_pow10.
        // The first value is an exact multiple of 10^6, the other two have a remainder.
        let input = [
            Some(20_000_000_000_000_000_000_i128),
            Some(20_000_000_000_000_000_001_i128),
            Some(-20_000_000_000_000_000_001_i128),
            None,
        ];
        check_decimal_array(
            spark_ceil,
            &input,
            (38, 6),
            38,
            vec![
                Some(20_000_000_000_000_i128),
                Some(20_000_000_000_001_i128),
                Some(-20_000_000_000_000_i128),
                None,
            ],
        )?;
        check_decimal_array(
            spark_floor,
            &input,
            (38, 6),
            38,
            vec![
                Some(20_000_000_000_000_i128),
                Some(20_000_000_000_000_i128),
                Some(-20_000_000_000_001_i128),
                None,
            ],
        )
    }

    #[test]
    fn test_decimal128_i64_boundary_array() -> Result<()> {
        // decimal_round_pow10 switches from the i64 fast path to decimal_round_wide exactly at
        // i64::MAX / i64::MIN. Straddling both edges pins that the two paths agree: each pair
        // below differs by one unscaled unit but rounds to the same value, so a divergence
        // between the 64-bit and 128-bit divisions would show up as a mismatch here.
        let input = [
            Some(i64::MAX as i128),     //  9223372036854.775807 -> fast path
            Some(i64::MAX as i128 + 1), //  9223372036854.775808 -> wide path
            Some(i64::MIN as i128),     // -9223372036854.775808 -> fast path
            Some(i64::MIN as i128 - 1), // -9223372036854.775809 -> wide path
        ];
        check_decimal_array(
            spark_ceil,
            &input,
            (38, 6),
            38,
            vec![
                Some(9_223_372_036_855_i128),
                Some(9_223_372_036_855_i128),
                Some(-9_223_372_036_854_i128),
                Some(-9_223_372_036_854_i128),
            ],
        )?;
        check_decimal_array(
            spark_floor,
            &input,
            (38, 6),
            38,
            vec![
                Some(9_223_372_036_854_i128),
                Some(9_223_372_036_854_i128),
                Some(-9_223_372_036_855_i128),
                Some(-9_223_372_036_855_i128),
            ],
        )
    }

    #[test]
    fn test_decimal128_large_scale_array() -> Result<()> {
        // dispatch_pow10! only specializes scales whose divisor fits in an i64, so scale 19 takes
        // the runtime `decimal_round_f` fallback. Untested on the array path otherwise.
        let input = [
            Some(20_000_000_000_000_000_000_000_i128),  //  2000.0
            Some(20_000_000_000_000_000_000_001_i128),  //  2000.0000000000000000001
            Some(-20_000_000_000_000_000_000_001_i128), // -2000.0000000000000000001
            None,
        ];
        check_decimal_array(
            spark_ceil,
            &input,
            (38, 19),
            38,
            vec![Some(2000), Some(2001), Some(-2000), None],
        )?;
        check_decimal_array(
            spark_floor,
            &input,
            (38, 19),
            38,
            vec![Some(2000), Some(2000), Some(-2001), None],
        )
    }

    #[test]
    fn test_f32_scalar() -> Result<()> {
        check_scalar(
            spark_ceil,
            ScalarValue::Float32(Some(125.2345)),
            &DataType::Float32,
            ScalarValue::Int64(Some(126)),
        )?;
        check_scalar(
            spark_floor,
            ScalarValue::Float32(Some(125.9345)),
            &DataType::Float32,
            ScalarValue::Int64(Some(125)),
        )
    }

    #[test]
    fn test_f64_scalar() -> Result<()> {
        check_scalar(
            spark_ceil,
            ScalarValue::Float64(Some(-1.1)),
            &DataType::Float64,
            ScalarValue::Int64(Some(-1)),
        )?;
        check_scalar(
            spark_floor,
            ScalarValue::Float64(Some(-1.999)),
            &DataType::Float64,
            ScalarValue::Int64(Some(-2)),
        )
    }

    #[test]
    fn test_i64_scalar() -> Result<()> {
        for round in [spark_ceil as RoundFn, spark_floor] {
            check_scalar(
                round,
                ScalarValue::Int64(Some(48)),
                &DataType::Int64,
                ScalarValue::Int64(Some(48)),
            )?;
        }
        Ok(())
    }

    #[test]
    fn test_decimal128_scalar() -> Result<()> {
        let input = ScalarValue::Decimal128(Some(567), 3, 1); // 56.7
        let output_type = DataType::Decimal128(3, 0);
        check_scalar(
            spark_ceil,
            input.clone(),
            &output_type,
            ScalarValue::Decimal128(Some(57), 3, 0),
        )?;
        check_scalar(
            spark_floor,
            input,
            &output_type,
            ScalarValue::Decimal128(Some(56), 3, 0),
        )
    }
}
