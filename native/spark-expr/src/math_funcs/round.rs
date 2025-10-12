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

use crate::arithmetic_overflow_error;
use crate::math_funcs::utils::{get_precision_scale, make_decimal_array, make_decimal_scalar};
use arrow::array::{Array, ArrowNativeTypeOp};
use arrow::array::{Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use datafusion::common::{exec_err, internal_err, DataFusionError, ScalarValue};
use datafusion::{functions::math::round::round, physical_plan::ColumnarValue};
use std::{cmp::min, sync::Arc};

macro_rules! integer_round {
    ($X:expr, $DIV:expr, $HALF:expr, $FAIL_ON_ERROR:expr) => {{
        let rem = $X % $DIV;
        if rem <= -$HALF {
            if $FAIL_ON_ERROR {
                match ($X - rem).sub_checked($DIV) {
                    Ok(v) => Ok(v),
                    Err(_e) => Err(ArrowError::ComputeError(
                        arithmetic_overflow_error("integer").to_string(),
                    )),
                }
            } else {
                Ok(($X - rem).sub_wrapping($DIV))
            }
        } else if rem >= $HALF {
            if $FAIL_ON_ERROR {
                match ($X - rem).add_checked($DIV) {
                    Ok(v) => Ok(v),
                    Err(_e) => Err(ArrowError::ComputeError(
                        arithmetic_overflow_error("integer").to_string(),
                    )),
                }
            } else {
                Ok(($X - rem).add_wrapping($DIV))
            }
        } else {
            if $FAIL_ON_ERROR {
                match $X.sub_checked(rem) {
                    Ok(v) => Ok(v),
                    Err(_e) => Err(ArrowError::ComputeError(
                        arithmetic_overflow_error("integer").to_string(),
                    )),
                }
            } else {
                Ok($X.sub_wrapping(rem))
            }
        }
    }};
}

macro_rules! round_integer_array {
    ($ARRAY:expr, $POINT:expr, $TYPE:ty, $NATIVE:ty, $FAIL_ON_ERROR:expr) => {{
        let array = $ARRAY.as_any().downcast_ref::<$TYPE>().unwrap();
        let ten: $NATIVE = 10;
        let result: $TYPE = if let Some(div) = ten.checked_pow((-(*$POINT)) as u32) {
            let half = div / 2;
            arrow::compute::kernels::arity::try_unary(array, |x| {
                integer_round!(x, div, half, $FAIL_ON_ERROR)
            })?
        } else {
            arrow::compute::kernels::arity::try_unary(array, |_| Ok(0))?
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

macro_rules! round_integer_scalar {
    ($SCALAR:expr, $POINT:expr, $TYPE:expr, $NATIVE:ty, $FAIL_ON_ERROR:expr) => {{
        let ten: $NATIVE = 10;
        if let Some(div) = ten.checked_pow((-(*$POINT)) as u32) {
            let half = div / 2;
            let scalar_opt = match $SCALAR {
                Some(x) => match integer_round!(x, div, half, $FAIL_ON_ERROR) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        return Err(DataFusionError::ArrowError(
                            Box::from(e),
                            Some(DataFusionError::get_back_trace()),
                        ))
                    }
                },
                None => None,
            };
            Ok(ColumnarValue::Scalar($TYPE(scalar_opt)))
        } else {
            Ok(ColumnarValue::Scalar($TYPE(Some(0))))
        }
    }};
}

/// `round` function that simulates Spark `round` expression
pub fn spark_round(
    args: &[ColumnarValue],
    data_type: &DataType,
    fail_on_error: bool,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    let point = &args[1];
    let ColumnarValue::Scalar(ScalarValue::Int64(Some(point))) = point else {
        return internal_err!("Invalid point argument for Round(): {:#?}", point);
    };
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 if *point < 0 => {
                round_integer_array!(array, point, Int64Array, i64, fail_on_error)
            }
            DataType::Int32 if *point < 0 => {
                round_integer_array!(array, point, Int32Array, i32, fail_on_error)
            }
            DataType::Int16 if *point < 0 => {
                round_integer_array!(array, point, Int16Array, i16, fail_on_error)
            }
            DataType::Int8 if *point < 0 => {
                round_integer_array!(array, point, Int8Array, i8, fail_on_error)
            }
            DataType::Decimal128(_, scale) if *scale >= 0 => {
                let f = decimal_round_f(scale, point);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_array(array, precision, scale, &f)
            }
            DataType::Float32 | DataType::Float64 => Ok(ColumnarValue::Array(round(&[
                Arc::clone(array),
                args[1].to_array(array.len())?,
            ])?)),
            dt => exec_err!("Not supported datatype for ROUND: {dt}"),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Int64(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int64, i64, fail_on_error)
            }
            ScalarValue::Int32(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int32, i32, fail_on_error)
            }
            ScalarValue::Int16(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int16, i16, fail_on_error)
            }
            ScalarValue::Int8(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int8, i8, fail_on_error)
            }
            ScalarValue::Decimal128(a, _, scale) if *scale >= 0 => {
                let f = decimal_round_f(scale, point);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, &f)
            }
            ScalarValue::Float32(_) | ScalarValue::Float64(_) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&round(&[a.to_array()?, args[1].to_array(1)?])?, 0)?,
            )),
            dt => exec_err!("Not supported datatype for ROUND: {dt}"),
        },
    }
}

// Spark uses BigDecimal. See RoundBase implementation in Spark. Instead, we do the same by
// 1) add the half of divisor, 2) round down by division, 3) adjust precision by multiplication
#[inline]
fn decimal_round_f(scale: &i8, point: &i64) -> Box<dyn Fn(i128) -> i128> {
    if *point < 0 {
        if let Some(div) = 10_i128.checked_pow((-(*point) as u32) + (*scale as u32)) {
            let half = div / 2;
            let mul = 10_i128.pow_wrapping((-(*point)) as u32);
            // i128 can hold 39 digits of a base 10 number, adding half will not cause overflow
            Box::new(move |x: i128| (x + x.signum() * half) / div * mul)
        } else {
            Box::new(move |_: i128| 0)
        }
    } else {
        let div = 10_i128.pow_wrapping((*scale as u32) - min(*scale as u32, *point as u32));
        let half = div / 2;
        Box::new(move |x: i128| (x + x.signum() * half) / div)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::spark_round;

    use arrow::array::{Float32Array, Float64Array};
    use arrow::datatypes::DataType;
    use datafusion::common::cast::{as_float32_array, as_float64_array};
    use datafusion::common::{Result, ScalarValue};
    use datafusion::physical_plan::ColumnarValue;

    #[test]
    #[cfg_attr(miri, ignore)] // rounding does not work when miri enabled
    fn test_round_f32_array() -> Result<()> {
        let args = vec![
            ColumnarValue::Array(Arc::new(Float32Array::from(vec![
                125.2345, 15.3455, 0.1234, 0.125, 0.785, 123.123,
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let ColumnarValue::Array(result) = spark_round(&args, &DataType::Float32, false)? else {
            unreachable!()
        };
        let floats = as_float32_array(&result)?;
        let expected = Float32Array::from(vec![125.23, 15.35, 0.12, 0.13, 0.79, 123.12]);
        assert_eq!(floats, &expected);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)] // rounding does not work when miri enabled
    fn test_round_f64_array() -> Result<()> {
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                125.2345, 15.3455, 0.1234, 0.125, 0.785, 123.123,
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let ColumnarValue::Array(result) = spark_round(&args, &DataType::Float64, false)? else {
            unreachable!()
        };
        let floats = as_float64_array(&result)?;
        let expected = Float64Array::from(vec![125.23, 15.35, 0.12, 0.13, 0.79, 123.12]);
        assert_eq!(floats, &expected);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)] // rounding does not work when miri enabled
    fn test_round_f32_scalar() -> Result<()> {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float32(Some(125.2345))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let ColumnarValue::Scalar(ScalarValue::Float32(Some(result))) =
            spark_round(&args, &DataType::Float32, false)?
        else {
            unreachable!()
        };
        assert_eq!(result, 125.23);
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)] // rounding does not work when miri enabled
    fn test_round_f64_scalar() -> Result<()> {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(125.2345))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(result))) =
            spark_round(&args, &DataType::Float64, false)?
        else {
            unreachable!()
        };
        assert_eq!(result, 125.23);
        Ok(())
    }
}
