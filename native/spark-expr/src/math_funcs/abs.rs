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
use arrow::array::*;
use arrow::datatypes::*;
use arrow::error::ArrowError;
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

macro_rules! legacy_compute_op {
    ($ARRAY:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $RESULT = arrow::compute::kernels::arity::unary(array, |x| x.$FUNC());
                Ok(res)
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for abs"
            ))),
        }
    }};
}

macro_rules! ansi_compute_op {
    ($ARRAY:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident, $NATIVE:ident, $FROM_TYPE:expr) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                match arrow::compute::kernels::arity::try_unary(array, |x| {
                    if x == $NATIVE::MIN {
                        Err(ArrowError::ArithmeticOverflow($FROM_TYPE.to_string()))
                    } else {
                        Ok(x.$FUNC())
                    }
                }) {
                    Ok(res) => Ok(ColumnarValue::Array(Arc::<PrimitiveArray<$RESULT>>::new(
                        res,
                    ))),
                    Err(_) => Err(arithmetic_overflow_error($FROM_TYPE).into()),
                }
            }
            _ => Err(DataFusionError::Internal("Invalid data type".to_string())),
        }
    }};
}

/// This function mimics SparkSQL's [Abs]: https://github.com/apache/spark/blob/v4.0.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L148
/// Spark's [ANSI-compliant]: https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html#arithmetic-operations dialect mode throws org.apache.spark.SparkArithmeticException
/// when abs causes overflow.
pub fn abs(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.is_empty() || args.len() > 2 {
        return exec_err!("abs takes 1 or 2 arguments, but got: {}", args.len());
    }

    let fail_on_error = if args.len() == 2 {
        match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error))) => *fail_on_error,
            _ => {
                return exec_err!(
                    "The second argument must be boolean scalar, but got: {:?}",
                    args[1]
                );
            }
        }
    } else {
        false
    };

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Null
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(args[0].clone()),
            DataType::Int8 => {
                if !fail_on_error {
                    let result = legacy_compute_op!(array, wrapping_abs, Int8Array, Int8Array);
                    Ok(ColumnarValue::Array(Arc::new(result?)))
                } else {
                    ansi_compute_op!(array, abs, Int8Array, Int8Type, i8, "Int8")
                }
            }
            DataType::Int16 => {
                if !fail_on_error {
                    let result = legacy_compute_op!(array, wrapping_abs, Int16Array, Int16Array);
                    Ok(ColumnarValue::Array(Arc::new(result?)))
                } else {
                    ansi_compute_op!(array, abs, Int16Array, Int16Type, i16, "Int16")
                }
            }
            DataType::Int32 => {
                if !fail_on_error {
                    let result = legacy_compute_op!(array, wrapping_abs, Int32Array, Int32Array);
                    Ok(ColumnarValue::Array(Arc::new(result?)))
                } else {
                    ansi_compute_op!(array, abs, Int32Array, Int32Type, i32, "Int32")
                }
            }
            DataType::Int64 => {
                if !fail_on_error {
                    let result = legacy_compute_op!(array, wrapping_abs, Int64Array, Int64Array);
                    Ok(ColumnarValue::Array(Arc::new(result?)))
                } else {
                    ansi_compute_op!(array, abs, Int64Array, Int64Type, i64, "Int64")
                }
            }
            DataType::Float32 => {
                let result = legacy_compute_op!(array, abs, Float32Array, Float32Array);
                Ok(ColumnarValue::Array(Arc::new(result?)))
            }
            DataType::Float64 => {
                let result = legacy_compute_op!(array, abs, Float64Array, Float64Array);
                Ok(ColumnarValue::Array(Arc::new(result?)))
            }
            DataType::Decimal128(precision, scale) => {
                if !fail_on_error {
                    let result =
                        legacy_compute_op!(array, wrapping_abs, Decimal128Array, Decimal128Array)?;
                    let result = result.with_data_type(DataType::Decimal128(*precision, *scale));
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    // Need to pass precision and scale from input, so not using ansi_compute_op
                    let input = array.as_any().downcast_ref::<Decimal128Array>();
                    match input {
                        Some(i) => {
                            match arrow::compute::kernels::arity::try_unary(i, |x| {
                                if x == i128::MIN {
                                    Err(ArrowError::ArithmeticOverflow("Decimal128".to_string()))
                                } else {
                                    Ok(x.abs())
                                }
                            }) {
                                Ok(res) => Ok(ColumnarValue::Array(Arc::<
                                    PrimitiveArray<Decimal128Type>,
                                >::new(
                                    res.with_data_type(DataType::Decimal128(*precision, *scale)),
                                ))),
                                Err(_) => Err(arithmetic_overflow_error("Decimal128").into()),
                            }
                        }
                        _ => Err(DataFusionError::Internal("Invalid data type".to_string())),
                    }
                }
            }
            DataType::Decimal256(precision, scale) => {
                if !fail_on_error {
                    let result =
                        legacy_compute_op!(array, wrapping_abs, Decimal256Array, Decimal256Array)?;
                    let result = result.with_data_type(DataType::Decimal256(*precision, *scale));
                    Ok(ColumnarValue::Array(Arc::new(result)))
                } else {
                    // Need to pass precision and scale from input, so not using ansi_compute_op
                    let input = array.as_any().downcast_ref::<Decimal256Array>();
                    match input {
                        Some(i) => {
                            match arrow::compute::kernels::arity::try_unary(i, |x| {
                                if x == i256::MIN {
                                    Err(ArrowError::ArithmeticOverflow("Decimal256".to_string()))
                                } else {
                                    Ok(x.wrapping_abs()) // i256 doesn't define abs() method
                                }
                            }) {
                                Ok(res) => Ok(ColumnarValue::Array(Arc::<
                                    PrimitiveArray<Decimal256Type>,
                                >::new(
                                    res.with_data_type(DataType::Decimal256(*precision, *scale)),
                                ))),
                                Err(_) => Err(arithmetic_overflow_error("Decimal256").into()),
                            }
                        }
                        _ => Err(DataFusionError::Internal("Invalid data type".to_string())),
                    }
                }
            }
            dt => exec_err!("Not supported datatype for ABS: {dt}"),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Null
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_) => Ok(args[0].clone()),
            ScalarValue::Int8(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(abs_val)))),
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(*v))))
                        } else {
                            Err(arithmetic_overflow_error("Int8").into())
                        }
                    }
                },
            },
            ScalarValue::Int16(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(abs_val)))),
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(*v))))
                        } else {
                            Err(arithmetic_overflow_error("Int16").into())
                        }
                    }
                },
            },
            ScalarValue::Int32(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(abs_val)))),
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(*v))))
                        } else {
                            Err(arithmetic_overflow_error("Int32").into())
                        }
                    }
                },
            },
            ScalarValue::Int64(a) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(abs_val)))),
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(*v))))
                        } else {
                            Err(arithmetic_overflow_error("Int64").into())
                        }
                    }
                },
            },
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Float32(
                a.map(|x| x.abs()),
            ))),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                a.map(|x| x.abs()),
            ))),
            ScalarValue::Decimal128(a, precision, scale) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        Some(abs_val),
                        *precision,
                        *scale,
                    ))),
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                Some(*v),
                                *precision,
                                *scale,
                            )))
                        } else {
                            Err(arithmetic_overflow_error("Decimal128").into())
                        }
                    }
                },
            },
            ScalarValue::Decimal256(a, precision, scale) => match a {
                None => Ok(args[0].clone()),
                Some(v) => match v.checked_abs() {
                    Some(abs_val) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                        Some(abs_val),
                        *precision,
                        *scale,
                    ))),
                    None => {
                        if !fail_on_error {
                            // return the original value
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                                Some(*v),
                                *precision,
                                *scale,
                            )))
                        } else {
                            Err(arithmetic_overflow_error("Decimal256").into())
                        }
                    }
                },
            },
            dt => exec_err!("Not supported datatype for ABS: {dt}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::cast::{
        as_decimal128_array, as_decimal256_array, as_float32_array, as_float64_array,
        as_int16_array, as_int32_array, as_int64_array, as_int8_array, as_uint64_array,
    };

    fn with_fail_on_error<F: Fn(bool) -> Result<()>>(test_fn: F) {
        for fail_on_error in [true, false] {
            test_fn(fail_on_error).expect("test should pass on error successfully");
        }
    }

    // Unsigned types, return as is
    #[test]
    fn test_abs_u8_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::UInt8(Some(u8::MAX)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::UInt8(Some(result)))) => {
                    assert_eq!(result, u8::MAX);
                    Ok(())
                }
                Err(e) => {
                    unreachable!("Didn't expect error, but got: {e:?}")
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i8_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::Int8(Some(i8::MIN)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(result)))) => {
                    assert_eq!(result, i8::MIN);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        unreachable!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i16_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::Int16(Some(i16::MIN)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(result)))) => {
                    assert_eq!(result, i16::MIN);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        unreachable!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i32_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::Int32(Some(i32::MIN)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(result)))) => {
                    assert_eq!(result, i32::MIN);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i64_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MIN)));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(result)))) => {
                    assert_eq!(result, i64::MIN);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_decimal128_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::Decimal128(Some(i128::MIN), 18, 10));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(result),
                    precision,
                    scale,
                ))) => {
                    assert_eq!(result, i128::MIN);
                    assert_eq!(precision, 18);
                    assert_eq!(scale, 10);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_decimal256_scalar() {
        with_fail_on_error(|fail_on_error| {
            let args = ColumnarValue::Scalar(ScalarValue::Decimal256(Some(i256::MIN), 10, 2));
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));
            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                    Some(result),
                    precision,
                    scale,
                ))) => {
                    assert_eq!(result, i256::MIN);
                    assert_eq!(precision, 10);
                    assert_eq!(scale, 2);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i8_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Int8Array::from(vec![Some(-1), Some(i8::MIN), Some(i8::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Int8Array::from(vec![Some(1), Some(i8::MIN), Some(i8::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_int8_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i16_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Int16Array::from(vec![Some(-1), Some(i16::MIN), Some(i16::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Int16Array::from(vec![Some(1), Some(i16::MIN), Some(i16::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_int16_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i32_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Int32Array::from(vec![Some(-1), Some(i32::MIN), Some(i32::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Int32Array::from(vec![Some(1), Some(i32::MIN), Some(i32::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_int32_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_i64_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Int64Array::from(vec![Some(-1), Some(i64::MIN), Some(i64::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Int64Array::from(vec![Some(1), Some(i64::MIN), Some(i64::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_int64_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_f32_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Float32Array::from(vec![
                Some(-1f32),
                Some(f32::MIN),
                Some(f32::MAX),
                None,
                Some(f32::NAN),
                Some(f32::NEG_INFINITY),
                Some(f32::INFINITY),
                Some(-0.0),
                Some(0.0),
            ]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Float32Array::from(vec![
                Some(1f32),
                Some(f32::MAX),
                Some(f32::MAX),
                None,
                Some(f32::NAN),
                Some(f32::INFINITY),
                Some(f32::INFINITY),
                Some(0.0),
                Some(0.0),
            ]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_float32_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_f64_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Float64Array::from(vec![Some(-1f64), Some(f64::MIN), Some(f64::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected =
                Float64Array::from(vec![Some(1f64), Some(f64::MAX), Some(f64::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_float64_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_decimal128_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Decimal128Array::from(vec![Some(i128::MIN), None])
                .with_precision_and_scale(38, 37)?;
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Decimal128Array::from(vec![Some(i128::MIN), None])
                .with_precision_and_scale(38, 37)?;
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_decimal128_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_decimal256_array() {
        with_fail_on_error(|fail_on_error| {
            let input = Decimal256Array::from(vec![Some(i256::MIN), None])
                .with_precision_and_scale(5, 2)?;
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = Decimal256Array::from(vec![Some(i256::MIN), None])
                .with_precision_and_scale(5, 2)?;
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_decimal256_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_u64_array() {
        with_fail_on_error(|fail_on_error| {
            let input = UInt64Array::from(vec![Some(u64::MIN), Some(u64::MAX), None]);
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = UInt64Array::from(vec![Some(u64::MIN), Some(u64::MAX), None]);
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            match abs(&[args, fail_on_error_arg]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = as_uint64_array(&result)?;
                    assert_eq!(actual, &expected);
                    Ok(())
                }
                Err(e) => {
                    if fail_on_error {
                        assert!(
                            e.to_string().contains("ARITHMETIC_OVERFLOW"),
                            "Error message did not match. Actual message: {e}"
                        );
                        Ok(())
                    } else {
                        panic!("Didn't expect error, but got: {e:?}")
                    }
                }
                _ => unreachable!(),
            }
        });
    }

    #[test]
    fn test_abs_null_scalars() {
        // Test that NULL scalars return NULL (no panic) for all signed types
        with_fail_on_error(|fail_on_error| {
            let fail_on_error_arg =
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error)));

            // Test Int8
            let args = ColumnarValue::Scalar(ScalarValue::Int8(None));
            match abs(&[args.clone(), fail_on_error_arg.clone()]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int8(None))) => {}
                _ => panic!("Expected NULL Int8, got different result"),
            }

            // Test Int16
            let args = ColumnarValue::Scalar(ScalarValue::Int16(None));
            match abs(&[args.clone(), fail_on_error_arg.clone()]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int16(None))) => {}
                _ => panic!("Expected NULL Int16, got different result"),
            }

            // Test Int32
            let args = ColumnarValue::Scalar(ScalarValue::Int32(None));
            match abs(&[args.clone(), fail_on_error_arg.clone()]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(None))) => {}
                _ => panic!("Expected NULL Int32, got different result"),
            }

            // Test Int64
            let args = ColumnarValue::Scalar(ScalarValue::Int64(None));
            match abs(&[args.clone(), fail_on_error_arg.clone()]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))) => {}
                _ => panic!("Expected NULL Int64, got different result"),
            }

            // Test Decimal128
            let args = ColumnarValue::Scalar(ScalarValue::Decimal128(None, 10, 2));
            match abs(&[args.clone(), fail_on_error_arg.clone()]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(None, 10, 2))) => {}
                _ => panic!("Expected NULL Decimal128, got different result"),
            }

            // Test Decimal256
            let args = ColumnarValue::Scalar(ScalarValue::Decimal256(None, 10, 2));
            match abs(&[args.clone(), fail_on_error_arg.clone()]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(None, 10, 2))) => {}
                _ => panic!("Expected NULL Decimal256, got different result"),
            }

            // Test Float32
            let args = ColumnarValue::Scalar(ScalarValue::Float32(None));
            match abs(&[args.clone(), fail_on_error_arg.clone()]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(None))) => {}
                _ => panic!("Expected NULL Float32, got different result"),
            }

            // Test Float64
            let args = ColumnarValue::Scalar(ScalarValue::Float64(None));
            match abs(&[args.clone(), fail_on_error_arg.clone()]) {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(None))) => {}
                _ => panic!("Expected NULL Float64, got different result"),
            }

            Ok(())
        });
    }
}
