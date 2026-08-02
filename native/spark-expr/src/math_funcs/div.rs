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

use crate::error::unwrap_arrow_external_error;
use crate::math_funcs::utils::get_precision_scale;
use crate::{divide_by_zero_error, integral_divide_overflow_error, EvalMode};
use arrow::array::{Array, Decimal128Array};
use arrow::datatypes::{DataType, DECIMAL128_MAX_PRECISION};
use arrow::error::ArrowError;
use arrow::{
    array::{ArrayRef, AsArray},
    datatypes::Decimal128Type,
};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use num::{BigInt, Signed, ToPrimitive, Zero};
use std::sync::Arc;

pub fn spark_decimal_div(
    args: &[ColumnarValue],
    data_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    spark_decimal_div_internal(args, data_type, false, eval_mode, false)
}

pub fn spark_decimal_integral_div(
    args: &[ColumnarValue],
    data_type: &DataType,
    eval_mode: EvalMode,
    check_divide_overflow: bool,
) -> Result<ColumnarValue, DataFusionError> {
    spark_decimal_div_internal(args, data_type, true, eval_mode, check_divide_overflow)
}

// Let Decimal(p3, s3) as return type i.e. Decimal(p1, s1) / Decimal(p2, s2) = Decimal(p3, s3).
// Conversely, Decimal(p1, s1) = Decimal(p2, s2) * Decimal(p3, s3). This means that, in order to
// get enough scale that matches with Spark behavior, it requires to widen s1 to s2 + s3 + 1. Since
// both s2 and s3 are 38 at max., s1 is 77 at max. DataFusion division cannot handle such scale >
// Decimal256Type::MAX_SCALE. Therefore, we need to implement this decimal division using BigInt.
/// Convert a computed quotient to the `i128` stored in the result array, throwing
/// ARITHMETIC_OVERFLOW when the integral divide overflow check applies and the quotient
/// does not fit in a LONG (see `MathExpr.check_divide_overflow` in expr.proto).
#[inline]
fn quotient_to_i128<T: ToPrimitive>(
    res: &T,
    check_divide_overflow: bool,
) -> Result<i128, ArrowError> {
    let res = res.to_i128().unwrap_or(i128::MAX);
    if check_divide_overflow && i64::try_from(res).is_err() {
        return Err(ArrowError::ExternalError(Box::new(
            integral_divide_overflow_error(),
        )));
    }
    Ok(res)
}

fn spark_decimal_div_internal(
    args: &[ColumnarValue],
    data_type: &DataType,
    is_integral_div: bool,
    eval_mode: EvalMode,
    // See `MathExpr.check_divide_overflow` in expr.proto
    check_divide_overflow: bool,
) -> Result<ColumnarValue, DataFusionError> {
    // Spark captures rather than throws overflow errors in TRY mode, and never checks
    // in legacy mode, so the overflow check only ever throws under ANSI
    let check_divide_overflow = check_divide_overflow && eval_mode == EvalMode::Ansi;
    let left = &args[0];
    let right = &args[1];
    let (p3, s3) = get_precision_scale(data_type);

    let (left, right): (ArrayRef, ArrayRef) = match (left, right) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (Arc::clone(l), Arc::clone(r)),
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            (l.to_array_of_size(r.len())?, Arc::clone(r))
        }
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            (Arc::clone(l), r.to_array_of_size(l.len())?)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => (l.to_array()?, r.to_array()?),
    };
    let left = left.as_primitive::<Decimal128Type>();
    let right = right.as_primitive::<Decimal128Type>();
    let (p1, s1) = get_precision_scale(left.data_type());
    let (p2, s2) = get_precision_scale(right.data_type());

    let l_exp = ((s2 + s3 + 1) as u32).saturating_sub(s1 as u32);
    let r_exp = (s1 as u32).saturating_sub((s2 + s3 + 1) as u32);
    let result = if p1 as u32 + l_exp > DECIMAL128_MAX_PRECISION as u32
        || p2 as u32 + r_exp > DECIMAL128_MAX_PRECISION as u32
    {
        let ten = BigInt::from(10);
        let l_mul = ten.pow(l_exp);
        let r_mul = ten.pow(r_exp);
        let five = BigInt::from(5);
        let zero = BigInt::from(0);
        arrow::compute::kernels::arity::try_binary(left, right, |l, r| {
            let l = BigInt::from(l) * &l_mul;
            let r = BigInt::from(r) * &r_mul;
            // Previously this check included `&& is_integral_div`, so regular decimal `/`
            // silently returned 0 for a zero divisor in ANSI mode instead of throwing.
            // Spark throws DIVIDE_BY_ZERO for both `/` and `div` when ANSI is enabled, so
            // the `is_integral_div` guard was wrong and has been removed.
            if eval_mode == EvalMode::Ansi && r.is_zero() {
                return Err(ArrowError::ExternalError(Box::new(divide_by_zero_error())));
            }
            // Non-ANSI: zero divisors have already been replaced with null by the
            // `nullIfWhenPrimitive` wrapper applied in the Scala serde layer, so
            // `try_binary` will never invoke this closure for a zero `r` in legacy/try mode.
            // The fallback `zero.clone()` is therefore unreachable in practice.
            let div = if r.eq(&zero) { zero.clone() } else { &l / &r };
            let res = if is_integral_div {
                div
            } else if div.is_negative() {
                div - &five
            } else {
                div + &five
            } / &ten;
            quotient_to_i128(&res, check_divide_overflow)
        })
    } else {
        let l_mul = 10_i128.pow(l_exp);
        let r_mul = 10_i128.pow(r_exp);
        arrow::compute::kernels::arity::try_binary(left, right, |l, r| {
            let l = l * l_mul;
            let r = r * r_mul;
            // Previously this check included `&& is_integral_div`, so regular decimal `/`
            // silently returned 0 for a zero divisor in ANSI mode instead of throwing.
            // Spark throws DIVIDE_BY_ZERO for both `/` and `div` when ANSI is enabled, so
            // the `is_integral_div` guard was wrong and has been removed.
            if eval_mode == EvalMode::Ansi && r == 0 {
                return Err(ArrowError::ExternalError(Box::new(divide_by_zero_error())));
            }
            // Non-ANSI: zero divisors have already been replaced with null by the
            // `nullIfWhenPrimitive` wrapper applied in the Scala serde layer, so
            // `try_binary` will never invoke this closure for a zero `r` in legacy/try mode.
            // The fallback `0` is therefore unreachable in practice.
            let div = if r == 0 { 0 } else { l / r };
            let res = if is_integral_div {
                div
            } else if div.is_negative() {
                div - 5
            } else {
                div + 5
            } / 10;
            quotient_to_i128(&res, check_divide_overflow)
        })
    };
    let result: Decimal128Array = result.map_err(unwrap_arrow_external_error)?;
    let result = result.with_data_type(DataType::Decimal128(p3, s3));
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SparkError;

    fn decimal(value: i128, precision: u8, scale: i8) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(
            Decimal128Array::from(vec![Some(value)])
                .with_data_type(DataType::Decimal128(precision, scale)),
        ))
    }

    fn spark_error(result: Result<ColumnarValue, DataFusionError>) -> SparkError {
        match result.unwrap_err() {
            DataFusionError::External(error) => *error
                .downcast::<SparkError>()
                .expect("expected external SparkError"),
            error => panic!("expected external SparkError, got {error:?}"),
        }
    }

    #[test]
    fn test_decimal_divide_by_zero_returns_spark_error() {
        // Exercise both the i128 and BigInt kernels.
        for (precision, scale) in [(10, 2), (38, 0)] {
            let result = spark_decimal_div(
                &[decimal(100, precision, scale), decimal(0, precision, scale)],
                &DataType::Decimal128(precision, scale),
                EvalMode::Ansi,
            );
            assert!(matches!(spark_error(result), SparkError::DivideByZero));
        }
    }

    #[test]
    fn test_integral_divide_overflow_returns_spark_error() {
        let result = quotient_to_i128(&(i64::MAX as i128 + 1), true);
        match result.unwrap_err() {
            ArrowError::ExternalError(error) => assert!(matches!(
                error.downcast_ref::<SparkError>(),
                Some(SparkError::IntegralDivideOverflow)
            )),
            error => panic!("expected external SparkError, got {error:?}"),
        }
    }
}
