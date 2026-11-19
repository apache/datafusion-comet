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
use num::{BigInt, Integer, Signed, ToPrimitive, Zero};
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
// get enough scale that matches with Spark behavior, it requires to widen s1 to s2 + s3. Since
// both s2 and s3 are 38 at max., s1 is 76 at max. DataFusion division cannot handle such scale >
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

    // The exponents below are computed in `u32`, so a negative scale wraps to roughly 4.29e9 and
    // the BigInt branch would evaluate `10.pow(~4.29e9)`. That is an unbounded allocation rather
    // than an overflow, so it hangs instead of failing. Comet's serde reports negative-scale
    // decimal arithmetic as unsupported, which keeps this unreachable from a Spark plan, but
    // reject it here too so any other caller gets a diagnosable error. See issue #5013.
    if s1 < 0 || s2 < 0 || s3 < 0 {
        return Err(DataFusionError::Execution(format!(
            "spark_decimal_div: negative decimal scale left={s1} right={s2} result={s3}"
        )));
    }

    // Both branches divide once at scale s3 and round from the remainder.
    let l_exp = ((s2 + s3) as u32).saturating_sub(s1 as u32);
    let r_exp = (s1 as u32).saturating_sub((s2 + s3) as u32);
    let result = if p1 as u32 + l_exp > DECIMAL128_MAX_PRECISION as u32
        || p2 as u32 + r_exp > DECIMAL128_MAX_PRECISION as u32
    {
        let ten = BigInt::from(10);
        let l_mul = ten.pow(l_exp);
        let r_mul = ten.pow(r_exp);
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
            if r.eq(&zero) {
                return quotient_to_i128(&zero, check_divide_overflow);
            }
            let (q, rem) = l.div_rem(&r);
            // Round half away from zero.
            let res = if !is_integral_div && rem.abs() * 2 >= r.abs() {
                if l.is_negative() == r.is_negative() {
                    q + 1
                } else {
                    q - 1
                }
            } else {
                q
            };
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
            if r == 0 {
                return quotient_to_i128(&0, check_divide_overflow);
            }
            let q = l / r;
            // Round half away from zero. `|q * r| <= |l|`, so the product can't overflow, and
            // `rem >= |r| - rem` is `2 * rem >= |r|` without the overflow of `2 * rem`.
            let rem = (l - q * r).unsigned_abs();
            let res = if !is_integral_div && rem >= r.unsigned_abs() - rem {
                if (l < 0) == (r < 0) {
                    q + 1
                } else {
                    q - 1
                }
            } else {
                q
            };
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
    use num::Integer;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

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
    fn test_decimal_div_rejects_negative_scale() {
        // `spark_decimal_div` computes its scale-alignment exponents in `u32`, so a negative
        // scale wraps to roughly 4.29e9, clears the precision check and sends the BigInt branch
        // into `10.pow(~4.29e9)`, an unbounded allocation that hangs rather than failing. The
        // guard turns that into an error. Comet's serde keeps a Spark plan away from this, so
        // the guard is for any other caller.
        for (left, right, result) in [
            (
                DataType::Decimal128(10, -1),
                DataType::Decimal128(10, 0),
                DataType::Decimal128(10, 0),
            ),
            (
                DataType::Decimal128(10, 0),
                DataType::Decimal128(10, -1),
                DataType::Decimal128(10, 0),
            ),
            (
                DataType::Decimal128(10, 0),
                DataType::Decimal128(10, 0),
                DataType::Decimal128(10, -1),
            ),
        ] {
            let (lp, ls) = match left {
                DataType::Decimal128(p, s) => (p, s),
                _ => unreachable!(),
            };
            let (rp, rs) = match right {
                DataType::Decimal128(p, s) => (p, s),
                _ => unreachable!(),
            };
            let err = spark_decimal_div(
                &[decimal(100, lp, ls), decimal(3, rp, rs)],
                &result,
                EvalMode::Legacy,
            )
            .expect_err("a negative scale must be rejected");
            assert!(
                err.to_string().contains("negative decimal scale"),
                "unexpected error: {err}"
            );
        }
    }

    /// The exact quotient `(l / 10^s1) / (r / 10^s2)` at scale `s3`, truncated for `div` and
    /// rounded half away from zero for `/`, saturated the way the kernel saturates. Also returns
    /// whether the exact quotient was halfway between two values at scale `s3`.
    fn reference_div(
        l: i128,
        r: i128,
        (s1, s2, s3): (i8, i8, i8),
        is_integral_div: bool,
    ) -> (i128, bool) {
        if r == 0 {
            return (0, false);
        }
        let ten = BigInt::from(10);
        let num = BigInt::from(l) * ten.pow((s2 + s3) as u32);
        let den = BigInt::from(r) * ten.pow(s1 as u32);
        let (mut q, rem) = num.div_rem(&den);
        let twice_rem = rem.abs() * 2;
        let half = twice_rem == den.abs();
        if !is_integral_div && twice_rem >= den.abs() {
            q += num.signum() * den.signum();
        }
        (q.to_i128().unwrap_or(i128::MAX), half)
    }

    /// Values at and around the bounds of precision `p`, values that make exact halves likely,
    /// and random values, each with both signs.
    fn test_values(p: u8, rng: &mut StdRng) -> Vec<i128> {
        let max = 10_i128.pow(p as u32) - 1;
        let mut v = vec![1, 2, 3, 5, 7, max, max - 1, max / 2, max / 3];
        v.push(10_i128.pow(p as u32 - 1));
        for j in [1, 5, 18, 37] {
            if j < p as u32 {
                v.push(2 * 10_i128.pow(j));
                v.push(3 * 10_i128.pow(j));
            }
        }
        v.extend((0..3).map(|_| rng.random_range(1..=max)));
        v.extend(v.clone().into_iter().map(|x| -x));
        v.push(0);
        v
    }

    #[test]
    fn test_decimal_div_matches_exact_quotient() {
        let mut rng = StdRng::seed_from_u64(42);
        let (mut halves, mut i128_branch, mut bigint_branch) = (0, 0, 0);
        let precisions: Vec<(u8, Vec<i128>)> = [1u8, 10, 19, 38]
            .into_iter()
            .map(|p| (p, test_values(p, &mut rng)))
            .collect();
        for (p1, ls) in &precisions {
            for (p2, rs) in &precisions {
                let (p1, p2) = (*p1, *p2);
                // Every pair, plus a null on each side.
                let (mut lv, mut rv): (Vec<_>, Vec<_>) = ls
                    .iter()
                    .flat_map(|&l| rs.iter().map(move |&r| (Some(l), Some(r))))
                    .unzip();
                lv.extend([None, Some(1)]);
                rv.extend([Some(1), None]);
                let (left, right) = (
                    Decimal128Array::from(lv.clone()),
                    Decimal128Array::from(rv.clone()),
                );
                for s1 in [0, 1, p1 / 2, p1 - 1, p1] {
                    for s2 in [0, 1, p2 / 2, p2 - 1, p2] {
                        for s3 in [0u8, 1, 6, 18, 38] {
                            let scales = (s1 as i8, s2 as i8, s3 as i8);
                            // The branch choice before this kernel removed its extra digit.
                            let l_exp = (s2 + s3 + 1).saturating_sub(s1);
                            let r_exp = s1.saturating_sub(s2 + s3 + 1);
                            if p1 + l_exp > 38 || p2 + r_exp > 38 {
                                bigint_branch += 1;
                            } else {
                                i128_branch += 1;
                            }
                            let left = left
                                .clone()
                                .with_data_type(DataType::Decimal128(p1, s1 as i8));
                            let right = right
                                .clone()
                                .with_data_type(DataType::Decimal128(p2, s2 as i8));
                            let args = [
                                ColumnarValue::Array(Arc::new(left)),
                                ColumnarValue::Array(Arc::new(right)),
                            ];
                            let ret = DataType::Decimal128(38, s3 as i8);
                            for is_integral_div in [false, true] {
                                let result = if is_integral_div {
                                    spark_decimal_integral_div(&args, &ret, EvalMode::Legacy, true)
                                } else {
                                    spark_decimal_div(&args, &ret, EvalMode::Legacy)
                                };
                                let ColumnarValue::Array(result) = result.unwrap() else {
                                    unreachable!()
                                };
                                let result = result.as_primitive::<Decimal128Type>();
                                assert_eq!(result.data_type(), &ret);
                                for (i, (l, r)) in lv.iter().zip(&rv).enumerate() {
                                    let (Some(l), Some(r)) = (l, r) else {
                                        assert!(result.is_null(i));
                                        continue;
                                    };
                                    let (expected, half) =
                                        reference_div(*l, *r, scales, is_integral_div);
                                    halves += half as usize;
                                    assert_eq!(
                                        result.value(i),
                                        expected,
                                        "{l} / {r}, scales {scales:?}, div {is_integral_div}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        assert!(halves > 1000, "only {halves} exact halves");
        assert!(i128_branch > 100 && bigint_branch > 100);
    }

    #[test]
    fn test_integral_divide_overflow_through_kernel() {
        // (38, 0) div (38, 0) runs the i128 branch. (38, 0) div (38, 1) runs the BigInt branch.
        for s2 in [0, 1] {
            let divisor = 10_i128.pow(s2 as u32);
            for (dividend, overflows) in [
                (i64::MAX as i128, false),
                (i64::MIN as i128, false),
                (i64::MAX as i128 + 1, true),
                (i64::MIN as i128 - 1, true),
            ] {
                let result = spark_decimal_integral_div(
                    &[decimal(dividend, 38, 0), decimal(divisor, 38, s2)],
                    &DataType::Decimal128(38, 0),
                    EvalMode::Ansi,
                    true,
                );
                if overflows {
                    assert!(matches!(
                        spark_error(result),
                        SparkError::IntegralDivideOverflow
                    ));
                } else {
                    let ColumnarValue::Array(result) = result.unwrap() else {
                        unreachable!()
                    };
                    assert_eq!(result.as_primitive::<Decimal128Type>().value(0), dividend);
                }
            }
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
