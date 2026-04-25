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
use arrow::array::{Array, ArrowNativeTypeOp, Float32Array, Float64Array};
use arrow::array::{Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use bigdecimal::{BigDecimal, RoundingMode};
use datafusion::common::{exec_err, internal_err, DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::{cmp::min, sync::Arc};

macro_rules! integer_round {
    ($X:expr, $DIV:expr, $HALF:expr, $FAIL_ON_ERROR:expr) => {{
        let rem = $X % $DIV;
        if rem <= -$HALF {
            if $FAIL_ON_ERROR {
                ($X - rem).sub_checked($DIV).map_err(|_| {
                    ArrowError::ComputeError(arithmetic_overflow_error("integer").to_string())
                })
            } else {
                Ok(($X - rem).sub_wrapping($DIV))
            }
        } else if rem >= $HALF {
            if $FAIL_ON_ERROR {
                ($X - rem).add_checked($DIV).map_err(|_| {
                    ArrowError::ComputeError(arithmetic_overflow_error("integer").to_string())
                })
            } else {
                Ok(($X - rem).add_wrapping($DIV))
            }
        } else {
            if $FAIL_ON_ERROR {
                $X.sub_checked(rem).map_err(|_| {
                    ArrowError::ComputeError(arithmetic_overflow_error("integer").to_string())
                })
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
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let result: Float64Array = arrow::compute::kernels::arity::unary(array, |v| {
                    spark_round_via_bigdecimal_f64(v, *point)
                });
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let result: Float32Array = arrow::compute::kernels::arity::unary(array, |v| {
                    spark_round_via_bigdecimal_f32(v, *point)
                });
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
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
            ScalarValue::Float64(Some(v)) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
                spark_round_via_bigdecimal_f64(*v, *point),
            )))),
            ScalarValue::Float64(None) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(None))),
            ScalarValue::Float32(Some(v)) => Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(
                spark_round_via_bigdecimal_f32(*v, *point),
            )))),
            ScalarValue::Float32(None) => Ok(ColumnarValue::Scalar(ScalarValue::Float32(None))),
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

/// Replicate JDK 17's `Double.toString` (Gay/dtoa algorithm) for Spark-compatible rounding.
///
/// The Gay algorithm extracts decimal digits one at a time, stopping when the remainder
/// is small enough that the output uniquely identifies the double. We implement the core
/// stopping criterion using BigDecimal arithmetic.
fn double_to_bigdecimal_like_java(v: f64) -> BigDecimal {
    let abs_v = v.abs();
    let bits = abs_v.to_bits();

    if bits == 0 {
        return BigDecimal::from(0);
    }

    // Extract significand and exponent (matching JDK's convention)
    // JDK: binExp = unbiased exponent, fractBits has hidden bit at position 52
    let t = bits & 0x000F_FFFF_FFFF_FFFF;
    let bq = ((bits >> 52) & 0x7FF) as i32;
    let (fract_bits, bin_exp) = if bq == 0 {
        // Subnormal: normalize
        let lz = t.leading_zeros() as i32 - 11;
        (
            (t << lz) & 0x000F_FFFF_FFFF_FFFF | (1u64 << 52),
            -1023 + 1 - lz,
        )
    } else {
        // Normal
        (t | (1u64 << 52), bq - 1023)
    };

    let n_fract_bits = 53 - fract_bits.trailing_zeros() as i32;
    let n_sig_bits = if bq != 0 {
        53
    } else {
        64 - (t.leading_zeros() as i32)
    };
    let n_tiny = (n_fract_bits - bin_exp - 1).max(0);

    // JDK fast path: for small exponents where the value fits in a long integer
    if (-52..=62).contains(&bin_exp) && n_tiny == 0 {
        // Value is an exact integer (no fractional bits)
        let long_val = if bin_exp >= 52 {
            fract_bits << (bin_exp - 52)
        } else {
            fract_bits >> (52 - bin_exp)
        };
        // Determine insignificant trailing digits
        let insignificant = if bin_exp > n_sig_bits {
            let p2 = (bin_exp - n_sig_bits - 1) as usize;
            if p2 > 1 && p2 < 64 {
                [
                    0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7,
                    7, 8, 8, 8, 9, 9, 9, 9, 10, 10, 10, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 14,
                    14, 14, 15, 15, 15, 15, 16, 16, 16, 17, 17, 17, 18, 18, 18, 19,
                ][p2]
            } else {
                0
            }
        } else {
            0
        };
        // Convert integer to BigDecimal, zeroing out insignificant trailing digits
        let mut long_str = long_val.to_string();
        if insignificant > 0 && insignificant < long_str.len() {
            let sig_len = long_str.len() - insignificant;
            let bytes = unsafe { long_str.as_bytes_mut() };
            for b in &mut bytes[sig_len..] {
                *b = b'0';
            }
        }
        let bd: BigDecimal = long_str.parse().unwrap();
        return if v < 0.0 { -bd } else { bd };
    }

    let dec_exp_est = estimate_dec_exp(fract_bits, bin_exp);

    let b5 = (-dec_exp_est).max(0);
    let mut b2 = b5 + n_tiny + bin_exp;
    let s5 = dec_exp_est.max(0);
    let mut s2 = s5 + n_tiny;
    let m5 = b5;
    let mut m2 = b2 - n_sig_bits;

    // Remove trailing zeros from fract_bits and adjust B2
    let tail_zeros = fract_bits.trailing_zeros() as i32;
    let fract_reduced = fract_bits >> tail_zeros;
    b2 -= n_fract_bits - 1;

    // Remove common factor of 2
    let common2 = b2.min(s2).min(m2);
    b2 -= common2;
    s2 -= common2;
    m2 -= common2;

    // For exact powers of 2, halve M
    if n_fract_bits == 1 {
        m2 -= 1;
    }

    // If M2 < 0, scale everything up
    if m2 < 0 {
        b2 -= m2;
        s2 -= m2;
        m2 = 0;
    }

    use num::bigint::BigUint;
    use num::ToPrimitive;

    let b5u = b5.max(0) as u32;
    let s5u = s5.max(0) as u32;
    let m5u = m5.max(0) as u32;
    let b2u = b2.max(0) as u32;
    let s2u = s2.max(0) as u32;
    let m2u = m2.max(0) as u32;

    // Determine whether to use FDBigInteger-style comparison (>=) or int/long-style (>)
    // for the 'high' check. The JDK uses >= for the FDBigInteger path (large values)
    // and > for the int/long path (small values).
    let n_fract_bits_b2 = n_fract_bits + b2;
    let n5_b5 = if (b5u as usize) < 25 {
        [
            0, 3, 5, 7, 10, 12, 14, 17, 19, 21, 24, 26, 28, 31, 33, 35, 38, 40, 42, 45, 47, 49, 52,
            54, 56,
        ][b5u as usize]
    } else {
        b5 * 3
    };
    let bbits = n_fract_bits_b2 + n5_b5;
    let n5_s5p1 = if ((s5u + 1) as usize) < 25 {
        [
            0, 3, 5, 7, 10, 12, 14, 17, 19, 21, 24, 26, 28, 31, 33, 35, 38, 40, 42, 45, 47, 49, 52,
            54, 56,
        ][(s5u + 1) as usize]
    } else {
        (s5 + 1) * 3
    };
    let ten_sbits = s2 + 1 + n5_s5p1;
    let use_bigint_path = bbits >= 64 || ten_sbits >= 64;

    let pow5 = |n: u32| -> BigUint { BigUint::from(5u32).pow(n) };

    // Normalize S to improve division accuracy (matching JDK's shiftBias)
    let s_base = pow5(s5u) << s2u;
    let shift_bias = if use_bigint_path {
        // getNormalizationBias: shift S so its highest bit fills the MSB of a u32 word
        let s_bits = s_base.bits() as u32;
        let word_bits = s_bits.div_ceil(32) * 32;
        (word_bits - s_bits) as u32
    } else {
        0u32
    };

    let mut b_val = (BigUint::from(fract_reduced) * pow5(b5u)) << (b2u + shift_bias);
    let s_val = &s_base << shift_bias;
    let mut m_val = pow5(m5u) << (m2u + shift_bias);
    let tens = &s_val * BigUint::from(10u32);

    let mut digits = Vec::with_capacity(20);
    let mut dec_exp = dec_exp_est;

    // First digit
    let q = (&b_val / &s_val).to_u32().unwrap_or(0);
    b_val = (&b_val % &s_val) * BigUint::from(10u32);
    m_val = &m_val * BigUint::from(10u32);

    let mut low = b_val < m_val;
    let mut high = if use_bigint_path {
        &b_val + &m_val >= tens
    } else {
        &b_val + &m_val > tens
    };

    #[cfg(test)]
    if abs_v > 1e18 {
        eprintln!(
            "DTOA: q={} low={} high={} dec_exp={} s_bits={} tens_bits={} m_bits={}",
            q,
            low,
            high,
            dec_exp,
            s_val.bits(),
            tens.bits(),
            m_val.bits()
        );
    }

    if q == 0 && !high {
        dec_exp -= 1;
    } else {
        digits.push(q as u8);
    }

    // HACK: for E-form, require more digits
    if !(-3..8).contains(&dec_exp) {
        low = false;
        high = false;
    }

    // Extract remaining digits
    let mut iter_count = 0;
    while !low && !high {
        let q = (&b_val / &s_val).to_u32().unwrap_or(0);
        b_val = (&b_val % &s_val) * BigUint::from(10u32);
        m_val = &m_val * BigUint::from(10u32);

        if m_val > BigUint::from(0u32) {
            low = b_val < m_val;
            high = if use_bigint_path {
                &b_val + &m_val >= tens
            } else {
                &b_val + &m_val > tens
            };
        } else {
            low = true;
            high = true;
        }
        digits.push(q as u8);
        iter_count += 1;
        #[cfg(test)]
        if abs_v > 1e18 {
            eprintln!(
                "  iter {}: q={} ndigits={} low={} high={} b_bits={} m_bits={}",
                iter_count,
                q,
                digits.len(),
                low,
                high,
                b_val.bits(),
                m_val.bits()
            );
        }
        if iter_count > 20 {
            break;
        } // safety
    }

    // Final rounding
    if high {
        if low {
            let b2 = &b_val << 1u32;
            let cmp = b2.cmp(&tens);
            if cmp == std::cmp::Ordering::Equal {
                // Tie: round to even
                if let Some(&last) = digits.last() {
                    if last & 1 != 0 {
                        round_up_digits(&mut digits, &mut dec_exp);
                    }
                }
            } else if cmp == std::cmp::Ordering::Greater {
                round_up_digits(&mut digits, &mut dec_exp);
            }
        } else {
            round_up_digits(&mut digits, &mut dec_exp);
        }
    }

    // Convert digits + decExp to BigDecimal
    // The value is 0.d1d2d3...dn * 10^(dec_exp+1)
    let mut sig = 0i64;
    for &d in &digits {
        sig = sig * 10 + d as i64;
    }
    let n_digits = digits.len() as i32;
    let scale = n_digits - (dec_exp + 1);
    let bd = BigDecimal::new(num::BigInt::from(sig), scale as i64);
    if v < 0.0 {
        -bd
    } else {
        bd
    }
}

fn round_up_digits(digits: &mut Vec<u8>, dec_exp: &mut i32) {
    if let Some(last) = digits.last_mut() {
        if *last < 9 {
            *last += 1;
            return;
        }
    }
    // Carry propagation
    let mut i = digits.len();
    while i > 0 {
        i -= 1;
        if digits[i] < 9 {
            digits[i] += 1;
            digits.truncate(i + 1);
            return;
        }
        digits[i] = 0;
    }
    // All 9s: e.g., 999 -> 1000
    digits.clear();
    digits.push(1);
    *dec_exp += 1;
}

fn estimate_dec_exp(fract_bits: u64, bin_exp: i32) -> i32 {
    let d2_bits = 0x3FF0_0000_0000_0000u64 | (fract_bits & 0x000F_FFFF_FFFF_FFFF);
    let d2 = f64::from_bits(d2_bits);
    // These constants are from JDK's estimateDecExp and must match exactly
    #[allow(clippy::approx_constant)]
    let d = (d2 - 1.5) * 0.289529654 + 0.176091259 + (bin_exp as f64) * 0.301029995663981;
    d.floor() as i32
}

/// Spark-compatible round for f64.
fn spark_round_via_bigdecimal_f64(v: f64, scale: i64) -> f64 {
    if !v.is_finite() {
        return v;
    }
    let bd = double_to_bigdecimal_like_java(v);
    bd.with_scale_round(scale, RoundingMode::HalfUp)
        .to_string()
        .parse::<f64>()
        .unwrap()
}

/// Spark-compatible round for f32.
fn spark_round_via_bigdecimal_f32(v: f32, scale: i64) -> f32 {
    if !v.is_finite() {
        return v;
    }
    let bd = double_to_bigdecimal_like_java(f64::from(v));
    bd.with_scale_round(scale, RoundingMode::HalfUp)
        .to_string()
        .parse::<f32>()
        .unwrap()
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

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_round_f64_spark_bigdecimal_edge_case() {
        use super::spark_round_via_bigdecimal_f64;
        // -5.81855622136895E8: Java toString = "-5.81855622136895E8" (15 sig digits).
        // At 15 sig digits, the closest representation is "-5.81855622136895e8"
        // which matches Java. The 6th fractional digit is '5' -> rounds up at scale=5.
        let v = -5.81855622136895E8_f64;
        let result = spark_round_via_bigdecimal_f64(v, 5);
        assert_eq!(result, -5.8185562213690E8_f64);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_round_f64_spark_bigdecimal_tostring_roundtrip() {
        use super::spark_round_via_bigdecimal_f64;
        // 6.1317116247283497E18: JDK 17 fast path gives integer 6131711624728349600
        // (last 2 digits insignificant, zeroed). Digit at 10^5 is '4' -> rounds DOWN.
        let v = 6.131_711_624_728_35E18_f64;
        let result = spark_round_via_bigdecimal_f64(v, -5);
        let expected: f64 = "6.1317116247282995E18".parse().unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_round_f64_large_integer_string() {
        use super::spark_round_via_bigdecimal_f64;
        // cast("-8316362075006449156" as double): exact = -8316362075006449664.
        // Rust's Display (shortest repr) gives "-8316362075006450000" = 15 sig digits.
        // Java 17's Schubfach gives "-8.3163620750064497E18" = 17 sig digits (closer to exact).
        // Both are valid, but the different digit count causes different rounding at scale=-5.
        let v: f64 = "-8316362075006449156".parse().unwrap();
        let result = spark_round_via_bigdecimal_f64(v, -5);
        // Rust shortest-repr: digit at 10^5 is '5' -> rounds UP (Spark rounds DOWN)
        let expected: f64 = "-8.3163620750064005E18".parse().unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_round_f64_half_up() {
        use super::spark_round_via_bigdecimal_f64;
        assert_eq!(spark_round_via_bigdecimal_f64(2.5, 0), 3.0);
        assert_eq!(spark_round_via_bigdecimal_f64(3.5, 0), 4.0);
        assert_eq!(spark_round_via_bigdecimal_f64(-2.5, 0), -3.0);
        assert_eq!(spark_round_via_bigdecimal_f64(-3.5, 0), -4.0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_round_f64_special_values() {
        use super::spark_round_via_bigdecimal_f64;
        assert!(spark_round_via_bigdecimal_f64(f64::NAN, 2).is_nan());
        assert_eq!(
            spark_round_via_bigdecimal_f64(f64::INFINITY, 2),
            f64::INFINITY
        );
        assert_eq!(
            spark_round_via_bigdecimal_f64(f64::NEG_INFINITY, 2),
            f64::NEG_INFINITY
        );
        assert_eq!(spark_round_via_bigdecimal_f64(0.0, 2), 0.0);
        assert_eq!(spark_round_via_bigdecimal_f64(-0.0, 2), 0.0);
        assert_eq!(spark_round_via_bigdecimal_f64(f64::MIN_POSITIVE, 2), 0.0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_round_f64_negative_scale() {
        use super::spark_round_via_bigdecimal_f64;
        assert_eq!(spark_round_via_bigdecimal_f64(123.456, -1), 120.0);
        assert_eq!(spark_round_via_bigdecimal_f64(155.0, -2), 200.0);
        assert_eq!(spark_round_via_bigdecimal_f64(-155.0, -2), -200.0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_round_f32_spark_compatible() {
        use super::spark_round_via_bigdecimal_f32;
        assert_eq!(spark_round_via_bigdecimal_f32(2.5_f32, 0), 3.0_f32);
        assert_eq!(spark_round_via_bigdecimal_f32(-2.5_f32, 0), -3.0_f32);
        assert_eq!(spark_round_via_bigdecimal_f32(0.125_f32, 2), 0.13_f32);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_round_f64_null_scalar() -> Result<()> {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(None)),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let ColumnarValue::Scalar(ScalarValue::Float64(None)) =
            spark_round(&args, &DataType::Float64, false)?
        else {
            unreachable!()
        };
        Ok(())
    }
}
