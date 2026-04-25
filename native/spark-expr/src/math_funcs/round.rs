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


/// Replicate Java's `Double.toString` via the Schubfach algorithm.
fn double_to_bigdecimal_like_java(v: f64) -> BigDecimal {
    let abs_v = v.abs();
    let bits = abs_v.to_bits();

    if bits == 0 {
        return BigDecimal::from(0);
    }

    let (f, e) = schubfach_to_decimal(bits);
    let bd = BigDecimal::new(num::BigInt::from(f as i64), -(e as i64));
    if v < 0.0 { -bd } else { bd }
}

fn flog10pow2(q: i32) -> i32 {
    ((q as i64 * 661_971_961_083i64) >> 41) as i32
}

fn flog10_three_quarters_pow2(q: i32) -> i32 {
    ((q as i64 * 661_971_961_083i64 + (-274_743_187_321i64)) >> 41) as i32
}

fn flog2pow10(e: i32) -> i32 {
    ((e as i64 * 913_124_641_741i64) >> 38) as i32
}

/// Compute the Schubfach g values for index `e`.
/// g = floor(10^e * 2^(-r)) + 1 where r = flog2pow10(e) - 125.
/// g1 = g >> 63, g0 = g & (2^63 - 1).
fn compute_g(e: i32) -> (u64, u64) {
    use num::bigint::BigUint;
    use num::ToPrimitive;

    let r = flog2pow10(e) - 125;
    // beta = 10^e * 2^(-r)
    // For e >= 0: 10^e = 2^e * 5^e, so beta = 2^e * 5^e / 2^r = 5^e * 2^(e - r)
    // For e < 0: 10^e = 1/(2^|e| * 5^|e|), so beta = 2^(-r) / (2^|e| * 5^|e|)
    //            = 2^(-r - |e|) / 5^|e| = 2^(-r + e) / 5^|e|
    // In both cases: beta = 2^(e - r) * 5^e  (where 5^e for negative e means 1/5^|e|)
    // g = floor(beta) + 1

    let shift = e - r; // e - r = 125 - flog2pow10(e) + e. This is always ~125.
    let g: BigUint = if e >= 0 {
        let pow5 = BigUint::from(5u32).pow(e as u32);
        if shift >= 0 {
            (pow5 << (shift as u32)) + BigUint::from(1u32)
        } else {
            (pow5 >> ((-shift) as u32)) + BigUint::from(1u32)
        }
    } else {
        let pow5 = BigUint::from(5u32).pow((-e) as u32);
        // 2^shift / 5^|e| + 1
        (BigUint::from(1u32) << (shift as u32)) / pow5 + BigUint::from(1u32)
    };

    let mask63 = (BigUint::from(1u128) << 63u32) - BigUint::from(1u32);
    let g1 = (&g >> 63u32).to_u64().unwrap();
    let g0 = (&g & mask63).to_u64().unwrap();
    (g1, g0)
}

/// Compute rop(cp * g / 2^127), matching JDK's rop function.
fn rop(g1: u64, g0: u64, cp: u64) -> u64 {
    let x1 = ((g0 as u128) * (cp as u128)) >> 64;
    let y0 = g1.wrapping_mul(cp); // lower 64 bits of g1*cp
    let y1 = ((g1 as u128) * (cp as u128)) >> 64; // upper 64 bits of g1*cp
    let z = ((y0 >> 1) as u128) + x1;
    let vbp = y1 + (z >> 63);
    let round = ((z & 0x7FFF_FFFF_FFFF_FFFF) + 0x7FFF_FFFF_FFFF_FFFF) >> 63;
    (vbp | round) as u64
}

/// Compute floor(a * b / 2^64) for unsigned 64-bit values.
fn mul_high(a: u64, b: u64) -> u64 {
    ((a as u128 * b as u128) >> 64) as u64
}

/// Core Schubfach algorithm using BigUint for exact boundary computation.
/// Ported from JDK 17's DoubleToDecimal.toDecimal.
fn schubfach_to_decimal(bits: u64) -> (u64, i32) {

    let t = bits & 0x000F_FFFF_FFFF_FFFF;
    let bq = ((bits >> 52) & 0x7FF) as i32;

    let (c, q) = if bq == 0 {
        (t, -1074i32)
    } else {
        ((1u64 << 52) | t, bq - 1075)
    };

    let out = (c & 1) as u64;
    let cb = c << 2;
    let cbr = cb + 2;
    let cbl;
    let k;

    if c != (1u64 << 52) || q == -1074 {
        cbl = cb - 2;
        k = flog10pow2(q);
    } else {
        cbl = cb - 1;
        k = flog10_three_quarters_pow2(q);
    }
    let h = q + flog2pow10(-k) + 2;

    // Compute vbl, vb, vbr using the JDK's rop function.
    let (g1, g0) = compute_g(-k);
    let vbl = rop(g1, g0, cbl << h);
    let vb = rop(g1, g0, cb << h);
    let vbr = rop(g1, g0, cbr << h);

    let s = vb >> 2;
    if s >= 100 {
        // Try to remove one digit: sp10 = floor(s/10) * 10
        let sp10 = s / 10 * 10;
        let tp10 = sp10 + 10;
        let upin = vbl + out <= sp10 << 2;
        let wpin = (tp10 << 2) + out <= vbr;
        if upin != wpin {
            let f = if upin { sp10 } else { tp10 };
            return (f, k);
        }
    }

    // Cannot remove a digit (or s < 100). Determine s or s+1.
    let t_val = s + 1;
    let uin = vbl + out <= s << 2;
    let win = (t_val << 2) + out <= vbr;
    if uin != win {
        let f = if uin { s } else { t_val };
        return (f, k);
    }
    // Both in range: pick closest to v.
    let cmp = (vb as i64) - ((s + t_val) << 1) as i64;
    let f = if cmp > 0 || (cmp == 0 && s & 1 != 0) { t_val } else { s };
    (f, k)
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
        // 6.1317116247283497E18 exact binary is 6131711624728349696.
        // JDK 12+ Double.toString produces "6.13171162472835e18"
        // → BigDecimal = 6131711624728350000 → at scale=-5, the 5th digit
        //   from right is '5' → HALF_UP rounds up → 6131711624728400000.
        let v = 6.131_711_624_728_35E18_f64;
        let result = spark_round_via_bigdecimal_f64(v, -5);
        assert_eq!(result, 6.1317116247284E18_f64);
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
    fn test_compute_g() {
        let (g1, g0) = super::compute_g(16);
        assert_eq!(g1, 0x470D_E4DF_8200_0000u64, "g1 mismatch for e=16");
        assert_eq!(g0, 1u64, "g0 mismatch for e=16");

        let (g1, g0) = super::compute_g(0);
        assert_eq!(g1, 0x4000_0000_0000_0000u64, "g1 mismatch for e=0");
        assert_eq!(g0, 1u64, "g0 mismatch for e=0");

        let (g1, g0) = super::compute_g(3);
        assert_eq!(g1, 0x7D00_0000_0000_0000u64, "g1 mismatch for e=3");
        assert_eq!(g0, 1u64, "g0 mismatch for e=3");

        let (g1, g0) = super::compute_g(-7);
        assert_eq!(g1, 0x6B5F_CA6A_F2BD_215Eu64, "g1 mismatch for e=-7");
        assert_eq!(g0, 0x0F4C_A41D_811A_46D4u64, "g0 mismatch for e=-7");

        let (g1, g0) = super::compute_g(-16);
        assert_eq!(g1, 0x734A_CA5F_6226_F0ADu64, "g1 mismatch for e=-16");
        assert_eq!(g0, 0x530B_AF9A_1E62_6A6Du64, "g0 mismatch for e=-16");
    }

    #[test]
    fn test_rop() {
        let (g1, g0) = super::compute_g(-3);

        let vbl = super::rop(g1, g0, 129943157421975768);
        let vb = super::rop(g1, g0, 129943157421975776);
        let vbr = super::rop(g1, g0, 129943157421975784);
        eprintln!("g(-3): g1=0x{:016X} g0=0x{:016X}", g1, g0);
        eprintln!("vbl={} vb={} vbr={}", vbl, vb, vbr);

        let s = vb >> 2;
        eprintln!("s={}", s);
        let sp10 = s / 10 * 10;
        let tp10 = sp10 + 10;
        let out = 0u64;
        let upin = vbl + out <= sp10 << 2;
        let wpin = (tp10 << 2) + out <= vbr;
        eprintln!("sp10={} tp10={} upin={} wpin={}", sp10, tp10, upin, wpin);

        // Check s vs s+1
        let t_val = s + 1;
        let uin = vbl + out <= s << 2;
        let win = (t_val << 2) + out <= vbr;
        eprintln!("uin={} win={}", uin, win);
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
