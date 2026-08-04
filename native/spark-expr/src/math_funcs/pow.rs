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

use arrow::array::Float64Array;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark-compatible scalar power matching Java's `Math.pow`.
///
/// Rust's `f64::powf` follows C99 `pow` semantics, which agree with `Math.pow` on almost every
/// input (including `pow(0, -1) == Infinity`, the signed-zero rules, and the infinite-base rules)
/// but diverge in one place: when `|base| == 1` and the exponent is infinite or `NaN`, C99 `pow`
/// returns `1` whereas `Math.pow` returns `NaN`. Special-case that so the native result matches
/// Spark exactly. `Math.pow` still returns `1` for `pow(x, ±0)` even when the base is non-finite,
/// but that is a finite exponent and is left to `powf`.
#[inline]
fn spark_powf(base: f64, exp: f64) -> f64 {
    if base.abs() == 1.0 && !exp.is_finite() {
        return f64::NAN;
    }
    base.powf(exp)
}

/// Spark-compatible power: `pow(base, exponent)`.
///
/// Matches Spark's `Pow` expression, which delegates to Java's `Math.pow`, via [`spark_powf`].
/// Unlike DataFusion's `power`, `pow(0, -1)` returns `Infinity` rather than erroring. Only null
/// inputs produce null; otherwise every result is the `spark_powf` value.
pub fn spark_pow(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "spark_pow requires 2 arguments, got {}",
            args.len()
        )));
    }

    fn as_f64_array(
        value: &Arc<dyn arrow::array::Array>,
    ) -> Result<&Float64Array, DataFusionError> {
        value
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "spark_pow expected Float64, got {:?}",
                    value.data_type()
                ))
            })
    }

    fn as_f64_scalar(scalar: &ScalarValue) -> Result<Option<f64>, DataFusionError> {
        match scalar {
            ScalarValue::Float64(v) => Ok(*v),
            _ => Err(DataFusionError::Internal(format!(
                "spark_pow expected Float64 scalar, got {scalar:?}",
            ))),
        }
    }

    match (&args[0], &args[1]) {
        (ColumnarValue::Array(base_arr), ColumnarValue::Array(exp_arr)) => {
            let bases = as_f64_array(base_arr)?;
            let exps = as_f64_array(exp_arr)?;
            let result: Float64Array = bases
                .iter()
                .zip(exps.iter())
                .map(|(b, e)| match (b, e) {
                    (Some(base), Some(exp)) => Some(spark_powf(base, exp)),
                    _ => None,
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Scalar(base_scalar), ColumnarValue::Array(exp_arr)) => {
            let exps = as_f64_array(exp_arr)?;
            let result: Float64Array = match as_f64_scalar(base_scalar)? {
                Some(base) => exps
                    .iter()
                    .map(|e| e.map(|exp| spark_powf(base, exp)))
                    .collect(),
                None => Float64Array::new_null(exp_arr.len()),
            };
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Array(base_arr), ColumnarValue::Scalar(exp_scalar)) => {
            let bases = as_f64_array(base_arr)?;
            let result: Float64Array = match as_f64_scalar(exp_scalar)? {
                Some(exp) => bases
                    .iter()
                    .map(|b| b.map(|base| spark_powf(base, exp)))
                    .collect(),
                None => Float64Array::new_null(base_arr.len()),
            };
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Scalar(base_scalar), ColumnarValue::Scalar(exp_scalar)) => {
            let result = match (as_f64_scalar(base_scalar)?, as_f64_scalar(exp_scalar)?) {
                (Some(base), Some(exp)) => ScalarValue::Float64(Some(spark_powf(base, exp))),
                _ => ScalarValue::Float64(None),
            };
            Ok(ColumnarValue::Scalar(result))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn test_spark_pow_basic() {
        let bases = Float64Array::from(vec![2.0, 2.0, -1.0]);
        let exps = Float64Array::from(vec![3.0, -1.0, 2.0]);
        let result = spark_pow(&[
            ColumnarValue::Array(Arc::new(bases)),
            ColumnarValue::Array(Arc::new(exps)),
        ])
        .unwrap();
        if let ColumnarValue::Array(arr) = result {
            let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!((arr.value(0) - 8.0).abs() < 1e-10);
            assert!((arr.value(1) - 0.5).abs() < 1e-10);
            assert!((arr.value(2) - 1.0).abs() < 1e-10);
        } else {
            panic!("expected array result");
        }
    }

    /// Evaluate `spark_pow` over paired base/exponent columns and return the result array.
    fn eval_pairs(bases: Vec<f64>, exps: Vec<f64>) -> Float64Array {
        let result = spark_pow(&[
            ColumnarValue::Array(Arc::new(Float64Array::from(bases))),
            ColumnarValue::Array(Arc::new(Float64Array::from(exps))),
        ])
        .unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                arr.as_any().downcast_ref::<Float64Array>().unwrap().clone()
            }
            _ => panic!("expected array result"),
        }
    }

    /// Assert a value is a negative zero (distinct from +0.0, which compares equal under `==`).
    fn assert_negative_zero(v: f64) {
        assert_eq!(v, 0.0, "expected zero, got {v}");
        assert!(v.is_sign_negative(), "expected negative zero, got +0.0");
    }

    #[test]
    fn test_spark_pow_zero_negative_exp_is_infinity() {
        // Spark/Java Math.pow(0, -1) == +Infinity (DataFusion's power errors here instead).
        let arr = eval_pairs(vec![0.0], vec![-1.0]);
        assert_eq!(arr.value(0), f64::INFINITY);
    }

    #[test]
    fn test_spark_pow_abs_one_nonfinite_exp_is_nan() {
        // Java Math.pow returns NaN when |base| == 1 and the exponent is infinite or NaN, whereas
        // C99 pow / Rust powf return 1. spark_pow must match Spark and return NaN.
        let arr = eval_pairs(
            vec![1.0, -1.0, 1.0, -1.0, 1.0, -1.0],
            vec![
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
                f64::NAN,
                f64::NAN,
            ],
        );
        for i in 0..arr.len() {
            assert!(
                arr.value(i).is_nan(),
                "row {i} expected NaN, got {}",
                arr.value(i)
            );
        }
        // A finite exponent on |base| == 1 is unaffected: pow(1, 0) == 1, pow(-1, 3) == -1.
        let finite = eval_pairs(vec![1.0, -1.0], vec![0.0, 3.0]);
        assert_eq!(finite.value(0), 1.0);
        assert_eq!(finite.value(1), -1.0);
    }

    #[test]
    fn test_spark_pow_negative_zero_base() {
        // Signed-zero rules from Math.pow: odd/even and sign of exponent select sign/infinity.
        let arr = eval_pairs(vec![-0.0, -0.0, -0.0, -0.0], vec![-1.0, -2.0, 3.0, 2.0]);
        assert_eq!(arr.value(0), f64::NEG_INFINITY); // (-0)^-1
        assert_eq!(arr.value(1), f64::INFINITY); // (-0)^-2
        assert_negative_zero(arr.value(2)); // (-0)^3 == -0.0
        assert_eq!(arr.value(3), 0.0); // (-0)^2 == +0.0
        assert!(arr.value(3).is_sign_positive());
    }

    #[test]
    fn test_spark_pow_infinite_base_and_exp() {
        let arr = eval_pairs(
            vec![
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
                2.0,
                0.5,
            ],
            vec![2.0, 3.0, -1.0, f64::NEG_INFINITY, f64::NEG_INFINITY],
        );
        assert_eq!(arr.value(0), f64::INFINITY); // (-inf)^2
        assert_eq!(arr.value(1), f64::NEG_INFINITY); // (-inf)^3
        assert_negative_zero(arr.value(2)); // (-inf)^-1 == -0.0
        assert_eq!(arr.value(3), 0.0); // 2^-inf == +0.0
        assert_eq!(arr.value(4), f64::INFINITY); // 0.5^-inf == +inf
    }

    #[test]
    fn test_spark_pow_subnormal() {
        // Smallest positive subnormal (Double.MIN_VALUE). Squaring underflows to +0.0; raising a
        // finite base to a subnormal exponent rounds to 1.0. Both match Spark.
        let min_subnormal = f64::from_bits(1);
        let arr = eval_pairs(vec![min_subnormal, 2.0], vec![2.0, min_subnormal]);
        assert_eq!(arr.value(0), 0.0);
        assert_eq!(arr.value(1), 1.0);
    }

    #[test]
    fn test_spark_pow_scalar_abs_one_nonfinite_exp_is_nan() {
        // The |base| == 1 fix must apply on the scalar-base and scalar-exponent paths too.
        let scalar_base = spark_pow(&[
            ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![f64::INFINITY]))),
        ])
        .unwrap();
        if let ColumnarValue::Array(arr) = scalar_base {
            let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!(arr.value(0).is_nan());
        } else {
            panic!("expected array result");
        }

        let scalar_exp = spark_pow(&[
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![-1.0]))),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::INFINITY))),
        ])
        .unwrap();
        if let ColumnarValue::Array(arr) = scalar_exp {
            let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!(arr.value(0).is_nan());
        } else {
            panic!("expected array result");
        }

        let both_scalar = spark_pow(&[
            ColumnarValue::Scalar(ScalarValue::Float64(Some(-1.0))),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::NAN))),
        ])
        .unwrap();
        if let ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) = both_scalar {
            assert!(v.is_nan());
        } else {
            panic!("expected scalar float64 result");
        }
    }

    #[test]
    fn test_spark_pow_null_propagation() {
        let bases = Float64Array::from(vec![Some(2.0), None]);
        let exps = Float64Array::from(vec![None, Some(2.0)]);
        let result = spark_pow(&[
            ColumnarValue::Array(Arc::new(bases)),
            ColumnarValue::Array(Arc::new(exps)),
        ])
        .unwrap();
        if let ColumnarValue::Array(arr) = result {
            let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!(arr.is_null(0));
            assert!(arr.is_null(1));
        } else {
            panic!("expected array result");
        }
    }

    #[test]
    fn test_spark_pow_scalar_base() {
        let exps = Float64Array::from(vec![Some(3.0), None]);
        let result = spark_pow(&[
            ColumnarValue::Scalar(ScalarValue::Float64(Some(2.0))),
            ColumnarValue::Array(Arc::new(exps)),
        ])
        .unwrap();
        if let ColumnarValue::Array(arr) = result {
            let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!((arr.value(0) - 8.0).abs() < 1e-10);
            assert!(arr.is_null(1));
        } else {
            panic!("expected array result");
        }
    }
}
