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

/// Spark-compatible power: `pow(base, exponent)`.
///
/// Matches Spark's `Pow` expression, which delegates to Java's `Math.pow`. Rust's `f64::powf`
/// follows the same IEEE 754 semantics, so no special-casing is required: `pow(0, -1)` returns
/// `Infinity` rather than erroring (which is where DataFusion's `power` diverges from Spark).
/// Only null inputs produce null; otherwise every result is the direct `powf` value.
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
                    (Some(base), Some(exp)) => Some(base.powf(exp)),
                    _ => None,
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Scalar(base_scalar), ColumnarValue::Array(exp_arr)) => {
            let exps = as_f64_array(exp_arr)?;
            let result: Float64Array = match as_f64_scalar(base_scalar)? {
                Some(base) => exps.iter().map(|e| e.map(|exp| base.powf(exp))).collect(),
                None => Float64Array::new_null(exp_arr.len()),
            };
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Array(base_arr), ColumnarValue::Scalar(exp_scalar)) => {
            let bases = as_f64_array(base_arr)?;
            let result: Float64Array = match as_f64_scalar(exp_scalar)? {
                Some(exp) => bases.iter().map(|b| b.map(|base| base.powf(exp))).collect(),
                None => Float64Array::new_null(base_arr.len()),
            };
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Scalar(base_scalar), ColumnarValue::Scalar(exp_scalar)) => {
            let result = match (as_f64_scalar(base_scalar)?, as_f64_scalar(exp_scalar)?) {
                (Some(base), Some(exp)) => ScalarValue::Float64(Some(base.powf(exp))),
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

    #[test]
    fn test_spark_pow_zero_negative_exp_is_infinity() {
        // Spark/Java Math.pow(0, -1) == +Infinity (DataFusion's power errors here instead).
        let bases = Float64Array::from(vec![0.0]);
        let exps = Float64Array::from(vec![-1.0]);
        let result = spark_pow(&[
            ColumnarValue::Array(Arc::new(bases)),
            ColumnarValue::Array(Arc::new(exps)),
        ])
        .unwrap();
        if let ColumnarValue::Array(arr) = result {
            let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(arr.value(0), f64::INFINITY);
        } else {
            panic!("expected array result");
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
