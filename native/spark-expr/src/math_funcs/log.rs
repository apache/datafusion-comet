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

use arrow::array::{Array, Float64Array};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark-compatible two-argument logarithm: `log(base, value)`.
///
/// Returns `log(value) / log(base)`, matching Spark's `Logarithm` expression.
/// Returns null when `base <= 0` or `value <= 0`, matching Spark's `nullSafeEval`.
pub fn spark_log(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "spark_log requires 2 arguments, got {}",
            args.len()
        )));
    }

    // Spark's Logarithm: log(base, value) = ln(value) / ln(base)
    // Returns null when base <= 0 or value <= 0
    fn compute(base: f64, value: f64) -> Option<f64> {
        if base <= 0.0 || value <= 0.0 {
            None
        } else {
            Some(value.ln() / base.ln())
        }
    }

    match (&args[0], &args[1]) {
        (ColumnarValue::Array(base_arr), ColumnarValue::Array(val_arr)) => {
            let bases = base_arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "spark_log expected Float64 for base, got {:?}",
                        base_arr.data_type()
                    ))
                })?;
            let values = val_arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "spark_log expected Float64 for value, got {:?}",
                        val_arr.data_type()
                    ))
                })?;
            let result: Float64Array = bases
                .iter()
                .zip(values.iter())
                .map(|(b, v)| match (b, v) {
                    (Some(base), Some(value)) => compute(base, value),
                    _ => None,
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Scalar(base_scalar), ColumnarValue::Array(val_arr)) => {
            let base = match base_scalar {
                ScalarValue::Float64(Some(b)) => *b,
                ScalarValue::Float64(None) => {
                    let result = Float64Array::new_null(val_arr.len());
                    return Ok(ColumnarValue::Array(Arc::new(result)));
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "spark_log expected Float64 scalar for base, got {base_scalar:?}",
                    )));
                }
            };
            let values = val_arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "spark_log expected Float64 for value, got {:?}",
                        val_arr.data_type()
                    ))
                })?;
            let result: Float64Array = values
                .iter()
                .map(|v| v.and_then(|value| compute(base, value)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Array(base_arr), ColumnarValue::Scalar(val_scalar)) => {
            let value = match val_scalar {
                ScalarValue::Float64(Some(v)) => *v,
                ScalarValue::Float64(None) => {
                    let result = Float64Array::new_null(base_arr.len());
                    return Ok(ColumnarValue::Array(Arc::new(result)));
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "spark_log expected Float64 scalar for value, got {val_scalar:?}",
                    )));
                }
            };
            let bases = base_arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "spark_log expected Float64 for base, got {:?}",
                        base_arr.data_type()
                    ))
                })?;
            let result: Float64Array = bases
                .iter()
                .map(|b| b.and_then(|base| compute(base, value)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Scalar(base_scalar), ColumnarValue::Scalar(val_scalar)) => {
            let result = match (base_scalar, val_scalar) {
                (ScalarValue::Float64(Some(base)), ScalarValue::Float64(Some(value))) => {
                    ScalarValue::Float64(compute(*base, *value))
                }
                (ScalarValue::Float64(_), ScalarValue::Float64(_)) => ScalarValue::Float64(None),
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "spark_log expected Float64 scalars, got {base_scalar:?} and {val_scalar:?}",
                    )));
                }
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
    fn test_spark_log_basic() {
        let bases = Float64Array::from(vec![10.0, 2.0, 10.0]);
        let values = Float64Array::from(vec![100.0, 8.0, 1.0]);
        let result = spark_log(&[
            ColumnarValue::Array(Arc::new(bases)),
            ColumnarValue::Array(Arc::new(values)),
        ])
        .unwrap();
        if let ColumnarValue::Array(arr) = result {
            let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!((arr.value(0) - 2.0).abs() < 1e-10);
            assert!((arr.value(1) - 3.0).abs() < 1e-10);
            assert!((arr.value(2) - 0.0).abs() < 1e-10);
        } else {
            panic!("expected array result");
        }
    }

    #[test]
    fn test_spark_log_non_positive_returns_null() {
        let bases = Float64Array::from(vec![Some(0.0), Some(-1.0), Some(10.0), Some(10.0)]);
        let values = Float64Array::from(vec![Some(10.0), Some(10.0), Some(0.0), Some(-1.0)]);
        let result = spark_log(&[
            ColumnarValue::Array(Arc::new(bases)),
            ColumnarValue::Array(Arc::new(values)),
        ])
        .unwrap();
        if let ColumnarValue::Array(arr) = result {
            let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!(arr.is_null(0));
            assert!(arr.is_null(1));
            assert!(arr.is_null(2));
            assert!(arr.is_null(3));
        } else {
            panic!("expected array result");
        }
    }

    #[test]
    fn test_spark_log_null_propagation() {
        let bases = Float64Array::from(vec![Some(10.0), None]);
        let values = Float64Array::from(vec![None, Some(10.0)]);
        let result = spark_log(&[
            ColumnarValue::Array(Arc::new(bases)),
            ColumnarValue::Array(Arc::new(values)),
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
    fn test_spark_log_base_one_returns_nan() {
        // log(1, 1) = ln(1) / ln(1) = 0/0 = NaN
        let bases = Float64Array::from(vec![1.0]);
        let values = Float64Array::from(vec![1.0]);
        let result = spark_log(&[
            ColumnarValue::Array(Arc::new(bases)),
            ColumnarValue::Array(Arc::new(values)),
        ])
        .unwrap();
        if let ColumnarValue::Array(arr) = result {
            let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            assert!(arr.value(0).is_nan());
        } else {
            panic!("expected array result");
        }
    }
}
