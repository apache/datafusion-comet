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

use arrow::array::{ArrayRef, AsArray, Float64Array};
use arrow::datatypes::Float64Type;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark-compatible `sqrt`, matching `java.lang.Math.sqrt`: a negative input produces
/// `NaN` rather than an error, unlike DataFusion's own `sqrt`.
pub fn spark_sqrt(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "spark_sqrt requires 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        ColumnarValue::Array(array) => {
            let values = array.as_primitive_opt::<Float64Type>().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "spark_sqrt expected Float64, got {:?}",
                    array.data_type()
                ))
            })?;
            let result: Float64Array = values.unary(|v| v.sqrt());
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        ColumnarValue::Scalar(ScalarValue::Float64(v)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Float64(v.map(f64::sqrt)),
        )),
        ColumnarValue::Scalar(other) => Err(DataFusionError::Internal(format!(
            "spark_sqrt expected Float64 scalar, got {other:?}",
        ))),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn test_spark_sqrt_negative_is_nan() {
        let input = Float64Array::from(vec![Some(4.0), Some(-1.0), Some(0.0), None]);
        let result = spark_sqrt(&[ColumnarValue::Array(Arc::new(input))]).unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0), 2.0);
        assert!(result.value(1).is_nan());
        assert_eq!(result.value(2), 0.0);
        assert!(result.is_null(3));
    }

    #[test]
    fn test_spark_sqrt_scalar_negative_is_nan() {
        let result =
            spark_sqrt(&[ColumnarValue::Scalar(ScalarValue::Float64(Some(-1.0)))]).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(result))) = result else {
            unreachable!()
        };
        assert!(result.is_nan());
    }

    #[test]
    fn test_spark_sqrt_scalar_null() {
        let result = spark_sqrt(&[ColumnarValue::Scalar(ScalarValue::Float64(None))]).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Float64(None)) = result else {
            unreachable!()
        };
    }
}
