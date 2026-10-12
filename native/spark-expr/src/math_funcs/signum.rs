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

/// Spark-compatible `signum`, matching `java.lang.Math.signum`: a zero is returned as it is, so
/// `-0.0` keeps its sign. DataFusion's own `signum` returns `0.0` for both zeros.
pub fn spark_signum(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "spark_signum requires 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        ColumnarValue::Array(array) => {
            let values = array.as_primitive_opt::<Float64Type>().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "spark_signum expected Float64, got {:?}",
                    array.data_type()
                ))
            })?;
            let result: Float64Array = values.unary(signum);
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        ColumnarValue::Scalar(ScalarValue::Float64(v)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(v.map(signum))))
        }
        ColumnarValue::Scalar(other) => Err(DataFusionError::Internal(format!(
            "spark_signum expected Float64 scalar, got {other:?}",
        ))),
    }
}

/// `f64::signum` returns `1.0` and `-1.0` for the two zeros, so a zero is passed through instead.
/// NaN is not equal to zero, and `f64::signum` returns NaN for it.
#[inline]
fn signum(v: f64) -> f64 {
    if v == 0.0 {
        v
    } else {
        v.signum()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn test_spark_signum_keeps_the_sign_of_zero() {
        let input = Float64Array::from(vec![
            Some(5.5),
            Some(-5.5),
            Some(0.0),
            Some(-0.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
        ]);
        let result = spark_signum(&[ColumnarValue::Array(Arc::new(input))]).unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0), 1.0);
        assert_eq!(result.value(1), -1.0);
        // `0.0 == -0.0`, so the zeros are compared by their bits.
        assert_eq!(result.value(2).to_bits(), 0.0_f64.to_bits());
        assert_eq!(result.value(3).to_bits(), (-0.0_f64).to_bits());
        assert!(result.value(4).is_nan());
        assert_eq!(result.value(5), 1.0);
        assert_eq!(result.value(6), -1.0);
        assert!(result.is_null(7));
    }

    #[test]
    fn test_spark_signum_scalar_negative_zero() {
        let result =
            spark_signum(&[ColumnarValue::Scalar(ScalarValue::Float64(Some(-0.0)))]).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(result))) = result else {
            unreachable!()
        };
        assert_eq!(result.to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    fn test_spark_signum_scalar_null() {
        let result = spark_signum(&[ColumnarValue::Scalar(ScalarValue::Float64(None))]).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Float64(None)) = result else {
            unreachable!()
        };
    }
}
