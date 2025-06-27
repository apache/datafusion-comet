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

use crate::downcast_compute_op;
use crate::math_funcs::utils::{get_precision_scale, make_decimal_array, make_decimal_scalar};
use arrow::array::{Array, ArrowNativeTypeOp};
use arrow::array::{Float32Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use num::integer::div_floor;
use std::sync::Arc;

/// `floor` function that simulates Spark `floor` expression
pub fn spark_floor(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let result = downcast_compute_op!(array, "floor", floor, Float32Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Float64 => {
                let result = downcast_compute_op!(array, "floor", floor, Float64Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Int64 => {
                let result = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(ColumnarValue::Array(Arc::new(result.clone())))
            }
            DataType::Decimal128(_, scale) if *scale > 0 => {
                let f = decimal_floor_f(scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_array(array, precision, scale, &f)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function floor",
                other,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.floor() as i64),
            ))),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.floor() as i64),
            ))),
            ScalarValue::Int64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(a.map(|x| x)))),
            ScalarValue::Decimal128(a, _, scale) if *scale > 0 => {
                let f = decimal_floor_f(scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, &f)
            }
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function floor",
                value.data_type(),
            ))),
        },
    }
}

#[inline]
fn decimal_floor_f(scale: &i8) -> impl Fn(i128) -> i128 {
    let div = 10_i128.pow_wrapping(*scale as u32);
    move |x: i128| div_floor(x, div)
}

#[cfg(test)]
mod test {
    use crate::spark_floor;
    use arrow::array::{Decimal128Array, Float32Array, Float64Array, Int64Array};
    use arrow::datatypes::DataType;
    use datafusion::common::cast::as_int64_array;
    use datafusion::common::{Result, ScalarValue};
    use datafusion::physical_plan::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_floor_f32_array() -> Result<()> {
        let input = Float32Array::from(vec![
            Some(125.9345),
            Some(15.9999),
            Some(0.9),
            Some(-0.1),
            Some(-1.999),
            Some(123.0),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Float32)? else {
            unreachable!()
        };
        let actual = as_int64_array(&result)?;
        let expected = Int64Array::from(vec![
            Some(125),
            Some(15),
            Some(0),
            Some(-1),
            Some(-2),
            Some(123),
            None,
        ]);
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_floor_f64_array() -> Result<()> {
        let input = Float64Array::from(vec![
            Some(125.9345),
            Some(15.9999),
            Some(0.9),
            Some(-0.1),
            Some(-1.999),
            Some(123.0),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Float64)? else {
            unreachable!()
        };
        let actual = as_int64_array(&result)?;
        let expected = Int64Array::from(vec![
            Some(125),
            Some(15),
            Some(0),
            Some(-1),
            Some(-2),
            Some(123),
            None,
        ]);
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_floor_i64_array() -> Result<()> {
        let input = Int64Array::from(vec![Some(-1), Some(0), Some(1), None]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Int64)? else {
            unreachable!()
        };
        let actual = as_int64_array(&result)?;
        let expected = Int64Array::from(vec![Some(-1), Some(0), Some(1), None]);
        assert_eq!(actual, &expected);
        Ok(())
    }

    // https://github.com/apache/datafusion-comet/issues/1729
    #[test]
    #[ignore]
    fn test_floor_decimal128_array() -> Result<()> {
        let array = Decimal128Array::from(vec![
            Some(12345),  // 123.45
            Some(12500),  // 125.00
            Some(-12999), // -129.99
            None,
        ])
        .with_precision_and_scale(5, 2)?;
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Decimal128(5, 2))? else {
            unreachable!()
        };
        let expected = Decimal128Array::from(vec![
            Some(12300),  // 123.00
            Some(12500),  // 125.00
            Some(-13000), // -130.00
            None,
        ])
        .with_precision_and_scale(5, 2)?;
        let actual = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_floor_f32_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Float32(Some(125.9345)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) =
            spark_floor(&args, &DataType::Float32)?
        else {
            unreachable!()
        };
        assert_eq!(result, 125);
        Ok(())
    }

    #[test]
    fn test_floor_f64_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(-1.999)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) =
            spark_floor(&args, &DataType::Float64)?
        else {
            unreachable!()
        };
        assert_eq!(result, -2);
        Ok(())
    }

    #[test]
    fn test_floor_i64_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(48)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) =
            spark_floor(&args, &DataType::Int64)?
        else {
            unreachable!()
        };
        assert_eq!(result, 48);
        Ok(())
    }

    // https://github.com/apache/datafusion-comet/issues/1729
    #[test]
    #[ignore]
    fn test_floor_decimal128_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(567),
            3,
            1,
        ))]; // 56.7
        let ColumnarValue::Scalar(ScalarValue::Decimal128(Some(result), 3, 1)) =
            spark_floor(&args, &DataType::Decimal128(3, 1))?
        else {
            unreachable!()
        };
        assert_eq!(result, 560); // 56.0
        Ok(())
    }
}
