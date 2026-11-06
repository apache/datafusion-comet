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

use crate::math_funcs::utils::{spark_ceil_or_floor, Floor};
use arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ColumnarValue;

/// `floor` function that simulates Spark `floor` expression
pub fn spark_floor(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    spark_ceil_or_floor::<Floor>(args, data_type)
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

    #[test]
    fn test_floor_decimal128_array() -> Result<()> {
        let array = Decimal128Array::from(vec![
            Some(12345),  // 123.45
            Some(12500),  // 125.00
            Some(-12999), // -129.99
            None,
        ])
        .with_precision_and_scale(5, 2)?;
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Decimal128(4, 0))? else {
            unreachable!()
        };
        let expected = Decimal128Array::from(vec![
            Some(123),  // 123.00
            Some(125),  // 125.00
            Some(-130), // -130.00
            None,
        ])
        .with_precision_and_scale(4, 0)?;
        let actual = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_floor_decimal128_wide_array() -> Result<()> {
        // Unscaled values above i64::MAX (~9.22e18) take the wide fallback in decimal_floor_pow10.
        // Scale 6 targeting Decimal128(38, 0):
        //   20_000_000_000_000_000_000 / 10^6 = 20_000_000_000_000  (exact multiple)
        //   20_000_000_000_000_000_001 / 10^6 -> floor 20_000_000_000_000  (toward zero)
        //  -20_000_000_000_000_000_001 / 10^6 -> floor -20_000_000_000_001  (toward -inf)
        let array = Decimal128Array::from(vec![
            Some(20_000_000_000_000_000_000_i128),
            Some(20_000_000_000_000_000_001_i128),
            Some(-20_000_000_000_000_000_001_i128),
            None,
        ])
        .with_precision_and_scale(38, 6)?;
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Decimal128(38, 0))? else {
            unreachable!()
        };
        let expected = Decimal128Array::from(vec![
            Some(20_000_000_000_000_i128),
            Some(20_000_000_000_000_i128),
            Some(-20_000_000_000_001_i128),
            None,
        ])
        .with_precision_and_scale(38, 0)?;
        let actual = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_floor_decimal128_i64_boundary_array() -> Result<()> {
        // decimal_floor_pow10 switches from the i64 fast path to decimal_floor_wide exactly at
        // i64::MAX / i64::MIN. Straddling both edges pins that the two paths agree: each pair
        // below differs by one unscaled unit but floors to the same value, so a divergence
        // between the 64-bit and 128-bit divisions would show up as a mismatch here.
        let array = Decimal128Array::from(vec![
            Some(i64::MAX as i128),     //  9223372036854.775807 -> fast path
            Some(i64::MAX as i128 + 1), //  9223372036854.775808 -> wide path
            Some(i64::MIN as i128),     // -9223372036854.775808 -> fast path
            Some(i64::MIN as i128 - 1), // -9223372036854.775809 -> wide path
        ])
        .with_precision_and_scale(38, 6)?;
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Decimal128(38, 0))? else {
            unreachable!()
        };
        let expected = Decimal128Array::from(vec![
            Some(9_223_372_036_854_i128),
            Some(9_223_372_036_854_i128),
            Some(-9_223_372_036_855_i128),
            Some(-9_223_372_036_855_i128),
        ])
        .with_precision_and_scale(38, 0)?;
        let actual = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_floor_decimal128_large_scale_array() -> Result<()> {
        // dispatch_pow10! only specializes scales whose divisor fits in an i64, so scale 19 takes
        // the runtime `decimal_floor_f` fallback. Untested on the array path otherwise.
        let array = Decimal128Array::from(vec![
            Some(20_000_000_000_000_000_000_000_i128),  //  2000.0
            Some(20_000_000_000_000_000_000_001_i128),  //  2000.0000000000000000001
            Some(-20_000_000_000_000_000_000_001_i128), // -2000.0000000000000000001
            None,
        ])
        .with_precision_and_scale(38, 19)?;
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Decimal128(38, 0))? else {
            unreachable!()
        };
        let expected = Decimal128Array::from(vec![
            Some(2000_i128),
            Some(2000_i128),
            Some(-2001_i128),
            None,
        ])
        .with_precision_and_scale(38, 0)?;
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

    #[test]
    fn test_floor_decimal128_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(567),
            3,
            1,
        ))]; // 56.7
        let ColumnarValue::Scalar(ScalarValue::Decimal128(Some(result), 3, 0)) =
            spark_floor(&args, &DataType::Decimal128(3, 0))?
        else {
            unreachable!()
        };
        assert_eq!(result, 56); // 56.0
        Ok(())
    }
}
