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

use crate::SparkResult;
use arrow::array::{ArrayRef, AsArray, Decimal128Array};
use arrow::datatypes::DataType;
use std::sync::Arc;

pub fn is_df_cast_from_bool_spark_compatible(to_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        to_type,
        Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Utf8
    )
}

// only DF incompatible boolean cast
pub fn cast_boolean_to_decimal(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> SparkResult<ArrayRef> {
    let bool_array = array.as_boolean();
    let scaled_val = 10_i128.pow(scale as u32);
    let result: Decimal128Array = bool_array
        .iter()
        .map(|v| v.map(|b| if b { scaled_val } else { 0 }))
        .collect();
    Ok(Arc::new(result.with_precision_and_scale(precision, scale)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cast::cast_array;
    use crate::{EvalMode, SparkCastOptions};
    use arrow::array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, StringArray,
    };
    use arrow::datatypes::DataType::Decimal128;
    use std::sync::Arc;

    fn test_input_bool_array() -> ArrayRef {
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]))
    }

    fn test_input_spark_opts() -> SparkCastOptions {
        SparkCastOptions::new(EvalMode::Legacy, "Asia/Kolkata", false)
    }

    #[test]
    fn test_is_df_cast_from_bool_spark_compatible() {
        assert!(!is_df_cast_from_bool_spark_compatible(&DataType::Boolean));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Int8));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Int16));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Int32));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Int64));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Float32));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Float64));
        assert!(is_df_cast_from_bool_spark_compatible(&DataType::Utf8));
        assert!(!is_df_cast_from_bool_spark_compatible(
            &DataType::Decimal128(10, 4)
        ));
        assert!(!is_df_cast_from_bool_spark_compatible(&DataType::Null));
    }

    #[test]
    fn test_bool_to_int8_cast() {
        let result = cast_array(
            test_input_bool_array(),
            &DataType::Int8,
            &test_input_spark_opts(),
        )
        .unwrap();
        let arr = result.as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_bool_to_int16_cast() {
        let result = cast_array(
            test_input_bool_array(),
            &DataType::Int16,
            &test_input_spark_opts(),
        )
        .unwrap();
        let arr = result.as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_bool_to_int32_cast() {
        let result = cast_array(
            test_input_bool_array(),
            &DataType::Int32,
            &test_input_spark_opts(),
        )
        .unwrap();
        let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_bool_to_int64_cast() {
        let result = cast_array(
            test_input_bool_array(),
            &DataType::Int64,
            &test_input_spark_opts(),
        )
        .unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), 0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_bool_to_float32_cast() {
        let result = cast_array(
            test_input_bool_array(),
            &DataType::Float32,
            &test_input_spark_opts(),
        )
        .unwrap();
        let arr = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(arr.value(0), 1.0);
        assert_eq!(arr.value(1), 0.0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_bool_to_float64_cast() {
        let result = cast_array(
            test_input_bool_array(),
            &DataType::Float64,
            &test_input_spark_opts(),
        )
        .unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 1.0);
        assert_eq!(arr.value(1), 0.0);
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_bool_to_string_cast() {
        let result = cast_array(
            test_input_bool_array(),
            &DataType::Utf8,
            &test_input_spark_opts(),
        )
        .unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "true");
        assert_eq!(arr.value(1), "false");
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_bool_to_decimal_cast() {
        let result = cast_array(
            test_input_bool_array(),
            &Decimal128(10, 4),
            &test_input_spark_opts(),
        )
        .unwrap();
        let expected_arr = Decimal128Array::from(vec![10000_i128, 0_i128])
            .with_precision_and_scale(10, 4)
            .unwrap();
        let arr = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(arr.value(0), expected_arr.value(0));
        assert_eq!(arr.value(1), expected_arr.value(1));
        assert!(arr.is_null(2));
    }
}
