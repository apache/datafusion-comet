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

use crate::{EvalMode, SparkError, SparkResult};
use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, TimestampMicrosecondBuilder};
use arrow::datatypes::{is_validate_decimal_precision, DataType};
use std::sync::Arc;

pub fn is_df_cast_from_bool_spark_compatible(to_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        to_type,
        Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Utf8
    )
}

pub fn cast_boolean_to_decimal(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    let bool_array = array.as_boolean();
    let scaled_val = 10_i128.pow(scale as u32);

    // Spark's Cast uses `nullOnOverflow = !ansiEnabled`: legacy/try return NULL
    // on overflow, only ANSI raises. `false` maps to 0 which always fits, so
    // overflow only happens for `true` when 10^scale exceeds `precision`.
    let overflows = !is_validate_decimal_precision(scaled_val, precision);
    if overflows && eval_mode == EvalMode::Ansi {
        return Err(crate::error::decimal_overflow_error(
            scaled_val, precision, scale,
        ));
    }

    let result: Decimal128Array = bool_array
        .iter()
        .map(|v| match v {
            Some(false) => Some(0),
            Some(true) if !overflows => Some(scaled_val),
            _ => None,
        })
        .collect();
    let decimal_array = result
        .with_precision_and_scale(precision, scale)
        .map_err(|e| SparkError::Arrow(Arc::new(e)))?;
    Ok(Arc::new(decimal_array))
}

pub(crate) fn cast_boolean_to_timestamp(
    array_ref: &ArrayRef,
    target_tz: &Option<Arc<str>>,
) -> SparkResult<ArrayRef> {
    let bool_array = array_ref.as_boolean();
    let mut builder = TimestampMicrosecondBuilder::with_capacity(bool_array.len());

    for i in 0..bool_array.len() {
        if bool_array.is_null(i) {
            builder.append_null();
        } else {
            let micros = if bool_array.value(i) { 1 } else { 0 };
            builder.append_value(micros);
        }
    }

    Ok(Arc::new(builder.finish().with_timezone_opt(target_tz.clone())) as ArrayRef)
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
    use arrow::datatypes::TimestampMicrosecondType;
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

    #[test]
    fn test_bool_to_decimal_overflow_returns_null_for_nonansi() {
        // 10^1 = 10 does not fit in DECIMAL(1,1); Spark returns NULL for `true`
        // in legacy and try modes.
        for mode in [EvalMode::Legacy, EvalMode::Try] {
            let result = cast_array(
                test_input_bool_array(),
                &Decimal128(1, 1),
                &SparkCastOptions::new(mode, "UTC", false),
            )
            .unwrap();
            let arr = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert!(arr.is_null(0), "mode {mode:?}");
            assert_eq!(arr.value(1), 0, "mode {mode:?}");
            assert!(arr.is_null(2), "mode {mode:?}");
        }
    }

    #[test]
    fn test_bool_to_decimal_overflow_ansi_errors() {
        let result = cast_array(
            test_input_bool_array(),
            &Decimal128(1, 1),
            &SparkCastOptions::new(EvalMode::Ansi, "UTC", false),
        );
        let err = result.expect_err("expected ANSI overflow error");
        assert!(
            err.to_string().contains("cannot be represented"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn test_cast_boolean_to_timestamp() {
        let timezones: [Option<Arc<str>>; 3] = [
            Some(Arc::from("UTC")),
            Some(Arc::from("America/Los_Angeles")),
            None,
        ];

        for tz in &timezones {
            let bool_array: ArrayRef =
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));

            let result = cast_boolean_to_timestamp(&bool_array, tz).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();

            assert_eq!(ts_array.value(0), 1); // true -> 1 microsecond
            assert_eq!(ts_array.value(1), 0); // false -> 0 (epoch)
            assert!(ts_array.is_null(2));
            assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));
        }
    }
}
