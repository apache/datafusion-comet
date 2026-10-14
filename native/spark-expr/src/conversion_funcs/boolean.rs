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

use crate::conversion_funcs::utils::timestamp_micros_with_tz;
use crate::SparkResult;
use arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::{DataType, TimestampMicrosecondType};
use std::sync::Arc;

/// Boolean -> Decimal is intentionally absent: it has no native path and is routed through the
/// JVM codegen dispatcher on the Scala side (see `CometCast.canCastFromBoolean`).
pub fn is_df_cast_from_bool_spark_compatible(to_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        to_type,
        Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Utf8
    )
}

pub(crate) fn cast_boolean_to_timestamp(
    array_ref: &ArrayRef,
    target_tz: &Option<Arc<str>>,
) -> SparkResult<ArrayRef> {
    let bool_array = array_ref.as_boolean();
    // true is 1 microsecond and false is 0. `from_unary` maps every slot in one
    // pass and copies the null buffer. The conversion cannot fail.
    let micros = PrimitiveArray::<TimestampMicrosecondType>::from_unary(bool_array, |value| {
        i64::from(value)
    });
    Ok(timestamp_micros_with_tz(micros, target_tz))
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
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::DataType::Decimal128;
    use arrow::datatypes::TimestampMicrosecondType;
    use std::sync::Arc;

    fn test_input_bool_array() -> ArrayRef {
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]))
    }

    fn test_input_spark_opts() -> SparkCastOptions {
        SparkCastOptions::new(EvalMode::Legacy, "Asia/Kolkata")
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
    fn test_bool_to_decimal_cast_is_not_supported() {
        // Boolean -> Decimal has no native path; the Scala planner routes it through the JVM
        // codegen dispatcher, so reaching the native cast at all is a bug.
        let err = cast_array(
            test_input_bool_array(),
            &Decimal128(10, 4),
            &test_input_spark_opts(),
        )
        .expect_err("expected boolean -> decimal to be unsupported");
        assert!(
            err.to_string()
                .contains("Native cast invoked for unsupported cast"),
            "unexpected error: {err}"
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

    #[test]
    fn test_cast_boolean_to_timestamp_edges() {
        let tz = Some(Arc::from("UTC"));

        let all_true: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, true]));
        let out = cast_boolean_to_timestamp(&all_true, &tz).unwrap();
        let ts = out.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.values().as_ref(), &[1, 1, 1]);
        assert!(ts.nulls().is_none());
        assert_eq!(ts.timezone(), Some("UTC"));

        let all_false: ArrayRef = Arc::new(BooleanArray::from(vec![false, false]));
        let out = cast_boolean_to_timestamp(&all_false, &None).unwrap();
        let ts = out.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.values().as_ref(), &[0, 0]);
        assert!(ts.nulls().is_none());
        assert!(ts.timezone().is_none());

        let all_null = BooleanArray::new_null(3);
        let out = cast_boolean_to_timestamp(&(Arc::new(all_null) as ArrayRef), &tz).unwrap();
        let ts = out.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.len(), 3);
        assert_eq!(ts.null_count(), 3);
        assert!(ts.is_null(0) && ts.is_null(1) && ts.is_null(2));

        let empty: ArrayRef = Arc::new(BooleanArray::from(Vec::<bool>::new()));
        let out = cast_boolean_to_timestamp(&empty, &None).unwrap();
        assert_eq!(out.len(), 0);
        assert!(out
            .as_primitive::<TimestampMicrosecondType>()
            .nulls()
            .is_none());

        let full = BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]);
        let sliced = Array::slice(&full, 1, 3);
        let out = cast_boolean_to_timestamp(&sliced, &tz).unwrap();
        let ts = out.as_primitive::<TimestampMicrosecondType>();
        assert!(ts.is_null(0));
        assert_eq!(ts.value(1), 0);
        assert_eq!(ts.value(2), 1);
        assert_eq!(ts.null_count(), 1);

        // A true bit sitting under a null stays null and does not affect the neighbor.
        let bits = BooleanArray::from(vec![true, false, true]);
        let masked = BooleanArray::new(
            bits.values().clone(),
            Some(NullBuffer::from(vec![false, true, false])),
        );
        let out = cast_boolean_to_timestamp(&(Arc::new(masked) as ArrayRef), &None).unwrap();
        let ts = out.as_primitive::<TimestampMicrosecondType>();
        assert!(ts.is_null(0));
        assert_eq!(ts.value(1), 0);
        assert!(ts.is_null(2));
    }
}
