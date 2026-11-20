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
use arrow::array::{ArrayRef, AsArray, PrimitiveArray, StringBuilder};
use arrow::datatypes::{ArrowPrimitiveType, DataType, TimestampMicrosecondType};
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
    // true is 1 microsecond and false is 0.
    let micros = PrimitiveArray::<TimestampMicrosecondType>::from_unary(bool_array, |value| {
        i64::from(value)
    });
    Ok(timestamp_micros_with_tz(micros, target_tz))
}

/// true is 1 and false is 0, as in Spark. `from_unary` maps every slot in one pass and keeps the
/// null buffer, where Arrow's cast checks each row for null.
pub(crate) fn cast_boolean_to_numeric<T>(array_ref: &ArrayRef) -> ArrayRef
where
    T: ArrowPrimitiveType,
    T::Native: From<bool>,
{
    Arc::new(PrimitiveArray::<T>::from_unary(
        array_ref.as_boolean(),
        T::Native::from,
    ))
}

/// "true" or "false", as in Spark, into a builder sized for the longest value, where Arrow's cast
/// formats each row through a dynamic formatter.
pub(crate) fn cast_boolean_to_utf8(array_ref: &ArrayRef) -> ArrayRef {
    let bool_array = array_ref.as_boolean();
    let mut builder = StringBuilder::with_capacity(bool_array.len(), bool_array.len() * 5);
    for value in bool_array.iter() {
        match value {
            Some(true) => builder.append_value("true"),
            Some(false) => builder.append_value("false"),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cast::cast_array;
    use crate::{EvalMode, SparkCastOptions};
    use arrow::array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, StringArray, TimestampMicrosecondBuilder,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::DataType::{Decimal128, Timestamp};
    use arrow::datatypes::{TimeUnit, TimestampMicrosecondType};
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
        let all_true: ArrayRef = Arc::new(BooleanArray::from(vec![true, true]));
        let out = cast_boolean_to_timestamp(&all_true, &None).unwrap();
        let ts = out.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.values().as_ref(), &[1, 1]);
        assert!(ts.nulls().is_none());
    }

    #[test]
    fn test_cast_boolean_to_timestamp_null_slot_matches_builder() {
        let tz = Some(Arc::from("UTC"));
        // The slice starts on a null whose bit is set.
        let bits = BooleanArray::from(vec![false, true, false, true]);
        let masked = BooleanArray::new(
            bits.values().clone(),
            Some(NullBuffer::from(vec![true, false, true, false])),
        );
        let input = Array::slice(&masked, 1, 3);
        let bools = input.as_boolean();
        let mut builder = TimestampMicrosecondBuilder::with_capacity(bools.len());
        for i in 0..bools.len() {
            if bools.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(i64::from(bools.value(i)));
            }
        }
        let expected = builder.finish().with_timezone_opt(tz.clone());

        let out = cast_array(
            Arc::clone(&input),
            &Timestamp(TimeUnit::Microsecond, tz),
            &SparkCastOptions::new(EvalMode::Ansi, "UTC"),
        )
        .unwrap();
        let actual = out.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual.timezone(), expected.timezone());
        for i in 0..actual.len() {
            assert_eq!(actual.is_null(i), expected.is_null(i), "index {i}");
            if actual.is_valid(i) {
                assert_eq!(actual.value(i), expected.value(i), "index {i}");
            }
        }
    }

    #[test]
    fn test_bool_casts_match_arrow_cast() {
        // The native kernels replaced Arrow's cast for these targets; on a sliced input with nulls
        // they must give the same arrays.
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(false),
            None,
            Some(true),
        ]));
        let sliced = input.slice(1, 5);
        for to_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
        ] {
            for array in [&input, &sliced] {
                let actual =
                    cast_array(Arc::clone(array), &to_type, &test_input_spark_opts()).unwrap();
                let expected = arrow::compute::cast(array, &to_type).unwrap();
                assert_eq!(&actual, &expected, "{to_type}");
            }
        }
    }
}
