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

use crate::utils::array_with_timezone;
use arrow::array::{Array, ArrayRef, Int32Array, TimestampMicrosecondArray};
use arrow::compute::{date_part, DatePart};
use arrow::datatypes::{DataType, Int32Type, TimeUnit::Microsecond};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::fmt::Debug;
use std::sync::Arc;

/// Returns true when the type is a timestamp without a timezone (Spark's TimestampNTZType),
/// including when wrapped in a dictionary. Such values store local wall-clock time and must not
/// have any session timezone offset applied when extracting date parts.
fn is_timestamp_ntz(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, None) => true,
        DataType::Dictionary(_, value_type) => is_timestamp_ntz(value_type),
        _ => false,
    }
}

const MICROS_PER_DAY: i64 = 86_400_000_000;
const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_MINUTE: i64 = 60_000_000;
const MICROS_PER_SECOND: i64 = 1_000_000;

/// Session timezones whose UTC offset is zero at every instant, so a clock field read from the
/// stored UTC instant needs no shift. The list is deliberately conservative: a name that is not
/// listed simply takes the general timezone-aware path, which is always correct.
fn is_utc_timezone(timezone: &str) -> bool {
    matches!(
        timezone,
        "UTC" | "Etc/UTC" | "Etc/GMT" | "GMT" | "Z" | "+00:00" | "-00:00" | "00:00"
    )
}

/// Returns the microsecond values when the clock field is a pure function of them, i.e. when no
/// timezone offset applies. That is the case for `TimestampNTZ` (already local wall-clock) and
/// for a timezone-aware timestamp read in a UTC session -- `array_with_timezone` only re-tags
/// such an array, so the stored value is the UTC instant either way.
///
/// Dictionaries and non-microsecond units return `None` and keep the general path.
fn micros_without_offset<'a>(
    array: &'a ArrayRef,
    timezone: &str,
) -> Option<&'a TimestampMicrosecondArray> {
    match array.data_type() {
        DataType::Timestamp(Microsecond, None) => {}
        DataType::Timestamp(Microsecond, Some(_)) if is_utc_timezone(timezone) => {}
        _ => return None,
    }
    array.as_any().downcast_ref::<TimestampMicrosecondArray>()
}

/// Euclidean division is required: at UTC, `-1` microsecond is `1969-12-31 23:59:59.999999`, so
/// truncation toward zero would give the wrong field for pre-epoch instants.
#[inline]
fn hour_of_day(micros: i64) -> i32 {
    (micros.rem_euclid(MICROS_PER_DAY) / MICROS_PER_HOUR) as i32
}

#[inline]
fn minute_of_hour(micros: i64) -> i32 {
    micros.div_euclid(MICROS_PER_MINUTE).rem_euclid(60) as i32
}

#[inline]
fn second_of_minute(micros: i64) -> i32 {
    micros.div_euclid(MICROS_PER_SECOND).rem_euclid(60) as i32
}

macro_rules! extract_date_part {
    ($struct_name:ident, $fn_name:expr, $date_part_variant:ident, $kernel:ident) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $struct_name {
            signature: Signature,
            aliases: Vec<String>,
            timezone: String,
        }

        impl $struct_name {
            pub fn new(timezone: String) -> Self {
                Self {
                    signature: Signature::user_defined(Volatility::Immutable),
                    aliases: vec![],
                    timezone,
                }
            }
        }

        impl ScalarUDFImpl for $struct_name {
            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
                Ok(match &arg_types[0] {
                    DataType::Dictionary(_, _) => {
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32))
                    }
                    _ => DataType::Int32,
                })
            }

            fn invoke_with_args(
                &self,
                args: ScalarFunctionArgs,
            ) -> datafusion::common::Result<ColumnarValue> {
                let args: [ColumnarValue; 1] = args.args.try_into().map_err(|_| {
                    internal_datafusion_err!(concat!($fn_name, " expects exactly one argument"))
                })?;

                match args {
                    [ColumnarValue::Array(array)] => {
                        // Fast path: when no offset applies the field is arithmetic on the stored
                        // microseconds, so no calendar datetime is built per row. `unary` carries
                        // the null buffer over untouched.
                        if let Some(micros) = micros_without_offset(&array, &self.timezone) {
                            let result: Int32Array = micros.unary::<_, Int32Type>($kernel);
                            return Ok(ColumnarValue::Array(Arc::new(result)));
                        }

                        // TimestampNTZ values are stored as local wall-clock time, so the date
                        // part is extracted directly. Timezone-aware timestamps are stored in UTC
                        // and must be shifted to the session timezone first.
                        let array = if is_timestamp_ntz(array.data_type()) {
                            array
                        } else {
                            array_with_timezone(
                                array,
                                self.timezone.clone(),
                                Some(&DataType::Timestamp(
                                    Microsecond,
                                    Some(self.timezone.clone().into()),
                                )),
                            )?
                        };
                        let result = date_part(&array, DatePart::$date_part_variant)?;
                        Ok(ColumnarValue::Array(result))
                    }
                    _ => Err(DataFusionError::Execution(
                        concat!($fn_name, "(scalar) should be fold in Spark JVM side.").to_string(),
                    )),
                }
            }

            fn aliases(&self) -> &[String] {
                &self.aliases
            }
        }
    };
}

extract_date_part!(SparkHour, "hour", Hour, hour_of_day);
extract_date_part!(SparkMinute, "minute", Minute, minute_of_hour);
extract_date_part!(SparkSecond, "second", Second, second_of_minute);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, TimeUnit};
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    // 2024-01-15 18:30:45 UTC, in microseconds since the epoch.
    const MICROS: i64 = 1_705_343_445_000_000;

    /// Invokes a single-argument date-part UDF on `array` and returns the first extracted value.
    fn invoke<U: ScalarUDFImpl>(udf: &U, array: ArrayRef) -> i32 {
        let return_field = Arc::new(Field::new("v", DataType::Int32, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(array)],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(arr) => {
                arr.as_any().downcast_ref::<Int32Array>().unwrap().value(0)
            }
            _ => panic!("Expected array"),
        }
    }

    fn ntz_array() -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(vec![Some(MICROS)]))
    }

    /// Invokes a UDF over a whole array and returns every extracted value.
    fn invoke_all<U: ScalarUDFImpl + ?Sized>(udf: &U, array: ArrayRef) -> Int32Array {
        let rows = array.len();
        let return_field = Arc::new(Field::new("v", DataType::Int32, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(array)],
            number_rows: rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<Int32Array>().unwrap().clone(),
            _ => panic!("Expected array"),
        }
    }

    /// The UTC fast path must agree with the general timezone-aware path. Both are exercised
    /// here on the same instant: a UTC session takes the fast path, `Etc/GMT+0` is not in the
    /// allowlist and takes the general path, and both describe the same zero offset.
    #[test]
    fn utc_fast_path_matches_general_path() {
        let array = TimestampMicrosecondArray::from(vec![Some(MICROS)]).with_timezone("UTC");
        let fast = SparkHour::new("UTC".to_string());
        let general = SparkHour::new("Etc/GMT+0".to_string());
        assert!(is_utc_timezone("UTC") && !is_utc_timezone("Etc/GMT+0"));
        assert_eq!(
            invoke(&fast, Arc::new(array.clone())),
            invoke(&general, Arc::new(array))
        );
    }

    /// Pre-epoch instants are where truncation toward zero would diverge from Spark: -1 us is
    /// 1969-12-31 23:59:59.999999, not 1970-01-01 00:00:00.
    #[test]
    fn pre_epoch_instants_use_euclidean_division() {
        let values = vec![Some(-1i64), Some(-MICROS_PER_DAY), Some(-MICROS_PER_SECOND)];
        for (udf, expected) in [
            (
                Box::new(SparkHour::new("UTC".to_string())) as Box<dyn ScalarUDFImpl>,
                vec![23, 0, 23],
            ),
            (
                Box::new(SparkMinute::new("UTC".to_string())),
                vec![59, 0, 59],
            ),
            (
                Box::new(SparkSecond::new("UTC".to_string())),
                vec![59, 0, 59],
            ),
        ] {
            // Both the NTZ shape and the UTC-tagged shape take the fast path.
            let ntz: ArrayRef = Arc::new(TimestampMicrosecondArray::from(values.clone()));
            let tagged: ArrayRef =
                Arc::new(TimestampMicrosecondArray::from(values.clone()).with_timezone("UTC"));
            for array in [ntz, tagged] {
                assert_eq!(
                    invoke_all(udf.as_ref(), array).values(),
                    &expected[..],
                    "{} mismatch",
                    udf.name()
                );
            }
        }
    }

    /// The fast path must carry the null buffer over unchanged, and the value stored under a
    /// null slot must not influence the result.
    #[test]
    fn fast_path_preserves_nulls() {
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(MICROS),
            None,
            Some(-1),
        ]));
        let out = invoke_all(&SparkHour::new("UTC".to_string()), array);
        assert_eq!(out.null_count(), 1);
        assert!(out.is_null(1));
        assert_eq!(out.value(0), 18);
        assert_eq!(out.value(2), 23);
    }

    /// A session timezone with a real offset must not be routed through the fast path.
    #[test]
    fn non_utc_session_is_not_accelerated() {
        for tz in ["America/Los_Angeles", "Asia/Tokyo", "+05:30"] {
            assert!(!is_utc_timezone(tz), "{tz} must not be treated as UTC");
        }
        let array: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(MICROS)]).with_timezone("UTC"));
        // 18:30 UTC is 03:30 the next day in Tokyo (UTC+9).
        assert_eq!(invoke(&SparkHour::new("Asia/Tokyo".to_string()), array), 3);
    }

    #[test]
    fn timestamp_with_timezone_converts_to_session_timezone() {
        // A timezone-aware timestamp stores the absolute UTC instant. In a Los Angeles session
        // (UTC-8 in January) the local hour is 18 - 8 = 10.
        let udf = SparkHour::new("America/Los_Angeles".to_string());
        let array = TimestampMicrosecondArray::from(vec![Some(MICROS)]).with_timezone("UTC");
        assert_eq!(invoke(&udf, Arc::new(array)), 10);
    }

    #[test]
    fn timestamp_ntz_extracts_local_time_without_conversion() {
        // TimestampNTZ stores local wall-clock time, so the hour is extracted directly (18) with
        // no session timezone offset applied (issue #3180).
        let udf = SparkHour::new("America/Los_Angeles".to_string());
        assert_eq!(invoke(&udf, ntz_array()), 18);
    }

    #[test]
    fn timestamp_ntz_result_is_independent_of_session_timezone() {
        // The extracted hour must be the same regardless of the session timezone.
        for tz in ["UTC", "America/Los_Angeles", "Asia/Tokyo"] {
            let udf = SparkHour::new(tz.to_string());
            assert_eq!(invoke(&udf, ntz_array()), 18);
        }
    }

    #[test]
    fn minute_and_second_on_timestamp_ntz() {
        assert_eq!(
            invoke(&SparkMinute::new("Asia/Tokyo".to_string()), ntz_array()),
            30
        );
        assert_eq!(
            invoke(&SparkSecond::new("Asia/Tokyo".to_string()), ntz_array()),
            45
        );
    }

    #[test]
    fn is_timestamp_ntz_detects_plain_and_dictionary_wrapped() {
        assert!(is_timestamp_ntz(&DataType::Timestamp(
            TimeUnit::Microsecond,
            None
        )));
        assert!(is_timestamp_ntz(&DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Timestamp(TimeUnit::Microsecond, None)),
        )));
        // Timezone-aware timestamps and dictionaries of them are not NTZ.
        assert!(!is_timestamp_ntz(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )));
        assert!(!is_timestamp_ntz(&DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
        )));
    }

    #[test]
    fn timestamp_ntz_dictionary_input_extracts_local_time() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;
        // Comet only emits Int32-keyed dictionaries, so the test mirrors that production shape and
        // the declared Dictionary(Int32, Int32) return type.
        let values = Arc::new(TimestampMicrosecondArray::from(vec![Some(MICROS)])) as ArrayRef;
        let keys = Int32Array::from(vec![0i32]);
        let dict = DictionaryArray::<Int32Type>::new(keys, values);
        let udf = SparkHour::new("America/Los_Angeles".to_string());
        let return_field = Arc::new(Field::new(
            "hour",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
            true,
        ));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(dict))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        let arr = match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        // The kernel preserves the Int32 key type, so the result keeps the declared dictionary shape.
        assert_eq!(
            arr.data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32))
        );
        let dict = arr
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let key = dict.keys().value(0) as usize;
        let extracted = dict
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(key);
        assert_eq!(extracted, 18);
    }
}
