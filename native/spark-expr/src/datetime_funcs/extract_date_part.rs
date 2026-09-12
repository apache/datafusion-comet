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
use arrow::array::Decimal128Array;
use arrow::compute::{date_part, DatePart};
use arrow::datatypes::{DataType, TimeUnit::Microsecond};
use datafusion::common::cast::as_time64_nanosecond_array;
use datafusion::common::{
    internal_datafusion_err, utils::take_function_args, DataFusionError, Result, ScalarValue,
};
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

macro_rules! extract_date_part {
    ($struct_name:ident, $fn_name:expr, $date_part_variant:ident) => {
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

extract_date_part!(SparkHour, "hour", Hour);
extract_date_part!(SparkMinute, "minute", Minute);
extract_date_part!(SparkSecond, "second", Second);

/// Spark 4.1 EXTRACT(SECOND FROM TIME): truncate to the input precision and return
/// Decimal(8,6). The precision is a literal supplied by Spark's TimeType lowering.
pub fn spark_seconds_of_time(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [time, precision] = take_function_args("seconds_of_time", args)?;
    let precision = match precision {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(p))) if (0..=6).contains(p) => *p as u32,
        _ => {
            return Err(internal_datafusion_err!(
                "seconds_of_time requires a literal precision from 0 to 6"
            ))
        }
    };
    let divisor = 10_i64.pow(9 - precision);
    let multiplier = 10_i128.pow(6 - precision);
    let extract = |nanos: i64| i128::from((nanos % 60_000_000_000) / divisor) * multiplier;
    match time {
        ColumnarValue::Array(array) => {
            let times = as_time64_nanosecond_array(array.as_ref())?;
            let seconds: Decimal128Array = times.iter().map(|nanos| nanos.map(extract)).collect();
            Ok(ColumnarValue::Array(Arc::new(
                seconds.with_precision_and_scale(8, 6)?,
            )))
        }
        ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(nanos)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Decimal128(nanos.map(extract), 8, 6),
        )),
        _ => Err(internal_datafusion_err!(
            "seconds_of_time requires Time64(Nanosecond)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, TimeUnit};
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    #[test]
    fn seconds_of_time_truncates_to_input_precision() {
        for (precision, expected) in [
            45_000_000, 45_100_000, 45_120_000, 45_123_000, 45_123_400, 45_123_450, 45_123_456,
        ]
        .into_iter()
        .enumerate()
        {
            let precision = ColumnarValue::Scalar(ScalarValue::Int32(Some(precision as i32)));
            for nanos in [Some(45_045_123_456_789), None] {
                let args = [
                    ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(nanos)),
                    precision.clone(),
                ];
                let ColumnarValue::Scalar(result) = spark_seconds_of_time(&args).unwrap() else {
                    panic!("expected scalar")
                };
                assert_eq!(
                    result,
                    ScalarValue::Decimal128(nanos.map(|_| expected), 8, 6)
                );
            }
            let times =
                arrow::array::Time64NanosecondArray::from(vec![Some(45_045_123_456_789), None]);
            let args = [ColumnarValue::Array(Arc::new(times)), precision];
            let ColumnarValue::Array(result) = spark_seconds_of_time(&args).unwrap() else {
                panic!("expected array")
            };
            let expected = Decimal128Array::from(vec![Some(expected), None])
                .with_precision_and_scale(8, 6)
                .unwrap();
            assert_eq!(result.as_ref(), &expected);
        }
    }

    #[test]
    fn seconds_of_time_boundaries() {
        for (nanos, expected) in [
            (0, 0),
            (999, 0),
            (1_000, 1),
            (59_999_999_999, 59_999_999),
            (60_000_000_000, 0),
            (86_399_999_999_999, 59_999_999),
        ] {
            let args = [
                ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(Some(nanos))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(6))),
            ];
            let ColumnarValue::Scalar(result) = spark_seconds_of_time(&args).unwrap() else {
                panic!("expected scalar")
            };
            assert_eq!(result, ScalarValue::Decimal128(Some(expected), 8, 6));
        }
        for precision in [-1, 7, i32::MAX] {
            assert!(spark_seconds_of_time(&[
                ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(Some(0))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(precision)))
            ])
            .is_err());
        }
    }

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
