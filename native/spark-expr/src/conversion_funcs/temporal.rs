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

use crate::conversion_funcs::string::parse_sign_offset;
use crate::conversion_funcs::utils::{timestamp_micros_with_tz, MICROS_PER_SECOND};
use crate::{EvalMode, SparkCastOptions, SparkError, SparkResult};
use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Date32Type};
use arrow::error::ArrowError;
use std::sync::Arc;

pub(crate) fn is_df_cast_from_date_spark_compatible(to_type: &DataType) -> bool {
    matches!(to_type, DataType::Int32 | DataType::Utf8)
}

pub(crate) fn is_df_cast_from_timestamp_spark_compatible(to_type: &DataType) -> bool {
    matches!(
        to_type,
        DataType::Int64 | DataType::Date32 | DataType::Utf8 | DataType::Timestamp(_, _)
    )
}

pub(crate) fn cast_date_to_timestamp(
    array_ref: &ArrayRef,
    cast_options: &SparkCastOptions,
    target_tz: &Option<Arc<str>>,
) -> SparkResult<ArrayRef> {
    // CometTimeZone canonicalizes fixed offsets. Spark handles region zones, whose
    // start-of-day rules extend beyond chrono-tz's transition tables.
    let offset = if target_tz.is_none() || cast_options.timezone == "UTC" {
        0
    } else {
        parse_sign_offset(&cast_options.timezone).ok_or_else(|| {
            ArrowError::ParseError(format!(
                "Expected a fixed offset for DATE to TIMESTAMP, got {}",
                cast_options.timezone
            ))
        })?
    };
    // Every Date32 fits in seconds. Spark applies the offset before multiplying
    // by micros per second, so an offset can move midnight across the i64 boundary.
    let to_micros = |day| {
        let seconds = i64::from(day) * 86_400 - i64::from(offset);
        seconds.checked_mul(MICROS_PER_SECOND)
    };
    let dates = array_ref.as_primitive::<Date32Type>();
    let timestamps = if cast_options.eval_mode == EvalMode::Try {
        dates.unary_opt(to_micros)
    } else {
        dates
            .try_unary(|day| to_micros(day).ok_or(()))
            .map_err(|()| SparkError::LongOverflow)?
    };
    Ok(timestamp_micros_with_tz(timestamps, target_tz))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, TimestampMicrosecondArray};
    use arrow::datatypes::TimeUnit;

    #[test]
    fn test_cast_date_to_timestamp() {
        // 95,026,237 is +262143-01-01, the day after chrono's maximum date.
        let days = [Some(0), Some(95_026_237), Some(-95_026_237), None];
        let dates: ArrayRef = Arc::new(Date32Array::from_iter(days));
        for (zone, offset) in [("UTC", 0), ("-08:00", -28_800), ("+05:30", 19_800)] {
            let actual = cast_date_to_timestamp(
                &dates,
                &SparkCastOptions::new(EvalMode::Legacy, zone),
                &Some("UTC".into()),
            )
            .unwrap();
            let expected = TimestampMicrosecondArray::from_iter(
                days.map(|day| day.map(|d| i64::from(d) * 86_400_000_000 - offset * 1_000_000)),
            )
            .with_timezone("UTC");
            assert_eq!(actual.as_ref(), &expected, "{zone}");
        }
    }

    #[test]
    fn test_cast_date_to_timestamp_offset_at_boundary() {
        // The same day can fit or overflow depending on the offset.
        for (days, zone, expected) in [
            (106_751_991, "-18:00", None),
            (-106_751_991, "+18:00", None),
            (106_751_991, "+18:00", Some(9_223_371_957_600_000_000)),
            (-106_751_991, "-18:00", Some(-9_223_371_957_600_000_000)),
            (i32::MAX, "UTC", None),
            (i32::MIN, "UTC", None),
        ] {
            let dates: ArrayRef = Arc::new(Date32Array::from(vec![days]));
            for mode in [EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
                let result = cast_date_to_timestamp(
                    &dates,
                    &SparkCastOptions::new(mode, zone),
                    &Some("UTC".into()),
                );
                if expected.is_none() && mode != EvalMode::Try {
                    assert!(matches!(result, Err(SparkError::LongOverflow)));
                } else {
                    let expected =
                        TimestampMicrosecondArray::from(vec![expected]).with_timezone("UTC");
                    assert_eq!(result.unwrap().as_ref(), &expected);
                }
            }
        }
    }

    #[test]
    fn test_cast_date_to_timestamp_ntz() {
        use crate::EvalMode;
        use arrow::array::Date32Array;
        use arrow::array::{Array, ArrayRef};
        use arrow::datatypes::TimestampMicrosecondType;

        // For NTZ, result is always days * 86_400_000_000 regardless of session TZ
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(0),     // 1970-01-01
            Some(1),     // 1970-01-02
            Some(-1),    // 1969-12-31
            Some(19723), // 2024-01-01
            None,
            Some(106_751_991), // largest day count representable in microseconds
            Some(-106_751_991),
        ]));

        // NTZ target: no timezone annotation
        let ntz_target: Option<Arc<str>> = None;

        // session TZ should be ignored for NTZ
        for tz in &[
            "UTC",
            "America/Los_Angeles",
            "America/New_York",
            "Asia/Kolkata",
        ] {
            let result = cast_date_to_timestamp(
                &dates,
                &SparkCastOptions::new(EvalMode::Legacy, tz),
                &ntz_target,
            )
            .unwrap();
            let ts = result.as_primitive::<TimestampMicrosecondType>();
            // values are pure arithmetic regardless of session TZ
            assert_eq!(ts.value(0), 0, "epoch, tz={tz}");
            assert_eq!(ts.value(1), 86_400_000_000i64, "day+1, tz={tz}");
            assert_eq!(ts.value(2), -86_400_000_000i64, "day-1, tz={tz}");
            assert_eq!(
                ts.value(3),
                19723i64 * 86_400_000_000i64,
                "2024-01-01, tz={tz}"
            );
            assert!(ts.is_null(4), "null, tz={tz}");
            assert_eq!(ts.value(5), 106_751_991i64 * 86_400_000_000);
            assert_eq!(ts.value(6), -106_751_991i64 * 86_400_000_000);
            // output array has no timezone annotation
            assert_eq!(ts.timezone(), None, "no tz annotation, tz={tz}");
        }

        for day in [106_751_992, -106_751_992, i32::MIN, i32::MAX] {
            let dates: ArrayRef = Arc::new(Date32Array::from(vec![None, Some(day)]));
            let result = cast_date_to_timestamp(
                &dates,
                &SparkCastOptions::new(EvalMode::Legacy, "UTC"),
                &ntz_target,
            );
            assert!(matches!(result, Err(SparkError::LongOverflow)), "day={day}");
        }
    }

    #[test]
    fn test_spark_cast_date_to_timestamp_ntz_modes() {
        use crate::spark_cast;
        use arrow::array::{Date32Array, TimestampMicrosecondArray};
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::ColumnarValue;

        let target = DataType::Timestamp(TimeUnit::Microsecond, None);
        let days = vec![
            Some(0),
            Some(106_751_992),
            None,
            Some(-1),
            Some(-106_751_992),
        ];
        for mode in [EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
            let options = SparkCastOptions::new(mode, "America/Los_Angeles");
            let result = spark_cast(
                ColumnarValue::Array(Arc::new(Date32Array::from(days.clone()))),
                &target,
                &options,
            );
            if mode == EvalMode::Try {
                let ColumnarValue::Array(actual) = result.unwrap() else {
                    panic!("expected array");
                };
                let expected = TimestampMicrosecondArray::from(vec![
                    Some(0),
                    None,
                    None,
                    Some(-86_400_000_000),
                    None,
                ]);
                assert_eq!(actual.as_ref(), &expected);
            } else {
                assert!(result.is_err(), "mode={mode:?}");
            }

            for day in &days {
                let result = spark_cast(
                    ColumnarValue::Scalar(ScalarValue::Date32(*day)),
                    &target,
                    &options,
                );
                let micros = day.and_then(|d| i64::from(d).checked_mul(86_400_000_000));
                if day.is_some() && micros.is_none() && mode != EvalMode::Try {
                    assert!(result.is_err(), "mode={mode:?}, day={day:?}");
                } else {
                    let ColumnarValue::Scalar(actual) = result.unwrap() else {
                        panic!("expected scalar");
                    };
                    assert_eq!(actual, ScalarValue::TimestampMicrosecond(micros, None));
                }
            }
        }
    }
}
