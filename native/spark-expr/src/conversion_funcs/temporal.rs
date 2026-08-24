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

use crate::{EvalMode, SparkCastOptions, SparkError, SparkResult};
use arrow::array::{ArrayRef, AsArray, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, Date32Type};
use arrow::error::ArrowError;
use chrono::FixedOffset;
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
    let date_array = array_ref.as_primitive::<Date32Type>();
    let mut builder = TimestampMicrosecondBuilder::with_capacity(date_array.len());

    // Date32 has a wider range than both chrono's calendar and an i64 microsecond timestamp.
    // Use day arithmetic for NTZ and fixed-offset zones. Region-zone casts run through Spark's
    // JVM codegen dispatcher (see CometCast.canCastFromDate), which owns their full-range rules.
    let offset_seconds = if target_tz.is_none()
        || cast_options.timezone.is_empty()
        || cast_options.timezone == "UTC"
    {
        0
    } else {
        // Reject any unexpected region-zone call safely instead of constructing a chrono date.
        cast_options
            .timezone
            .parse::<FixedOffset>()
            .map_err(|_| {
                ArrowError::ParseError(format!(
                    "Invalid timezone \"{}\": expected a fixed offset",
                    cast_options.timezone
                ))
            })?
            .local_minus_utc()
    };
    for date in date_array.iter() {
        match date {
            Some(d) => {
                // Date32 days fit i64 seconds. Apply the offset before checking the microsecond
                // range, since the offset can move midnight across a timestamp range boundary.
                let seconds = i64::from(d) * 86_400 - i64::from(offset_seconds);
                match seconds.checked_mul(1_000_000) {
                    Some(micros) => builder.append_value(micros),
                    None if cast_options.eval_mode == EvalMode::Try => builder.append_null(),
                    // Spark's daysToMicros uses Math.multiplyExact even in non-ANSI mode.
                    None => return Err(SparkError::LongOverflow),
                }
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(
        builder.finish().with_timezone_opt(target_tz.clone()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn test_cast_date_to_timestamp() {
        use crate::EvalMode;
        use arrow::array::Date32Array;
        use arrow::array::{Array, ArrayRef};
        use arrow::datatypes::TimestampMicrosecondType;

        // Region-zone and DST behavior is covered by Spark's codegen fallback tests.
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(19723),
            Some(19793),
            None,
        ]));

        let non_dst_date = 1704067200000000i64;
        let dst_date = 1710115200000000i64;
        let seven_hours_ts = 25200000000i64;
        let eight_hours_ts = 28800000000i64;

        // validate UTC
        let target_tz: Option<Arc<str>> = Some("UTC".into());
        let result = cast_date_to_timestamp(
            &dates,
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
            &target_tz,
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), 0);
        assert_eq!(ts.value(1), non_dst_date);
        assert_eq!(ts.value(2), dst_date);
        assert!(ts.is_null(3));

        // Negative fixed offset
        let result = cast_date_to_timestamp(
            &dates,
            &SparkCastOptions::new(EvalMode::Legacy, "-08:00", false),
            &target_tz,
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), eight_hours_ts);
        assert_eq!(ts.value(1), non_dst_date + eight_hours_ts);
        assert_eq!(ts.value(2), dst_date + eight_hours_ts);
        assert!(ts.is_null(3));

        // A different fixed offset
        let result = cast_date_to_timestamp(
            &dates,
            &SparkCastOptions::new(EvalMode::Legacy, "-07:00", false),
            &target_tz,
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), seven_hours_ts);
        assert_eq!(ts.value(1), non_dst_date + seven_hours_ts);
        assert_eq!(ts.value(2), dst_date + seven_hours_ts);
        assert!(ts.is_null(3));
    }

    #[test]
    fn test_cast_wide_dates_to_timestamp() {
        use arrow::array::{Array, Date32Array};
        use arrow::datatypes::TimestampMicrosecondType;

        // +262143-01-01, both valid UTC-midnight timestamp boundaries, and null.
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(95_026_237),
            Some(106_751_991),
            Some(-106_751_991),
            None,
        ]));
        for target_tz in [None, Some("UTC".into())] {
            let result = cast_date_to_timestamp(
                &dates,
                &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
                &target_tz,
            )
            .unwrap();
            let ts = result.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(ts.value(0), 8_210_266_876_800_000_000);
            assert_eq!(ts.value(1), 9_223_372_022_400_000_000);
            assert_eq!(ts.value(2), -9_223_372_022_400_000_000);
            assert!(ts.is_null(3));
            assert_eq!(ts.timezone(), target_tz.as_deref());
        }

        let result = cast_date_to_timestamp(
            &dates,
            &SparkCastOptions::new(EvalMode::Legacy, "+01:00", false),
            &Some("UTC".into()),
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), 8_210_266_873_200_000_000);
        assert_eq!(ts.value(1), 9_223_372_018_800_000_000);
        assert_eq!(ts.value(2), -9_223_372_026_000_000_000);
        assert!(ts.is_null(3));
    }

    #[test]
    fn test_cast_date_to_timestamp_overflow() {
        use arrow::array::{Array, Date32Array};
        use arrow::datatypes::TimestampMicrosecondType;

        for (timezone, days) in [
            ("UTC", 108_853_388), // +300000-06-15
            ("UTC", 106_751_992),
            ("UTC", -106_751_992),
            ("UTC", i32::MAX),
            ("UTC", i32::MIN),
            ("-18:00", 106_751_991),
            ("+18:00", -106_751_991),
        ] {
            for target_tz in [None, Some("UTC".into())] {
                // NTZ ignores the session offset: these midnight boundaries remain valid.
                if target_tz.is_none() && timezone != "UTC" {
                    continue;
                }
                let dates: ArrayRef =
                    Arc::new(Date32Array::from(vec![Some(0), Some(days), None, Some(1)]));
                for mode in [EvalMode::Legacy, EvalMode::Ansi] {
                    let result = cast_date_to_timestamp(
                        &dates,
                        &SparkCastOptions::new(mode, timezone, false),
                        &target_tz,
                    );
                    assert!(matches!(result, Err(SparkError::LongOverflow)));
                }
                let result = cast_date_to_timestamp(
                    &dates,
                    &SparkCastOptions::new(EvalMode::Try, timezone, false),
                    &target_tz,
                )
                .unwrap();
                let ts = result.as_primitive::<TimestampMicrosecondType>();
                assert!(!ts.is_null(0));
                assert!(ts.is_null(1));
                assert!(ts.is_null(2));
                assert!(!ts.is_null(3));
            }
        }
    }

    #[test]
    fn test_cast_date_to_timestamp_rejects_region_timezone() {
        use arrow::array::Date32Array;

        let dates: ArrayRef = Arc::new(Date32Array::from(vec![95_026_237]));
        let result = cast_date_to_timestamp(
            &dates,
            &SparkCastOptions::new(EvalMode::Legacy, "America/Los_Angeles", false),
            &Some("UTC".into()),
        );
        assert!(result.is_err());
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
                &SparkCastOptions::new(EvalMode::Legacy, tz, false),
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
            // output array has no timezone annotation
            assert_eq!(ts.timezone(), None, "no tz annotation, tz={tz}");
        }
    }
}
