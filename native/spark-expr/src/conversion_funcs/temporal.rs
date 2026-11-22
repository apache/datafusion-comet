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
use arrow::array::{ArrayRef, AsArray, Date32Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Date32Type};
use std::sync::Arc;

const DAYS_PER_400_YEARS: i64 = 146_097;

/// JVM offsets for converting each local date to the instant returned by
/// `LocalDate.atStartOfDay(zone)`. Subtracting the effective offset from midnight
/// UTC yields that instant even when a gap spans midnight or skips a whole date.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct DateOffsets {
    repeat_from_day: i32,
    starts: Vec<i32>,
    offsets: Vec<i32>,
}

impl DateOffsets {
    /// Each offset in seconds applies from its inclusive start day until the next
    /// start. Recurring rules repeat every 400 Gregorian years from `repeat_from_day`.
    /// Zones without recurring rules use `i32::MAX` as `repeat_from_day`.
    pub fn try_new(repeat_from_day: i32, starts: Vec<i32>, offsets: Vec<i32>) -> SparkResult<Self> {
        let cycle_end = i64::from(repeat_from_day) + DAYS_PER_400_YEARS;
        if starts.first() != Some(&i32::MIN)
            || starts.len() != offsets.len()
            || !starts.is_sorted_by(|a, b| a < b)
            || starts
                .last()
                .is_some_and(|&day| i64::from(day) >= cycle_end)
            || offsets.iter().any(|s| !(-64_800..=64_800).contains(s))
        {
            return Err(SparkError::Internal(
                "Invalid start-of-day offsets for DATE to TIMESTAMP".to_string(),
            ));
        }
        Ok(Self {
            repeat_from_day,
            starts,
            offsets,
        })
    }

    fn offset(&self, day: i32) -> i32 {
        let elapsed = i64::from(day) - i64::from(self.repeat_from_day);
        let day = if elapsed >= DAYS_PER_400_YEARS {
            self.repeat_from_day + (elapsed % DAYS_PER_400_YEARS) as i32
        } else {
            day
        };
        self.offsets[self.starts.partition_point(|&start| start <= day) - 1]
    }
}

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
    let dates = array_ref.as_primitive::<Date32Type>();
    let mode = cast_options.eval_mode;
    let timestamps = if target_tz.is_none() || cast_options.timezone == "UTC" {
        dates_to_micros(dates, mode, |_| 0)
    } else if let Some(offsets) = &cast_options.date_offsets {
        dates_to_micros(dates, mode, |day| offsets.offset(day))
    } else {
        let offset = parse_sign_offset(&cast_options.timezone).ok_or_else(|| {
            SparkError::Internal(format!(
                "Missing start-of-day offsets for DATE to TIMESTAMP in {}",
                cast_options.timezone
            ))
        })?;
        dates_to_micros(dates, mode, |_| offset)
    }?;
    Ok(timestamp_micros_with_tz(timestamps, target_tz))
}

fn dates_to_micros(
    dates: &Date32Array,
    mode: EvalMode,
    offset: impl Fn(i32) -> i32,
) -> SparkResult<TimestampMicrosecondArray> {
    // Every Date32 fits in seconds. Spark applies the offset before multiplying
    // by micros per second, so an offset can move midnight across the i64 boundary.
    let to_micros = |day| {
        let seconds = i64::from(day) * 86_400 - i64::from(offset(day));
        seconds.checked_mul(MICROS_PER_SECOND)
    };
    if mode == EvalMode::Try {
        Ok(dates.unary_opt(to_micros))
    } else {
        dates
            .try_unary(|day| to_micros(day).ok_or(()))
            .map_err(|()| SparkError::LongOverflow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;

    #[test]
    fn test_date_offsets() {
        // The first day of the recurring rules inherits the preceding offset.
        let offsets =
            DateOffsets::try_new(1, vec![i32::MIN, -1, 4, 146_097], vec![123, 3600, 7200, 0])
                .unwrap();
        assert_eq!(offsets.offset(i32::MIN), 123);
        assert_eq!(offsets.offset(-2), 123);
        assert_eq!(offsets.offset(-1), 3600);
        for cycle_start in [1, 1 + 146_097, 1 + 700 * 146_097] {
            assert_eq!(offsets.offset(cycle_start), 3600);
            assert_eq!(offsets.offset(cycle_start + 2), 3600);
            assert_eq!(offsets.offset(cycle_start + 3), 7200);
            assert_eq!(offsets.offset(cycle_start + 146_096), 0);
        }
    }

    #[test]
    fn test_invalid_date_offsets() {
        for (starts, offsets) in [
            (vec![], vec![]),
            (vec![0], vec![0]),
            (vec![i32::MIN], vec![]),
            (vec![i32::MIN, 1, 0], vec![0, 1, 2]),
            (vec![i32::MIN, 146_097], vec![0, 1]),
            (vec![i32::MIN], vec![-64_801]),
        ] {
            assert!(DateOffsets::try_new(0, starts, offsets).is_err());
        }
    }

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
