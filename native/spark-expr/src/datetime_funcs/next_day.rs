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

use arrow::array::{Array, Date32Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Date32Type};
use chrono::{Datelike, Duration, Weekday};
use datafusion::common::{utils::take_function_args, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// Spark-compatible `next_day(start_date, day_of_week)` function.
///
/// Returns the first date which is later than `start_date` and named as `day_of_week`. Unlike the
/// upstream `datafusion-spark` implementation, this matches Spark's `DateTimeUtils
/// .getDayOfWeekFromString` exactly: the `day_of_week` argument is *not* trimmed before matching,
/// and when it cannot be parsed the behaviour follows `spark.sql.ansi.enabled` (carried here as
/// `fail_on_error`): throw when ANSI is enabled, otherwise return NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNextDay {
    signature: Signature,
    fail_on_error: bool,
}

impl SparkNextDay {
    pub fn new(fail_on_error: bool) -> Self {
        Self {
            // Accept any 2 args - we cast to Date32 / Utf8 internally.
            signature: Signature::any(2, Volatility::Immutable),
            fail_on_error,
        }
    }
}

impl Default for SparkNextDay {
    fn default() -> Self {
        Self::new(false)
    }
}

/// Longest recognized (upper-cased) day-of-week name is "WEDNESDAY" at 9 bytes.
const MAX_DAY_OF_WEEK_LEN: usize = 9;

/// Match an already upper-cased day-of-week name (as raw bytes) to a [`Weekday`].
#[inline]
fn weekday_from_uppercase(upper: &[u8]) -> Option<Weekday> {
    match upper {
        b"SU" | b"SUN" | b"SUNDAY" => Some(Weekday::Sun),
        b"MO" | b"MON" | b"MONDAY" => Some(Weekday::Mon),
        b"TU" | b"TUE" | b"TUESDAY" => Some(Weekday::Tue),
        b"WE" | b"WED" | b"WEDNESDAY" => Some(Weekday::Wed),
        b"TH" | b"THU" | b"THURSDAY" => Some(Weekday::Thu),
        b"FR" | b"FRI" | b"FRIDAY" => Some(Weekday::Fri),
        b"SA" | b"SAT" | b"SATURDAY" => Some(Weekday::Sat),
        _ => None,
    }
}

/// Match a day-of-week name to a [`Weekday`]. Mirrors Spark's
/// `DateTimeUtils.getDayOfWeekFromString`: case-insensitive, but with no whitespace trimming.
fn day_of_week_from_string(day_of_week: &str) -> Option<Weekday> {
    // Fast path: for ASCII input (the overwhelmingly common case) Unicode
    // upper-casing is identical to ASCII upper-casing, so upper-case into a
    // stack buffer and avoid the per-row heap allocation that `to_uppercase`
    // incurs.
    if day_of_week.is_ascii() {
        let bytes = day_of_week.as_bytes();
        // The buffer only needs to hold the longest recognized name; anything
        // longer cannot match, so reject it rather than overflow the buffer.
        let mut buf = [0u8; MAX_DAY_OF_WEEK_LEN];
        if bytes.len() > buf.len() {
            return None;
        }
        let upper = &mut buf[..bytes.len()];
        upper.copy_from_slice(bytes);
        upper.make_ascii_uppercase();
        return weekday_from_uppercase(upper);
    }
    weekday_from_uppercase(day_of_week.to_uppercase().as_bytes())
}

/// The first date strictly after `days` (days since the Unix epoch) that falls on `weekday`.
/// Equivalent to Spark's `DateTimeUtils.getNextDateForDayOfWeek` (a same-weekday start advances a
/// full week). Returns None only if `days` is not a representable date.
fn next_date_for_day_of_week(days: i32, weekday: Weekday) -> Option<i32> {
    let date = Date32Type::to_naive_date_opt(days)?;
    let advance = 7 - date.weekday().days_since(weekday) as i64;
    Some(Date32Type::from_naive_date(date + Duration::days(advance)))
}

impl ScalarUDFImpl for SparkNextDay {
    fn name(&self) -> &str {
        "next_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        // Spark marks next_day as always nullable because an invalid day_of_week yields NULL
        // (when ANSI is disabled) even for non-null inputs.
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [date, day_of_week] = take_function_args(self.name(), args.args)?;

        let num_rows = [&date, &day_of_week]
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);

        let date_arr = date.into_array(num_rows)?;
        let day_of_week_arr = day_of_week.into_array(num_rows)?;

        let date_arr = cast(date_arr.as_ref(), &DataType::Date32).map_err(|e| {
            DataFusionError::Execution(format!(
                "next_day: failed to cast start date to Date32: {e}"
            ))
        })?;
        let day_of_week_arr = cast(day_of_week_arr.as_ref(), &DataType::Utf8).map_err(|e| {
            DataFusionError::Execution(format!("next_day: failed to cast day of week to Utf8: {e}"))
        })?;

        let date_array = date_arr
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("next_day: failed to cast start date to Date32".into())
            })?;
        let day_of_week_array = day_of_week_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("next_day: failed to cast day of week to Utf8".into())
            })?;

        let len = date_array.len();
        let mut builder = Date32Array::builder(len);

        for i in 0..len {
            if date_array.is_null(i) || day_of_week_array.is_null(i) {
                builder.append_null();
                continue;
            }
            let days = date_array.value(i);
            let day_of_week = day_of_week_array.value(i);
            match day_of_week_from_string(day_of_week) {
                Some(weekday) => match next_date_for_day_of_week(days, weekday) {
                    Some(result) => builder.append_value(result),
                    None => builder.append_null(),
                },
                None => {
                    if self.fail_on_error {
                        return Err(DataFusionError::Execution(format!(
                            "Illegal input for day of week: {day_of_week}"
                        )));
                    }
                    builder.append_null();
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_day_of_week_from_string_no_trim() {
        // Recognised names match case-insensitively.
        assert_eq!(day_of_week_from_string("mon"), Some(Weekday::Mon));
        assert_eq!(day_of_week_from_string("MONDAY"), Some(Weekday::Mon));
        assert_eq!(day_of_week_from_string("Su"), Some(Weekday::Sun));
        // Surrounding whitespace is NOT trimmed (Spark does not trim).
        assert_eq!(day_of_week_from_string(" MO "), None);
        assert_eq!(day_of_week_from_string("MO "), None);
        assert_eq!(day_of_week_from_string(""), None);
        assert_eq!(day_of_week_from_string("NOT_A_DAY"), None);
        // Mixed case is accepted on the ASCII fast path.
        assert_eq!(day_of_week_from_string("wEdNeSdAy"), Some(Weekday::Wed));
        // Inputs longer than any recognized name never match.
        assert_eq!(day_of_week_from_string("SUNDAYSUNDAY"), None);
        // Non-ASCII input falls back to full Unicode upper-casing, matching the
        // reference behavior (e.g. U+017F LONG S upper-cases to 'S').
        assert_eq!(day_of_week_from_string("\u{17f}unday"), Some(Weekday::Sun));
        assert_eq!(day_of_week_from_string("úñîçödé"), None);
    }

    #[test]
    fn test_next_date_for_day_of_week() {
        // 2024-01-01 is a Monday (epoch day 19723). Next Monday is 7 days later.
        let monday =
            Date32Type::from_naive_date(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        let next_mon = next_date_for_day_of_week(monday, Weekday::Mon).unwrap();
        assert_eq!(
            Date32Type::to_naive_date_opt(next_mon).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 8).unwrap()
        );
        // Next Tuesday after a Monday is the following day.
        let next_tue = next_date_for_day_of_week(monday, Weekday::Tue).unwrap();
        assert_eq!(
            Date32Type::to_naive_date_opt(next_tue).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()
        );
    }
}
