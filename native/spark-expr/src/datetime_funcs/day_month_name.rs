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

//! Spark-compatible `dayname` and `monthname` (Spark 4.0+).
//!
//! Spark's `DayName` / `MonthName` call `DateTimeUtils.getDayName` / `getMonthName`, which map a
//! `DateType` value through `DayOfWeek` / `Month` `.getDisplayName(TextStyle.SHORT, Locale.US)`.
//! `DateFormatter.defaultLocale` is the constant `Locale.US`, so the output is a fixed set of
//! abbreviated English names, independent of the session locale or timezone. We reproduce that
//! exactly with the lookup tables below, computing the weekday / month from the Date32 value.

use arrow::array::{Array, Date32Array, StringArray};
use arrow::temporal_conversions::date32_to_datetime;
use chrono::Datelike;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

// `DayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US)`, indexed Monday-first to match
// `chrono::Weekday::num_days_from_monday` (0 = Monday) and Spark's `DayOfWeek.ordinal()`.
const DAY_NAMES: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

// `Month.getDisplayName(TextStyle.SHORT, Locale.US)`, indexed by `month0` (0 = January).
const MONTH_NAMES: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

fn day_name(days: i32) -> Option<&'static str> {
    date32_to_datetime(days)
        .map(|dt| DAY_NAMES[dt.date().weekday().num_days_from_monday() as usize])
}

fn month_name(days: i32) -> Option<&'static str> {
    date32_to_datetime(days).map(|dt| MONTH_NAMES[dt.date().month0() as usize])
}

/// Spark-compatible `dayname(date)`.
pub fn spark_day_name(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    day_month_name(args, "dayname", day_name)
}

/// Spark-compatible `monthname(date)`.
pub fn spark_month_name(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    day_month_name(args, "monthname", month_name)
}

fn day_month_name(
    args: &[ColumnarValue],
    name: &str,
    f: fn(i32) -> Option<&'static str>,
) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return Err(DataFusionError::Execution(format!(
            "{name} expects exactly one argument, got {}",
            args.len()
        )));
    }
    match &args[0] {
        ColumnarValue::Array(array) => {
            let dates = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{name} expects a Date32 argument, got {:?}",
                        array.data_type()
                    ))
                })?;
            let result: StringArray = dates.iter().map(|d| d.and_then(f)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        ColumnarValue::Scalar(ScalarValue::Date32(days)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Utf8(days.and_then(f).map(|s| s.to_string())),
        )),
        ColumnarValue::Scalar(ScalarValue::Null) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        other => Err(DataFusionError::Execution(format!(
            "{name} expects a Date32 argument, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 2024-01-15 is a Monday; 2024-06-30 is a Sunday; 2020-12-31 is a Thursday.
    // Days since epoch: 2024-01-15 = 19737, 2024-06-30 = 19904, 2020-12-31 = 18627.
    #[test]
    fn day_names() {
        assert_eq!(day_name(19737), Some("Mon"));
        assert_eq!(day_name(19904), Some("Sun"));
        assert_eq!(day_name(18627), Some("Thu"));
        assert_eq!(day_name(0), Some("Thu")); // 1970-01-01 is a Thursday
        assert_eq!(day_name(-1), Some("Wed")); // 1969-12-31
    }

    #[test]
    fn month_names() {
        assert_eq!(month_name(19737), Some("Jan"));
        assert_eq!(month_name(19904), Some("Jun"));
        assert_eq!(month_name(18627), Some("Dec"));
        assert_eq!(month_name(0), Some("Jan"));
    }
}
