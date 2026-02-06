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
use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate, Weekday};
use datafusion::common::{utils::take_function_args, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible next_day function.
/// Returns the first date after the given start date that falls on the specified day of week.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNextDay {
    signature: Signature,
}

impl SparkNextDay {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Date32, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for SparkNextDay {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse day of week string to chrono Weekday.
/// Supports full names (case-insensitive) and common abbreviations.
/// Returns None for invalid day names.
fn parse_day_of_week(day_str: &str) -> Option<Weekday> {
    let day_lower = day_str.trim().to_lowercase();
    match day_lower.as_str() {
        "sunday" | "sun" | "su" => Some(Weekday::Sun),
        "monday" | "mon" | "mo" => Some(Weekday::Mon),
        "tuesday" | "tue" | "tu" => Some(Weekday::Tue),
        "wednesday" | "wed" | "we" => Some(Weekday::Wed),
        "thursday" | "thu" | "th" => Some(Weekday::Thu),
        "friday" | "fri" | "fr" => Some(Weekday::Fri),
        "saturday" | "sat" | "sa" => Some(Weekday::Sat),
        _ => None,
    }
}

/// Calculate the next date that falls on the target day of week.
/// The result is always after the start date (never the same day).
fn next_day_from_date(date: NaiveDate, target_day: Weekday) -> NaiveDate {
    let current_day = date.weekday();
    let current_day_num = current_day.num_days_from_sunday(); // 0 = Sunday
    let target_day_num = target_day.num_days_from_sunday();

    // Calculate days to add (always at least 1)
    let days_to_add = if target_day_num > current_day_num {
        target_day_num - current_day_num
    } else {
        // target_day_num <= current_day_num, so we need to go to next week
        7 - current_day_num + target_day_num
    };

    date + chrono::Duration::days(days_to_add as i64)
}

/// Convert Date32 (days since epoch) to NaiveDate
fn date32_to_naive_date(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days as i64)))
}

/// Convert NaiveDate to Date32 (days since epoch)
fn naive_date_to_date32(date: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (date - epoch).num_days() as i32
}

impl ScalarUDFImpl for SparkNextDay {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "next_day"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [date_arg, day_arg] = take_function_args(self.name(), args.args)?;

        match (date_arg, day_arg) {
            // Array date, scalar day of week
            (
                ColumnarValue::Array(date_arr),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(day_str))),
            ) => {
                let date_array =
                    date_arr
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("next_day expects Date32 array".to_string())
                        })?;

                let target_day = parse_day_of_week(&day_str);

                let result: Date32Array = date_array
                    .iter()
                    .map(|opt_date| {
                        match (opt_date, &target_day) {
                            (Some(days), Some(day)) => date32_to_naive_date(days)
                                .map(|date| naive_date_to_date32(next_day_from_date(date, *day))),
                            _ => None, // null date or invalid day name returns null
                        }
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            // Array date, array day of week
            (ColumnarValue::Array(date_arr), ColumnarValue::Array(day_arr)) => {
                let date_array =
                    date_arr
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("next_day expects Date32 array".to_string())
                        })?;
                let day_array =
                    day_arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "next_day expects String array for day of week".to_string(),
                            )
                        })?;

                let result: Date32Array = date_array
                    .iter()
                    .zip(day_array.iter())
                    .map(|(opt_date, opt_day)| {
                        match (opt_date, opt_day) {
                            (Some(days), Some(day_str)) => {
                                let target_day = parse_day_of_week(day_str);
                                match target_day {
                                    Some(day) => date32_to_naive_date(days).map(|date| {
                                        naive_date_to_date32(next_day_from_date(date, day))
                                    }),
                                    None => None, // Invalid day name returns null
                                }
                            }
                            _ => None,
                        }
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            // Scalar date, scalar day of week
            (
                ColumnarValue::Scalar(ScalarValue::Date32(opt_days)),
                ColumnarValue::Scalar(ScalarValue::Utf8(opt_day_str)),
            ) => {
                let result = match (opt_days, opt_day_str) {
                    (Some(days), Some(day_str)) => {
                        let target_day = parse_day_of_week(&day_str);
                        match target_day {
                            Some(day) => date32_to_naive_date(days)
                                .map(|date| naive_date_to_date32(next_day_from_date(date, day))),
                            None => None,
                        }
                    }
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(result)))
            }
            // Handle null day of week
            (date_arg, ColumnarValue::Scalar(ScalarValue::Utf8(None))) => {
                let arr = date_arg.into_array(1)?;
                let null_result: Date32Array = (0..arr.len()).map(|_| None::<i32>).collect();
                Ok(ColumnarValue::Array(Arc::new(null_result)))
            }
            _ => Err(DataFusionError::Execution(
                "next_day expects (Date32, Utf8) arguments".to_string(),
            )),
        }
    }
}
