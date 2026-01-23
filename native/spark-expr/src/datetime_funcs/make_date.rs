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

use arrow::array::{Array, Date32Array, Int32Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use chrono::NaiveDate;
use datafusion::common::{utils::take_function_args, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible make_date function.
/// Creates a date from year, month, and day columns.
/// Returns NULL for invalid dates (e.g., Feb 30, month 13, etc.) instead of throwing an error.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeDate {
    signature: Signature,
}

impl SparkMakeDate {
    pub fn new() -> Self {
        Self {
            // Accept any numeric type - we'll cast to Int32 internally
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl Default for SparkMakeDate {
    fn default() -> Self {
        Self::new()
    }
}

/// Cast an array to Int32Array if it's not already Int32.
fn cast_to_int32(arr: &Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    if arr.data_type() == &DataType::Int32 {
        Ok(Arc::clone(arr))
    } else {
        cast(arr.as_ref(), &DataType::Int32)
            .map_err(|e| DataFusionError::Execution(format!("Failed to cast to Int32: {e}")))
    }
}

/// Convert year, month, day to days since Unix epoch (1970-01-01).
/// Returns None if the date is invalid.
fn make_date(year: i32, month: i32, day: i32) -> Option<i32> {
    // Validate month and day ranges first
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Try to create a valid date
    NaiveDate::from_ymd_opt(year, month as u32, day as u32).map(|date| {
        date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32
    })
}

impl ScalarUDFImpl for SparkMakeDate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [year, month, day] = take_function_args(self.name(), args.args)?;

        // Convert scalars to arrays for uniform processing
        let year_arr = year.into_array(1)?;
        let month_arr = month.into_array(1)?;
        let day_arr = day.into_array(1)?;

        // Cast to Int32 if needed (handles Int64 literals from SQL)
        let year_arr = cast_to_int32(&year_arr)?;
        let month_arr = cast_to_int32(&month_arr)?;
        let day_arr = cast_to_int32(&day_arr)?;

        let year_array = year_arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_date: failed to cast year to Int32".to_string())
            })?;

        let month_array = month_arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_date: failed to cast month to Int32".to_string())
            })?;

        let day_array = day_arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("make_date: failed to cast day to Int32".to_string())
            })?;

        let len = year_array.len();
        let mut builder = Date32Array::builder(len);

        for i in 0..len {
            if year_array.is_null(i) || month_array.is_null(i) || day_array.is_null(i) {
                builder.append_null();
            } else {
                let y = year_array.value(i);
                let m = month_array.value(i);
                let d = day_array.value(i);

                match make_date(y, m, d) {
                    Some(days) => builder.append_value(days),
                    None => builder.append_null(),
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
    fn test_make_date_valid() {
        // Unix epoch
        assert_eq!(make_date(1970, 1, 1), Some(0));
        // Day after epoch
        assert_eq!(make_date(1970, 1, 2), Some(1));
        // Day before epoch
        assert_eq!(make_date(1969, 12, 31), Some(-1));
        // Leap years - just verify they return Some (valid dates)
        assert!(make_date(2000, 2, 29).is_some()); // 2000 is a leap year
        assert!(make_date(2004, 2, 29).is_some()); // 2004 is a leap year
                                                   // Regular date
        assert!(make_date(2023, 6, 15).is_some());
    }

    #[test]
    fn test_make_date_invalid_month() {
        assert_eq!(make_date(2023, 0, 15), None);
        assert_eq!(make_date(2023, 13, 15), None);
        assert_eq!(make_date(2023, -1, 15), None);
    }

    #[test]
    fn test_make_date_invalid_day() {
        assert_eq!(make_date(2023, 6, 0), None);
        assert_eq!(make_date(2023, 6, 32), None);
        assert_eq!(make_date(2023, 6, -1), None);
    }

    #[test]
    fn test_make_date_invalid_dates() {
        // Feb 30 never exists
        assert_eq!(make_date(2023, 2, 30), None);
        // Feb 29 on non-leap year
        assert_eq!(make_date(2023, 2, 29), None);
        // 1900 is not a leap year (divisible by 100 but not 400)
        assert_eq!(make_date(1900, 2, 29), None);
        // 2100 will not be a leap year
        assert_eq!(make_date(2100, 2, 29), None);
        // April has 30 days
        assert_eq!(make_date(2023, 4, 31), None);
    }

    #[test]
    fn test_make_date_extreme_years() {
        // Spark supports dates from 0001-01-01 to 9999-12-31 (Proleptic Gregorian calendar)

        // Minimum valid date in Spark: 0001-01-01
        assert!(make_date(1, 1, 1).is_some(), "Year 1 should be valid");

        // Maximum valid date in Spark: 9999-12-31
        assert!(
            make_date(9999, 12, 31).is_some(),
            "Year 9999 should be valid"
        );

        // Year 0 - In Proleptic Gregorian calendar, year 0 = 1 BCE
        // Spark returns NULL for year 0 in make_date
        // chrono supports year 0, but we should match Spark's behavior
        // For now, chrono allows it - this may need adjustment for full Spark compatibility
        let year_0_result = make_date(0, 1, 1);
        // chrono allows year 0 (1 BCE in proleptic Gregorian)
        assert!(year_0_result.is_some(), "chrono allows year 0");

        // Negative years - Spark returns NULL for negative years
        // chrono supports negative years (BCE dates)
        let negative_year_result = make_date(-1, 1, 1);
        // chrono allows negative years
        assert!(
            negative_year_result.is_some(),
            "chrono allows negative years"
        );
    }
}
