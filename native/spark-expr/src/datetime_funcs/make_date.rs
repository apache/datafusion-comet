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
use datafusion::common::{utils::take_function_args, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

use crate::{arithmetic_overflow_error, conversion_funcs::ymd_to_epoch_day, SparkError};

/// Spark-compatible make_date function.
/// Creates a date from year, month, and day columns.
/// For an invalid `(year, month, day)` triple Spark returns NULL when `spark.sql.ansi.enabled` is
/// false, and throws otherwise. The ANSI flag is carried here as `fail_on_error`.
/// Valid calendar dates outside Date32 raise arithmetic overflow in either mode.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeDate {
    signature: Signature,
    fail_on_error: bool,
}

impl SparkMakeDate {
    pub fn new(fail_on_error: bool) -> Self {
        Self {
            // Accept any numeric type - we'll cast to Int32 internally
            signature: Signature::any(3, Volatility::Immutable),
            fail_on_error,
        }
    }
}

impl Default for SparkMakeDate {
    fn default() -> Self {
        Self::new(false)
    }
}

/// Build the error message Spark surfaces for an invalid date under ANSI mode. Spark wraps the
/// `java.time.DateTimeException` raised by `LocalDate.of` (via `ansiDateTimeArgumentOutOfRange` /
/// `ansiDateTimeError`), so we reproduce `java.time`'s messages and validation order: year range,
/// month range, day range, then the day-vs-month check.
fn invalid_date_message(year: i32, month: i32, day: i32) -> String {
    const MONTH_NAMES: [&str; 12] = [
        "JANUARY",
        "FEBRUARY",
        "MARCH",
        "APRIL",
        "MAY",
        "JUNE",
        "JULY",
        "AUGUST",
        "SEPTEMBER",
        "OCTOBER",
        "NOVEMBER",
        "DECEMBER",
    ];
    if !(-999_999_999..=999_999_999).contains(&year) {
        return format!("Invalid value for Year (valid values -999999999 - 999999999): {year}");
    }
    if !(1..=12).contains(&month) {
        return format!("Invalid value for MonthOfYear (valid values 1 - 12): {month}");
    }
    if !(1..=31).contains(&day) {
        return format!("Invalid value for DayOfMonth (valid values 1 - 28/31): {day}");
    }
    if day == 29 && month == 2 {
        return format!("Invalid date 'February 29' as '{year}' is not a leap year");
    }
    format!("Invalid date '{} {day}'", MONTH_NAMES[(month - 1) as usize])
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
/// Invalid Java calendar dates return None; valid dates outside Date32 raise arithmetic overflow.
fn make_date(year: i32, month: i32, day: i32) -> Result<Option<i32>> {
    if !(-999_999_999..=999_999_999).contains(&year) {
        return Ok(None);
    }
    let Some(days) = ymd_to_epoch_day(year.into(), month.into(), day.into()) else {
        return Ok(None);
    };
    i32::try_from(days)
        .map(Some)
        .map_err(|_| arithmetic_overflow_error("integer").into())
}

impl ScalarUDFImpl for SparkMakeDate {
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

        // Scalar arguments must not evaluate a date when the input batch has no rows.
        if args.number_rows == 0 {
            return Ok(ColumnarValue::Array(Arc::new(Date32Array::new_null(0))));
        }

        // Determine the batch size from array arguments (scalars have no inherent size)
        let num_rows = [&year, &month, &day]
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);

        // Convert scalars to arrays for uniform processing, using the correct batch size
        let year_arr = year.into_array(num_rows)?;
        let month_arr = month.into_array(num_rows)?;
        let day_arr = day.into_array(num_rows)?;

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

                match make_date(y, m, d)? {
                    Some(days) => builder.append_value(days),
                    None => {
                        if self.fail_on_error {
                            return Err(SparkError::DatetimeFieldOutOfBounds {
                                range_message: invalid_date_message(y, m, d),
                            }
                            .into());
                        }
                        builder.append_null();
                    }
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::{RecordBatch, RecordBatchOptions};
    use datafusion::common::ScalarValue;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};

    fn invoke_make_date(
        columns: [Vec<Option<i32>>; 3],
        fail_on_error: bool,
    ) -> Result<ColumnarValue> {
        let number_rows = columns[0].len();
        SparkMakeDate::new(fail_on_error).invoke_with_args(ScalarFunctionArgs {
            args: columns
                .into_iter()
                .map(|values| ColumnarValue::Array(Arc::new(Int32Array::from(values))))
                .collect(),
            arg_fields: vec![],
            number_rows,
            return_field: Arc::new(Field::new("make_date", DataType::Date32, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn assert_integer_overflow(error: DataFusionError) {
        let DataFusionError::External(error) = error else {
            panic!("Expected typed SparkError, got {error:?}");
        };
        assert!(matches!(
            error.downcast_ref::<SparkError>(),
            Some(SparkError::ArithmeticOverflow { from_type, function_name })
                if from_type == "integer" && function_name.is_empty()
        ));
    }

    #[test]
    fn valid_calendar_dates_outside_date32_overflow_in_both_modes() {
        for [year, month, day] in [
            [5_881_580, 7, 12],
            [-5_877_641, 6, 22],
            [6_000_000, 1, 1],
            [-6_000_000, 1, 1],
            [999_999_999, 1, 1],
            [-999_999_999, 1, 1],
        ] {
            for fail_on_error in [false, true] {
                let columns = [vec![Some(year)], vec![Some(month)], vec![Some(day)]];
                assert_integer_overflow(invoke_make_date(columns, fail_on_error).unwrap_err());
            }
        }
    }

    #[test]
    fn invalid_calendar_fields_take_precedence_over_date32_overflow() {
        for ([year, month, day], expected) in [
            (
                [1_000_000_000, 13, 32],
                "Invalid value for Year (valid values -999999999 - 999999999): 1000000000",
            ),
            (
                [-1_000_000_000, 1, 1],
                "Invalid value for Year (valid values -999999999 - 999999999): -1000000000",
            ),
            (
                [6_000_000, 13, 32],
                "Invalid value for MonthOfYear (valid values 1 - 12): 13",
            ),
            ([6_000_000, 2, 30], "Invalid date 'FEBRUARY 30'"),
        ] {
            let columns = [vec![Some(year)], vec![Some(month)], vec![Some(day)]];
            let result = invoke_make_date(columns.clone(), false)
                .unwrap()
                .into_array(1)
                .unwrap();
            assert!(result.is_null(0));

            let error = invoke_make_date(columns, true).unwrap_err();
            let DataFusionError::External(error) = error else {
                panic!("Expected typed SparkError, got {error:?}");
            };
            assert!(matches!(
                error.downcast_ref::<SparkError>(),
                Some(SparkError::DatetimeFieldOutOfBounds { range_message })
                    if range_message == expected
            ));
        }
    }

    #[test]
    fn null_arguments_skip_date_validation() {
        let columns = [
            vec![None, Some(6_000_000), Some(6_000_000), Some(1970)],
            vec![Some(13), None, Some(1), Some(1)],
            vec![Some(32), Some(1), None, Some(1)],
        ];
        for fail_on_error in [false, true] {
            let result = invoke_make_date(columns.clone(), fail_on_error)
                .unwrap()
                .into_array(4)
                .unwrap();
            assert_eq!(
                result.as_ref(),
                &Date32Array::from(vec![None, None, None, Some(0)])
            );
        }
    }

    fn evaluate_scalar_make_date(
        values: [Option<i32>; 3],
        number_rows: usize,
        fail_on_error: bool,
    ) -> Result<ColumnarValue> {
        let args = values
            .into_iter()
            .map(|value| Arc::new(Literal::new(ScalarValue::Int32(value))) as Arc<dyn PhysicalExpr>)
            .collect();
        let expression = ScalarFunctionExpr::new(
            "make_date",
            Arc::new(ScalarUDF::new_from_impl(SparkMakeDate::new(fail_on_error))),
            args,
            Arc::new(Field::new("make_date", DataType::Date32, true)),
            Arc::new(ConfigOptions::default()),
        );
        // A batch without columns may still represent a real input row.
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(number_rows)),
        )?;
        expression.evaluate(&batch)
    }

    #[test]
    fn scalar_arguments_only_validate_dates_for_nonempty_batches() {
        for fail_on_error in [false, true] {
            for values in [
                [Some(6_000_000), Some(1), Some(1)],
                [Some(2024), Some(13), Some(1)],
            ] {
                let result = evaluate_scalar_make_date(values, 0, fail_on_error)
                    .unwrap()
                    .into_array(0)
                    .unwrap();
                assert_eq!(result.data_type(), &DataType::Date32);
                assert!(result.is_empty());
            }

            for number_rows in [1, 3] {
                assert_integer_overflow(
                    evaluate_scalar_make_date(
                        [Some(6_000_000), Some(1), Some(1)],
                        number_rows,
                        fail_on_error,
                    )
                    .unwrap_err(),
                );
                for (values, expected) in [
                    ([Some(1970), Some(1), Some(1)], Some(0)),
                    ([Some(6_000_000), None, Some(1)], None),
                ] {
                    let result = evaluate_scalar_make_date(values, number_rows, fail_on_error)
                        .unwrap()
                        .into_array(number_rows)
                        .unwrap();
                    assert_eq!(
                        result.as_ref(),
                        &Date32Array::from(vec![expected; number_rows])
                    );
                }
            }
        }
    }

    #[test]
    fn test_make_date_valid() {
        // Unix epoch
        assert_eq!(make_date(1970, 1, 1).unwrap(), Some(0));
        // Day after epoch
        assert_eq!(make_date(1970, 1, 2).unwrap(), Some(1));
        // Day before epoch
        assert_eq!(make_date(1969, 12, 31).unwrap(), Some(-1));
        // Leap years - just verify they return Some (valid dates)
        assert!(make_date(2000, 2, 29).unwrap().is_some()); // 2000 is a leap year
        assert!(make_date(2004, 2, 29).unwrap().is_some()); // 2004 is a leap year
                                                            // Regular date
        assert!(make_date(2023, 6, 15).unwrap().is_some());
    }

    #[test]
    fn test_make_date_invalid_month() {
        assert_eq!(make_date(2023, 0, 15).unwrap(), None);
        assert_eq!(make_date(2023, 13, 15).unwrap(), None);
        assert_eq!(make_date(2023, -1, 15).unwrap(), None);
    }

    #[test]
    fn test_make_date_invalid_day() {
        assert_eq!(make_date(2023, 6, 0).unwrap(), None);
        assert_eq!(make_date(2023, 6, 32).unwrap(), None);
        assert_eq!(make_date(2023, 6, -1).unwrap(), None);
    }

    #[test]
    fn test_make_date_invalid_dates() {
        // Feb 30 never exists
        assert_eq!(make_date(2023, 2, 30).unwrap(), None);
        // Feb 29 on non-leap year
        assert_eq!(make_date(2023, 2, 29).unwrap(), None);
        // 1900 is not a leap year (divisible by 100 but not 400)
        assert_eq!(make_date(1900, 2, 29).unwrap(), None);
        // 2100 will not be a leap year
        assert_eq!(make_date(2100, 2, 29).unwrap(), None);
        // April has 30 days
        assert_eq!(make_date(2023, 4, 31).unwrap(), None);
    }

    #[test]
    fn test_make_date_wide_year_range() {
        assert_eq!(make_date(0, 1, 1).unwrap(), Some(-719_528));
        assert_eq!(make_date(-1, 1, 1).unwrap(), Some(-719_893));
        assert_eq!(make_date(300_000, 6, 15).unwrap(), Some(108_853_388));
        assert_eq!(make_date(300_000, 2, 29).unwrap(), Some(108_853_281));

        assert_eq!(make_date(5_881_580, 7, 11).unwrap(), Some(i32::MAX));
        assert_eq!(make_date(-5_877_641, 6, 23).unwrap(), Some(i32::MIN));
    }
}
