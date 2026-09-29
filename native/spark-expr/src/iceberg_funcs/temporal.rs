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

//! Iceberg's `years`, `months`, `days`, and `hours` transforms.
//!
//! Iceberg's `DateTimeUtil` evaluates all four in UTC regardless of the Spark session timezone
//! (`TimestampType` and `TimestampNTZType` are handled identically), and all four floor: a value
//! before the epoch maps to a negative period. `years` and `months` are calendar-aware, `days` and
//! `hours` are plain floor division of the epoch value. `days` returns a date (Iceberg's
//! `DaysFunction.resultType()` is `DateType`), the other three return an int.
//!
//! The kernels read the raw epoch values instead of Arrow's timezone-aware `date_part`. That is a
//! defensive choice rather than a fix for an observed bug: Comet tags every `TimestampType` array
//! `UTC` and the Iceberg writer casts each batch to a schema that tags `Timestamptz` as `+00:00`,
//! so `date_part` would agree today. It only keeps agreeing for as long as that tagging holds,
//! whereas the epoch arithmetic below is correct for any tag.
//!
//! The calendar split is integer arithmetic rather than a `chrono::NaiveDate`, which covers only
//! about ±262k years. A Spark `DateType` is an `i32` epoch day (up to year 5881580) and Java's
//! `LocalDate`, which Iceberg uses, covers all of it, so going through `chrono` would turn values
//! the JVM handles into execution errors.

use super::{apply_unary, unsupported_type};
use arrow::array::{ArrayRef, AsArray, Int32Array};
use arrow::datatypes::{DataType, Date32Type, Int32Type, TimeUnit, TimestampMicrosecondType};
use datafusion::common::{utils::take_function_args, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::integer::div_floor;
use std::sync::Arc;

const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const UNIX_EPOCH_YEAR: i32 = 1970;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TemporalUnit {
    Years,
    Months,
    Days,
    Hours,
}

impl TemporalUnit {
    fn fn_name(self) -> &'static str {
        match self {
            TemporalUnit::Years => "iceberg_years",
            TemporalUnit::Months => "iceberg_months",
            TemporalUnit::Days => "iceberg_days",
            TemporalUnit::Hours => "iceberg_hours",
        }
    }

    fn return_type(self) -> DataType {
        match self {
            TemporalUnit::Days => DataType::Date32,
            _ => DataType::Int32,
        }
    }
}

/// `DateTimeUtil.microsToDays`: floor division, so `-1` micros is day `-1`. The quotient of the
/// widest `i64` micros is about 1.07e8, so the narrowing is always exact here.
#[inline]
fn micros_to_days(micros: i64) -> i32 {
    div_floor(micros, MICROS_PER_DAY) as i32
}

/// `DateTimeUtil.microsToHours`. Java narrows the hour count with a plain `(int)` cast, which
/// wraps beyond about 7.7e18 micros; `as i32` truncates the same way.
#[inline]
fn micros_to_hours(micros: i64) -> i32 {
    div_floor(micros, MICROS_PER_HOUR) as i32
}

/// `DateTimeUtil.daysToYears`: whole calendar years between the epoch and the day, floored.
fn days_to_years(days: i32) -> i32 {
    civil_from_days(days).0 - UNIX_EPOCH_YEAR
}

/// `DateTimeUtil.daysToMonths`: whole calendar months between the epoch and the day, floored.
fn days_to_months(days: i32) -> i32 {
    let (year, month0) = civil_from_days(days);
    (year - UNIX_EPOCH_YEAR) * 12 + month0
}

/// Splits an epoch day into its proleptic Gregorian `(year, month0)`, following Howard Hinnant's
/// `civil_from_days`. The intermediates are `i64` so that every `i32` epoch day is in range, which
/// is what `LocalDate` gives Iceberg; the widest results, at `i32::MIN` and `i32::MAX` days, are
/// years -5877641 and 5881580, so both the year and the month count still fit in an `i32`.
fn civil_from_days(days: i32) -> (i32, i32) {
    // Shift the epoch to 0000-03-01 so that the leap day falls at the end of the year.
    let shifted = days as i64 + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097); // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365; // [0, 399]
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100); // [0, 365]
    let shifted_month = (5 * day_of_year + 2) / 153; // [0, 11], March is 0
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = era * 400 + year_of_era + i64::from(month <= 2);
    (year as i32, (month - 1) as i32)
}

/// Applies a calendar function of the epoch day to a date or timestamp column in one pass.
fn map_epoch_days(fn_name: &str, array: &ArrayRef, f: impl Fn(i32) -> i32) -> Result<Int32Array> {
    match array.data_type() {
        DataType::Date32 => Ok(array.as_primitive::<Date32Type>().unary(f)),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(array
            .as_primitive::<TimestampMicrosecondType>()
            .unary(|micros| f(micros_to_days(micros)))),
        other => Err(unsupported_type(fn_name, other)),
    }
}

fn transform_array(unit: TemporalUnit, array: &ArrayRef) -> Result<ArrayRef> {
    let fn_name = unit.fn_name();
    let result: ArrayRef = match unit {
        TemporalUnit::Years => Arc::new(map_epoch_days(fn_name, array, days_to_years)?),
        TemporalUnit::Months => Arc::new(map_epoch_days(fn_name, array, days_to_months)?),
        TemporalUnit::Days => match array.data_type() {
            DataType::Date32 => Arc::clone(array),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .unary::<_, Date32Type>(micros_to_days),
            ),
            other => return Err(unsupported_type(fn_name, other)),
        },
        TemporalUnit::Hours => match array.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => Arc::new(
                array
                    .as_primitive::<TimestampMicrosecondType>()
                    .unary::<_, Int32Type>(micros_to_hours),
            ),
            other => return Err(unsupported_type(fn_name, other)),
        },
    };
    Ok(result)
}

/// `iceberg_years(value)`, `iceberg_months(value)`, `iceberg_days(value)`, and
/// `iceberg_hours(value)`; see the module docs for the semantics.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIcebergTemporalTransform {
    unit: TemporalUnit,
    signature: Signature,
}

impl SparkIcebergTemporalTransform {
    pub(crate) fn new(unit: TemporalUnit) -> Self {
        Self {
            unit,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    pub fn years() -> Self {
        Self::new(TemporalUnit::Years)
    }

    pub fn months() -> Self {
        Self::new(TemporalUnit::Months)
    }

    pub fn days() -> Self {
        Self::new(TemporalUnit::Days)
    }

    pub fn hours() -> Self {
        Self::new(TemporalUnit::Hours)
    }
}

impl ScalarUDFImpl for SparkIcebergTemporalTransform {
    fn name(&self) -> &str {
        self.unit.fn_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.unit.return_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = take_function_args(self.name(), &args.args)?;
        apply_unary(value, |array| transform_array(self.unit, array))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::invoke;
    use super::*;
    use arrow::array::{Array, Date32Array, TimestampMicrosecondArray};

    fn transform(unit: TemporalUnit, value: ArrayRef) -> ArrayRef {
        invoke(
            &SparkIcebergTemporalTransform::new(unit),
            vec![ColumnarValue::Array(value)],
        )
        .unwrap()
    }

    // Boundaries around the epoch, as (epoch days, years, months). The values past the epoch
    // block are outside `chrono::NaiveDate`'s range but well inside `LocalDate`'s; they come from
    // running Iceberg's `DateTimeUtil.convertDays` on a JDK 17 JVM.
    const DAY_CASES: &[(i32, i32, i32)] = &[
        (17_486, 47, 574),                         // 2017-11-16, the Iceberg spec example
        (0, 0, 0),                                 // 1970-01-01
        (-1, -1, -1),                              // 1969-12-31
        (-365, -1, -12),                           // 1969-01-01
        (-366, -2, -13),                           // 1968-12-31
        (365, 1, 12),                              // 1971-01-01
        (364, 0, 11),                              // 1970-12-31
        (31, 0, 1),                                // 1970-02-01
        (30, 0, 0),                                // 1970-01-31
        (100_000_000, 273_790, 3_285_488),         // +275760-09-13
        (-100_000_000, -273_791, -3_285_489),      // -271821-04-20
        (1_000_000_000, 2_737_907, 32_854_884),    // +2739877-01-03
        (-1_000_000_000, -2_737_908, -32_854_885), // -2735938-12-29
        (i32::MAX, 5_879_610, 70_555_326),         // +5881580-07-11
        (i32::MIN, -5_879_611, -70_555_327),       // -5877641-06-23
    ];

    #[test]
    fn dates_match_iceberg_date_time_util() {
        let days: Vec<Option<i32>> = DAY_CASES.iter().map(|c| Some(c.0)).chain([None]).collect();
        let input: ArrayRef = Arc::new(Date32Array::from(days.clone()));

        let years = transform(TemporalUnit::Years, Arc::clone(&input));
        let months = transform(TemporalUnit::Months, Arc::clone(&input));
        let dates = transform(TemporalUnit::Days, Arc::clone(&input));
        for (i, (_, y, m)) in DAY_CASES.iter().enumerate() {
            assert_eq!(
                years.as_primitive::<Int32Type>().value(i),
                *y,
                "years of {:?}",
                DAY_CASES[i]
            );
            assert_eq!(
                months.as_primitive::<Int32Type>().value(i),
                *m,
                "months of {:?}",
                DAY_CASES[i]
            );
        }
        assert_eq!(dates.data_type(), &DataType::Date32);
        assert_eq!(dates.as_primitive::<Date32Type>(), &Date32Array::from(days));
        let last = DAY_CASES.len();
        assert!(years.is_null(last) && months.is_null(last) && dates.is_null(last));

        let err = invoke(
            &SparkIcebergTemporalTransform::hours(),
            vec![ColumnarValue::Array(input)],
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("does not support input type Date32"));
    }

    #[test]
    fn timestamps_match_iceberg_date_time_util_in_utc() {
        // (micros, years, months, days, hours)
        let cases: &[(i64, i32, i32, i32, i32)] = &[
            (1_510_871_468_000_000, 47, 574, 17_486, 419_686), // 2017-11-16T22:31:08 (spec)
            (0, 0, 0, 0, 0),
            (-1, -1, -1, -1, -1),                   // 1969-12-31T23:59:59.999999
            (-MICROS_PER_HOUR, -1, -1, -1, -1),     // 1969-12-31T23:00:00
            (-MICROS_PER_HOUR - 1, -1, -1, -1, -2), // 1969-12-31T22:59:59.999999
            (-MICROS_PER_DAY, -1, -1, -1, -24),     // 1969-12-31T00:00:00
            (-MICROS_PER_DAY - 1, -1, -1, -2, -25), // 1969-12-30T23:59:59.999999
            (365 * MICROS_PER_DAY, 1, 12, 365, 8_760), // 1971-01-01T00:00:00
            (365 * MICROS_PER_DAY - 1, 0, 11, 364, 8_759),
            // The extremes of Spark's timestamp domain, from Iceberg's `DateTimeUtil` on a JVM.
            // The hour count is the one conversion that does not fit an `i32` there, and Java's
            // `(int)` narrowing wraps it exactly as `as i32` does.
            (i64::MAX, 292_277, 3_507_324, 106_751_991, -1_732_919_508),
            (i64::MIN, -292_278, -3_507_325, -106_751_992, 1_732_919_507),
            (
                8_000_000_000_000_000_000,
                253_509,
                3_042_118,
                92_592_592,
                -2_072_745_074,
            ),
            (
                -8_000_000_000_000_000_000,
                -253_510,
                -3_042_119,
                -92_592_593,
                2_072_745_073,
            ),
        ];
        let micros: Vec<Option<i64>> = cases.iter().map(|c| Some(c.0)).chain([None]).collect();
        // A non-UTC timezone tag must not change the result.
        for tz in [
            None,
            Some("UTC"),
            Some("America/Los_Angeles"),
            Some("Asia/Kathmandu"),
        ] {
            let mut array = TimestampMicrosecondArray::from(micros.clone());
            if let Some(tz) = tz {
                array = array.with_timezone(tz);
            }
            let input: ArrayRef = Arc::new(array);
            let years = transform(TemporalUnit::Years, Arc::clone(&input));
            let months = transform(TemporalUnit::Months, Arc::clone(&input));
            let days = transform(TemporalUnit::Days, Arc::clone(&input));
            let hours = transform(TemporalUnit::Hours, Arc::clone(&input));
            assert_eq!(days.data_type(), &DataType::Date32);
            for (i, (_, y, m, d, h)) in cases.iter().enumerate() {
                let case = cases[i];
                assert_eq!(
                    years.as_primitive::<Int32Type>().value(i),
                    *y,
                    "years {case:?} {tz:?}"
                );
                assert_eq!(
                    months.as_primitive::<Int32Type>().value(i),
                    *m,
                    "months {case:?} {tz:?}"
                );
                assert_eq!(
                    days.as_primitive::<Date32Type>().value(i),
                    *d,
                    "days {case:?} {tz:?}"
                );
                assert_eq!(
                    hours.as_primitive::<Int32Type>().value(i),
                    *h,
                    "hours {case:?} {tz:?}"
                );
            }
            let last = cases.len();
            assert!(
                years.is_null(last)
                    && months.is_null(last)
                    && days.is_null(last)
                    && hours.is_null(last)
            );
        }
    }

    /// The pinned cases above check the endpoints of the domain; this checks the calendar split
    /// itself everywhere `chrono` can still represent the date, so the hand-rolled arithmetic
    /// cannot drift in between.
    #[test]
    fn civil_from_days_agrees_with_chrono() {
        use chrono::Datelike;
        // Every day of one full 400-year Gregorian cycle either side of the epoch, then a stride
        // over the rest of the `i32` domain (`to_naive_date_opt` returns `None` past chrono's
        // range, which is exactly the region the pinned cases cover).
        let dense = -146_097..=146_097;
        for days in dense.chain((i32::MIN..=i32::MAX).step_by(999_983)) {
            if let Some(date) = Date32Type::to_naive_date_opt(days) {
                assert_eq!(
                    civil_from_days(days),
                    (date.year(), date.month0() as i32),
                    "day {days} ({date})"
                );
            }
        }
    }

    #[test]
    fn rejects_unsupported_types() {
        for unit in [
            TemporalUnit::Years,
            TemporalUnit::Months,
            TemporalUnit::Days,
            TemporalUnit::Hours,
        ] {
            let err = invoke(
                &SparkIcebergTemporalTransform::new(unit),
                vec![ColumnarValue::Array(Arc::new(Int32Array::from(vec![1])))],
            )
            .unwrap_err();
            assert!(err
                .to_string()
                .contains("does not support input type Int32"));
        }
    }
}
