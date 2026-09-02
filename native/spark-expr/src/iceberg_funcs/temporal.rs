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
//! The kernels work on the raw epoch values rather than going through Arrow's timezone-aware
//! `date_part`, which would otherwise shift a `TimestampType` column by the session offset.

use super::{apply_unary, unsupported_type};
use arrow::array::{ArrayRef, AsArray, Date32Array, Int32Array};
use arrow::datatypes::{DataType, Date32Type, Int32Type, TimeUnit, TimestampMicrosecondType};
use chrono::Datelike;
use datafusion::common::{utils::take_function_args, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::integer::div_floor;
use std::sync::Arc;

const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const UNIX_EPOCH_YEAR: i32 = 1970;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TemporalUnit {
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

/// `DateTimeUtil.microsToDays`: floor division, so `-1` micros is day `-1`.
#[inline]
fn micros_to_days(micros: i64) -> i32 {
    div_floor(micros, MICROS_PER_DAY) as i32
}

/// `DateTimeUtil.microsToHours`.
#[inline]
fn micros_to_hours(micros: i64) -> i32 {
    div_floor(micros, MICROS_PER_HOUR) as i32
}

/// `DateTimeUtil.daysToYears`: whole calendar years between the epoch and the day, floored.
fn days_to_years(days: i32) -> Result<i32> {
    Ok(civil_date(days)?.year() - UNIX_EPOCH_YEAR)
}

/// `DateTimeUtil.daysToMonths`: whole calendar months between the epoch and the day, floored.
fn days_to_months(days: i32) -> Result<i32> {
    let date = civil_date(days)?;
    Ok((date.year() - UNIX_EPOCH_YEAR) * 12 + date.month0() as i32)
}

fn civil_date(days: i32) -> Result<chrono::NaiveDate> {
    Date32Type::to_naive_date_opt(days).ok_or_else(|| {
        DataFusionError::Execution(format!("day {days} is out of the supported date range"))
    })
}

/// Reduces both supported input types to days since the epoch.
fn to_epoch_days(fn_name: &str, array: &ArrayRef) -> Result<Date32Array> {
    match array.data_type() {
        DataType::Date32 => Ok(array.as_primitive::<Date32Type>().clone()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(array
            .as_primitive::<TimestampMicrosecondType>()
            .unary(micros_to_days)),
        other => Err(unsupported_type(fn_name, other)),
    }
}

fn transform_array(unit: TemporalUnit, array: &ArrayRef) -> Result<ArrayRef> {
    let fn_name = unit.fn_name();
    let result: ArrayRef = match unit {
        TemporalUnit::Years => {
            Arc::new(to_epoch_days(fn_name, array)?.try_unary::<_, Int32Type, _>(days_to_years)?)
        }
        TemporalUnit::Months => {
            Arc::new(to_epoch_days(fn_name, array)?.try_unary::<_, Int32Type, _>(days_to_months)?)
        }
        TemporalUnit::Days => Arc::new(to_epoch_days(fn_name, array)?),
        TemporalUnit::Hours => match array.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let hours: Int32Array = array
                    .as_primitive::<TimestampMicrosecondType>()
                    .unary(micros_to_hours);
                Arc::new(hours)
            }
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
    pub fn new(unit: TemporalUnit) -> Self {
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
    use arrow::array::{Array, TimestampMicrosecondArray};

    fn transform(unit: TemporalUnit, value: ArrayRef) -> ArrayRef {
        invoke(
            &SparkIcebergTemporalTransform::new(unit),
            vec![ColumnarValue::Array(value)],
        )
        .unwrap()
    }

    // Boundaries around the epoch, as (epoch days, years, months).
    const DAY_CASES: &[(i32, i32, i32)] = &[
        (17_486, 47, 574), // 2017-11-16, the Iceberg spec example
        (0, 0, 0),         // 1970-01-01
        (-1, -1, -1),      // 1969-12-31
        (-365, -1, -12),   // 1969-01-01
        (-366, -2, -13),   // 1968-12-31
        (365, 1, 12),      // 1971-01-01
        (364, 0, 11),      // 1970-12-31
        (31, 0, 1),        // 1970-02-01
        (30, 0, 0),        // 1970-01-31
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
