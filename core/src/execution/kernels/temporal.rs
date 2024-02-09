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

//! temporal kernels

use chrono::{DateTime, Datelike, Duration, NaiveDateTime, Timelike, Utc};

use std::sync::Arc;

use arrow::{array::*, datatypes::DataType};
use arrow_array::{
    downcast_dictionary_array, downcast_temporal_array,
    temporal_conversions::*,
    timezone::Tz,
    types::{ArrowTemporalType, Date32Type, TimestampMicrosecondType},
    ArrowNumericType,
};

use arrow_schema::TimeUnit;

use crate::errors::ExpressionError;

// Copied from arrow_arith/temporal.rs
macro_rules! return_compute_error_with {
    ($msg:expr, $param:expr) => {
        return {
            Err(ExpressionError::ArrowError(format!(
                "{}: {:?}",
                $msg, $param
            )))
        }
    };
}

// The number of days between the beginning of the proleptic gregorian calendar (0001-01-01)
// and the beginning of the Unix Epoch (1970-01-01)
const DAYS_TO_UNIX_EPOCH: i32 = 719_163;
const MICROS_TO_UNIX_EPOCH: i64 = 62_167_132_800 * 1_000_000;

// Copied from arrow_arith/temporal.rs with modification to the output datatype
// Transforms a array of NaiveDate to an array of Date32 after applying an operation
fn as_datetime_with_op<A: ArrayAccessor<Item = T::Native>, T: ArrowTemporalType, F>(
    iter: ArrayIter<A>,
    mut builder: PrimitiveBuilder<Date32Type>,
    op: F,
) -> Date32Array
where
    F: Fn(NaiveDateTime) -> i32,
    i64: From<T::Native>,
{
    iter.into_iter().for_each(|value| {
        if let Some(value) = value {
            match as_datetime::<T>(i64::from(value)) {
                Some(dt) => builder.append_value(op(dt)),
                None => builder.append_null(),
            }
        } else {
            builder.append_null();
        }
    });

    builder.finish()
}

// Based on arrow_arith/temporal.rs:extract_component_from_datetime_array
// Transforms an array of DateTime<Tz> to an arrayOf TimeStampMicrosecond after applying an
// operation
fn as_timestamp_tz_with_op<A: ArrayAccessor<Item = T::Native>, T: ArrowTemporalType, F>(
    iter: ArrayIter<A>,
    mut builder: PrimitiveBuilder<TimestampMicrosecondType>,
    tz: &str,
    op: F,
) -> Result<TimestampMicrosecondArray, ExpressionError>
where
    F: Fn(DateTime<Tz>) -> i64,
    i64: From<T::Native>,
{
    let tz: Tz = tz.parse()?;
    for value in iter {
        match value {
            Some(value) => match as_datetime_with_timezone::<T>(value.into(), tz) {
                Some(time) => builder.append_value(op(time)),
                _ => {
                    return Err(ExpressionError::ArrowError(
                        "Unable to read value as datetime".to_string(),
                    ));
                }
            },
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

#[inline]
fn as_days_from_unix_epoch(dt: Option<NaiveDateTime>) -> i32 {
    dt.unwrap().num_days_from_ce() - DAYS_TO_UNIX_EPOCH
}

// Apply the Tz to the Naive Date Time,,convert to UTC, and return as microseconds in Unix epoch
#[inline]
fn as_micros_from_unix_epoch_utc(dt: Option<DateTime<Tz>>) -> i64 {
    dt.unwrap().with_timezone(&Utc).timestamp_micros()
}

#[inline]
fn trunc_date_to_year<T: Datelike + Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_hour(0))
        .and_then(|d| d.with_day0(0))
        .and_then(|d| d.with_month0(0))
}

/// returns the month of the beginning of the quarter
#[inline]
fn quarter_month<T: Datelike>(dt: &T) -> u32 {
    1 + 3 * ((dt.month() - 1) / 3)
}

#[inline]
fn trunc_date_to_quarter<T: Datelike + Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_hour(0))
        .and_then(|d| d.with_day0(0))
        .and_then(|d| d.with_month(quarter_month(&d)))
}

#[inline]
fn trunc_date_to_month<T: Datelike + Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_hour(0))
        .and_then(|d| d.with_day0(0))
}

#[inline]
fn trunc_date_to_week<T>(dt: T) -> Option<T>
where
    T: Datelike + Timelike + std::ops::Sub<Duration, Output = T> + Copy,
{
    Some(dt)
        .map(|d| d - Duration::seconds(60 * 60 * 24 * d.weekday() as i64))
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_hour(0))
}

#[inline]
fn trunc_date_to_day<T: Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_hour(0))
}

#[inline]
fn trunc_date_to_hour<T: Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_minute(0))
}

#[inline]
fn trunc_date_to_minute<T: Timelike>(dt: T) -> Option<T> {
    Some(dt)
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0))
}

#[inline]
fn trunc_date_to_second<T: Timelike>(dt: T) -> Option<T> {
    Some(dt).and_then(|d| d.with_nanosecond(0))
}

#[inline]
fn trunc_date_to_ms<T: Timelike>(dt: T) -> Option<T> {
    Some(dt).and_then(|d| d.with_nanosecond(1_000_000 * (d.nanosecond() / 1_000_000)))
}

#[inline]
fn trunc_date_to_microsec<T: Timelike>(dt: T) -> Option<T> {
    Some(dt).and_then(|d| d.with_nanosecond(1_000 * (d.nanosecond() / 1_000)))
}

pub fn date_trunc_dyn(array: &dyn Array, format: String) -> Result<ArrayRef, ExpressionError> {
    match array.data_type().clone() {
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                array => {
                    let truncated_values = date_trunc_dyn(array.values(), format)?;
                    Ok(Arc::new(array.with_values(truncated_values)))
                }
                dt => return_compute_error_with!("date_trunc does not support", dt),
            )
        }
        _ => {
            downcast_temporal_array!(
                array => {
                   date_trunc(array, format)
                    .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!("date_trunc does not support", dt),
            )
        }
    }
}

pub fn date_trunc<T>(
    array: &PrimitiveArray<T>,
    format: String,
) -> Result<Date32Array, ExpressionError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let builder = Date32Builder::with_capacity(array.len());
    let iter = ArrayIter::new(array);
    match array.data_type() {
        DataType::Date32 => match format.to_uppercase().as_str() {
            "YEAR" | "YYYY" | "YY" => Ok(as_datetime_with_op::<&PrimitiveArray<T>, T, _>(
                iter,
                builder,
                |dt| as_days_from_unix_epoch(trunc_date_to_year(dt)),
            )),
            "QUARTER" => Ok(as_datetime_with_op::<&PrimitiveArray<T>, T, _>(
                iter,
                builder,
                |dt| as_days_from_unix_epoch(trunc_date_to_quarter(dt)),
            )),
            "MONTH" | "MON" | "MM" => Ok(as_datetime_with_op::<&PrimitiveArray<T>, T, _>(
                iter,
                builder,
                |dt| as_days_from_unix_epoch(trunc_date_to_month(dt)),
            )),
            "WEEK" => Ok(as_datetime_with_op::<&PrimitiveArray<T>, T, _>(
                iter,
                builder,
                |dt| as_days_from_unix_epoch(trunc_date_to_week(dt)),
            )),
            _ => Err(ExpressionError::ArrowError(format!(
                "Unsupported format: {:?} for function 'date_trunc'",
                format
            ))),
        },
        dt => return_compute_error_with!(
            "Unsupported input type '{:?}' for function 'date_trunc'",
            dt
        ),
    }
}

pub fn timestamp_trunc_dyn(array: &dyn Array, format: String) -> Result<ArrayRef, ExpressionError> {
    match array.data_type().clone() {
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                array => {
                    let truncated_values = timestamp_trunc_dyn(array.values(), format)?;
                    Ok(Arc::new(array.with_values(truncated_values)))
                }
                dt => return_compute_error_with!("timestamp_trunc does not support", dt),
            )
        }
        _ => {
            downcast_temporal_array!(
                array => {
                   timestamp_trunc(array, format)
                    .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!("timestamp_trunc does not support", dt),
            )
        }
    }
}

pub fn timestamp_trunc<T>(
    array: &PrimitiveArray<T>,
    format: String,
) -> Result<TimestampMicrosecondArray, ExpressionError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let builder = TimestampMicrosecondBuilder::with_capacity(array.len());
    let iter = ArrayIter::new(array);
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            match format.to_uppercase().as_str() {
                "YEAR" | "YYYY" | "YY" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_year(dt))
                    })
                }
                "QUARTER" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_quarter(dt))
                    })
                }
                "MONTH" | "MON" | "MM" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_month(dt))
                    })
                }
                "WEEK" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_week(dt))
                    })
                }
                "DAY" | "DD" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_day(dt))
                    })
                }
                "HOUR" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_hour(dt))
                    })
                }
                "MINUTE" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_minute(dt))
                    })
                }
                "SECOND" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_second(dt))
                    })
                }
                "MILLISECOND" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_ms(dt))
                    })
                }
                "MICROSECOND" => {
                    as_timestamp_tz_with_op::<&PrimitiveArray<T>, T, _>(iter, builder, tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_date_to_microsec(dt))
                    })
                }
                _ => Err(ExpressionError::ArrowError(format!(
                    "Unsupported format: {:?} for function 'timestamp_trunc'",
                    format
                ))),
            }
        }
        dt => return_compute_error_with!(
            "Unsupported input type '{:?}' for function 'timestamp_trunc'",
            dt
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::kernels::temporal::{date_trunc, timestamp_trunc};
    use arrow_array::{Date32Array, TimestampMicrosecondArray};

    #[test]
    fn test_date_trunc() {
        let size = 1000;
        let mut vec: Vec<i32> = Vec::with_capacity(size);
        for i in 0..size {
            vec.push(i as i32);
        }
        let array = Date32Array::from(vec);
        for fmt in [
            "YEAR", "YYYY", "YY", "QUARTER", "MONTH", "MON", "MM", "WEEK",
        ] {
            match date_trunc(&array, fmt.to_string()) {
                Ok(a) => {
                    for i in 0..size {
                        assert!(array.values().get(i) >= a.values().get(i))
                    }
                }
                _ => assert!(false),
            }
        }
    }

    #[test]
    fn test_timestamp_trunc() {
        let size = 1000;
        let mut vec: Vec<i64> = Vec::with_capacity(size);
        for i in 0..size {
            vec.push(i as i64);
        }
        let array = TimestampMicrosecondArray::from(vec).with_timezone_utc();
        for fmt in [
            "YEAR",
            "YYYY",
            "YY",
            "QUARTER",
            "MONTH",
            "MON",
            "MM",
            "WEEK",
            "DAY",
            "DD",
            "HOUR",
            "MINUTE",
            "SECOND",
            "MILLISECOND",
            "MICROSECOND",
        ] {
            match timestamp_trunc(&array, fmt.to_string()) {
                Ok(a) => {
                    for i in 0..size {
                        assert!(array.values().get(i) >= a.values().get(i))
                    }
                }
                _ => assert!(false),
            }
        }
    }
}
