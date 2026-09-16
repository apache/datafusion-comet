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

use chrono::{
    DateTime, Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc,
};

use std::sync::Arc;

use arrow::array::{
    downcast_dictionary_array, downcast_temporal_array,
    temporal_conversions::*,
    timezone::Tz,
    types::{ArrowDictionaryKeyType, ArrowTemporalType, TimestampMicrosecondType},
    ArrowNumericType,
};
use arrow::{
    array::*,
    datatypes::{DataType, Field, TimeUnit},
};
use datafusion::{
    common::{config::ConfigOptions, ScalarValue},
    logical_expr::{ColumnarValue, ScalarFunctionArgs},
};
use datafusion_functions::datetime;

use crate::SparkError;

/// Invoke DataFusion's physical `date_trunc` implementation with a scalar granularity.
///
/// Spark syntax normalization and compatibility fallback decisions deliberately live outside
/// this helper so the upstream execution boundary stays obvious.
fn datafusion_date_trunc(
    array: ArrayRef,
    granularity: &'static str,
) -> Result<ArrayRef, SparkError> {
    let data_type = array.data_type().clone();
    let number_rows = array.len();
    let args = vec![
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(granularity.to_string()))),
        ColumnarValue::Array(array),
    ];
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(index, value)| {
            Arc::new(Field::new(
                format!("date_trunc_arg_{index}"),
                value.data_type(),
                true,
            ))
        })
        .collect();

    datetime::date_trunc()
        .invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("date_trunc", data_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .and_then(|value| value.to_array(number_rows))
        .map_err(|error| SparkError::Internal(error.to_string()))
}

// Copied from arrow_arith/temporal.rs
macro_rules! return_compute_error_with {
    ($msg:expr, $param:expr) => {
        return { Err(SparkError::Internal(format!("{}: {:?}", $msg, $param))) }
    };
}

// The number of days between the beginning of the proleptic gregorian calendar (0001-01-01)
// and the beginning of the Unix Epoch (1970-01-01)
const DAYS_TO_UNIX_EPOCH: i32 = 719_163;

// Optimized date truncation functions that work directly with days since epoch
// These avoid the overhead of converting to/from NaiveDateTime

/// Convert days since Unix epoch to NaiveDate
#[inline]
fn days_to_date(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(days + DAYS_TO_UNIX_EPOCH)
}

/// Truncate date to first day of year - optimized version
/// Uses ordinal (day of year) to avoid creating a new date
#[inline]
fn trunc_days_to_year(days: i32) -> Option<i32> {
    let date = days_to_date(days)?;
    let day_of_year_offset = date.ordinal() as i32 - 1;
    Some(days - day_of_year_offset)
}

/// Truncate date to first day of quarter - optimized version
/// Computes offset from first day of quarter without creating a new date
#[inline]
fn trunc_days_to_quarter(days: i32) -> Option<i32> {
    let date = days_to_date(days)?;
    let month = date.month(); // 1-12
    let quarter = (month - 1) / 3; // 0-3
    let first_month_of_quarter = quarter * 3 + 1; // 1, 4, 7, or 10

    // Find day of year for first day of quarter
    let first_day_of_quarter = NaiveDate::from_ymd_opt(date.year(), first_month_of_quarter, 1)?;
    let quarter_start_ordinal = first_day_of_quarter.ordinal() as i32;
    let current_ordinal = date.ordinal() as i32;

    Some(days - (current_ordinal - quarter_start_ordinal))
}

/// Truncate date to first day of month - optimized version
/// Instead of creating a new date, just subtract day offset
#[inline]
fn trunc_days_to_month(days: i32) -> Option<i32> {
    let date = days_to_date(days)?;
    let day_offset = date.day() as i32 - 1;
    Some(days - day_offset)
}

/// Truncate date to first day of week (Monday) - optimized version
#[inline]
fn trunc_days_to_week(days: i32) -> Option<i32> {
    let date = days_to_date(days)?;
    // weekday().num_days_from_monday() gives 0 for Monday, 1 for Tuesday, etc.
    let days_since_monday = date.weekday().num_days_from_monday() as i32;
    Some(days - days_since_monday)
}

// Based on arrow_arith/temporal.rs:extract_component_from_datetime_array
// Transforms an array of DateTime<Tz> to an array of TimestampMicrosecond after applying an
// operation. The output array carries the input timezone annotation so downstream operators
// (shuffle, sort, row converter) observe a matching schema.
fn as_timestamp_tz_with_op<A: ArrayAccessor<Item = T::Native>, T: ArrowTemporalType, F>(
    iter: ArrayIter<A>,
    mut builder: PrimitiveBuilder<TimestampMicrosecondType>,
    tz_str: &str,
    op: F,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    F: Fn(DateTime<Tz>) -> i64,
    i64: From<T::Native>,
{
    let tz: Tz = tz_str.parse()?;
    for value in iter {
        match value {
            Some(value) => match as_datetime_with_timezone::<T>(value.into(), tz) {
                Some(time) => builder.append_value(op(time)),
                _ => {
                    return Err(SparkError::Internal(
                        "Unable to read value as datetime".to_string(),
                    ));
                }
            },
            None => builder.append_null(),
        }
    }
    Ok(builder.finish().with_timezone(tz_str))
}

fn as_timestamp_tz_with_op_single<T: ArrowTemporalType, F>(
    value: Option<T::Native>,
    builder: &mut PrimitiveBuilder<TimestampMicrosecondType>,
    tz: &Tz,
    op: F,
) -> Result<(), SparkError>
where
    F: Fn(DateTime<Tz>) -> i64,
    i64: From<T::Native>,
{
    match value {
        Some(value) => match as_datetime_with_timezone::<T>(value.into(), *tz) {
            Some(time) => builder.append_value(op(time)),
            _ => {
                return Err(SparkError::Internal(
                    "Unable to read value as datetime".to_string(),
                ));
            }
        },
        None => builder.append_null(),
    }
    Ok(())
}

// Apply the Tz to the Naive Date Time, convert to UTC, and return as microseconds in Unix epoch.
// After truncation the carried UTC offset may be wrong if the truncated time falls in a different
// DST period than the original (e.g., truncating a December/PST timestamp to QUARTER yields
// October 1 which is in PDT). We re-resolve the naive local time through the timezone so that
// chrono picks the correct offset for the target date.
#[inline]
fn as_micros_from_unix_epoch_utc(dt: Option<DateTime<Tz>>) -> i64 {
    let dt = dt.unwrap();
    let naive = dt.naive_local();
    let tz = dt.timezone();

    match tz.from_local_datetime(&naive) {
        LocalResult::Single(resolved) | LocalResult::Ambiguous(resolved, _) => {
            resolved.with_timezone(&Utc).timestamp_micros()
        }
        LocalResult::None => dt.with_timezone(&Utc).timestamp_micros(),
    }
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
        .map(|d| d - Duration::try_seconds(60 * 60 * 24 * d.weekday() as i64).unwrap())
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

///
/// Implements the spark [TRUNC](https://spark.apache.org/docs/latest/api/sql/index.html#trunc)
/// function where the specified format is a scalar value
///
///   array is an array of Date32 values. The array may be a dictionary array.
///
///   format is a scalar string specifying the format to apply to the timestamp value.
pub fn date_trunc_dyn(array: &dyn Array, format: String) -> Result<ArrayRef, SparkError> {
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

pub(crate) fn date_trunc<T>(
    array: &PrimitiveArray<T>,
    format: String,
) -> Result<Date32Array, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    match array.data_type() {
        DataType::Date32 => {
            // Use optimized path for Date32 that works directly with days
            date_trunc_date32(
                array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .expect("Date32 type mismatch"),
                format,
            )
        }
        dt => return_compute_error_with!(
            "Unsupported input type '{:?}' for function 'date_trunc'",
            dt
        ),
    }
}

/// Truncates a date expressed as days since the epoch, returning `None` if it is out of range.
type DateTruncFn = fn(i32) -> Option<i32>;

/// The `date_trunc` formats Spark accepts, and the truncation each one selects.
const DATE_TRUNC_FORMATS: [(&str, DateTruncFn); 8] = [
    ("YEAR", trunc_days_to_year),
    ("YYYY", trunc_days_to_year),
    ("YY", trunc_days_to_year),
    ("QUARTER", trunc_days_to_quarter),
    ("MONTH", trunc_days_to_month),
    ("MON", trunc_days_to_month),
    ("MM", trunc_days_to_month),
    ("WEEK", trunc_days_to_week),
];

/// Resolve a `date_trunc` format string to the corresponding truncation function.
///
/// Every supported format is ASCII, so `eq_ignore_ascii_case` on the input as-is is exactly
/// what Spark does: a non-ASCII input cannot match any ASCII table entry after ASCII case
/// folding, and Spark also only recognizes those ASCII literals. This is allocation-free.
fn date_trunc_fn_for_format(format: &str) -> Result<DateTruncFn, SparkError> {
    DATE_TRUNC_FORMATS
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(format))
        .map(|(_, trunc_fn)| *trunc_fn)
        .ok_or_else(|| {
            SparkError::Internal(format!(
                "Unsupported format: {format:?} for function 'date_trunc'"
            ))
        })
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

#[inline]
fn fits_timestamp_nanosecond(micros: i64) -> bool {
    micros.checked_mul(1_000).is_some()
}

/// DataFusion's coarse truncation first converts the input to nanoseconds. Although an input near
/// the lower TimestampNanosecond bound can itself be represented, truncating it may move the
/// result before that bound: for example, `1677-09-22` truncated to YEAR becomes `1677-01-01`.
/// The result can move backward by 365 days (366 when starting from December 31 in a leap year),
/// and timezone gap handling can shift it by a few more hours. Round that worst case up to 370
/// days so both DataFusion's input and coarse-truncation result remain representable. The
/// effective microsecond interval is approximately `1678-09-26T00:12:43.145225Z` through
/// `2262-04-11T23:47:16.854775Z`; because Date32 values are UTC midnight, its first upstream date
/// is 1678-09-27 and its last is 2262-04-11.
#[inline]
fn fits_datafusion_coarse_trunc_range(micros: i64) -> bool {
    const LOWER_NANOSECOND_MICROS: i64 = i64::MIN / 1_000;
    const COARSE_TRUNC_MARGIN_MICROS: i64 = 370 * MICROS_PER_DAY;

    fits_timestamp_nanosecond(micros)
        && micros >= LOWER_NANOSECOND_MICROS + COARSE_TRUNC_MARGIN_MICROS
}

/// Truncate Date32 values directly in days since the epoch.
///
/// Routing Date32 through DataFusion's timestamp kernel requires two casts and temporary arrays,
/// which is materially slower than this single-pass implementation.
fn date_trunc_date32(array: &Date32Array, format: String) -> Result<Date32Array, SparkError> {
    let trunc_fn = date_trunc_fn_for_format(&format)?;
    Ok(array.iter().map(|value| value.and_then(trunc_fn)).collect())
}

///
/// Implements the spark [TRUNC](https://spark.apache.org/docs/latest/api/sql/index.html#trunc)
/// function where the specified format may be an array
///
///   array is an array of Date32 values. The array may be a dictionary array.
///
///   format is an array of strings specifying the format to apply to the corresponding date value.
///             The array may be a dictionary array.
pub(crate) fn date_trunc_array_fmt_dyn(
    array: &dyn Array,
    formats: &dyn Array,
) -> Result<ArrayRef, SparkError> {
    match (array.data_type().clone(), formats.data_type().clone()) {
        (DataType::Dictionary(_, v), DataType::Dictionary(_, f)) => {
            if !matches!(*v, DataType::Date32) {
                return_compute_error_with!("date_trunc does not support", v)
            }
            if !matches!(*f, DataType::Utf8) {
                return_compute_error_with!("date_trunc does not support format type ", f)
            }
            downcast_dictionary_array!(
                formats => {
                    downcast_dictionary_array!(
                        array => {
                            date_trunc_array_fmt_dict_dict(
                                    &array.downcast_dict::<Date32Array>().unwrap(),
                                    &formats.downcast_dict::<StringArray>().unwrap())
                            .map(|a| Arc::new(a) as ArrayRef)
                        }
                        dt => return_compute_error_with!("date_trunc does not support", dt)
                    )
                }
                fmt => return_compute_error_with!("date_trunc does not support format type", fmt),
            )
        }
        (DataType::Dictionary(_, v), DataType::Utf8) => {
            if !matches!(*v, DataType::Date32) {
                return_compute_error_with!("date_trunc does not support", v)
            }
            downcast_dictionary_array!(
                array => {
                  date_trunc_array_fmt_dict_plain(
                        &array.downcast_dict::<Date32Array>().unwrap(),
                        formats.as_any().downcast_ref::<StringArray>()
                            .expect("Unexpected value type in formats"))
                  .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!("date_trunc does not support", dt),
            )
        }
        (DataType::Date32, DataType::Dictionary(_, f)) => {
            if !matches!(*f, DataType::Utf8) {
                return_compute_error_with!("date_trunc does not support format type ", f)
            }
            downcast_dictionary_array!(
                formats => {
                downcast_temporal_array!(array => {
                        date_trunc_array_fmt_plain_dict(
                            array.as_any().downcast_ref::<Date32Array>()
                                .expect("Unexpected error in casting date array"),
                            &formats.downcast_dict::<StringArray>().unwrap())
                        .map(|a| Arc::new(a) as ArrayRef)
                    }
                    dt => return_compute_error_with!("date_trunc does not support", dt),
                    )
                }
                fmt => return_compute_error_with!("date_trunc does not support format type", fmt),
            )
        }
        (DataType::Date32, DataType::Utf8) => date_trunc_array_fmt_plain_plain(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Unexpected error in casting date array"),
            formats
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Unexpected value type in formats"),
        )
        .map(|a| Arc::new(a) as ArrayRef),
        (dt, fmt) => Err(SparkError::Internal(format!(
            "Unsupported datatype: {dt:}, format: {fmt:?} for function 'date_trunc'"
        ))),
    }
}

macro_rules! date_trunc_array_fmt_helper {
    ($array: ident, $formats: ident, $datatype: ident) => {{
        let mut builder = Date32Builder::with_capacity($array.len());
        let iter = $array.into_iter();
        match $datatype {
            DataType::Date32 => {
                // Format columns are almost always constant or very low cardinality, so remember
                // the last format seen and skip re-resolving it for every row.
                let mut cached: Option<(&str, DateTruncFn)> = None;
                for (index, val) in iter.enumerate() {
                    let format = $formats.value(index);
                    let trunc_fn = match cached {
                        Some((cached_format, trunc_fn)) if cached_format == format => trunc_fn,
                        _ => {
                            let trunc_fn = date_trunc_fn_for_format(format)?;
                            cached = Some((format, trunc_fn));
                            trunc_fn
                        }
                    };
                    match val.and_then(trunc_fn) {
                        Some(days) => builder.append_value(days),
                        None => builder.append_null(),
                    }
                }
                Ok(builder.finish())
            }
            dt => return_compute_error_with!(
                "Unsupported input type '{:?}' for function 'date_trunc'",
                dt
            ),
        }
    }};
}

fn date_trunc_array_fmt_plain_plain(
    array: &Date32Array,
    formats: &StringArray,
) -> Result<Date32Array, SparkError>
where
{
    let data_type = array.data_type();
    date_trunc_array_fmt_helper!(array, formats, data_type)
}

fn date_trunc_array_fmt_plain_dict<K>(
    array: &Date32Array,
    formats: &TypedDictionaryArray<K, StringArray>,
) -> Result<Date32Array, SparkError>
where
    K: ArrowDictionaryKeyType,
{
    let data_type = array.data_type();
    date_trunc_array_fmt_helper!(array, formats, data_type)
}

fn date_trunc_array_fmt_dict_plain<K>(
    array: &TypedDictionaryArray<K, Date32Array>,
    formats: &StringArray,
) -> Result<Date32Array, SparkError>
where
    K: ArrowDictionaryKeyType,
{
    let data_type = array.values().data_type();
    date_trunc_array_fmt_helper!(array, formats, data_type)
}

fn date_trunc_array_fmt_dict_dict<K, F>(
    array: &TypedDictionaryArray<K, Date32Array>,
    formats: &TypedDictionaryArray<F, StringArray>,
) -> Result<Date32Array, SparkError>
where
    K: ArrowDictionaryKeyType,
    F: ArrowDictionaryKeyType,
{
    let data_type = array.values().data_type();
    date_trunc_array_fmt_helper!(array, formats, data_type)
}

///
/// Implements the spark [DATE_TRUNC](https://spark.apache.org/docs/latest/api/sql/index.html#date_trunc)
/// function where the specified format is a scalar value
///
///   array is an array of Timestamp(Microsecond) values. Timestamp values must have a valid
///            timezone or no timezone. The array may be a dictionary array.
///
///   format is a scalar string specifying the format to apply to the timestamp value.
pub(crate) fn timestamp_trunc_dyn(
    array: &dyn Array,
    format: String,
) -> Result<ArrayRef, SparkError> {
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

/// Convert microseconds since epoch to NaiveDateTime
#[inline]
fn micros_to_naive(micros: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_micros(micros).map(|dt| dt.naive_utc())
}

/// Convert NaiveDateTime back to microseconds since epoch
#[inline]
fn naive_to_micros(dt: NaiveDateTime) -> i64 {
    dt.and_utc().timestamp_micros()
}

/// Truncates a `NaiveDateTime`, returning `None` if the result is out of range.
type NtzTruncFn = fn(NaiveDateTime) -> Option<NaiveDateTime>;

/// Truncates a `DateTime<Tz>`, returning `None` if the result is out of range.
type TzTruncFn = fn(DateTime<Tz>) -> Option<DateTime<Tz>>;

/// The Spark `date_trunc` spellings and their canonical DataFusion granularities.
const TIMESTAMP_TRUNC_ALIASES: [(&str, &str); 15] = [
    ("YEAR", "year"),
    ("YYYY", "year"),
    ("YY", "year"),
    ("QUARTER", "quarter"),
    ("MONTH", "month"),
    ("MON", "month"),
    ("MM", "month"),
    ("WEEK", "week"),
    ("DAY", "day"),
    ("DD", "day"),
    ("HOUR", "hour"),
    ("MINUTE", "minute"),
    ("SECOND", "second"),
    ("MILLISECOND", "millisecond"),
    ("MICROSECOND", "microsecond"),
];

/// The `timestamp_trunc` formats Spark accepts for the NTZ path, and the truncation each one
/// selects. All entries are ASCII, so `eq_ignore_ascii_case` on the raw input matches Spark
/// without allocating.
const TIMESTAMP_TRUNC_FORMATS_NTZ: [(&str, NtzTruncFn); 15] = [
    ("YEAR", trunc_date_to_year),
    ("YYYY", trunc_date_to_year),
    ("YY", trunc_date_to_year),
    ("QUARTER", trunc_date_to_quarter),
    ("MONTH", trunc_date_to_month),
    ("MON", trunc_date_to_month),
    ("MM", trunc_date_to_month),
    ("WEEK", trunc_date_to_week),
    ("DAY", trunc_date_to_day),
    ("DD", trunc_date_to_day),
    ("HOUR", trunc_date_to_hour),
    ("MINUTE", trunc_date_to_minute),
    ("SECOND", trunc_date_to_second),
    ("MILLISECOND", trunc_date_to_ms),
    ("MICROSECOND", trunc_date_to_microsec),
];

/// Same formats as `TIMESTAMP_TRUNC_FORMATS_NTZ`, monomorphized for the timezone-aware path.
const TIMESTAMP_TRUNC_FORMATS_TZ: [(&str, TzTruncFn); 15] = [
    ("YEAR", trunc_date_to_year),
    ("YYYY", trunc_date_to_year),
    ("YY", trunc_date_to_year),
    ("QUARTER", trunc_date_to_quarter),
    ("MONTH", trunc_date_to_month),
    ("MON", trunc_date_to_month),
    ("MM", trunc_date_to_month),
    ("WEEK", trunc_date_to_week),
    ("DAY", trunc_date_to_day),
    ("DD", trunc_date_to_day),
    ("HOUR", trunc_date_to_hour),
    ("MINUTE", trunc_date_to_minute),
    ("SECOND", trunc_date_to_second),
    ("MILLISECOND", trunc_date_to_ms),
    ("MICROSECOND", trunc_date_to_microsec),
];

/// Resolve a truncation format string to the corresponding NaiveDateTime truncation function.
///
/// All supported formats are ASCII, so `eq_ignore_ascii_case` on the input as-is is exactly what
/// Spark does and allocation-free.
fn ntz_trunc_fn_for_format(format: &str) -> Result<NtzTruncFn, SparkError> {
    TIMESTAMP_TRUNC_FORMATS_NTZ
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(format))
        .map(|(_, trunc_fn)| *trunc_fn)
        .ok_or_else(|| {
            SparkError::Internal(format!(
                "Unsupported format: {format:?} for function 'timestamp_trunc'"
            ))
        })
}

/// Timezone-aware sibling of `ntz_trunc_fn_for_format`.
fn tz_trunc_fn_for_format(format: &str) -> Result<TzTruncFn, SparkError> {
    TIMESTAMP_TRUNC_FORMATS_TZ
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(format))
        .map(|(_, trunc_fn)| *trunc_fn)
        .ok_or_else(|| {
            SparkError::Internal(format!(
                "Unsupported format: {format:?} for function 'timestamp_trunc'"
            ))
        })
}

/// Normalize Spark `date_trunc` aliases without accepting additional DataFusion spellings.
fn normalize_timestamp_trunc_format(format: &str) -> Result<&'static str, SparkError> {
    TIMESTAMP_TRUNC_ALIASES
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(format))
        .map(|(_, granularity)| *granularity)
        .ok_or_else(|| {
            SparkError::Internal(format!(
                "Unsupported format: {format:?} for function 'timestamp_trunc'"
            ))
        })
}

/// Truncate a TimestampNTZ array without any timezone conversion.
/// NTZ values are timezone-independent; we treat the raw microseconds as a naive datetime.
fn timestamp_trunc_ntz<T>(
    array: &PrimitiveArray<T>,
    format: String,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let trunc_fn = ntz_trunc_fn_for_format(&format)?;

    let result: TimestampMicrosecondArray = array
        .iter()
        .map(|opt_val| {
            opt_val.and_then(|v| {
                let micros: i64 = v.into();
                micros_to_naive(micros)
                    .and_then(trunc_fn)
                    .map(naive_to_micros)
            })
        })
        .collect();

    Ok(result)
}

/// The scalar-format implementation retained for values outside DataFusion 55.1's internal
/// TimestampNanosecond range. Row-format paths continue to call the same underlying helpers.
fn timestamp_trunc_legacy(
    array: &TimestampMicrosecondArray,
    format: &str,
) -> Result<TimestampMicrosecondArray, SparkError> {
    let builder = TimestampMicrosecondBuilder::with_capacity(array.len());
    let iter = ArrayIter::new(array);
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            timestamp_trunc_ntz(array, format.to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
            let trunc_fn = tz_trunc_fn_for_format(format)?;
            as_timestamp_tz_with_op::<&TimestampMicrosecondArray, TimestampMicrosecondType, _>(
                iter,
                builder,
                tz,
                |dt| as_micros_from_unix_epoch_utc(trunc_fn(dt)),
            )
        }
        dt => return_compute_error_with!(
            "Unsupported input type '{:?}' for function 'timestamp_trunc'",
            dt
        ),
    }
}

fn datafusion_timestamp_trunc_requires_nanos(granularity: &str, has_timezone: bool) -> bool {
    match granularity {
        "microsecond" | "millisecond" | "second" | "minute" => false,
        "hour" | "day" => has_timezone,
        "week" | "month" | "quarter" | "year" => true,
        _ => unreachable!("granularity was normalized before compatibility dispatch"),
    }
}

fn timestamp_trunc_upstream(
    array: &TimestampMicrosecondArray,
    format: &str,
) -> Result<TimestampMicrosecondArray, SparkError> {
    let granularity = normalize_timestamp_trunc_format(format)?;
    let requires_nanos =
        datafusion_timestamp_trunc_requires_nanos(granularity, array.timezone().is_some());

    if !requires_nanos
        || array
            .iter()
            .flatten()
            .all(fits_datafusion_coarse_trunc_range)
    {
        let result = datafusion_date_trunc(Arc::new(array.clone()), granularity)?;
        return Ok(result
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("DataFusion date_trunc timestamp result mismatch")
            .clone());
    }

    let upstream_input = TimestampMicrosecondArray::from_iter(
        array
            .iter()
            .map(|value| value.filter(|micros| fits_datafusion_coarse_trunc_range(*micros))),
    )
    .with_timezone_opt(array.timezone());
    let legacy_input = TimestampMicrosecondArray::from_iter(
        array
            .iter()
            .map(|value| value.filter(|micros| !fits_datafusion_coarse_trunc_range(*micros))),
    )
    .with_timezone_opt(array.timezone());

    let upstream = datafusion_date_trunc(Arc::new(upstream_input), granularity)?;
    let upstream = upstream
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("DataFusion date_trunc timestamp result mismatch");
    let legacy = timestamp_trunc_legacy(&legacy_input, format)?;

    Ok(
        TimestampMicrosecondArray::from_iter(array.iter().enumerate().map(|(index, value)| {
            value.map(|micros| {
                if fits_datafusion_coarse_trunc_range(micros) {
                    upstream.value(index)
                } else {
                    legacy.value(index)
                }
            })
        }))
        .with_timezone_opt(array.timezone()),
    )
}

/// Truncate a single NTZ value and append to builder
fn timestamp_trunc_ntz_single<F>(
    value: Option<i64>,
    builder: &mut PrimitiveBuilder<TimestampMicrosecondType>,
    op: F,
) -> Result<(), SparkError>
where
    F: Fn(NaiveDateTime) -> Option<NaiveDateTime>,
{
    match value {
        Some(micros) => match micros_to_naive(micros).and_then(op) {
            Some(truncated) => builder.append_value(naive_to_micros(truncated)),
            None => {
                return Err(SparkError::Internal(
                    "Unable to truncate NTZ timestamp".to_string(),
                ))
            }
        },
        None => builder.append_null(),
    }
    Ok(())
}

pub(crate) fn timestamp_trunc<T>(
    array: &PrimitiveArray<T>,
    format: String,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => timestamp_trunc_upstream(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("TimestampMicrosecond type mismatch"),
            &format,
        ),
        dt => return_compute_error_with!(
            "Unsupported input type '{:?}' for function 'timestamp_trunc'",
            dt
        ),
    }
}

///
/// Implements the spark [DATE_TRUNC](https://spark.apache.org/docs/latest/api/sql/index.html#date_trunc)
/// function where the specified format may be an array
///
///   array is an array of Timestamp(Microsecond) values. Timestamp values must have a valid
///            timezone or no timezone. The array may be a dictionary array.
///
///   format is an array of strings specifying the format to apply to the corresponding timestamp
///             value. The array may be a dictionary array.
pub(crate) fn timestamp_trunc_array_fmt_dyn(
    array: &dyn Array,
    formats: &dyn Array,
) -> Result<ArrayRef, SparkError> {
    match (array.data_type().clone(), formats.data_type().clone()) {
        (DataType::Dictionary(_, _), DataType::Dictionary(_, _)) => {
            downcast_dictionary_array!(
                formats => {
                    downcast_dictionary_array!(
                        array => {
                            timestamp_trunc_array_fmt_dict_dict(
                                    &array.downcast_dict::<TimestampMicrosecondArray>().unwrap(),
                                    &formats.downcast_dict::<StringArray>().unwrap())
                            .map(|a| Arc::new(a) as ArrayRef)
                        }
                        dt => return_compute_error_with!("timestamp_trunc does not support", dt)
                    )
                }
                fmt => return_compute_error_with!("timestamp_trunc does not support format type", fmt),
            )
        }
        (DataType::Dictionary(_, _), DataType::Utf8) => {
            downcast_dictionary_array!(
                array => {
                  timestamp_trunc_array_fmt_dict_plain(
                        &array.downcast_dict::<PrimitiveArray<TimestampMicrosecondType>>().unwrap(),
                        formats.as_any().downcast_ref::<StringArray>()
                            .expect("Unexpected value type in formats"))
                  .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!("timestamp_trunc does not support", dt),
            )
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Dictionary(_, _)) => {
            downcast_dictionary_array!(
                formats => {
                downcast_temporal_array!(array => {
                        timestamp_trunc_array_fmt_plain_dict(
                                array,
                                &formats.downcast_dict::<StringArray>().unwrap())
                        .map(|a| Arc::new(a) as ArrayRef)
                    }
                    dt => return_compute_error_with!("timestamp_trunc does not support", dt),
                    )
                }
                fmt => return_compute_error_with!("timestamp_trunc does not support format type", fmt),
            )
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), DataType::Utf8) => {
            downcast_temporal_array!(
                array => {
                    timestamp_trunc_array_fmt_plain_plain(array,
                        formats.as_any().downcast_ref::<StringArray>().expect("Unexpected value type in formats"))
                    .map(|a| Arc::new(a) as ArrayRef)
                },
                dt => return_compute_error_with!("timestamp_trunc does not support", dt),
            )
        }
        (dt, fmt) => Err(SparkError::Internal(format!(
            "Unsupported datatype: {dt:}, format: {fmt:?} for function 'timestamp_trunc'"
        ))),
    }
}

macro_rules! timestamp_trunc_array_fmt_helper {
    ($array: ident, $formats: ident, $datatype: ident) => {{
        let mut builder = TimestampMicrosecondBuilder::with_capacity($array.len());
        let iter = $array.into_iter();
        assert_eq!(
            $array.len(),
            $formats.len(),
            "lengths of values array and format array must be the same"
        );
        match $datatype {
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                // TimestampNTZ: operate directly on naive microsecond values
                for (index, val) in iter.enumerate() {
                    let micros_val = val.map(|v| i64::from(v));
                    let trunc_fn = ntz_trunc_fn_for_format($formats.value(index))?;
                    timestamp_trunc_ntz_single(micros_val, &mut builder, trunc_fn)?;
                }
                Ok(builder.finish())
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz_str)) => {
                let tz: Tz = tz_str.parse()?;
                for (index, val) in iter.enumerate() {
                    let trunc_fn = tz_trunc_fn_for_format($formats.value(index))?;
                    as_timestamp_tz_with_op_single::<T, _>(val, &mut builder, &tz, |dt| {
                        as_micros_from_unix_epoch_utc(trunc_fn(dt))
                    })?;
                }
                Ok(builder.finish().with_timezone(tz_str.as_ref()))
            }
            dt => {
                return_compute_error_with!(
                    "Unsupported input type '{:?}' for function 'timestamp_trunc'",
                    dt
                )
            }
        }
    }};
}

fn timestamp_trunc_array_fmt_plain_plain<T>(
    array: &PrimitiveArray<T>,
    formats: &StringArray,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let data_type = array.data_type();
    timestamp_trunc_array_fmt_helper!(array, formats, data_type)
}
fn timestamp_trunc_array_fmt_plain_dict<T, K>(
    array: &PrimitiveArray<T>,
    formats: &TypedDictionaryArray<K, StringArray>,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
    K: ArrowDictionaryKeyType,
{
    let data_type = array.data_type();
    timestamp_trunc_array_fmt_helper!(array, formats, data_type)
}

fn timestamp_trunc_array_fmt_dict_plain<T, K>(
    array: &TypedDictionaryArray<K, PrimitiveArray<T>>,
    formats: &StringArray,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
    K: ArrowDictionaryKeyType,
{
    let data_type = array.values().data_type();
    timestamp_trunc_array_fmt_helper!(array, formats, data_type)
}

fn timestamp_trunc_array_fmt_dict_dict<T, K, F>(
    array: &TypedDictionaryArray<K, PrimitiveArray<T>>,
    formats: &TypedDictionaryArray<F, StringArray>,
) -> Result<TimestampMicrosecondArray, SparkError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
    K: ArrowDictionaryKeyType,
    F: ArrowDictionaryKeyType,
{
    let data_type = array.values().data_type();
    timestamp_trunc_array_fmt_helper!(array, formats, data_type)
}

#[cfg(test)]
mod tests {
    use crate::kernels::temporal::{
        date_trunc, date_trunc_array_fmt_dyn, date_trunc_dyn, timestamp_trunc,
        timestamp_trunc_array_fmt_dyn, timestamp_trunc_dyn,
    };
    use crate::SparkError;
    use arrow::array::{
        builder::{PrimitiveDictionaryBuilder, StringDictionaryBuilder},
        iterator::ArrayIter,
        types::{Date32Type, Int32Type, TimestampMicrosecondType},
        Array, Date32Array, PrimitiveArray, StringArray, TimestampMicrosecondArray,
    };
    use chrono::{DateTime, Datelike, NaiveDate};
    use std::sync::Arc;

    fn epoch_days(date: &str) -> i32 {
        NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .unwrap()
            .num_days_from_ce()
            - 719_163
    }

    fn assert_date_trunc(format: &str, input: &[Option<&str>], expected: &[Option<&str>]) {
        let input = Date32Array::from(
            input
                .iter()
                .map(|date| date.map(epoch_days))
                .collect::<Vec<_>>(),
        );
        let expected = Date32Array::from(
            expected
                .iter()
                .map(|date| date.map(epoch_days))
                .collect::<Vec<_>>(),
        );
        assert_eq!(date_trunc(&input, format.to_string()).unwrap(), expected);
    }

    fn instant_micros(instant: &str) -> i64 {
        DateTime::parse_from_rfc3339(instant)
            .unwrap()
            .timestamp_micros()
    }

    fn assert_timestamp_trunc(
        format: &str,
        timezone: Option<&str>,
        input: &[Option<&str>],
        expected: &[Option<&str>],
    ) {
        let input = TimestampMicrosecondArray::from(
            input
                .iter()
                .map(|instant| instant.map(instant_micros))
                .collect::<Vec<_>>(),
        )
        .with_timezone_opt(timezone);
        let expected = TimestampMicrosecondArray::from(
            expected
                .iter()
                .map(|instant| instant.map(instant_micros))
                .collect::<Vec<_>>(),
        )
        .with_timezone_opt(timezone);
        assert_eq!(
            timestamp_trunc(&input, format.to_string()).unwrap(),
            expected
        );
    }

    #[test]
    fn test_date_trunc() {
        for format in ["YEAR", "YYYY", "YY", "year", "Year", "yEaR"] {
            assert_date_trunc(format, &[Some("2024-05-17")], &[Some("2024-01-01")]);
        }
        for format in ["MONTH", "MON", "MM", "month", "Mon"] {
            assert_date_trunc(format, &[Some("2024-05-17")], &[Some("2024-05-01")]);
        }

        assert_date_trunc(
            "QUARTER",
            &[
                Some("2024-01-01"),
                Some("2024-03-31"),
                Some("2024-04-01"),
                Some("2024-06-30"),
                Some("2024-07-01"),
                Some("2024-10-01"),
            ],
            &[
                Some("2024-01-01"),
                Some("2024-01-01"),
                Some("2024-04-01"),
                Some("2024-04-01"),
                Some("2024-07-01"),
                Some("2024-10-01"),
            ],
        );

        assert_date_trunc(
            "week",
            &[
                Some("2024-05-13"),
                Some("2024-05-14"),
                Some("2024-05-19"),
                Some("2024-05-01"),
                Some("2024-01-01"),
                Some("2023-12-31"),
            ],
            &[
                Some("2024-05-13"),
                Some("2024-05-13"),
                Some("2024-05-13"),
                Some("2024-04-29"),
                Some("2024-01-01"),
                Some("2023-12-25"),
            ],
        );

        let dates = [
            Some("2024-02-29"),
            Some("2000-02-29"),
            Some("1900-02-28"),
            Some("1969-12-31"),
            Some("1960-02-29"),
            Some("1900-01-01"),
            // The input fits TimestampNanosecond, but truncating it to YEAR does not.
            Some("1677-09-22"),
            // Valid Spark Date32 outside TimestampNanosecond's range. DataFusion 55.1
            // date_trunc internally converts coarse granularities to nanoseconds.
            Some("3333-05-17"),
            None,
        ];
        assert_date_trunc(
            "YEAR",
            &dates,
            &[
                Some("2024-01-01"),
                Some("2000-01-01"),
                Some("1900-01-01"),
                Some("1969-01-01"),
                Some("1960-01-01"),
                Some("1900-01-01"),
                Some("1677-01-01"),
                Some("3333-01-01"),
                None,
            ],
        );
        assert_date_trunc(
            "QUARTER",
            &dates,
            &[
                Some("2024-01-01"),
                Some("2000-01-01"),
                Some("1900-01-01"),
                Some("1969-10-01"),
                Some("1960-01-01"),
                Some("1900-01-01"),
                Some("1677-07-01"),
                Some("3333-04-01"),
                None,
            ],
        );
        assert_date_trunc(
            "MONTH",
            &dates,
            &[
                Some("2024-02-01"),
                Some("2000-02-01"),
                Some("1900-02-01"),
                Some("1969-12-01"),
                Some("1960-02-01"),
                Some("1900-01-01"),
                Some("1677-09-01"),
                Some("3333-05-01"),
                None,
            ],
        );
        assert_date_trunc(
            "WEEK",
            &dates,
            &[
                Some("2024-02-26"),
                Some("2000-02-28"),
                Some("1900-02-26"),
                Some("1969-12-29"),
                Some("1960-02-29"),
                Some("1900-01-01"),
                Some("1677-09-20"),
                Some("3333-05-11"),
                None,
            ],
        );

        let input = Date32Array::from(vec![epoch_days("2024-05-17")]);
        for format in ["DAY", "HOUR", "SECOND", "invalid", " YEAR ", ""] {
            let SparkError::Internal(message) = date_trunc(&input, format.to_string()).unwrap_err()
            else {
                panic!("expected an internal unsupported-format error");
            };
            assert_eq!(
                message,
                format!("Unsupported format: {format:?} for function 'date_trunc'")
            );
        }
    }

    #[test]
    fn test_date_trunc_scalar_format_dictionary() {
        let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Date32Type>::new();
        builder.append(epoch_days("2024-05-17")).unwrap();
        builder.append(epoch_days("2024-06-30")).unwrap();
        builder.append(epoch_days("2024-05-17")).unwrap();
        builder.append_null();
        builder.append(epoch_days("1969-12-31")).unwrap();
        let input = builder.finish();
        let input_keys = input.keys().clone();

        let result = date_trunc_dyn(&input, "MONTH".to_string()).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(result.keys(), &input_keys);

        let decoded = result.downcast_dict::<Date32Array>().unwrap();
        assert_eq!(
            decoded.into_iter().collect::<Vec<_>>(),
            vec![
                Some(epoch_days("2024-05-01")),
                Some(epoch_days("2024-06-01")),
                Some(epoch_days("2024-05-01")),
                None,
                Some(epoch_days("1969-12-01")),
            ]
        );
    }

    #[test]
    // This test only verifies that the various input array types work. Actually correctness to
    // ensure this produces the same results as spark is verified in the JVM tests
    fn test_date_trunc_array_fmt_dyn() {
        let size = 10;
        let formats = [
            "YEAR", "YYYY", "YY", "QUARTER", "MONTH", "MON", "MM", "WEEK",
        ];
        let mut vec: Vec<i32> = Vec::with_capacity(size * formats.len());
        let mut fmt_vec: Vec<&str> = Vec::with_capacity(size * formats.len());
        for i in 0..size {
            for fmt_value in &formats {
                vec.push(i as i32 * 1_000_001);
                fmt_vec.push(fmt_value);
            }
        }

        // timestamp array
        let array = Date32Array::from(vec);

        // formats array
        let fmt_array = StringArray::from(fmt_vec);

        // timestamp dictionary array
        let mut date_dict_builder = PrimitiveDictionaryBuilder::<Int32Type, Date32Type>::new();
        for v in array.iter() {
            date_dict_builder
                .append(v.unwrap())
                .expect("Error in building timestamp array");
        }
        let mut array_dict = date_dict_builder.finish();
        // apply timezone
        array_dict = array_dict.with_values(Arc::new(
            array_dict
                .values()
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .clone(),
        ));

        // formats dictionary array
        let mut formats_dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        for v in fmt_array.iter() {
            formats_dict_builder
                .append(v.unwrap())
                .expect("Error in building formats array");
        }
        let fmt_dict = formats_dict_builder.finish();

        // verify input arrays
        let iter = ArrayIter::new(&array);
        let mut dict_iter = array_dict
            .downcast_dict::<PrimitiveArray<Date32Type>>()
            .unwrap()
            .into_iter();
        for val in iter {
            assert_eq!(
                dict_iter
                    .next()
                    .expect("array and dictionary array do not match"),
                val
            )
        }

        // verify input format arrays
        let fmt_iter = ArrayIter::new(&fmt_array);
        let mut fmt_dict_iter = fmt_dict.downcast_dict::<StringArray>().unwrap().into_iter();
        for val in fmt_iter {
            assert_eq!(
                fmt_dict_iter
                    .next()
                    .expect("formats and dictionary formats do not match"),
                val
            )
        }

        // test cases
        if let Ok(a) = date_trunc_array_fmt_dyn(&array, &fmt_array) {
            for i in 0..array.len() {
                assert!(
                    array.value(i) >= a.as_any().downcast_ref::<Date32Array>().unwrap().value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = date_trunc_array_fmt_dyn(&array_dict, &fmt_array) {
            for i in 0..array.len() {
                assert!(
                    array.value(i) >= a.as_any().downcast_ref::<Date32Array>().unwrap().value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = date_trunc_array_fmt_dyn(&array, &fmt_dict) {
            for i in 0..array.len() {
                assert!(
                    array.value(i) >= a.as_any().downcast_ref::<Date32Array>().unwrap().value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = date_trunc_array_fmt_dyn(&array_dict, &fmt_dict) {
            for i in 0..array.len() {
                assert!(
                    array.value(i) >= a.as_any().downcast_ref::<Date32Array>().unwrap().value(i)
                )
            }
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_timestamp_trunc() {
        let input = [Some("2024-05-17T12:34:56.123456Z"), None];
        for format in ["YEAR", "YYYY", "YY", "year", "Year", "yEaR"] {
            assert_timestamp_trunc(
                format,
                Some("UTC"),
                &input,
                &[Some("2024-01-01T00:00:00Z"), None],
            );
        }
        for format in ["MONTH", "MON", "MM", "month", "Mon"] {
            assert_timestamp_trunc(
                format,
                Some("UTC"),
                &input,
                &[Some("2024-05-01T00:00:00Z"), None],
            );
        }
        for (format, expected) in [
            ("QUARTER", "2024-04-01T00:00:00Z"),
            ("WEEK", "2024-05-13T00:00:00Z"),
            ("DAY", "2024-05-17T00:00:00Z"),
            ("DD", "2024-05-17T00:00:00Z"),
            ("HOUR", "2024-05-17T12:00:00Z"),
            ("MINUTE", "2024-05-17T12:34:00Z"),
            ("SECOND", "2024-05-17T12:34:56Z"),
            ("MILLISECOND", "2024-05-17T12:34:56.123Z"),
            ("MICROSECOND", "2024-05-17T12:34:56.123456Z"),
        ] {
            assert_timestamp_trunc(format, Some("UTC"), &input, &[Some(expected), None]);
        }

        let invalid_input =
            TimestampMicrosecondArray::from(vec![instant_micros("2024-05-17T12:34:56Z")])
                .with_timezone_utc();
        for format in ["MILLISECONDS", "invalid", " DAY ", ""] {
            let SparkError::Internal(message) =
                timestamp_trunc(&invalid_input, format.to_string()).unwrap_err()
            else {
                panic!("expected an internal unsupported-format error");
            };
            assert_eq!(
                message,
                format!("Unsupported format: {format:?} for function 'timestamp_trunc'")
            );
        }
    }

    #[test]
    fn test_timestamp_trunc_wide_range_fallback() {
        let input = [
            Some("2024-05-17T12:34:56.123456Z"),
            Some("3333-05-17T12:34:56.123456Z"),
            Some("1969-12-31T23:59:59.123456Z"),
            Some("1677-09-22T00:00:00Z"),
            None,
        ];
        assert_timestamp_trunc(
            "YEAR",
            Some("UTC"),
            &input,
            &[
                Some("2024-01-01T00:00:00Z"),
                Some("3333-01-01T00:00:00Z"),
                Some("1969-01-01T00:00:00Z"),
                Some("1677-01-01T00:00:00Z"),
                None,
            ],
        );
        assert_timestamp_trunc(
            "QUARTER",
            Some("UTC"),
            &input,
            &[
                Some("2024-04-01T00:00:00Z"),
                Some("3333-04-01T00:00:00Z"),
                Some("1969-10-01T00:00:00Z"),
                Some("1677-07-01T00:00:00Z"),
                None,
            ],
        );
        assert_timestamp_trunc(
            "WEEK",
            Some("UTC"),
            &input,
            &[
                Some("2024-05-13T00:00:00Z"),
                Some("3333-05-11T00:00:00Z"),
                Some("1969-12-29T00:00:00Z"),
                Some("1677-09-20T00:00:00Z"),
                None,
            ],
        );
    }

    #[test]
    fn test_timestamp_trunc_dst_gap_and_overlap() {
        assert_timestamp_trunc(
            "HOUR",
            Some("America/Los_Angeles"),
            &[
                Some("2024-11-03T08:30:15.123456Z"),
                Some("2024-11-03T09:30:15.123456Z"),
            ],
            &[Some("2024-11-03T08:00:00Z"), Some("2024-11-03T09:00:00Z")],
        );
        assert_timestamp_trunc(
            "DAY",
            Some("America/New_York"),
            &[
                Some("2024-03-10T06:30:15.123456Z"),
                Some("2024-03-10T07:30:15.123456Z"),
                Some("2024-11-03T06:30:15.123456Z"),
            ],
            &[
                Some("2024-03-10T05:00:00Z"),
                Some("2024-03-10T05:00:00Z"),
                Some("2024-11-03T04:00:00Z"),
            ],
        );
        // Sao Paulo advanced from 23:59:59 on November 3 directly to 01:00 on November 4.
        // Spark resolves DAY for a post-gap instant to that day's first valid local time.
        assert_timestamp_trunc(
            "DAY",
            Some("America/Sao_Paulo"),
            &[
                Some("2018-11-04T02:30:15.123456Z"),
                Some("2018-11-04T03:30:15.123456Z"),
            ],
            &[Some("2018-11-03T03:00:00Z"), Some("2018-11-04T03:00:00Z")],
        );
    }

    #[test]
    fn test_timestamp_trunc_scalar_format_dictionary() {
        let mut builder = PrimitiveDictionaryBuilder::<Int32Type, TimestampMicrosecondType>::new();
        builder
            .append(instant_micros("2024-05-17T12:34:56Z"))
            .unwrap();
        builder
            .append(instant_micros("2024-06-30T23:59:59Z"))
            .unwrap();
        builder
            .append(instant_micros("2024-05-17T12:34:56Z"))
            .unwrap();
        builder.append_null();
        let input = builder.finish();
        let input = input.with_values(Arc::new(
            input
                .values()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .clone()
                .with_timezone_utc(),
        ));
        let input_keys = input.keys().clone();

        let result = timestamp_trunc_dyn(&input, "MONTH".to_string()).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(result.keys(), &input_keys);
        let decoded = result.downcast_dict::<TimestampMicrosecondArray>().unwrap();
        assert_eq!(
            decoded.into_iter().collect::<Vec<_>>(),
            vec![
                Some(instant_micros("2024-05-01T00:00:00Z")),
                Some(instant_micros("2024-06-01T00:00:00Z")),
                Some(instant_micros("2024-05-01T00:00:00Z")),
                None,
            ]
        );
    }

    #[test]
    // test takes too long with miri
    #[cfg_attr(miri, ignore)]
    // This test only verifies that the various input array types work. Actually correctness to
    // ensure this produces the same results as spark is verified in the JVM tests
    fn test_timestamp_trunc_array_fmt_dyn() {
        let size = 10;
        let formats = [
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
        ];
        let mut vec: Vec<i64> = Vec::with_capacity(size * formats.len());
        let mut fmt_vec: Vec<&str> = Vec::with_capacity(size * formats.len());
        for i in 0..size {
            for fmt_value in &formats {
                vec.push(i as i64 * 1_000_000_001);
                fmt_vec.push(fmt_value);
            }
        }

        // timestamp array
        let array = TimestampMicrosecondArray::from(vec).with_timezone_utc();

        // formats array
        let fmt_array = StringArray::from(fmt_vec);

        // timestamp dictionary array
        let mut timestamp_dict_builder =
            PrimitiveDictionaryBuilder::<Int32Type, TimestampMicrosecondType>::new();
        for v in array.iter() {
            timestamp_dict_builder
                .append(v.unwrap())
                .expect("Error in building timestamp array");
        }
        let mut array_dict = timestamp_dict_builder.finish();
        // apply timezone
        array_dict = array_dict.with_values(Arc::new(
            array_dict
                .values()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .clone()
                .with_timezone_utc(),
        ));

        // formats dictionary array
        let mut formats_dict_builder = StringDictionaryBuilder::<Int32Type>::new();
        for v in fmt_array.iter() {
            formats_dict_builder
                .append(v.unwrap())
                .expect("Error in building formats array");
        }
        let fmt_dict = formats_dict_builder.finish();

        // verify input arrays
        let iter = ArrayIter::new(&array);
        let mut dict_iter = array_dict
            .downcast_dict::<PrimitiveArray<TimestampMicrosecondType>>()
            .unwrap()
            .into_iter();
        for val in iter {
            assert_eq!(
                dict_iter
                    .next()
                    .expect("array and dictionary array do not match"),
                val
            )
        }

        // verify input format arrays
        let fmt_iter = ArrayIter::new(&fmt_array);
        let mut fmt_dict_iter = fmt_dict.downcast_dict::<StringArray>().unwrap().into_iter();
        for val in fmt_iter {
            assert_eq!(
                fmt_dict_iter
                    .next()
                    .expect("formats and dictionary formats do not match"),
                val
            )
        }

        // test cases
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array, &fmt_array) {
            for i in 0..array.len() {
                assert!(
                    array.value(i)
                        >= a.as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array_dict, &fmt_array) {
            for i in 0..array.len() {
                assert!(
                    array.value(i)
                        >= a.as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array, &fmt_dict) {
            for i in 0..array.len() {
                assert!(
                    array.value(i)
                        >= a.as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(i)
                )
            }
        } else {
            unreachable!()
        }
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array_dict, &fmt_dict) {
            for i in 0..array.len() {
                assert!(
                    array.value(i)
                        >= a.as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(i)
                )
            }
        } else {
            unreachable!()
        }
    }

    /// Truncating a November timestamp in `America/Denver` to QUARTER must land on the start of
    /// Q4, which is October 1 — and October 1 is still MDT (UTC-6), not MST (UTC-7). The
    /// pre-fix kernel reused the input's MST offset for the truncated date, producing a result
    /// one hour late. Also verifies the output array carries the input timezone, which is what
    /// allows the result to flow through shuffle/sort without a `RowConverter` schema mismatch.
    #[test]
    fn test_timestamp_trunc_dst_boundary() {
        // 2023-11-15 18:30:00 UTC = 2023-11-15 11:30 MST
        let ts_utc_micros: i64 = 1700069400 * 1_000_000;
        let array =
            TimestampMicrosecondArray::from(vec![ts_utc_micros]).with_timezone("America/Denver");

        let result = timestamp_trunc(&array, "QUARTER".to_string()).unwrap();

        // 2023-10-01 00:00:00 MDT = 2023-10-01 06:00:00 UTC
        let expected_utc_micros: i64 = 1696140000 * 1_000_000;
        assert_eq!(result.value(0), expected_utc_micros);
        assert_eq!(
            result.data_type(),
            &arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("America/Denver".into())
            )
        );
    }
}
