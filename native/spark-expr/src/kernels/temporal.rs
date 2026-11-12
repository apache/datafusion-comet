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
    DateTime, Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeZone,
    Timelike, Utc,
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
const MICROS_PER_MINUTE: i64 = 60_000_000;

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

// Integer proleptic Gregorian calendar conversion, adapted from DataFusion 55.1.0's
// datetime/date_trunc.rs (Howard Hinnant's algorithms):
// https://howardhinnant.github.io/date_algorithms.html
// These helpers only receive days derived from i64 microseconds (about +/-107 million),
// so the calendar intermediates fit in i64 even outside chrono's year range.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_index = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Truncate UTC/NTZ coarse units directly in microseconds, avoiding both DataFusion's
/// intermediate nanosecond conversion and chrono's year limit. A representable input can
/// truncate below i64::MIN; report that overflow rather than returning NULL or wrapping.
fn timestamp_trunc_coarse_micros(micros: i64, granularity: &str) -> Result<i64, SparkError> {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let truncated_days = match granularity {
        // The epoch is Thursday, three days after Monday.
        "week" => days - (days + 3).rem_euclid(7),
        "month" => {
            let (_, _, day) = civil_from_days(days);
            days - (day - 1)
        }
        "quarter" => {
            let (year, month, _) = civil_from_days(days);
            days_from_civil(year, 1 + 3 * ((month - 1) / 3), 1)
        }
        "year" => {
            let (year, _, _) = civil_from_days(days);
            days_from_civil(year, 1, 1)
        }
        _ => unreachable!("integer fallback only handles normalized coarse units"),
    };
    truncated_days.checked_mul(MICROS_PER_DAY).ok_or_else(|| {
        SparkError::Internal(format!(
            "long overflow: Timestamp {micros} out of range after date_trunc({granularity})"
        ))
    })
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
///
///   wrap_second_millisecond_overflow selects how SECOND and MILLISECOND truncation treats a
///            result below the smallest timestamp: Spark before 4.2 wraps, 4.2 and later raise.
pub(crate) fn timestamp_trunc_dyn(
    array: &dyn Array,
    format: String,
    wrap_second_millisecond_overflow: bool,
) -> Result<ArrayRef, SparkError> {
    match array.data_type().clone() {
        DataType::Dictionary(_, _) => {
            downcast_dictionary_array!(
                array => {
                    // Safe, low-cardinality truncation can operate only on distinct values,
                    // rather than scanning every key just to discover unused values.
                    if !timestamp_trunc_dictionary_needs_mask(
                        array.values(),
                        array.len(),
                        &format,
                        wrap_second_millisecond_overflow,
                    )? {
                        let values = timestamp_trunc_dyn(
                            array.values(),
                            format,
                            wrap_second_millisecond_overflow,
                        )?;
                        return Ok(Arc::new(array.with_values(values)));
                    }
                    // Dictionary values can outlive the rows that reference them (e.g. after
                    // filtering). Unused entries, including entries hidden by NULL keys,
                    // must not raise errors in the fallible timestamp kernel.
                    let mut unused = vec![true; array.values().len()];
                    for key in array.keys_iter().flatten() {
                        unused[key] = false;
                    }
                    let values = if unused.iter().any(|unused| *unused) {
                        arrow::compute::nullif(array.values(), &BooleanArray::from(unused))
                            .map_err(|error| SparkError::Internal(error.to_string()))?
                    } else {
                        Arc::clone(array.values())
                    };
                    let truncated_values = timestamp_trunc_dyn(
                        values.as_ref(),
                        format,
                        wrap_second_millisecond_overflow,
                    )?;
                    Ok(Arc::new(array.with_values(truncated_values)))
                }
                dt => return_compute_error_with!("timestamp_trunc does not support", dt),
            )
        }
        _ => {
            downcast_temporal_array!(
                array => {
                   timestamp_trunc(array, format, wrap_second_millisecond_overflow)
                    .map(|a| Arc::new(a) as ArrayRef)
                }
                dt => return_compute_error_with!("timestamp_trunc does not support", dt),
            )
        }
    }
}

/// Keep key masking for fallible values and for coarse units with many distinct values, where
/// masking can avoid expensive calendar work on unused entries. Fine arithmetic processes the
/// physical values regardless of validity, so infallible fine units never need a key scan.
/// MICROSECOND is always infallible. SECOND and MILLISECOND ignore the timezone and, when they do
/// not wrap, fail only near the lower bound, so only such a value needs the key scan.
fn timestamp_trunc_dictionary_needs_mask(
    values: &dyn Array,
    keys_len: usize,
    format: &str,
    wrap_second_millisecond_overflow: bool,
) -> Result<bool, SparkError> {
    let Some(values) = values.as_any().downcast_ref::<TimestampMicrosecondArray>() else {
        return Ok(true);
    };
    // Truncation moves backwards by at most a year. Above this conservative lower-bound margin,
    // UTC/NTZ truncation cannot underflow i64 and all values can be evaluated without a key scan.
    const LOWER_SAFE_MICROS: i64 = i64::MIN + 370 * MICROS_PER_DAY;
    let has_value_near_lower_bound = || {
        values
            .iter()
            .flatten()
            .any(|micros| micros < LOWER_SAFE_MICROS)
    };
    let granularity = normalize_timestamp_trunc_format(format)?;
    if granularity == "microsecond"
        || (wrap_second_millisecond_overflow && matches!(granularity, "millisecond" | "second"))
    {
        return Ok(false);
    }
    if matches!(granularity, "millisecond" | "second") {
        // These units ignore the timezone, so a non-UTC value needs no masking of its own.
        return Ok(has_value_near_lower_bound());
    }
    if values.timezone().is_some_and(|tz| !is_utc_timezone(tz)) {
        // Unused non-UTC values can also hit chrono boundary panics, not just Result errors.
        return Ok(true);
    }
    if matches!(granularity, "week" | "month" | "quarter" | "year") && values.len() > keys_len / 64
    {
        // Require substantial repetition before speculatively computing unused calendar values.
        // Dense NULLs/high cardinality otherwise benefit from masking before truncation.
        return Ok(true);
    }
    Ok(has_value_near_lower_bound())
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

/// The zone-aware fallback for HOUR/DAY outside DataFusion 55.1's internal TimestampNanosecond
/// range, for literal formats and format columns alike. UTC/NTZ coarse units use integer calendar
/// arithmetic instead. Like every native local-time conversion, it stops applying DST after
/// chrono-tz's last transition, around 2099, while Spark keeps applying the zone's rules (#6816).
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

/// Returns whether a timezone has a zero UTC offset at every instant.
///
/// Keep this list conservative: an unlisted timezone takes the zone-aware path, which is always
/// correct even when its current offset happens to be zero.
fn is_utc_timezone(timezone: &str) -> bool {
    matches!(
        timezone,
        "UTC" | "Etc/UTC" | "Etc/GMT" | "GMT" | "Z" | "+00:00" | "-00:00" | "00:00"
    )
}

/// Truncate timezone-aware timestamps to the local minute boundary.
///
/// DataFusion floors the stored UTC microseconds directly. That is only equivalent to Spark's
/// local-time truncation when the timezone offset is a whole number of minutes. Historical offsets
/// can include seconds, so resolve the offset for each instant before finding the local remainder.
/// If truncation crosses an offset transition, re-resolve the truncated local datetime just like
/// `ZonedDateTime.truncatedTo` rather than retaining the input instant's offset.
fn timestamp_trunc_minute_tz(
    array: &TimestampMicrosecondArray,
    timezone: &str,
) -> Result<TimestampMicrosecondArray, SparkError> {
    as_timestamp_tz_with_op::<&TimestampMicrosecondArray, TimestampMicrosecondType, _>(
        ArrayIter::new(array),
        TimestampMicrosecondBuilder::with_capacity(array.len()),
        timezone,
        |dt| {
            let micros = dt.timestamp_micros();
            let timezone = dt.timezone();
            let original_offset_secs = dt.offset().fix().local_minus_utc();
            let offset_micros = i64::from(original_offset_secs) * 1_000_000;
            let candidate = micros - (micros + offset_micros).rem_euclid(MICROS_PER_MINUTE);
            let candidate_dt =
                as_datetime_with_timezone::<TimestampMicrosecondType>(candidate, timezone)
                    .expect("truncated minute candidate must be a valid datetime");
            let candidate_offset_secs = candidate_dt.offset().fix().local_minus_utc();

            if candidate_offset_secs == original_offset_secs {
                return candidate;
            }

            let truncated_local = dt
                .naive_local()
                .with_second(0)
                .and_then(|local| local.with_nanosecond(0))
                .expect("truncated local minute must be a valid datetime");
            match timezone.from_local_datetime(&truncated_local) {
                LocalResult::Single(resolved) => resolved.timestamp_micros(),
                LocalResult::Ambiguous(earlier, later) => {
                    // ZonedDateTime retains the original offset when it is valid in an overlap.
                    if earlier.offset().fix().local_minus_utc() == original_offset_secs {
                        earlier.timestamp_micros()
                    } else if later.offset().fix().local_minus_utc() == original_offset_secs {
                        later.timestamp_micros()
                    } else {
                        earlier.timestamp_micros()
                    }
                }
                LocalResult::None => {
                    // The candidate lies immediately before a forward transition. Java advances
                    // a nonexistent local time by the gap, which is equivalent to resolving it
                    // with the candidate's pre-transition offset.
                    naive_to_micros(truncated_local) - i64::from(candidate_offset_secs) * 1_000_000
                }
            }
        },
    )
}

/// Truncate timezone-aware timestamps to a coarse local-date boundary.
///
/// Spark resolves these boundaries with `LocalDate.atStartOfDay`. Unlike
/// `ZonedDateTime.truncatedTo`, an overlap always selects the earlier instant, independent of
/// the input timestamp's offset. A midnight in a gap resolves to the transition instant,
/// the first valid local time after the gap, even when the gap starts before midnight.
fn timestamp_trunc_coarse_tz(
    array: &TimestampMicrosecondArray,
    format: &str,
    timezone: &str,
) -> Result<TimestampMicrosecondArray, SparkError> {
    let trunc_fn = ntz_trunc_fn_for_format(format)?;
    let tz: Tz = timezone.parse()?;

    as_timestamp_tz_with_op::<&TimestampMicrosecondArray, TimestampMicrosecondType, _>(
        ArrayIter::new(array),
        TimestampMicrosecondBuilder::with_capacity(array.len()),
        timezone,
        |dt| {
            let truncated_local = trunc_fn(dt.naive_local())
                .expect("truncated coarse local datetime must be a valid datetime");

            match tz.from_local_datetime(&truncated_local) {
                LocalResult::Single(resolved) => resolved.timestamp_micros(),
                // `LocalDate.atStartOfDay` takes the earlier occurrence in an overlap.
                LocalResult::Ambiguous(earlier, _) => earlier.timestamp_micros(),
                LocalResult::None => {
                    // atStartOfDay returns the end of the gap, not midnight shifted by its
                    // length. Find a valid pre-gap instant, extending the usual three-hour
                    // probe for zones that skipped an entire date (e.g. Pacific/Apia).
                    let mut probe = truncated_local - Duration::hours(3);
                    let before_gap = loop {
                        match tz.from_local_datetime(&probe) {
                            LocalResult::Single(resolved) | LocalResult::Ambiguous(resolved, _) => {
                                break resolved;
                            }
                            LocalResult::None => probe -= Duration::hours(3),
                        }
                    };
                    let pre_gap_offset_secs = before_gap.offset().fix().local_minus_utc();
                    let mut low = before_gap.timestamp();
                    let mut high =
                        truncated_local.and_utc().timestamp() - i64::from(pre_gap_offset_secs);
                    // TZ offsets and transitions have whole-second precision. The old offset
                    // holds at low, and has changed by high; find the first changed second.
                    while high - low > 1 {
                        let mid = low + (high - low) / 2;
                        let utc = DateTime::from_timestamp(mid, 0)
                            .expect("midnight gap probe must be a valid datetime")
                            .naive_utc();
                        if tz.offset_from_utc_datetime(&utc).fix().local_minus_utc()
                            == pre_gap_offset_secs
                        {
                            low = mid;
                        } else {
                            high = mid;
                        }
                    }
                    high * 1_000_000
                }
            }
        },
    )
}

/// Handle the rare fine-unit batch whose physical values would underflow DataFusion's
/// unchecked subtraction. Iterate logical values so NULL slots never cause errors.
fn timestamp_trunc_fine_boundary(
    array: &TimestampMicrosecondArray,
    granularity: &str,
    unit: i64,
    wrap_second_millisecond_overflow: bool,
) -> Result<TimestampMicrosecondArray, SparkError> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(array.len());
    for value in array.iter() {
        match value {
            None => builder.append_null(),
            Some(micros) => {
                let remainder = micros.rem_euclid(unit);
                let truncated = if wrap_second_millisecond_overflow
                    && matches!(granularity, "second" | "millisecond")
                {
                    // Before 4.2, Spark uses unchecked Long subtraction for these
                    // timezone-independent units.
                    micros.wrapping_sub(remainder)
                } else {
                    // MINUTE/HOUR/DAY use Spark's exact instant-to-microseconds conversion, and
                    // Spark 4.2 and later (SPARK-56663) check the SECOND/MILLISECOND
                    // subtraction too.
                    micros.checked_sub(remainder).ok_or_else(|| {
                        SparkError::Internal(format!(
                            "long overflow: Timestamp {micros} out of range after date_trunc({granularity})"
                        ))
                    })?
                };
                builder.append_value(truncated);
            }
        }
    }
    Ok(builder.finish().with_timezone_opt(array.timezone()))
}

fn timestamp_trunc_upstream(
    array: &TimestampMicrosecondArray,
    format: &str,
    wrap_second_millisecond_overflow: bool,
) -> Result<TimestampMicrosecondArray, SparkError> {
    let granularity = normalize_timestamp_trunc_format(format)?;

    if matches!(
        granularity,
        "hour" | "day" | "week" | "month" | "quarter" | "year"
    ) && array.timezone().is_some_and(is_utc_timezone)
    {
        // UTC local time equals the instant. Removing its label lets DataFusion use arithmetic
        // for HOUR/DAY (without a nanosecond limit) and integer calendar code for coarse units.
        // The unlabelled call still applies the coarse-unit range guard and calendar fallback.
        let input = array.clone().with_timezone_opt(None::<Arc<str>>);
        let result = timestamp_trunc_upstream(&input, format, wrap_second_millisecond_overflow)?;
        return Ok(result.with_timezone_opt(array.timezone()));
    }

    if granularity == "minute" {
        if let Some(timezone) = array.timezone().filter(|tz| !is_utc_timezone(tz)) {
            return timestamp_trunc_minute_tz(array, timezone);
        }
    }

    if matches!(granularity, "week" | "month" | "quarter" | "year") {
        if let Some(timezone) = array.timezone().filter(|tz| !is_utc_timezone(tz)) {
            return timestamp_trunc_coarse_tz(array, format, timezone);
        }
    }

    let fine_unit = match granularity {
        "millisecond" => Some(1_000),
        "second" => Some(1_000_000),
        "minute" => Some(MICROS_PER_MINUTE),
        "hour" if array.timezone().is_none() => Some(3_600_000_000),
        "day" if array.timezone().is_none() => Some(MICROS_PER_DAY),
        _ => None,
    };
    if let Some(unit) = fine_unit {
        // First aligned microsecond value that can be represented after truncation.
        let lower = i64::MIN + (unit - i64::MIN.rem_euclid(unit)).rem_euclid(unit);
        // Check physical values too: DataFusion processes NULL slots before restoring validity.
        if array.values().iter().any(|micros| *micros < lower) {
            return timestamp_trunc_fine_boundary(
                array,
                granularity,
                unit,
                wrap_second_millisecond_overflow,
            );
        }
    }

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
    let upstream = datafusion_date_trunc(Arc::new(upstream_input), granularity)?;
    let upstream = upstream
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("DataFusion date_trunc timestamp result mismatch");
    // Non-UTC HOUR/DAY still need the zone-aware fallback. UTC coarse units have already
    // removed their timezone label and must not fall back through chrono.
    let legacy = if array.timezone().is_some() {
        let input = TimestampMicrosecondArray::from_iter(
            array
                .iter()
                .map(|value| value.filter(|micros| !fits_datafusion_coarse_trunc_range(*micros))),
        )
        .with_timezone_opt(array.timezone());
        Some(timestamp_trunc_legacy(&input, format)?)
    } else {
        None
    };
    let mut builder = TimestampMicrosecondBuilder::with_capacity(array.len());
    for (index, value) in array.iter().enumerate() {
        match value {
            None => builder.append_null(),
            Some(micros) if fits_datafusion_coarse_trunc_range(micros) => {
                builder.append_option((!upstream.is_null(index)).then(|| upstream.value(index)));
            }
            Some(micros) => match &legacy {
                Some(result) => {
                    builder.append_option((!result.is_null(index)).then(|| result.value(index)));
                }
                None => builder.append_value(timestamp_trunc_coarse_micros(micros, granularity)?),
            },
        }
    }
    Ok(builder.finish().with_timezone_opt(array.timezone()))
}

pub(crate) fn timestamp_trunc<T>(
    array: &PrimitiveArray<T>,
    format: String,
    wrap_second_millisecond_overflow: bool,
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
            wrap_second_millisecond_overflow,
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
///
///   wrap_second_millisecond_overflow is passed to the literal-format kernel, as in
///            `timestamp_trunc_dyn`.
pub(crate) fn timestamp_trunc_array_fmt_dyn(
    array: &dyn Array,
    formats: &dyn Array,
    wrap_second_millisecond_overflow: bool,
) -> Result<ArrayRef, SparkError> {
    let array = unpack_dictionary(array)?;
    let formats = unpack_dictionary(formats)?;
    let array = match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            array.as_primitive::<TimestampMicrosecondType>()
        }
        dt => {
            return_compute_error_with!("Unsupported input type for function 'timestamp_trunc'", dt)
        }
    };
    let formats = match formats.data_type() {
        DataType::Utf8 => formats.as_string::<i32>(),
        fmt => return_compute_error_with!("timestamp_trunc does not support format type", fmt),
    };
    Ok(Arc::new(timestamp_trunc_by_row_format(
        array,
        formats,
        wrap_second_millisecond_overflow,
    )?))
}

fn unpack_dictionary(array: &dyn Array) -> Result<ArrayRef, SparkError> {
    match array.data_type() {
        DataType::Dictionary(_, value_type) => arrow::compute::cast(array, value_type)
            .map_err(|error| SparkError::Internal(error.to_string())),
        _ => Ok(make_array(array.to_data())),
    }
}

/// Truncates each row with the format in the same row.
///
/// Rows are grouped by format, and each group goes through the literal-format kernel with the
/// other rows set to NULL. A row is therefore truncated by exactly the rules a literal format
/// applies, including the DST handling, and a value in one group cannot make another group fail.
/// A NULL format gives a NULL result, as in Spark.
fn timestamp_trunc_by_row_format(
    array: &TimestampMicrosecondArray,
    formats: &StringArray,
    wrap_second_millisecond_overflow: bool,
) -> Result<TimestampMicrosecondArray, SparkError> {
    if array.len() != formats.len() {
        return Err(SparkError::Internal(format!(
            "timestamp_trunc has {} values but {} formats",
            array.len(),
            formats.len()
        )));
    }

    let mut granularities: Vec<&'static str> = Vec::new();
    // A batch rarely holds more than a few spellings, so parse each one once.
    let mut spellings: Vec<(&str, usize)> = Vec::new();
    let mut row_groups: Vec<Option<usize>> = Vec::with_capacity(array.len());
    let mut null_format_hides_value = false;
    for index in 0..array.len() {
        if array.is_null(index) {
            row_groups.push(None);
            continue;
        }
        if formats.is_null(index) {
            null_format_hides_value = true;
            row_groups.push(None);
            continue;
        }
        let spelling = formats.value(index);
        let group = match spellings.iter().find(|(seen, _)| *seen == spelling) {
            Some((_, group)) => *group,
            None => {
                let granularity = normalize_timestamp_trunc_format(spelling)?;
                let group = match granularities.iter().position(|g| *g == granularity) {
                    Some(group) => group,
                    None => {
                        granularities.push(granularity);
                        granularities.len() - 1
                    }
                };
                spellings.push((spelling, group));
                group
            }
        };
        row_groups.push(Some(group));
    }

    if granularities.len() == 1 && !null_format_hides_value {
        return timestamp_trunc_upstream(array, granularities[0], wrap_second_millisecond_overflow);
    }

    let truncated = granularities
        .iter()
        .enumerate()
        .map(|(group, granularity)| {
            let outside_group: BooleanArray = row_groups
                .iter()
                .map(|row_group| Some(*row_group != Some(group)))
                .collect();
            let input = arrow::compute::nullif(array, &outside_group)
                .map_err(|error| SparkError::Internal(error.to_string()))?;
            timestamp_trunc_upstream(
                input.as_primitive(),
                granularity,
                wrap_second_millisecond_overflow,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut builder = TimestampMicrosecondBuilder::with_capacity(array.len());
    for (index, row_group) in row_groups.iter().enumerate() {
        match row_group.map(|group| &truncated[group]) {
            Some(result) if result.is_valid(index) => builder.append_value(result.value(index)),
            _ => builder.append_null(),
        }
    }
    Ok(builder.finish().with_timezone_opt(array.timezone()))
}

#[cfg(test)]
mod tests {
    use super::{
        naive_to_micros, normalize_timestamp_trunc_format, ntz_trunc_fn_for_format,
        timestamp_trunc_coarse_micros, timestamp_trunc_dictionary_needs_mask, timestamp_trunc_ntz,
        MICROS_PER_DAY, TIMESTAMP_TRUNC_ALIASES,
    };
    use crate::kernels::temporal::{
        date_trunc, date_trunc_array_fmt_dyn, date_trunc_dyn, timestamp_trunc,
        timestamp_trunc_array_fmt_dyn, timestamp_trunc_dyn,
    };
    use crate::SparkError;
    use arrow::array::{
        builder::{PrimitiveDictionaryBuilder, StringDictionaryBuilder},
        iterator::ArrayIter,
        types::{Date32Type, Int32Type, TimestampMicrosecondType},
        Array, Date32Array, DictionaryArray, Int32Array, PrimitiveArray, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};
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
            timestamp_trunc(&input, format.to_string(), true).unwrap(),
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
                timestamp_trunc(&invalid_input, format.to_string(), true).unwrap_err()
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
    fn test_timestamp_trunc_utc_aliases_and_wide_range() {
        let input = [
            Some("1500-06-15T12:34:56.123456Z"),
            Some("1678-06-01T12:34:56.123456Z"),
            Some("1969-12-31T23:59:59.123456Z"),
            Some("3333-05-17T12:34:56.123456Z"),
            None,
        ];
        for timezone in [
            "UTC", "Etc/UTC", "Etc/GMT", "GMT", "Z", "+00:00", "-00:00", "00:00",
        ] {
            for (format, expected) in [
                (
                    "HOUR",
                    [
                        Some("1500-06-15T12:00:00Z"),
                        Some("1678-06-01T12:00:00Z"),
                        Some("1969-12-31T23:00:00Z"),
                        Some("3333-05-17T12:00:00Z"),
                        None,
                    ],
                ),
                (
                    "DAY",
                    [
                        Some("1500-06-15T00:00:00Z"),
                        Some("1678-06-01T00:00:00Z"),
                        Some("1969-12-31T00:00:00Z"),
                        Some("3333-05-17T00:00:00Z"),
                        None,
                    ],
                ),
                (
                    "YEAR",
                    [
                        Some("1500-01-01T00:00:00Z"),
                        Some("1678-01-01T00:00:00Z"),
                        Some("1969-01-01T00:00:00Z"),
                        Some("3333-01-01T00:00:00Z"),
                        None,
                    ],
                ),
            ] {
                assert_timestamp_trunc(format, Some(timezone), &input, &expected);
            }
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
    fn test_timestamp_trunc_out_of_chrono_range() {
        for timezone in [None, Some("UTC"), Some("Etc/UTC"), Some("+00:00")] {
            let input = TimestampMicrosecondArray::from(vec![
                Some(instant_micros("2024-05-17T12:34:56Z")),
                Some(i64::MAX),
                Some(i64::MAX - 1),
                Some(i64::MAX - 2),
                Some(instant_micros("3333-05-17T12:34:56Z")),
                Some(-9_000_000_000_000_000_000),
                None,
            ])
            .with_timezone_opt(timezone);
            // Expected extremes were computed independently with java.time.LocalDate:
            // MAX, MAX - 1, and MAX - 2 are all +294247-01-10;
            // the negative input is -283229-05-10.
            for (format, recent, future, maximum, negative) in [
                (
                    "YEAR",
                    "2024-01-01T00:00:00Z",
                    "3333-01-01T00:00:00Z",
                    9_223_371_244_800_000_000,
                    -9_000_011_174_400_000_000,
                ),
                (
                    "QUARTER",
                    "2024-04-01T00:00:00Z",
                    "3333-04-01T00:00:00Z",
                    9_223_371_244_800_000_000,
                    -9_000_003_398_400_000_000,
                ),
                (
                    "MONTH",
                    "2024-05-01T00:00:00Z",
                    "3333-05-01T00:00:00Z",
                    9_223_371_244_800_000_000,
                    -9_000_000_806_400_000_000,
                ),
                (
                    "WEEK",
                    "2024-05-13T00:00:00Z",
                    "3333-05-11T00:00:00Z",
                    9_223_371_504_000_000_000,
                    -9_000_000_028_800_000_000,
                ),
            ] {
                let expected = TimestampMicrosecondArray::from(vec![
                    Some(instant_micros(recent)),
                    Some(maximum),
                    Some(maximum),
                    Some(maximum),
                    Some(instant_micros(future)),
                    Some(negative),
                    None,
                ])
                .with_timezone_opt(timezone);
                assert_eq!(
                    timestamp_trunc(&input, format.to_string(), true).unwrap(),
                    expected
                );
                for (alias, canonical) in TIMESTAMP_TRUNC_ALIASES {
                    if canonical == normalize_timestamp_trunc_format(format).unwrap() {
                        assert_eq!(
                            timestamp_trunc(&input, alias.to_lowercase(), true).unwrap(),
                            expected
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_coarse_overflow() {
        for timezone in [None, Some("UTC"), Some("Etc/UTC"), Some("+00:00")] {
            let input =
                TimestampMicrosecondArray::from(vec![Some(i64::MIN)]).with_timezone_opt(timezone);
            for format in ["YEAR", "QUARTER", "MONTH", "WEEK"] {
                let error = timestamp_trunc(&input, format.to_string(), true).unwrap_err();
                assert!(error.to_string().contains("out of range"), "{error}");
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_integer_calendar_matches_chrono() {
        // Exercise negative eras, year zero, Gregorian leap centuries, and month boundaries.
        for year in [-2000, -400, -1, 0, 1, 1500, 1600, 1900, 2000, 2024, 3333] {
            for month in 1..=12 {
                for day in [1, 15, 28, 29, 30, 31] {
                    let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
                        continue;
                    };
                    let datetime = date.and_hms_micro_opt(23, 59, 59, 999_999).unwrap();
                    for format in ["YEAR", "QUARTER", "MONTH", "WEEK"] {
                        let expected = naive_to_micros(
                            ntz_trunc_fn_for_format(format).unwrap()(datetime).unwrap(),
                        );
                        assert_eq!(
                            timestamp_trunc_coarse_micros(
                                naive_to_micros(datetime),
                                normalize_timestamp_trunc_format(format).unwrap()
                            )
                            .unwrap(),
                            expected
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_coarse_guard_boundaries() {
        let lower = i64::MIN / 1_000 + 370 * MICROS_PER_DAY;
        let upper = i64::MAX / 1_000;
        let input = TimestampMicrosecondArray::from(vec![
            Some(lower - 1),
            Some(lower),
            Some(lower + 1),
            Some(upper - 1),
            Some(upper),
            Some(upper + 1),
            None,
        ]);
        for format in ["YEAR", "QUARTER", "MONTH", "WEEK"] {
            let expected = timestamp_trunc_ntz(&input, format.to_string()).unwrap();
            assert_eq!(
                timestamp_trunc(&input, format.to_string(), true).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn test_timestamp_trunc_extreme_dictionary() {
        let values = TimestampMicrosecondArray::from(vec![Some(i64::MAX), None]);
        let keys = Int32Array::from(vec![Some(0), None, Some(1), Some(0)]);
        let input =
            DictionaryArray::<arrow::datatypes::Int32Type>::try_new(keys.clone(), Arc::new(values))
                .unwrap();
        let expected_values =
            TimestampMicrosecondArray::from(vec![Some(9_223_371_244_800_000_000), None]);
        let expected = DictionaryArray::<arrow::datatypes::Int32Type>::try_new(
            keys,
            Arc::new(expected_values),
        )
        .unwrap();
        let result = timestamp_trunc_dyn(&input, "YEAR".to_string(), true).unwrap();
        assert_eq!(result.as_ref(), &expected as &dyn Array);
    }

    #[test]
    fn test_timestamp_trunc_empty_and_null_batches() {
        for timezone in [None, Some("UTC")] {
            for values in [vec![], vec![None, None]] {
                let input = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone);
                for format in ["YEAR", "QUARTER", "MONTH", "WEEK"] {
                    assert_eq!(
                        timestamp_trunc(&input, format.to_string(), true).unwrap(),
                        input
                    );
                }
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_dictionary_unused_overflow() {
        for timezone in [None, Some("UTC")] {
            let values = TimestampMicrosecondArray::from(vec![
                Some(instant_micros("2024-05-17T12:34:56Z")),
                Some(i64::MIN),
                None,
            ])
            .with_timezone_opt(timezone);
            for keys in [
                vec![Some(0), None, Some(2), Some(0)],
                vec![None, None],
                vec![],
                vec![Some(0); 8192],
            ] {
                let keys = Int32Array::from(keys);
                let input =
                    DictionaryArray::<Int32Type>::try_new(keys.clone(), Arc::new(values.clone()))
                        .unwrap();
                let expected_values = TimestampMicrosecondArray::from(vec![
                    Some(instant_micros("2024-01-01T00:00:00Z")),
                    None,
                    None,
                ])
                .with_timezone_opt(timezone);
                // Slice past the first key to check that referenced-entry masking respects offsets.
                let input = input.slice(
                    usize::from(!input.is_empty()),
                    input.len().saturating_sub(1),
                );
                let expected =
                    DictionaryArray::<Int32Type>::try_new(keys, Arc::new(expected_values)).unwrap();
                let expected = expected.slice(
                    usize::from(!expected.is_empty()),
                    expected.len().saturating_sub(1),
                );
                let result = timestamp_trunc_dyn(&input, "YEAR".into(), true).unwrap();
                // Unused entries can be masked, so compare the decoded logical rows.
                let result = arrow::compute::cast(
                    result.as_ref(),
                    &DataType::Timestamp(TimeUnit::Microsecond, timezone.map(Into::into)),
                )
                .unwrap();
                let expected = arrow::compute::cast(&expected, result.data_type()).unwrap();
                assert_eq!(result.as_ref(), expected.as_ref());
                let used = DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![1]),
                    Arc::new(values.clone()),
                )
                .unwrap();
                assert!(timestamp_trunc_dyn(&used, "YEAR".into(), true)
                    .unwrap_err()
                    .to_string()
                    .contains("long overflow"));
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_dictionary_unused_chrono_boundary() {
        // These unused values can panic in timezone-aware calendar helpers, so they must still
        // be masked before computing the distinct values, even in a low-cardinality dictionary.
        for timezone in ["+01:00", "Asia/Tokyo"] {
            let values = TimestampMicrosecondArray::from(vec![
                instant_micros("2024-05-17T12:34:56Z"),
                chrono::NaiveDateTime::MIN.and_utc().timestamp_micros() + MICROS_PER_DAY,
            ])
            .with_timezone(timezone);
            let input = DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0); 8192]),
                Arc::new(values.clone()),
            )
            .unwrap();
            for format in ["YEAR", "WEEK"] {
                let result = timestamp_trunc_dyn(&input, format.into(), true).unwrap();
                let decoded = arrow::compute::cast(result.as_ref(), values.data_type()).unwrap();
                let expected = timestamp_trunc(&values.slice(0, 1), format.into(), true).unwrap();
                let decoded = decoded
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                assert!(decoded.iter().all(|value| value == Some(expected.value(0))));
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_dictionary_fine_mask_decision() {
        // SECOND/MILLISECOND ignore the timezone, so a dictionary of safe values is truncated
        // through its values alone in either mode. Only a value that can fail needs the key scan.
        for timezone in [None, Some("UTC"), Some("Asia/Tokyo")] {
            let safe = TimestampMicrosecondArray::from(vec![
                Some(instant_micros("2024-05-17T12:34:56Z")),
                None,
            ])
            .with_timezone_opt(timezone);
            let near_min =
                TimestampMicrosecondArray::from(vec![Some(i64::MIN)]).with_timezone_opt(timezone);
            for format in ["SECOND", "MILLISECOND"] {
                for wrap in [true, false] {
                    assert!(
                        !timestamp_trunc_dictionary_needs_mask(&safe, 8192, format, wrap).unwrap()
                    );
                }
                assert!(
                    !timestamp_trunc_dictionary_needs_mask(&near_min, 8192, format, true).unwrap()
                );
                assert!(
                    timestamp_trunc_dictionary_needs_mask(&near_min, 8192, format, false).unwrap()
                );
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_dictionary_fine_unused_extremes() {
        // Unused MIN entries must not raise, whether SECOND/MILLISECOND wrap (Spark before 4.2)
        // or fail (Spark 4.2 and later).
        for wrap in [true, false] {
            for timezone in [None, Some("UTC"), Some("Asia/Tokyo")] {
                let values = TimestampMicrosecondArray::from(vec![
                    Some(instant_micros("2024-05-17T12:34:56Z")),
                    Some(i64::MIN),
                    None,
                ])
                .with_timezone_opt(timezone);
                for keys in [
                    vec![Some(0), None, Some(2), Some(0)],
                    vec![None, None],
                    vec![],
                ] {
                    let input = DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from(keys),
                        Arc::new(values.clone()),
                    )
                    .unwrap();
                    let decoded_input = arrow::compute::cast(&input, values.data_type()).unwrap();
                    for format in ["MICROSECOND", "MILLISECOND", "SECOND"] {
                        let result = timestamp_trunc_dyn(&input, format.into(), wrap).unwrap();
                        let decoded =
                            arrow::compute::cast(result.as_ref(), values.data_type()).unwrap();
                        let expected =
                            timestamp_trunc_dyn(decoded_input.as_ref(), format.into(), wrap)
                                .unwrap();
                        assert_eq!(decoded.as_ref(), expected.as_ref());
                    }
                }
                if !wrap {
                    // A used MIN entry still raises.
                    let used = DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from(vec![0, 1]),
                        Arc::new(values.clone()),
                    )
                    .unwrap();
                    for format in ["MILLISECOND", "SECOND"] {
                        assert!(timestamp_trunc_dyn(&used, format.into(), wrap)
                            .unwrap_err()
                            .to_string()
                            .contains("long overflow"));
                    }
                }
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_fine_poisoned_nulls() {
        for wrap in [true, false] {
            for timezone in [None, Some("UTC")] {
                let input = TimestampMicrosecondArray::new(
                    vec![
                        i64::MIN,
                        instant_micros("2024-05-17T12:34:56Z"),
                        i64::MIN + 1,
                        i64::MAX,
                    ]
                    .into(),
                    Some(arrow::buffer::NullBuffer::from(vec![
                        false, true, false, true,
                    ])),
                )
                .with_timezone_opt(timezone);
                let clean = TimestampMicrosecondArray::from(vec![
                    None,
                    Some(instant_micros("2024-05-17T12:34:56Z")),
                    None,
                    Some(i64::MAX),
                ])
                .with_timezone_opt(timezone);
                for format in [
                    "MINUTE",
                    "HOUR",
                    "DAY",
                    "SECOND",
                    "MILLISECOND",
                    "MICROSECOND",
                ] {
                    assert_eq!(
                        timestamp_trunc(&input, format.into(), wrap).unwrap(),
                        timestamp_trunc(&clean, format.into(), wrap).unwrap()
                    );
                    let slice = input.slice(2, 2);
                    assert_eq!(
                        timestamp_trunc(&slice, format.into(), wrap).unwrap(),
                        timestamp_trunc(&clean.slice(2, 2), format.into(), wrap).unwrap()
                    );
                }
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_fine_lower_bound() {
        // First representable UTC boundary for each unit.
        for (format, boundary) in [
            ("MINUTE", -9_223_372_036_800_000_000),
            ("HOUR", -9_223_372_036_800_000_000),
            ("DAY", -9_223_372_022_400_000_000),
        ] {
            for timezone in [None, Some("UTC")] {
                for micros in [i64::MIN, i64::MIN + 1, boundary - 1] {
                    let input =
                        TimestampMicrosecondArray::from(vec![micros]).with_timezone_opt(timezone);
                    assert!(timestamp_trunc(&input, format.into(), true)
                        .unwrap_err()
                        .to_string()
                        .contains("long overflow"));
                }
                let input = TimestampMicrosecondArray::from(vec![boundary, boundary + 1])
                    .with_timezone_opt(timezone);
                let expected = TimestampMicrosecondArray::from(vec![boundary, boundary])
                    .with_timezone_opt(timezone);
                assert_eq!(
                    timestamp_trunc(&input, format.into(), true).unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_fine_spark_wrapping() {
        // Before 4.2, Spark uses unchecked Long subtraction for SECOND/MILLISECOND, even at MIN.
        for (format, expected) in [
            ("SECOND", 9_223_372_036_854_551_616),
            ("MILLISECOND", 9_223_372_036_854_775_616),
        ] {
            for timezone in [None, Some("UTC"), Some("Asia/Kolkata")] {
                let input = TimestampMicrosecondArray::from(vec![Some(i64::MIN), None])
                    .with_timezone_opt(timezone);
                let expected = TimestampMicrosecondArray::from(vec![Some(expected), None])
                    .with_timezone_opt(timezone);
                assert_eq!(
                    timestamp_trunc(&input, format.into(), true).unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn test_timestamp_trunc_fine_spark42_overflow() {
        // Spark 4.2 and later (SPARK-56663) check the SECOND/MILLISECOND subtraction like every
        // other unit. First representable boundary for each unit.
        for (format, boundary) in [
            ("SECOND", -9_223_372_036_854_000_000),
            ("MILLISECOND", -9_223_372_036_854_775_000),
        ] {
            for timezone in [None, Some("UTC"), Some("Asia/Kolkata")] {
                for micros in [i64::MIN, i64::MIN + 1, boundary - 1] {
                    let input = TimestampMicrosecondArray::from(vec![Some(micros), None])
                        .with_timezone_opt(timezone);
                    assert!(timestamp_trunc(&input, format.into(), false)
                        .unwrap_err()
                        .to_string()
                        .contains("long overflow"));
                }
                let input =
                    TimestampMicrosecondArray::from(vec![Some(boundary), Some(boundary + 1), None])
                        .with_timezone_opt(timezone);
                let expected =
                    TimestampMicrosecondArray::from(vec![Some(boundary), Some(boundary), None])
                        .with_timezone_opt(timezone);
                assert_eq!(
                    timestamp_trunc(&input, format.into(), false).unwrap(),
                    expected
                );
            }
        }
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
    fn test_timestamp_trunc_coarse_midnight_gap() {
        // Toronto's gap starts on the previous date at 23:30 and ends at 00:30.
        // atStartOfDay selects 00:30, rather than shifting midnight to 01:00.
        for timezone in [
            "America/Toronto",
            "Canada/Eastern",
            "America/Montreal",
            "America/Nassau",
        ] {
            assert_timestamp_trunc(
                "WEEK",
                Some(timezone),
                &[
                    Some("1919-04-02T16:00:00Z"),
                    Some("1919-03-31T04:30:00Z"),
                    None,
                ],
                &[
                    Some("1919-03-31T04:30:00Z"),
                    Some("1919-03-31T04:30:00Z"),
                    None,
                ],
            );
        }
        // Asuncion's gap starts exactly at midnight, at both a month and quarter boundary.
        for format in ["MONTH", "QUARTER"] {
            assert_timestamp_trunc(
                format,
                Some("America/Asuncion"),
                &[Some("2023-10-15T12:00:00Z"), None],
                &[Some("2023-10-01T04:00:00Z"), None],
            );
        }
    }

    #[test]
    fn test_timestamp_trunc_coarse_midnight_overlap() {
        // Havana repeated local midnight on 2026-11-01. MONTH must use the first occurrence
        // (UTC-4), even though the input is after the fall-back transition and has UTC-5.
        assert_timestamp_trunc(
            "MONTH",
            Some("America/Havana"),
            &[Some("2026-11-15T12:00:00Z")],
            &[Some("2026-11-01T04:00:00Z")],
        );
    }

    #[test]
    fn test_timestamp_trunc_minute_with_historical_offset_seconds() {
        // Africa/Monrovia used UTC-00:44:30 in 1960. The input instant is local 10:30:45,
        // which Spark truncates to local 10:30:00 (11:14:30 UTC), not a UTC minute boundary.
        assert_timestamp_trunc(
            "MINUTE",
            Some("Africa/Monrovia"),
            &[Some("1960-06-15T11:15:15Z"), None],
            &[Some("1960-06-15T11:14:30Z"), None],
        );

        // Asia/Aden changed from UTC+03:06:52 to UTC+03:00 within the local 23:53 minute.
        // Truncating the post-transition 23:53:30 must re-resolve local 23:53:00 with the
        // pre-transition offset, matching ZonedDateTime.truncatedTo.
        assert_timestamp_trunc(
            "MINUTE",
            Some("Asia/Aden"),
            &[Some("1947-03-13T20:53:30.123Z")],
            &[Some("1947-03-13T20:46:08Z")],
        );

        // Monrovia's 1972 transition skipped local 00:00:00 through 00:44:29. Truncating
        // 00:44:45 targets the gap, which ZonedDateTime shifts forward by 44 minutes 30 seconds.
        assert_timestamp_trunc(
            "MINUTE",
            Some("Africa/Monrovia"),
            &[Some("1972-01-07T00:44:45Z")],
            &[Some("1972-01-07T01:28:30Z")],
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

        let result = timestamp_trunc_dyn(&input, "MONTH".to_string(), true).unwrap();
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

    /// Instants around DST and offset transitions, keyed by the session timezone the kernel sees.
    /// `None` is a TIMESTAMP_NTZ array.
    fn transition_instants() -> Vec<(Option<&'static str>, Vec<i64>)> {
        let at = |instants: &[&str]| {
            instants
                .iter()
                .map(|i| instant_micros(i))
                .collect::<Vec<_>>()
        };
        vec![
            // Toronto skipped 1919-03-30 23:30 to 1919-03-31 00:30.
            (
                Some("America/Toronto"),
                at(&[
                    "1919-03-31T04:30:00Z",
                    "1919-03-31T04:45:00Z",
                    "1919-04-02T16:00:00Z",
                ]),
            ),
            // Havana repeated midnight on 2020-11-01.
            (
                Some("America/Havana"),
                at(&[
                    "2020-11-01T04:30:00Z",
                    "2020-11-01T05:30:00Z",
                    "2020-11-15T12:00:00Z",
                ]),
            ),
            // Sao Paulo skipped midnight on 2018-11-04 and repeated 23:00 on 2019-02-16.
            (
                Some("America/Sao_Paulo"),
                at(&[
                    "2018-11-04T03:30:00Z",
                    "2018-11-04T12:00:00Z",
                    "2019-02-17T02:30:00Z",
                ]),
            ),
            (
                Some("America/Los_Angeles"),
                at(&[
                    "2024-03-10T10:30:00Z",
                    "2024-03-10T11:15:30Z",
                    "2024-11-03T08:30:00Z",
                    "2024-11-03T09:30:00Z",
                    "1883-06-15T10:30:45Z",
                ]),
            ),
            // Monrovia used -00:44:30 until 1972.
            (
                Some("Africa/Monrovia"),
                at(&[
                    "1960-06-15T11:15:45Z",
                    "1972-01-07T00:44:29Z",
                    "1972-01-07T00:44:31Z",
                ]),
            ),
            // Asuncion skipped midnight on 2023-10-01, a MONTH and QUARTER boundary.
            (
                Some("America/Asuncion"),
                at(&[
                    "2023-10-01T03:30:00Z",
                    "2023-10-01T04:30:00Z",
                    "2023-10-15T12:00:00Z",
                ]),
            ),
            // Apia skipped 2011-12-30 entirely.
            (
                Some("Pacific/Apia"),
                at(&[
                    "2011-12-30T09:59:59Z",
                    "2011-12-30T10:00:00Z",
                    "2011-12-31T12:00:00Z",
                ]),
            ),
            (
                Some("UTC"),
                at(&["1500-06-15T12:34:56.123456Z", "3333-05-17T12:34:56.123456Z"]),
            ),
            (
                None,
                vec![
                    i64::MAX,
                    i64::MIN + 400 * MICROS_PER_DAY,
                    instant_micros("1969-12-31T23:59:59.999999Z"),
                    instant_micros("2024-05-17T12:34:56.123456Z"),
                ],
            ),
        ]
    }

    #[test]
    fn row_formats_truncate_like_literal_formats() {
        let spellings: Vec<&str> = TIMESTAMP_TRUNC_ALIASES
            .iter()
            .map(|(name, _)| *name)
            .collect();
        for ((timezone, instants), wrap) in transition_instants()
            .into_iter()
            .flat_map(|zone| [(zone.clone(), true), (zone, false)])
        {
            let input = TimestampMicrosecondArray::from(instants).with_timezone_opt(timezone);
            let by_spelling: Vec<TimestampMicrosecondArray> = spellings
                .iter()
                .map(|spelling| timestamp_trunc(&input, spelling.to_string(), wrap).unwrap())
                .collect();
            // Rotate the spellings across the rows so every row meets every format in a column
            // that mixes formats, which exercises the grouping and masking.
            for shift in 0..spellings.len() {
                let pick = |row: usize| (row + shift) % spellings.len();
                let formats = StringArray::from(
                    (0..input.len())
                        .map(|row| spellings[pick(row)])
                        .collect::<Vec<_>>(),
                );
                let result = timestamp_trunc_array_fmt_dyn(&input, &formats, true).unwrap();
                let result = result
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                assert_eq!(result.data_type(), input.data_type());
                for row in 0..input.len() {
                    assert_eq!(
                        result.value(row),
                        by_spelling[pick(row)].value(row),
                        "{timezone:?} {} at {}",
                        spellings[pick(row)],
                        input.value(row)
                    );
                }
            }
        }
    }

    #[test]
    fn row_format_week_resolves_a_midnight_gap_to_its_end() {
        // Toronto's gap ran from 1919-03-30 23:30 to 1919-03-31 00:30, so the Monday that WEEK
        // lands on starts at 00:30. DAY moves its nonexistent midnight forward by the gap to 01:00.
        let input = TimestampMicrosecondArray::from(vec![
            instant_micros("1919-04-02T16:00:00Z"),
            instant_micros("1919-03-31T04:45:00Z"),
        ])
        .with_timezone("America/Toronto");
        let formats = StringArray::from(vec!["WEEK", "DAY"]);
        let result = timestamp_trunc_array_fmt_dyn(&input, &formats, true).unwrap();
        let expected = TimestampMicrosecondArray::from(vec![
            instant_micros("1919-03-31T04:30:00Z"),
            instant_micros("1919-03-31T05:00:00Z"),
        ])
        .with_timezone("America/Toronto");
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap(),
            &expected
        );
    }

    #[test]
    fn row_formats_handle_nulls_like_spark() {
        let input = TimestampMicrosecondArray::from(vec![
            Some(instant_micros("2024-05-17T12:34:56Z")),
            Some(instant_micros("2024-05-17T12:34:56Z")),
            None,
            Some(instant_micros("2024-05-17T12:34:56Z")),
        ])
        .with_timezone("America/Los_Angeles");
        // A NULL format gives NULL, and so does a NULL value, whatever its format says.
        let formats = StringArray::from(vec![Some("YEAR"), None, Some("not_a_unit"), Some("HOUR")]);
        let result = timestamp_trunc_array_fmt_dyn(&input, &formats, true).unwrap();
        let expected = TimestampMicrosecondArray::from(vec![
            Some(instant_micros("2024-01-01T08:00:00Z")),
            None,
            None,
            Some(instant_micros("2024-05-17T12:00:00Z")),
        ])
        .with_timezone("America/Los_Angeles");
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap(),
            &expected
        );

        let formats = StringArray::from(vec!["YEAR", "not_a_unit", "YEAR", "YEAR"]);
        assert!(timestamp_trunc_array_fmt_dyn(&input, &formats, true).is_err());
    }

    #[test]
    fn row_format_groups_do_not_fail_each_other() {
        // SECOND wraps at the lower bound like Spark before 4.2, while YEAR would overflow there.
        // The YEAR group only sees its own row.
        let input =
            TimestampMicrosecondArray::from(vec![i64::MIN, instant_micros("2024-05-17T12:34:56Z")]);
        let formats = StringArray::from(vec!["SECOND", "YEAR"]);
        let result = timestamp_trunc_array_fmt_dyn(&input, &formats, true).unwrap();
        let literal = timestamp_trunc(&input.slice(0, 1), "SECOND".to_string(), true).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(result.value(0), literal.value(0));
        assert_eq!(result.value(1), instant_micros("2024-01-01T00:00:00Z"));

        let formats = StringArray::from(vec!["YEAR", "YEAR"]);
        assert!(timestamp_trunc_array_fmt_dyn(&input, &formats, true).is_err());
    }

    #[test]
    fn row_formats_accept_dictionaries() {
        let instants = [
            instant_micros("2024-03-10T10:30:00Z"),
            instant_micros("2024-11-03T09:30:00Z"),
            instant_micros("2024-11-03T09:30:00Z"),
        ];
        let input =
            TimestampMicrosecondArray::from(instants.to_vec()).with_timezone("America/Los_Angeles");
        let formats = StringArray::from(vec!["DAY", "HOUR", "MONTH"]);
        let expected = timestamp_trunc_array_fmt_dyn(&input, &formats, true).unwrap();

        let keys = Int32Array::from(vec![0, 1, 1]);
        let values = TimestampMicrosecondArray::from(vec![instants[0], instants[1]])
            .with_timezone("America/Los_Angeles");
        let input_dict = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();
        let mut formats_builder = StringDictionaryBuilder::<Int32Type>::new();
        for format in ["DAY", "HOUR", "MONTH"] {
            formats_builder.append(format).unwrap();
        }
        let formats_dict = formats_builder.finish();

        for (array, formats) in [
            (&input_dict as &dyn Array, &formats as &dyn Array),
            (&input as &dyn Array, &formats_dict as &dyn Array),
            (&input_dict as &dyn Array, &formats_dict as &dyn Array),
        ] {
            assert_eq!(
                &timestamp_trunc_array_fmt_dyn(array, formats, true).unwrap(),
                &expected
            );
        }
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
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array, &fmt_array, true) {
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
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array_dict, &fmt_array, true) {
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
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array, &fmt_dict, true) {
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
        if let Ok(a) = timestamp_trunc_array_fmt_dyn(&array_dict, &fmt_dict, true) {
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

    #[test]
    fn row_formats_follow_the_overflow_policy() {
        // Spark before 4.2 wraps SECOND and MILLISECOND below the smallest timestamp, and 4.2 and
        // later raise. Cover a column with one format and one that mixes formats.
        let input = TimestampMicrosecondArray::from(vec![
            i64::MIN,
            i64::MIN,
            instant_micros("2024-05-17T12:34:56Z"),
        ])
        .with_timezone("UTC");
        for formats in [
            vec!["SECOND", "SECOND", "SECOND"],
            vec!["SECOND", "MILLISECOND", "YEAR"],
        ] {
            let formats = StringArray::from(formats);
            assert!(timestamp_trunc_array_fmt_dyn(&input, &formats, true).is_ok());
            let error = timestamp_trunc_array_fmt_dyn(&input, &formats, false).unwrap_err();
            assert!(error.to_string().contains("long overflow"), "{error}");
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

        let result = timestamp_trunc(&array, "QUARTER".to_string(), true).unwrap();

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
