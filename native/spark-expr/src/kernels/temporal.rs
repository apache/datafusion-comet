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
    DateTime, Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeZone, Timelike,
};

use std::sync::Arc;

use arrow::array::{
    downcast_dictionary_array, downcast_temporal_array,
    timezone::Tz,
    types::{ArrowDictionaryKeyType, ArrowTemporalType, TimestampMicrosecondType},
    ArrowNumericType,
};
use arrow::{
    array::*,
    datatypes::{DataType, TimeUnit},
};

use crate::utils::resolve_local_datetime;
use crate::SparkError;

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

/// How `date_trunc` truncates a timestamp with a timezone. Spark's `DateTimeUtils.truncTimestamp`
/// treats the levels differently, and matching it matters around DST transitions.
#[derive(Clone, Copy)]
enum TzTrunc {
    /// `MICROSECOND`, `MILLISECOND` and `SECOND`. Offsets are whole seconds, so Spark truncates the
    /// instant itself. The value is the unit in microseconds.
    Instant(i64),
    /// `MINUTE`, `HOUR` and `DAY`. Spark uses `ZonedDateTime.truncatedTo`, which truncates the local
    /// time and keeps the input's offset if the result is ambiguous.
    LocalTime(NtzTruncFn),
    /// `WEEK`, `MONTH`, `QUARTER` and `YEAR`. Spark truncates the local date and then takes
    /// `LocalDate.atStartOfDay`, which uses the earlier offset if midnight is ambiguous.
    LocalDate(NtzTruncFn),
}

/// Truncates `micros` in `tz` the way Spark's `DateTimeUtils.truncTimestamp` does. A truncated
/// local time that falls in a DST gap takes the offset from before the gap, which gives the same
/// instant as Java moving it forward by the gap's length. For the date levels that is also where
/// `atStartOfDay` puts a day whose midnight falls in a gap that starts at midnight. Returns `None`
/// if `micros` is out of chrono's range.
fn trunc_timestamp_in_tz(micros: i64, tz: &Tz, trunc: TzTrunc) -> Option<i64> {
    let (trunc_fn, keep_offset) = match trunc {
        TzTrunc::Instant(unit) => return Some(micros - micros.rem_euclid(unit)),
        TzTrunc::LocalTime(trunc_fn) => (trunc_fn, true),
        TzTrunc::LocalDate(trunc_fn) => (trunc_fn, false),
    };
    let utc = DateTime::from_timestamp_micros(micros)?.naive_utc();
    let input_offset = tz.offset_from_utc_datetime(&utc).fix();
    let local = trunc_fn(utc.checked_add_offset(input_offset)?)?;
    let truncated = match tz.offset_from_local_datetime(&local) {
        LocalResult::Single(offset) => local.checked_sub_offset(offset.fix())?,
        LocalResult::Ambiguous(earlier, later) => {
            let offset = if keep_offset && later.fix() == input_offset {
                later
            } else {
                earlier
            };
            local.checked_sub_offset(offset.fix())?
        }
        LocalResult::None => resolve_local_datetime(tz, local).naive_utc(),
    };
    Some(truncated.and_utc().timestamp_micros())
}

/// Truncates one timezone-aware value and appends it to `builder`.
fn append_trunc_in_tz(
    builder: &mut PrimitiveBuilder<TimestampMicrosecondType>,
    value: Option<i64>,
    tz: &Tz,
    trunc: TzTrunc,
) -> Result<(), SparkError> {
    match value {
        Some(micros) => match trunc_timestamp_in_tz(micros, tz, trunc) {
            Some(truncated) => builder.append_value(truncated),
            None => {
                return Err(SparkError::Internal(
                    "Unable to read value as datetime".to_string(),
                ))
            }
        },
        None => builder.append_null(),
    }
    Ok(())
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

/// Optimized date truncation for Date32 arrays
/// Works directly with days since epoch instead of converting to/from NaiveDateTime
fn date_trunc_date32(array: &Date32Array, format: String) -> Result<Date32Array, SparkError> {
    // Select the truncation function based on format
    let trunc_fn = date_trunc_fn_for_format(&format)?;

    // Apply truncation to each element
    let result: Date32Array = array
        .iter()
        .map(|opt_days| opt_days.and_then(trunc_fn))
        .collect();

    Ok(result)
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

/// The same formats as `TIMESTAMP_TRUNC_FORMATS_NTZ`, for the timezone-aware path.
const TIMESTAMP_TRUNC_FORMATS_TZ: [(&str, TzTrunc); 15] = [
    ("YEAR", TzTrunc::LocalDate(trunc_date_to_year)),
    ("YYYY", TzTrunc::LocalDate(trunc_date_to_year)),
    ("YY", TzTrunc::LocalDate(trunc_date_to_year)),
    ("QUARTER", TzTrunc::LocalDate(trunc_date_to_quarter)),
    ("MONTH", TzTrunc::LocalDate(trunc_date_to_month)),
    ("MON", TzTrunc::LocalDate(trunc_date_to_month)),
    ("MM", TzTrunc::LocalDate(trunc_date_to_month)),
    ("WEEK", TzTrunc::LocalDate(trunc_date_to_week)),
    ("DAY", TzTrunc::LocalTime(trunc_date_to_day)),
    ("DD", TzTrunc::LocalTime(trunc_date_to_day)),
    ("HOUR", TzTrunc::LocalTime(trunc_date_to_hour)),
    ("MINUTE", TzTrunc::LocalTime(trunc_date_to_minute)),
    ("SECOND", TzTrunc::Instant(1_000_000)),
    ("MILLISECOND", TzTrunc::Instant(1_000)),
    ("MICROSECOND", TzTrunc::Instant(1)),
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
fn tz_trunc_for_format(format: &str) -> Result<TzTrunc, SparkError> {
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
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            // TimestampNTZ: operate directly on naive microsecond values without timezone
            timestamp_trunc_ntz(array, format)
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz_str)) => {
            let trunc = tz_trunc_for_format(&format)?;
            let tz: Tz = tz_str.parse()?;
            let mut builder = TimestampMicrosecondBuilder::with_capacity(array.len());
            for value in array.iter() {
                append_trunc_in_tz(&mut builder, value.map(i64::from), &tz, trunc)?;
            }
            Ok(builder.finish().with_timezone(tz_str.as_ref()))
        }
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
                    let trunc = tz_trunc_for_format($formats.value(index))?;
                    append_trunc_in_tz(&mut builder, val.map(|v| i64::from(v)), &tz, trunc)?;
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
        date_trunc, date_trunc_array_fmt_dyn, timestamp_trunc, timestamp_trunc_array_fmt_dyn,
    };
    use arrow::array::{
        builder::{PrimitiveDictionaryBuilder, StringDictionaryBuilder},
        iterator::ArrayIter,
        types::{Date32Type, Int32Type, TimestampMicrosecondType},
        Array, Date32Array, PrimitiveArray, StringArray, TimestampMicrosecondArray,
    };
    use std::sync::Arc;

    #[test]
    #[cfg_attr(miri, ignore)] // test takes too long with miri
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
                _ => unreachable!(),
            }
        }
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
    #[cfg_attr(miri, ignore)] // test takes too long with miri
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
                _ => unreachable!(),
            }
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
    /// Truncation around DST transitions, against java.time, which Spark's `truncTimestamp` uses.
    /// The ambiguous hours check that `MINUTE`, `HOUR` and `DAY` keep the input's offset, and São
    /// Paulo on 2018-11-04 has no midnight.
    #[test]
    fn test_timestamp_trunc_matches_spark_at_dst_transitions() {
        let formats = ["MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR"];
        #[rustfmt::skip]
        let cases: [(&str, i64, [i64; 7]); 7] = [
            // 01:30 PDT on 2024-11-03, the first 01:30 of the fall-back day
            ("America/Los_Angeles", 1_730_622_600_000_000, [1_730_622_600_000_000, 1_730_620_800_000_000,
                1_730_617_200_000_000, 1_730_098_800_000_000, 1_730_444_400_000_000, 1_727_766_000_000_000,
                1_704_096_000_000_000]),
            // 01:30 PST on 2024-11-03, the second 01:30
            ("America/Los_Angeles", 1_730_626_200_000_000, [1_730_626_200_000_000, 1_730_624_400_000_000,
                1_730_617_200_000_000, 1_730_098_800_000_000, 1_730_444_400_000_000, 1_727_766_000_000_000,
                1_704_096_000_000_000]),
            // 03:30 PDT on 2024-03-10, just after the spring-forward gap
            ("America/Los_Angeles", 1_710_066_600_000_000, [1_710_066_600_000_000, 1_710_064_800_000_000,
                1_710_057_600_000_000, 1_709_539_200_000_000, 1_709_280_000_000_000, 1_704_096_000_000_000,
                1_704_096_000_000_000]),
            // 01:39 PST on 1970-10-25, the second 01:39 of that fall-back day
            ("America/Los_Angeles", 25_695_540_000_000, [25_695_540_000_000, 25_693_200_000_000,
                25_686_000_000_000, 25_167_600_000_000, 23_612_400_000_000, 23_612_400_000_000,
                28_800_000_000]),
            // 13:00 on 2018-11-04 in São Paulo, a day whose midnight was skipped
            ("America/Sao_Paulo", 1_541_343_600_000_000, [1_541_343_600_000_000, 1_541_343_600_000_000,
                1_541_300_400_000_000, 1_540_782_000_000_000, 1_541_041_200_000_000, 1_538_362_800_000_000,
                1_514_772_000_000_000]),
            // 23:30 -02:00 on 2019-02-16, the first 23:30 of that fall-back night
            ("America/Sao_Paulo", 1_550_367_000_000_000, [1_550_367_000_000_000, 1_550_365_200_000_000,
                1_550_282_400_000_000, 1_549_850_400_000_000, 1_548_986_400_000_000, 1_546_308_000_000_000,
                1_546_308_000_000_000]),
            // 23:30 -03:00 on 2019-02-16, the second 23:30
            ("America/Sao_Paulo", 1_550_370_600_000_000, [1_550_370_600_000_000, 1_550_368_800_000_000,
                1_550_282_400_000_000, 1_549_850_400_000_000, 1_548_986_400_000_000, 1_546_308_000_000_000,
                1_546_308_000_000_000]),
        ];
        for (tz, micros, expected) in cases {
            let input = TimestampMicrosecondArray::from(vec![micros]).with_timezone(tz);
            for (format, want) in formats.iter().zip(expected) {
                let result = timestamp_trunc(&input, format.to_string()).unwrap();
                assert_eq!(result.value(0), want, "{tz} {micros} {format}");
            }
        }
    }

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
