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

use crate::conversion_funcs::cast_string_to_date;
use crate::EvalMode;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Timestamp;
use arrow::datatypes::TimeUnit::Microsecond;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use chrono_tz::Tz;
use datafusion::common::internal_err;
use datafusion::common::{DataFusionError, Result};
use datafusion::functions::utils::make_scalar_function;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use std::str::FromStr;
use std::sync::Arc;

const TO_TIMESTAMP: &str = "to_timestamp";
const TO_DATE: &str = "to_date";

#[derive(Debug, Clone, Copy, PartialEq)]
enum FractionPrecision {
    Millis,
    Micros,
    Nanos,
}

/// Detect Spark fractional precision from a format string.
fn detect_fraction_precision(fmt: &str) -> Option<FractionPrecision> {
    let count = fmt.chars().filter(|&c| c == 'S').count();
    match count {
        0 => None,
        1..=3 => Some(FractionPrecision::Millis),
        4..=6 => Some(FractionPrecision::Micros),
        _ => Some(FractionPrecision::Nanos),
    }
}

/// The set of Spark/Java SimpleDateFormat tokens that this implementation
/// can reliably convert to chrono strftime patterns.  Any token outside this
/// set would pass through unconverted and silently produce wrong results, so
/// we reject the format early and let the caller fall back to Spark.
const SUPPORTED_SPARK_TOKENS: &[&str] = &[
    "yyyy",
    "MM",
    "dd",
    "HH",
    "mm",
    "ss",
    ".SSSSSSSSS",
    ".SSSSSS",
    ".SSS",
    "XXX",
    "Z",
];

/// Return `true` when every alphabetic run in `fmt` is covered by one of the
/// supported tokens.  Literal separators (`-`, `/`, ` `, `:`, `.`, `T`, `'`)
/// are allowed.
fn is_supported_spark_format(fmt: &str) -> bool {
    // Build a scratch copy, blank-out every known token, then check that no
    // unrecognised letter remains.
    let mut scratch = fmt.to_string();
    for tok in SUPPORTED_SPARK_TOKENS {
        scratch = scratch.replace(tok, &" ".repeat(tok.len()));
    }
    // After blanking known tokens, only separators / whitespace / digits
    // should remain.  Any remaining ASCII letter means an unsupported token.
    !scratch.chars().any(|c| c.is_ascii_alphabetic())
}

/// Convert a Spark/Java SimpleDateFormat pattern to a chrono strftime pattern.
///
/// Returns `None` when the format contains tokens that this implementation
/// does not support, so the caller can fall back to Spark.
fn spark_to_chrono(fmt: &str) -> Option<(String, Option<FractionPrecision>)> {
    if !is_supported_spark_format(fmt) {
        return None;
    }

    let precision = detect_fraction_precision(fmt);

    let mut out = fmt.to_string();

    // Date
    out = out.replace("yyyy", "%Y");
    out = out.replace("MM", "%m");
    out = out.replace("dd", "%d");

    // Time
    out = out.replace("HH", "%H");
    out = out.replace("mm", "%M");
    out = out.replace("ss", "%S");

    // Fractions — longest match first to avoid partial replacement
    out = out
        .replace(".SSSSSSSSS", "%.f")
        .replace(".SSSSSS", "%.f")
        .replace(".SSS", "%.f");

    // Timezones
    out = out.replace("XXX", "%:z");
    out = out.replace("Z", "%z");

    Some((out, precision))
}

/// Returns true when the Spark format string contains a time component.
fn spark_format_has_time(fmt: &str) -> bool {
    fmt.contains("HH") || fmt.contains("mm") || fmt.contains("ss")
}

/// Parse a string value using a Spark format, returning a `NaiveDateTime`.
/// Date-only formats are expanded to midnight.
///
/// Returns a `DataFusionError` when the format contains unsupported tokens
/// or when the value cannot be parsed.
fn parse_spark_naive(
    value: &str,
    spark_fmt: &str,
) -> Result<(NaiveDateTime, Option<FractionPrecision>), DataFusionError> {
    let (chrono_fmt, precision) = spark_to_chrono(spark_fmt).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Unsupported Spark format pattern '{spark_fmt}': \
             contains tokens not handled by the native implementation"
        ))
    })?;

    if spark_format_has_time(spark_fmt) {
        let ts = NaiveDateTime::parse_from_str(value, &chrono_fmt).map_err(|_| {
            DataFusionError::Plan(format!("Error parsing '{value}' with format '{spark_fmt}'"))
        })?;
        Ok((ts, precision))
    } else {
        let date = NaiveDate::parse_from_str(value, &chrono_fmt).map_err(|_| {
            DataFusionError::Plan(format!("Error parsing '{value}' with format '{spark_fmt}'"))
        })?;
        Ok((date.and_hms_opt(0, 0, 0).unwrap(), precision))
    }
}

/// Truncate sub-second precision to what the Spark format string actually represents.
fn normalize_fraction(
    mut ts: NaiveDateTime,
    precision: Option<FractionPrecision>,
) -> Option<NaiveDateTime> {
    match precision {
        Some(FractionPrecision::Millis) => {
            let ms = ts.and_utc().timestamp_subsec_millis();
            ts = ts.with_nanosecond(ms * 1_000_000)?;
        }
        Some(FractionPrecision::Micros) => {
            let us = ts.and_utc().timestamp_subsec_micros();
            ts = ts.with_nanosecond(us * 1_000)?;
        }
        Some(FractionPrecision::Nanos) | None => {}
    }
    Some(ts)
}

/// Parse a string using a Spark format pattern and timezone, returning UTC microseconds.
pub fn spark_to_timestamp_parse(
    value: &str,
    spark_fmt: &str,
    tz: Tz,
) -> Result<i64, DataFusionError> {
    let (naive, precision) = parse_spark_naive(value, spark_fmt)?;

    let naive = normalize_fraction(naive, precision)
        .ok_or_else(|| DataFusionError::Plan("Invalid fractional timestamp".into()))?;

    let local: DateTime<Tz> = tz.from_local_datetime(&naive).single().ok_or_else(|| {
        DataFusionError::Plan(format!("Ambiguous or invalid datetime in timezone {tz}"))
    })?;

    Ok(local.timestamp_micros())
}

// to_timestamp — As a Comet scalar UDF
pub fn to_timestamp(args: &[ColumnarValue], fail_on_error: bool) -> Result<ColumnarValue> {
    make_scalar_function(
        move |input_args| spark_to_timestamp(input_args, fail_on_error),
        vec![],
    )(args)
}

/// Core implementation of `to_timestamp(value, format[, timezone])`.
///
/// Accepts a string, date, or timestamp column as the first argument.
/// Date and timestamp inputs are cast to `TimestampMicrosecond` via Arrow before parsing,
/// matching Spark's behaviour for `GetTimestamp`.
pub fn spark_to_timestamp(args: &[ArrayRef], fail_on_error: bool) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return internal_err!(
            "`{}` function requires 2 or 3 arguments, got {} arguments",
            TO_TIMESTAMP,
            args.len()
        );
    }

    // Normalise the first argument to StringArray; Date32 / Timestamp inputs are
    // cast to string first so they can be re-parsed with the supplied format.
    let input_array = normalise_to_string(&args[0])?;
    let dates: &StringArray = input_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "`{TO_TIMESTAMP}`: first argument must be a string, date, or timestamp column"
            ))
        })?;

    let format_array: &StringArray =
        args[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "`{TO_TIMESTAMP}`: second argument (format) must be a string column"
                ))
            })?;

    let tz: Tz = args
        .get(2)
        .and_then(|arg| arg.as_any().downcast_ref::<StringArray>())
        .and_then(|v| {
            if v.is_null(0) {
                None
            } else {
                Tz::from_str(v.value(0)).ok()
            }
        })
        .unwrap_or(Tz::UTC);

    let utc_tz: Arc<str> = Arc::from(chrono_tz::UTC.name());

    let values: Result<Vec<ScalarValue>> = dates
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let format = if format_array.len() == 1 {
                if format_array.is_null(0) {
                    None
                } else {
                    Some(format_array.value(0))
                }
            } else if format_array.is_null(index) {
                None
            } else {
                Some(format_array.value(index))
            };

            match (value, format) {
                (None, _) | (_, None) => ScalarValue::Int64(None)
                    .cast_to(&Timestamp(Microsecond, Some(Arc::clone(&utc_tz)))),
                (Some(date_raw), Some(format)) => {
                    let parsed_value = spark_to_timestamp_parse(date_raw, format, tz);

                    match (parsed_value, fail_on_error) {
                        (Ok(v), _) => ScalarValue::Int64(Some(v))
                            .cast_to(&Timestamp(Microsecond, Some(Arc::clone(&utc_tz)))),
                        (Err(err), true) => Err(err),
                        (Err(_), false) => ScalarValue::Int64(None)
                            .cast_to(&Timestamp(Microsecond, Some(Arc::clone(&utc_tz)))),
                    }
                }
            }
        })
        .collect::<Result<Vec<ScalarValue>>>();

    let output: ArrayRef = ScalarValue::iter_to_array(values?)?;
    Ok(output)
}

// to_date — As a Comet scalar UDF

pub fn to_date(args: &[ColumnarValue], fail_on_error: bool) -> Result<ColumnarValue> {
    make_scalar_function(
        move |input_args| spark_to_date(input_args, fail_on_error),
        vec![],
    )(args)
}

/// Core implementation of `to_date(value[, format])`.
///
/// - Without format: delegates to `cast_string_to_date` (Spark's default ISO parsing).
/// - With format: parses via `spark_to_timestamp_parse`, then drops the time component.
/// - Date inputs pass through unchanged; Timestamp inputs are truncated to date.
pub fn spark_to_date(args: &[ArrayRef], fail_on_error: bool) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 2 {
        return internal_err!(
            "`{}` function requires 1 or 2 arguments, got {} arguments",
            TO_DATE,
            args.len()
        );
    }

    let eval_mode = if fail_on_error {
        EvalMode::Ansi
    } else {
        EvalMode::Legacy
    };

    // Fast path: Date32 input → return as-is.
    if args[0].data_type() == &DataType::Date32 {
        return Ok(Arc::clone(&args[0]));
    }

    // Fast path: Timestamp input without format → cast to Date32 via Arrow.
    if matches!(args[0].data_type(), DataType::Timestamp(_, _)) && args.len() == 1 {
        let date_array = cast(&args[0], &DataType::Date32)?;
        return Ok(date_array);
    }

    // Normalise input to StringArray for all remaining paths.
    let input_array = normalise_to_string(&args[0])?;

    if args.len() == 1 {
        // No format — use the existing Spark-compatible ISO parser.
        return cast_string_to_date(&input_array, &DataType::Date32, eval_mode)
            .map_err(DataFusionError::from);
    }

    // With format — parse via spark_to_timestamp_parse then truncate to date.
    let format_array: &StringArray =
        args[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "`{TO_DATE}`: second argument (format) must be a string column"
                ))
            })?;

    let string_array: &StringArray = input_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "`{TO_DATE}`: first argument must be a string, date, or timestamp"
            ))
        })?;

    let values: Result<Vec<ScalarValue>> = string_array
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let format = if format_array.len() == 1 {
                if format_array.is_null(0) {
                    None
                } else {
                    Some(format_array.value(0))
                }
            } else if format_array.is_null(index) {
                None
            } else {
                Some(format_array.value(index))
            };

            match (value, format) {
                (None, _) | (_, None) => Ok(ScalarValue::Date32(None)),
                (Some(date_raw), Some(format)) => {
                    let parsed = spark_to_timestamp_parse(date_raw, format, Tz::UTC);
                    match (parsed, fail_on_error) {
                        (Ok(micros), _) => {
                            // Convert UTC microseconds to days since epoch.
                            // Use div_euclid (floor division) so that pre-epoch
                            // timestamps with non-midnight times round toward
                            // negative infinity instead of toward zero.
                            let secs = micros.div_euclid(1_000_000);
                            let days = secs.div_euclid(86_400) as i32;
                            Ok(ScalarValue::Date32(Some(days)))
                        }
                        (Err(err), true) => Err(err),
                        (Err(_), false) => Ok(ScalarValue::Date32(None)),
                    }
                }
            }
        })
        .collect::<Result<Vec<ScalarValue>>>();

    let output: ArrayRef = ScalarValue::iter_to_array(values?)?;
    Ok(output)
}

// Helpers

/// Convert a Date32 or Timestamp array to a StringArray using Arrow's built-in cast,
/// so that string-based parsers can operate on all supported input types.
fn normalise_to_string(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => Ok(Arc::clone(array)),
        DataType::Date32 | DataType::Timestamp(_, _) => {
            cast(array, &DataType::Utf8).map_err(DataFusionError::from)
        }
        other => internal_err!("Unsupported input type for to_timestamp/to_date: {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Date32Array, StringArray, TimestampMicrosecondArray};
    use chrono::{NaiveDate, NaiveDateTime};
    use chrono_tz::UTC;

    #[test]
    fn detects_no_fraction() {
        assert_eq!(detect_fraction_precision("yyyy-MM-dd HH:mm:ss"), None);
    }

    #[test]
    fn detects_millis_precision() {
        assert_eq!(
            detect_fraction_precision("yyyy-MM-dd HH:mm:ss.SSS"),
            Some(FractionPrecision::Millis)
        );
    }

    #[test]
    fn detects_micros_precision() {
        assert_eq!(
            detect_fraction_precision("yyyy-MM-dd HH:mm:ss.SSSSSS"),
            Some(FractionPrecision::Micros)
        );
    }

    #[test]
    fn detects_nanos_precision() {
        assert_eq!(
            detect_fraction_precision("yyyy-MM-dd HH:mm:ss.SSSSSSSSS"),
            Some(FractionPrecision::Nanos)
        );
    }

    #[test]
    fn converts_basic_date_format() {
        let (fmt, precision) = spark_to_chrono("yyyy-MM-dd").unwrap();
        assert_eq!(fmt, "%Y-%m-%d");
        assert_eq!(precision, None);
    }

    #[test]
    fn converts_timestamp_with_millis() {
        let (fmt, precision) = spark_to_chrono("yyyy-MM-dd HH:mm:ss.SSS").unwrap();

        assert_eq!(fmt, "%Y-%m-%d %H:%M:%S%.f");
        assert_eq!(precision, Some(FractionPrecision::Millis));
    }

    #[test]
    fn converts_timestamp_with_timezone() {
        let (fmt, _) = spark_to_chrono("yyyy-MM-dd HH:mm:ssXXX").unwrap();

        assert_eq!(fmt, "%Y-%m-%d %H:%M:%S%:z");
    }

    #[test]
    fn rejects_unsupported_format_tokens() {
        // Single-digit month (M), 2-digit year (yy), AM/PM (a), day-of-week (E)
        assert!(spark_to_chrono("yyyy-M-dd").is_none());
        assert!(spark_to_chrono("yy-MM-dd").is_none());
        assert!(spark_to_chrono("yyyy-MM-dd hh:mm:ss a").is_none());
        assert!(spark_to_chrono("EEE, yyyy-MM-dd").is_none());
    }

    #[test]
    fn detects_date_only_format() {
        assert!(!spark_format_has_time("yyyy-MM-dd"));
    }

    #[test]
    fn detects_timestamp_format() {
        assert!(spark_format_has_time("yyyy-MM-dd HH:mm:ss"));
    }

    #[test]
    fn parses_date_as_midnight_timestamp() {
        let (ts, _) = parse_spark_naive("2026-01-30", "yyyy-MM-dd").unwrap();

        let expected = NaiveDate::from_ymd_opt(2026, 1, 30)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        assert_eq!(ts, expected);
    }

    #[test]
    fn parses_timestamp_with_millis() {
        let (ts, precision) =
            parse_spark_naive("2026-01-30 10:30:52.123", "yyyy-MM-dd HH:mm:ss.SSS").unwrap();

        assert_eq!(precision, Some(FractionPrecision::Millis));
        assert_eq!(ts.and_utc().timestamp_subsec_millis(), 123);
    }

    #[test]
    fn normalizes_millis_precision() {
        let ts =
            NaiveDateTime::parse_from_str("2026-01-30 10:30:52.123456", "%Y-%m-%d %H:%M:%S%.6f")
                .unwrap();

        let normalized = normalize_fraction(ts, Some(FractionPrecision::Millis)).unwrap();

        assert_eq!(normalized.and_utc().timestamp_subsec_nanos(), 123_000_000);
    }

    #[test]
    fn normalizes_micros_precision() {
        let ts =
            NaiveDateTime::parse_from_str("2026-01-30 10:30:52.123456", "%Y-%m-%d %H:%M:%S%.6f")
                .unwrap();

        let normalized = normalize_fraction(ts, Some(FractionPrecision::Micros)).unwrap();

        assert_eq!(normalized.and_utc().timestamp_subsec_nanos(), 123_456_000);
    }

    #[test]
    fn parses_timestamp_and_preserves_millis() {
        let micros =
            spark_to_timestamp_parse("2026-01-30 10:30:52.123", "yyyy-MM-dd HH:mm:ss.SSS", UTC)
                .unwrap();

        let expected =
            NaiveDateTime::parse_from_str("2026-01-30 10:30:52.123", "%Y-%m-%d %H:%M:%S%.3f")
                .unwrap()
                .and_utc()
                .timestamp_micros();

        assert_eq!(micros, expected);
    }

    #[test]
    fn parses_date_literal_as_midnight() {
        let micros = spark_to_timestamp_parse("2026-01-30", "yyyy-MM-dd", UTC).unwrap();

        let expected = NaiveDate::from_ymd_opt(2026, 1, 30)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();

        assert_eq!(micros, expected);
    }

    #[test]
    fn supports_two_arguments_without_timezone() {
        let dates: ArrayRef =
            Arc::new(StringArray::from(vec![Some("2026-01-30 10:30:52")])) as ArrayRef;
        let formats: ArrayRef =
            Arc::new(StringArray::from(vec![Some("yyyy-MM-dd HH:mm:ss")])) as ArrayRef;

        let result = spark_to_timestamp(&[dates, formats], true).unwrap();
        let ts = result
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert!(!ts.is_null(0));
    }

    #[test]
    fn returns_null_on_parse_error_when_fail_on_error_is_false() {
        let dates: ArrayRef = Arc::new(StringArray::from(vec![Some("malformed")])) as ArrayRef;
        let formats: ArrayRef = Arc::new(StringArray::from(vec![Some("yyyy-MM-dd")])) as ArrayRef;

        let result = spark_to_timestamp(&[dates, formats], false).unwrap();
        let ts = result
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert!(ts.is_null(0));
    }

    #[test]
    fn to_date_with_format_returns_date32() {
        let dates: ArrayRef = Arc::new(StringArray::from(vec![Some("2026/01/30")])) as ArrayRef;
        let formats: ArrayRef = Arc::new(StringArray::from(vec![Some("yyyy/MM/dd")])) as ArrayRef;

        let result = spark_to_date(&[dates, formats], false).unwrap();
        let arr = result.as_any().downcast_ref::<Date32Array>().unwrap();

        assert!(!arr.is_null(0));
        // 2026-01-30 = days since epoch
        let expected = NaiveDate::from_ymd_opt(2026, 1, 30)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        assert_eq!(arr.value(0), expected);
    }

    #[test]
    fn to_date_no_format_returns_date32() {
        let dates: ArrayRef = Arc::new(StringArray::from(vec![Some("2026-01-30")])) as ArrayRef;

        let result = spark_to_date(&[dates], false).unwrap();
        let arr = result.as_any().downcast_ref::<Date32Array>().unwrap();

        assert!(!arr.is_null(0));
    }

    #[test]
    fn to_date_null_input_returns_null() {
        let dates: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef;

        let result = spark_to_date(&[dates], false).unwrap();
        let arr = result.as_any().downcast_ref::<Date32Array>().unwrap();

        assert!(arr.is_null(0));
    }

    #[test]
    fn to_date_malformed_returns_null_when_not_fail_on_error() {
        let dates: ArrayRef = Arc::new(StringArray::from(vec![Some("not-a-date")])) as ArrayRef;
        let formats: ArrayRef = Arc::new(StringArray::from(vec![Some("yyyy-MM-dd")])) as ArrayRef;

        let result = spark_to_date(&[dates, formats], false).unwrap();
        let arr = result.as_any().downcast_ref::<Date32Array>().unwrap();

        assert!(arr.is_null(0));
    }

    #[test]
    fn to_date_date32_input_passes_through() {
        let days = NaiveDate::from_ymd_opt(2026, 1, 30)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        let input: ArrayRef = Arc::new(Date32Array::from(vec![Some(days)])) as ArrayRef;

        let result = spark_to_date(&[input], false).unwrap();
        let arr = result.as_any().downcast_ref::<Date32Array>().unwrap();

        assert_eq!(arr.value(0), days);
    }

    #[test]
    fn to_date_pre_epoch_with_non_midnight_time() {
        // "1969-12-31 12:00:00" should map to day -1 (1969-12-31), not day 0 (1970-01-01).
        let dates: ArrayRef =
            Arc::new(StringArray::from(vec![Some("1969-12-31 12:00:00")])) as ArrayRef;
        let formats: ArrayRef =
            Arc::new(StringArray::from(vec![Some("yyyy-MM-dd HH:mm:ss")])) as ArrayRef;

        let result = spark_to_date(&[dates, formats], false).unwrap();
        let arr = result.as_any().downcast_ref::<Date32Array>().unwrap();

        let expected_days = NaiveDate::from_ymd_opt(1969, 12, 31)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        assert_eq!(expected_days, -1);
        assert_eq!(arr.value(0), expected_days);
    }

    #[test]
    fn to_date_pre_epoch_midnight() {
        // "1969-12-31" at midnight should map to day -1.
        let dates: ArrayRef = Arc::new(StringArray::from(vec![Some("1969-12-31")])) as ArrayRef;
        let formats: ArrayRef = Arc::new(StringArray::from(vec![Some("yyyy-MM-dd")])) as ArrayRef;

        let result = spark_to_date(&[dates, formats], false).unwrap();
        let arr = result.as_any().downcast_ref::<Date32Array>().unwrap();

        assert_eq!(arr.value(0), -1);
    }

    #[test]
    fn to_date_unsupported_format_returns_null_when_not_fail_on_error() {
        // Format with single-digit month (M) is unsupported and should produce null.
        let dates: ArrayRef = Arc::new(StringArray::from(vec![Some("2026-1-30")])) as ArrayRef;
        let formats: ArrayRef = Arc::new(StringArray::from(vec![Some("yyyy-M-dd")])) as ArrayRef;

        let result = spark_to_date(&[dates, formats], false).unwrap();
        let arr = result.as_any().downcast_ref::<Date32Array>().unwrap();

        assert!(arr.is_null(0));
    }

    #[test]
    fn to_timestamp_unsupported_format_returns_null_when_not_fail_on_error() {
        let dates: ArrayRef =
            Arc::new(StringArray::from(vec![Some("2026-1-30 10:30:52")])) as ArrayRef;
        let formats: ArrayRef =
            Arc::new(StringArray::from(vec![Some("yyyy-M-dd HH:mm:ss")])) as ArrayRef;

        let result = spark_to_timestamp(&[dates, formats], false).unwrap();
        let ts = result
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert!(ts.is_null(0));
    }

    #[test]
    fn is_supported_spark_format_accepts_valid_formats() {
        assert!(is_supported_spark_format("yyyy-MM-dd"));
        assert!(is_supported_spark_format("yyyy-MM-dd HH:mm:ss"));
        assert!(is_supported_spark_format("yyyy-MM-dd HH:mm:ss.SSS"));
        assert!(is_supported_spark_format("yyyy/MM/dd"));
        assert!(is_supported_spark_format("yyyy-MM-dd HH:mm:ssXXX"));
    }

    #[test]
    fn is_supported_spark_format_rejects_unsupported() {
        assert!(!is_supported_spark_format("yyyy-M-dd"));
        assert!(!is_supported_spark_format("yy-MM-dd"));
        assert!(!is_supported_spark_format("yyyy-MM-dd hh:mm:ss a"));
        assert!(!is_supported_spark_format("EEE, yyyy-MM-dd"));
        assert!(!is_supported_spark_format("yyyy-MM-dd'T'HH:mm:ss"));
    }
}
