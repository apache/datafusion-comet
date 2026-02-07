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

//! Define JNI APIs which can be called from Java/Scala.

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::DataType::Timestamp;
use arrow::datatypes::TimeUnit::Microsecond;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use chrono_tz::Tz;
use datafusion::common::{DataFusionError, Result};
use datafusion::functions::downcast_named_arg;
use datafusion::functions::utils::make_scalar_function;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use datafusion_common;
use datafusion_common::internal_err;
use std::str::FromStr;
use std::sync::Arc;

const TO_TIMESTAMP: &str = "custom_to_timestamp";

#[derive(Debug, Clone, Copy)]
#[derive(PartialEq)]
enum FractionPrecision {
    Millis,
    Micros,
    Nanos,
}

/// Detect Spark fractional precision
fn detect_fraction_precision(fmt: &str) -> Option<FractionPrecision> {
    let count = fmt.chars().filter(|&c| c == 'S').count();
    match count {
        0 => None,
        1..=3 => Some(FractionPrecision::Millis),
        4..=6 => Some(FractionPrecision::Micros),
        _ => Some(FractionPrecision::Nanos),
    }
}

/// Convert Spark → Chrono format
fn spark_to_chrono(fmt: &str) -> (String, Option<FractionPrecision>) {
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

    // Fractions
    out = out
        .replace(".SSSSSSSSS", "%.f")
        .replace(".SSSSSS", "%.f")
        .replace(".SSS", "%.f");

    // Timezones
    out = out.replace("XXX", "%:z");
    out = out.replace("Z", "%z");

    (out, precision)
}

/// Detect if Spark format contains time
fn spark_format_has_time(fmt: &str) -> bool {
    fmt.contains("HH") || fmt.contains("mm") || fmt.contains("ss")
}

/// Parse Spark date or timestamp
fn parse_spark_naive(
    value: &str,
    spark_fmt: &str,
) -> Result<(NaiveDateTime, Option<FractionPrecision>), chrono::ParseError> {
    let (chrono_fmt, precision) = spark_to_chrono(spark_fmt);

    if spark_format_has_time(spark_fmt) {
        let ts = NaiveDateTime::parse_from_str(value, &chrono_fmt)?;
        Ok((ts, precision))
    } else {
        let date = NaiveDate::parse_from_str(value, &chrono_fmt)?;
        Ok((date.and_hms_opt(0, 0, 0).unwrap(), precision))
    }
}

/// Normalize fractional seconds
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

/// Final Spark-like timestamp parse → UTC micros
pub fn spark_to_timestamp_parse(
    value: &str,
    spark_fmt: &str,
    tz: Tz,
) -> Result<i64, DataFusionError> {
    let (naive, precision) = parse_spark_naive(value, spark_fmt).map_err(|_| {
        DataFusionError::Plan(format!("Error parsing '{value}' with format '{spark_fmt}'"))
    })?;

    let naive = normalize_fraction(naive, precision)
        .ok_or_else(|| DataFusionError::Plan("Invalid fractional timestamp".into()))?;

    let local: DateTime<Tz> = tz.from_local_datetime(&naive).single().ok_or_else(|| {
        DataFusionError::Plan(format!("Ambiguous or invalid datetime in timezone {tz}"))
    })?;

    Ok(local.timestamp_micros())
}

pub fn custom_to_timestamp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    make_scalar_function(spark_custom_to_timestamp, vec![])(&args)
}

pub fn spark_custom_to_timestamp(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return internal_err!(
            "`{}` function requires 2 or 3 arguments, got {} arguments",
            TO_TIMESTAMP,
            args.len()
        );
    }
    let dates: &StringArray = downcast_named_arg!(&args[0], "date", StringArray);
    let format: &str = downcast_named_arg!(&args[1], "format", StringArray).value(0);
    let tz: Tz = opt_downcast_arg!(&args[2], StringArray)
        .and_then(|v| Tz::from_str(v.value(0)).ok())
        .unwrap_or(Tz::UTC);

    let utc_tz: String = chrono_tz::UTC.to_string();
    let utc_tz: Arc<str> = Arc::from(utc_tz);
    let values: Result<Vec<ScalarValue>> = dates
        .iter()
        .map(|value| match value {
            None => ScalarValue::Int64(None).cast_to(&Timestamp(Microsecond, Some(utc_tz.clone()))),
            Some(date_raw) => {
                let parsed_value: Result<i64> = spark_to_timestamp_parse(date_raw, &format, tz);

                ScalarValue::Int64(Some(parsed_value?))
                    .cast_to(&Timestamp(Microsecond, Some(utc_tz.clone())))
            }
        })
        .collect::<Result<Vec<ScalarValue>>>();

    let scalar_values: Vec<ScalarValue> = values?;
    let decimal_array: ArrayRef = ScalarValue::iter_to_array(scalar_values)?;

    Ok(decimal_array)
}

macro_rules! opt_downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>()
    }};
}

pub(crate) use opt_downcast_arg;



#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime};
    use chrono_tz::UTC;

    // ----------------------------
    // detect_fraction_precision
    // ----------------------------

    #[test]
    fn detects_no_fraction() {
        assert_eq!(
            detect_fraction_precision("yyyy-MM-dd HH:mm:ss"),
            None
        );
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

    // ----------------------------
    // spark_to_chrono
    // ----------------------------

    #[test]
    fn converts_basic_date_format() {
        let (fmt, precision) = spark_to_chrono("yyyy-MM-dd");
        assert_eq!(fmt, "%Y-%m-%d");
        assert_eq!(precision, None);
    }

    #[test]
    fn converts_timestamp_with_millis() {
        let (fmt, precision) =
            spark_to_chrono("yyyy-MM-dd HH:mm:ss.SSS");

        assert_eq!(fmt, "%Y-%m-%d %H:%M:%S%.f");
        assert_eq!(precision, Some(FractionPrecision::Millis));
    }

    #[test]
    fn converts_timestamp_with_timezone() {
        let (fmt, _) =
            spark_to_chrono("yyyy-MM-dd HH:mm:ssXXX");

        assert_eq!(fmt, "%Y-%m-%d %H:%M:%S%:z");
    }

    // ----------------------------
    // spark_format_has_time
    // ----------------------------

    #[test]
    fn detects_date_only_format() {
        assert!(!spark_format_has_time("yyyy-MM-dd"));
    }

    #[test]
    fn detects_timestamp_format() {
        assert!(spark_format_has_time("yyyy-MM-dd HH:mm:ss"));
    }

    // ----------------------------
    // parse_spark_naive
    // ----------------------------

    #[test]
    fn parses_date_as_midnight_timestamp() {
        let (ts, _) = parse_spark_naive(
            "2026-01-30",
            "yyyy-MM-dd",
        )
            .unwrap();

        let expected =
            NaiveDate::from_ymd_opt(2026, 1, 30)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();

        assert_eq!(ts, expected);
    }

    #[test]
    fn parses_timestamp_with_millis() {
        let (ts, precision) = parse_spark_naive(
            "2026-01-30 10:30:52.123",
            "yyyy-MM-dd HH:mm:ss.SSS",
        )
            .unwrap();

        assert_eq!(precision, Some(FractionPrecision::Millis));
        assert_eq!(ts.and_utc().timestamp_subsec_millis(), 123);
    }

    // ----------------------------
    // normalize_fraction
    // ----------------------------

    #[test]
    fn normalizes_millis_precision() {
        let ts = NaiveDateTime::parse_from_str(
            "2026-01-30 10:30:52.123456",
            "%Y-%m-%d %H:%M:%S%.6f",
        )
            .unwrap();

        let normalized =
            normalize_fraction(ts, Some(FractionPrecision::Millis))
                .unwrap();

        assert_eq!(
            normalized.and_utc().timestamp_subsec_nanos(),
            123_000_000
        );
    }

    #[test]
    fn normalizes_micros_precision() {
        let ts = NaiveDateTime::parse_from_str(
            "2026-01-30 10:30:52.123456",
            "%Y-%m-%d %H:%M:%S%.6f",
        )
            .unwrap();

        let normalized =
            normalize_fraction(ts, Some(FractionPrecision::Micros))
                .unwrap();

        assert_eq!(
            normalized.and_utc().timestamp_subsec_nanos(),
            123_456_000
        );
    }

    // ----------------------------
    // spark_to_timestamp_parse (end-to-end)
    // ----------------------------

    #[test]
    fn parses_timestamp_and_preserves_millis() {
        let micros = spark_to_timestamp_parse(
            "2026-01-30 10:30:52.123",
            "yyyy-MM-dd HH:mm:ss.SSS",
            UTC,
        )
            .unwrap();

        let expected = NaiveDateTime::parse_from_str(
            "2026-01-30 10:30:52.123",
            "%Y-%m-%d %H:%M:%S%.3f",
        )
            .unwrap()
            .and_utc()
            .timestamp_micros();

        assert_eq!(micros, expected);
    }

    #[test]
    fn parses_date_literal_as_midnight() {
        let micros = spark_to_timestamp_parse(
            "2026-01-30",
            "yyyy-MM-dd",
            UTC,
        )
            .unwrap();

        let expected = NaiveDate::from_ymd_opt(2026, 1, 30)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();

        assert_eq!(micros, expected);
    }
}
