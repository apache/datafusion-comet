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

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType::Timestamp;
use arrow::datatypes::TimeUnit::Microsecond;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone};
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

/// VERY SMALL subset Spark → chrono
fn spark_to_chrono(fmt: &str) -> String {
    fmt.replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
}

fn format_has_time(fmt: &str) -> bool {
    fmt.contains("%H") || fmt.contains("%M") || fmt.contains("%S")
}

fn parse_date_or_timestamp(value: &str, format: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    if format_has_time(format) {
        NaiveDateTime::parse_from_str(value, format)
    } else {
        let date: NaiveDate = NaiveDate::parse_from_str(value, format)?;
        Ok(date.and_hms_opt(0, 0, 0).unwrap())
    }
}

fn to_utc_micros(naive: NaiveDateTime, tz: Tz) -> Option<i64> {
    let local: DateTime<Tz> = tz.from_local_datetime(&naive).single()?;
    Some(local.with_timezone(&tz).timestamp_micros())
}

fn spark_to_timestamp_parse(value: &str, format: &str, tz: Tz) -> Result<i64> {
    let result: NaiveDateTime = parse_date_or_timestamp(value, format).map_err(|_| {
        DataFusionError::Plan(format!("Error parsing '{value}' with format'{format}'."))
    })?;
    to_utc_micros(result, tz)
        .ok_or_else(|| DataFusionError::Plan(format!("Error using the timezone {tz}.")))
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
    let format: String = spark_to_chrono(format);
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
