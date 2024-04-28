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

use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::errors::{CometError, CometResult};
use arrow::{
    compute::{cast_with_options, CastOptions},
    datatypes::TimestampMillisecondType,
    record_batch::RecordBatch,
    util::display::FormatOptions,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, GenericStringArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{internal_err, Result as DataFusionResult, ScalarValue};
use datafusion_physical_expr::PhysicalExpr;
use regex::Regex;

use crate::execution::datafusion::expressions::utils::{
    array_with_timezone, down_cast_any_ref, spark_cast,
};

static TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");
static CAST_OPTIONS: CastOptions = CastOptions {
    safe: true,
    format_options: FormatOptions::new()
        .with_timestamp_tz_format(TIMESTAMP_FORMAT)
        .with_timestamp_format(TIMESTAMP_FORMAT),
};

#[derive(Debug, Hash, PartialEq, Clone, Copy)]
pub enum EvalMode {
    Legacy,
    Ansi,
    Try,
}

#[derive(Debug, Hash)]
pub struct Cast {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub eval_mode: EvalMode,

    /// When cast from/to timezone related types, we need timezone, which will be resolved with
    /// session local timezone by an analyzer in Spark.
    pub timezone: String,
}

// It will be useful if we want to extend support to various timestamp formats
// right now it is millisecond timestamp
macro_rules! cast_utf8_to_timestamp {
    ($array:expr, $eval_mode:expr, $array_type:ty, $cast_method:ident) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len);
        for i in 0..len {
            if $array.is_null(i) {
                cast_array.append_null()
            } else if let Some(cast_value) = $cast_method($array.value(i).trim(), $eval_mode)? {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        let result: CometResult<ArrayRef> = Ok(Arc::new(cast_array.finish()) as ArrayRef);
        result.unwrap()
    }};
}

impl Cast {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        eval_mode: EvalMode,
        timezone: String,
    ) -> Self {
        Self {
            child,
            data_type,
            timezone,
            eval_mode,
        }
    }

    pub fn new_without_timezone(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        eval_mode: EvalMode,
    ) -> Self {
        Self {
            child,
            data_type,
            timezone: "".to_string(),
            eval_mode,
        }
    }

    fn cast_array(&self, array: ArrayRef) -> DataFusionResult<ArrayRef> {
        let to_type = &self.data_type;
        let array = array_with_timezone(array, self.timezone.clone(), Some(to_type));
        let from_type = array.data_type();
        let cast_result = match (from_type, to_type) {
            (DataType::Utf8, DataType::Boolean) => {
                Self::spark_cast_utf8_to_boolean::<i32>(&array, self.eval_mode)?
            }
            (DataType::LargeUtf8, DataType::Boolean) => {
                Self::spark_cast_utf8_to_boolean::<i64>(&array, self.eval_mode)?
            }
            (DataType::UInt8, DataType::Timestamp(_, _)) => {
                println!("Casting UInt8 to Timestamp");
                Self::cast_string_to_timestamp(&array, to_type, self.eval_mode)?
            }
            _ => cast_with_options(&array, to_type, &CAST_OPTIONS)?,
        };
        let result = spark_cast(cast_result, from_type, to_type);
        Ok(result)
    }

    fn cast_string_to_timestamp(
        array: &ArrayRef,
        to_type: &DataType,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef> {
        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("Expected a string array");

        let cast_array: ArrayRef = match to_type {
            DataType::Timestamp(_, _) => {
                cast_utf8_to_timestamp!(
                    string_array,
                    eval_mode,
                    TimestampMillisecondType,
                    parse_timestamp
                )
            }
            _ => unreachable!("Invalid data type {:?} in cast from string", to_type),
        };
        Ok(cast_array)
    }

    fn spark_cast_utf8_to_boolean<OffsetSize>(
        from: &dyn Array,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
    where
        OffsetSize: OffsetSizeTrait,
    {
        let array = from
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .unwrap();

        let output_array = array
            .iter()
            .map(|value| match value {
                Some(value) => match value.to_ascii_lowercase().trim() {
                    "t" | "true" | "y" | "yes" | "1" => Ok(Some(true)),
                    "f" | "false" | "n" | "no" | "0" => Ok(Some(false)),
                    _ if eval_mode == EvalMode::Ansi => Err(CometError::CastInvalidValue {
                        value: value.to_string(),
                        from_type: "STRING".to_string(),
                        to_type: "BOOLEAN".to_string(),
                    }),
                    _ => Ok(None),
                },
                _ => Ok(None),
            })
            .collect::<Result<BooleanArray, _>>()?;

        Ok(Arc::new(output_array))
    }
}

impl Display for Cast {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cast [data_type: {}, timezone: {}, child: {}, eval_mode: {:?}]",
            self.data_type, self.timezone, self.child, &self.eval_mode
        )
    }
}

impl PartialEq<dyn Any> for Cast {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.child.eq(&x.child)
                    && self.timezone.eq(&x.timezone)
                    && self.data_type.eq(&x.data_type)
                    && self.eval_mode.eq(&x.eval_mode)
            })
            .unwrap_or(false)
    }
}

impl PhysicalExpr for Cast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(self.cast_array(array)?)),
            ColumnarValue::Scalar(scalar) => {
                // Note that normally CAST(scalar) should be fold in Spark JVM side. However, for
                // some cases e.g., scalar subquery, Spark will not fold it, so we need to handle it
                // here.
                let array = scalar.to_array()?;
                let scalar = ScalarValue::try_from_array(&self.cast_array(array)?, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(Cast::new(
                children[0].clone(),
                self.data_type.clone(),
                self.eval_mode,
                self.timezone.clone(),
            ))),
            _ => internal_err!("Cast should have exactly one child"),
        }
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.data_type.hash(&mut s);
        self.timezone.hash(&mut s);
        self.eval_mode.hash(&mut s);
        self.hash(&mut s);
    }
}

fn parse_timestamp(value: &str, eval_mode: EvalMode) -> CometResult<Option<i64>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }

    // Define regex patterns and corresponding parsing functions
    let patterns = &[
        (
            Regex::new(r"^\d{4}$").unwrap(),
            parse_str_to_year_timestamp as fn(&str) -> CometResult<Option<i64>>,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}$").unwrap(),
            parse_str_to_month_timestamp,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap(),
            parse_str_to_day_timestamp,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{1,2}$").unwrap(),
            parse_str_to_hour_timestamp,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$").unwrap(),
            parse_str_to_minute_timestamp,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$").unwrap(),
            parse_str_to_second_timestamp,
        ),
        (
            Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}$").unwrap(),
            parse_str_to_nanosecond_timestamp,
        ),
        (
            Regex::new(r"^T\d{1,2}$").unwrap(),
            parse_str_to_time_only_timestamp,
        ),
    ];

    let mut timestamp = None;

    // Iterate through patterns and try matching
    for (pattern, parse_func) in patterns {
        if pattern.is_match(value) {
            timestamp = parse_func(value)?;
            break;
        }
    }

    if eval_mode == EvalMode::Ansi && timestamp.is_none() {
        return Err(CometError::CastInvalidValue {
            value: value.to_string(),
            from_type: "STRING".to_string(),
            to_type: "TIMESTAMP".to_string(),
        });
    }

    Ok(Some(timestamp.unwrap()))
}

fn parse_ymd_timestamp(year: i32, month: u32, day: u32) -> CometResult<Option<i64>> {
    let datetime = chrono::NaiveDate::from_ymd_opt(year, month, day);
    let timestamp = datetime.unwrap().and_hms_milli_opt(0, 0, 0, 0);
    Ok(Some(timestamp.unwrap().and_utc().timestamp_millis()))
}

fn parse_hms_timestamp(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    millisecond: u32,
) -> CometResult<Option<i64>> {
    let datetime = chrono::NaiveDate::from_ymd_opt(year, month, day);
    let timestamp = datetime
        .unwrap()
        .and_hms_nano_opt(hour, minute, second, millisecond);
    Ok(Some(timestamp.unwrap().and_utc().timestamp_millis()))
}

fn get_timestamp_values(value: &str, timestamp_type: &str) -> CometResult<Option<i64>> {
    let values: Vec<_> = value
        .split(|c| c == 'T' || c == '-' || c == ':' || c == '.')
        .collect();
    let year = values[0].parse::<i32>().unwrap_or_default();
    let month = values.get(1).map_or(1, |m| m.parse::<u32>().unwrap_or(1));
    let day = values.get(2).map_or(1, |d| d.parse::<u32>().unwrap_or(1));
    let hour = values.get(3).map_or(0, |h| h.parse::<u32>().unwrap_or(0));
    let minute = values.get(4).map_or(0, |m| m.parse::<u32>().unwrap_or(0));
    let second = values.get(5).map_or(0, |s| s.parse::<u32>().unwrap_or(0));
    let millisecond = values.get(6).map_or(0, |ms| ms.parse::<u32>().unwrap_or(0));

    match timestamp_type {
        "year" => parse_ymd_timestamp(year, 1, 1),
        "month" => parse_ymd_timestamp(year, month, 1),
        "day" => parse_ymd_timestamp(year, month, day),
        "hour" => parse_hms_timestamp(year, month, day, hour, 0, 0, 0),
        "minute" => parse_hms_timestamp(year, month, day, hour, minute, 0, 0),
        "second" => parse_hms_timestamp(year, month, day, hour, minute, second, 0),
        "millisecond" => parse_hms_timestamp(year, month, day, hour, minute, second, millisecond),
        _ => Err(CometError::CastInvalidValue {
            value: value.to_string(),
            from_type: "STRING".to_string(),
            to_type: "TIMESTAMP".to_string(),
        }),
    }
}

fn parse_str_to_year_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "year")
}

fn parse_str_to_month_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "month")
}

fn parse_str_to_day_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "day")
}

fn parse_str_to_hour_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "hour")
}

fn parse_str_to_minute_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "minute")
}

fn parse_str_to_second_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "second")
}

fn parse_str_to_nanosecond_timestamp(value: &str) -> CometResult<Option<i64>> {
    get_timestamp_values(value, "millisecond")
}

fn parse_str_to_time_only_timestamp(value: &str) -> CometResult<Option<i64>> {
    let values: Vec<_> = value.split('T').collect();
    let time_values: Vec<_> = values[1].split(':').collect();
    let hour = time_values[0].parse::<u32>().unwrap();
    let minute = time_values
        .get(1)
        .map_or(0, |m| m.parse::<u32>().unwrap_or(0));
    let second = time_values
        .get(2)
        .map_or(0, |s| s.parse::<u32>().unwrap_or(0));
    let millisecond = time_values
        .get(3)
        .map_or(0, |ms| ms.parse::<u32>().unwrap_or(0));
    let datetime = chrono::Local::now().to_utc().date_naive();
    let timestamp = datetime.and_hms_milli_opt(hour, minute, second, millisecond);
    Ok(Some(timestamp.unwrap().and_utc().timestamp_millis()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_timestamp_test() {
        // write for all formats
        assert_eq!(
            parse_timestamp("2020", EvalMode::Legacy).unwrap(),
            Some(1577836800000)
        );
        assert_eq!(
            parse_timestamp("2020-01", EvalMode::Legacy).unwrap(),
            Some(1577836800000)
        );
        assert_eq!(
            parse_timestamp("2020-01-01", EvalMode::Legacy).unwrap(),
            Some(1577836800000)
        );
        assert_eq!(
            parse_timestamp("2020-01-01T12", EvalMode::Legacy).unwrap(),
            Some(1577880000000)
        );
        assert_eq!(
            parse_timestamp("2020-01-01T12:34", EvalMode::Legacy).unwrap(),
            Some(1577882040000)
        );
        assert_eq!(
            parse_timestamp("2020-01-01T12:34:56", EvalMode::Legacy).unwrap(),
            Some(1577882096000)
        );
        assert_eq!(
            parse_timestamp("2020-01-01T12:34:56.123456", EvalMode::Legacy).unwrap(),
            Some(1577882096000)
        );
        assert_eq!(
            parse_timestamp("T2", EvalMode::Legacy).unwrap(),
            Some(1714183200000)
        );
    }
}
