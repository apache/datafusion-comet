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

use crate::csv_funcs::csv_write_options::CsvWriteOptions;
use crate::{spark_cast, EvalMode, SparkCastOptions};
use datafusion::arrow::array::timezone::Tz;
use arrow::array::{as_string_array, as_struct_array, Array, ArrayRef, Date32Array, Date64Array, ListArray, StringArray, StringBuilder};
use arrow::array::{RecordBatch, StructArray};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use chrono::{DateTime, NaiveDate, Utc};
use datafusion::common::cast::{as_date32_array, as_date64_array, as_timestamp_microsecond_array, as_timestamp_millisecond_array, as_timestamp_nanosecond_array, as_timestamp_second_array};

/// to_csv spark function
#[derive(Debug, Eq)]
pub struct ToCsv {
    expr: Arc<dyn PhysicalExpr>,
    null_as_quoted_empty_string: bool,
    options: CsvWriteOptions,
}

impl Hash for ToCsv {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.null_as_quoted_empty_string.hash(state);
        self.options.hash(state);
    }
}

impl PartialEq for ToCsv {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self
                .null_as_quoted_empty_string
                .eq(&other.null_as_quoted_empty_string)
            && self.options.eq(&other.options)
    }
}

impl ToCsv {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        null_as_quoted_empty_string: bool,
        csv_write_options: CsvWriteOptions,
    ) -> Self {
        Self {
            expr,
            null_as_quoted_empty_string,
            options: csv_write_options,
        }
    }
}

impl Display for ToCsv {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "to_csv({}, null_as_quoted_empty_string={}, csv_write_options={})",
            self.expr, self.null_as_quoted_empty_string, self.options
        )
    }
}

impl PhysicalExpr for ToCsv {
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input_array = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        let mut cast_options =
            SparkCastOptions::new(EvalMode::Legacy, &self.options.timezone, false);
        cast_options.null_string = self.options.null_value.clone();
        let struct_array = as_struct_array(&input_array);

        let csv_array = to_csv_inner(struct_array, &cast_options, &self.options)?;

        Ok(ColumnarValue::Array(csv_array))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.null_as_quoted_empty_string,
            self.options.clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

const TIMESTAMP_FORMAT_DEFAULT: &'static str = "%Y-%m-%dT%H:%M:%S%.3f";

pub fn to_csv_inner(
    array: &StructArray,
    cast_options: &SparkCastOptions,
    write_options: &CsvWriteOptions,
) -> Result<ArrayRef> {
    let string_arrays: Vec<ArrayRef> = as_struct_array(&array)
        .columns()
        .iter()
        .map(|array| {
            spark_cast(
                ColumnarValue::Array(Arc::clone(array)),
                &DataType::Utf8,
                cast_options,
            )?
            .into_array(array.len())
        })
        .collect::<Result<Vec<_>>>()?;
    let string_arrays: Vec<&StringArray> = string_arrays
        .iter()
        .map(|array| as_string_array(array))
        .collect();
    let is_string: Vec<bool> = array
        .fields()
        .iter()
        .map(|f| matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8))
        .collect();

    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 16);
    let mut csv_string = String::with_capacity(array.len() * 16);

    let quote_char = write_options.quote;
    let escape_char = write_options.escape;
    for row_idx in 0..array.len() {
        if array.is_null(row_idx) {
            builder.append_null();
        } else {
            csv_string.clear();
            for (col_idx, column) in string_arrays.iter().enumerate() {
                if col_idx > 0 {
                    csv_string.push_str(&write_options.delimiter);
                }
                if column.is_null(row_idx) {
                    if write_options.quote_all {
                        csv_string.push(quote_char);
                    }
                    csv_string.push_str(&write_options.null_value);
                    if write_options.quote_all {
                        csv_string.push(quote_char);
                    }
                } else {
                    let mut value = column.value(row_idx);
                    let is_string_field = is_string[col_idx];

                    if is_string_field {
                        if write_options.ignore_leading_white_space {
                            value = value.trim_start();
                        }
                        if write_options.ignore_trailing_white_space {
                            value = value.trim_end();
                        }
                    }

                    let needs_quoting = write_options.quote_all
                        || (is_string_field
                            && (value.contains(&write_options.delimiter)
                                || value.contains(quote_char)
                                || value.contains('\n')
                                || value.contains('\r'))
                            || value.is_empty());

                    let needs_escaping = needs_quoting
                        && (value.contains(quote_char) || value.contains(escape_char));

                    if needs_quoting {
                        csv_string.push(quote_char);
                    }
                    if needs_escaping {
                        escape_value(value, quote_char, escape_char, &mut csv_string);
                    } else {
                        csv_string.push_str(value);
                    }
                    if needs_quoting {
                        csv_string.push(quote_char);
                    }
                }
            }
            builder.append_value(&csv_string);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn format_field(
    array: &ArrayRef,
    row_idx: usize,
    data_type: &DataType,
    options: &CsvWriteOptions,
) -> Result<String> {
    match data_type {
        DataType::Timestamp(time_unit, tz_opt) => {
            format_timestamp(array, row_idx, time_unit, tz_opt, &options.timestamp_format, &options.timezone)
        }
        DataType::Date32 => {
            let date32_array = as_date32_array(array)?;
            format_date32(date32_array, row_idx, &options.date_format)
        }
        DataType::Date64 => {
            let date64_array = as_date64_array(array)?;
            format_date64(date64_array, row_idx, &options.date_format)
        }
        DataType::List(field) => {

        }
        _ => unimplemented!(),
    }
}

fn format_list(
    values: &ArrayRef,
    data_type: &DataType,
    options: &CsvWriteOptions
) -> Result<String> {
    let mut list_string = String::from("[");
    for i in 0..values.len() {
        if i > 0 {
            list_string.push(',');
        }
        if values.is_null(i) {
            append_nested_null(&mut output, options);
        }
    }
    list_string.push(']');
    Ok(list_string)
}

fn format_date32(array: &Date32Array, row_idx: usize, date_format: &String) -> Result<String> {
    let days = array.value(row_idx);
    let date = NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days as i64)))
        .ok_or_else(|| DataFusionError::Execution(format!("Date32 value out of range: {days}")))?;
    Ok(date.format(date_format).to_string())
}

fn format_date64(array: &Date64Array, row_idx: usize, date_format: &String) -> Result<String> {
    let millis = array.value(row_idx);
    let secs = millis.div_euclid(1_000);
    let date = DateTime::<Utc>::from_timestamp(secs, 0)
        .map(|dt| dt.date_naive())
        .ok_or_else(|| {
            DataFusionError::Execution(format!("Date64 value out of range: {millis}"))
        })?;
    Ok(date.format(date_format).to_string())
}

fn format_timestamp(
    array: &ArrayRef,
    row_idx: usize,
    time_unit: &TimeUnit,
    tz_opt: &Option<Arc<str>>,
    timestamp_format: &str,
    timezone: &str,
) -> Result<String> {
    let micros = match time_unit {
        TimeUnit::Second => {
            let ts_sec_array = as_timestamp_second_array(array)?;
            ts_sec_array.value(row_idx) * 1_000_000
        }
        TimeUnit::Microsecond => {
            let ts_micros_array = as_timestamp_microsecond_array(array)?;
            ts_micros_array.value(row_idx)
        }
        TimeUnit::Nanosecond => {
            let ts_nanos_array = as_timestamp_nanosecond_array(array)?;
            ts_nanos_array.value(row_idx) / 1_000
        }
        TimeUnit::Millisecond => {
            let ts_millis_array = as_timestamp_millisecond_array(array)?;
            ts_millis_array.value(row_idx) * 1_000
        }
    };

    let secs = micros.div_euclid(1_000_000);
    let nanos = (micros.rem_euclid(1_000_000) * 1_000) as u32;
    let is_default_format = timestamp_format == TIMESTAMP_FORMAT_DEFAULT;

    if tz_opt.is_some() {
        let tz: Tz = timezone.parse().map_err(|e| {
            DataFusionError::Execution(format!("Invalid session timezone '{timezone}': {e}"))
        })?;
        let utc_dt = DateTime::<Utc>::from_timestamp(secs, nanos).ok_or_else(|| {
            DataFusionError::Execution(format!("Timestamp out of range: {micros}"))
        })?;
        let local_dt = utc_dt.with_timezone(&tz);
        if is_default_format {
            Ok(local_dt
                .format("%Y-%m-%dT%H:%M:%S%.3f%:z")
                .to_string()
                .replace("+00:00", "Z"))
        } else {
            Ok(local_dt.format(timestamp_format).to_string())
        }
    } else {
        let naive = DateTime::from_timestamp(secs, nanos)
            .map(|dt| dt.naive_utc())
            .ok_or_else(|| {
                DataFusionError::Execution(format!("Timestamp out of range: {micros}"))
            })?;
        Ok(naive.format(timestamp_format).to_string())
    }
}

fn format_binary(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut output = String::from("[");
    for (i, byte) in value.iter().enumerate() {
        if i > 0 {
            output.push(' ');
        }
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output.push(']');
    output
}

macro_rules! format_float {
    ($value:expr) => {{
        let v = $value;
        if v.is_nan() {
            "NaN".to_string()
        } else if v.is_infinite() {
            if v.is_sign_positive() {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            }
        } else {
            v.to_string()
        }
    }};
}

fn trim_value(mut value: &str, options: &CsvWriteOptions) -> String {
    if options.ignore_leading_white_space {
        value = value.trim_start();
    }
    if options.ignore_trailing_white_space {
        value = value.trim_end();
    }
    value.to_string()
}

fn quote_value(value: &str, options: &CsvWriteOptions) -> String {
    let contains_quote = value.contains(options.quote);
    let needs_quoting = options.quote_all
        || value.contains(&options.delimiter)
        || value.contains('\n')
        || value.contains('\r')
        || (options.escape_quotes && contains_quote);

    if needs_quoting {
        let mut output = String::with_capacity(value.len() + 2);
        output.push(options.quote);
        for ch in value.chars() {
            if ch == options.quote || ch == options.escape {
                output.push(options.escape);
            }
            output.push(ch);
        }
        output.push(options.quote);
        output
    } else {
        value.to_string()
    }
}
