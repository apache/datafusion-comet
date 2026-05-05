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
use arrow::array::{as_string_array, as_struct_array, Array, ArrayRef, StringArray, StringBuilder};
use arrow::array::{RecordBatch, StructArray};
use arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

/// to_csv spark function
#[derive(Debug, Eq)]
pub struct ToCsv {
    expr: Arc<dyn PhysicalExpr>,
    timezone: String,
    csv_write_options: CsvWriteOptions,
}

impl Hash for ToCsv {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.timezone.hash(state);
        self.csv_write_options.hash(state);
    }
}

impl PartialEq for ToCsv {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.timezone.eq(&other.timezone)
            && self.csv_write_options.eq(&other.csv_write_options)
    }
}

impl ToCsv {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        timezone: &str,
        csv_write_options: CsvWriteOptions,
    ) -> Self {
        Self {
            expr,
            timezone: timezone.to_owned(),
            csv_write_options,
        }
    }
}

impl Display for ToCsv {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "to_csv({}, timezone={}, csv_write_options={})",
            self.expr, self.timezone, self.csv_write_options
        )
    }
}

impl PhysicalExpr for ToCsv {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input_array = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        let mut cast_options = SparkCastOptions::new(EvalMode::Legacy, &self.timezone, false);
        cast_options.null_string = self.csv_write_options.null_value.clone();
        let struct_array = as_struct_array(&input_array);

        let csv_array = to_csv_inner(struct_array, &cast_options, &self.csv_write_options)?;

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
            &self.timezone,
            self.csv_write_options.clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

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

    let quote_char = write_options.quote.chars().next().unwrap_or('"');
    let escape_char = write_options.escape.chars().next().unwrap_or('\\');
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

#[inline]
fn escape_value(value: &str, quote_char: char, escape_char: char, output: &mut String) {
    for ch in value.chars() {
        if ch == quote_char || ch == escape_char {
            output.push(escape_char);
        }
        output.push(ch);
    }
}
