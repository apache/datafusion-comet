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
    delimiter: String,
    quote: String,
    escape: String,
    null_value: String,
    timezone: String,
    quote_all: bool,
}

impl Hash for ToCsv {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.delimiter.hash(state);
        self.quote.hash(state);
        self.escape.hash(state);
        self.null_value.hash(state);
        self.timezone.hash(state);
        self.quote_all.hash(state);
    }
}

impl PartialEq for ToCsv {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.delimiter.eq(&other.delimiter)
            && self.quote.eq(&other.quote)
            && self.escape.eq(&other.escape)
            && self.null_value.eq(&other.null_value)
            && self.timezone.eq(&other.timezone)
            && self.quote_all.eq(&other.quote_all)
    }
}

impl ToCsv {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        delimiter: &str,
        quote: &str,
        escape: &str,
        null_value: &str,
        timezone: &str,
        quote_all: bool,
    ) -> Self {
        Self {
            expr,
            delimiter: delimiter.to_owned(),
            quote: quote.to_owned(),
            escape: escape.to_owned(),
            null_value: null_value.to_owned(),
            timezone: timezone.to_owned(),
            quote_all,
        }
    }
}

impl Display for ToCsv {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "to_csv({}, delimiter={}, quote={}, escape={}, null_value={}, quote_all={}, timezone={})",
            self.expr, self.delimiter, self.quote, self.escape, self.null_value, self.quote_all, self.timezone
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
        cast_options.null_string = self.null_value.clone();
        let struct_array = as_struct_array(&input_array);

        let csv_array = to_csv_inner(
            struct_array,
            &cast_options,
            &self.delimiter,
            &self.quote,
            &self.escape,
            self.quote_all,
        )?;

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
            &self.delimiter,
            &self.quote,
            &self.escape,
            &self.null_value,
            &self.timezone,
            self.quote_all,
        )))
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

pub fn to_csv_inner(
    array: &StructArray,
    cast_options: &SparkCastOptions,
    delimiter: &str,
    quote: &str,
    escape: &str,
    quote_all: bool,
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

    for row_idx in 0..array.len() {
        if array.is_null(row_idx) {
            builder.append_null();
        } else {
            csv_string.clear();
            for (col_idx, column) in string_arrays.iter().enumerate() {
                if col_idx > 0 {
                    csv_string.push_str(delimiter);
                }
                let value = column.value(row_idx);
                let is_string_field = is_string[col_idx];

                let needs_quoting = quote_all
                    || (is_string_field && (value.contains(delimiter) || value.contains(quote)));

                let needs_escaping = is_string_field && needs_quoting;
                if needs_quoting {
                    csv_string.push_str(quote);
                }
                if needs_escaping {
                    escape_value(value, quote, escape, &mut csv_string);
                } else {
                    csv_string.push_str(value);
                }
                if needs_quoting {
                    csv_string.push_str(quote);
                }
            }
            builder.append_value(&csv_string);
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[inline]
fn escape_value(value: &str, quote: &str, escape: &str, output: &mut String) {
    for ch in value.chars() {
        let ch_str = ch.to_string();
        if ch_str == quote || ch_str == escape {
            output.push_str(escape);
        }
        output.push(ch);
    }
}
