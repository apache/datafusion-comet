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
    let escaper = FieldEscaper::new(&write_options.delimiter, quote_char, escape_char);
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
                        || value.is_empty()
                        || (is_string_field && escaper.forces_quoting(value));

                    if needs_quoting {
                        csv_string.push(quote_char);
                        escaper.write_escaped(value, &mut csv_string);
                        csv_string.push(quote_char);
                    } else {
                        csv_string.push_str(value);
                    }
                }
            }
            builder.append_value(&csv_string);
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Decides whether a CSV field must be quoted, and escapes the content of quoted fields.
///
/// The delimiter, quote and escape characters are fixed for a whole batch, so they are
/// classified once up front. When the delimiter is a single ASCII byte and the quote and
/// escape characters are both ASCII (by far the most common configuration) a value can be
/// inspected one byte at a time: no ASCII byte can occur inside a multi-byte UTF-8 sequence,
/// so byte equality is equivalent to character equality, and run boundaries always fall on
/// `char` boundaries. Otherwise we fall back to substring/char searches.
enum FieldEscaper<'a> {
    Ascii {
        delimiter: u8,
        quote: u8,
        escape: u8,
    },
    General {
        delimiter: &'a str,
        quote: char,
        escape: char,
    },
}

impl<'a> FieldEscaper<'a> {
    fn new(delimiter: &'a str, quote: char, escape: char) -> Self {
        match *delimiter.as_bytes() {
            [d] if d.is_ascii() && quote.is_ascii() && escape.is_ascii() => Self::Ascii {
                delimiter: d,
                quote: quote as u8,
                escape: escape as u8,
            },
            _ => Self::General {
                delimiter,
                quote,
                escape,
            },
        }
    }

    /// Whether `value` contains a character that forces the field to be quoted.
    #[inline]
    fn forces_quoting(&self, value: &str) -> bool {
        match *self {
            Self::Ascii {
                delimiter, quote, ..
            } => value
                .as_bytes()
                .iter()
                .any(|&b| b == delimiter || b == quote || b == b'\n' || b == b'\r'),
            Self::General {
                delimiter, quote, ..
            } => {
                value.contains(delimiter)
                    || value.contains(quote)
                    || value.contains('\n')
                    || value.contains('\r')
            }
        }
    }

    /// Appends `value` to `output`, prefixing every quote and escape character with the
    /// escape character. Runs between escapes are copied in bulk rather than one character
    /// at a time.
    #[inline]
    fn write_escaped(&self, value: &str, output: &mut String) {
        let mut run_start = 0;
        match *self {
            Self::Ascii { quote, escape, .. } => {
                for (i, &b) in value.as_bytes().iter().enumerate() {
                    if b == quote || b == escape {
                        output.push_str(&value[run_start..i]);
                        output.push(escape as char);
                        run_start = i;
                    }
                }
            }
            Self::General { quote, escape, .. } => {
                for (i, ch) in value.char_indices() {
                    if ch == quote || ch == escape {
                        output.push_str(&value[run_start..i]);
                        output.push(escape);
                        run_start = i;
                    }
                }
            }
        }
        output.push_str(&value[run_start..]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_csv(values: Vec<Option<&str>>, write_options: &CsvWriteOptions) -> Vec<Option<String>> {
        let strings: ArrayRef = Arc::new(StringArray::from(values));
        let struct_array = StructArray::from(vec![(
            Arc::new(arrow::datatypes::Field::new("f1", DataType::Utf8, true)),
            strings,
        )]);
        let mut cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
        cast_options.null_string = write_options.null_value.clone();
        let result = to_csv_inner(&struct_array, &cast_options, write_options).unwrap();
        let result = as_string_array(&result);
        (0..result.len())
            .map(|i| {
                if result.is_null(i) {
                    None
                } else {
                    Some(result.value(i).to_string())
                }
            })
            .collect()
    }

    fn options(delimiter: &str, quote: &str, escape: &str) -> CsvWriteOptions {
        CsvWriteOptions::new(
            delimiter.to_string(),
            quote.to_string(),
            escape.to_string(),
            "".to_string(),
            false,
            true,
            true,
        )
    }

    #[test]
    fn test_quoting_and_escaping() {
        let actual = to_csv(
            vec![
                Some("plain"),
                Some("a,b"),
                Some("say \"hi\""),
                Some("back\\slash"),
                Some("line\nbreak"),
                Some(""),
                None,
            ],
            &options(",", "\"", "\\"),
        );
        assert_eq!(
            actual,
            vec![
                Some("plain".to_string()),
                Some("\"a,b\"".to_string()),
                Some("\"say \\\"hi\\\"\"".to_string()),
                Some("back\\slash".to_string()),
                Some("\"line\nbreak\"".to_string()),
                Some("\"\"".to_string()),
                Some("".to_string()),
            ]
        );
    }

    #[test]
    fn test_multi_byte_quote_and_multi_char_delimiter() {
        let actual = to_csv(
            vec![Some("a||b"), Some("qu§ote"), Some("plain")],
            &options("||", "§", "é"),
        );
        assert_eq!(
            actual,
            vec![
                Some("§a||b§".to_string()),
                Some("§qué§ote§".to_string()),
                Some("plain".to_string()),
            ]
        );
    }
}
