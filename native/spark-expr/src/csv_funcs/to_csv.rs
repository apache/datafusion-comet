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

use arrow::array::{RecordBatch, StringBuilder, StructArray};
use arrow::array::{Array, ArrayRef, StringArray};
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
    delimiter: char,
    quote: char,
    escape: char,
    null_value: String,
}

impl Hash for ToCsv {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.delimiter.hash(state);
        self.quote.hash(state);
        self.escape.hash(state);
        self.null_value.hash(state);
    }
}

impl PartialEq for ToCsv {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.delimiter.eq(&other.delimiter)
            && self.quote.eq(&other.quote)
            && self.escape.eq(&other.escape)
            && self.null_value.eq(&other.null_value)
    }
}

impl ToCsv {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>, delimiter: char,
        quote: char,
        escape: char,
        null_value: String
    ) -> Self {
        Self { expr, delimiter, quote, escape, null_value }
    }
}

impl Display for ToCsv {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

impl PhysicalExpr for ToCsv {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input_value = self.expr.evaluate(batch)?;

        let array = match input_value {
            ColumnarValue::Array(arr) => arr,
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let mut builder = StringBuilder::with_capacity(array.len(), 1024);

        for row_idx in 0..array.len() {
            if array.is_null(row_idx) {
                builder.append_null();
            } else {
                let csv_string = struct_to_csv(&array, row_idx)?;
                builder.append_value(&csv_string);
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        todo!()
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

fn struct_to_csv(array: &StructArray, row_idx: usize) -> Result<String> {
    use arrow::array::StructArray;

    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let mut csv_string = String::new();
    let num_columns = struct_array.num_columns();

    for (col_idx, column) in struct_array.columns().iter().enumerate() {
        if col_idx > 0 {
            csv_string.push(self.options.delimiter);
        }

        if column.is_null(row_idx) {
            csv_string.push_str(&self.options.null_value);
        } else {
            let value = self.format_value(column, row_idx)?;
            let escaped = self.escape_value(&value);
            csv_string.push_str(&escaped);
        }
    }

    Ok(csv_string)
}

fn format_value(array: &ArrayRef, row_idx: usize) -> Result<String> {
    use arrow::array::*;

    match array.data_type() {
        DataType::Null => Ok(self.options.null_value.clone()),
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = arr.value(row_idx);
            Ok(format!("{:?}", bytes))
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Timestamp(unit, tz) => {
            let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Decimal128(precision, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let value = arr.value(row_idx);
            let divisor = 10_i128.pow(*scale as u32);
            let integer_part = value / divisor;
            let fractional_part = (value % divisor).abs();
            Ok(format!("{}.{:0width$}", integer_part, fractional_part, width = *scale as usize))
        }
        DataType::List(_) | DataType::LargeList(_) => {
            // Для массивов рекурсивно форматируем
            Ok(format!("[...]")) // Упрощенная версия
        }
        DataType::Struct(_) => {
            // Вложенные структуры - рекурсивный вызов
            self.struct_to_csv(array, row_idx)
        }
        _ => Err(DataFusionError::NotImplemented(
            format!("to_csv not implemented for type: {:?}", array.data_type())
        )),
    }
}
