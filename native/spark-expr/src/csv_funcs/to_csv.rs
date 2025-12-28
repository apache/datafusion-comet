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

use arrow::array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray,
    StringArray, StringBuilder,
};
use arrow::array::{RecordBatch, StructArray};
use arrow::datatypes::{DataType, Schema};
use datafusion::common::{exec_err, Result};
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
        expr: Arc<dyn PhysicalExpr>,
        delimiter: &str,
        quote: &str,
        escape: &str,
        null_value: &str,
    ) -> Self {
        Self {
            expr,
            delimiter: delimiter.to_owned(),
            quote: quote.to_owned(),
            escape: escape.to_owned(),
            null_value: null_value.to_owned(),
        }
    }
}

impl Display for ToCsv {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "to_csv({}, delimiter={}, quote={}, escape={}, null_value={})",
            self.expr, self.delimiter, self.quote, self.escape, self.null_value
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

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input_value = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;

        let struct_array = input_value
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("A StructType is expected");

        let result = struct_to_csv(struct_array, &self.delimiter, &self.null_value)?;

        Ok(ColumnarValue::Array(result))
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
        )))
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

fn struct_to_csv(array: &StructArray, delimiter: &str, null_value: &str) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 16);
    let mut csv_string = String::with_capacity(array.len() * 16);
    for row_idx in 0..array.len() {
        if array.is_null(row_idx) {
            builder.append_null();
        } else {
            csv_string.clear();
            for (col_idx, column) in array.columns().iter().enumerate() {
                if col_idx > 0 {
                    csv_string.push_str(delimiter);
                }
                if column.is_null(row_idx) {
                    csv_string.push_str(null_value);
                } else {
                    let value = convert_to_string(column, row_idx)?;
                    csv_string.push_str(&value);
                }
            }
        }
        builder.append_value(&csv_string);
    }
    Ok(Arc::new(builder.finish()))
}

fn convert_to_string(array: &ArrayRef, row_idx: usize) -> Result<String> {
    match array.data_type() {
        DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(array.value(row_idx).to_string())
        }
        DataType::Int8 => {
            let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(array.value(row_idx).to_string())
        }
        DataType::Int16 => {
            let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(array.value(row_idx).to_string())
        }
        DataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(array.value(row_idx).to_string())
        }
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(array.value(row_idx).to_string())
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(array.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(array.value(row_idx).to_string())
        }
        _ => exec_err!("to_csv not implemented for type: {:?}", array.data_type()),
    }
}

#[cfg(test)]
mod tests {
    use crate::csv_funcs::to_csv::struct_to_csv;
    use arrow::array::{as_string_array, ArrayRef, Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::Result;
    use std::sync::Arc;

    #[test]
    fn test_to_csv_basic() -> Result<()> {
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("foo"), None, Some("baz")])) as ArrayRef,
            ),
        ]);

        let expected = &StringArray::from(vec!["1,foo", "2,", "3,baz"]);

        let result = struct_to_csv(&Arc::new(struct_array), ",", "")?;
        let result = as_string_array(&result);

        assert_eq!(result, expected);

        Ok(())
    }
}
