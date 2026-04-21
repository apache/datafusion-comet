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

#![allow(deprecated)]

use crate::kernels::strings::substring;
use arrow::array::{as_dictionary_array, as_largestring_array, as_string_array, Array, ArrayRef};
use arrow::datatypes::{DataType, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct SubstringExpr {
    pub child: Arc<dyn PhysicalExpr>,
    pub start: i64,
    pub len: u64,
}

impl Hash for SubstringExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.start.hash(state);
        self.len.hash(state);
    }
}

impl PartialEq for SubstringExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.start.eq(&other.start) && self.len.eq(&other.len)
    }
}

impl SubstringExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, start: i64, len: u64) -> Self {
        Self { child, start, len }
    }
}

impl Display for SubstringExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Substring [start: {}, len: {}, child: {}]",
            self.start, self.len, self.child
        )
    }
}

impl PhysicalExpr for SubstringExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion::common::Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let result = if self.start < 0 {
                    // Spark and Arrow differ for negative start: Arrow clamps
                    // start to 0 then takes `len` chars, but Spark computes
                    // end = unclamped_start + len, then clamps both independently.
                    spark_substring_negative_start(&array, self.start, self.len)?
                } else {
                    substring(&array, self.start, self.len)?
                };
                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "Substring(scalar) should be fold in Spark JVM side.".to_string(),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(SubstringExpr::new(
            Arc::clone(&children[0]),
            self.start,
            self.len,
        )))
    }
}

/// Implement Spark's substring semantics for negative start positions.
/// Spark: start = numChars + pos, end = start + len, clamp both, empty if start >= end.
/// Arrow: start = max(0, numChars + pos), take len chars — differs when start is clamped.
fn spark_substring_negative_start(
    array: &ArrayRef,
    start: i64,
    len: u64,
) -> datafusion::common::Result<ArrayRef> {
    use arrow::array::{DictionaryArray, GenericStringBuilder};

    match array.data_type() {
        DataType::Utf8 => {
            let str_array = as_string_array(array);
            let mut builder = GenericStringBuilder::<i32>::new();
            for i in 0..str_array.len() {
                if str_array.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(spark_substr_negative(str_array.value(i), start, len));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let str_array = as_largestring_array(array);
            let mut builder = GenericStringBuilder::<i64>::new();
            for i in 0..str_array.len() {
                if str_array.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(spark_substr_negative(str_array.value(i), start, len));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(array);
            let values = spark_substring_negative_start(dict.values(), start, len)?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result) as ArrayRef)
        }
        _ => Ok(Arc::clone(array)),
    }
}

fn spark_substr_negative(s: &str, pos: i64, len: u64) -> String {
    let num_chars = s.chars().count() as i64;
    let start = num_chars + pos;
    let end = start.saturating_add(len as i64).min(num_chars);
    let start = start.max(0);

    if start >= end {
        return String::new();
    }

    s.chars()
        .skip(start as usize)
        .take((end - start) as usize)
        .collect()
}
