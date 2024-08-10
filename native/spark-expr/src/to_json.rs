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

// TODO upstream this to DataFusion as long as we have a way to specify all
// of the Spark-specific compatibility features that we need (including
// being able to specify Spark-compatible cast from all types to string)

use crate::{spark_cast, EvalMode};
use arrow_array::builder::StringBuilder;
use arrow_array::{Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Schema};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// to_json function
#[derive(Debug, Hash)]
pub struct ToJson {
    /// The input to convert to JSON
    expr: Arc<dyn PhysicalExpr>, //TODO options such as null handling
}

impl ToJson {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl Display for ToJson {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO options & timezone
        write!(f, "to_json({})", self.expr)
    }
}

impl PartialEq<dyn Any> for ToJson {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<ToJson>() {
            //TODO compare options & timezone
            self.expr.eq(&other.expr)
        } else {
            false
        }
    }
}

impl PhysicalExpr for ToJson {
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
        let input = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        if let Some(struct_array) = input.as_any().downcast_ref::<StructArray>() {
            Ok(ColumnarValue::Array(Arc::new(struct_to_json(
                struct_array,
            )?)))
        } else {
            todo!()
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert!(children.len() == 1);
        // TODO options & timezone
        Ok(Arc::new(Self::new(children[0].clone())))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        // TODO options & timezone
        let mut s = state;
        self.expr.hash(&mut s);
        self.hash(&mut s);
    }
}

fn struct_to_json(array: &StructArray) -> Result<StringArray> {
    // create string representation of each column first
    let string_arrays: Vec<Arc<StringArray>> = array
        .columns()
        .iter()
        .map(|arr| {
            spark_cast(
                ColumnarValue::Array(arr.clone()),
                &DataType::Utf8,
                EvalMode::Legacy,
                "UTC".to_string(), // TODO Remove hard-coded timezone
            )
            .and_then(|casted_value| casted_value.into_array(array.len()))
            .and_then(|array_ref| {
                array_ref
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|string_array| Arc::new(string_array.clone()))
                    .ok_or_else(|| DataFusionError::Execution("Expected StringArray".to_string()))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let field_names: Vec<String> = array.fields().iter().map(|f| f.name().clone()).collect();

    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 64);
    for row_index in 0..array.len() {
        if array.is_null(row_index) {
            builder.append_null();
        } else {
            let mut json = String::new();
            let mut any_fields_written = false;
            json.push('{');
            for col_index in 0..string_arrays.len() {
                if !string_arrays[col_index].is_null(row_index) {
                    if any_fields_written {
                        json.push(',');
                    }
                    // quoted field name
                    json.push('"');
                    json.push_str(&field_names[col_index]);
                    json.push_str("\":");
                    // value
                    json.push_str(string_arrays[col_index].value(row_index));
                    any_fields_written = true;
                }
            }
            json.push('}');
            builder.append_value(json);
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use crate::to_json::struct_to_json;
    use arrow_array::Array;
    use arrow_array::{ArrayRef, BooleanArray, Int32Array, StructArray};
    use arrow_schema::{DataType, Field};
    use datafusion_common::Result;
    use std::sync::Arc;

    #[test]
    fn test_primitives() -> Result<()> {
        let bools: ArrayRef = Arc::new(BooleanArray::from(vec![
            None,
            Some(true),
            Some(false),
            Some(false),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(123),
            None,
            Some(i32::MAX),
            Some(i32::MIN),
        ]));
        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Boolean, true)), bools),
            (Arc::new(Field::new("b", DataType::Int32, true)), ints),
        ]);
        let json = struct_to_json(&struct_array)?;
        assert_eq!(4, json.len());
        assert_eq!(r#"{"b":123}"#, json.value(0));
        assert_eq!(r#"{"a":true}"#, json.value(1));
        assert_eq!(r#"{"a":false,"b":2147483647}"#, json.value(2));
        assert_eq!(r#"{"a":false,"b":-2147483648}"#, json.value(3));
        Ok(())
    }
}
