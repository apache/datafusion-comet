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
    Array, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    RecordBatch, StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

/// from_json function - parses JSON strings into structured types
#[derive(Debug, Eq)]
pub struct FromJson {
    /// The JSON string input expression
    expr: Arc<dyn PhysicalExpr>,
    /// Target schema for parsing
    schema: DataType,
    /// Timezone for timestamp parsing (future use)
    timezone: String,
}

impl PartialEq for FromJson {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.schema == other.schema && self.timezone == other.timezone
    }
}

impl std::hash::Hash for FromJson {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        // Note: DataType doesn't implement Hash, so we hash its debug representation
        format!("{:?}", self.schema).hash(state);
        self.timezone.hash(state);
    }
}

impl FromJson {
    pub fn new(expr: Arc<dyn PhysicalExpr>, schema: DataType, timezone: &str) -> Self {
        Self {
            expr,
            schema,
            timezone: timezone.to_owned(),
        }
    }
}

impl Display for FromJson {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "from_json({}, schema={:?}, timezone={})",
            self.expr, self.schema, self.timezone
        )
    }
}

impl PartialEq<dyn Any> for FromJson {
    fn eq(&self, other: &dyn Any) -> bool {
        if let Some(other) = other.downcast_ref::<FromJson>() {
            self.expr.eq(&other.expr)
                && self.schema == other.schema
                && self.timezone == other.timezone
        } else {
            false
        }
    }
}

impl PhysicalExpr for FromJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }

    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.schema.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        // Always nullable - parse errors return null in PERMISSIVE mode
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(json_string_to_struct(
            &input,
            &self.schema,
        )?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert!(children.len() == 1);
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.schema.clone(),
            &self.timezone,
        )))
    }
}

/// Parse JSON string array into struct array
fn json_string_to_struct(arr: &Arc<dyn Array>, schema: &DataType) -> Result<ArrayRef> {
    use arrow::array::StringArray;
    use arrow::buffer::NullBuffer;

    let string_array = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        datafusion::common::DataFusionError::Execution("from_json expects string input".to_string())
    })?;

    let DataType::Struct(fields) = schema else {
        return Err(datafusion::common::DataFusionError::Execution(
            "from_json requires struct schema".to_string(),
        ));
    };

    let num_rows = string_array.len();
    let mut field_builders = create_field_builders(fields, num_rows)?;
    let mut struct_nulls = vec![true; num_rows];
    for (row_idx, struct_null) in struct_nulls.iter_mut().enumerate() {
        if string_array.is_null(row_idx) {
            // Null input -> null struct
            *struct_null = false;
            append_null_to_all_builders(&mut field_builders);
        } else {
            let json_str = string_array.value(row_idx);

            // Parse JSON (PERMISSIVE mode: return null fields on error)
            match serde_json::from_str::<serde_json::Value>(json_str) {
                Ok(json_value) => {
                    if let serde_json::Value::Object(obj) = json_value {
                        // Struct is not null, extract each field
                        *struct_null = true;
                        for (field, builder) in fields.iter().zip(field_builders.iter_mut()) {
                            let field_value = obj.get(field.name());
                            append_field_value(builder, field, field_value)?;
                        }
                    } else {
                        // Not an object -> struct with null fields
                        *struct_null = true;
                        append_null_to_all_builders(&mut field_builders);
                    }
                }
                Err(_) => {
                    // Parse error -> struct with null fields (PERMISSIVE mode)
                    *struct_null = true;
                    append_null_to_all_builders(&mut field_builders);
                }
            }
        }
    }

    let arrays: Vec<ArrayRef> = field_builders
        .into_iter()
        .map(finish_builder)
        .collect::<Result<Vec<_>>>()?;
    let null_buffer = NullBuffer::from(struct_nulls);
    Ok(Arc::new(StructArray::new(
        fields.clone(),
        arrays,
        Some(null_buffer),
    )))
}

/// Builder enum for different data types
enum FieldBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    String(StringBuilder),
    Struct {
        fields: arrow::datatypes::Fields,
        builders: Vec<FieldBuilder>,
        null_buffer: Vec<bool>,
    },
}

fn create_field_builders(
    fields: &arrow::datatypes::Fields,
    capacity: usize,
) -> Result<Vec<FieldBuilder>> {
    fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::Int32 => Ok(FieldBuilder::Int32(Int32Builder::with_capacity(capacity))),
            DataType::Int64 => Ok(FieldBuilder::Int64(Int64Builder::with_capacity(capacity))),
            DataType::Float32 => Ok(FieldBuilder::Float32(Float32Builder::with_capacity(
                capacity,
            ))),
            DataType::Float64 => Ok(FieldBuilder::Float64(Float64Builder::with_capacity(
                capacity,
            ))),
            DataType::Boolean => Ok(FieldBuilder::Boolean(BooleanBuilder::with_capacity(
                capacity,
            ))),
            DataType::Utf8 => Ok(FieldBuilder::String(StringBuilder::with_capacity(
                capacity,
                capacity * 16,
            ))),
            DataType::Struct(nested_fields) => {
                let nested_builders = create_field_builders(nested_fields, capacity)?;
                Ok(FieldBuilder::Struct {
                    fields: nested_fields.clone(),
                    builders: nested_builders,
                    null_buffer: Vec::with_capacity(capacity),
                })
            }
            dt => Err(datafusion::common::DataFusionError::Execution(format!(
                "Unsupported field type in from_json: {:?}",
                dt
            ))),
        })
        .collect()
}

fn append_null_to_all_builders(builders: &mut [FieldBuilder]) {
    for builder in builders {
        match builder {
            FieldBuilder::Int32(b) => b.append_null(),
            FieldBuilder::Int64(b) => b.append_null(),
            FieldBuilder::Float32(b) => b.append_null(),
            FieldBuilder::Float64(b) => b.append_null(),
            FieldBuilder::Boolean(b) => b.append_null(),
            FieldBuilder::String(b) => b.append_null(),
            FieldBuilder::Struct {
                builders: nested_builders,
                null_buffer,
                ..
            } => {
                // Append null to nested struct
                null_buffer.push(false);
                append_null_to_all_builders(nested_builders);
            }
        }
    }
}

fn append_field_value(
    builder: &mut FieldBuilder,
    field: &Field,
    json_value: Option<&serde_json::Value>,
) -> Result<()> {
    use serde_json::Value;

    let value = match json_value {
        Some(Value::Null) | None => {
            // Missing field or explicit null -> append null
            match builder {
                FieldBuilder::Int32(b) => b.append_null(),
                FieldBuilder::Int64(b) => b.append_null(),
                FieldBuilder::Float32(b) => b.append_null(),
                FieldBuilder::Float64(b) => b.append_null(),
                FieldBuilder::Boolean(b) => b.append_null(),
                FieldBuilder::String(b) => b.append_null(),
                FieldBuilder::Struct {
                    builders: nested_builders,
                    null_buffer,
                    ..
                } => {
                    null_buffer.push(false);
                    append_null_to_all_builders(nested_builders);
                }
            }
            return Ok(());
        }
        Some(v) => v,
    };

    match (builder, field.data_type()) {
        (FieldBuilder::Int32(b), DataType::Int32) => {
            if let Some(i) = value.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    b.append_value(i as i32);
                } else {
                    b.append_null(); // Overflow
                }
            } else {
                b.append_null(); // Type mismatch
            }
        }
        (FieldBuilder::Int64(b), DataType::Int64) => {
            if let Some(i) = value.as_i64() {
                b.append_value(i);
            } else {
                b.append_null();
            }
        }
        (FieldBuilder::Float32(b), DataType::Float32) => {
            if let Some(f) = value.as_f64() {
                b.append_value(f as f32);
            } else {
                b.append_null();
            }
        }
        (FieldBuilder::Float64(b), DataType::Float64) => {
            if let Some(f) = value.as_f64() {
                b.append_value(f);
            } else {
                b.append_null();
            }
        }
        (FieldBuilder::Boolean(b), DataType::Boolean) => {
            if let Some(bool_val) = value.as_bool() {
                b.append_value(bool_val);
            } else {
                b.append_null();
            }
        }
        (FieldBuilder::String(b), DataType::Utf8) => {
            if let Some(s) = value.as_str() {
                b.append_value(s);
            } else {
                // Stringify non-string values
                b.append_value(value.to_string());
            }
        }
        (
            FieldBuilder::Struct {
                fields: nested_fields,
                builders: nested_builders,
                null_buffer,
            },
            DataType::Struct(_),
        ) => {
            // Handle nested struct
            if let Some(obj) = value.as_object() {
                // Non-null nested struct
                null_buffer.push(true);
                for (nested_field, nested_builder) in
                    nested_fields.iter().zip(nested_builders.iter_mut())
                {
                    let nested_value = obj.get(nested_field.name());
                    append_field_value(nested_builder, nested_field, nested_value)?;
                }
            } else {
                // Not an object -> null nested struct
                null_buffer.push(false);
                append_null_to_all_builders(nested_builders);
            }
        }
        _ => {
            return Err(datafusion::common::DataFusionError::Execution(
                "Type mismatch in from_json".to_string(),
            ));
        }
    }

    Ok(())
}

fn finish_builder(builder: FieldBuilder) -> Result<ArrayRef> {
    Ok(match builder {
        FieldBuilder::Int32(mut b) => Arc::new(b.finish()),
        FieldBuilder::Int64(mut b) => Arc::new(b.finish()),
        FieldBuilder::Float32(mut b) => Arc::new(b.finish()),
        FieldBuilder::Float64(mut b) => Arc::new(b.finish()),
        FieldBuilder::Boolean(mut b) => Arc::new(b.finish()),
        FieldBuilder::String(mut b) => Arc::new(b.finish()),
        FieldBuilder::Struct {
            fields,
            builders,
            null_buffer,
        } => {
            let nested_arrays: Vec<ArrayRef> = builders
                .into_iter()
                .map(finish_builder)
                .collect::<Result<Vec<_>>>()?;
            let null_buf = arrow::buffer::NullBuffer::from(null_buffer);
            Arc::new(StructArray::new(fields, nested_arrays, Some(null_buf)))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::Fields;

    #[test]
    fn test_simple_struct() -> Result<()> {
        let schema = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        let input: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some(r#"{"a": 123, "b": "hello"}"#),
            Some(r#"{"a": 456}"#),
            Some(r#"invalid json"#),
            None,
        ]));

        let result = json_string_to_struct(&input, &schema)?;
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        assert_eq!(struct_array.len(), 4);

        // First row
        let a_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(a_array.value(0), 123);
        let b_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(b_array.value(0), "hello");

        // Second row (missing field b)
        assert_eq!(a_array.value(1), 456);
        assert!(b_array.is_null(1));

        // Third row (parse error -> struct NOT null, all fields null)
        assert!(!struct_array.is_null(2), "Struct should not be null");
        assert!(a_array.is_null(2));
        assert!(b_array.is_null(2));

        // Fourth row (null input -> struct IS null)
        assert!(struct_array.is_null(3), "Struct itself should be null");

        Ok(())
    }

    #[test]
    fn test_all_primitive_types() -> Result<()> {
        let schema = DataType::Struct(Fields::from(vec![
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("bool", DataType::Boolean, true),
            Field::new("str", DataType::Utf8, true),
        ]));

        let input: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some(
            r#"{"i32":123,"i64":9999999999,"f32":1.5,"f64":2.5,"bool":true,"str":"test"}"#,
        )]));

        let result = json_string_to_struct(&input, &schema)?;
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        assert_eq!(struct_array.len(), 1);

        // Verify all types
        let i32_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(i32_array.value(0), 123);

        let i64_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(i64_array.value(0), 9999999999);

        let f32_array = struct_array
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(f32_array.value(0), 1.5);

        let f64_array = struct_array
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(f64_array.value(0), 2.5);

        let bool_array = struct_array
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(bool_array.value(0));

        let str_array = struct_array
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(str_array.value(0), "test");

        Ok(())
    }

    #[test]
    fn test_empty_and_null_json() -> Result<()> {
        let schema = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        let input: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some(r#"{}"#),   // Empty object
            Some(r#"null"#), // JSON null
            Some(r#"[]"#),   // Array (not object)
            Some(r#"123"#),  // Number (not object)
        ]));

        let result = json_string_to_struct(&input, &schema)?;
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        assert_eq!(struct_array.len(), 4);

        let a_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // All rows should have non-null structs with null field values
        for i in 0..4 {
            assert!(
                !struct_array.is_null(i),
                "Row {} struct should not be null",
                i
            );
            assert!(a_array.is_null(i), "Row {} field a should be null", i);
            assert!(b_array.is_null(i), "Row {} field b should be null", i);
        }

        Ok(())
    }

    #[test]
    fn test_nested_struct() -> Result<()> {
        let schema = DataType::Struct(Fields::from(vec![
            Field::new(
                "outer",
                DataType::Struct(Fields::from(vec![
                    Field::new("inner_a", DataType::Int32, true),
                    Field::new("inner_b", DataType::Utf8, true),
                ])),
                true,
            ),
            Field::new("top_level", DataType::Int32, true),
        ]));

        let input: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some(r#"{"outer":{"inner_a":123,"inner_b":"hello"},"top_level":999}"#),
            Some(r#"{"outer":{"inner_a":456},"top_level":888}"#), // Missing nested field
            Some(r#"{"outer":null,"top_level":777}"#),            // Null nested struct
            Some(r#"{"top_level":666}"#),                         // Missing nested struct
        ]));

        let result = json_string_to_struct(&input, &schema)?;
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        assert_eq!(struct_array.len(), 4);

        // Check outer struct
        let outer_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let top_level_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Row 0: Valid nested struct
        assert!(!outer_array.is_null(0), "Nested struct should not be null");
        let inner_a_array = outer_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let inner_b_array = outer_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(inner_a_array.value(0), 123);
        assert_eq!(inner_b_array.value(0), "hello");
        assert_eq!(top_level_array.value(0), 999);

        // Row 1: Missing nested field
        assert!(!outer_array.is_null(1));
        assert_eq!(inner_a_array.value(1), 456);
        assert!(inner_b_array.is_null(1));
        assert_eq!(top_level_array.value(1), 888);

        // Row 2: Null nested struct
        assert!(outer_array.is_null(2), "Nested struct should be null");
        assert_eq!(top_level_array.value(2), 777);

        // Row 3: Missing nested struct
        assert!(outer_array.is_null(3), "Nested struct should be null");
        assert_eq!(top_level_array.value(3), 666);

        Ok(())
    }
}
