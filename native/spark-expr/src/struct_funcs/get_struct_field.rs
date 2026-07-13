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

use arrow::array::{make_array, Array, ArrayRef, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::{
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct GetStructField {
    child: Arc<dyn PhysicalExpr>,
    ordinal: usize,
}

impl Hash for GetStructField {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.ordinal.hash(state);
    }
}
impl PartialEq for GetStructField {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.ordinal.eq(&other.ordinal)
    }
}

impl GetStructField {
    pub fn new(child: Arc<dyn PhysicalExpr>, ordinal: usize) -> Self {
        Self { child, ordinal }
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<Arc<Field>> {
        match self.child.data_type(input_schema)? {
            DataType::Struct(fields) => Ok(Arc::clone(&fields[self.ordinal])),
            data_type => Err(DataFusionError::Plan(format!(
                "Expect struct field, got {data_type:?}"
            ))),
        }
    }

    /// Extract field `ordinal` from a struct array, propagating the parent struct's null mask.
    ///
    /// Spark semantics: a field of a NULL struct is NULL. Arrow stores a StructArray's child
    /// arrays with their own validity, INDEPENDENT of the parent struct's null buffer -- so the
    /// raw child value at a row where the struct itself is null can be non-null (e.g. parquet
    /// files where a logically-null struct column still has a populated child buffer). Returning
    /// the child column verbatim then makes `isnotnull(struct.field)` wrongly true for a null
    /// struct. Union the struct's null mask into the child's (null where the struct is null OR
    /// the child is null).
    fn project_field(struct_array: &StructArray, ordinal: usize) -> DataFusionResult<ArrayRef> {
        let child = struct_array.column(ordinal);
        match struct_array.nulls() {
            Some(_) => {
                let combined = NullBuffer::union(struct_array.nulls(), child.nulls());
                let data = child.to_data().into_builder().nulls(combined).build()?;
                Ok(make_array(data))
            }
            None => Ok(Arc::clone(child)),
        }
    }
}

impl PhysicalExpr for GetStructField {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.child_field(input_schema)?.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        // A field extracted from a struct is nullable if EITHER the field itself is declared
        // nullable OR the parent struct can be null -- a field of a null struct is null (Spark
        // semantics, enforced by `project_field` unioning the parent null mask). Reporting only
        // the field's own nullability under-declares: a non-nullable field of a nullable struct
        // then carries the parent's nulls while claiming non-nullable, which fails Arrow's
        // RecordBatch validation downstream with "declared as non-nullable but contains null
        // values" (e.g. once the projected column reaches a shuffle/sort). Mirrors Spark's
        // `GetStructField.nullable = child.nullable || field.nullable`.
        Ok(self.child.nullable(input_schema)? || self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let child_value = self.child.evaluate(batch)?;

        match child_value {
            ColumnarValue::Array(array) => {
                let struct_array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("A struct is expected");

                Ok(ColumnarValue::Array(Self::project_field(
                    struct_array,
                    self.ordinal,
                )?))
            }
            ColumnarValue::Scalar(ScalarValue::Struct(struct_array)) => Ok(ColumnarValue::Array(
                Self::project_field(&struct_array, self.ordinal)?,
            )),
            value => Err(DataFusionError::Execution(format!(
                "Expected a struct array, got {value:?}"
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(GetStructField::new(
            Arc::clone(&children[0]),
            self.ordinal,
        )))
    }
}

impl Display for GetStructField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetStructField [child: {:?}, ordinal: {:?}]",
            self.child, self.ordinal
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::Fields;
    use datafusion::physical_expr::expressions::Column;

    // A field of a NULL struct must be NULL (Spark semantics) even when the child buffer holds a
    // non-null value at that row -- Arrow stores child validity independently of the parent
    // struct's null mask, so a logically-null struct column read from parquet can still carry a
    // populated child buffer. Without propagating the parent null mask, `isnotnull(struct.field)`
    // wrongly evaluates TRUE for a null struct.
    #[test]
    fn field_of_null_struct_is_null() {
        // Child is non-null at every row; the struct itself is null at rows 1 and 3.
        let child = Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40])) as ArrayRef;
        let fields: Fields = Fields::from(vec![Field::new("version", DataType::Int64, true)]);
        let nulls = NullBuffer::from(vec![true, false, true, false]);
        let struct_array = StructArray::new(fields.clone(), vec![child], Some(nulls));
        let schema = Schema::new(vec![Field::new("cm", DataType::Struct(fields), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)]).unwrap();

        let expr = GetStructField::new(Arc::new(Column::new("cm", 0)), 0);
        let out = expr
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();

        assert!(!out.is_null(0) && out.value(0) == 10);
        assert!(out.is_null(1), "field of a null struct must be null");
        assert!(!out.is_null(2) && out.value(2) == 30);
        assert!(out.is_null(3), "field of a null struct must be null");
    }

    // A NON-nullable field of a NULLABLE struct must report `nullable() == true`: `project_field`
    // unions the parent struct's null mask, so the projected column carries nulls wherever the
    // struct is null. Reporting the field's own (non-nullable) flag would make the output schema
    // lie, failing Arrow RecordBatch validation downstream with "declared as non-nullable but
    // contains null values" once the column reaches a shuffle/sort.
    #[test]
    fn non_nullable_field_of_nullable_struct_is_nullable() {
        // `size` is declared non-nullable, but the enclosing struct is nullable.
        let inner: Fields = Fields::from(vec![Field::new("size", DataType::Int64, false)]);
        let schema = Schema::new(vec![Field::new(
            "add",
            DataType::Struct(inner),
            /* struct nullable */ true,
        )]);

        let expr = GetStructField::new(Arc::new(Column::new("add", 0)), 0);
        assert!(
            expr.nullable(&schema).unwrap(),
            "a field of a nullable struct must be nullable even if the field itself is non-nullable"
        );
    }

    // A non-nullable field of a NON-nullable struct stays non-nullable (no over-declaring).
    #[test]
    fn non_nullable_field_of_non_nullable_struct_stays_non_nullable() {
        let inner: Fields = Fields::from(vec![Field::new("size", DataType::Int64, false)]);
        let schema = Schema::new(vec![Field::new(
            "add",
            DataType::Struct(inner),
            /* struct nullable */ false,
        )]);

        let expr = GetStructField::new(Arc::new(Column::new("add", 0)), 0);
        assert!(
            !expr.nullable(&schema).unwrap(),
            "a non-nullable field of a non-nullable struct must remain non-nullable"
        );
    }
}
