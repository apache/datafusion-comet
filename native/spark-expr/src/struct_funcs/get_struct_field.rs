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

use arrow::array::{Array, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_common::child_with_parent_nulls;
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
        // semantics, enforced by unioning the parent null mask into the child). Reporting only
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

                // A field of a null struct is null, so the parent's null mask has to be unioned
                // into the child; see `datafusion_comet_common::struct_nulls`.
                Ok(ColumnarValue::Array(child_with_parent_nulls(
                    struct_array,
                    self.ordinal,
                )?))
            }
            ColumnarValue::Scalar(ScalarValue::Struct(struct_array)) => {
                let child = child_with_parent_nulls(&struct_array, self.ordinal)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    child.as_ref(),
                    0,
                )?))
            }
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
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Fields;
    use datafusion::physical_expr::expressions::{Column, Literal};

    fn assert_scalar_for_batch_sizes(expr: &GetStructField, expected: ScalarValue) {
        for num_rows in [4, 1, 0] {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let input = Arc::new(Int64Array::from_iter_values(0..num_rows as i64));
            let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
            let result = expr.evaluate(&batch).unwrap();

            // A projection materializes scalars to its input batch length. Returning the
            // one-element struct child as an array fails this check for multi-row/empty batches.
            let output = result.clone().into_array_of_size(num_rows).unwrap();
            assert_eq!(output.len(), num_rows);
            assert_eq!(
                output.as_ref(),
                expected.to_array_of_size(num_rows).unwrap().as_ref()
            );
            match result {
                ColumnarValue::Scalar(value) => assert_eq!(value, expected),
                other => panic!("expected a scalar struct field, got {other:?}"),
            }
        }
    }

    #[test]
    fn scalar_field_is_broadcast_to_batch_length() {
        let fields = Fields::from(vec![Field::new("value", DataType::Int64, false)]);
        let child = Arc::new(Int64Array::from(vec![42_i64])) as ArrayRef;
        let scalar = ScalarValue::Struct(Arc::new(StructArray::new(fields, vec![child], None)));
        let expr = GetStructField::new(Arc::new(Literal::new(scalar)), 0);

        assert_scalar_for_batch_sizes(&expr, ScalarValue::Int64(Some(42)));
    }

    #[test]
    fn scalar_field_of_null_struct_is_null() {
        let fields = Fields::from(vec![Field::new("value", DataType::Int64, false)]);
        // The null parent hides a populated, non-nullable child buffer.
        let child = Arc::new(Int64Array::from(vec![42_i64])) as ArrayRef;
        let scalar = ScalarValue::Struct(Arc::new(StructArray::new(
            fields,
            vec![child],
            Some(NullBuffer::from(vec![false])),
        )));
        let expr = GetStructField::new(Arc::new(Literal::new(scalar)), 0);

        assert_scalar_for_batch_sizes(&expr, ScalarValue::Int64(None));
    }

    #[test]
    fn scalar_null_field_is_null() {
        let fields = Fields::from(vec![Field::new("value", DataType::Int64, true)]);
        let child = Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef;
        let scalar = ScalarValue::Struct(Arc::new(StructArray::new(fields, vec![child], None)));
        let expr = GetStructField::new(Arc::new(Literal::new(scalar)), 0);

        assert_scalar_for_batch_sizes(&expr, ScalarValue::Int64(None));
    }

    #[test]
    fn nested_scalar_field_retains_scalar_semantics() {
        for outer_valid in [true, false] {
            for inner_valid in [true, false] {
                for value in [Some(42_i64), None] {
                    let inner_fields =
                        Fields::from(vec![Field::new("value", DataType::Int64, true)]);
                    let inner = Arc::new(StructArray::new(
                        inner_fields.clone(),
                        vec![Arc::new(Int64Array::from(vec![value]))],
                        Some(NullBuffer::from(vec![inner_valid])),
                    ));
                    let outer_fields = Fields::from(vec![Field::new(
                        "nested",
                        DataType::Struct(inner_fields.clone()),
                        true,
                    )]);
                    let scalar = ScalarValue::Struct(Arc::new(StructArray::new(
                        outer_fields,
                        vec![Arc::clone(&inner) as ArrayRef],
                        Some(NullBuffer::from(vec![outer_valid])),
                    )));
                    let nested = GetStructField::new(Arc::new(Literal::new(scalar)), 0);
                    let expected_inner = if outer_valid {
                        inner
                    } else {
                        Arc::new(StructArray::new_null(inner_fields, 1))
                    };
                    assert_scalar_for_batch_sizes(&nested, ScalarValue::Struct(expected_inner));

                    let leaf = GetStructField::new(Arc::new(nested), 0);
                    let expected_value = if outer_valid && inner_valid {
                        value
                    } else {
                        None
                    };
                    assert_scalar_for_batch_sizes(&leaf, ScalarValue::Int64(expected_value));
                }
            }
        }
    }

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

    // A NON-nullable field of a NULLABLE struct must report `nullable() == true`: the parent mask
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
