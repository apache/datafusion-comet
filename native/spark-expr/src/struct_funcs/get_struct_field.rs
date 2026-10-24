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

use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::{expressions::Literal, PhysicalExpr};
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

    /// Read only an already-materialized literal field for consumer sizing. Do not evaluate
    /// expressions or include unrelated sibling fields in the repeated-input estimate.
    pub(crate) fn literal_field(&self) -> Option<ArrayRef> {
        let value: ArrayRef = if let Some(literal) = self.child.downcast_ref::<Literal>() {
            let ScalarValue::Struct(value) = literal.value() else {
                return None;
            };
            Arc::<StructArray>::clone(value)
        } else {
            self.child.downcast_ref::<Self>()?.literal_field()?
        };
        let structure = value.as_any().downcast_ref::<StructArray>()?;
        if structure.len() != 1 || self.ordinal >= structure.num_columns() {
            return None;
        }
        child_with_parent_nulls(structure, self.ordinal).ok()
    }

    fn broadcast_field(child: ArrayRef, rows: usize) -> DataFusionResult<ColumnarValue> {
        if rows == 1 {
            // The masked child already has the required shape; retain its buffers.
            return Ok(ColumnarValue::Array(child));
        }
        Ok(ColumnarValue::Array(
            ScalarValue::try_from_array(child.as_ref(), 0)?.to_array_of_size(rows)?,
        ))
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
        // Select the full literal field chain before expanding it, so an unused wide sibling
        // of a nested field is never materialized for every input row.
        if let Some(child) = self.literal_field() {
            return Self::broadcast_field(child, batch.num_rows());
        }
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
                // Keep the same array shape as a struct column, including IF branch shortcuts.
                Self::broadcast_field(child, batch.num_rows())
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
    use crate::{spark_in_list, spark_lpad, spark_rpad, IfExpr};
    use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Fields;
    use datafusion::physical_expr::expressions::{Column, Literal};

    fn assert_array_for_batch_sizes(expr: &GetStructField, expected: ScalarValue) {
        for num_rows in [4, 1, 0] {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let input = Arc::new(Int64Array::from_iter_values(0..num_rows as i64));
            let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
            let result = expr.evaluate(&batch).unwrap();

            // Consumers must receive an array directly, including through IF's whole-batch
            // branch shortcut. A scalar or the one-element struct child violates this contract.
            let ColumnarValue::Array(output) = result else {
                panic!("expected a batch-sized struct field array, got {result:?}");
            };
            assert_eq!(output.len(), num_rows);
            assert_eq!(
                output.data_type(),
                &expr.data_type(&batch.schema()).unwrap()
            );
            assert_eq!(
                output.as_ref(),
                expected.to_array_of_size(num_rows).unwrap().as_ref()
            );
        }
    }

    #[test]
    fn scalar_field_is_broadcast_to_batch_length() {
        let fields = Fields::from(vec![Field::new("value", DataType::Int64, false)]);
        let child = Arc::new(Int64Array::from(vec![42_i64])) as ArrayRef;
        let scalar = ScalarValue::Struct(Arc::new(StructArray::new(fields, vec![child], None)));
        let expr = GetStructField::new(Arc::new(Literal::new(scalar)), 0);

        assert_array_for_batch_sizes(&expr, ScalarValue::Int64(Some(42)));
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

        assert_array_for_batch_sizes(&expr, ScalarValue::Int64(None));
    }

    #[test]
    fn scalar_null_field_is_null() {
        let fields = Fields::from(vec![Field::new("value", DataType::Int64, true)]);
        let child = Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef;
        let scalar = ScalarValue::Struct(Arc::new(StructArray::new(fields, vec![child], None)));
        let expr = GetStructField::new(Arc::new(Literal::new(scalar)), 0);

        assert_array_for_batch_sizes(&expr, ScalarValue::Int64(None));
    }

    #[test]
    fn nested_scalar_field_retains_nulls_and_metadata() {
        for outer_valid in [true, false] {
            for inner_valid in [true, false] {
                for value in [Some(42_i64), None] {
                    let inner_fields =
                        Fields::from(vec![Field::new("value", DataType::Int64, true)
                            .with_metadata(
                                [("comment".to_string(), "nested scalar field".to_string())].into(),
                            )]);
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
                    assert_array_for_batch_sizes(&nested, ScalarValue::Struct(expected_inner));

                    let leaf = GetStructField::new(Arc::new(nested), 0);
                    let expected_value = if outer_valid && inner_valid {
                        value
                    } else {
                        None
                    };
                    assert_array_for_batch_sizes(&leaf, ScalarValue::Int64(expected_value));
                }
            }
        }
    }

    #[test]
    fn scalar_field_as_in_candidate_handles_empty_and_multirow_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![
                Some(42),
                Some(7),
                None,
                Some(42),
            ]))],
        )
        .unwrap();
        for value in [Some(42_i64), None] {
            let fields = Fields::from(vec![Field::new("value", DataType::Int64, true)]);
            let scalar = ScalarValue::Struct(Arc::new(StructArray::new(
                fields,
                vec![Arc::new(Int64Array::from(vec![value]))],
                None,
            )));
            let candidate: Arc<dyn PhysicalExpr> =
                Arc::new(GetStructField::new(Arc::new(Literal::new(scalar)), 0));
            // IN probes candidates with an empty batch. An empty array selects its dynamic
            // path; it must still compare every input row with the scalar struct's field.
            assert!(matches!(
                candidate.evaluate(&RecordBatch::new_empty(Arc::clone(&schema))).unwrap(),
                ColumnarValue::Array(array) if array.is_empty()
            ));
            for negated in [false, true] {
                let expr = spark_in_list(
                    Arc::new(Column::new("id", 0)),
                    vec![Arc::clone(&candidate)],
                    negated,
                    &schema,
                )
                .unwrap();
                let expected = BooleanArray::from(if value.is_some() {
                    vec![Some(!negated), Some(negated), None, Some(!negated)]
                } else {
                    vec![None; 4]
                });
                for num_rows in [0, 1, 4] {
                    let output = expr.evaluate(&batch.slice(0, num_rows)).unwrap();
                    let ColumnarValue::Array(output) = output else {
                        panic!("expected an IN result array, got {output:?}");
                    };
                    assert_eq!(output.as_ref(), &expected.slice(0, num_rows));
                }
            }
        }
    }

    #[test]
    fn scalar_field_through_if_remains_an_array_for_padding() {
        for parent_valid in [true, false] {
            for value in [Some("b"), None] {
                let fields = Fields::from(vec![Field::new("value", DataType::Utf8, true)]);
                let scalar = ScalarValue::Struct(Arc::new(StructArray::new(
                    fields,
                    vec![Arc::new(StringArray::from(vec![value]))],
                    Some(NullBuffer::from(vec![parent_valid])),
                )));
                let expr = IfExpr::new(
                    Arc::new(Column::new("p", 0)),
                    Arc::new(GetStructField::new(Arc::new(Literal::new(scalar)), 0)),
                    Arc::new(Column::new("s", 1)),
                );
                for predicates in [
                    vec![Some(true); 4],
                    vec![Some(false); 4],
                    vec![Some(true), Some(false), None, Some(true)],
                    vec![Some(true)],
                    vec![],
                ] {
                    let num_rows = predicates.len();
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("p", DataType::Boolean, true),
                        Field::new("s", DataType::Utf8, false),
                    ]));
                    let batch = RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(BooleanArray::from(predicates.clone())),
                            Arc::new(StringArray::from(vec!["a"; num_rows])),
                        ],
                    )
                    .unwrap();
                    let selected = expr.evaluate(&batch).unwrap();
                    // All-true batches return the THEN expression directly. Pin its shape
                    // before padding's scalar adapter can hide a regression in the field read.
                    assert!(
                        matches!(&selected, ColumnarValue::Array(array) if array.len() == num_rows),
                        "expected a batch-sized IF array, got {selected:?}"
                    );
                    for (pad, scalar_value, column_value) in [
                        (
                            spark_lpad as fn(&[ColumnarValue]) -> DataFusionResult<ColumnarValue>,
                            "000b",
                            "000a",
                        ),
                        (spark_rpad, "b000", "a000"),
                    ] {
                        let output = pad(&[
                            selected.clone(),
                            ColumnarValue::Scalar(ScalarValue::Int32(Some(4))),
                            ColumnarValue::Scalar(ScalarValue::Utf8(Some("0".to_string()))),
                        ])
                        .unwrap();
                        let expected = StringArray::from(
                            predicates
                                .iter()
                                .map(|predicate| {
                                    if *predicate == Some(true) {
                                        value.filter(|_| parent_valid).map(|_| scalar_value)
                                    } else {
                                        Some(column_value)
                                    }
                                })
                                .collect::<Vec<_>>(),
                        );
                        let ColumnarValue::Array(output) = output else {
                            panic!("expected an array after padding, got {output:?}");
                        };
                        assert_eq!(output.as_ref(), &expected);
                    }
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
