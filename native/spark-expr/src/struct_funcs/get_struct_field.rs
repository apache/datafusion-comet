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

use arrow::array::{Array, ArrayRef, AsArray, StructArray};
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
            // A scalar struct holds one row, so its field is a scalar too. A one-row array would
            // not match the batch's row count.
            ColumnarValue::Scalar(ScalarValue::Struct(struct_array)) => Ok(ColumnarValue::Scalar(
                single_row_scalar(child_with_parent_nulls(&struct_array, self.ordinal)?)?,
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

/// The scalar that a one-row array holds. A list wraps the array as is: `try_from_array` rebuilds
/// a list's element field from its name and nullability, which drops metadata such as a Variant
/// element's extension type, so the scalar's type would no longer match `data_type`. A dictionary
/// converts the value its key points at here for the same reason, since `try_from_array` would
/// convert it with itself.
fn single_row_scalar(array: ArrayRef) -> DataFusionResult<ScalarValue> {
    Ok(match array.data_type() {
        DataType::List(_) => ScalarValue::List(Arc::new(array.as_list::<i32>().clone())),
        DataType::LargeList(_) => ScalarValue::LargeList(Arc::new(array.as_list::<i64>().clone())),
        DataType::FixedSizeList(_, _) => {
            ScalarValue::FixedSizeList(Arc::new(array.as_fixed_size_list().clone()))
        }
        DataType::ListView(_) => {
            ScalarValue::ListView(Arc::new(array.as_list_view::<i32>().clone()))
        }
        DataType::LargeListView(_) => {
            ScalarValue::LargeListView(Arc::new(array.as_list_view::<i64>().clone()))
        }
        DataType::Dictionary(key_type, _) => {
            let dict = array.as_any_dictionary();
            let value = if dict.keys().is_valid(0) {
                single_row_scalar(dict.values().slice(dict.normalized_keys()[0], 1))?
            } else {
                ScalarValue::try_from(dict.values().data_type())?
            };
            ScalarValue::Dictionary(key_type.clone(), Box::new(value))
        }
        _ => ScalarValue::try_from_array(&array, 0)?,
    })
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
    use arrow::array::{
        DictionaryArray, FixedSizeListArray, Int32Array, Int64Array, LargeListArray,
        LargeListViewArray, ListArray, ListViewArray, NullArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::Fields;
    use datafusion::physical_expr::expressions::{Column, Literal};

    // A field of a scalar struct is a scalar, so a consumer broadcasts it over the batch instead of
    // reading a one-row array beside full columns. A null struct gives a null field, for a typed
    // field and a Null-typed one.
    #[test]
    fn field_of_scalar_struct_is_scalar() {
        let fields: Fields = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("n", DataType::Null, true),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![0_i64, 1, 2, 3]))],
        )
        .unwrap();

        let present = ScalarValue::Struct(Arc::new(StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7_i64])),
                Arc::new(NullArray::new(1)),
            ],
            None,
        )));
        let absent = ScalarValue::Struct(Arc::new(StructArray::new_null(fields.clone(), 1)));
        // A null struct whose child slot still holds a value: the field is null all the same.
        let hidden = ScalarValue::Struct(Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(Int64Array::from(vec![7_i64])),
                Arc::new(NullArray::new(1)),
            ],
            Some(NullBuffer::new_null(1)),
        )));

        for (scalar, ordinal, expected) in [
            (present.clone(), 0, ScalarValue::Int64(Some(7))),
            (present, 1, ScalarValue::Null),
            (absent.clone(), 0, ScalarValue::Int64(None)),
            (absent, 1, ScalarValue::Null),
            (hidden, 0, ScalarValue::Int64(None)),
        ] {
            let expr = GetStructField::new(Arc::new(Literal::new(scalar)), ordinal);
            match expr.evaluate(&batch).unwrap() {
                ColumnarValue::Scalar(value) => assert_eq!(value, expected),
                ColumnarValue::Array(array) => {
                    panic!("expected a scalar for ordinal {ordinal}, got array {array:?}")
                }
            }
        }
    }

    // A list field of a scalar struct is the struct's list, and a null struct gives a null list.
    // Either way it keeps its element field, metadata included, so the scalar's type is the one
    // `data_type` declares.
    #[test]
    fn list_field_of_scalar_struct_keeps_its_type() {
        let item =
            Arc::new(Field::new("item", DataType::Int64, true).with_metadata(
                [("ARROW:extension:name".to_string(), "test.ext".to_string())].into(),
            ));
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        let lists: Vec<ArrayRef> = vec![
            Arc::new(ListArray::new(
                Arc::clone(&item),
                OffsetBuffer::from_lengths([2]),
                Arc::clone(&values),
                None,
            )),
            Arc::new(LargeListArray::new(
                Arc::clone(&item),
                OffsetBuffer::from_lengths([2]),
                Arc::clone(&values),
                None,
            )),
            Arc::new(FixedSizeListArray::new(
                Arc::clone(&item),
                2,
                Arc::clone(&values),
                None,
            )),
            Arc::new(ListViewArray::new(
                Arc::clone(&item),
                ScalarBuffer::from(vec![0_i32]),
                ScalarBuffer::from(vec![2_i32]),
                Arc::clone(&values),
                None,
            )),
            Arc::new(LargeListViewArray::new(
                Arc::clone(&item),
                ScalarBuffer::from(vec![0_i64]),
                ScalarBuffer::from(vec![2_i64]),
                Arc::clone(&values),
                None,
            )),
        ];
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![0_i64, 1, 2]))],
        )
        .unwrap();

        for list in lists {
            let fields = Fields::from(vec![Field::new("l", list.data_type().clone(), true)]);
            // The second struct is null although its child slot holds the list.
            for struct_nulls in [None, Some(NullBuffer::new_null(1))] {
                let is_null = struct_nulls.is_some();
                let scalar = ScalarValue::Struct(Arc::new(StructArray::new(
                    fields.clone(),
                    vec![Arc::clone(&list)],
                    struct_nulls,
                )));
                let expr = GetStructField::new(Arc::new(Literal::new(scalar)), 0);
                let value = match expr.evaluate(&batch).unwrap() {
                    ColumnarValue::Scalar(value) => value,
                    ColumnarValue::Array(array) => panic!("expected a scalar, got array {array:?}"),
                };
                let kind = list.data_type();
                assert_eq!(
                    value.data_type(),
                    expr.data_type(&schema).unwrap(),
                    "{kind}"
                );
                assert_eq!(value.is_null(), is_null, "{kind}, null struct: {is_null}");
                if !is_null {
                    assert_eq!(value.to_array().unwrap().as_ref(), list.as_ref(), "{kind}");
                }
            }
        }
    }

    // A dictionary field of a scalar struct is the value its key points at, still dictionary
    // encoded with the same key type. A list value keeps its element field, so the field broadcasts
    // into a column of the type `data_type` declares.
    #[test]
    fn dictionary_field_of_scalar_struct_keeps_its_type() {
        let item =
            Arc::new(Field::new("item", DataType::Int64, true).with_metadata(
                [("ARROW:extension:name".to_string(), "test.ext".to_string())].into(),
            ));
        let lists = ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::from_lengths([1, 2]),
            Arc::new(Int64Array::from(vec![0_i64, 1, 2])),
            None,
        );
        let dict = |key: Option<i32>| -> ArrayRef {
            Arc::new(DictionaryArray::new(
                Int32Array::from(vec![key]),
                Arc::new(lists.clone()),
            ))
        };
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![0_i64, 1, 2]))],
        )
        .unwrap();

        // The key points at the second list, a null key gives a null, and so does a null struct
        // whose child slot still holds a key.
        for (field, struct_nulls, expected) in [
            (dict(Some(1)), None, Some(lists.slice(1, 1))),
            (dict(None), None, None),
            (dict(Some(1)), Some(NullBuffer::new_null(1)), None),
        ] {
            let fields = Fields::from(vec![Field::new("d", field.data_type().clone(), true)]);
            let scalar = ScalarValue::Struct(Arc::new(StructArray::new(
                fields,
                vec![field],
                struct_nulls,
            )));
            let expr = GetStructField::new(Arc::new(Literal::new(scalar)), 0);
            let value = match expr.evaluate(&batch).unwrap() {
                ColumnarValue::Scalar(value) => value,
                ColumnarValue::Array(array) => panic!("expected a scalar, got array {array:?}"),
            };
            let declared = Arc::new(Schema::new(vec![Field::new(
                "d",
                expr.data_type(&schema).unwrap(),
                true,
            )]));
            RecordBatch::try_new(
                declared,
                vec![value.to_array_of_size(batch.num_rows()).unwrap()],
            )
            .unwrap();
            let ScalarValue::Dictionary(key_type, inner) = value else {
                panic!("expected a dictionary scalar, got {value:?}")
            };
            assert_eq!(*key_type, DataType::Int32);
            match expected {
                Some(list) => assert_eq!(*inner, ScalarValue::List(Arc::new(list))),
                None => assert!(inner.is_null(), "expected a null, got {inner:?}"),
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
