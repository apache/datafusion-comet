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

use arrow::array::{make_array, Array, GenericListArray, OffsetSizeTrait, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{
    cast::{as_large_list_array, as_list_array},
    internal_err, DataFusionError, Result as DataFusionResult,
};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct GetArrayStructFields {
    child: Arc<dyn PhysicalExpr>,
    ordinal: usize,
}

impl Hash for GetArrayStructFields {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.ordinal.hash(state);
    }
}
impl PartialEq for GetArrayStructFields {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.ordinal.eq(&other.ordinal)
    }
}

impl GetArrayStructFields {
    pub fn new(child: Arc<dyn PhysicalExpr>, ordinal: usize) -> Self {
        Self { child, ordinal }
    }

    fn list_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        match self.child.data_type(input_schema)? {
            DataType::List(field) | DataType::LargeList(field) => Ok(field),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {data_type:?}"
            ))),
        }
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        match self.list_field(input_schema)?.data_type() {
            DataType::Struct(fields) => Ok(Arc::clone(&fields[self.ordinal])),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {data_type:?}"
            ))),
        }
    }

    /// The field describing the extracted list's element, used by both `data_type` and `evaluate`
    /// so the declared output type always matches the array `evaluate` produces. A row whose
    /// parent struct element is null yields a null in the extracted child even when that child
    /// field is declared non-nullable, so the element field must be nullable whenever the parent
    /// struct element can be null.
    fn output_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        let list_field = self.list_field(input_schema)?;
        let struct_field = self.child_field(input_schema)?;
        if list_field.is_nullable() && !struct_field.is_nullable() {
            Ok(Arc::new(struct_field.as_ref().clone().with_nullable(true)))
        } else {
            Ok(struct_field)
        }
    }
}

impl PhysicalExpr for GetArrayStructFields {
    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        let struct_field = self.output_field(input_schema)?;
        match self.child.data_type(input_schema)? {
            DataType::List(_) => Ok(DataType::List(struct_field)),
            DataType::LargeList(_) => Ok(DataType::LargeList(struct_field)),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {data_type:?}"
            ))),
        }
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(self.list_field(input_schema)?.is_nullable()
            || self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let output_field = self.output_field(&batch.schema())?;
        let child_value = self.child.evaluate(batch)?.into_array(batch.num_rows())?;

        match child_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&child_value)?;

                get_array_struct_fields(list_array, self.ordinal, output_field)
            }
            DataType::LargeList(_) => {
                let list_array = as_large_list_array(&child_value)?;

                get_array_struct_fields(list_array, self.ordinal, output_field)
            }
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected child type for ListExtract: {data_type:?}"
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
        match children.len() {
            1 => Ok(Arc::new(GetArrayStructFields::new(
                Arc::clone(&children[0]),
                self.ordinal,
            ))),
            _ => internal_err!("GetArrayStructFields should have exactly one child"),
        }
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

fn get_array_struct_fields<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    ordinal: usize,
    output_field: FieldRef,
) -> DataFusionResult<ColumnarValue> {
    let values = list_array
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("A StructType is expected");

    // Get struct column by ordinal
    let extracted_column = values.column(ordinal);

    let data = if values.null_count() == extracted_column.null_count() {
        Arc::clone(extracted_column)
    } else {
        // In some cases the column obtained from struct by ordinal doesn't
        // represent all nulls that imposed by parent values.
        // This maybe caused by a low level reader bug and needs more investigation.
        // For this specific case we patch the null buffer for the column by merging nulls buffers
        // from parent and column
        let merged_nulls = NullBuffer::union(values.nulls(), extracted_column.nulls());
        make_array(
            extracted_column
                .into_data()
                .into_builder()
                .nulls(merged_nulls)
                .build()?,
        )
    };

    // `output_field` is the same field `data_type()` declares for the extracted list's element,
    // already widened to nullable when the parent struct element is nullable. Guard the remaining
    // case where the parent's runtime null buffer is narrower than its declared nullability (a
    // low-level reader can under-report), which merges a null into `data` under a field that the
    // schema still called non-nullable; GenericListArray's constructor would then reject it.
    let field = if data.null_count() > 0 && !output_field.is_nullable() {
        Arc::new(output_field.as_ref().clone().with_nullable(true))
    } else {
        output_field
    };

    let array = GenericListArray::new(
        field,
        list_array.offsets().clone(),
        data,
        list_array.nulls().cloned(),
    );

    Ok(ColumnarValue::Array(Arc::new(array)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields};
    use datafusion::physical_expr::expressions::Column;

    /// A list of `struct<e: struct<>>` whose element `e` is declared non-nullable, but where the
    /// parent struct's null bitmap marks row 0 null (mirrors `array_repeat(n, 1).e` for a
    /// nullable `n`). Returns `(input_schema, batch, expr)` for the extraction of ordinal 0.
    fn nested_empty_struct_setup() -> (Schema, RecordBatch, GetArrayStructFields) {
        let e_field = Arc::new(Field::new("e", DataType::Struct(Fields::empty()), false));
        let e_array = StructArray::new_empty_fields(2, None);
        let n_fields = Fields::from(vec![e_field]);
        let n_values = StructArray::new(
            n_fields.clone(),
            vec![Arc::new(e_array)],
            Some(NullBuffer::from(vec![false, true])),
        );

        // The list element (`n`) is nullable while its `e` field is not.
        let elem_field = Arc::new(Field::new("item", DataType::Struct(n_fields), true));
        let offsets = OffsetBuffer::new(vec![0, 1, 2].into());
        let list_array = ListArray::new(
            Arc::clone(&elem_field),
            offsets,
            Arc::new(n_values),
            Some(NullBuffer::from(vec![true, true])),
        );

        let schema = Schema::new(vec![Field::new("arr", DataType::List(elem_field), true)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(list_array)]).unwrap();
        let expr = GetArrayStructFields::new(Arc::new(Column::new("arr", 0)), 0);
        (schema, batch, expr)
    }

    #[test]
    fn extracts_a_non_nullable_field_when_its_parent_struct_is_null() {
        let (_schema, batch, expr) = nested_empty_struct_setup();

        let extracted = expr
            .evaluate(&batch)
            .expect("must not panic")
            .into_array(2)
            .expect("must produce an array");
        let extracted_list = extracted
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("result is a ListArray");

        let e_values = extracted_list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("extracted values are a StructArray");
        assert!(e_values.is_null(0), "row 0's parent `n` was NULL");
        assert!(!e_values.is_null(1), "row 1's parent `n` was Row(Row())");
    }

    #[test]
    fn declared_data_type_matches_the_array_evaluate_produces() {
        // The reviewer's blocker: `data_type()` must advertise the same element-field nullability
        // that `evaluate()` produces, otherwise a projection re-wrapping the output in a batch
        // built from `data_type()` fails Arrow's field-vs-data validation.
        let (schema, batch, expr) = nested_empty_struct_setup();

        let declared = expr.data_type(&schema).expect("data_type");
        let output = expr.evaluate(&batch).unwrap().into_array(2).unwrap();

        assert_eq!(
            &declared,
            output.data_type(),
            "declared output type must equal the produced array's type"
        );
        // And the produced array must satisfy a batch built from the declared type.
        let out_schema = Schema::new(vec![Field::new("out", declared, true)]);
        RecordBatch::try_new(Arc::new(out_schema), vec![output])
            .expect("produced array must validate against the declared output schema");
    }

    #[test]
    fn non_nullable_parent_and_child_keeps_the_field_non_nullable() {
        // Control: when neither the list element nor the extracted field is nullable, the output
        // stays non-nullable (no over-widening from the blocker fix).
        let inner = Arc::new(Field::new("v", DataType::Int32, false));
        let s_fields = Fields::from(vec![inner]);
        let s_values = StructArray::new(
            s_fields.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
            None,
        );
        let elem_field = Arc::new(Field::new("item", DataType::Struct(s_fields), false));
        let list_array = ListArray::new(
            Arc::clone(&elem_field),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(s_values),
            None,
        );
        let schema = Schema::new(vec![Field::new("arr", DataType::List(elem_field), false)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(list_array)]).unwrap();
        let expr = GetArrayStructFields::new(Arc::new(Column::new("arr", 0)), 0);

        match expr.data_type(&schema).unwrap() {
            DataType::List(f) => assert!(!f.is_nullable(), "field must not be widened"),
            other => panic!("expected List, got {other:?}"),
        }
        let output = expr.evaluate(&batch).unwrap().into_array(2).unwrap();
        assert_eq!(&expr.data_type(&schema).unwrap(), output.data_type());
    }
}

impl Display for GetArrayStructFields {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetArrayStructFields [child: {:?}, ordinal: {:?}]",
            self.child, self.ordinal
        )
    }
}
