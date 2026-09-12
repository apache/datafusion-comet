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

use arrow::array::{Array, GenericListArray, OffsetSizeTrait, StructArray};
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{
    cast::{as_large_list_array, as_list_array},
    internal_err, DataFusionError, Result as DataFusionResult,
};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_common::child_with_parent_nulls;
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
        let list_field = self.list_field(input_schema)?;
        match list_field.data_type() {
            DataType::Struct(fields) => {
                let field = &fields[self.ordinal];
                // A null struct element yields null even when the field itself is required.
                Ok(Arc::new(field.as_ref().clone().with_nullable(
                    list_field.is_nullable() || field.is_nullable(),
                )))
            }
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {data_type:?}"
            ))),
        }
    }
}

impl PhysicalExpr for GetArrayStructFields {
    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        let struct_field = self.child_field(input_schema)?;
        match self.child.data_type(input_schema)? {
            DataType::List(_) => Ok(DataType::List(struct_field)),
            DataType::LargeList(_) => Ok(DataType::LargeList(struct_field)),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {data_type:?}"
            ))),
        }
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let child_value = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let field = self.child_field(batch.schema().as_ref())?;

        match child_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&child_value)?;

                get_array_struct_fields(list_array, self.ordinal, field)
            }
            DataType::LargeList(_) => {
                let list_array = as_large_list_array(&child_value)?;

                get_array_struct_fields(list_array, self.ordinal, field)
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
    field: FieldRef,
) -> DataFusionResult<ColumnarValue> {
    let values = list_array
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("A StructType is expected");

    // A field of a null struct is null, and Arrow keeps the children's validity independent of the
    // parent's, so the parent's nulls have to be unioned in. See
    // `datafusion_comet_common::struct_nulls`.
    //
    // This previously skipped the union when the parent and child null counts matched, which is not
    // the same question: equal counts can still sit at different rows, and then a null of the
    // parent's was dropped.
    let data = child_with_parent_nulls(values, ordinal)?;

    let array = GenericListArray::try_new(
        field,
        list_array.offsets().clone(),
        data,
        list_array.nulls().cloned(),
    )?;

    Ok(ColumnarValue::Array(Arc::new(array)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::Column;

    fn check_nullability<O: OffsetSizeTrait>() {
        for list_nullable in [true, false] {
            for element_nullable in [true, false] {
                for field_nullable in [false, true] {
                    let field = Arc::new(Field::new("a", DataType::Int32, field_nullable));
                    let values = Arc::new(StructArray::new(
                        vec![field].into(),
                        vec![Arc::new(Int32Array::from(vec![1, 99]))],
                        element_nullable.then(|| NullBuffer::from(vec![true, false])),
                    ));
                    let list = Arc::new(GenericListArray::<O>::new(
                        Arc::new(Field::new(
                            "element",
                            values.data_type().clone(),
                            element_nullable,
                        )),
                        OffsetBuffer::from_lengths([1, 1, 0, 0]),
                        values,
                        list_nullable.then(|| NullBuffer::from(vec![true, true, true, false])),
                    ));
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "l",
                        list.data_type().clone(),
                        list_nullable,
                    )]));
                    let batch = RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::<GenericListArray<O>>::clone(&list)],
                    )
                    .unwrap();
                    let expr = GetArrayStructFields::new(Arc::new(Column::new("l", 0)), 0);
                    assert_eq!(expr.nullable(&schema).unwrap(), list_nullable);
                    let result = expr.evaluate(&batch).unwrap().into_array(4).unwrap();
                    assert_eq!(expr.data_type(&schema).unwrap(), *result.data_type());
                    let result = result
                        .as_any()
                        .downcast_ref::<GenericListArray<O>>()
                        .unwrap();
                    let output_field = match result.data_type() {
                        DataType::List(field) | DataType::LargeList(field) => field,
                        _ => unreachable!(),
                    };
                    assert_eq!(
                        output_field.is_nullable(),
                        element_nullable || field_nullable
                    );
                    let values = result
                        .values()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    assert_eq!(values.value(0), 1);
                    assert_eq!(values.is_null(1), element_nullable);
                    assert_eq!(result.offsets(), list.offsets());
                    assert_eq!(result.nulls(), list.nulls());
                }
            }
        }
    }

    #[test]
    fn mismatched_output_field_returns_error() {
        let values = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("a", DataType::Int32, false))].into(),
            vec![Arc::new(Int32Array::from(vec![1]))],
            None,
        ));
        let list = GenericListArray::<i32>::new(
            Arc::new(Field::new("element", values.data_type().clone(), false)),
            OffsetBuffer::from_lengths([1]),
            values,
            None,
        );
        assert!(get_array_struct_fields(
            &list,
            0,
            Arc::new(Field::new("element", DataType::Int64, false)),
        )
        .is_err());
    }

    #[test]
    fn get_array_struct_fields_nullability() {
        check_nullability::<i32>();
        check_nullability::<i64>();
    }
}
