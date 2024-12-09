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

use arrow::{array::MutableArrayData, datatypes::ArrowNativeType, record_batch::RecordBatch};
use arrow_array::{Array, GenericListArray, Int32Array, OffsetSizeTrait, StructArray};
use arrow_schema::{DataType, FieldRef, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr_common::physical_expr::down_cast_any_ref;
use datafusion_common::{
    cast::{as_int32_array, as_large_list_array, as_list_array},
    internal_err, DataFusionError, Result as DataFusionResult, ScalarValue,
};
use datafusion_physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};
#[derive(Debug, Hash)]
pub struct ListExtract {
    child: Arc<dyn PhysicalExpr>,
    ordinal: Arc<dyn PhysicalExpr>,
    default_value: Option<Arc<dyn PhysicalExpr>>,
    one_based: bool,
    fail_on_error: bool,
}

impl ListExtract {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        ordinal: Arc<dyn PhysicalExpr>,
        default_value: Option<Arc<dyn PhysicalExpr>>,
        one_based: bool,
        fail_on_error: bool,
    ) -> Self {
        Self {
            child,
            ordinal,
            default_value,
            one_based,
            fail_on_error,
        }
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        match self.child.data_type(input_schema)? {
            DataType::List(field) | DataType::LargeList(field) => Ok(field),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in ListExtract: {:?}",
                data_type
            ))),
        }
    }
}

impl PhysicalExpr for ListExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.child_field(input_schema)?.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        // Only non-nullable if fail_on_error is enabled and the element is non-nullable
        Ok(!self.fail_on_error || self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let child_value = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let ordinal_value = self.ordinal.evaluate(batch)?.into_array(batch.num_rows())?;

        let default_value = self
            .default_value
            .as_ref()
            .map(|d| {
                d.evaluate(batch).map(|value| match value {
                    ColumnarValue::Scalar(scalar)
                        if !scalar.data_type().equals_datatype(child_value.data_type()) =>
                    {
                        scalar.cast_to(child_value.data_type())
                    }
                    ColumnarValue::Scalar(scalar) => Ok(scalar),
                    v => Err(DataFusionError::Execution(format!(
                        "Expected scalar default value for ListExtract, got {:?}",
                        v
                    ))),
                })
            })
            .transpose()?
            .unwrap_or(self.data_type(&batch.schema())?.try_into())?;

        let adjust_index = if self.one_based {
            one_based_index
        } else {
            zero_based_index
        };

        match child_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&child_value)?;
                let index_array = as_int32_array(&ordinal_value)?;

                list_extract(
                    list_array,
                    index_array,
                    &default_value,
                    self.fail_on_error,
                    adjust_index,
                )
            }
            DataType::LargeList(_) => {
                let list_array = as_large_list_array(&child_value)?;
                let index_array = as_int32_array(&ordinal_value)?;

                list_extract(
                    list_array,
                    index_array,
                    &default_value,
                    self.fail_on_error,
                    adjust_index,
                )
            }
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected child type for ListExtract: {:?}",
                data_type
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child, &self.ordinal]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            2 => Ok(Arc::new(ListExtract::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
                self.default_value.clone(),
                self.one_based,
                self.fail_on_error,
            ))),
            _ => internal_err!("ListExtract should have exactly two children"),
        }
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.ordinal.hash(&mut s);
        self.default_value.hash(&mut s);
        self.one_based.hash(&mut s);
        self.fail_on_error.hash(&mut s);
        self.hash(&mut s);
    }
}

fn one_based_index(index: i32, len: usize) -> DataFusionResult<Option<usize>> {
    if index == 0 {
        return Err(DataFusionError::Execution(
            "Invalid index of 0 for one-based ListExtract".to_string(),
        ));
    }

    let abs_index = index.abs().as_usize();
    if abs_index <= len {
        if index > 0 {
            Ok(Some(abs_index - 1))
        } else {
            Ok(Some(len - abs_index))
        }
    } else {
        Ok(None)
    }
}

fn zero_based_index(index: i32, len: usize) -> DataFusionResult<Option<usize>> {
    if index < 0 {
        Ok(None)
    } else {
        let positive_index = index.as_usize();
        if positive_index < len {
            Ok(Some(positive_index))
        } else {
            Ok(None)
        }
    }
}

fn list_extract<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    index_array: &Int32Array,
    default_value: &ScalarValue,
    fail_on_error: bool,
    adjust_index: impl Fn(i32, usize) -> DataFusionResult<Option<usize>>,
) -> DataFusionResult<ColumnarValue> {
    let values = list_array.values();
    let offsets = list_array.offsets();

    let data = values.to_data();

    let default_data = default_value.to_array()?.to_data();

    let mut mutable = MutableArrayData::new(vec![&data, &default_data], true, index_array.len());

    for (row, (offset_window, index)) in offsets.windows(2).zip(index_array.values()).enumerate() {
        let start = offset_window[0].as_usize();
        let len = offset_window[1].as_usize() - start;

        if let Some(i) = adjust_index(*index, len)? {
            mutable.extend(0, start + i, start + i + 1);
        } else if list_array.is_null(row) {
            mutable.extend_nulls(1);
        } else if fail_on_error {
            return Err(DataFusionError::Execution(
                "Index out of bounds for array".to_string(),
            ));
        } else {
            mutable.extend(1, 0, 1);
        }
    }

    let data = mutable.freeze();
    Ok(ColumnarValue::Array(arrow::array::make_array(data)))
}

impl Display for ListExtract {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ListExtract [child: {:?}, ordinal: {:?}, default_value: {:?}, one_based: {:?}, fail_on_error: {:?}]",
            self.child, self.ordinal,  self.default_value, self.one_based, self.fail_on_error
        )
    }
}

impl PartialEq<dyn Any> for ListExtract {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.child.eq(&x.child)
                    && self.ordinal.eq(&x.ordinal)
                    && (self.default_value.is_none() == x.default_value.is_none())
                    && self
                        .default_value
                        .as_ref()
                        .zip(x.default_value.as_ref())
                        .map(|(s, x)| s.eq(x))
                        .unwrap_or(true)
                    && self.one_based.eq(&x.one_based)
                    && self.fail_on_error.eq(&x.fail_on_error)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug, Hash)]
pub struct GetArrayStructFields {
    child: Arc<dyn PhysicalExpr>,
    ordinal: usize,
}

impl GetArrayStructFields {
    pub fn new(child: Arc<dyn PhysicalExpr>, ordinal: usize) -> Self {
        Self { child, ordinal }
    }

    fn list_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        match self.child.data_type(input_schema)? {
            DataType::List(field) | DataType::LargeList(field) => Ok(field),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {:?}",
                data_type
            ))),
        }
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        match self.list_field(input_schema)?.data_type() {
            DataType::Struct(fields) => Ok(Arc::clone(&fields[self.ordinal])),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {:?}",
                data_type
            ))),
        }
    }
}

impl PhysicalExpr for GetArrayStructFields {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        let struct_field = self.child_field(input_schema)?;
        match self.child.data_type(input_schema)? {
            DataType::List(_) => Ok(DataType::List(struct_field)),
            DataType::LargeList(_) => Ok(DataType::LargeList(struct_field)),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {:?}",
                data_type
            ))),
        }
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(self.list_field(input_schema)?.is_nullable()
            || self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let child_value = self.child.evaluate(batch)?.into_array(batch.num_rows())?;

        match child_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&child_value)?;

                get_array_struct_fields(list_array, self.ordinal)
            }
            DataType::LargeList(_) => {
                let list_array = as_large_list_array(&child_value)?;

                get_array_struct_fields(list_array, self.ordinal)
            }
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected child type for ListExtract: {:?}",
                data_type
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(GetArrayStructFields::new(
                Arc::clone(&children[0]),
                self.ordinal,
            ))),
            _ => internal_err!("GetArrayStructFields should have exactly one child"),
        }
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.ordinal.hash(&mut s);
        self.hash(&mut s);
    }
}

fn get_array_struct_fields<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    ordinal: usize,
) -> DataFusionResult<ColumnarValue> {
    let values = list_array
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("A struct is expected");

    let column = Arc::clone(values.column(ordinal));
    let field = Arc::clone(&values.fields()[ordinal]);

    let offsets = list_array.offsets();
    let array = GenericListArray::new(field, offsets.clone(), column, list_array.nulls().cloned());

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

impl PartialEq<dyn Any> for GetArrayStructFields {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.child.eq(&x.child) && self.ordinal.eq(&x.ordinal))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod test {
    use crate::list::{list_extract, zero_based_index};

    use arrow::datatypes::Int32Type;
    use arrow_array::{Array, Int32Array, ListArray};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::ColumnarValue;

    #[test]
    fn test_list_extract_default_value() -> Result<()> {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1)]),
            None,
            Some(vec![]),
        ]);
        let indices = Int32Array::from(vec![0, 0, 0]);

        let null_default = ScalarValue::Int32(None);

        let ColumnarValue::Array(result) =
            list_extract(&list, &indices, &null_default, false, zero_based_index)?
        else {
            unreachable!()
        };

        assert_eq!(
            &result.to_data(),
            &Int32Array::from(vec![Some(1), None, None]).to_data()
        );

        let zero_default = ScalarValue::Int32(Some(0));

        let ColumnarValue::Array(result) =
            list_extract(&list, &indices, &zero_default, false, zero_based_index)?
        else {
            unreachable!()
        };

        assert_eq!(
            &result.to_data(),
            &Int32Array::from(vec![Some(1), None, Some(0)]).to_data()
        );
        Ok(())
    }
}
