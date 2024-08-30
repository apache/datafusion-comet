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
use arrow_array::{Array, GenericListArray, Int32Array, OffsetSizeTrait};
use arrow_schema::{DataType, FieldRef, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{
    cast::{as_int32_array, as_large_list_array, as_list_array},
    DataFusionError, Result as DataFusionResult, ScalarValue,
};
use datafusion_physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::utils::down_cast_any_ref;

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

                array_extract(
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

                array_extract(
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
        Ok(Arc::new(ListExtract::new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.default_value.clone(),
            self.one_based,
            self.fail_on_error,
        )))
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

fn array_extract<O: OffsetSizeTrait>(
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

// #[cfg(test)]
// mod test {
//     use super::CreateNamedStruct;
//     use arrow_array::{Array, DictionaryArray, Int32Array, RecordBatch, StringArray};
//     use arrow_schema::{DataType, Field, Schema};
//     use datafusion_common::Result;
//     use datafusion_expr::ColumnarValue;
//     use datafusion_physical_expr_common::expressions::column::Column;
//     use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
//     use std::sync::Arc;

//     #[test]
//     fn test_create_struct_from_dict_encoded_i32() -> Result<()> {
//         let keys = Int32Array::from(vec![0, 1, 2]);
//         let values = Int32Array::from(vec![0, 111, 233]);
//         let dict = DictionaryArray::try_new(keys, Arc::new(values))?;
//         let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
//         let schema = Schema::new(vec![Field::new("a", data_type, false)]);
//         let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)])?;
//         let field_names = vec!["a".to_string()];
//         let x = CreateNamedStruct::new(vec![Arc::new(Column::new("a", 0))], field_names);
//         let ColumnarValue::Array(x) = x.evaluate(&batch)? else {
//             unreachable!()
//         };
//         assert_eq!(3, x.len());
//         Ok(())
//     }

//     #[test]
//     fn test_create_struct_from_dict_encoded_string() -> Result<()> {
//         let keys = Int32Array::from(vec![0, 1, 2]);
//         let values = StringArray::from(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
//         let dict = DictionaryArray::try_new(keys, Arc::new(values))?;
//         let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
//         let schema = Schema::new(vec![Field::new("a", data_type, false)]);
//         let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)])?;
//         let field_names = vec!["a".to_string()];
//         let x = CreateNamedStruct::new(vec![Arc::new(Column::new("a", 0))], field_names);
//         let ColumnarValue::Array(x) = x.evaluate(&batch)? else {
//             unreachable!()
//         };
//         assert_eq!(3, x.len());
//         Ok(())
//     }
// }
