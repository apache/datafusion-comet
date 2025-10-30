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

use arrow::array::{make_array, Array, ArrayRef, GenericListArray, Int32Array, OffsetSizeTrait};
use arrow::datatypes::{DataType, Schema};
use arrow::{
    array::{as_primitive_array, Capacities, MutableArrayData},
    buffer::{NullBuffer, OffsetBuffer},
    record_batch::RecordBatch,
};
use datafusion::common::{
    cast::{as_large_list_array, as_list_array},
    internal_err, DataFusionError, Result as DataFusionResult,
};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

// 2147483632 == java.lang.Integer.MAX_VALUE - 15
// It is a value of ByteArrayUtils.MAX_ROUNDED_ARRAY_LENGTH
// https://github.com/apache/spark/blob/master/common/utils/src/main/java/org/apache/spark/unsafe/array/ByteArrayUtils.java
const MAX_ROUNDED_ARRAY_LENGTH: usize = 2147483632;

#[derive(Debug, Eq)]
pub struct ArrayInsert {
    src_array_expr: Arc<dyn PhysicalExpr>,
    pos_expr: Arc<dyn PhysicalExpr>,
    item_expr: Arc<dyn PhysicalExpr>,
    legacy_negative_index: bool,
}

impl Hash for ArrayInsert {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.src_array_expr.hash(state);
        self.pos_expr.hash(state);
        self.item_expr.hash(state);
        self.legacy_negative_index.hash(state);
    }
}
impl PartialEq for ArrayInsert {
    fn eq(&self, other: &Self) -> bool {
        self.src_array_expr.eq(&other.src_array_expr)
            && self.pos_expr.eq(&other.pos_expr)
            && self.item_expr.eq(&other.item_expr)
            && self.legacy_negative_index.eq(&other.legacy_negative_index)
    }
}

impl ArrayInsert {
    pub fn new(
        src_array_expr: Arc<dyn PhysicalExpr>,
        pos_expr: Arc<dyn PhysicalExpr>,
        item_expr: Arc<dyn PhysicalExpr>,
        legacy_negative_index: bool,
    ) -> Self {
        Self {
            src_array_expr,
            pos_expr,
            item_expr,
            legacy_negative_index,
        }
    }

    pub fn array_type(&self, data_type: &DataType) -> DataFusionResult<DataType> {
        match data_type {
            DataType::List(field) => Ok(DataType::List(Arc::clone(field))),
            DataType::LargeList(field) => Ok(DataType::LargeList(Arc::clone(field))),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected src array type in ArrayInsert: {data_type:?}"
            ))),
        }
    }
}

impl PhysicalExpr for ArrayInsert {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        self.array_type(&self.src_array_expr.data_type(input_schema)?)
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        self.src_array_expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let pos_value = self
            .pos_expr
            .evaluate(batch)?
            .into_array(batch.num_rows())?;

        // Spark supports only IntegerType (Int32):
        // https://github.com/apache/spark/blob/branch-3.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/collectionOperations.scala#L4737
        if !matches!(pos_value.data_type(), DataType::Int32) {
            return Err(DataFusionError::Internal(format!(
                "Unexpected index data type in ArrayInsert: {:?}, expected type is Int32",
                pos_value.data_type()
            )));
        }

        // Check that src array is actually an array and get it's value type
        let src_value = self
            .src_array_expr
            .evaluate(batch)?
            .into_array(batch.num_rows())?;

        let src_element_type = match self.array_type(src_value.data_type())? {
            DataType::List(field) => &field.data_type().clone(),
            DataType::LargeList(field) => &field.data_type().clone(),
            _ => unreachable!(),
        };

        // Check that inserted value has the same type as an array
        let item_value = self
            .item_expr
            .evaluate(batch)?
            .into_array(batch.num_rows())?;
        if item_value.data_type() != src_element_type {
            return Err(DataFusionError::Internal(format!(
                "Type mismatch in ArrayInsert: array type is {:?} but item type is {:?}",
                src_element_type,
                item_value.data_type()
            )));
        }

        match src_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&src_value)?;
                array_insert(
                    list_array,
                    &item_value,
                    &pos_value,
                    self.legacy_negative_index,
                )
            }
            DataType::LargeList(_) => {
                let list_array = as_large_list_array(&src_value)?;
                array_insert(
                    list_array,
                    &item_value,
                    &pos_value,
                    self.legacy_negative_index,
                )
            }
            _ => unreachable!(), // This case is checked already
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.src_array_expr, &self.pos_expr, &self.item_expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        match children.len() {
            3 => Ok(Arc::new(ArrayInsert::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
                Arc::clone(&children[2]),
                self.legacy_negative_index,
            ))),
            _ => internal_err!("ArrayInsert should have exactly three childrens"),
        }
    }
}

fn array_insert<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    items_array: &ArrayRef,
    pos_array: &ArrayRef,
    legacy_mode: bool,
) -> DataFusionResult<ColumnarValue> {
    // Implementation aligned with Arrow's half-open offset ranges and Spark semantics.

    let values = list_array.values();
    let offsets = list_array.offsets();
    let values_data = values.to_data();
    let item_data = items_array.to_data();

    // Estimate capacity (original values + inserted items upper bound)
    let new_capacity = Capacities::Array(values_data.len() + item_data.len());

    let mut mutable_values =
        MutableArrayData::with_capacities(vec![&values_data, &item_data], true, new_capacity);

    // New offsets and top-level list validity bitmap
    let mut new_offsets = Vec::with_capacity(list_array.len() + 1);
    new_offsets.push(O::usize_as(0));
    let mut list_valid = Vec::<bool>::with_capacity(list_array.len());

    // Spark supports only Int32 position indices
    let pos_data: &Int32Array = as_primitive_array(&pos_array);

    for (row_index, window) in offsets.windows(2).enumerate() {
        let start = window[0].as_usize();
        let end = window[1].as_usize();
        let len = end - start;
        let pos = pos_data.value(row_index);

        if list_array.is_null(row_index) {
            // Top-level list row is NULL: do not write any child values and do not advance offset
            new_offsets.push(new_offsets[row_index]);
            list_valid.push(false);
            continue;
        }

        if pos == 0 {
            return Err(DataFusionError::Internal(
                "Position for array_insert should be greater or less than zero".to_string(),
            ));
        }

        let final_len: usize;

        if pos > 0 {
            // Positive index (1-based)
            let pos1 = pos as usize;
            if pos1 <= len + 1 {
                // In-range insertion (including appending to end)
                let corrected = pos1 - 1; // 0-based insertion point
                mutable_values.extend(0, start, start + corrected);
                mutable_values.extend(1, row_index, row_index + 1);
                mutable_values.extend(0, start + corrected, end);
                final_len = len + 1;
            } else {
                // Beyond end: pad with nulls then insert
                let corrected = pos1 - 1;
                let padding = corrected - len;
                mutable_values.extend(0, start, end);
                mutable_values.extend_nulls(padding);
                mutable_values.extend(1, row_index, row_index + 1);
                final_len = corrected + 1; // equals pos1
            }
        } else {
            // Negative index (1-based from the end)
            let k = (-pos) as usize;

            if k <= len {
                // In-range negative insertion
                // Non-legacy: -1 behaves like append to end (corrected = len - k + 1)
                // Legacy:     -1 behaves like insert before the last element (corrected = len - k)
                let base_offset = if legacy_mode { 0 } else { 1 };
                let corrected = len - k + base_offset;
                mutable_values.extend(0, start, start + corrected);
                mutable_values.extend(1, row_index, row_index + 1);
                mutable_values.extend(0, start + corrected, end);
                final_len = len + 1;
            } else {
                // Negative index beyond the start (Spark-specific behavior):
                // Place item first, then pad with nulls, then append the original array.
                // Final length = k + base_offset, where base_offset = 1 in legacy mode, otherwise 0.
                let base_offset = if legacy_mode { 1 } else { 0 };
                let target_len = k + base_offset;
                let padding = target_len.saturating_sub(len + 1);
                mutable_values.extend(1, row_index, row_index + 1); // insert item first
                mutable_values.extend_nulls(padding); // pad nulls
                mutable_values.extend(0, start, end); // append original values
                final_len = target_len;
            }
        }

        if final_len > MAX_ROUNDED_ARRAY_LENGTH {
            return Err(DataFusionError::Internal(format!(
                "Max array length in Spark is {MAX_ROUNDED_ARRAY_LENGTH}, but got {final_len}"
            )));
        }

        let prev = new_offsets[row_index].as_usize();
        new_offsets.push(O::usize_as(prev + final_len));
        list_valid.push(true);
    }

    let child = make_array(mutable_values.freeze());

    // Reuse the original list element field (name/type/nullability)
    let elem_field = match list_array.data_type() {
        DataType::List(field) => Arc::clone(field),
        DataType::LargeList(field) => Arc::clone(field),
        _ => unreachable!(),
    };

    // Build the resulting list array
    let new_list = GenericListArray::<O>::try_new(
        elem_field,
        OffsetBuffer::new(new_offsets.into()),
        child,
        Some(NullBuffer::new(list_valid.into())),
    )?;

    Ok(ColumnarValue::Array(Arc::new(new_list)))
}

impl Display for ArrayInsert {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ArrayInsert [array: {:?}, pos: {:?}, item: {:?}]",
            self.src_array_expr, self.pos_expr, self.item_expr
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int32Array, ListArray};
    use arrow::datatypes::Int32Type;
    use datafusion::common::Result;
    use datafusion::physical_plan::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_array_insert() -> Result<()> {
        // Test inserting an item into a list array
        // Inputs and expected values are taken from the Spark results
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![None]),
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
        ]);

        let positions = Int32Array::from(vec![2, 1, 1, 5, 6, 1]);
        let items = Int32Array::from(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(100),
            Some(100),
            Some(40),
        ]);

        let ColumnarValue::Array(result) = array_insert(
            &list,
            &(Arc::new(items) as ArrayRef),
            &(Arc::new(positions) as ArrayRef),
            false,
        )?
        else {
            unreachable!()
        };

        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(10), Some(2), Some(3)]),
            Some(vec![Some(20), Some(4), Some(5)]),
            Some(vec![Some(30), None]),
            Some(vec![Some(1), Some(2), Some(3), None, Some(100)]),
            Some(vec![Some(1), Some(2), Some(3), None, None, Some(100)]),
            None,
        ]);

        assert_eq!(&result.to_data(), &expected.to_data());

        Ok(())
    }

    #[test]
    fn test_array_insert_negative_index() -> Result<()> {
        // Test insert with negative index
        // Inputs and expected values are taken from the Spark results
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(1)]),
            None,
        ]);

        let positions = Int32Array::from(vec![-2, -1, -3, -1]);
        let items = Int32Array::from(vec![Some(10), Some(20), Some(100), Some(30)]);

        let ColumnarValue::Array(result) = array_insert(
            &list,
            &(Arc::new(items) as ArrayRef),
            &(Arc::new(positions) as ArrayRef),
            false,
        )?
        else {
            unreachable!()
        };

        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(10), Some(3)]),
            Some(vec![Some(4), Some(5), Some(20)]),
            Some(vec![Some(100), None, Some(1)]),
            None,
        ]);

        assert_eq!(&result.to_data(), &expected.to_data());

        Ok(())
    }

    #[test]
    fn test_array_insert_legacy_mode() -> Result<()> {
        // Test the so-called "legacy" mode exisiting in the Spark
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            None,
        ]);

        let positions = Int32Array::from(vec![-1, -1, -1]);
        let items = Int32Array::from(vec![Some(10), Some(20), Some(30)]);

        let ColumnarValue::Array(result) = array_insert(
            &list,
            &(Arc::new(items) as ArrayRef),
            &(Arc::new(positions) as ArrayRef),
            true,
        )?
        else {
            unreachable!()
        };

        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(10), Some(3)]),
            Some(vec![Some(4), Some(20), Some(5)]),
            None,
        ]);

        assert_eq!(&result.to_data(), &expected.to_data());

        Ok(())
    }

    #[test]
    fn test_array_insert_bug_repro_null_item_pos1_fixed() -> Result<()> {
        use arrow::array::{Array, ArrayRef, Int32Array, ListArray};
        use arrow::datatypes::Int32Type;

        // row0 = [0, null, 0]
        // row1 = [1, null, 1]
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(0), None, Some(0)]),
            Some(vec![Some(1), None, Some(1)]),
        ]);

        let positions = Int32Array::from(vec![1, 1]);
        let items = Int32Array::from(vec![None, None]);

        let ColumnarValue::Array(result) = array_insert(
            &list,
            &(Arc::new(items) as ArrayRef),
            &(Arc::new(positions) as ArrayRef),
            false, // legacy_mode = false
        )?
        else {
            unreachable!()
        };

        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![None, Some(0), None, Some(0)]),
            Some(vec![None, Some(1), None, Some(1)]),
        ]);
        assert_eq!(&result.to_data(), &expected.to_data());
        Ok(())
    }
}
