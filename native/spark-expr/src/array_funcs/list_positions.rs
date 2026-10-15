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

use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::{exec_err, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;

/// A `PhysicalExpr` that takes a `List<T>` input and produces a `List<Int32>` where each row's
/// values are `[0, 1, ..., len - 1]`. Per-row lengths and the null bitmap are inherited from the
/// input, so when the resulting list is unnested in parallel with the original list it produces
/// the `pos` column expected by Spark's `posexplode`.
///
/// Offsets are rebased to zero so a sliced input (whose `offsets.first()` may be non-zero while
/// `values()` stays unsliced) lines up with the freshly built values array. See
/// <https://github.com/apache/datafusion-comet/issues/5224>.
#[derive(Debug, Clone)]
pub struct ListPositionsExpr {
    child: Arc<dyn PhysicalExpr>,
    field: FieldRef,
}

impl ListPositionsExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        let field = Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ));
        Self { child, field }
    }
}

impl Display for ListPositionsExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "list_positions({})", self.child)
    }
}

impl PartialEq for ListPositionsExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for ListPositionsExpr {}

impl Hash for ListPositionsExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PhysicalExpr for ListPositionsExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let value = self.child.evaluate(batch)?;
        let array = value.into_array(batch.num_rows())?;

        let list = match array.as_any().downcast_ref::<ListArray>() {
            Some(list) => list,
            None => {
                return exec_err!(
                    "ListPositionsExpr expected List input, got {}",
                    array.data_type()
                );
            }
        };

        let offsets = list.offsets();
        // `ListArray::slice` slices `value_offsets` and `nulls` but leaves `values` unsliced, so
        // a sliced input has `offsets.first() > 0` while the underlying values run to the full
        // original length. We build a fresh values array of length
        // `offsets.last() - offsets.first()`, so we must rebase offsets to zero to line them up.
        // See https://github.com/apache/datafusion-comet/issues/5224.
        let base = offsets.first().copied().unwrap_or(0);
        let total_len = (*offsets.last().unwrap() - base) as usize;

        let mut values: Vec<i32> = Vec::with_capacity(total_len);
        for window in offsets.windows(2) {
            let start = window[0];
            let end = window[1];
            values.extend(0..(end - start));
        }

        let rebased_offsets = if base == 0 {
            offsets.clone()
        } else {
            OffsetBuffer::new(offsets.iter().map(|o| o - base).collect::<Vec<_>>().into())
        };

        let element_field = Arc::new(Field::new("item", DataType::Int32, true));
        let result = ListArray::new(
            element_field,
            rebased_offsets,
            Arc::new(Int32Array::from(values)),
            list.nulls().cloned(),
        );

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return exec_err!(
                "ListPositionsExpr expects exactly 1 child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(ListPositionsExpr::new(Arc::clone(&children[0]))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;

    fn element_field() -> Arc<Field> {
        Arc::new(Field::new("item", DataType::Int32, true))
    }

    fn evaluate_ref(list: ArrayRef) -> ArrayRef {
        let schema = Schema::new(vec![Field::new("arr", list.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![list]).unwrap();
        let expr = ListPositionsExpr::new(Arc::new(Column::new("arr", 0)));
        let result = expr.evaluate(&batch).unwrap();
        result.into_array(batch.num_rows()).unwrap()
    }

    fn evaluate(list: ListArray) -> ListArray {
        let array: ArrayRef = Arc::new(list);
        let out = evaluate_ref(array);
        out.as_any().downcast_ref::<ListArray>().unwrap().clone()
    }

    fn row_values(list: &ListArray, row: usize) -> Vec<i32> {
        let values = list.value(row);
        let ints = values.as_any().downcast_ref::<Int32Array>().unwrap();
        (0..ints.len()).map(|i| ints.value(i)).collect()
    }

    #[test]
    fn non_sliced_input_produces_position_per_row() {
        // Rows: [10, 20, 30], [40], [50, 60]. Positions must be [0,1,2],[0],[0,1].
        let values = Int32Array::from(vec![10, 20, 30, 40, 50, 60]);
        let offsets = OffsetBuffer::new(vec![0, 3, 4, 6].into());
        let input = ListArray::new(element_field(), offsets, Arc::new(values), None);
        let output = evaluate(input);

        assert_eq!(output.len(), 3);
        assert!(output.nulls().is_none());
        assert_eq!(row_values(&output, 0), vec![0, 1, 2]);
        assert_eq!(row_values(&output, 1), vec![0]);
        assert_eq!(row_values(&output, 2), vec![0, 1]);
    }

    #[test]
    fn empty_rows_produce_empty_position_rows() {
        // Rows: [], [10], []. Positions must be [], [0], [].
        let values = Int32Array::from(vec![10]);
        let offsets = OffsetBuffer::new(vec![0, 0, 1, 1].into());
        let input = ListArray::new(element_field(), offsets, Arc::new(values), None);
        let output = evaluate(input);

        assert_eq!(output.len(), 3);
        assert_eq!(output.value(0).len(), 0);
        assert_eq!(row_values(&output, 1), vec![0]);
        assert_eq!(output.value(2).len(), 0);
    }

    #[test]
    fn null_rows_are_inherited() {
        // Row 1 is null; positions must inherit the null bit so downstream unnest
        // aligns with the value array.
        let values = Int32Array::from(vec![10, 20, 30]);
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 3].into());
        let nulls = NullBuffer::from(vec![true, false, true]);
        let input = ListArray::new(element_field(), offsets, Arc::new(values), Some(nulls));
        let output = evaluate(input);

        let out_nulls = output.nulls().expect("null bitmap must be inherited");
        assert!(out_nulls.is_valid(0));
        assert!(!out_nulls.is_valid(1));
        assert!(out_nulls.is_valid(2));
        assert_eq!(row_values(&output, 0), vec![0, 1]);
        assert_eq!(row_values(&output, 2), vec![0]);
    }

    #[test]
    fn zero_row_batch() {
        let values = Int32Array::from(Vec::<i32>::new());
        let offsets = OffsetBuffer::new(vec![0].into());
        let input = ListArray::new(element_field(), offsets, Arc::new(values), None);
        let output = evaluate(input);
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn sliced_input_with_non_zero_offset_base() {
        // 5-row list, slice out rows 1..4. `offsets()` becomes [2, 3, 3, 5]
        // (base 2) while `values()` stays unsliced at length 6. Before the
        // rebase fix, `ListArray::new` panics with
        // `InvalidArgumentError("Max offset of 5 exceeds length of values 3")`.
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 3, 5, 6].into());
        let input = ListArray::new(element_field(), offsets, Arc::new(values), None);
        let sliced: ArrayRef = Arc::new(input.slice(1, 3));

        let out = evaluate_ref(sliced);
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(list.len(), 3);
        // Logical row 0: was [3]                -> [0]
        assert_eq!(row_values(list, 0), vec![0]);
        // Logical row 1: was []                 -> []
        assert_eq!(list.value(1).len(), 0);
        // Logical row 2: was [4, 5]             -> [0, 1]
        assert_eq!(row_values(list, 2), vec![0, 1]);
    }

    #[test]
    fn sliced_input_with_nulls_and_empty_rows() {
        // Underlying: [1, 2], NULL, [], [3, 4, 5], [6]. Slice rows 1..5,
        // so the logical view has a non-zero offset base and mixes null,
        // empty, and non-empty rows.
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 2, 5, 6].into());
        let nulls = NullBuffer::from(vec![true, false, true, true, true]);
        let input = ListArray::new(element_field(), offsets, Arc::new(values), Some(nulls));
        let sliced: ArrayRef = Arc::new(input.slice(1, 4));

        let out = evaluate_ref(sliced);
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 4);

        let out_nulls = list.nulls().expect("null bitmap must be inherited");
        // Logical row 0: NULL
        assert!(!out_nulls.is_valid(0));
        // Logical row 1: []
        assert!(out_nulls.is_valid(1));
        assert_eq!(list.value(1).len(), 0);
        // Logical row 2: [3, 4, 5]
        assert!(out_nulls.is_valid(2));
        assert_eq!(row_values(list, 2), vec![0, 1, 2]);
        // Logical row 3: [6]
        assert!(out_nulls.is_valid(3));
        assert_eq!(row_values(list, 3), vec![0]);
    }
}
