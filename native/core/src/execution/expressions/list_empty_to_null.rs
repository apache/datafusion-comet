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

use arrow::array::{Array, ArrayRef, ListArray, RecordBatch};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::{exec_err, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;

/// A `PhysicalExpr` that marks every empty row of a `List<T>` input as null.
/// Bridges DataFusion's `UnnestExec` (which drops empty rows under
/// `preserve_nulls=true`) to Spark's `explode_outer`/`posexplode_outer`
/// semantics. See <https://github.com/apache/datafusion/issues/19053>.
#[derive(Debug, Clone)]
pub struct ListEmptyToNullExpr {
    child: Arc<dyn PhysicalExpr>,
}

impl ListEmptyToNullExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self { child }
    }
}

impl Display for ListEmptyToNullExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "list_empty_to_null({})", self.child)
    }
}

impl PartialEq for ListEmptyToNullExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for ListEmptyToNullExpr {}

impl Hash for ListEmptyToNullExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PhysicalExpr for ListEmptyToNullExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn return_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        // Preserve the child field's name and element type; force the outer
        // list to nullable because we mark empty rows as null.
        let child_field = self.child.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            child_field.name(),
            child_field.data_type().clone(),
            true,
        )))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let value = self.child.evaluate(batch)?;
        let array = value.into_array(batch.num_rows())?;

        let Some(list) = array.as_any().downcast_ref::<ListArray>() else {
            return exec_err!(
                "ListEmptyToNullExpr expected List input, got {}",
                array.data_type()
            );
        };

        let offsets = list.offsets();
        let len = list.len();
        let existing_nulls = list.nulls();

        // Fast path: no currently-valid row is empty, so the input already
        // satisfies outer semantics. `is_valid` returns true when `nulls` is
        // `None`, so this single scan short-circuits on the first empty
        // valid row without allocating.
        let has_valid_empty = (0..len)
            .any(|i| offsets[i + 1] == offsets[i] && existing_nulls.is_none_or(|n| n.is_valid(i)));
        if !has_valid_empty {
            return Ok(ColumnarValue::Array(Arc::clone(&array)));
        }

        let combined = BooleanBuffer::collect_bool(len, |i| {
            offsets[i + 1] > offsets[i] && existing_nulls.is_none_or(|n| n.is_valid(i))
        });
        let new_nulls = NullBuffer::new(combined);

        let DataType::List(element_field) = list.data_type() else {
            unreachable!("ListArray downcast guarantees DataType::List");
        };

        let result = ListArray::try_new(
            Arc::clone(element_field),
            offsets.clone(),
            Arc::clone(list.values()),
            Some(new_nulls),
        )?;

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
                "ListEmptyToNullExpr expects exactly 1 child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(ListEmptyToNullExpr::new(Arc::clone(&children[0]))))
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

    fn list_field(nullable: bool) -> Arc<Field> {
        Arc::new(Field::new("arr", DataType::List(element_field()), nullable))
    }

    fn build_batch(list: ArrayRef) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("arr", list.data_type().clone(), true)]);
        RecordBatch::try_new(Arc::new(schema), vec![list]).unwrap()
    }

    fn evaluate_ref(list: ArrayRef) -> ArrayRef {
        let batch = build_batch(list);
        let expr = ListEmptyToNullExpr::new(Arc::new(Column::new("arr", 0)));
        let result = expr.evaluate(&batch).unwrap();
        result.into_array(batch.num_rows()).unwrap()
    }

    fn evaluate(list: ListArray) -> ListArray {
        let array: ArrayRef = Arc::new(list);
        let out = evaluate_ref(array);
        out.as_any().downcast_ref::<ListArray>().unwrap().clone()
    }

    #[test]
    fn fast_path_no_empty_rows_returns_input_untouched() {
        // Rows: [1,2,3], [4], [5,6] -- no empty rows, so the fast path should
        // return the original array pointer without allocating a new bitmap.
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let offsets = OffsetBuffer::new(vec![0, 3, 4, 6].into());
        let input: ArrayRef = Arc::new(ListArray::new(
            element_field(),
            offsets,
            Arc::new(values),
            None,
        ));

        let out = evaluate_ref(Arc::clone(&input));
        assert!(
            Arc::ptr_eq(&out, &input),
            "fast path should return the same ArrayRef"
        );
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.nulls().is_none());
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn fast_path_when_only_empty_row_is_already_null() {
        // Rows: [1,2], NULL (offsets 2..2 -- looks empty), [3]. The middle row
        // is already null so the fast path applies without materializing a new
        // bitmap.
        let values = Int32Array::from(vec![1, 2, 3]);
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 3].into());
        let nulls = NullBuffer::from(vec![true, false, true]);
        let input: ArrayRef = Arc::new(ListArray::new(
            element_field(),
            offsets,
            Arc::new(values),
            Some(nulls),
        ));

        let out = evaluate_ref(Arc::clone(&input));
        assert!(
            Arc::ptr_eq(&out, &input),
            "fast path should return the same ArrayRef when only empty rows are already null"
        );
    }

    #[test]
    fn mixed_empty_null_and_non_empty_rows() {
        // Rows: [10, 20], [], NULL, [30]. The empty row (index 1) must become
        // null; the already-null row (index 2) must stay null; the two data
        // rows must survive intact.
        let values = Int32Array::from(vec![10, 20, 30]);
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 2, 3].into());
        let nulls = NullBuffer::from(vec![true, true, false, true]);
        let input = ListArray::new(element_field(), offsets, Arc::new(values), Some(nulls));
        let output = evaluate(input);
        let out_nulls = output.nulls().expect("nulls buffer must be present");
        assert!(out_nulls.is_valid(0));
        assert!(!out_nulls.is_valid(1), "empty row must be marked null");
        assert!(!out_nulls.is_valid(2), "already-null row must stay null");
        assert!(out_nulls.is_valid(3));
        // Offsets and values must be preserved so downstream unnest still
        // reads the original element slices for the valid rows.
        assert_eq!(output.value(0).len(), 2);
        assert_eq!(output.value(3).len(), 1);
    }

    #[test]
    fn empty_row_without_prior_null_bitmap() {
        // Fresh (no nulls) input containing one empty row must materialize a
        // null bitmap with only that row cleared.
        let values = Int32Array::from(vec![1, 2, 3]);
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 3].into());
        let input = ListArray::new(element_field(), offsets, Arc::new(values), None);
        let output = evaluate(input);
        let out_nulls = output.nulls().expect("nulls buffer must be materialized");
        assert!(out_nulls.is_valid(0));
        assert!(!out_nulls.is_valid(1));
        assert!(out_nulls.is_valid(2));
    }

    #[test]
    fn zero_row_batch_takes_fast_path() {
        // A zero-row batch has no rows to inspect, so the fast path returns
        // the input untouched.
        let values = Int32Array::from(Vec::<i32>::new());
        let offsets = OffsetBuffer::new(vec![0].into());
        let input: ArrayRef = Arc::new(ListArray::new(
            element_field(),
            offsets,
            Arc::new(values),
            None,
        ));

        let out = evaluate_ref(Arc::clone(&input));
        assert_eq!(out.len(), 0);
        assert!(
            Arc::ptr_eq(&out, &input),
            "zero-row batch should take the fast path"
        );
    }

    #[test]
    fn sliced_input_with_non_zero_offset() {
        // Build a 5-row list, then slice out rows 1..4 -- exposes non-zero
        // logical offset while `values()` stays unsliced. The empty row that
        // now lives at logical index 1 (was index 2 in the underlying array)
        // must be flipped to null.
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 3, 5, 6].into());
        let input = ListArray::new(element_field(), offsets, Arc::new(values), None);
        let sliced = input.slice(1, 3);
        let output = evaluate(sliced);
        assert_eq!(output.len(), 3);
        let out_nulls = output.nulls().expect("nulls buffer must be materialized");
        // Logical row 0: was [3] -- non-empty
        assert!(out_nulls.is_valid(0));
        // Logical row 1: was [] -- must be null
        assert!(!out_nulls.is_valid(1));
        // Logical row 2: was [4, 5] -- non-empty
        assert!(out_nulls.is_valid(2));
        // The non-empty rows must still expose their original elements.
        assert_eq!(output.value(0).len(), 1);
        assert_eq!(output.value(2).len(), 2);
    }

    #[test]
    fn return_field_forces_nullable() {
        // The output field must be nullable regardless of the child's
        // nullability, because empty rows are marked null downstream.
        let schema = Schema::new(vec![list_field(false).as_ref().clone()]);
        let expr = ListEmptyToNullExpr::new(Arc::new(Column::new("arr", 0)));
        let field = expr.return_field(&schema).unwrap();
        assert!(field.is_nullable());
        assert_eq!(field.name(), "arr");
    }
}
