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

//! Spark-compatible `arrays_overlap` with correct SQL three-valued null logic.
//!
//! DataFusion's `array_has_any` uses `RowConverter` for element comparison, which
//! treats NULL == NULL as true (grouping semantics). Spark's `arrays_overlap` uses
//! SQL equality where NULL == NULL is unknown (null). This implementation correctly
//! returns:
//!   - true  if any non-null element appears in both arrays
//!   - null  if no definite overlap but either array contains null elements
//!   - false if no overlap and neither array contains null elements

use arrow::array::{Array, ArrayRef, BooleanArray, GenericListArray, OffsetSizeTrait, Scalar};
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{exec_err, utils::take_function_args, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArraysOverlap {
    signature: Signature,
}

impl Default for SparkArraysOverlap {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArraysOverlap {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArraysOverlap {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_arrays_overlap"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(
        &self,
        _args: datafusion::logical_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        Ok(Arc::new(arrow::datatypes::Field::new(
            self.name(),
            DataType::Boolean,
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = take_function_args(self.name(), &args.args)?;

        // Return null if either input is a null scalar
        if let ColumnarValue::Scalar(s) = &left {
            if s.is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
            }
        }
        if let ColumnarValue::Scalar(s) = &right {
            if s.is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
            }
        }

        match (left, right) {
            (ColumnarValue::Array(left_arr), ColumnarValue::Array(right_arr)) => {
                let result = match (left_arr.data_type(), right_arr.data_type()) {
                    (DataType::List(_), DataType::List(_)) => arrays_overlap_list::<i32>(
                        left_arr.as_any().downcast_ref().unwrap(),
                        right_arr.as_any().downcast_ref().unwrap(),
                    )?,
                    (DataType::LargeList(_), DataType::LargeList(_)) => arrays_overlap_list::<i64>(
                        left_arr.as_any().downcast_ref().unwrap(),
                        right_arr.as_any().downcast_ref().unwrap(),
                    )?,
                    (l, r) => {
                        return exec_err!(
                            "spark_arrays_overlap does not support types '{l}' and '{r}'"
                        )
                    }
                };
                Ok(ColumnarValue::Array(result))
            }
            (left, right) => {
                // Handle scalar inputs by converting to arrays
                let left_arr = left.to_array(1)?;
                let right_arr = right.to_array(1)?;
                let result = match (left_arr.data_type(), right_arr.data_type()) {
                    (DataType::List(_), DataType::List(_)) => arrays_overlap_list::<i32>(
                        left_arr.as_any().downcast_ref().unwrap(),
                        right_arr.as_any().downcast_ref().unwrap(),
                    )?,
                    (DataType::LargeList(_), DataType::LargeList(_)) => arrays_overlap_list::<i64>(
                        left_arr.as_any().downcast_ref().unwrap(),
                        right_arr.as_any().downcast_ref().unwrap(),
                    )?,
                    (l, r) => {
                        return exec_err!(
                            "spark_arrays_overlap does not support types '{l}' and '{r}'"
                        )
                    }
                };
                let scalar = ScalarValue::try_from_array(&result, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }
}

/// Spark-compatible arrays_overlap with SQL three-valued null logic.
///
/// For each row, compares elements of two list arrays and returns:
/// - null if either array is null
/// - true if any non-null element appears in both arrays
/// - null if no definite overlap but either array contains null elements
/// - false otherwise
fn arrays_overlap_list<OffsetSize: OffsetSizeTrait>(
    left: &GenericListArray<OffsetSize>,
    right: &GenericListArray<OffsetSize>,
) -> Result<ArrayRef> {
    let len = left.len();
    let mut builder = BooleanArray::builder(len);

    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
            continue;
        }

        let left_values = left.value(i);
        let right_values = right.value(i);

        // Empty array cannot overlap
        if left_values.is_empty() || right_values.is_empty() {
            builder.append_value(false);
            continue;
        }

        // DataFusion's make_array(NULL) produces a List<Null> with NullArray values.
        // NullArray means all elements are null by definition.
        if left_values.data_type() == &DataType::Null || right_values.data_type() == &DataType::Null
        {
            builder.append_null();
            continue;
        }

        let mut found_overlap = false;
        let mut has_null_eq = false;

        // Ensure smaller array is on the probe side for fewer comparisons
        let (probe, search) = if left_values.len() <= right_values.len() {
            (&left_values, &right_values)
        } else {
            (&right_values, &left_values)
        };

        let is_nested = matches!(
            left_values.data_type(),
            DataType::List(_) | DataType::LargeList(_)
        );

        if is_nested {
            // Element-by-element comparison for nested types where Arrow's eq kernel
            // does not support direct comparison.
            'outer: for pi in 0..probe.len() {
                if probe.is_null(pi) {
                    has_null_eq = true;
                    continue;
                }
                for si in 0..search.len() {
                    if search.is_null(si) {
                        has_null_eq = true;
                        continue;
                    }
                    match three_valued_eq(probe.as_ref(), pi, search.as_ref(), si)? {
                        Some(true) => {
                            found_overlap = true;
                            break 'outer;
                        }
                        None => has_null_eq = true,
                        Some(false) => {}
                    }
                }
            }
        } else {
            // Vectorized comparison for flat types using Arrow's eq kernel.
            // One kernel call per probe element: O(p * s) total work with vectorized inner loop.
            for pi in 0..probe.len() {
                if probe.is_null(pi) {
                    continue;
                }
                let scalar = Scalar::new(probe.slice(pi, 1));
                let eq_result = eq(search, &scalar)?;
                if eq_result.true_count() > 0 {
                    found_overlap = true;
                    break;
                }
                if eq_result.null_count() > 0 {
                    has_null_eq = true;
                }
            }
        }

        if found_overlap {
            builder.append_value(true);
        } else if has_null_eq || left_values.null_count() > 0 || right_values.null_count() > 0 {
            builder.append_null();
        } else {
            builder.append_value(false);
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// SQL three-valued equality for array elements at given indices.
/// Returns Some(true) if equal, Some(false) if not equal, None if indeterminate (null).
fn three_valued_eq(left: &dyn Array, li: usize, right: &dyn Array, ri: usize) -> Result<Option<bool>> {
    if left.is_null(li) || right.is_null(ri) {
        return Ok(None);
    }

    match left.data_type() {
        DataType::List(_) => {
            let ll = left.as_any().downcast_ref::<GenericListArray<i32>>().unwrap();
            let rl = right.as_any().downcast_ref::<GenericListArray<i32>>().unwrap();
            list_values_eq(&ll.value(li), &rl.value(ri))
        }
        DataType::LargeList(_) => {
            let ll = left.as_any().downcast_ref::<GenericListArray<i64>>().unwrap();
            let rl = right.as_any().downcast_ref::<GenericListArray<i64>>().unwrap();
            list_values_eq(&ll.value(li), &rl.value(ri))
        }
        _ => {
            let l = Scalar::new(left.slice(li, 1));
            let r = Scalar::new(right.slice(ri, 1));
            let result = eq(&l, &r)?;
            Ok(if result.is_null(0) {
                None
            } else {
                Some(result.value(0))
            })
        }
    }
}

/// Three-valued element-wise equality for two array values.
fn list_values_eq(left: &ArrayRef, right: &ArrayRef) -> Result<Option<bool>> {
    if left.len() != right.len() {
        return Ok(Some(false));
    }
    let mut has_null = false;
    for k in 0..left.len() {
        match three_valued_eq(left.as_ref(), k, right.as_ref(), k)? {
            Some(false) => return Ok(Some(false)),
            None => has_null = true,
            Some(true) => {}
        }
    }
    Ok(if has_null { None } else { Some(true) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;

    fn make_list_array(
        values: &Int32Array,
        offsets: &[i32],
        nulls: Option<NullBuffer>,
    ) -> ListArray {
        ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(offsets.to_vec().into()),
            Arc::new(values.clone()),
            nulls,
        )
    }

    #[test]
    fn test_basic_overlap() -> Result<()> {
        // [1, 2, 3] vs [3, 4, 5] => true
        let left = make_list_array(&Int32Array::from(vec![1, 2, 3]), &[0, 3], None);
        let right = make_list_array(&Int32Array::from(vec![3, 4, 5]), &[0, 3], None);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.value(0));
        assert!(result.is_valid(0));
        Ok(())
    }

    #[test]
    fn test_no_overlap() -> Result<()> {
        // [1, 2] vs [3, 4] => false
        let left = make_list_array(&Int32Array::from(vec![1, 2]), &[0, 2], None);
        let right = make_list_array(&Int32Array::from(vec![3, 4]), &[0, 2], None);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!result.value(0));
        assert!(result.is_valid(0));
        Ok(())
    }

    #[test]
    fn test_null_only_overlap() -> Result<()> {
        // [1, NULL] vs [NULL, 2] => null (no definite overlap, but nulls present)
        let left = make_list_array(&Int32Array::from(vec![Some(1), None]), &[0, 2], None);
        let right = make_list_array(&Int32Array::from(vec![None, Some(2)]), &[0, 2], None);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_null(0));
        Ok(())
    }

    #[test]
    fn test_null_with_overlap() -> Result<()> {
        // [1, NULL] vs [1, 2] => true (definite overlap on 1)
        let left = make_list_array(&Int32Array::from(vec![Some(1), None]), &[0, 2], None);
        let right = make_list_array(&Int32Array::from(vec![1, 2]), &[0, 2], None);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.value(0));
        assert!(result.is_valid(0));
        Ok(())
    }

    #[test]
    fn test_empty_array() -> Result<()> {
        // [1, NULL, 3] vs [] => false
        let left = make_list_array(
            &Int32Array::from(vec![Some(1), None, Some(3)]),
            &[0, 3],
            None,
        );
        let right = make_list_array(&Int32Array::from(Vec::<i32>::new()), &[0, 0], None);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!result.value(0));
        assert!(result.is_valid(0));
        Ok(())
    }

    #[test]
    fn test_null_array() -> Result<()> {
        // NULL vs [1, 2] => null
        let left = make_list_array(
            &Int32Array::from(Vec::<i32>::new()),
            &[0, 0],
            Some(NullBuffer::from(vec![false])),
        );
        let right = make_list_array(&Int32Array::from(vec![1, 2]), &[0, 2], None);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_null(0));
        Ok(())
    }

    #[test]
    fn test_both_null_elements() -> Result<()> {
        // [NULL] vs [NULL] => null
        let left = make_list_array(&Int32Array::from(vec![None::<i32>]), &[0, 1], None);
        let right = make_list_array(&Int32Array::from(vec![None::<i32>]), &[0, 1], None);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_null(0));
        Ok(())
    }

    #[test]
    fn test_both_null_elements_via_null_array() -> Result<()> {
        // Simulate what DataFusion's make_array(NULL) produces: List<Null> with NullArray values
        use arrow::array::NullArray;

        let null_values = Arc::new(NullArray::new(1)) as ArrayRef;
        let null_field = Arc::new(Field::new("item", DataType::Null, true));
        let left = ListArray::new(
            Arc::clone(&null_field),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::clone(&null_values),
            None,
        );
        let right = ListArray::new(
            null_field,
            OffsetBuffer::new(vec![0, 1].into()),
            null_values,
            None,
        );

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(
            result.is_null(0),
            "Expected null for [NULL] vs [NULL] (NullArray representation), got {:?}",
            result
        );
        Ok(())
    }

    #[test]
    fn test_one_null_element_no_overlap() -> Result<()> {
        // [3, NULL] vs [1, 2] => null
        let left = make_list_array(&Int32Array::from(vec![Some(3), None]), &[0, 2], None);
        let right = make_list_array(&Int32Array::from(vec![1, 2]), &[0, 2], None);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_null(0));
        Ok(())
    }

    /// Build a single-row ListArray of nested lists: List<List<Int32>>
    fn make_nested_list(elements: Vec<Option<Vec<Option<i32>>>>) -> ListArray {
        use arrow::array::{Int32Builder, ListBuilder};

        let inner_builder = ListBuilder::new(Int32Builder::new());
        let mut outer_builder = ListBuilder::new(inner_builder);

        for elem in &elements {
            match elem {
                Some(inner) => {
                    let inner_list_builder = outer_builder.values();
                    for val in inner {
                        match val {
                            Some(v) => inner_list_builder.values().append_value(*v),
                            None => inner_list_builder.values().append_null(),
                        }
                    }
                    inner_list_builder.append(true);
                }
                None => {
                    outer_builder.values().append(false);
                }
            }
        }
        outer_builder.append(true);
        outer_builder.finish()
    }

    #[test]
    fn test_nested_array_basic_overlap() -> Result<()> {
        // [[1,2], [3,4]] vs [[3,4], [5,6]] => true
        let left = make_nested_list(vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3), Some(4)])]);
        let right =
            make_nested_list(vec![Some(vec![Some(3), Some(4)]), Some(vec![Some(5), Some(6)])]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(result.value(0));
        Ok(())
    }

    #[test]
    fn test_nested_array_no_overlap() -> Result<()> {
        // [[1,2]] vs [[3,4]] => false
        let left = make_nested_list(vec![Some(vec![Some(1), Some(2)])]);
        let right = make_nested_list(vec![Some(vec![Some(3), Some(4)])]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(!result.value(0));
        Ok(())
    }

    #[test]
    fn test_nested_array_inner_nulls_indeterminate() -> Result<()> {
        // [[1,NULL]] vs [[1,NULL]] => null (indeterminate due to inner nulls)
        let left = make_nested_list(vec![Some(vec![Some(1), None])]);
        let right = make_nested_list(vec![Some(vec![Some(1), None])]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(
            result.is_null(0),
            "Expected null for [[1,NULL]] vs [[1,NULL]], got {:?}",
            result
        );
        Ok(())
    }

    #[test]
    fn test_nested_array_inner_nulls_no_match() -> Result<()> {
        // [[1,NULL]] vs [[1,2], [3,4]] => null (eq([1,NULL],[1,2]) is null, no definite match)
        let left = make_nested_list(vec![Some(vec![Some(1), None])]);
        let right =
            make_nested_list(vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3), Some(4)])]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(
            result.is_null(0),
            "Expected null for [[1,NULL]] vs [[1,2],[3,4]], got {:?}",
            result
        );
        Ok(())
    }

    #[test]
    fn test_nested_array_definite_match_despite_inner_nulls() -> Result<()> {
        // [[1,2], [1,NULL]] vs [[1,2]] => true (definite match on [1,2])
        let left = make_nested_list(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(1), None]),
        ]);
        let right = make_nested_list(vec![Some(vec![Some(1), Some(2)])]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(result.value(0));
        Ok(())
    }
}
