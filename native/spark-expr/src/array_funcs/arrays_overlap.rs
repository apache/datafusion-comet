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

use arrow::array::{
    Array, ArrayRef, BooleanArray, GenericListArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion::common::ScalarValue;
use datafusion::common::{
    cast::{as_large_list_array, as_list_array},
    DataFusionError, Result as DataFusionResult,
};
use datafusion::logical_expr::ColumnarValue;
use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;

/// Spark-compatible arrays_overlap.
///
/// Semantics (matching Spark):
/// - If either input array is null -> null
/// - If the arrays share a common **non-null** element -> true
/// - If no common non-null elements exist, but either array contains a null element -> null
/// - Otherwise -> false
pub fn spark_arrays_overlap(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(
            "arrays_overlap requires exactly 2 arguments".to_string(),
        ));
    }

    let len = match (&args[0], &args[1]) {
        (ColumnarValue::Array(a), _) => a.len(),
        (_, ColumnarValue::Array(a)) => a.len(),
        (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => 1,
    };

    let left = args[0].clone().into_array(len)?;
    let right = args[1].clone().into_array(len)?;

    let result = match left.data_type() {
        DataType::List(_) => {
            let left_list = as_list_array(&left)?;
            let right_list = as_list_array(&right)?;
            arrays_overlap_inner(left_list, right_list)?
        }
        DataType::LargeList(_) => {
            let left_list = as_large_list_array(&left)?;
            let right_list = as_large_list_array(&right)?;
            arrays_overlap_inner(left_list, right_list)?
        }
        dt => {
            return Err(DataFusionError::Internal(format!(
                "arrays_overlap expected List or LargeList, got {dt:?}"
            )))
        }
    };

    Ok(ColumnarValue::Array(Arc::new(result)))
}

/// Check overlap for a single row using a pre-populated set and a probe function.
/// This unifies the three-valued null logic across all type specializations.
fn check_overlap<V: Eq + Hash>(
    set: &HashSet<V>,
    left_has_null: bool,
    right_values_iter: impl Iterator<Item = Option<V>>,
) -> Option<bool> {
    let mut right_has_null = false;
    for item in right_values_iter {
        match item {
            None => right_has_null = true,
            Some(v) => {
                if set.contains(&v) {
                    return Some(true);
                }
            }
        }
    }
    if left_has_null || right_has_null {
        None
    } else {
        Some(false)
    }
}

fn arrays_overlap_inner<O: OffsetSizeTrait>(
    left: &GenericListArray<O>,
    right: &GenericListArray<O>,
) -> DataFusionResult<BooleanArray> {
    let len = left.len();
    let left_values = left.values();
    let right_values = right.values();
    let element_type = left_values.data_type();

    let mut builder = BooleanArray::builder(len);

    // Dispatch on element type once per batch, then loop over rows inside
    // each specialization. This avoids per-row type matching and downcasting.
    // Float types are excluded because f32/f64 do not implement Eq + Hash;
    // they fall through to the ScalarValue path which uses total ordering.
    macro_rules! dispatch_and_loop {
        (primitive: $($dt:ident => $at:ty),+ $(,)?) => {
            match element_type {
                $(DataType::$dt => {
                    let left_arr = left_values.as_any().downcast_ref::<PrimitiveArray<$at>>().unwrap();
                    let right_arr = right_values.as_any().downcast_ref::<PrimitiveArray<$at>>().unwrap();
                    let mut set = HashSet::new();
                    for i in 0..len {
                        if let Some((l_range, r_range)) = row_ranges(left, right, i) {
                            set.clear();
                            let left_has_null = populate_set_primitive(left_arr, l_range, &mut set);
                            let result = check_overlap(&set, left_has_null,
                                r_range.map(|idx| if right_arr.is_null(idx) { None } else { Some(right_arr.value(idx)) }));
                            match result {
                                Some(v) => builder.append_value(v),
                                None => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                })+
                DataType::Utf8 => {
                    let left_arr = left_values.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                    let right_arr = right_values.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                    overlap_loop_string(left, right, left_arr, right_arr, &mut builder, len);
                }
                DataType::LargeUtf8 => {
                    let left_arr = left_values.as_any().downcast_ref::<arrow::array::LargeStringArray>().unwrap();
                    let right_arr = right_values.as_any().downcast_ref::<arrow::array::LargeStringArray>().unwrap();
                    overlap_loop_string(left, right, left_arr, right_arr, &mut builder, len);
                }
                _ => {
                    overlap_loop_scalar(left, right, left_values, right_values, &mut builder, len)?;
                }
            }
        };
    }

    dispatch_and_loop! {
        primitive:
        Int8 => arrow::datatypes::Int8Type,
        Int16 => arrow::datatypes::Int16Type,
        Int32 => arrow::datatypes::Int32Type,
        Int64 => arrow::datatypes::Int64Type,
        UInt8 => arrow::datatypes::UInt8Type,
        UInt16 => arrow::datatypes::UInt16Type,
        UInt32 => arrow::datatypes::UInt32Type,
        UInt64 => arrow::datatypes::UInt64Type,
        Date32 => arrow::datatypes::Date32Type,
        Date64 => arrow::datatypes::Date64Type,
    }

    Ok(builder.finish())
}

/// Returns the left and right element ranges for row `i`, or None if either list is null.
fn row_ranges<O: OffsetSizeTrait>(
    left: &GenericListArray<O>,
    right: &GenericListArray<O>,
    i: usize,
) -> Option<(Range<usize>, Range<usize>)> {
    if left.is_null(i) || right.is_null(i) {
        return None;
    }
    let l_start = left.value_offsets()[i].as_usize();
    let l_end = left.value_offsets()[i + 1].as_usize();
    let r_start = right.value_offsets()[i].as_usize();
    let r_end = right.value_offsets()[i + 1].as_usize();
    Some((l_start..l_end, r_start..r_end))
}

/// Populate a HashSet from the non-null primitive values in the given range.
/// Returns whether any null was encountered.
fn populate_set_primitive<T: ArrowPrimitiveType>(
    arr: &PrimitiveArray<T>,
    range: Range<usize>,
    set: &mut HashSet<T::Native>,
) -> bool
where
    T::Native: Eq + Hash,
{
    let mut has_null = false;
    for idx in range {
        if arr.is_null(idx) {
            has_null = true;
        } else {
            set.insert(arr.value(idx));
        }
    }
    has_null
}

/// String-typed overlap loop (handles both Utf8 and LargeUtf8 via GenericStringArray).
fn overlap_loop_string<O: OffsetSizeTrait, S: OffsetSizeTrait>(
    left: &GenericListArray<O>,
    right: &GenericListArray<O>,
    left_arr: &arrow::array::GenericStringArray<S>,
    right_arr: &arrow::array::GenericStringArray<S>,
    builder: &mut arrow::array::BooleanBuilder,
    len: usize,
) {
    let mut set: HashSet<&str> = HashSet::new();
    for i in 0..len {
        if let Some((l_range, r_range)) = row_ranges(left, right, i) {
            set.clear();
            let mut left_has_null = false;
            for idx in l_range {
                if left_arr.is_null(idx) {
                    left_has_null = true;
                } else {
                    set.insert(left_arr.value(idx));
                }
            }
            let result = check_overlap(
                &set,
                left_has_null,
                r_range.map(|idx| {
                    if right_arr.is_null(idx) {
                        None
                    } else {
                        Some(right_arr.value(idx))
                    }
                }),
            );
            match result {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        } else {
            builder.append_null();
        }
    }
}

/// Fallback loop using ScalarValue for types without specialized implementations.
fn overlap_loop_scalar<O: OffsetSizeTrait>(
    left: &GenericListArray<O>,
    right: &GenericListArray<O>,
    left_values: &ArrayRef,
    right_values: &ArrayRef,
    builder: &mut arrow::array::BooleanBuilder,
    len: usize,
) -> DataFusionResult<()> {
    let mut set: HashSet<ScalarValue> = HashSet::new();
    for i in 0..len {
        if let Some((l_range, r_range)) = row_ranges(left, right, i) {
            set.clear();
            let mut left_has_null = false;
            for idx in l_range {
                if left_values.is_null(idx) {
                    left_has_null = true;
                } else {
                    set.insert(ScalarValue::try_from_array(left_values.as_ref(), idx)?);
                }
            }
            let mut right_has_null = false;
            let mut found = false;
            for idx in r_range {
                if right_values.is_null(idx) {
                    right_has_null = true;
                } else {
                    let sv = ScalarValue::try_from_array(right_values.as_ref(), idx)?;
                    if set.contains(&sv) {
                        found = true;
                        break;
                    }
                }
            }
            if found {
                builder.append_value(true);
            } else if left_has_null || right_has_null {
                builder.append_null();
            } else {
                builder.append_value(false);
            }
        } else {
            builder.append_null();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ListArray;
    use arrow::datatypes::Int32Type;
    use datafusion::common::Result;

    #[test]
    fn test_no_overlap() -> Result<()> {
        let left = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
        ])]);
        let right = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(3),
            Some(4),
        ])]);
        let result = arrays_overlap_inner(&left, &right)?;
        assert!(!result.value(0));
        assert!(!result.is_null(0));
        Ok(())
    }

    #[test]
    fn test_overlap() -> Result<()> {
        let left = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
        ])]);
        let right = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(3),
            Some(4),
            Some(5),
        ])]);
        let result = arrays_overlap_inner(&left, &right)?;
        assert!(result.value(0));
        assert!(!result.is_null(0));
        Ok(())
    }

    #[test]
    fn test_null_only_overlap_returns_null() -> Result<()> {
        // array(1, NULL) vs array(NULL, 2): no common non-null, but both have nulls -> null
        let left = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            None,
        ])]);
        let right = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            None,
            Some(2),
        ])]);
        let result = arrays_overlap_inner(&left, &right)?;
        assert!(result.is_null(0));
        Ok(())
    }

    #[test]
    fn test_null_array_returns_null() -> Result<()> {
        let left =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![None::<Vec<Option<i32>>>]);
        let right = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1)])]);
        let result = arrays_overlap_inner(&left, &right)?;
        assert!(result.is_null(0));
        Ok(())
    }

    #[test]
    fn test_empty_array() -> Result<()> {
        let left = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![])]);
        let right = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1)])]);
        let result = arrays_overlap_inner(&left, &right)?;
        assert!(!result.value(0));
        assert!(!result.is_null(0));
        Ok(())
    }

    #[test]
    fn test_full_scenario() -> Result<()> {
        // Matches the issue: 5 rows
        let left = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]), // overlap with right -> true
            Some(vec![Some(1), Some(2)]),           // no overlap -> false
            Some(vec![]),                           // empty -> false
            None,                                  // null array -> null
            Some(vec![Some(1), None]),              // null but no overlap -> null
        ]);
        let right = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(3), Some(4), Some(5)]),
            Some(vec![Some(3), Some(4)]),
            Some(vec![Some(1)]),
            Some(vec![Some(1)]),
            Some(vec![None, Some(2)]),
        ]);
        let result = arrays_overlap_inner(&left, &right)?;

        assert!(result.value(0));
        assert!(!result.is_null(0));

        assert!(!result.value(1));
        assert!(!result.is_null(1));

        assert!(!result.value(2));
        assert!(!result.is_null(2));

        assert!(result.is_null(3));

        assert!(result.is_null(4));

        Ok(())
    }
}
