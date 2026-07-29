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

//! Spark-compatible `arrays_overlap` with correct null handling.
//!
//! DataFusion's `array_has_any` uses `RowConverter` for element comparison, which
//! treats NULL == NULL as true (grouping semantics). For outer-level null elements,
//! Spark's `arrays_overlap` uses three-valued logic: NULL elements are skipped but
//! cause the result to be null if no definite overlap is found. For comparing
//! non-null elements (including nested types), Spark uses structural equality via
//! `ordering.equiv` where NULL == NULL is true.
//!
//! This implementation returns:
//!   - true  if any non-null element appears in both arrays
//!   - null  if no definite overlap but either array contains null elements
//!   - false if no overlap and neither array contains null elements

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, FixedSizeListArray, GenericListArray,
    GenericStringArray, OffsetSizeTrait, PrimitiveArray, Scalar, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal128Type, FieldRef, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use datafusion::common::{exec_err, utils::take_function_args, HashSet, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::hash::Hash;
use std::ops::Range;
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
    let left_values = left.values();
    let right_values = right.values();

    if left_values.data_type() != right_values.data_type() {
        return arrays_overlap_list_generic(left, right);
    }

    // Fast paths for flat element types: probe the flat value buffers directly instead of
    // slicing each row and running an Arrow compare kernel once per probe element.
    macro_rules! flat_fast_path {
        ($l:expr, $r:expr) => {
            return Ok(overlap_rows(left, right, flat_row_overlap($l, $r)))
        };
    }
    macro_rules! primitive_fast_path {
        ($t:ty) => {
            flat_fast_path!(
                left_values.as_primitive::<$t>(),
                right_values.as_primitive::<$t>()
            )
        };
    }

    match left_values.data_type() {
        DataType::Boolean => flat_fast_path!(left_values.as_boolean(), right_values.as_boolean()),
        DataType::Int8 => primitive_fast_path!(Int8Type),
        DataType::Int16 => primitive_fast_path!(Int16Type),
        DataType::Int32 => primitive_fast_path!(Int32Type),
        DataType::Int64 => primitive_fast_path!(Int64Type),
        DataType::UInt8 => primitive_fast_path!(UInt8Type),
        DataType::UInt16 => primitive_fast_path!(UInt16Type),
        DataType::UInt32 => primitive_fast_path!(UInt32Type),
        DataType::UInt64 => primitive_fast_path!(UInt64Type),
        DataType::Float32 => primitive_fast_path!(Float32Type),
        DataType::Float64 => primitive_fast_path!(Float64Type),
        DataType::Date32 => primitive_fast_path!(Date32Type),
        DataType::Date64 => primitive_fast_path!(Date64Type),
        DataType::Decimal128(_, _) => primitive_fast_path!(Decimal128Type),
        DataType::Timestamp(TimeUnit::Second, _) => primitive_fast_path!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive_fast_path!(TimestampMillisecondType)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive_fast_path!(TimestampMicrosecondType)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive_fast_path!(TimestampNanosecondType)
        }
        DataType::Utf8 => flat_fast_path!(
            left_values.as_string::<i32>(),
            right_values.as_string::<i32>()
        ),
        DataType::LargeUtf8 => flat_fast_path!(
            left_values.as_string::<i64>(),
            right_values.as_string::<i64>()
        ),
        _ => arrays_overlap_list_generic(left, right),
    }
}

/// Drives the row loop for the flat fast paths. `row_overlap` reports whether the two element
/// ranges share a non-null value; null bookkeeping is identical to the generic path: when there
/// is no definite overlap, the row is null if either side holds a null element.
fn overlap_rows<OffsetSize: OffsetSizeTrait>(
    left: &GenericListArray<OffsetSize>,
    right: &GenericListArray<OffsetSize>,
    mut row_overlap: impl FnMut(Range<usize>, Range<usize>) -> bool,
) -> ArrayRef {
    let len = left.len();
    let left_offsets = left.offsets();
    let right_offsets = right.offsets();
    let left_element_nulls = left.values().nulls();
    let right_element_nulls = right.values().nulls();

    let mut builder = BooleanArray::builder(len);

    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
            continue;
        }

        let left_range = left_offsets[i].as_usize()..left_offsets[i + 1].as_usize();
        let right_range = right_offsets[i].as_usize()..right_offsets[i + 1].as_usize();

        if left_range.is_empty() || right_range.is_empty() {
            builder.append_value(false);
        } else if row_overlap(left_range.clone(), right_range.clone()) {
            builder.append_value(true);
        } else if range_has_null(left_element_nulls, left_range)
            || range_has_null(right_element_nulls, right_range)
        {
            builder.append_null();
        } else {
            builder.append_value(false);
        }
    }

    Arc::new(builder.finish())
}

/// True if the validity bitmap marks any element in `range` as null. Slicing counts the bitmap a
/// word at a time rather than testing each element.
fn range_has_null(nulls: Option<&NullBuffer>, range: Range<usize>) -> bool {
    nulls.is_some_and(|n| n.null_count() > 0 && n.slice(range.start, range.len()).null_count() > 0)
}

/// Projects a native value onto a hashable key whose equality matches the Arrow compare kernels:
/// the value itself for integral types, the bit pattern for floats. Arrow orders floats by total
/// order rather than IEEE semantics (NaN equals NaN, and 0.0 does not equal -0.0), which is
/// exactly bit equality.
trait OverlapKey: Copy {
    type Key: Hash + Eq + Copy;

    fn overlap_key(self) -> Self::Key;
}

macro_rules! identity_overlap_key {
    ($($t:ty),*) => {
        $(impl OverlapKey for $t {
            type Key = $t;

            fn overlap_key(self) -> $t {
                self
            }
        })*
    };
}
identity_overlap_key!(i8, i16, i32, i64, i128, u8, u16, u32, u64);

impl OverlapKey for f32 {
    type Key = u32;

    fn overlap_key(self) -> u32 {
        self.to_bits()
    }
}

impl OverlapKey for f64 {
    type Key = u64;

    fn overlap_key(self) -> u64 {
        self.to_bits()
    }
}

/// A flat element array whose value at an index reduces to a hashable key, or `None` when the
/// element is null and so can never take part in an overlap.
trait KeyedValues<'a> {
    type Key: Hash + Eq + Copy;

    fn key_at(&self, i: usize) -> Option<Self::Key>;
}

impl<'a, T: ArrowPrimitiveType> KeyedValues<'a> for &'a PrimitiveArray<T>
where
    T::Native: OverlapKey,
{
    type Key = <T::Native as OverlapKey>::Key;

    fn key_at(&self, i: usize) -> Option<Self::Key> {
        (!self.is_null(i)).then(|| self.value(i).overlap_key())
    }
}

impl<'a> KeyedValues<'a> for &'a BooleanArray {
    type Key = bool;

    fn key_at(&self, i: usize) -> Option<bool> {
        (!self.is_null(i)).then(|| self.value(i))
    }
}

impl<'a, S: OffsetSizeTrait> KeyedValues<'a> for &'a GenericStringArray<S> {
    type Key = &'a str;

    fn key_at(&self, i: usize) -> Option<&'a str> {
        let values: &'a GenericStringArray<S> = self;
        (!values.is_null(i)).then(|| values.value(i))
    }
}

/// Above this many pairwise comparisons a hash probe beats the nested scan.
const NESTED_SCAN_BUDGET: usize = 256;

/// Row overlap for flat element types. The scratch set is reused across rows.
fn flat_row_overlap<'a, V>(left: V, right: V) -> impl FnMut(Range<usize>, Range<usize>) -> bool + 'a
where
    V: KeyedValues<'a> + Copy + 'a,
{
    let mut seen: HashSet<V::Key> = HashSet::new();

    move |left_range, right_range| {
        // Probe with the smaller side so the inner loop, and the hash table, stay small.
        let (probe, probe_range, search, search_range) = if left_range.len() <= right_range.len() {
            (left, left_range, right, right_range)
        } else {
            (right, right_range, left, left_range)
        };

        if probe_range.len() * search_range.len() <= NESTED_SCAN_BUDGET {
            for pi in probe_range {
                let Some(key) = probe.key_at(pi) else {
                    continue;
                };
                for si in search_range.clone() {
                    if search.key_at(si) == Some(key) {
                        return true;
                    }
                }
            }
            return false;
        }

        seen.clear();
        seen.reserve(probe_range.len());
        seen.extend(probe_range.filter_map(|pi| probe.key_at(pi)));
        search_range
            .into_iter()
            .any(|si| search.key_at(si).is_some_and(|key| seen.contains(&key)))
    }
}

/// Fallback for nested and otherwise unhandled element types.
fn arrays_overlap_list_generic<OffsetSize: OffsetSizeTrait>(
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

        if left_values.is_empty() || right_values.is_empty() {
            builder.append_value(false);
            continue;
        }

        // DataFusion's make_array(NULL) produces a List<Null> with NullArray values.
        if left_values.data_type() == &DataType::Null || right_values.data_type() == &DataType::Null
        {
            builder.append_null();
            continue;
        }

        let mut found_overlap = false;
        let mut has_null = false;

        // Put smaller array on the probe side: fewer find_in_array calls means
        // fewer kernel dispatches and allocations in the flat vectorized path.
        let (probe, search) = if left_values.len() <= right_values.len() {
            (&left_values, &right_values)
        } else {
            (&right_values, &left_values)
        };

        // Check element type once outside the loop.
        let use_vectorized = !needs_recursive_eq(probe.data_type());

        for pi in 0..probe.len() {
            if probe.is_null(pi) {
                has_null = true;
                continue;
            }
            let (found, null_eq) = if use_vectorized {
                find_in_array_flat(probe, pi, search)?
            } else {
                find_in_array_nested(probe, pi, search)?
            };
            if null_eq {
                has_null = true;
            }
            if found {
                found_overlap = true;
                break;
            }
        }

        if found_overlap {
            builder.append_value(true);
        } else if has_null {
            builder.append_null();
        } else {
            builder.append_value(false);
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Vectorized search using Arrow's `eq` kernel. One SIMD call per probe element.
fn find_in_array_flat(probe: &ArrayRef, pi: usize, search: &ArrayRef) -> Result<(bool, bool)> {
    let scalar = Scalar::new(probe.slice(pi, 1));
    let eq_result = eq(search, &scalar)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok((eq_result.true_count() > 0, eq_result.null_count() > 0))
}

/// Element-by-element search using structural equality for nested types.
fn find_in_array_nested(probe: &ArrayRef, pi: usize, search: &ArrayRef) -> Result<(bool, bool)> {
    let mut has_null = false;
    for si in 0..search.len() {
        if search.is_null(si) {
            has_null = true;
            continue;
        }
        if structural_eq(probe.as_ref(), pi, search.as_ref(), si)? {
            return Ok((true, has_null));
        }
    }
    Ok((false, has_null))
}

fn needs_recursive_eq(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
    )
}

/// Structural equality for array elements (grouping semantics: NULL == NULL is true).
/// This matches Spark's `ordering.equiv` used inside `arrays_overlap`.
/// Three-valued null logic only applies to outer-level null elements (handled by the caller).
fn structural_eq(left: &dyn Array, li: usize, right: &dyn Array, ri: usize) -> Result<bool> {
    // NullArray::is_null() returns false (no null buffer), so check data type first.
    if left.data_type() == &DataType::Null && right.data_type() == &DataType::Null {
        return Ok(true);
    }

    if left.is_null(li) && right.is_null(ri) {
        return Ok(true);
    }
    if left.is_null(li) || right.is_null(ri) {
        return Ok(false);
    }

    match left.data_type() {
        DataType::List(_) => {
            let ll = left
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .unwrap();
            let rl = right
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .unwrap();
            list_structural_eq(&ll.value(li), &rl.value(ri))
        }
        DataType::LargeList(_) => {
            let ll = left
                .as_any()
                .downcast_ref::<GenericListArray<i64>>()
                .unwrap();
            let rl = right
                .as_any()
                .downcast_ref::<GenericListArray<i64>>()
                .unwrap();
            list_structural_eq(&ll.value(li), &rl.value(ri))
        }
        DataType::FixedSizeList(_, _) => {
            let ll = left.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let rl = right.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            list_structural_eq(&ll.value(li), &rl.value(ri))
        }
        DataType::Struct(_) => {
            let ls = left.as_any().downcast_ref::<StructArray>().unwrap();
            let rs = right.as_any().downcast_ref::<StructArray>().unwrap();
            struct_structural_eq(ls, li, rs, ri)
        }
        _ => {
            // Both non-null at this point; eq on two non-null scalars is definitive.
            let l = Scalar::new(left.slice(li, 1));
            let r = Scalar::new(right.slice(ri, 1));
            let result = eq(&l, &r)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            Ok(result.value(0))
        }
    }
}

fn list_structural_eq(left: &ArrayRef, right: &ArrayRef) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for k in 0..left.len() {
        if !structural_eq(left.as_ref(), k, right.as_ref(), k)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn struct_structural_eq(
    left: &StructArray,
    li: usize,
    right: &StructArray,
    ri: usize,
) -> Result<bool> {
    for (lc, rc) in left.columns().iter().zip(right.columns().iter()) {
        if !structural_eq(lc.as_ref(), li, rc.as_ref(), ri)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int32Builder, ListArray, ListBuilder, StructBuilder};
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
        let left = make_nested_list(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
        ]);
        let right = make_nested_list(vec![
            Some(vec![Some(3), Some(4)]),
            Some(vec![Some(5), Some(6)]),
        ]);

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
    fn test_nested_array_inner_nulls_match() -> Result<()> {
        // [[1,NULL]] vs [[1,NULL]] => true (structural equality: NULL == NULL)
        let left = make_nested_list(vec![Some(vec![Some(1), None])]);
        let right = make_nested_list(vec![Some(vec![Some(1), None])]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(result.value(0));
        Ok(())
    }

    #[test]
    fn test_nested_array_inner_nulls_no_match() -> Result<()> {
        // [[1,NULL]] vs [[1,2], [3,4]] => false (structural: [1,NULL] != [1,2], [1,NULL] != [3,4])
        let left = make_nested_list(vec![Some(vec![Some(1), None])]);
        let right = make_nested_list(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
        ]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(!result.value(0));
        Ok(())
    }

    #[test]
    fn test_nested_array_all_null_elements_match() -> Result<()> {
        // [[NULL]] vs [[NULL]] => true (structural equality: [NULL] == [NULL])
        let left = make_nested_list(vec![Some(vec![None])]);
        let right = make_nested_list(vec![Some(vec![None])]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(
            result.is_valid(0),
            "Expected true for [[NULL]] vs [[NULL]], got null"
        );
        assert!(
            result.value(0),
            "Expected true for [[NULL]] vs [[NULL]], got false"
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

    /// Build a single-row ListArray of structs: List<Struct<a: Int32, b: Int32>>
    fn make_struct_list(elements: Vec<Option<(Option<i32>, Option<i32>)>>) -> ListArray {
        let fields = vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ];
        let struct_builder = StructBuilder::new(
            fields.clone(),
            vec![Box::new(Int32Builder::new()), Box::new(Int32Builder::new())],
        );
        let mut list_builder = ListBuilder::new(struct_builder);

        for elem in &elements {
            let sb = list_builder.values();
            match elem {
                Some((a, b)) => {
                    sb.field_builder::<Int32Builder>(0)
                        .unwrap()
                        .append_option(*a);
                    sb.field_builder::<Int32Builder>(1)
                        .unwrap()
                        .append_option(*b);
                    sb.append(true);
                }
                None => {
                    sb.field_builder::<Int32Builder>(0).unwrap().append_null();
                    sb.field_builder::<Int32Builder>(1).unwrap().append_null();
                    sb.append(false);
                }
            }
        }
        list_builder.append(true);
        list_builder.finish()
    }

    #[test]
    fn test_struct_basic_overlap() -> Result<()> {
        // [{1,2}, {3,4}] vs [{3,4}, {5,6}] => true
        let left = make_struct_list(vec![Some((Some(1), Some(2))), Some((Some(3), Some(4)))]);
        let right = make_struct_list(vec![Some((Some(3), Some(4))), Some((Some(5), Some(6)))]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(result.value(0));
        Ok(())
    }

    #[test]
    fn test_struct_no_overlap() -> Result<()> {
        // [{1,2}] vs [{3,4}] => false
        let left = make_struct_list(vec![Some((Some(1), Some(2)))]);
        let right = make_struct_list(vec![Some((Some(3), Some(4)))]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(!result.value(0));
        Ok(())
    }

    #[test]
    fn test_struct_with_null_field_match() -> Result<()> {
        // [{1,NULL}] vs [{1,NULL}] => true (structural equality: NULL == NULL)
        let left = make_struct_list(vec![Some((Some(1), None))]);
        let right = make_struct_list(vec![Some((Some(1), None))]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(result.value(0));
        Ok(())
    }

    #[test]
    fn test_struct_definite_match_with_null_field() -> Result<()> {
        // [{1,2}, {1,NULL}] vs [{1,2}] => true (definite match on {1,2})
        let left = make_struct_list(vec![Some((Some(1), Some(2))), Some((Some(1), None))]);
        let right = make_struct_list(vec![Some((Some(1), Some(2)))]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(result.value(0));
        Ok(())
    }

    #[test]
    fn test_struct_null_element() -> Result<()> {
        // [NULL] vs [{1,2}] => null (null outer element)
        let left = make_struct_list(vec![None]);
        let right = make_struct_list(vec![Some((Some(1), Some(2)))]);

        let result = arrays_overlap_list::<i32>(&left, &right)?;
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_null(0));
        Ok(())
    }
}
