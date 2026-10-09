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

use super::{compare_floats, has_float_leaf};
use arrow::array::{make_comparator, Array, AsArray, DynComparator, OffsetSizeTrait};
use arrow::buffer::NullBuffer;
use arrow::compute::SortOptions;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use datafusion::common::{internal_err, DFSchema, Result};
use num::Float;
use std::cmp::Ordering;
use std::ops::Range;

/// Builds a comparator of `left[i]` against `right[j]` in Spark's SQL ordering, in which floats
/// compare as [`compare_floats`] does at any depth. Inner nulls sort first, lists compare element
/// by element and then by length, and structs compare field by field.
///
/// Subtrees without a float leaf use Arrow's comparator, which orders them the same way. The two
/// arrays must have the same type, ignoring field names and nullability.
pub fn spark_comparator(left: &dyn Array, right: &dyn Array) -> Result<DynComparator> {
    check_types(left, right)?;
    comparator(left, right, false, true)
}

/// Builds a test of whether `left[i]` equals `right[j]` in the ordering of [`spark_comparator`].
/// Lists of different lengths are unequal without comparing their elements.
pub fn spark_equality(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    check_types(left, right)?;
    let compare = comparator(left, right, true, true)?;
    Ok(Box::new(move |i, j| compare(i, j).is_eq()))
}

/// [`spark_comparator`] for a caller that applies the top-level nulls of `left` and `right`
/// itself, as SQL comparisons do. A null list or struct is compared by the values under it instead
/// of sorting first, which saves testing both null buffers on every call. Nulls inside a list or
/// struct still sort first.
pub(crate) fn spark_comparator_ignoring_nulls(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<DynComparator> {
    check_types(left, right)?;
    comparator(left, right, false, false)
}

/// [`spark_equality`] for a caller that applies the top-level nulls itself. See
/// [`spark_comparator_ignoring_nulls`].
pub(crate) fn spark_equality_ignoring_nulls(
    left: &dyn Array,
    right: &dyn Array,
) -> Result<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    check_types(left, right)?;
    let compare = comparator(left, right, true, false)?;
    Ok(Box::new(move |i, j| compare(i, j).is_eq()))
}

fn check_types(left: &dyn Array, right: &dyn Array) -> Result<()> {
    if DFSchema::datatype_is_logically_equal(left.data_type(), right.data_type()) {
        Ok(())
    } else {
        internal_err!(
            "Spark comparison requires matching types, got {} and {}",
            left.data_type(),
            right.data_type()
        )
    }
}

/// With `equality` set, the comparator only has to tell equal from unequal values. With `nulls`
/// unset, it ignores the nulls of `left` and `right` themselves, though not those of their
/// children.
fn comparator(
    left: &dyn Array,
    right: &dyn Array,
    equality: bool,
    nulls: bool,
) -> Result<DynComparator> {
    if !has_float_leaf(left.data_type()) {
        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };
        return Ok(make_comparator(left, right, options)?);
    }
    match (left.data_type(), right.data_type()) {
        (DataType::Float32, DataType::Float32) => {
            Ok(float_comparator::<Float32Type>(left, right, nulls))
        }
        (DataType::Float64, DataType::Float64) => {
            Ok(float_comparator::<Float64Type>(left, right, nulls))
        }
        (DataType::List(_), DataType::List(_)) => {
            list_comparator::<i32>(left, right, equality, nulls)
        }
        (DataType::LargeList(_), DataType::LargeList(_)) => {
            list_comparator::<i64>(left, right, equality, nulls)
        }
        (DataType::FixedSizeList(_, _), DataType::FixedSizeList(_, _)) => {
            fixed_size_list_comparator(left, right, equality, nulls)
        }
        (DataType::Struct(_), DataType::Struct(_)) => {
            struct_comparator(left, right, equality, nulls)
        }
        (l, r) => internal_err!("Unsupported types for Spark comparison: {l} and {r}"),
    }
}

fn float_comparator<T: ArrowPrimitiveType>(
    left: &dyn Array,
    right: &dyn Array,
    nulls: bool,
) -> DynComparator
where
    T::Native: Float,
{
    let l = left.as_primitive::<T>().values().clone();
    let r = right.as_primitive::<T>().values().clone();
    nulls_first(left, right, nulls, move |i, j| compare_floats(l[i], r[j]))
}

fn list_comparator<O: OffsetSizeTrait>(
    left: &dyn Array,
    right: &dyn Array,
    equality: bool,
    nulls: bool,
) -> Result<DynComparator> {
    let (l, r) = (left.as_list::<O>(), right.as_list::<O>());
    let compare = comparator(l.values().as_ref(), r.values().as_ref(), equality, true)?;
    let (l, r) = (l.offsets().clone(), r.offsets().clone());
    Ok(nulls_first(left, right, nulls, move |i, j| {
        let left = l[i].as_usize()..l[i + 1].as_usize();
        let right = r[j].as_usize()..r[j + 1].as_usize();
        lexicographic(&compare, left, right, equality)
    }))
}

fn fixed_size_list_comparator(
    left: &dyn Array,
    right: &dyn Array,
    equality: bool,
    nulls: bool,
) -> Result<DynComparator> {
    let (l, r) = (left.as_fixed_size_list(), right.as_fixed_size_list());
    let compare = comparator(l.values().as_ref(), r.values().as_ref(), equality, true)?;
    let (l, r) = (l.value_length() as usize, r.value_length() as usize);
    Ok(nulls_first(left, right, nulls, move |i, j| {
        lexicographic(&compare, i * l..(i + 1) * l, j * r..(j + 1) * r, equality)
    }))
}

/// Compares two runs of child values element by element, then by length. When only equality
/// matters, runs of different lengths are unequal without comparing any elements.
fn lexicographic(
    compare: &DynComparator,
    left: Range<usize>,
    right: Range<usize>,
    equality: bool,
) -> Ordering {
    let lengths = left.len().cmp(&right.len());
    if equality && lengths.is_ne() {
        return lengths;
    }
    left.zip(right)
        .map(|(i, j)| compare(i, j))
        .find(|ordering| ordering.is_ne())
        .unwrap_or(lengths)
}

fn struct_comparator(
    left: &dyn Array,
    right: &dyn Array,
    equality: bool,
    nulls: bool,
) -> Result<DynComparator> {
    let fields = left
        .as_struct()
        .columns()
        .iter()
        .zip(right.as_struct().columns())
        .map(|(l, r)| comparator(l.as_ref(), r.as_ref(), equality, true))
        .collect::<Result<Vec<_>>>()?;
    Ok(nulls_first(left, right, nulls, move |i, j| {
        fields
            .iter()
            .map(|compare| compare(i, j))
            .find(|ordering| ordering.is_ne())
            .unwrap_or(Ordering::Equal)
    }))
}

/// Orders nulls before values, unless `apply` is unset. A null slot then never reaches `compare`,
/// because the child values under a null list or struct can be anything. Without `apply`, the
/// caller masks the result for null slots: `compare` reads the values under them, which are in
/// bounds in any valid array.
fn nulls_first(
    left: &dyn Array,
    right: &dyn Array,
    apply: bool,
    compare: impl Fn(usize, usize) -> Ordering + Send + Sync + 'static,
) -> DynComparator {
    let present = |nulls: &&NullBuffer| apply && nulls.null_count() > 0;
    let left = left.nulls().filter(present).cloned();
    let right = right.nulls().filter(present).cloned();
    if left.is_none() && right.is_none() {
        return Box::new(compare);
    }
    Box::new(move |i, j| {
        let l = left.as_ref().is_some_and(|nulls| nulls.is_null(i));
        let r = right.as_ref().is_some_and(|nulls| nulls.is_null(j));
        match (l, r) {
            (false, false) => compare(i, j),
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::float_semantics::{NEGATIVE_NAN, PAYLOAD_NAN};
    use arrow::array::{
        ArrayRef, DictionaryArray, FixedSizeListArray, Float64Array, Int32Array, LargeListArray,
        ListArray, StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    /// Wraps every value in a one-element list, large list and fixed-size list, and in a struct,
    /// so that each nesting compares the same pairs of leaves.
    fn nestings(values: ArrayRef) -> Vec<ArrayRef> {
        let field = Arc::new(Field::new("item", values.data_type().clone(), true));
        let lengths = || std::iter::repeat_n(1, values.len());
        vec![
            Arc::clone(&values),
            Arc::new(ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths(lengths()),
                Arc::clone(&values),
                None,
            )),
            Arc::new(LargeListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths(lengths()),
                Arc::clone(&values),
                None,
            )),
            Arc::new(FixedSizeListArray::new(
                Arc::clone(&field),
                1,
                Arc::clone(&values),
                None,
            )),
            Arc::new(StructArray::new(
                vec![field].into(),
                vec![Arc::clone(&values)],
                None,
            )),
        ]
    }

    #[test]
    fn floats_compare_as_spark_orders_them_at_every_depth() -> Result<()> {
        let left = vec![
            Some(-0.0),
            Some(NEGATIVE_NAN),
            None,
            Some(1.0),
            Some(f64::INFINITY),
        ];
        let right = vec![
            Some(0.0),
            Some(f64::INFINITY),
            None,
            Some(PAYLOAD_NAN),
            Some(f64::NEG_INFINITY),
            Some(-1.0),
        ];
        let expected = |l: Option<f64>, r: Option<f64>| match (l, r) {
            (Some(l), Some(r)) => compare_floats(l, r),
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
        };
        let nested_left = nestings(Arc::new(Float64Array::from(left.clone())));
        let nested_right = nestings(Arc::new(Float64Array::from(right.clone())));
        for (l_array, r_array) in nested_left.iter().zip(&nested_right) {
            let compare = spark_comparator(l_array.as_ref(), r_array.as_ref())?;
            let equal = spark_equality(l_array.as_ref(), r_array.as_ref())?;
            for (i, &l) in left.iter().enumerate() {
                for (j, &r) in right.iter().enumerate() {
                    let expected = expected(l, r);
                    let context = format!("{l:?} vs {r:?} in {}", l_array.data_type());
                    assert_eq!(compare(i, j), expected, "{context}");
                    assert_eq!(equal(i, j), expected.is_eq(), "{context}");
                }
            }
        }
        Ok(())
    }

    #[test]
    fn lists_compare_elements_then_length_and_skip_null_slots() -> Result<()> {
        let field = Arc::new(Field::new("item", DataType::Float64, true));
        // The last row of each side is a null list over values that differ between the sides.
        let left: ArrayRef = Arc::new(ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths([2, 1, 0, 1]),
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(1.0),
                Some(f64::NAN),
                Some(1.0),
            ])),
            Some(NullBuffer::from(vec![true, true, true, false])),
        ));
        let right: ArrayRef = Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths([1, 2, 1, 1]),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(f64::INFINITY),
                Some(5.0),
                None,
                Some(9.0),
            ])),
            Some(NullBuffer::from(vec![true, true, true, false])),
        ));
        let compare = spark_comparator(left.as_ref(), right.as_ref())?;
        // `[0.0, 1.0]` extends `[-0.0]`, so it is greater.
        assert_eq!(compare(0, 0), Ordering::Greater);
        // `[NaN]` against `[Infinity, 5.0]`: the first element decides before the length.
        assert_eq!(compare(1, 1), Ordering::Greater);
        // `[]` is a prefix of `[null]`.
        assert_eq!(compare(2, 2), Ordering::Less);
        assert_eq!(compare(3, 3), Ordering::Equal);
        assert_eq!(compare(3, 2), Ordering::Less);
        assert_eq!(compare(2, 3), Ordering::Greater);
        let equal = spark_equality(left.as_ref(), right.as_ref())?;
        let pairs = [(0, 0), (1, 1), (2, 2), (3, 3), (3, 2), (2, 3), (0, 1)];
        for (i, j) in pairs {
            assert_eq!(equal(i, j), compare(i, j).is_eq(), "({i}, {j})");
        }
        Ok(())
    }

    /// Ignoring the top-level nulls compares a null list or struct by the values under it, and
    /// leaves every other slot, and nulls further down, as `spark_comparator` orders them.
    #[test]
    fn ignoring_nulls_compares_the_values_under_a_null_slot() -> Result<()> {
        let field = Arc::new(Field::new("item", DataType::Float64, true));
        // The second row of each side is null, over `[-0.0]` and `[1.0]`, and the third holds an
        // inner null.
        let nulls = || Some(NullBuffer::from(vec![true, false, true]));
        let list = |values: Vec<Option<f64>>, nulls: Option<NullBuffer>| -> ArrayRef {
            Arc::new(ListArray::new(
                Arc::clone(&field),
                OffsetBuffer::from_lengths([1, 1, 1]),
                Arc::new(Float64Array::from(values)),
                nulls,
            ))
        };
        let left = vec![Some(0.0), Some(-0.0), None];
        let right = vec![Some(-0.0), Some(1.0), Some(f64::NAN)];
        let struct_of = |values: Vec<Option<f64>>| -> ArrayRef {
            let list = list(values, None);
            Arc::new(StructArray::new(
                vec![Arc::new(Field::new("v", list.data_type().clone(), true))].into(),
                vec![list],
                nulls(),
            ))
        };
        for (l, r) in [
            (list(left.clone(), nulls()), list(right.clone(), nulls())),
            (struct_of(left), struct_of(right)),
        ] {
            let masked = spark_comparator(l.as_ref(), r.as_ref())?;
            let unmasked = spark_comparator_ignoring_nulls(l.as_ref(), r.as_ref())?;
            let equal = spark_equality_ignoring_nulls(l.as_ref(), r.as_ref())?;
            assert_eq!(masked(1, 1), Ordering::Equal);
            assert_eq!(unmasked(1, 1), Ordering::Less, "{}", l.data_type());
            assert!(!equal(1, 1));
            for row in [0, 2] {
                assert_eq!(unmasked(row, row), masked(row, row), "{}", l.data_type());
                assert_eq!(equal(row, row), masked(row, row).is_eq());
            }
        }
        Ok(())
    }

    #[test]
    fn struct_fields_without_floats_break_ties() -> Result<()> {
        let structs = |floats: Vec<f64>, ints: Vec<i32>| -> ArrayRef {
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("f", DataType::Float64, true)),
                    Arc::new(Float64Array::from(floats)) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("i", DataType::Int32, true)),
                    Arc::new(Int32Array::from(ints)) as ArrayRef,
                ),
            ]))
        };
        let left = structs(vec![-0.0, f64::NAN, 1.0], vec![2, 1, 1]);
        let right = structs(vec![0.0, NEGATIVE_NAN, 2.0], vec![1, 1, 0]);
        let compare = spark_comparator(left.as_ref(), right.as_ref())?;
        assert_eq!(compare(0, 0), Ordering::Greater);
        assert_eq!(compare(1, 1), Ordering::Equal);
        assert_eq!(compare(2, 2), Ordering::Less);
        Ok(())
    }

    #[test]
    fn mismatched_types_are_rejected() {
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        // A dictionary passes the logical type check, so it must not reach a downcast either.
        let dictionary: ArrayRef = Arc::new(DictionaryArray::new(
            Int32Array::from(vec![0]),
            Arc::clone(&floats),
        ));
        let list = |values: &ArrayRef| -> ArrayRef {
            let field = Arc::new(Field::new("item", values.data_type().clone(), true));
            Arc::new(ListArray::new(
                field,
                OffsetBuffer::from_lengths([1]),
                Arc::clone(values),
                None,
            ))
        };
        let (float_list, dictionary_list) = (list(&floats), list(&dictionary));
        for (left, right) in [
            (&floats, &ints),
            (&floats, &dictionary),
            (&dictionary, &floats),
            (&float_list, &dictionary_list),
            (&dictionary_list, &float_list),
        ] {
            let types = format!("{} and {}", left.data_type(), right.data_type());
            assert!(
                spark_comparator(left.as_ref(), right.as_ref()).is_err(),
                "{types}"
            );
            assert!(
                spark_equality(left.as_ref(), right.as_ref()).is_err(),
                "{types}"
            );
        }
    }
}
