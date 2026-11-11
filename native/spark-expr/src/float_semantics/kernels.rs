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

//! Comparison kernels for Float32 and Float64 arrays in Spark's SQL ordering, which read the
//! operands' value buffers as they are instead of normalizing copies of them first.
//!
//! Each operator is one test over a pair of values, written with `&` and `|` rather than `&&` and
//! `||` so that it stays free of branches, and the loops pack 64 results into a word at a time,
//! which the compiler vectorizes.

use arrow::array::{Array, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, MutableBuffer, NullBuffer};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use arrow::util::bit_util;
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::logical_expr::Operator;
use num::Float;

/// `left op right` for two Float32 or two Float64 arrays of the same length, where `op` is one of
/// `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS DISTINCT FROM` and `IS NOT DISTINCT FROM`. Returns `None`
/// for any other pair of types, which the caller compares another way.
pub(crate) fn compare_float_arrays(
    op: Operator,
    left: &dyn Array,
    right: &dyn Array,
) -> Result<Option<BooleanArray>> {
    match (left.data_type(), right.data_type()) {
        (DataType::Float32, DataType::Float32) => {
            arrays::<Float32Type>(op, left.as_primitive(), right.as_primitive()).map(Some)
        }
        (DataType::Float64, DataType::Float64) => {
            arrays::<Float64Type>(op, left.as_primitive(), right.as_primitive()).map(Some)
        }
        _ => Ok(None),
    }
}

/// `left op right` for a Float32 or Float64 array and a scalar of the same type. Returns `None` for
/// any other pair of types. See [`compare_float_arrays`].
pub(crate) fn compare_float_array_scalar(
    op: Operator,
    left: &dyn Array,
    right: &ScalarValue,
) -> Result<Option<BooleanArray>> {
    match (left.data_type(), right) {
        (DataType::Float32, ScalarValue::Float32(right)) => {
            array_scalar::<Float32Type>(op, left.as_primitive(), *right).map(Some)
        }
        (DataType::Float64, ScalarValue::Float64(right)) => {
            array_scalar::<Float64Type>(op, left.as_primitive(), *right).map(Some)
        }
        _ => Ok(None),
    }
}

fn arrays<T: ArrowPrimitiveType>(
    op: Operator,
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T::Native: Float,
{
    if left.len() != right.len() {
        return internal_err!(
            "Cannot compare arrays of different lengths, got {} and {}",
            left.len(),
            right.len()
        );
    }
    let (l, r) = (left.values().as_ref(), right.values().as_ref());
    // Spark's SQL ordering as single tests, the same as `compare_floats(a, b)` followed by
    // `is_eq()`, `is_ne()`, `is_lt()`, `is_le()`, `is_gt()` or `is_ge()`. IEEE 754 comparisons
    // already treat `-0.0` as equal to `0.0`; the NaN terms put every NaN above the other values
    // and make all NaNs equal.
    let values = match op {
        Operator::Eq | Operator::IsNotDistinctFrom => {
            pack_pairs(l, r, |a, b| (a == b) | (a.is_nan() & b.is_nan()))
        }
        Operator::NotEq | Operator::IsDistinctFrom => {
            pack_pairs(l, r, |a, b| (a != b) & !(a.is_nan() & b.is_nan()))
        }
        Operator::Lt => pack_pairs(l, r, |a, b| (a < b) | (!a.is_nan() & b.is_nan())),
        Operator::LtEq => pack_pairs(l, r, |a, b| (a <= b) | b.is_nan()),
        Operator::Gt => pack_pairs(l, r, |a, b| (a > b) | (a.is_nan() & !b.is_nan())),
        Operator::GtEq => pack_pairs(l, r, |a, b| (a >= b) | a.is_nan()),
        _ => return internal_err!("Unsupported operator for a Spark float comparison: {op}"),
    };
    Ok(comparison_with_nulls(
        op,
        values,
        left.nulls(),
        right.nulls(),
    ))
}

fn array_scalar<T: ArrowPrimitiveType>(
    op: Operator,
    left: &PrimitiveArray<T>,
    right: Option<T::Native>,
) -> Result<BooleanArray>
where
    T::Native: Float,
{
    let len = left.len();
    let Some(s) = right else {
        // Every comparison with a null is null, and only a null is not distinct from one.
        return Ok(match op {
            Operator::IsNotDistinctFrom => BooleanArray::new(
                left.nulls()
                    .map(|nulls| !nulls.inner())
                    .unwrap_or_else(|| BooleanBuffer::new_unset(len)),
                None,
            ),
            Operator::IsDistinctFrom => BooleanArray::new(
                left.nulls()
                    .map(|nulls| nulls.inner().clone())
                    .unwrap_or_else(|| BooleanBuffer::new_set(len)),
                None,
            ),
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => BooleanArray::new_null(len),
            _ => return internal_err!("Unsupported operator for a Spark float comparison: {op}"),
        });
    };
    let l = left.values().as_ref();
    // With the scalar fixed, each operator reduces to one or two tests, chosen by whether the
    // scalar is NaN. Every value sorts below a NaN or equals it, and a NaN sorts above any other
    // value.
    let values = match (op, s.is_nan()) {
        (Operator::Eq | Operator::IsNotDistinctFrom, true) => pack(l, |a| a.is_nan()),
        (Operator::Eq | Operator::IsNotDistinctFrom, false) => pack(l, |a| a == s),
        (Operator::NotEq | Operator::IsDistinctFrom, true) => pack(l, |a| !a.is_nan()),
        (Operator::NotEq | Operator::IsDistinctFrom, false) => pack(l, |a| a != s),
        (Operator::Lt, true) => pack(l, |a| !a.is_nan()),
        (Operator::Lt, false) => pack(l, |a| a < s),
        (Operator::LtEq, true) => BooleanBuffer::new_set(len),
        (Operator::LtEq, false) => pack(l, |a| a <= s),
        (Operator::Gt, true) => BooleanBuffer::new_unset(len),
        (Operator::Gt, false) => pack(l, |a| (a > s) | a.is_nan()),
        (Operator::GtEq, true) => pack(l, |a| a.is_nan()),
        (Operator::GtEq, false) => pack(l, |a| (a >= s) | a.is_nan()),
        _ => return internal_err!("Unsupported operator for a Spark float comparison: {op}"),
    };
    Ok(comparison_with_nulls(op, values, left.nulls(), None))
}

/// Applies SQL's null rules to `values`, the comparison of every slot including the null ones.
/// A null operand makes `=`, `<>`, `<`, `<=`, `>` and `>=` null. `IS NOT DISTINCT FROM`, for which
/// `values` holds equality, and `IS DISTINCT FROM`, for which it holds inequality, compare a null
/// as a value instead: equal to another null and distinct from anything else.
pub(crate) fn comparison_with_nulls(
    op: Operator,
    values: BooleanBuffer,
    left: Option<&NullBuffer>,
    right: Option<&NullBuffer>,
) -> BooleanArray {
    match op {
        Operator::IsNotDistinctFrom => {
            let values = match (left, right) {
                (None, None) => values,
                (Some(nulls), None) | (None, Some(nulls)) => &values & nulls.inner(),
                (Some(l), Some(r)) => {
                    let (l, r) = (l.inner(), r.inner());
                    &(&(&values & l) & r) | &!&(l | r)
                }
            };
            BooleanArray::new(values, None)
        }
        Operator::IsDistinctFrom => {
            let values = match (left, right) {
                (None, None) => values,
                (Some(nulls), None) | (None, Some(nulls)) => &values | &!nulls.inner(),
                (Some(l), Some(r)) => {
                    let (l, r) = (l.inner(), r.inner());
                    &(&(&values & l) & r) | &(l ^ r)
                }
            };
            BooleanArray::new(values, None)
        }
        _ => BooleanArray::new(values, NullBuffer::union(left, right)),
    }
}

/// Packs `test(value)` for each value into a bitmap, 64 values to a word.
#[inline]
fn pack<T: Copy>(values: &[T], test: impl Fn(T) -> bool) -> BooleanBuffer {
    let mut buffer = MutableBuffer::new(bit_util::ceil(values.len(), 64) * 8);
    let (chunks, remainder) = values.as_chunks::<64>();
    for chunk in chunks {
        let mut word = 0u64;
        for (bit, &value) in chunk.iter().enumerate() {
            word |= (test(value) as u64) << bit;
        }
        buffer.push(word);
    }
    if !remainder.is_empty() {
        let mut word = 0u64;
        for (bit, &value) in remainder.iter().enumerate() {
            word |= (test(value) as u64) << bit;
        }
        buffer.push(word);
    }
    BooleanBuffer::new(buffer.into(), 0, values.len())
}

/// Packs `test(left[i], right[i])` for each pair into a bitmap. See [`pack`].
#[inline]
fn pack_pairs<T: Copy>(left: &[T], right: &[T], test: impl Fn(T, T) -> bool) -> BooleanBuffer {
    debug_assert_eq!(left.len(), right.len());
    let mut buffer = MutableBuffer::new(bit_util::ceil(left.len(), 64) * 8);
    let (left_chunks, left_remainder) = left.as_chunks::<64>();
    let (right_chunks, right_remainder) = right.as_chunks::<64>();
    for (l, r) in left_chunks.iter().zip(right_chunks) {
        let mut word = 0u64;
        for bit in 0..64 {
            word |= (test(l[bit], r[bit]) as u64) << bit;
        }
        buffer.push(word);
    }
    if !left_remainder.is_empty() {
        let mut word = 0u64;
        for (bit, (&l, &r)) in left_remainder.iter().zip(right_remainder).enumerate() {
            word |= (test(l, r) as u64) << bit;
        }
        buffer.push(word);
    }
    BooleanBuffer::new(buffer.into(), 0, left.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::float_semantics::{compare_floats, EDGE_VALUES};
    use arrow::array::{ArrayRef, Float32Array, Float64Array};
    use std::cmp::Ordering;
    use std::sync::Arc;

    const OPERATORS: [Operator; 8] = [
        Operator::Eq,
        Operator::NotEq,
        Operator::Lt,
        Operator::LtEq,
        Operator::Gt,
        Operator::GtEq,
        Operator::IsDistinctFrom,
        Operator::IsNotDistinctFrom,
    ];

    /// Spark's answer for `left op right` from [`compare_floats`].
    fn expected(op: Operator, left: Option<f64>, right: Option<f64>) -> Option<bool> {
        let ordering = match (left, right) {
            (Some(l), Some(r)) => Some(compare_floats(l, r)),
            _ => None,
        };
        match op {
            Operator::IsNotDistinctFrom => Some(match (left, right) {
                (None, None) => true,
                (Some(_), Some(_)) => ordering == Some(Ordering::Equal),
                _ => false,
            }),
            Operator::IsDistinctFrom => {
                expected(Operator::IsNotDistinctFrom, left, right).map(|v| !v)
            }
            _ => ordering.map(|ordering| match op {
                Operator::Eq => ordering.is_eq(),
                Operator::NotEq => ordering.is_ne(),
                Operator::Lt => ordering.is_lt(),
                Operator::LtEq => ordering.is_le(),
                Operator::Gt => ordering.is_gt(),
                Operator::GtEq => ordering.is_ge(),
                _ => unreachable!(),
            }),
        }
    }

    /// `values` as a Float64 array, or as a Float32 array holding the same values.
    fn array(values: &[Option<f64>], float32: bool) -> ArrayRef {
        if float32 {
            Arc::new(Float32Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| v as f32))
                    .collect::<Vec<_>>(),
            ))
        } else {
            Arc::new(Float64Array::from(values.to_vec()))
        }
    }

    fn scalar(value: Option<f64>, float32: bool) -> ScalarValue {
        if float32 {
            ScalarValue::Float32(value.map(|v| v as f32))
        } else {
            ScalarValue::Float64(value)
        }
    }

    /// Every pair of edge values, under every operator, as two arrays and as an array and a
    /// scalar, with enough rows that the comparison spans whole words and a partial one.
    #[test]
    fn every_pair_of_edge_values_matches_compare_floats() -> Result<()> {
        // Repeat the pairs so that a run covers several 64-value words and a remainder.
        let pairs: Vec<(Option<f64>, Option<f64>)> = EDGE_VALUES
            .iter()
            .flat_map(|&l| EDGE_VALUES.iter().map(move |&r| (l, r)))
            .cycle()
            .take(EDGE_VALUES.len() * EDGE_VALUES.len() * 3 + 7)
            .collect();
        let left: Vec<_> = pairs.iter().map(|p| p.0).collect();
        let right: Vec<_> = pairs.iter().map(|p| p.1).collect();
        for float32 in [false, true] {
            // Float32 holds every edge value exactly, NaNs included, as the same class of value.
            let (l, r) = (array(&left, float32), array(&right, float32));
            for op in OPERATORS {
                let actual = compare_float_arrays(op, l.as_ref(), r.as_ref())?.unwrap();
                let want: Vec<_> = pairs.iter().map(|&(a, b)| expected(op, a, b)).collect();
                assert_eq!(
                    actual.iter().collect::<Vec<_>>(),
                    want,
                    "a {op} b, f32={float32}"
                );
                for &value in &EDGE_VALUES {
                    let s = scalar(value, float32);
                    let actual = compare_float_array_scalar(op, l.as_ref(), &s)?.unwrap();
                    let want: Vec<_> = left.iter().map(|&a| expected(op, a, value)).collect();
                    assert_eq!(
                        actual.iter().collect::<Vec<_>>(),
                        want,
                        "a {op} {value:?}, f32={float32}"
                    );
                }
            }
        }
        Ok(())
    }

    /// A sliced array starts its values and its null bitmap at an offset.
    #[test]
    fn sliced_inputs() -> Result<()> {
        let values: Vec<Option<f64>> = (0..200)
            .map(|i| EDGE_VALUES[(i * 7) % EDGE_VALUES.len()])
            .collect();
        let others: Vec<Option<f64>> = (0..200)
            .map(|i| EDGE_VALUES[(i * 3 + 1) % EDGE_VALUES.len()])
            .collect();
        let (l, r) = (array(&values, false), array(&others, false));
        for (offset, len) in [(1, 150), (63, 70), (64, 64), (130, 5), (3, 0)] {
            let (ls, rs) = (l.slice(offset, len), r.slice(offset + 2, len));
            for op in OPERATORS {
                let actual = compare_float_arrays(op, ls.as_ref(), rs.as_ref())?.unwrap();
                let want: Vec<_> = (0..len)
                    .map(|i| expected(op, values[offset + i], others[offset + 2 + i]))
                    .collect();
                assert_eq!(actual.iter().collect::<Vec<_>>(), want, "{op} at {offset}");
                let actual =
                    compare_float_array_scalar(op, ls.as_ref(), &ScalarValue::Float64(None))?
                        .unwrap();
                let want: Vec<_> = (0..len)
                    .map(|i| expected(op, values[offset + i], None))
                    .collect();
                assert_eq!(
                    actual.iter().collect::<Vec<_>>(),
                    want,
                    "{op} null at {offset}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn other_types_are_left_to_the_caller() -> Result<()> {
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        let narrow: ArrayRef = Arc::new(Float32Array::from(vec![1.0]));
        assert!(compare_float_arrays(Operator::Lt, floats.as_ref(), narrow.as_ref())?.is_none());
        assert!(compare_float_array_scalar(
            Operator::Lt,
            floats.as_ref(),
            &ScalarValue::Float32(Some(1.0))
        )?
        .is_none());
        assert!(
            compare_float_array_scalar(Operator::Lt, floats.as_ref(), &ScalarValue::Null)?
                .is_none()
        );
        assert!(compare_float_arrays(Operator::Plus, floats.as_ref(), floats.as_ref()).is_err());
        let different_lengths: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        assert!(
            compare_float_arrays(Operator::Lt, floats.as_ref(), different_lengths.as_ref())
                .is_err()
        );
        Ok(())
    }
}
