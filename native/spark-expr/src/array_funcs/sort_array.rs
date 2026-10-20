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

use std::cmp::Ordering;
use std::sync::Arc;

use super::with_values;
use crate::float_semantics::{
    compare_floats, compare_floats_java, float_gt, float_lt, spark_comparator,
};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBufferBuilder, ListArray, PrimitiveArray, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::take;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use datafusion::common::{exec_err, utils::take_function_args, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::Float;

/// Spark's `sort_array` for arrays whose elements hold floats at any depth.
///
/// Spark sorts in its SQL ordering, in which `-0.0` equals `0.0` and all NaNs are equal and larger
/// than every other value, at any depth, with a stable sort, so equal elements keep their order.
/// Null elements come first when ascending and last when descending. DataFusion's `array_sort`
/// uses IEEE 754 total order instead, in which a NaN with the sign bit set sorts first.
///
/// The arguments are the array, whether to sort ascending, and whether `-0.0` sorts before `0.0`.
/// Spark's generated code sorts that way when it sorts an ascending array of `FLOAT` or `DOUBLE`
/// that cannot hold a null with `java.util.Arrays.sort`. The serde decides, since it depends on
/// Spark's `containsNull`, which Arrow's field nullability does not carry.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkSortArray {
    signature: Signature,
}

impl Default for SparkSortArray {
    fn default() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSortArray {
    fn name(&self) -> &str {
        "spark_sort_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [array, ascending, negative_zero_first] = take_function_args(self.name(), &args.args)?;
        let order = match (constant(ascending), constant(negative_zero_first)) {
            (Some(true), Some(false)) => Order::Ascending,
            (Some(false), Some(false)) => Order::Descending,
            (Some(true), Some(true)) => Order::Java,
            _ => {
                return exec_err!(
                    "spark_sort_array takes constant boolean flags, and puts -0.0 first only \
                     when sorting ascending"
                )
            }
        };
        let is_scalar = matches!(array, ColumnarValue::Scalar(_));
        let rows = if is_scalar { 1 } else { args.number_rows };
        let array = array.to_array(rows)?;
        // Spark arrays use Arrow's 32-bit List layout.
        let Some(list) = array.as_list_opt::<i32>() else {
            return exec_err!("spark_sort_array takes a list, got {}", array.data_type());
        };
        let result = sort_array(list, order)?;
        if is_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn constant(value: &ColumnarValue) -> Option<bool> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(value))) => Some(*value),
        _ => None,
    }
}

/// The order of a Spark `sort_array`.
#[derive(Clone, Copy, PartialEq)]
enum Order {
    /// Spark's SQL ordering.
    Ascending,
    /// Spark's SQL ordering, reversed.
    Descending,
    /// [`compare_floats_java`], the order of `java.util.Arrays.sort`, which is ascending.
    Java,
}

fn sort_array(array: &ListArray, order: Order) -> Result<ArrayRef> {
    match array.value_type() {
        DataType::Float32 => Ok(sort_floats::<Float32Type>(array, order)),
        DataType::Float64 => Ok(sort_floats::<Float64Type>(array, order)),
        _ => {
            // The serde sorts -0.0 first only for FLOAT and DOUBLE elements.
            let values = array.values();
            let compare = spark_comparator(values.as_ref(), values.as_ref())?;
            sort_by(array, order != Order::Descending, compare)
        }
    }
}

/// Sorts the floats of each row as Spark does, within one copy of all the values. Null elements go
/// first when ascending and last when descending, as Spark's comparators put them.
fn sort_floats<T>(array: &ListArray, order: Order) -> ArrayRef
where
    T: ArrowPrimitiveType,
    T::Native: Float,
{
    let values = array.values().as_primitive::<T>();
    let buffer = values.values();
    let offsets = array.offsets();
    let base = offsets[0] as usize;
    let total = offsets[array.len()] as usize - base;
    let rebased = offsets.clone().subtract(offsets[0]);
    let Some(nulls) = values.nulls().filter(|nulls| nulls.null_count() > 0) else {
        // Without null elements, each row sorts where it is.
        let mut sorted = buffer[base..base + total].to_vec();
        for (row, bounds) in offsets.windows(2).enumerate() {
            if array.is_valid(row) {
                let (start, end) = (bounds[0] as usize, bounds[1] as usize);
                let original = buffer[start..end].iter().copied();
                sort_row(&mut sorted[start - base..end - base], original, order);
            }
        }
        let values = PrimitiveArray::<T>::new(sorted.into(), None);
        return with_values(array, rebased, Arc::new(values), array.nulls().cloned());
    };
    // Each row's valid values are copied to its end when ascending, after its nulls, or to its
    // start when descending, before them, and sorted there. The copy writes every element and
    // moves on past only the valid ones. A null row's elements stay null, hidden by the list's
    // null.
    let mut sorted = vec![T::Native::default(); total];
    let mut validity = BooleanBufferBuilder::new(total);
    for (row, bounds) in offsets.windows(2).enumerate() {
        let (start, end) = (bounds[0] as usize, bounds[1] as usize);
        if array.is_null(row) {
            validity.append_n(end - start, false);
            continue;
        }
        let (row_start, row_end) = (start - base, end - base);
        let valid = if order == Order::Descending {
            let mut next = row_start;
            for index in start..end {
                sorted[next] = buffer[index];
                next += usize::from(nulls.is_valid(index));
            }
            row_start..next
        } else {
            let mut next = row_end;
            for index in (start..end).rev() {
                sorted[next - 1] = buffer[index];
                next -= usize::from(nulls.is_valid(index));
            }
            next..row_end
        };
        validity.append_n(valid.start - row_start, false);
        validity.append_n(valid.len(), true);
        validity.append_n(row_end - valid.end, false);
        let original = (start..end)
            .filter(|&index| nulls.is_valid(index))
            .map(|index| buffer[index]);
        sort_row(&mut sorted[valid], original, order);
    }
    let values = PrimitiveArray::<T>::new(sorted.into(), Some(NullBuffer::new(validity.finish())));
    with_values(array, rebased, Arc::new(values), array.nulls().cloned())
}

/// The longest row that [`sort_row`] sorts stably. The standard library sorts rows this short by
/// insertion, stable or not, so an unstable sort would only add [`restore_ties`].
const STABLE_SORT_MAX_LEN: usize = 20;

/// Sorts a row of floats to the result of Spark's stable sort, given `original`, the row's values
/// in their original order. A longer row sorts faster with an unstable sort, after which
/// [`restore_ties`] puts the tied elements back in their original order.
fn sort_row<N: Float>(row: &mut [N], original: impl Iterator<Item = N> + Clone, order: Order) {
    let stable = row.len() <= STABLE_SORT_MAX_LEN;
    sort_by_order(row, stable, order);
    if !stable {
        restore_ties(row, original, order);
    }
}

/// Sorts a row in `order`, with each comparator compiled separately.
fn sort_by_order<N: Float>(row: &mut [N], stable: bool, order: Order) {
    match (order, stable) {
        (Order::Ascending, true) => row.sort_by(|a, b| compare_floats(*a, *b)),
        (Order::Ascending, false) => row.sort_unstable_by(|a, b| compare_floats(*a, *b)),
        (Order::Descending, true) => row.sort_by(|a, b| compare_floats(*b, *a)),
        (Order::Descending, false) => row.sort_unstable_by(|a, b| compare_floats(*b, *a)),
        (Order::Java, true) => row.sort_by(|a, b| compare_floats_java(*a, *b)),
        (Order::Java, false) => row.sort_unstable_by(|a, b| compare_floats_java(*a, *b)),
    }
}

/// Puts the tied elements of a row sorted by an unstable sort back in their order in `original`,
/// the row's values in their original order, as Spark's stable sort leaves them.
///
/// The only tied elements whose bits can differ are NaNs, which sort last unless descending, and
/// zeros of either sign, except in [`Order::Java`]. Each forms one run.
fn restore_ties<N: Float>(row: &mut [N], original: impl Iterator<Item = N> + Clone, order: Order) {
    let len = row.len();
    let nans = if order == Order::Descending {
        0..row.iter().take_while(|v| v.is_nan()).count()
    } else {
        len - row.iter().rev().take_while(|v| v.is_nan()).count()..len
    };
    restore_order(&mut row[nans], original.clone().filter(|v| v.is_nan()));
    let zero = N::zero();
    let first = match order {
        Order::Ascending => row.partition_point(|v| float_lt(*v, zero)),
        Order::Descending => row.partition_point(|v| float_gt(*v, zero)),
        Order::Java => return,
    };
    let end = first + row[first..].iter().take_while(|v| **v == zero).count();
    restore_order(&mut row[first..end], original.filter(|v| *v == zero));
}

/// Overwrites a run of tied elements with `values`, the same elements in their original order.
fn restore_order<N>(run: &mut [N], values: impl Iterator<Item = N>) {
    if run.len() > 1 {
        run.iter_mut()
            .zip(values)
            .for_each(|(slot, value)| *slot = value);
    }
}

/// Sorts the elements of each row stably with `compare`, reversed when descending. `compare` puts
/// null elements first, as Spark's ascending comparator does, so reversing it puts them last, as
/// the descending one does.
fn sort_by<F>(array: &ListArray, ascending: bool, compare: F) -> Result<ArrayRef>
where
    F: Fn(usize, usize) -> Ordering,
{
    let values = array.values();
    if values.len() > u32::MAX as usize {
        return exec_err!("sort_array cannot sort more than {} elements", u32::MAX);
    }
    let mut indices: Vec<u32> = Vec::with_capacity(values.len());
    let mut offsets = Vec::with_capacity(array.len() + 1);
    offsets.push(0i32);
    for (row, bounds) in array.offsets().windows(2).enumerate() {
        if array.is_valid(row) {
            let start = indices.len();
            indices.extend(bounds[0] as u32..bounds[1] as u32);
            indices[start..].sort_by(|&a, &b| {
                let (a, b) = (a as usize, b as usize);
                if ascending {
                    compare(a, b)
                } else {
                    compare(b, a)
                }
            });
        }
        offsets.push(indices.len() as i32);
    }
    let sorted = take(values.as_ref(), &UInt32Array::from(indices), None)?;
    Ok(with_values(
        array,
        OffsetBuffer::new(offsets.into()),
        sorted,
        array.nulls().cloned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::super::test_util::{bits, first_row_field_bits, list};
    use super::*;
    use crate::float_semantics::{EDGE_VALUES, NEGATIVE_NAN};
    use arrow::array::{Float32Array, Float64Array, StructArray};
    use arrow::datatypes::{Field, Fields};
    use datafusion::config::ConfigOptions;

    fn invoke(array: ArrayRef, ascending: bool, negative_zero_first: bool) -> Result<ArrayRef> {
        let rows = array.len();
        let return_field = Arc::new(Field::new("result", array.data_type().clone(), true));
        SparkSortArray::default()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(array),
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(ascending))),
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(negative_zero_first))),
                ],
                arg_fields: vec![],
                number_rows: rows,
                return_field,
                config_options: Arc::new(ConfigOptions::default()),
            })?
            .into_array(rows)
    }

    fn row_bits(row: &[Option<f64>]) -> Vec<Option<u64>> {
        row.iter().map(|v| v.map(f64::to_bits)).collect()
    }

    /// Pseudo-random sequences of edge values, the same on every run, of up to 82 elements, since
    /// sorts longer than 20 elements are where an unstable sort reorders ties.
    fn sequences() -> Vec<Option<Vec<Option<f64>>>> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        (0..200)
            .map(|i| {
                Some(
                    (0..i * 7 % 83)
                        .map(|_| EDGE_VALUES[(next() % EDGE_VALUES.len() as u64) as usize])
                        .collect(),
                )
            })
            .chain([None])
            .collect()
    }

    /// The sequences with their null elements removed.
    fn without_nulls(rows: &[Option<Vec<Option<f64>>>]) -> Vec<Option<Vec<Option<f64>>>> {
        rows.iter()
            .map(|row| {
                row.as_ref()
                    .map(|row| row.iter().flatten().map(|v| Some(*v)).collect())
            })
            .collect()
    }

    /// Spark's comparator sort: nulls first when ascending and last when descending, then
    /// `compareDoubles`, reversed when descending, in a stable sort.
    fn spark_sort(row: &[Option<f64>], ascending: bool) -> Vec<Option<u64>> {
        let mut sorted = row.to_vec();
        sorted.sort_by(|a, b| match (a, b) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) if ascending => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) if ascending => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(a), Some(b)) if ascending => compare_floats(*a, *b),
            (Some(a), Some(b)) => compare_floats(*b, *a),
        });
        row_bits(&sorted)
    }

    /// An array that can hold nulls sorts stably in Spark's order: the zeros tie and keep their
    /// order, and so do the NaNs, which sort last. That holds with and without null elements, and
    /// in a sliced array, whose values start past its first offset.
    #[test]
    fn floats_follow_spark_order() -> Result<()> {
        let with_nulls = sequences();
        let without_nulls = without_nulls(&with_nulls);
        for rows in [with_nulls, without_nulls] {
            for ascending in [true, false] {
                let expected: Vec<Option<Vec<Option<u64>>>> = rows
                    .iter()
                    .map(|row| row.as_ref().map(|row| spark_sort(row, ascending)))
                    .collect();
                let result = invoke(list(&rows), ascending, false)?;
                assert_eq!(bits(&result), expected, "ascending={ascending}");
                let result = invoke(list(&rows).slice(37, 120), ascending, false)?;
                assert_eq!(bits(&result), &expected[37..157], "ascending={ascending}");
            }
        }
        Ok(())
    }

    /// `java.util.Arrays.sort` on a `double[]` orders by `Double.compare`, which compares
    /// numerically and then by `doubleToLongBits`, so that all NaNs are equal and -0.0 comes before
    /// 0.0, and it keeps the NaNs in their original order.
    #[test]
    fn negative_zero_first_follows_java() -> Result<()> {
        let long_bits = |v: f64| {
            if v.is_nan() {
                f64::NAN.to_bits() as i64
            } else {
                v.to_bits() as i64
            }
        };
        let rows = without_nulls(&sequences());
        let expected: Vec<Option<Vec<Option<u64>>>> = rows
            .iter()
            .map(|row| {
                row.as_ref().map(|row| {
                    let mut sorted: Vec<f64> = row.iter().flatten().copied().collect();
                    sorted.sort_by(|a, b| {
                        a.partial_cmp(b)
                            .filter(|ordering| ordering.is_ne())
                            .unwrap_or_else(|| long_bits(*a).cmp(&long_bits(*b)))
                    });
                    sorted.iter().map(|v| Some(v.to_bits())).collect()
                })
            })
            .collect();
        let result = invoke(list(&rows), true, true)?;
        assert_eq!(bits(&result), expected);
        Ok(())
    }

    /// With `-0.0` first, the zeros no longer tie and keep their order.
    #[test]
    fn negative_zero_first() -> Result<()> {
        let rows = vec![Some(vec![
            Some(0.0),
            Some(1.0),
            Some(-0.0),
            Some(NEGATIVE_NAN),
        ])];
        let cases = [
            (true, vec![-0.0, 0.0, 1.0, NEGATIVE_NAN]),
            (false, vec![0.0, -0.0, 1.0, NEGATIVE_NAN]),
        ];
        for (negative_zero_first, expected) in cases {
            let result = invoke(list(&rows), true, negative_zero_first)?;
            let expected: Vec<Option<u64>> = expected.iter().map(|v| Some(v.to_bits())).collect();
            assert_eq!(
                bits(&result),
                vec![Some(expected)],
                "negative_zero_first={negative_zero_first}"
            );
        }
        Ok(())
    }

    /// A non-list, and `-0.0` first in a descending sort, which Spark never asks for.
    #[test]
    fn invalid_arguments() {
        assert!(invoke(Arc::new(Float64Array::from(vec![1.0])), true, false).is_err());
        assert!(invoke(list(&[Some(vec![Some(0.0)])]), false, true).is_err());
    }

    #[test]
    fn float32() -> Result<()> {
        let array = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![
                Some(f32::NAN),
                Some(0.0f32),
                Some(f32::from_bits(0xffc0_0000)),
                Some(-0.0),
                None,
            ]),
        ]));
        let result = invoke(array, true, false)?;
        let row = result.as_list::<i32>().value(0);
        let row: &Float32Array = row.as_primitive();
        let actual: Vec<Option<u32>> = row.iter().map(|v| v.map(f32::to_bits)).collect();
        let expected = [
            None,
            Some(0.0f32),
            Some(-0.0),
            Some(f32::NAN),
            Some(f32::from_bits(0xffc0_0000)),
        ];
        let expected: Vec<Option<u32>> = expected.iter().map(|v| v.map(f32::to_bits)).collect();
        assert_eq!(actual, expected);
        Ok(())
    }

    /// Struct elements compare their float fields in Spark's order and also sort stably.
    #[test]
    fn nested_elements() -> Result<()> {
        let fields = Fields::from(vec![Field::new("x", DataType::Float64, true)]);
        let element = Field::new("item", DataType::Struct(fields.clone()), true);
        let values = vec![1.0, NEGATIVE_NAN, 0.0, f64::NAN, -0.0, -1.0];
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(element),
            OffsetBuffer::from_lengths([values.len()]),
            Arc::new(StructArray::new(
                fields,
                vec![Arc::new(Float64Array::from(values))],
                None,
            )),
            None,
        ));
        for (ascending, expected) in [
            (true, vec![-1.0, 0.0, -0.0, 1.0, NEGATIVE_NAN, f64::NAN]),
            (false, vec![NEGATIVE_NAN, f64::NAN, 1.0, 0.0, -0.0, -1.0]),
        ] {
            let result = invoke(Arc::clone(&array), ascending, false)?;
            let expected: Vec<u64> = expected.iter().map(|v| v.to_bits()).collect();
            assert_eq!(
                first_row_field_bits(&result),
                expected,
                "ascending={ascending}"
            );
        }
        Ok(())
    }
}
