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

use crate::float_semantics::{compare_floats, spark_comparator};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBufferBuilder, ListArray, PrimitiveArray, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::take;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::Float;

/// Spark's `sort_array` for arrays whose elements hold floats at any depth.
///
/// Spark sorts in its SQL ordering, in which `-0.0` equals `0.0` and all NaNs are equal and larger
/// than every other value, at any depth, with a stable sort, so equal elements keep their order.
/// Null elements come first when ascending and last when descending. Spark's generated code makes
/// one exception: it sorts an ascending array of `FLOAT` or `DOUBLE` that cannot hold a null with
/// `java.util.Arrays.sort`, which puts `-0.0` before `0.0`. DataFusion's `array_sort` uses IEEE
/// 754 total order instead, in which a NaN with the sign bit set sorts first.
///
/// The arguments are the array, whether to sort ascending, and Spark's `containsNull` for the
/// array, which Arrow's field nullability does not carry.
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
        let [array, ascending, contains_null] = args.args.as_slice() else {
            return exec_err!("spark_sort_array takes exactly three arguments");
        };
        let (Some(ascending), Some(contains_null)) = (constant(ascending), constant(contains_null))
        else {
            return exec_err!("spark_sort_array takes constant ascending and containsNull flags");
        };
        let is_scalar = matches!(array, ColumnarValue::Scalar(_));
        let rows = if is_scalar { 1 } else { args.number_rows };
        let array = array.to_array(rows)?;
        // Spark arrays use Arrow's 32-bit List layout.
        let Some(list) = array.as_list_opt::<i32>() else {
            return exec_err!("spark_sort_array takes a list, got {}", array.data_type());
        };
        let result = sort_array(list, ascending, contains_null)?;
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

fn sort_array(array: &ListArray, ascending: bool, contains_null: bool) -> Result<ArrayRef> {
    let java_order = ascending && !contains_null;
    match array.value_type() {
        DataType::Float32 => Ok(sort_floats::<Float32Type>(array, ascending, java_order)),
        DataType::Float64 => Ok(sort_floats::<Float64Type>(array, ascending, java_order)),
        _ => {
            let values = array.values();
            let compare = spark_comparator(values.as_ref(), values.as_ref())?;
            sort_by(array, ascending, compare)
        }
    }
}

/// The order of `Double.compare`, which `java.util.Arrays.sort` uses: Spark's SQL ordering, except
/// that `-0.0` comes before `0.0`.
fn java_order<T: Float>(a: T, b: T) -> Ordering {
    match compare_floats(a, b) {
        // Equal values that are not NaN differ in sign only when they are -0.0 and 0.0.
        Ordering::Equal if !a.is_nan() => b.is_sign_negative().cmp(&a.is_sign_negative()),
        ordering => ordering,
    }
}

/// Sorts the floats of each row as Spark does, within one copy of all the values. Null elements go
/// first when ascending and last when descending, as Spark's comparators put them.
fn sort_floats<T>(array: &ListArray, ascending: bool, java: bool) -> ArrayRef
where
    T: ArrowPrimitiveType,
    T::Native: Float,
{
    let values = array.values().as_primitive::<T>();
    let buffer = values.values();
    let offsets = array.offsets();
    let base = offsets[0] as usize;
    let total = offsets[array.len()] as usize - base;
    let rebased = offsets.iter().map(|offset| offset - offsets[0]).collect();
    let Some(nulls) = values.nulls().filter(|nulls| nulls.null_count() > 0) else {
        // Without null elements, each row sorts where it is.
        let mut sorted = buffer[base..base + total].to_vec();
        for (row, bounds) in offsets.windows(2).enumerate() {
            if array.is_valid(row) {
                let (start, end) = (bounds[0] as usize, bounds[1] as usize);
                let row = &mut sorted[start - base..end - base];
                if !sort_row(row, ascending, java) {
                    restore_ties(row, buffer[start..end].iter().copied(), ascending, java);
                }
            }
        }
        let values = PrimitiveArray::<T>::new(sorted.into(), None);
        return with_values(array, rebased, Arc::new(values));
    };
    // Each row's valid values are copied after its nulls when ascending, or before them when
    // descending, and sorted there. The copy writes every element and moves past only the valid
    // ones, so it can write one place past a row's valid values, into the spare element at the
    // end. A null row's elements stay null, hidden by the list's null.
    let mut sorted = vec![T::Native::default(); total + 1];
    let mut validity = BooleanBufferBuilder::new(total);
    for (row, bounds) in offsets.windows(2).enumerate() {
        let (start, end) = (bounds[0] as usize, bounds[1] as usize);
        if array.is_null(row) {
            validity.append_n(end - start, false);
            continue;
        }
        let valid = nulls
            .buffer()
            .count_set_bits_offset(nulls.offset() + start, end - start);
        let leading_nulls = if ascending { end - start - valid } else { 0 };
        let first = start - base + leading_nulls;
        let mut next = first;
        for index in start..end {
            sorted[next] = buffer[index];
            next += usize::from(nulls.is_valid(index));
        }
        let row = &mut sorted[first..next];
        if !sort_row(row, ascending, java) {
            let original = (start..end)
                .filter(|&index| nulls.is_valid(index))
                .map(|index| buffer[index]);
            restore_ties(row, original, ascending, java);
        }
        validity.append_n(leading_nulls, false);
        validity.append_n(valid, true);
        validity.append_n(end - start - valid - leading_nulls, false);
    }
    sorted.truncate(total);
    let values = PrimitiveArray::<T>::new(sorted.into(), Some(NullBuffer::new(validity.finish())));
    with_values(array, rebased, Arc::new(values))
}

/// The longest row that [`sort_row`] sorts stably. The standard library sorts rows this short by
/// insertion, stable or not, so an unstable sort would only add [`restore_ties`].
const STABLE_SORT_MAX_LEN: usize = 20;

/// Sorts a row of floats in Spark's SQL ordering, reversed when descending, or in [`java_order`],
/// and returns whether the sort was stable, as Spark's is. A longer row sorts faster with an
/// unstable sort, after which [`restore_ties`] gives the stable result.
fn sort_row<N: Float>(row: &mut [N], ascending: bool, java: bool) -> bool {
    let stable = row.len() <= STABLE_SORT_MAX_LEN;
    match (java, ascending, stable) {
        (true, _, true) => row.sort_by(|a, b| java_order(*a, *b)),
        (true, _, false) => row.sort_unstable_by(|a, b| java_order(*a, *b)),
        (false, true, true) => row.sort_by(|a, b| compare_floats(*a, *b)),
        (false, true, false) => row.sort_unstable_by(|a, b| compare_floats(*a, *b)),
        (false, false, true) => row.sort_by(|a, b| compare_floats(*b, *a)),
        (false, false, false) => row.sort_unstable_by(|a, b| compare_floats(*b, *a)),
    }
    stable
}

/// Puts the tied elements of a row sorted by [`sort_row`] back in their order in `original`, the
/// row's values in their original order, as Spark's stable sort leaves them.
///
/// The only tied elements whose bits can differ are NaNs, which sort last when ascending and first
/// when descending, and zeros of either sign, except in [`java_order`]. Each forms one run.
fn restore_ties<N: Float>(
    row: &mut [N],
    original: impl Iterator<Item = N> + Clone,
    ascending: bool,
    java: bool,
) {
    let len = row.len();
    let nans = if ascending {
        len - row.iter().rev().take_while(|v| v.is_nan()).count()..len
    } else {
        0..row.iter().take_while(|v| v.is_nan()).count()
    };
    restore_order(&mut row[nans], original.clone().filter(|v| v.is_nan()));
    if !java {
        let zero = N::zero();
        let first = if ascending {
            row.partition_point(|v| *v < zero)
        } else {
            row.partition_point(|v| v.is_nan() || *v > zero)
        };
        let end = first + row[first..].iter().take_while(|v| **v == zero).count();
        restore_order(&mut row[first..end], original.filter(|v| *v == zero));
    }
}

/// Overwrites a run of tied elements with `values`, the same elements in their original order.
fn restore_order<N>(run: &mut [N], values: impl Iterator<Item = N>) {
    if run.len() > 1 {
        run.iter_mut()
            .zip(values)
            .for_each(|(slot, value)| *slot = value);
    }
}

/// A list with the type and nulls of `array`, holding `values` at `offsets`.
fn with_values(array: &ListArray, offsets: Vec<i32>, values: ArrayRef) -> ArrayRef {
    let DataType::List(field) = array.data_type() else {
        unreachable!("sort_array takes a List");
    };
    Arc::new(ListArray::new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        values,
        array.nulls().cloned(),
    ))
}

/// Sorts the elements of each row stably with `compare`, reversed when descending, after putting
/// null elements first when ascending and last when descending, as Spark's comparators do.
fn sort_by<F>(array: &ListArray, ascending: bool, compare: F) -> Result<ArrayRef>
where
    F: Fn(usize, usize) -> Ordering,
{
    let values = array.values();
    if values.len() > u32::MAX as usize {
        return exec_err!("sort_array cannot sort more than {} elements", u32::MAX);
    }
    let nulls = values.logical_nulls();
    let is_null = |index: usize| nulls.as_ref().is_some_and(|nulls| nulls.is_null(index));
    let mut indices: Vec<u32> = Vec::with_capacity(values.len());
    let mut offsets = Vec::with_capacity(array.len() + 1);
    offsets.push(0i32);
    for (row, bounds) in array.offsets().windows(2).enumerate() {
        if array.is_valid(row) {
            let start = indices.len();
            indices.extend(bounds[0] as u32..bounds[1] as u32);
            indices[start..].sort_by(|&a, &b| {
                let (a, b) = (a as usize, b as usize);
                match (is_null(a), is_null(b)) {
                    (true, true) => Ordering::Equal,
                    (true, false) if ascending => Ordering::Less,
                    (true, false) => Ordering::Greater,
                    (false, true) if ascending => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) if ascending => compare(a, b),
                    (false, false) => compare(b, a),
                }
            });
        }
        offsets.push(indices.len() as i32);
    }
    let sorted = take(values.as_ref(), &UInt32Array::from(indices), None)?;
    Ok(with_values(array, offsets, sorted))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Float64Array, StructArray};
    use arrow::datatypes::{Field, Fields};
    use datafusion::config::ConfigOptions;

    /// A NaN with the sign bit set, which arithmetic produces on x86-64.
    const NEGATIVE_NAN: f64 = f64::from_bits(0xfff8_0000_0000_0000);

    const EDGE_VALUES: [Option<f64>; 10] = [
        Some(f64::NEG_INFINITY),
        Some(-1.0),
        Some(-0.0),
        Some(0.0),
        Some(1.0),
        Some(f64::INFINITY),
        Some(f64::NAN),
        Some(NEGATIVE_NAN),
        // A NaN with a payload.
        Some(f64::from_bits(0x7ff0_0000_0000_0001)),
        None,
    ];

    fn invoke(array: ArrayRef, ascending: bool, contains_null: bool) -> Result<ArrayRef> {
        let rows = array.len();
        let return_field = Arc::new(Field::new("result", array.data_type().clone(), true));
        SparkSortArray::default()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(array),
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(ascending))),
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(contains_null))),
                ],
                arg_fields: vec![],
                number_rows: rows,
                return_field,
                config_options: Arc::new(ConfigOptions::default()),
            })?
            .into_array(rows)
    }

    fn list(rows: &[Option<Vec<Option<f64>>>]) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
            rows.iter().cloned(),
        ))
    }

    fn bits(array: &ArrayRef) -> Vec<Option<Vec<Option<u64>>>> {
        array
            .as_list::<i32>()
            .iter()
            .map(|row| {
                row.map(|row| {
                    row.as_primitive::<Float64Type>()
                        .iter()
                        .map(|v| v.map(f64::to_bits))
                        .collect()
                })
            })
            .collect()
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
                let result = invoke(list(&rows), ascending, true)?;
                assert_eq!(bits(&result), expected, "ascending={ascending}");
                let result = invoke(list(&rows).slice(37, 120), ascending, true)?;
                assert_eq!(bits(&result), &expected[37..157], "ascending={ascending}");
            }
        }
        Ok(())
    }

    /// `java.util.Arrays.sort` on a `double[]` orders by `Double.compare`, which compares
    /// numerically and then by `doubleToLongBits`, so that all NaNs are equal and -0.0 comes before
    /// 0.0, and it keeps the NaNs in their original order.
    #[test]
    fn non_null_ascending_follows_java() -> Result<()> {
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
        let result = invoke(list(&rows), true, false)?;
        assert_eq!(bits(&result), expected);
        Ok(())
    }

    /// Spark's generated code sorts an ascending array that cannot hold a null with
    /// `java.util.Arrays.sort`, which puts -0.0 before 0.0. Descending still ties them.
    #[test]
    fn non_null_ascending_puts_negative_zero_first() -> Result<()> {
        let rows = vec![Some(vec![
            Some(0.0),
            Some(1.0),
            Some(-0.0),
            Some(NEGATIVE_NAN),
        ])];
        let cases = [
            (true, false, vec![-0.0, 0.0, 1.0, NEGATIVE_NAN]),
            (true, true, vec![0.0, -0.0, 1.0, NEGATIVE_NAN]),
            (false, false, vec![NEGATIVE_NAN, 1.0, 0.0, -0.0]),
        ];
        for (ascending, contains_null, expected) in cases {
            let result = invoke(list(&rows), ascending, contains_null)?;
            let expected: Vec<Option<u64>> = expected.iter().map(|v| Some(v.to_bits())).collect();
            assert_eq!(
                bits(&result),
                vec![Some(expected)],
                "ascending={ascending} contains_null={contains_null}"
            );
        }
        Ok(())
    }

    #[test]
    fn non_list_is_an_error() {
        assert!(invoke(Arc::new(Float64Array::from(vec![1.0])), true, true).is_err());
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
        let result = invoke(array, true, true)?;
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
            // Not primitive, so Spark sorts in its SQL ordering whatever containsNull says.
            let result = invoke(Arc::clone(&array), ascending, false)?;
            let row = result.as_list::<i32>().value(0);
            let x = row
                .as_struct()
                .column(0)
                .as_primitive::<Float64Type>()
                .clone();
            let actual: Vec<u64> = x.values().iter().map(|v| v.to_bits()).collect();
            let expected: Vec<u64> = expected.iter().map(|v| v.to_bits()).collect();
            assert_eq!(actual, expected, "ascending={ascending}");
        }
        Ok(())
    }
}
