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

use arrow::array::{
    make_comparator, new_empty_array, Array, ArrayRef, AsArray, DynComparator, GenericListArray,
    GenericListViewArray, OffsetSizeTrait, PrimitiveArray, PrimitiveBuilder, UInt64Array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::{cast, take, SortOptions};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::functions_nested::min_max::{array_max_udf, array_min_udf};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use num::Float;

/// Spark's array_min/array_max retain the first non-null value on an ordering tie.
/// In particular, signed zeros compare equal and all NaNs compare equal and greater
/// than non-NaNs. Nested arrays and structs use the same ordering, with nulls first.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkArrayExtrema {
    is_min: bool,
    datafusion_udf: Arc<ScalarUDF>,
}

impl SparkArrayExtrema {
    pub fn new(is_min: bool) -> Self {
        Self {
            is_min,
            // Capture the original implementation, not a registry lookup: these UDFs
            // replace the DataFusion names in Comet's function registry.
            datafusion_udf: if is_min {
                array_min_udf()
            } else {
                array_max_udf()
            },
        }
    }
}

impl ScalarUDFImpl for SparkArrayExtrema {
    fn name(&self) -> &str {
        if self.is_min {
            "array_min"
        } else {
            "array_max"
        }
    }

    fn signature(&self) -> &Signature {
        self.datafusion_udf.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.datafusion_udf.return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return exec_err!("{} takes exactly one argument", self.name());
        };
        let element_type = self.return_type(&[input.data_type()])?;

        // DataFusion's non-primitive path reconstructs an array from scalars, which
        // cannot infer a type from an empty iterator. Keep the declared element type.
        if matches!(input, ColumnarValue::Array(array) if array.is_empty()) {
            return Ok(ColumnarValue::Array(new_empty_array(&element_type)));
        }
        if !needs_spark_ordering(&element_type) {
            return self.datafusion_udf.invoke_with_args(args);
        }

        let is_scalar = matches!(input, ColumnarValue::Scalar(_));
        let array = match input {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(value) => value.to_array()?,
        };
        let result = match array.data_type() {
            DataType::List(_) => array_extrema(array.as_list::<i32>(), self.is_min)?,
            DataType::LargeList(_) => array_extrema(array.as_list::<i64>(), self.is_min)?,
            other => return exec_err!("{} does not support type {other}", self.name()),
        };

        if is_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn needs_spark_ordering(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float32
            | DataType::Float64
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_)
            | DataType::Dictionary(_, _)
    )
}

fn array_extrema<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    is_min: bool,
) -> Result<ArrayRef> {
    match array.value_type() {
        DataType::Float32 => Ok(Arc::new(float_extrema::<O, Float32Type>(array, is_min))),
        DataType::Float64 => Ok(Arc::new(float_extrema::<O, Float64Type>(array, is_min))),
        _ => nested_extrema(array, is_min),
    }
}

/// Scan the flat value buffer for every list length. Arrow's float min/max kernels
/// use a different ordering, so long lists must not switch to those kernels.
fn float_extrema<O: OffsetSizeTrait, T: ArrowPrimitiveType>(
    array: &GenericListArray<O>,
    is_min: bool,
) -> PrimitiveArray<T>
where
    T::Native: Float,
{
    let values = array.values().as_primitive::<T>();
    let buffer = values.values();
    let nulls = values.nulls();
    let mut result = PrimitiveBuilder::<T>::with_capacity(array.len());
    for (row, offsets) in array.offsets().windows(2).enumerate() {
        let mut best: Option<T::Native> = None;
        if array.is_valid(row) {
            let start = offsets[0].as_usize();
            let end = offsets[1].as_usize();
            for (index, &candidate) in buffer[start..end].iter().enumerate() {
                if nulls.is_some_and(|nulls| nulls.is_null(start + index)) {
                    continue;
                }
                let replace = match best {
                    None => true,
                    Some(current) if is_min => {
                        candidate < current || (!candidate.is_nan() && current.is_nan())
                    }
                    Some(current) => {
                        candidate > current || (candidate.is_nan() && !current.is_nan())
                    }
                };
                if replace {
                    // Copy the winning value, never normalize its zero sign or NaN bits.
                    best = Some(candidate);
                }
            }
        }
        result.append_option(best);
    }
    result.finish()
}

fn nested_extrema<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    is_min: bool,
) -> Result<ArrayRef> {
    let values = array.values();
    let compare = spark_comparator(values)?;
    // Dictionary keys can refer to null values even when the keys themselves are valid.
    let nulls = values.logical_nulls();
    let ordering = if is_min {
        Ordering::Less
    } else {
        Ordering::Greater
    };
    let mut indices = Vec::with_capacity(array.len());
    for (row, offsets) in array.offsets().windows(2).enumerate() {
        let mut best = None;
        if array.is_valid(row) {
            for candidate in offsets[0].as_usize()..offsets[1].as_usize() {
                if nulls.as_ref().is_some_and(|nulls| nulls.is_null(candidate)) {
                    continue;
                }
                if best.is_none_or(|current| compare(candidate, current) == ordering) {
                    best = Some(candidate);
                }
            }
        }
        indices.push(best.map(|index| index as u64));
    }
    // Take from the original values, not comparator-normalized or reconstructed values.
    // This preserves nested fields, dictionary types, signed zeros, and NaN payloads.
    Ok(take(values.as_ref(), &UInt64Array::from(indices), None)?)
}

/// Build one comparator per child array, not per row. This is local to extrema:
/// DataFusion's ScalarValue nested comparisons put inner nulls last, unlike Spark.
fn spark_comparator(array: &ArrayRef) -> Result<DynComparator> {
    match array.data_type() {
        DataType::Float32 => Ok(float_comparator::<Float32Type>(array)),
        DataType::Float64 => Ok(float_comparator::<Float64Type>(array)),
        DataType::List(_) => list_comparator(array.as_list::<i32>()),
        DataType::LargeList(_) => list_comparator(array.as_list::<i64>()),
        DataType::ListView(_) => list_view_comparator(array.as_list_view::<i32>()),
        DataType::LargeListView(_) => list_view_comparator(array.as_list_view::<i64>()),
        DataType::FixedSizeList(_, _) => {
            let array = array.as_fixed_size_list();
            let compare = spark_comparator(array.values())?;
            let size = array.value_length() as usize;
            Ok(nulls_first(array.logical_nulls(), move |left, right| {
                compare_ranges(left * size, size, right * size, size, &compare)
            }))
        }
        DataType::Struct(_) => {
            let array = array.as_struct();
            let fields = array
                .columns()
                .iter()
                .map(spark_comparator)
                .collect::<Result<Vec<_>>>()?;
            Ok(nulls_first(array.logical_nulls(), move |left, right| {
                fields
                    .iter()
                    .map(|compare| compare(left, right))
                    .find(|&ordering| ordering != Ordering::Equal)
                    .unwrap_or(Ordering::Equal)
            }))
        }
        DataType::Dictionary(_, value_type) => {
            // Decode only for comparisons. Recursing after decoding also covers
            // dictionaries whose values are nested arrays or structs with floats.
            spark_comparator(&cast(array.as_ref(), value_type)?)
        }
        _ => Ok(make_comparator(
            array.as_ref(),
            array.as_ref(),
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )?),
    }
}

fn float_comparator<T: ArrowPrimitiveType>(array: &ArrayRef) -> DynComparator
where
    T::Native: Float,
{
    let values = array.as_primitive::<T>().values().clone();
    nulls_first(array.logical_nulls(), move |left, right| {
        let left = values[left];
        let right = values[right];
        if left == right || (left.is_nan() && right.is_nan()) {
            Ordering::Equal
        } else if left > right || left.is_nan() {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    })
}

fn list_comparator<O: OffsetSizeTrait>(array: &GenericListArray<O>) -> Result<DynComparator> {
    let compare = spark_comparator(array.values())?;
    let offsets = array.offsets().clone();
    Ok(nulls_first(array.logical_nulls(), move |left, right| {
        let left_start = offsets[left].as_usize();
        let right_start = offsets[right].as_usize();
        compare_ranges(
            left_start,
            offsets[left + 1].as_usize() - left_start,
            right_start,
            offsets[right + 1].as_usize() - right_start,
            &compare,
        )
    }))
}

fn list_view_comparator<O: OffsetSizeTrait>(
    array: &GenericListViewArray<O>,
) -> Result<DynComparator> {
    let compare = spark_comparator(array.values())?;
    let offsets = array.offsets().clone();
    let sizes = array.sizes().clone();
    Ok(nulls_first(array.logical_nulls(), move |left, right| {
        compare_ranges(
            offsets[left].as_usize(),
            sizes[left].as_usize(),
            offsets[right].as_usize(),
            sizes[right].as_usize(),
            &compare,
        )
    }))
}

fn compare_ranges(
    left_start: usize,
    left_len: usize,
    right_start: usize,
    right_len: usize,
    compare: &DynComparator,
) -> Ordering {
    for offset in 0..left_len.min(right_len) {
        let ordering = compare(left_start + offset, right_start + offset);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left_len.cmp(&right_len)
}

fn nulls_first(
    nulls: Option<NullBuffer>,
    compare: impl Fn(usize, usize) -> Ordering + Send + Sync + 'static,
) -> DynComparator {
    match nulls {
        None => Box::new(compare),
        Some(nulls) => {
            Box::new(
                move |left, right| match (nulls.is_null(left), nulls.is_null(right)) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    (false, false) => compare(left, right),
                },
            )
        }
    }
}

#[cfg(test)]
mod tests;
