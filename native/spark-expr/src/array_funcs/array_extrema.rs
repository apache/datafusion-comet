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

use crate::float_semantics::{float_gt, float_lt, spark_comparator};
use arrow::array::{
    make_array, new_empty_array, Array, ArrayAccessor, ArrayRef, AsArray, BooleanArray,
    DynComparator, ListArray, MutableArrayData, PrimitiveArray, PrimitiveBuilder, StringArrayType,
    StructArray, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::take;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::functions_nested::min_max::{array_max_udf, array_min_udf};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use num::Float;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
enum Utf8Collation {
    Binary,
    BinaryRtrim,
    Lcase,
    LcaseRtrim,
}

impl Utf8Collation {
    fn compare(self, mut left: &str, mut right: &str, unicode_version: u32) -> Ordering {
        if matches!(self, Self::BinaryRtrim | Self::LcaseRtrim) {
            // Spark RTRIM ignores trailing U+0020, not arbitrary Unicode whitespace.
            left = left.trim_end_matches(' ');
            right = right.trim_end_matches(' ');
        }
        if matches!(self, Self::Binary | Self::BinaryRtrim) {
            return left.cmp(right);
        }
        fn lower(value: &str, unicode_version: u32) -> impl Iterator<Item = u32> + '_ {
            value.chars().flat_map(move |c| {
                let cp = c as u32;
                let [first, second] = match cp {
                    // Spark treats final sigma as ordinary sigma.
                    0x3c2 => [0x3c3, 0],
                    // Unicode 17 adds these mappings to the library's Unicode 16 data.
                    0xa7ce | 0xa7d2 | 0xa7d4 if unicode_version == 17 => [cp + 1, 0],
                    0x16ea0..=0x16eb8 if unicode_version == 17 => [cp + 0x1b, 0],
                    _ => unicode_case_mapping::to_lowercase(c),
                };
                // The library uses zero for an unchanged code point or absent second value.
                std::iter::once(if first == 0 { cp } else { first })
                    .chain((second != 0).then_some(second))
            })
        }
        // Compare ASCII prefixes lazily so a retained long winner is not rescanned.
        for (offset, (l, r)) in left.bytes().zip(right.bytes()).enumerate() {
            if !l.is_ascii() || !r.is_ascii() {
                return lower(&left[offset..], unicode_version)
                    .cmp(lower(&right[offset..], unicode_version));
            }
            let ordering = l.to_ascii_lowercase().cmp(&r.to_ascii_lowercase());
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        left.len().cmp(&right.len())
    }
}

/// Spark's array_min/array_max retain the first non-null value on an ordering tie.
/// In particular, signed zeros compare equal and all NaNs compare equal and greater
/// than non-NaNs. Nested arrays and structs use the same ordering, with nulls first.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkArrayExtrema {
    is_min: bool,
    datafusion_udf: Arc<ScalarUDF>,
    string_collations: Vec<Utf8Collation>,
    unicode_version: u32,
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
            string_collations: Vec::new(),
            unicode_version: 0,
        }
    }

    /// Scala validates the Unicode version and lists string-leaf collations depth-first.
    pub fn with_collations(
        is_min: bool,
        collations: &[String],
        unicode_version: u32,
    ) -> Result<Self> {
        let string_collations = collations
            .iter()
            .map(|name| match name.as_str() {
                "UTF8_BINARY" => Ok(Utf8Collation::Binary),
                "UTF8_BINARY_RTRIM" => Ok(Utf8Collation::BinaryRtrim),
                "UTF8_LCASE" => Ok(Utf8Collation::Lcase),
                "UTF8_LCASE_RTRIM" => Ok(Utf8Collation::LcaseRtrim),
                _ => exec_err!("Unsupported array extrema collation: {name}"),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            string_collations,
            unicode_version,
            ..Self::new(is_min)
        })
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
        if self.string_collations.is_empty()
            && !matches!(
                element_type,
                DataType::Float32 | DataType::Float64 | DataType::List(_) | DataType::Struct(_)
            )
        {
            return self.datafusion_udf.invoke_with_args(args);
        }

        let is_scalar = matches!(input, ColumnarValue::Scalar(_));
        let array = match input {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(value) => value.to_array()?,
        };
        // Spark arrays use Arrow's 32-bit List layout.
        let array = array.as_list::<i32>();
        let result: ArrayRef = match array.value_type() {
            DataType::Float32 => Arc::new(float_extrema::<Float32Type>(array, self.is_min)),
            DataType::Float64 => Arc::new(float_extrema::<Float64Type>(array, self.is_min)),
            _ => nested_extrema(
                array,
                self.is_min,
                self.string_collations.iter(),
                self.unicode_version,
            )?,
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

/// Scan the flat value buffer for every list length. Arrow's float min/max kernels
/// use a different ordering, so long lists must not switch to those kernels.
/// Checking only Arrow's winner cannot establish that its answer agrees with Spark:
/// for max([-NaN, 1.0]), Arrow's total ordering selects 1.0 while Spark selects NaN.
/// A corrective scan triggered only by a zero or NaN winner would miss that case.
fn float_extrema<T: ArrowPrimitiveType>(array: &ListArray, is_min: bool) -> PrimitiveArray<T>
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
            let start = offsets[0] as usize;
            let end = offsets[1] as usize;
            for (index, &candidate) in buffer[start..end].iter().enumerate() {
                if nulls.is_some_and(|nulls| nulls.is_null(start + index)) {
                    continue;
                }
                let replace = match best {
                    None => true,
                    Some(current) if is_min => float_lt(candidate, current),
                    Some(current) => float_gt(candidate, current),
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

fn nested_extrema(
    array: &ListArray,
    is_min: bool,
    mut collations: std::slice::Iter<Utf8Collation>,
    unicode_version: u32,
) -> Result<ArrayRef> {
    let values = array.values();
    let compare = collated_comparator(values, &mut collations, unicode_version)?;
    let nulls = values.nulls();
    let ordering = if is_min {
        Ordering::Less
    } else {
        Ordering::Greater
    };
    let mut indices = Vec::with_capacity(array.len());
    for (row, offsets) in array.offsets().windows(2).enumerate() {
        let mut best = None;
        if array.is_valid(row) {
            for candidate in offsets[0] as usize..offsets[1] as usize {
                if nulls.is_some_and(|nulls| nulls.is_null(candidate)) {
                    continue;
                }
                if best.is_none_or(|current| compare(candidate, current) == ordering) {
                    best = Some(candidate);
                }
            }
        }
        indices.push(best.map(|index| index as u32));
    }
    take_extrema_values(values, &UInt32Array::from(indices))
}

#[inline(never)]
fn take_extrema_values(values: &ArrayRef, indices: &UInt32Array) -> Result<ArrayRef> {
    match values.data_type() {
        DataType::List(field) if !field.data_type().is_nested() => {
            let lists = values.as_list::<i32>();
            // A selected struct can contain a null list field with hidden child values.
            // Combine source-list validity with null indices before counting or copying children.
            let nulls = match lists.nulls().filter(|nulls| nulls.null_count() > 0) {
                Some(nulls) => {
                    let validity = BooleanArray::new(nulls.inner().clone(), None);
                    let selected = take(&validity, indices, None)?;
                    let selected = selected.as_boolean();
                    let valid_values = NullBuffer::new(selected.values().clone());
                    NullBuffer::union(Some(&valid_values), selected.nulls())
                }
                None => indices.nulls().cloned(),
            }
            .filter(|nulls| nulls.null_count() > 0);
            let source_offsets = lists.value_offsets();
            let length =
                |index: usize| (source_offsets[index + 1] - source_offsets[index]) as usize;
            // Reserve selected lengths, not the average length of all candidates: large losing
            // lists must not inflate even temporary child-buffer allocations.
            let capacity = match &nulls {
                Some(nulls) => nulls
                    .valid_indices()
                    .map(|row| length(indices.value(row) as usize))
                    .sum(),
                None => indices
                    .values()
                    .iter()
                    .map(|&index| length(index as usize))
                    .sum(),
            };
            let data = lists.values().to_data();
            let mut child = MutableArrayData::new(vec![&data], false, capacity);
            let mut offsets = Vec::with_capacity(indices.len() + 1);
            offsets.push(0);
            match &nulls {
                Some(nulls) => {
                    for row in nulls.valid_indices() {
                        offsets.resize(row + 1, child.len() as i32);
                        let index = indices.value(row) as usize;
                        child.try_extend(
                            0,
                            source_offsets[index] as usize,
                            source_offsets[index + 1] as usize,
                        )?;
                        offsets.push(child.len() as i32);
                    }
                    offsets.resize(indices.len() + 1, child.len() as i32);
                }
                None => {
                    for &index in indices.values() {
                        let index = index as usize;
                        child.try_extend(
                            0,
                            source_offsets[index] as usize,
                            source_offsets[index + 1] as usize,
                        )?;
                        offsets.push(child.len() as i32);
                    }
                }
            }
            // SAFETY: Offsets start at zero and increase with child.len(). Winners occupy
            // disjoint input ranges, so their total fits the input's i32 offsets.
            let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
            Ok(Arc::new(ListArray::try_new(
                Arc::clone(field),
                offsets,
                make_array(child.freeze()),
                nulls,
            )?))
        }
        DataType::List(_) => {
            // Arrow's list take estimates child capacity from all input lists, including
            // large losing candidates. Start empty so even temporary buffers grow only
            // with selected values, including for sparse or recursively nested results.
            let data = values.to_data();
            let mut result = MutableArrayData::new(vec![&data], true, 0);
            for index in indices.iter() {
                match index.filter(|&index| values.is_valid(index as usize)) {
                    Some(index) => result.try_extend(0, index as usize, index as usize + 1)?,
                    None => result.try_extend_nulls(1)?,
                }
            }
            Ok(make_array(result.freeze()))
        }
        DataType::Struct(fields) => {
            let columns = values
                .as_struct()
                .columns()
                .iter()
                .map(|column| take_extrema_values(column, indices))
                .collect::<Result<Vec<_>>>()?;
            let nulls = indices
                .iter()
                .map(|index| index.is_some_and(|index| values.is_valid(index as usize)))
                .collect::<NullBuffer>();
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                Some(nulls),
                indices.len(),
            )?))
        }
        _ => Ok(take(values.as_ref(), indices, None)?),
    }
}

/// Build one comparator per child array, adding string collation to Spark's ordering.
/// Noncollated values use the shared Spark comparator, including its floating-point rules.
fn collated_comparator(
    array: &ArrayRef,
    collations: &mut std::slice::Iter<Utf8Collation>,
    unicode_version: u32,
) -> Result<DynComparator> {
    if collations.is_none() {
        return spark_comparator(array.as_ref(), array.as_ref());
    }
    match array.data_type() {
        DataType::List(_) => {
            let array = array.as_list::<i32>();
            let compare = collated_comparator(array.values(), collations, unicode_version)?;
            let offsets = array.offsets().clone();
            Ok(nulls_first(array.nulls().cloned(), move |left, right| {
                let left_start = offsets[left] as usize;
                let right_start = offsets[right] as usize;
                let left_len = offsets[left + 1] as usize - left_start;
                let right_len = offsets[right + 1] as usize - right_start;
                for offset in 0..left_len.min(right_len) {
                    let ordering = compare(left_start + offset, right_start + offset);
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
                left_len.cmp(&right_len)
            }))
        }
        DataType::Struct(_) => {
            let array = array.as_struct();
            let fields = array
                .columns()
                .iter()
                .map(|field| collated_comparator(field, collations, unicode_version))
                .collect::<Result<Vec<_>>>()?;
            Ok(nulls_first(array.nulls().cloned(), move |left, right| {
                fields
                    .iter()
                    .map(|compare| compare(left, right))
                    .find(|&ordering| ordering != Ordering::Equal)
                    .unwrap_or(Ordering::Equal)
            }))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            if !collations.as_slice().is_empty() =>
        {
            let collation = collations.next().unwrap();
            Ok(match array.data_type() {
                DataType::Utf8 => string_comparator(
                    array.as_string::<i32>().clone(),
                    *collation,
                    unicode_version,
                ),
                DataType::LargeUtf8 => string_comparator(
                    array.as_string::<i64>().clone(),
                    *collation,
                    unicode_version,
                ),
                _ => string_comparator(array.as_string_view().clone(), *collation, unicode_version),
            })
        }
        _ => spark_comparator(array.as_ref(), array.as_ref()),
    }
}

fn string_comparator<A>(array: A, collation: Utf8Collation, unicode_version: u32) -> DynComparator
where
    A: Array + 'static,
    for<'a> &'a A: StringArrayType<'a>,
{
    nulls_first(array.nulls().cloned(), move |left, right| {
        collation.compare((&array).value(left), (&array).value(right), unicode_version)
    })
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
