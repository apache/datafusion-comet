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

use std::sync::Arc;

use super::with_values;
use crate::float_semantics::{compare_floats, spark_equality};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, ListArray, PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::compute::filter;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use datafusion::common::{exec_err, utils::take_function_args, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::Float;

/// Spark's `array_remove` for arrays whose elements hold floats at any depth.
///
/// Spark removes every element equal to the value under `genEqual`, in which `-0.0` equals `0.0`
/// and all NaNs are equal, at any depth of an array or struct element. It keeps null elements,
/// and returns null when the array or the value is null. DataFusion's `array_remove_all`, which
/// Comet uses for other element types, compares the bits instead, so it keeps a `-0.0` when
/// removing `0.0`, and a NaN whose bits differ.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkArrayRemove {
    signature: Signature,
}

impl Default for SparkArrayRemove {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayRemove {
    fn name(&self) -> &str {
        "spark_array_remove"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [array, value] = take_function_args(self.name(), &args.args)?;
        let all_scalars = matches!(
            (array, value),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        let rows = if all_scalars { 1 } else { args.number_rows };
        let array = array.to_array(rows)?;
        // Spark arrays use Arrow's 32-bit List layout.
        let Some(list) = array.as_list_opt::<i32>() else {
            return exec_err!("spark_array_remove takes a list, got {}", array.data_type());
        };
        let result = match value {
            ColumnarValue::Scalar(needle) => remove_constant(list, needle)?,
            ColumnarValue::Array(value) => array_remove(list, value)?,
        };
        if all_scalars {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn array_remove(array: &ListArray, value: &ArrayRef) -> Result<ArrayRef> {
    // Spark casts the value to the element type. Anything else fails in `spark_equality`.
    match (array.value_type(), value.data_type()) {
        (DataType::Float32, DataType::Float32) => Ok(remove_floats::<Float32Type>(array, value)),
        (DataType::Float64, DataType::Float64) => Ok(remove_floats::<Float64Type>(array, value)),
        _ => {
            let equal = spark_equality(array.values().as_ref(), value.as_ref())?;
            remove_where(array, value, equal)
        }
    }
}

/// [`array_remove`] of the same value from every row, which a float array does in one pass over
/// all the values, as DataFusion does for its own `array_remove`.
fn remove_constant(array: &ListArray, needle: &ScalarValue) -> Result<ArrayRef> {
    match (array.value_type(), needle) {
        (DataType::Float32, ScalarValue::Float32(Some(needle))) => {
            remove_float_constant::<Float32Type>(array, *needle)
        }
        (DataType::Float64, ScalarValue::Float64(Some(needle))) => {
            remove_float_constant::<Float64Type>(array, *needle)
        }
        _ => array_remove(array, &needle.to_array_of_size(array.len())?),
    }
}

fn remove_float_constant<T>(array: &ListArray, needle: T::Native) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: Float,
{
    let offsets = array.offsets();
    let first = offsets[0] as usize;
    let values = array
        .values()
        .slice(first, offsets[array.len()] as usize - first);
    let floats = values.as_primitive::<T>();
    let buffer = floats.values();
    // For a fixed value, Spark's genEqual is one test per element: IEEE `!=`, under which the two
    // zeros are equal and a NaN differs from every number, or `is_nan` for a NaN value.
    let differs = if needle.is_nan() {
        BooleanBuffer::collect_bool(buffer.len(), |index| !buffer[index].is_nan())
    } else {
        BooleanBuffer::collect_bool(buffer.len(), |index| buffer[index] != needle)
    };
    // Null elements stay.
    let keep = match floats.nulls() {
        Some(nulls) => &differs | &!nulls.inner(),
        None => differs,
    };
    // A null row keeps its elements in the values; the list's null hides them.
    let kept_offsets = kept_offsets(&keep, offsets);
    let kept_values = filter(&values, &BooleanArray::new(keep, None))?;
    Ok(with_values(
        array,
        kept_offsets,
        kept_values,
        array.nulls().cloned(),
    ))
}

/// The offsets of the elements that `keep` keeps, for a `keep` that starts at `offsets[0]`: the
/// number of its set bits before each offset, counted a word at a time in one pass.
fn kept_offsets(keep: &BooleanBuffer, offsets: &OffsetBuffer<i32>) -> OffsetBuffer<i32> {
    let chunks = keep.inner().bit_chunks(keep.offset(), keep.len());
    let mut words = chunks.iter_padded();
    let (mut word, mut word_start, mut before_word) = (words.next().unwrap_or(0), 0, 0);
    let kept = offsets.iter().map(|offset| {
        let end = (offset - offsets[0]) as usize;
        while end >= word_start + 64 {
            before_word += word.count_ones() as usize;
            word = words.next().unwrap_or(0);
            word_start += 64;
        }
        let below_end = word & ((1u64 << (end - word_start)) - 1);
        (before_word + below_end.count_ones() as usize) as i32
    });
    OffsetBuffer::new(kept.collect::<Vec<i32>>().into())
}

/// Copies the floats of each row that are null or differ from the row's value in Spark's
/// `genEqual`, straight from the flattened values. A row is null when the array or the value is.
fn remove_floats<T>(array: &ListArray, value: &ArrayRef) -> ArrayRef
where
    T: ArrowPrimitiveType,
    T::Native: Float,
{
    let values = array.values().as_primitive::<T>();
    let needles = value.as_primitive::<T>().values();
    let (buffer, nulls) = (values.values(), values.nulls());
    let row_nulls = NullBuffer::union(array.nulls(), value.nulls());
    let mut kept: Vec<T::Native> = Vec::with_capacity(values.len());
    let mut validity = nulls.map(|_| BooleanBufferBuilder::new(values.len()));
    let mut offsets = Vec::with_capacity(array.len() + 1);
    offsets.push(0i32);
    for (row, bounds) in array.offsets().windows(2).enumerate() {
        if row_nulls.as_ref().is_none_or(|nulls| nulls.is_valid(row)) {
            let needle = needles[row];
            let (start, end) = (bounds[0] as usize, bounds[1] as usize);
            match (nulls, validity.as_mut()) {
                (Some(nulls), Some(validity)) => {
                    for index in start..end {
                        let valid = nulls.is_valid(index);
                        if !valid || !compare_floats(buffer[index], needle).is_eq() {
                            kept.push(buffer[index]);
                            validity.append(valid);
                        }
                    }
                }
                _ => kept.extend(
                    buffer[start..end]
                        .iter()
                        .copied()
                        .filter(|&element| !compare_floats(element, needle).is_eq()),
                ),
            }
        }
        offsets.push(kept.len() as i32);
    }
    let kept = PrimitiveArray::<T>::new(
        kept.into(),
        validity.map(|mut validity| NullBuffer::new(validity.finish())),
    );
    with_values(
        array,
        OffsetBuffer::new(offsets.into()),
        Arc::new(kept),
        row_nulls,
    )
}

/// Copies each row's elements except the non-null ones for which `equal(element, row)` holds. A
/// row is null when the array or the value is.
fn remove_where<F>(array: &ListArray, value: &ArrayRef, equal: F) -> Result<ArrayRef>
where
    F: Fn(usize, usize) -> bool,
{
    let values = array.values();
    let element_nulls = values.logical_nulls();
    let row_nulls = NullBuffer::union(array.nulls(), value.nulls());
    let mut keep = BooleanBufferBuilder::new(values.len());
    keep.append_n(values.len(), false);
    let mut offsets = Vec::with_capacity(array.len() + 1);
    offsets.push(0i32);
    let mut kept = 0i32;
    for (row, bounds) in array.offsets().windows(2).enumerate() {
        if row_nulls.as_ref().is_none_or(|nulls| nulls.is_valid(row)) {
            for element in bounds[0] as usize..bounds[1] as usize {
                let is_null = element_nulls
                    .as_ref()
                    .is_some_and(|nulls| nulls.is_null(element));
                if is_null || !equal(element, row) {
                    keep.set_bit(element, true);
                    kept += 1;
                }
            }
        }
        offsets.push(kept);
    }
    let kept_values = filter(values, &BooleanArray::new(keep.finish(), None))?;
    Ok(with_values(
        array,
        OffsetBuffer::new(offsets.into()),
        kept_values,
        row_nulls,
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

    fn invoke(array: ColumnarValue, value: ColumnarValue, rows: usize) -> Result<ColumnarValue> {
        let return_field = Arc::new(Field::new("result", array.data_type(), true));
        SparkArrayRemove::default().invoke_with_args(ScalarFunctionArgs {
            args: vec![array, value],
            arg_fields: vec![],
            number_rows: rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    /// Spark's result for one row: drop the non-null elements that `genEqual` the value.
    fn spark_remove(row: &[Option<f64>], value: f64) -> Vec<Option<u64>> {
        row.iter()
            .filter(|element| element.is_none_or(|e| !compare_floats(e, value).is_eq()))
            .map(|element| element.map(f64::to_bits))
            .collect()
    }

    /// Every edge value removed from an array of all edge values, so each zero and each NaN meets
    /// every other, and the kept elements keep their bits.
    #[test]
    fn floats_follow_spark_equality() -> Result<()> {
        let elements: Vec<Option<f64>> = EDGE_VALUES.to_vec();
        let removable: Vec<f64> = EDGE_VALUES.iter().flatten().copied().collect();
        let rows: Vec<Option<Vec<Option<f64>>>> =
            removable.iter().map(|_| Some(elements.clone())).collect();
        let result = invoke(
            ColumnarValue::Array(list(&rows)),
            ColumnarValue::Array(Arc::new(Float64Array::from(removable.clone()))),
            rows.len(),
        )?
        .into_array(rows.len())?;
        let expected: Vec<Option<Vec<Option<u64>>>> = removable
            .iter()
            .map(|&value| Some(spark_remove(&elements, value)))
            .collect();
        assert_eq!(bits(&result), expected);
        Ok(())
    }

    /// Removing a constant takes one pass over all the values, which must still respect each row's
    /// bounds, null rows and null elements, in a sliced array too.
    #[test]
    fn constant_value() -> Result<()> {
        let rows = vec![
            Some(vec![Some(1.0), Some(-0.0)]),
            Some(vec![Some(0.0), None, Some(NEGATIVE_NAN)]),
            None,
            Some(vec![Some(f64::NAN), Some(0.0), Some(2.0)]),
        ];
        let full = list(&rows);
        for (array, rows) in [
            (Arc::clone(&full), &rows[..]),
            (full.slice(1, 3), &rows[1..]),
        ] {
            for needle in [Some(0.0), Some(f64::NAN), None] {
                let result = invoke(
                    ColumnarValue::Array(Arc::clone(&array)),
                    ColumnarValue::Scalar(ScalarValue::Float64(needle)),
                    rows.len(),
                )?
                .into_array(rows.len())?;
                let expected: Vec<Option<Vec<Option<u64>>>> = rows
                    .iter()
                    .map(|row| {
                        let needle = needle?;
                        row.as_ref().map(|row| spark_remove(row, needle))
                    })
                    .collect();
                assert_eq!(bits(&result), expected, "remove {needle:?}");
            }
        }
        Ok(())
    }

    /// Kept counts are read from the mask a 64-bit word at a time, so these rows end on word
    /// boundaries and inside words, span several words, or are empty or null, and so do those of
    /// the slice, whose values start at a word boundary.
    #[test]
    fn constant_value_across_words() -> Result<()> {
        let lengths = [64, 0, 64, 1, 63, 128, 5, 59, 130, 0, 70, 3];
        let rows: Vec<Option<Vec<Option<f64>>>> = lengths
            .iter()
            .enumerate()
            .map(|(row, &length)| {
                (row != 7).then(|| {
                    (0..length)
                        .map(|i| EDGE_VALUES[(row + i) % EDGE_VALUES.len()])
                        .collect()
                })
            })
            .collect();
        let full = list(&rows);
        for (array, rows) in [
            (Arc::clone(&full), &rows[..]),
            (full.slice(2, 8), &rows[2..10]),
        ] {
            for needle in [0.0, f64::NAN, 1.0] {
                let result = invoke(
                    ColumnarValue::Array(Arc::clone(&array)),
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(needle))),
                    rows.len(),
                )?
                .into_array(rows.len())?;
                let expected: Vec<Option<Vec<Option<u64>>>> = rows
                    .iter()
                    .map(|row| row.as_ref().map(|row| spark_remove(row, needle)))
                    .collect();
                assert_eq!(bits(&result), expected, "remove {needle}");
            }
        }
        Ok(())
    }

    #[test]
    fn nulls() -> Result<()> {
        let rows = vec![
            Some(vec![Some(0.0), None, Some(-0.0)]),
            None,
            Some(vec![Some(1.0)]),
        ];
        let value = Float64Array::from(vec![Some(0.0), Some(0.0), None]);
        let result = invoke(
            ColumnarValue::Array(list(&rows)),
            ColumnarValue::Array(Arc::new(value)),
            3,
        )?
        .into_array(3)?;
        // The null element stays; a null array or a null value gives a null row.
        assert_eq!(bits(&result), vec![Some(vec![None]), None, None]);
        Ok(())
    }

    #[test]
    fn float32_and_scalars() -> Result<()> {
        let array = ScalarValue::List(Arc::new(
            ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
                Some(-0.0f32),
                Some(f32::from_bits(0xffc0_0000)),
                Some(1.0),
            ])]),
        ));
        let value = ScalarValue::Float32(Some(0.0));
        let result = invoke(
            ColumnarValue::Scalar(array.clone()),
            ColumnarValue::Scalar(value),
            4,
        )?;
        let ColumnarValue::Scalar(ScalarValue::List(result)) = result else {
            panic!("expected a list scalar");
        };
        let values = result.value(0);
        let values: &Float32Array = values.as_primitive();
        assert_eq!(values.len(), 2, "only the zero is removed");
        let result = invoke(
            ColumnarValue::Scalar(array),
            ColumnarValue::Scalar(ScalarValue::Float32(Some(f32::NAN))),
            1,
        )?;
        let ColumnarValue::Scalar(ScalarValue::List(result)) = result else {
            panic!("expected a list scalar");
        };
        let values = result.value(0);
        let values: &Float32Array = values.as_primitive();
        assert_eq!(values.values().as_ref(), &[-0.0, 1.0]);
        assert_eq!(values.value(0).to_bits(), (-0.0f32).to_bits());
        Ok(())
    }

    /// Structs compare their float fields in Spark's order too.
    #[test]
    fn nested_elements() -> Result<()> {
        let fields = Fields::from(vec![Field::new("x", DataType::Float64, true)]);
        let structs = |values: Vec<f64>| -> ArrayRef {
            Arc::new(StructArray::new(
                fields.clone(),
                vec![Arc::new(Float64Array::from(values))],
                None,
            ))
        };
        let element = Field::new("item", DataType::Struct(fields.clone()), true);
        let array = ListArray::new(
            Arc::new(element),
            OffsetBuffer::from_lengths([3]),
            structs(vec![-0.0, NEGATIVE_NAN, 1.0]),
            None,
        );
        for (value, expected) in [(0.0, vec![NEGATIVE_NAN, 1.0]), (f64::NAN, vec![-0.0, 1.0])] {
            let result = invoke(
                ColumnarValue::Array(Arc::new(array.clone())),
                ColumnarValue::Array(structs(vec![value])),
                1,
            )?
            .into_array(1)?;
            let expected: Vec<u64> = expected.iter().map(|v| v.to_bits()).collect();
            assert_eq!(first_row_field_bits(&result), expected, "remove {value}");
        }
        Ok(())
    }

    /// Spark casts the value to the element type, so a value of another type is an error, not a
    /// panic, whether it is a constant or a column.
    #[test]
    fn mismatched_value_type() {
        let array = list(&[Some(vec![Some(1.0)])]);
        for value in [
            ColumnarValue::Scalar(ScalarValue::Float32(Some(1.0))),
            ColumnarValue::Array(Arc::new(Float32Array::from(vec![1.0f32]))),
        ] {
            assert!(invoke(ColumnarValue::Array(Arc::clone(&array)), value, 1).is_err());
        }
    }
}
