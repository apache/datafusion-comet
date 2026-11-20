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

use super::array_match::{invoke_with_mask, MatchOperation, MatchRows};
use super::{set_bits_before, with_values};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBufferBuilder, ListArray, PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::compute::filter;
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

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
        invoke_with_mask(self, args, Operator::IsDistinctFrom, from_mask)
    }
}

fn from_mask(array: &ListArray, values: &ArrayRef, keep: BooleanBuffer) -> Result<ArrayRef> {
    let offsets = OffsetBuffer::new(set_bits_before(&keep, array.offsets()).into());
    let values = filter(values, &BooleanArray::new(keep, None))?;
    Ok(with_values(array, offsets, values, array.nulls().cloned()))
}

impl MatchOperation<i32> for SparkArrayRemove {
    fn apply_float<T: ArrowPrimitiveType>(
        rows: &MatchRows<'_, i32>,
        values: &PrimitiveArray<T>,
        equal: impl Fn(usize, usize) -> bool,
    ) -> Result<ArrayRef> {
        let (buffer, nulls) = (values.values(), values.nulls());
        let mut kept: Vec<T::Native> = Vec::with_capacity(values.len());
        let mut validity = nulls.map(|_| BooleanBufferBuilder::new(values.len()));
        let mut offsets = Vec::with_capacity(rows.array.len() + 1);
        offsets.push(0i32);
        for (range, needle) in rows.iter() {
            match (nulls, validity.as_mut()) {
                (Some(nulls), Some(validity)) => {
                    for index in range {
                        let valid = nulls.is_valid(index);
                        if !valid || !equal(index, needle) {
                            kept.push(buffer[index]);
                            validity.append(valid);
                        }
                    }
                }
                _ => kept.extend(
                    range
                        .filter(|&index| !equal(index, needle))
                        .map(|index| buffer[index]),
                ),
            }
            offsets.push(kept.len() as i32);
        }
        let kept = PrimitiveArray::<T>::new(
            kept.into(),
            validity.map(|mut validity| NullBuffer::new(validity.finish())),
        );
        Ok(with_values(
            rows.array,
            OffsetBuffer::new(offsets.into()),
            Arc::new(kept),
            rows.nulls.clone(),
        ))
    }

    fn apply(rows: &MatchRows<'_, i32>, equal: impl Fn(usize, usize) -> bool) -> Result<ArrayRef> {
        let values = rows.array.values();
        let element_nulls = values.logical_nulls();
        let mut keep = BooleanBufferBuilder::new(values.len());
        keep.append_n(values.len(), false);
        let mut offsets = Vec::with_capacity(rows.array.len() + 1);
        offsets.push(0i32);
        let mut kept = 0i32;
        for (range, needle) in rows.iter() {
            for element in range {
                let is_null = element_nulls
                    .as_ref()
                    .is_some_and(|nulls| nulls.is_null(element));
                if is_null || !equal(element, needle) {
                    keep.set_bit(element, true);
                    kept += 1;
                }
            }
            offsets.push(kept);
        }
        let kept_values = filter(values, &BooleanArray::new(keep.finish(), None))?;
        Ok(with_values(
            rows.array,
            OffsetBuffer::new(offsets.into()),
            kept_values,
            rows.nulls.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::{bits, first_row_field_bits, list};
    use super::*;
    use crate::float_semantics::{compare_floats, EDGE_VALUES, NEGATIVE_NAN};
    use arrow::array::{AsArray, Float32Array, Float64Array, StructArray};
    use arrow::datatypes::{Field, Fields, Float32Type};
    use datafusion::common::ScalarValue;
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
