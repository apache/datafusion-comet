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
use super::set_bits_before;
use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBufferBuilder, ListArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Spark's `array_contains` for arrays whose elements hold floats at any depth.
///
/// Spark tests each element against the value with `genEqual`, in which `-0.0` equals `0.0` and
/// all NaNs are equal, at any depth of an array or struct element. The result is true when an
/// element matches, null when none does and the array holds a null element, and false otherwise;
/// a null array or value gives null. datafusion-spark's `array_contains`, which Comet uses for
/// other element types, compares the bits instead, so `0.0` misses a `-0.0` and a NaN misses a
/// NaN whose bits differ.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkFloatArrayContains {
    signature: Signature,
}

impl Default for SparkFloatArrayContains {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkFloatArrayContains {
    fn name(&self) -> &str {
        "spark_array_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_with_mask(self, args, Operator::IsNotDistinctFrom, from_mask)
    }
}

fn from_mask(array: &ListArray, elements: &ArrayRef, matches: BooleanBuffer) -> Result<ArrayRef> {
    let offsets = array.offsets();
    // One pass over each bitmap gives every row's match count and valid count.
    let matched = set_bits_before(&matches, offsets);
    let valid = elements
        .nulls()
        .map(|nulls| set_bits_before(nulls.inner(), offsets));
    let mut values = BooleanBufferBuilder::new(array.len());
    let mut validity = BooleanBufferBuilder::new(array.len());
    for (row, bounds) in offsets.windows(2).enumerate() {
        let found = matched[row + 1] > matched[row];
        let has_null = valid
            .as_ref()
            .is_some_and(|valid| valid[row + 1] - valid[row] < bounds[1] - bounds[0]);
        values.append(found);
        validity.append(array.is_valid(row) && (found || !has_null));
    }
    Ok(Arc::new(BooleanArray::new(
        values.finish(),
        Some(NullBuffer::new(validity.finish())),
    )))
}

impl MatchOperation<i32> for SparkFloatArrayContains {
    fn apply(rows: &MatchRows<'_, i32>, equal: impl Fn(usize, usize) -> bool) -> Result<ArrayRef> {
        let mut values = BooleanBufferBuilder::new(rows.array.len());
        let mut validity = BooleanBufferBuilder::new(rows.array.len());
        for (row, (position, has_null)) in rows.first_matches(equal).enumerate() {
            let found = position != 0;
            values.append(found);
            let valid = rows.nulls.as_ref().is_none_or(|nulls| nulls.is_valid(row));
            validity.append(valid && (found || !has_null));
        }
        Ok(Arc::new(BooleanArray::new(
            values.finish(),
            Some(NullBuffer::new(validity.finish())),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::list;
    use super::*;
    use crate::float_semantics::{compare_floats, EDGE_VALUES};
    use arrow::array::{AsArray, Float64Array, Int32Array, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields};
    use datafusion::common::ScalarValue;
    use datafusion::config::ConfigOptions;

    fn invoke(array: ColumnarValue, value: ColumnarValue, rows: usize) -> Result<ColumnarValue> {
        let return_field = Arc::new(Field::new("result", DataType::Boolean, true));
        SparkFloatArrayContains::default().invoke_with_args(ScalarFunctionArgs {
            args: vec![array, value],
            arg_fields: vec![],
            number_rows: rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn booleans(result: ColumnarValue) -> Vec<Option<bool>> {
        let ColumnarValue::Array(array) = result else {
            panic!("expected an array");
        };
        array.as_boolean().iter().collect()
    }

    /// Spark's result for one row: true if a non-null element `genEqual`s the value, else null if
    /// the row holds a null, else false.
    fn spark_contains(row: &[Option<f64>], value: f64) -> Option<bool> {
        if row
            .iter()
            .any(|element| element.is_some_and(|e| compare_floats(e, value).is_eq()))
        {
            Some(true)
        } else if row.contains(&None) {
            None
        } else {
            Some(false)
        }
    }

    /// Every edge value looked up in rows of edge values, so each zero and each NaN meets every
    /// other, through the constant path and the per-row path.
    #[test]
    fn floats_follow_spark_equality() -> Result<()> {
        let rows: Vec<Option<Vec<Option<f64>>>> = EDGE_VALUES
            .iter()
            .map(|skipped| {
                Some(
                    EDGE_VALUES
                        .iter()
                        .filter(|v| v != &skipped)
                        .copied()
                        .collect(),
                )
            })
            .collect();
        let array = list(&rows);
        for value in EDGE_VALUES.into_iter().flatten() {
            let expected: Vec<Option<bool>> = rows
                .iter()
                .map(|row| spark_contains(row.as_ref().unwrap(), value))
                .collect();
            let constant = invoke(
                ColumnarValue::Array(Arc::clone(&array)),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
                rows.len(),
            )?;
            assert_eq!(booleans(constant), expected, "constant {value:?}");
            let per_row = Float64Array::from(vec![Some(value); rows.len()]);
            let column = invoke(
                ColumnarValue::Array(Arc::clone(&array)),
                ColumnarValue::Array(Arc::new(per_row)),
                rows.len(),
            )?;
            assert_eq!(booleans(column), expected, "per row {value:?}");
        }
        Ok(())
    }

    #[test]
    fn null_rows_null_values_and_empty_arrays() -> Result<()> {
        let array = list(&[
            None,
            Some(vec![]),
            Some(vec![None]),
            Some(vec![Some(1.0), None]),
        ]);
        let constant = invoke(
            ColumnarValue::Array(Arc::clone(&array)),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
            4,
        )?;
        assert_eq!(
            booleans(constant),
            vec![None, Some(false), None, Some(true)]
        );
        let null_value = invoke(
            ColumnarValue::Array(Arc::clone(&array)),
            ColumnarValue::Scalar(ScalarValue::Float64(None)),
            4,
        )?;
        assert_eq!(booleans(null_value), vec![None; 4]);
        let per_row = Float64Array::from(vec![Some(1.0), Some(1.0), None, Some(2.0)]);
        let column = invoke(
            ColumnarValue::Array(array),
            ColumnarValue::Array(Arc::new(per_row)),
            4,
        )?;
        assert_eq!(booleans(column), vec![None, Some(false), None, None]);
        Ok(())
    }

    #[test]
    fn all_scalars() -> Result<()> {
        let row = ScalarValue::List(Arc::new(
            list(&[Some(vec![Some(-0.0), None])])
                .as_list::<i32>()
                .clone(),
        ));
        let result = invoke(
            ColumnarValue::Scalar(row.clone()),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))),
            1,
        )?;
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
        ));
        let result = invoke(
            ColumnarValue::Scalar(row),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0))),
            1,
        )?;
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Boolean(None))
        ));
        Ok(())
    }

    /// Elements that are themselves arrays go through `spark_equality`, which compares their
    /// floats the same way.
    #[test]
    fn nested_elements() -> Result<()> {
        // Two rows of two arrays each: [[-0.0], [1.0]] and [[NaN, 1.0], [2.0]].
        let inner = list(&[
            Some(vec![Some(-0.0)]),
            Some(vec![Some(1.0)]),
            Some(vec![Some(f64::NAN), Some(1.0)]),
            Some(vec![Some(2.0)]),
        ]);
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", inner.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 2, 4].into()),
            inner,
            None,
        ));
        // Looked-up values: [0.0] for the first row, [-NaN, 1.0] for the second.
        let values = list(&[
            Some(vec![Some(0.0)]),
            Some(vec![Some(-f64::NAN), Some(1.0)]),
        ]);
        let result = invoke(ColumnarValue::Array(array), ColumnarValue::Array(values), 2)?;
        assert_eq!(booleans(result), vec![Some(true), Some(true)]);
        Ok(())
    }

    /// Struct elements compare their float fields the same way; another field still has to match.
    #[test]
    fn struct_elements() -> Result<()> {
        let fields = Fields::from(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Int32, true),
        ]);
        let elements: ArrayRef = Arc::new(StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Float64Array::from(vec![-0.0, f64::NAN, 2.0])),
                Arc::new(Int32Array::from(vec![1, 1, 1])),
            ],
            None,
        ));
        // One row holding all three structs.
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", elements.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 3].into()),
            elements,
            None,
        ));
        let lookup = |x: f64, y: i32| -> Result<ColumnarValue> {
            let value: ArrayRef = Arc::new(StructArray::new(
                fields.clone(),
                vec![
                    Arc::new(Float64Array::from(vec![x])),
                    Arc::new(Int32Array::from(vec![y])),
                ],
                None,
            ));
            invoke(
                ColumnarValue::Array(Arc::clone(&array)),
                ColumnarValue::Array(value),
                1,
            )
        };
        assert_eq!(booleans(lookup(0.0, 1)?), vec![Some(true)]);
        assert_eq!(booleans(lookup(-f64::NAN, 1)?), vec![Some(true)]);
        assert_eq!(booleans(lookup(0.0, 2)?), vec![Some(false)]);
        Ok(())
    }
}
