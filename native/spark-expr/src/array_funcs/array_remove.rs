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

use crate::float_semantics::{compare_floats, has_float_leaf, spark_equality};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, ListArray, PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::compute::filter;
use arrow::datatypes::{ArrowPrimitiveType, DataType, FieldRef, Float32Type, Float64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::functions_nested::remove::array_remove_all_udf;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use num::Float;

/// Spark's `array_remove` for arrays whose elements hold floats at any depth.
///
/// Spark removes every element equal to the value under `genEqual`, in which `-0.0` equals `0.0`
/// and all NaNs are equal, at any depth of an array or struct element. It keeps null elements,
/// and returns null when the array or the value is null. DataFusion's `array_remove_all` compares
/// the bits instead, so it keeps a `-0.0` when removing `0.0`, and a NaN whose bits differ. Other
/// element types go to DataFusion's implementation, which this replaces in Comet's registry.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkArrayRemove {
    datafusion_udf: Arc<ScalarUDF>,
}

impl Default for SparkArrayRemove {
    fn default() -> Self {
        Self {
            datafusion_udf: array_remove_all_udf(),
        }
    }
}

impl ScalarUDFImpl for SparkArrayRemove {
    fn name(&self) -> &str {
        "array_remove_all"
    }

    fn signature(&self) -> &Signature {
        self.datafusion_udf.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    /// DataFusion's version answers only this, with the array's field made nullable when either
    /// argument is.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.datafusion_udf.return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [array, value] = args.args.as_slice() else {
            return exec_err!("array_remove takes exactly two arguments");
        };
        // Spark arrays use Arrow's 32-bit List layout.
        let DataType::List(field) = array.data_type() else {
            return self.datafusion_udf.invoke_with_args(args);
        };
        if !has_float_leaf(field.data_type()) {
            return self.datafusion_udf.invoke_with_args(args);
        }
        let all_scalars = matches!(
            (array, value),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        let rows = if all_scalars { 1 } else { args.number_rows };
        let array = array.to_array(rows)?;
        let result = match value {
            ColumnarValue::Scalar(needle) => remove_constant(array.as_list::<i32>(), needle)?,
            ColumnarValue::Array(value) => array_remove(array.as_list::<i32>(), value)?,
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
    match array.value_type() {
        DataType::Float32 => Ok(remove_floats::<Float32Type>(array, value)),
        DataType::Float64 => Ok(remove_floats::<Float64Type>(array, value)),
        _ => {
            let equal = spark_equality(array.values().as_ref(), value.as_ref())?;
            remove_where(array, value, equal)
        }
    }
}

/// [`array_remove`] of the same value from every row, which a float array does in one pass over
/// all the values, as DataFusion does for its own `array_remove`.
fn remove_constant(array: &ListArray, needle: &ScalarValue) -> Result<ArrayRef> {
    match needle {
        ScalarValue::Float32(Some(needle)) => remove_float_constant::<Float32Type>(array, *needle),
        ScalarValue::Float64(Some(needle)) => remove_float_constant::<Float64Type>(array, *needle),
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
    let mut kept_offsets = Vec::with_capacity(array.len() + 1);
    kept_offsets.push(0i32);
    let mut kept = 0i32;
    for bounds in offsets.windows(2) {
        let start = bounds[0] as usize - first;
        let length = bounds[1] as usize - first - start;
        kept += keep
            .inner()
            .count_set_bits_offset(keep.offset() + start, length) as i32;
        kept_offsets.push(kept);
    }
    let kept_values = filter(&values, &BooleanArray::new(keep, None))?;
    Ok(with_values(
        array,
        kept_offsets,
        kept_values,
        array.nulls().cloned(),
    ))
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
    with_values(array, offsets, Arc::new(kept), row_nulls)
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
    Ok(with_values(array, offsets, kept_values, row_nulls))
}

/// A list with the type of `array` and the given nulls, holding `values` at `offsets`.
fn with_values(
    array: &ListArray,
    offsets: Vec<i32>,
    values: ArrayRef,
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    let DataType::List(field) = array.data_type() else {
        unreachable!("array_remove takes a List");
    };
    Arc::new(ListArray::new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Float64Array, Int32Array, StructArray};
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
            let row = result.as_list::<i32>().value(0);
            let x = row
                .as_struct()
                .column(0)
                .as_primitive::<Float64Type>()
                .clone();
            let actual: Vec<u64> = x.values().iter().map(|v| v.to_bits()).collect();
            let expected: Vec<u64> = expected.iter().map(|v| v.to_bits()).collect();
            assert_eq!(actual, expected, "remove {value}");
        }
        Ok(())
    }

    /// The planner asks the UDF for its return field, which DataFusion's version only answers
    /// through `return_field_from_args`.
    #[test]
    fn return_field() -> Result<()> {
        let udf = ScalarUDF::new_from_impl(SparkArrayRemove::default());
        let list = Arc::new(Field::new(
            "array",
            DataType::new_list(DataType::Float64, true),
            false,
        ));
        let value = Arc::new(Field::new("value", DataType::Float64, true));
        let field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&list), value],
            scalar_arguments: &[None, None],
        })?;
        assert_eq!(field.data_type(), list.data_type());
        assert!(field.is_nullable(), "a null value gives a null array");
        Ok(())
    }

    #[test]
    fn other_types_go_to_datafusion() -> Result<()> {
        let array = Arc::new(ListArray::from_iter_primitive::<
            arrow::datatypes::Int32Type,
            _,
            _,
        >(vec![Some(vec![Some(1), Some(2), Some(1)])]));
        let result = invoke(
            ColumnarValue::Array(array),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![1]))),
            1,
        )?
        .into_array(1)?;
        let row = result.as_list::<i32>().value(0);
        assert_eq!(
            row.as_primitive::<arrow::datatypes::Int32Type>()
                .values()
                .as_ref(),
            &[2]
        );
        Ok(())
    }
}
