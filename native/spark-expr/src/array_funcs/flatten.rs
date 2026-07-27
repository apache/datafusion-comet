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

// Spark-compatible flatten(array<array<T>>).
//
// DataFusion's flatten preserves the outer array null bitmap only. Spark returns NULL for a row
// when any sub-array inside that row is NULL, so Comet needs a Spark-specific null bitmap.

use arrow::array::{Array, ArrayRef, GenericListArray, NullBufferBuilder, OffsetSizeTrait};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{
    DataType,
    DataType::{FixedSizeList, LargeList, List, Null},
    Field, FieldRef,
};
use datafusion::common::cast::{as_large_list_array, as_list_array};
use datafusion::common::{exec_err, utils::take_function_args, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

use super::arrays_zip::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFlatten {
    signature: Signature,
}

impl Default for SparkFlatten {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkFlatten {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkFlatten {
    fn name(&self) -> &str {
        "flatten"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        spark_flatten_return_type(&arg_types[0])
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [arg_field] = take_function_args(self.name(), args.arg_fields)?;
        let data_type = spark_flatten_return_type(arg_field.data_type())?;
        let nullable = match arg_field.data_type() {
            List(field) | LargeList(field) => arg_field.is_nullable() || field.is_nullable(),
            Null => true,
            _ => {
                return exec_err!(
                    "Not reachable, data_type should be List, LargeList or FixedSizeList"
                )
            }
        };

        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_flatten_inner)(&args.args)
    }
}

fn spark_flatten_return_type(arg_type: &DataType) -> Result<DataType> {
    let data_type = match arg_type {
        List(field) => match field.data_type() {
            List(field) | FixedSizeList(field, _) => List(Arc::clone(field)),
            LargeList(field) => LargeList(Arc::clone(field)),
            _ => arg_type.clone(),
        },
        LargeList(field) => match field.data_type() {
            List(field) | LargeList(field) | FixedSizeList(field, _) => {
                LargeList(Arc::clone(field))
            }
            _ => arg_type.clone(),
        },
        Null => Null,
        _ => exec_err!("Not reachable, data_type should be List, LargeList or FixedSizeList")?,
    };

    Ok(data_type)
}

fn spark_flatten_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("flatten", args)?;

    match array.data_type() {
        List(_) => {
            let outer = as_list_array(array)?;
            let (_field, offsets, values, _outer_nulls) = outer.clone().into_parts();
            let values = cast_fsl_to_list(values)?;

            match values.data_type() {
                List(_) => {
                    let inner = as_list_array(&values)?;
                    let nulls = spark_flatten_nulls(outer, inner);
                    let (inner_field, inner_offsets, inner_values, _) = inner.clone().into_parts();
                    let offsets = get_offsets_for_flatten::<i32, i32>(inner_offsets, &offsets);
                    let flattened_array =
                        GenericListArray::<i32>::new(inner_field, offsets, inner_values, nulls);

                    Ok(Arc::new(flattened_array) as ArrayRef)
                }
                LargeList(_) => {
                    let inner = as_large_list_array(&values)?;
                    let nulls = spark_flatten_nulls(outer, inner);
                    let (inner_field, inner_offsets, inner_values, _) = inner.clone().into_parts();
                    let offsets = get_offsets_for_flatten::<i64, i32>(inner_offsets, &offsets);
                    let flattened_array =
                        GenericListArray::<i64>::new(inner_field, offsets, inner_values, nulls);

                    Ok(Arc::new(flattened_array) as ArrayRef)
                }
                _ => Ok(Arc::clone(array) as ArrayRef),
            }
        }
        LargeList(_) => {
            let outer = as_large_list_array(array)?;
            let (_field, offsets, values, _outer_nulls) = outer.clone().into_parts();
            let values = cast_fsl_to_list(values)?;

            match values.data_type() {
                List(_) => {
                    let inner = as_list_array(&values)?;
                    let nulls = spark_flatten_nulls(outer, inner);
                    let (inner_field, inner_offsets, inner_values, _) = inner.clone().into_parts();
                    let offsets = get_large_offsets_for_flatten(inner_offsets, &offsets);
                    let flattened_array =
                        GenericListArray::<i64>::new(inner_field, offsets, inner_values, nulls);

                    Ok(Arc::new(flattened_array) as ArrayRef)
                }
                LargeList(_) => {
                    let inner = as_large_list_array(&values)?;
                    let nulls = spark_flatten_nulls(outer, inner);
                    let (inner_field, inner_offsets, inner_values, _) = inner.clone().into_parts();
                    let offsets = get_offsets_for_flatten::<i64, i64>(inner_offsets, &offsets);
                    let flattened_array =
                        GenericListArray::<i64>::new(inner_field, offsets, inner_values, nulls);

                    Ok(Arc::new(flattened_array) as ArrayRef)
                }
                _ => Ok(Arc::clone(array) as ArrayRef),
            }
        }
        Null => Ok(Arc::clone(array)),
        _ => {
            exec_err!("flatten does not support type '{}'", array.data_type())
        }
    }
}

fn spark_flatten_nulls<O: OffsetSizeTrait, P: OffsetSizeTrait>(
    outer: &GenericListArray<P>,
    inner: &GenericListArray<O>,
) -> Option<NullBuffer> {
    let mut nulls = NullBufferBuilder::new(outer.len());
    let inner_nulls = inner.nulls();

    for (row, offset_window) in outer.offsets().windows(2).enumerate() {
        if outer.is_null(row) {
            nulls.append_null();
            continue;
        }

        let start = offset_window[0].to_usize().unwrap();
        let end = offset_window[1].to_usize().unwrap();
        let has_null_subarray =
            inner_nulls.is_some_and(|n| (start..end).any(|inner_row| n.is_null(inner_row)));

        if has_null_subarray {
            nulls.append_null();
        } else {
            nulls.append_non_null();
        }
    }

    nulls.finish()
}

fn get_offsets_for_flatten<O: OffsetSizeTrait, P: OffsetSizeTrait>(
    inner_offsets: OffsetBuffer<O>,
    outer_offsets: &OffsetBuffer<P>,
) -> OffsetBuffer<O> {
    let buffer = inner_offsets.into_inner();
    let offsets: Vec<O> = outer_offsets
        .iter()
        .map(|i| buffer[i.to_usize().unwrap()])
        .collect();
    OffsetBuffer::new(offsets.into())
}

fn get_large_offsets_for_flatten<O: OffsetSizeTrait, P: OffsetSizeTrait>(
    inner_offsets: OffsetBuffer<O>,
    outer_offsets: &OffsetBuffer<P>,
) -> OffsetBuffer<i64> {
    let buffer = inner_offsets.into_inner();
    let offsets: Vec<i64> = outer_offsets
        .iter()
        .map(|i| buffer[i.to_usize().unwrap()].to_i64().unwrap())
        .collect();
    OffsetBuffer::new(offsets.into())
}

fn cast_fsl_to_list(array: ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        FixedSizeList(field, _) => Ok(arrow::compute::cast(&array, &List(Arc::clone(field)))?),
        _ => Ok(array),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int32Builder, ListArray, ListBuilder};
    use datafusion::common::ScalarValue;

    type IntElement = Option<i32>;
    type InnerArray = Option<Vec<IntElement>>;
    type OuterArray = Option<Vec<InnerArray>>;

    #[test]
    fn test_flatten_null_subarray_returns_null_row() {
        let input = nested_int_list_array(vec![
            Some(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(5)]),
            ]),
            Some(vec![Some(vec![Some(1)]), None]),
            Some(vec![None, None]),
        ]);

        let result = spark_flatten_inner(&[Arc::new(input)]).unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(result.len(), 3);
        assert_list_value(result, 0, &[Some(1), Some(2), Some(3), Some(4), Some(5)]);
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }

    #[test]
    fn test_flatten_preserves_null_elements() {
        let input = nested_int_list_array(vec![Some(vec![
            Some(vec![Some(1), None]),
            Some(vec![None, Some(2)]),
        ])]);

        let result = spark_flatten_inner(&[Arc::new(input)]).unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(result.len(), 1);
        assert_list_value(result, 0, &[Some(1), None, None, Some(2)]);
    }

    #[test]
    fn test_flatten_allows_scalar_input() {
        let input = nested_int_list_array(vec![Some(vec![Some(vec![Some(1), Some(2)])])]);
        let scalar = ColumnarValue::Scalar(ScalarValue::try_from_array(&input, 0).unwrap());

        let result = SparkFlatten::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![scalar],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(arrow::datatypes::Field::new("flatten", Null, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .unwrap();

        assert!(matches!(result, ColumnarValue::Scalar(_)));
    }

    fn nested_int_list_array(rows: Vec<OuterArray>) -> ListArray {
        let inner_builder = ListBuilder::new(Int32Builder::new());
        let mut outer_builder = ListBuilder::new(inner_builder);

        for row in rows {
            match row {
                Some(subarrays) => {
                    let inner_builder = outer_builder.values();
                    for subarray in subarrays {
                        match subarray {
                            Some(values) => {
                                for value in values {
                                    match value {
                                        Some(value) => inner_builder.values().append_value(value),
                                        None => inner_builder.values().append_null(),
                                    }
                                }
                                inner_builder.append(true);
                            }
                            None => inner_builder.append(false),
                        }
                    }
                    outer_builder.append(true);
                }
                None => outer_builder.append(false),
            }
        }

        outer_builder.finish()
    }

    fn assert_list_value(list_array: &ListArray, row: usize, expected: &[Option<i32>]) {
        let values = list_array.value(row);
        let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        let actual = values.iter().collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_return_type() {
        let input_type = List(Arc::new(arrow::datatypes::Field::new_list_field(
            List(Arc::new(arrow::datatypes::Field::new_list_field(
                DataType::Int32,
                true,
            ))),
            true,
        )));
        assert_eq!(
            SparkFlatten::new().return_type(&[input_type]).unwrap(),
            List(Arc::new(arrow::datatypes::Field::new_list_field(
                DataType::Int32,
                true,
            )))
        );
    }

    #[test]
    fn test_return_field_nullability_matches_spark() {
        let array_of_arrays = |outer_nullable, contains_null_subarray| {
            let element_field = Arc::new(arrow::datatypes::Field::new_list_field(
                DataType::Int32,
                true,
            ));
            Arc::new(arrow::datatypes::Field::new(
                "arg",
                List(Arc::new(arrow::datatypes::Field::new_list_field(
                    List(element_field),
                    contains_null_subarray,
                ))),
                outer_nullable,
            ))
        };

        let scalar_args: [Option<&ScalarValue>; 1] = [None];
        let arg_fields = [array_of_arrays(false, false)];
        let field = SparkFlatten::new()
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_args,
            })
            .unwrap();
        assert!(!field.is_nullable());

        let arg_fields = [array_of_arrays(false, true)];
        let field = SparkFlatten::new()
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_args,
            })
            .unwrap();
        assert!(field.is_nullable());

        let arg_fields = [array_of_arrays(true, false)];
        let field = SparkFlatten::new()
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_args,
            })
            .unwrap();
        assert!(field.is_nullable());
    }
}
