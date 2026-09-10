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

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, utils::take_function_args, Result};
use datafusion::functions_nested::map::MapFunc;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};

/// Checks row boundaries before the DataFusion `map` used by CometMapFromArrays.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct SparkMapFromArrays {
    inner: MapFunc,
}

impl ScalarUDFImpl for SparkMapFromArrays {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let has_array = args
            .args
            .iter()
            .any(|arg| matches!(arg, ColumnarValue::Array(_)));
        if has_array {
            // MapFunc checks the flattened lengths, which can match even when individual
            // rows have different lengths. Expand scalars to validate both mixed and batched
            // operands, retaining the upstream scalar-only path and constructor behavior.
            let number_rows = args.number_rows;
            let arrays = args
                .args
                .into_iter()
                .map(|arg| arg.into_array_of_size(number_rows))
                .collect::<Result<Vec<_>>>()?;
            let [keys, values] = take_function_args(self.name(), arrays.as_slice())?;
            validate_list_lengths(keys, values)?;
            args.args = arrays.into_iter().map(ColumnarValue::Array).collect();
        }
        self.inner.invoke_with_args(args)
    }
}

fn validate_list_lengths(keys: &ArrayRef, values: &ArrayRef) -> Result<()> {
    // The upstream array path uses key offsets for both flattened children without checking
    // each row's lengths. Do not let batched operands silently pair across map rows.
    // CometMapFromArrays supplies the null-intolerant CASE guard around the constructor.
    for row in 0..keys.len() {
        if keys.is_valid(row)
            && values.is_valid(row)
            && list_length(keys, row)? != list_length(values, row)?
        {
            return exec_err!("map requires key and value lists to have the same length");
        }
    }
    Ok(())
}

fn list_length(array: &ArrayRef, row: usize) -> Result<i64> {
    match array.data_type() {
        DataType::List(_) => Ok(i64::from(array.as_list::<i32>().value_length(row))),
        DataType::LargeList(_) => Ok(array.as_list::<i64>().value_length(row)),
        DataType::FixedSizeList(_, length) => Ok(i64::from(*length)),
        data_type => exec_err!("Expected List, LargeList, or FixedSizeList, got {data_type:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{new_empty_array, Int32Array, ListArray, NullArray};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion::common::{utils::SingleRowListArrayBuilder, ScalarValue};
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    fn scalar_list(values: ArrayRef) -> ColumnarValue {
        ColumnarValue::Scalar(SingleRowListArrayBuilder::new(values).build_list_scalar())
    }

    fn lists(rows: Vec<Option<Vec<Option<i32>>>>) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(rows))
    }

    fn invoke(
        udf: &dyn ScalarUDFImpl,
        args: Vec<ColumnarValue>,
        number_rows: usize,
    ) -> Result<ColumnarValue> {
        let arg_types = args
            .iter()
            .map(ColumnarValue::data_type)
            .collect::<Vec<_>>();
        let return_field = Arc::new(Field::new("map", udf.return_type(&arg_types)?, true));
        let arg_fields = arg_types
            .into_iter()
            .map(|data_type| Arc::new(Field::new("arg", data_type, true)))
            .collect();
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn assert_same_result(actual: ColumnarValue, expected: ColumnarValue, rows: usize) {
        assert_eq!(
            matches!(&actual, ColumnarValue::Scalar(_)),
            matches!(&expected, ColumnarValue::Scalar(_))
        );
        assert_eq!(
            actual.into_array_of_size(rows).unwrap().to_data(),
            expected.into_array_of_size(rows).unwrap().to_data()
        );
    }

    #[test]
    fn mixed_inputs_broadcast_empty_and_nonempty_lists() {
        let cases: Vec<(ArrayRef, ArrayRef)> = vec![
            (
                new_empty_array(&DataType::Null),
                new_empty_array(&DataType::Null),
            ),
            (
                new_empty_array(&DataType::Int32),
                new_empty_array(&DataType::Int32),
            ),
            (
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![Some(10), None])),
            ),
            (
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(NullArray::new(2)),
            ),
        ];
        for (keys, values) in cases {
            let keys = scalar_list(keys);
            let values = scalar_list(values);
            for rows in [0, 1, 3] {
                for scalar_keys in [false, true] {
                    let key_array = keys.to_array_of_size(rows).unwrap();
                    let value_array = values.to_array_of_size(rows).unwrap();
                    let args = if scalar_keys {
                        vec![keys.clone(), ColumnarValue::Array(Arc::clone(&value_array))]
                    } else {
                        vec![ColumnarValue::Array(Arc::clone(&key_array)), values.clone()]
                    };
                    let actual = invoke(&SparkMapFromArrays::default(), args, rows).unwrap();
                    let expected = invoke(
                        &MapFunc::default(),
                        vec![
                            ColumnarValue::Array(key_array),
                            ColumnarValue::Array(value_array),
                        ],
                        rows,
                    )
                    .unwrap();
                    assert_same_result(actual, expected, rows);
                }
            }
        }
    }

    #[test]
    fn homogeneous_inputs_preserve_upstream_paths() {
        let scalar_args = vec![
            scalar_list(Arc::new(Int32Array::from(vec![1]))),
            scalar_list(Arc::new(Int32Array::from(vec![Some(10)]))),
        ];
        let array_args = vec![
            ColumnarValue::Array(lists(vec![
                Some(vec![Some(1)]),
                None,
                Some(vec![Some(2), Some(3)]),
            ])),
            ColumnarValue::Array(lists(vec![
                Some(vec![Some(10)]),
                Some(vec![]),
                Some(vec![None, Some(30)]),
            ])),
        ];
        for args in [scalar_args, array_args] {
            let actual = invoke(&SparkMapFromArrays::default(), args.clone(), 3).unwrap();
            let expected = invoke(&MapFunc::default(), args, 3).unwrap();
            assert_same_result(actual, expected, 3);
        }
    }

    #[test]
    fn batched_inputs_reject_per_row_length_mismatch() {
        // Both flattened children have four elements after broadcasting, so comparing only
        // total lengths would allow values from one row to leak into the next map.
        let batch = ColumnarValue::Array(lists(vec![
            Some(vec![Some(1)]),
            Some(vec![Some(2), Some(3), Some(4)]),
        ]));
        let scalar = scalar_list(Arc::new(Int32Array::from(vec![10, 20])));
        let array = ColumnarValue::Array(scalar.to_array_of_size(2).unwrap());
        for args in [
            vec![batch.clone(), scalar.clone()],
            vec![scalar, batch.clone()],
            vec![batch.clone(), array.clone()],
            vec![array, batch],
        ] {
            let err = invoke(&SparkMapFromArrays::default(), args, 2).unwrap_err();
            assert!(err.to_string().contains("same length"), "{err}");
        }
    }

    #[test]
    fn sliced_inputs_check_only_visible_rows() {
        let keys = lists(vec![
            Some(vec![Some(0)]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
        ]);
        let values = lists(vec![
            Some(vec![Some(0), Some(0)]),
            Some(vec![Some(10), None]),
            Some(vec![Some(30), Some(40)]),
        ]);
        // The excluded first rows have different lengths and leave different starting offsets.
        let actual = invoke(
            &SparkMapFromArrays::default(),
            vec![
                ColumnarValue::Array(keys.slice(1, 2)),
                ColumnarValue::Array(values.slice(1, 2)),
            ],
            2,
        )
        .unwrap()
        .into_array_of_size(2)
        .unwrap();
        let map = actual.as_map();
        assert_eq!(map.value_offsets(), &[0, 2, 4]);
        assert_eq!(
            map.values().as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(10), None, Some(30), Some(40)])
        );
    }

    #[test]
    fn batched_inputs_reject_wrong_batch_length() {
        let batch = ColumnarValue::Array(lists(vec![Some(vec![Some(1)])]));
        let scalar = scalar_list(Arc::new(Int32Array::from(vec![10])));
        let array = ColumnarValue::Array(scalar.to_array_of_size(2).unwrap());
        for args in [
            vec![batch.clone(), scalar.clone()],
            vec![scalar, batch.clone()],
            vec![batch.clone(), array.clone()],
            vec![array, batch.clone()],
            vec![batch.clone(), batch],
        ] {
            let err = invoke(&SparkMapFromArrays::default(), args, 2).unwrap_err();
            assert!(err.to_string().contains("expected length 2"), "{err}");
        }
    }

    #[test]
    fn mixed_inputs_preserve_null_maps() {
        let keys = lists(vec![None, Some(vec![Some(1)]), None]);
        let values = scalar_list(Arc::new(Int32Array::from(vec![Some(10)])));
        let expected = invoke(
            &MapFunc::default(),
            vec![
                ColumnarValue::Array(Arc::clone(&keys)),
                ColumnarValue::Array(values.to_array_of_size(3).unwrap()),
            ],
            3,
        )
        .unwrap();
        let actual = invoke(
            &SparkMapFromArrays::default(),
            vec![ColumnarValue::Array(keys), values],
            3,
        )
        .unwrap();
        assert_same_result(actual, expected, 3);

        // Spark's null-intolerant CASE skips construction when either list is null. The
        // length check must not introduce an error for offsets hidden by a null list row.
        let nulls = lists(vec![None]);
        let nonempty = lists(vec![Some(vec![Some(1)])]);
        validate_list_lengths(&nulls, &nonempty).unwrap();
        validate_list_lengths(&nonempty, &nulls).unwrap();

        let null_keys =
            ColumnarValue::Scalar(ScalarValue::try_from_array(nulls.as_ref(), 0).unwrap());
        let actual = invoke(
            &SparkMapFromArrays::default(),
            vec![null_keys, ColumnarValue::Array(nonempty)],
            1,
        )
        .unwrap()
        .into_array_of_size(1)
        .unwrap();
        assert!(actual.is_null(0));
    }
}
