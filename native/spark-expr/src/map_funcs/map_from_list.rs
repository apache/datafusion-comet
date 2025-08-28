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

use arrow::array::{Array, ArrayData, ArrayRef, ListArray, MapArray, StructArray};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Converts a ListArray of Structs of key-value pairs to a MapArray preserving the original layout.
/// One use case is to re-construct the original Map type after doing the group by using hash
/// aggregation on their canonicalized form.
pub fn map_from_list(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("map_from_list expects exactly one argument");
    }

    let arr_arg: ArrayRef = match &args[0] {
        ColumnarValue::Array(array) => Arc::<dyn Array>::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
    };

    let list_arg = match arr_arg.data_type() {
        DataType::List(_) => arr_arg.as_any().downcast_ref::<ListArray>().unwrap(),
        _ => return exec_err!("map_from_list expects ListArray type as argument"),
    };

    let list_struct_array = list_arg
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "map_from_list expects ListArray to contain StructArray values".to_string(),
            )
        })?;
    let list_struct_array_data = list_struct_array.to_data();

    let list_data = list_arg.to_data();

    let list_offset_buffer = if list_data.buffers().len() == 1 {
        list_data.buffers()[0].clone()
    } else {
        return exec_err!("map_from_list expects ListArray to have a single offset buffer");
    };

    // TODO: Do no hard code field name and nullability.
    // This create a field for the MapArray entries.
    let map_entries_field = Arc::new(arrow::datatypes::Field::new(
        "entries",
        list_struct_array_data.data_type().clone(),
        false,
    ));

    // Build a MapArray preserving the same layout as the ListArray.
    let mut map_builder = ArrayData::builder(DataType::Map(map_entries_field, false))
        .len(list_arg.len())
        .offset(list_arg.offset())
        .add_buffer(list_offset_buffer)
        .child_data(vec![list_struct_array_data]);

    // Copy the null bitmaps if they exist.
    if let Some(list_nulls) = list_data.nulls() {
        map_builder = map_builder.nulls(Some(list_nulls.clone()));
    }

    let map_data = map_builder.build()?;
    let map_array = Arc::new(MapArray::from(map_data));
    Ok(ColumnarValue::Array(map_array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_create_list_array, test_create_map_array, test_create_nested_list_array,
        test_create_nested_map_array, test_verify_result_equals_map,
    };
    use arrow::array::builder::{Int32Builder, MapBuilder, StringBuilder};
    use arrow::array::{Int32Array, MapArray, MapFieldNames, StringArray};
    use datafusion::common::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_map_from_list_basic() {
        let keys_arg = [
            vec![
                Some("b".to_string()),
                Some("a".to_string()),
                Some("c".to_string()),
            ],
            Vec::<Option<String>>::new(),
        ];
        let values_arg = [vec![Some(2), Some(1), Some(3)], Vec::<Option<i32>>::new()];
        let validity = [true, true];
        let list_input = test_create_list_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            keys_arg,
            values_arg,
            validity
        );
        let expected_map_arr = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            [
                vec![
                    Some("b".to_string()),
                    Some("a".to_string()),
                    Some("c".to_string()),
                ],
                Vec::<Option<String>>::new(),
            ],
            [vec![Some(2), Some(1), Some(3)], Vec::<Option<i32>>::new()],
            [true, true]
        );
        let args = vec![ColumnarValue::Array(Arc::new(list_input))];
        let result = map_from_list(&args).unwrap();

        test_verify_result_equals_map!(StringArray, Int32Array, result, expected_map_arr);
    }

    #[test]
    fn test_map_from_list_with_scalar_argument() {
        let list_input = test_create_list_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec![Some("b".to_string()), Some("a".to_string())]],
            vec![vec![Some(2), Some(1)]],
            vec![true]
        );
        let args = vec![ColumnarValue::Scalar(
            ScalarValue::try_from_array(&list_input, 0).unwrap(),
        )];
        let result = map_from_list(&args).unwrap();
        let expected_map_arr = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec![Some("b".to_string()), Some("a".to_string())]],
            vec![vec![Some(2), Some(1)]],
            vec![true]
        );
        test_verify_result_equals_map!(StringArray, Int32Array, result, expected_map_arr);
    }

    #[test]
    fn test_map_from_list_with_invalid_arguments() {
        let res = map_from_list(&[]);
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("map_from_list expects exactly one argument"));

        let list_input = test_create_list_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec![Some("a".to_string())]],
            vec![vec![Some(1)]],
            vec![true]
        );
        let args = vec![
            ColumnarValue::Array(Arc::new(list_input.clone())),
            ColumnarValue::Array(Arc::new(list_input)),
        ];
        let res = map_from_list(&args);
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("map_from_list expects exactly one argument"));

        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let res = map_from_list(&[ColumnarValue::Array(int_array)]);
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("map_from_list expects ListArray type as argument"));
    }

    #[test]
    fn test_map_from_list_with_nested_maps() {
        let outer_keys = [
            vec![Some("key_b_1".to_string()), Some("key_a_1".to_string())],
            vec![Some("key_b_2".to_string()), Some("key_a_2".to_string())],
        ];
        let inner_map_data = [
            vec![
                (
                    vec![
                        Some("key_b_1=key1".to_string()),
                        Some("key_b_1=key0".to_string()),
                    ],
                    vec![
                        Some("key_b_1=value1".to_string()),
                        Some("key_b_1=value0".to_string()),
                    ],
                    true,
                ),
                (
                    vec![
                        Some("key_a_1=key0".to_string()),
                        Some("key_a_1=key1".to_string()),
                    ],
                    vec![
                        Some("key_a_1=value0".to_string()),
                        Some("key_a_1=value1".to_string()),
                    ],
                    true,
                ),
            ],
            vec![
                (
                    vec![
                        Some("key_b_2=key1".to_string()),
                        Some("key_b_2=key0".to_string()),
                    ],
                    vec![
                        Some("key_b_2=value1".to_string()),
                        Some("key_b_2=value0".to_string()),
                    ],
                    true,
                ),
                (
                    vec![
                        Some("key_a_2=key1".to_string()),
                        Some("key_a_2=key0".to_string()),
                    ],
                    vec![
                        Some("key_a_2=value1".to_string()),
                        Some("key_a_2=value0".to_string()),
                    ],
                    true,
                ),
            ],
        ];
        let validity = [true, true];

        let expected_map_arr = test_create_nested_map_array!(
            StringBuilder::new(),
            MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".into(),
                    key: "key".into(),
                    value: "value".into(),
                }),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            outer_keys,
            inner_map_data,
            validity
        );
        let list_input = test_create_nested_list_array!(
            StringBuilder::new(),
            MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".into(),
                    key: "key".into(),
                    value: "value".into(),
                }),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            vec![
                vec![Some("key_b_1".to_string()), Some("key_a_1".to_string())],
                vec![Some("key_b_2".to_string()), Some("key_a_2".to_string())],
            ],
            vec![
                vec![
                    (
                        vec![
                            Some("key_b_1=key1".to_string()),
                            Some("key_b_1=key0".to_string()),
                        ],
                        vec![
                            Some("key_b_1=value1".to_string()),
                            Some("key_b_1=value0".to_string()),
                        ],
                        true,
                    ),
                    (
                        vec![
                            Some("key_a_1=key0".to_string()),
                            Some("key_a_1=key1".to_string()),
                        ],
                        vec![
                            Some("key_a_1=value0".to_string()),
                            Some("key_a_1=value1".to_string()),
                        ],
                        true,
                    ),
                ],
                vec![
                    (
                        vec![
                            Some("key_b_2=key1".to_string()),
                            Some("key_b_2=key0".to_string()),
                        ],
                        vec![
                            Some("key_b_2=value1".to_string()),
                            Some("key_b_2=value0".to_string()),
                        ],
                        true,
                    ),
                    (
                        vec![
                            Some("key_a_2=key1".to_string()),
                            Some("key_a_2=key0".to_string()),
                        ],
                        vec![
                            Some("key_a_2=value1".to_string()),
                            Some("key_a_2=value0".to_string()),
                        ],
                        true,
                    ),
                ],
            ],
            vec![true, true]
        );
        let args = vec![ColumnarValue::Array(Arc::new(list_input))];
        let result = map_from_list(&args).unwrap();
        test_verify_result_equals_map!(StringArray, MapArray, result, expected_map_arr);
    }
}
