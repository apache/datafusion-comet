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

use arrow::array::{Array, ArrayData, ArrayRef, ListArray, MapArray};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Converts a MapArray to a ListArray of Structs of key-value pairs preserving the original layout.
/// This method can be used to canonicalize map representations.
/// One use case is to wrap the Map type with this function before doing the grouping.
pub fn map_to_list(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("map_to_list expects exactly one argument");
    }

    let arr_arg: ArrayRef = match &args[0] {
        ColumnarValue::Array(array) => Arc::<dyn Array>::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
    };

    // TODO: Add `is_sorted` field to the metadata.
    let (maps_arg, map_field) = match arr_arg.data_type() {
        DataType::Map(field, _) => (arr_arg.as_any().downcast_ref::<MapArray>().unwrap(), field),
        _ => return exec_err!("map_to_list expects MapArray type as argument"),
    };

    let maps_data = maps_arg.to_data();

    // A Map only has a single top-level buffer which is the offset buffer.
    let offset_buffer = maps_data.buffers()[0].clone();

    // These are the entries of the map, which is a StructArray of key-value pairs.
    let maps_entries = maps_arg.entries();
    let map_entries_data = maps_entries.to_data();

    // Build a ListArray preserving the same layout as the MapArray.
    let mut list_builder = ArrayData::builder(DataType::List(
        Arc::<arrow::datatypes::Field>::clone(map_field),
    ))
    .len(maps_arg.len())
    .offset(maps_arg.offset())
    .add_buffer(offset_buffer)
    .child_data(vec![map_entries_data]);

    // Copy the null bitmaps they exist.
    if let Some(maps_nulls) = maps_data.nulls() {
        list_builder = list_builder.nulls(Some(maps_nulls.clone()));
    }

    let list_data = list_builder.build()?;
    let list_array = Arc::new(ListArray::from(list_data));
    Ok(ColumnarValue::Array(list_array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test_create_map_array, test_create_nested_map_array};
    use arrow::array::builder::{Int32Builder, MapBuilder, StringBuilder};
    use arrow::array::{Int32Array, ListArray, MapArray, MapFieldNames, StringArray, StructArray};
    use datafusion::common::ScalarValue;
    use std::sync::Arc;

    macro_rules! verify_result {
        ($key_type:ty, $value_type:ty, $result:expr, $expected_map_arr:expr) => {{
            match $result {
                ColumnarValue::Array(actual_arr) => {
                    let actual_list = actual_arr.as_any().downcast_ref::<ListArray>().unwrap();

                    verify_metadata!(actual_list, $expected_map_arr);

                    let actual_entries = actual_list
                        .values()
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap();
                    let expected_entries = $expected_map_arr.entries();
                    verify_entries!($key_type, $value_type, actual_entries, expected_entries);
                }
                unexpected => {
                    panic!("Actual result: {unexpected:?} is not an Array ColumnarValue")
                }
            }
        }};
        ($outer_key_type:ty, $inner_key_type:ty, $inner_value_type:ty, $result:expr, $expected_map_arr:expr) => {{
            match $result {
                ColumnarValue::Array(actual_arr) => {
                    let list_arr = actual_arr.as_any().downcast_ref::<ListArray>().unwrap();

                    verify_metadata!(list_arr, $expected_map_arr);

                    let actual_entries = list_arr
                        .values()
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap();
                    let expected_entries = $expected_map_arr.entries();

                    let actual_keys = actual_entries
                        .column(0)
                        .as_any()
                        .downcast_ref::<$outer_key_type>()
                        .unwrap();
                    let expected_keys = expected_entries
                        .column(0)
                        .as_any()
                        .downcast_ref::<$outer_key_type>()
                        .unwrap();

                    assert_eq!(actual_keys.len(), expected_keys.len());

                    for idx in 0..actual_entries.len() {
                        assert_eq!(actual_keys.is_null(idx), expected_keys.is_null(idx));
                        if !actual_keys.is_null(idx) {
                            assert_eq!(actual_keys.value(idx), expected_keys.value(idx));
                        }
                    }

                    let actual_map_values = actual_entries
                        .column(1)
                        .as_any()
                        .downcast_ref::<MapArray>()
                        .unwrap();
                    let expected_map_values = expected_entries
                        .column(1)
                        .as_any()
                        .downcast_ref::<MapArray>()
                        .unwrap();

                    assert_eq!(actual_map_values.len(), expected_map_values.len());

                    let actual_map_offsets: Vec<i32> =
                        actual_map_values.offsets().iter().copied().collect();
                    let expected_map_offsets: Vec<i32> =
                        expected_map_values.offsets().iter().copied().collect();

                    assert_eq!(actual_map_offsets, expected_map_offsets);
                    assert_eq!(actual_map_values.nulls(), expected_map_values.nulls());

                    verify_entries!(
                        $inner_key_type,
                        $inner_value_type,
                        actual_map_values.entries(),
                        expected_map_values.entries()
                    );
                }
                unexpected => {
                    panic!("Actual result: {unexpected:?} is not an Array ColumnarValue")
                }
            }
        }};
    }

    macro_rules! verify_metadata {
        ($list_arr:ident, $map_arr:expr) => {{
            assert_eq!($list_arr.len(), $map_arr.len());
            assert_eq!($list_arr.offset(), $map_arr.offset());

            let actual_offsets: Vec<i32> = $list_arr.offsets().iter().copied().collect();
            let expected_offsets: Vec<i32> = $map_arr.offsets().iter().copied().collect();

            assert_eq!(actual_offsets, expected_offsets);
            assert_eq!($list_arr.nulls(), $map_arr.nulls());
        }};
    }

    macro_rules! verify_entries {
        ($key_type:ty, $value_type:ty, $actual_entries:expr, $expected_entries:expr) => {
            assert_eq!($actual_entries.data_type(), $expected_entries.data_type());
            assert_eq!($actual_entries.len(), $expected_entries.len());

            let actual_keys = $actual_entries
                .column(0)
                .as_any()
                .downcast_ref::<$key_type>()
                .unwrap();
            let expected_keys = $expected_entries
                .column(0)
                .as_any()
                .downcast_ref::<$key_type>()
                .unwrap();

            let actual_values = $actual_entries
                .column(1)
                .as_any()
                .downcast_ref::<$value_type>()
                .unwrap();
            let expected_values = $expected_entries
                .column(1)
                .as_any()
                .downcast_ref::<$value_type>()
                .unwrap();

            for idx in 0..$actual_entries.len() {
                assert_eq!(actual_keys.is_null(idx), expected_keys.is_null(idx));
                if !actual_keys.is_null(idx) {
                    assert_eq!(actual_keys.value(idx), expected_keys.value(idx));
                }

                assert_eq!(actual_values.is_null(idx), expected_values.is_null(idx));
                if !actual_values.is_null(idx) {
                    assert_eq!(actual_values.value(idx), expected_values.value(idx));
                }
            }
        };
    }

    #[test]
    fn test_map_to_list_basic() {
        let keys_arg = [
            vec![
                Some("b".to_string()),
                Some("a".to_string()),
                Some("c".to_string()),
            ],
            Vec::<Option<String>>::new(),
        ];
        let values_arg = [vec![Some(2), Some(1), Some(3)], Vec::<Option<i32>>::new()];
        let validity = [true, false];

        let map_arr_arg = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            keys_arg,
            values_arg,
            validity
        );

        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg.clone()))];
        let result = map_to_list(&args).unwrap();

        verify_result!(StringArray, Int32Array, result, map_arr_arg);
    }

    #[test]
    fn test_map_to_list_with_scalar_argument() {
        let map_arr = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec![Some("b".to_string()), Some("a".to_string())]],
            vec![vec![Some(2), Some(1)]],
            vec![true]
        );

        let args = vec![ColumnarValue::Scalar(
            ScalarValue::try_from_array(&map_arr, 0).unwrap(),
        )];

        let result = map_to_list(&args).unwrap();
        verify_result!(StringArray, Int32Array, result, map_arr);
    }

    #[test]
    fn test_map_to_list_with_invalid_arguments() {
        let res = map_to_list(&[]);
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("map_to_list expects exactly one argument"));

        let map_arr_arg = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec![Some("a".to_string())]],
            vec![vec![Some(1)]],
            vec![true]
        );
        let args = vec![
            ColumnarValue::Array(Arc::new(map_arr_arg.clone())),
            ColumnarValue::Array(Arc::new(map_arr_arg)),
        ];
        let res = map_to_list(&args);
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("map_to_list expects exactly one argument"));

        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let res = map_to_list(&[ColumnarValue::Array(int_array)]);
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("map_to_list expects MapArray type as argument"));
    }

    #[test]
    fn test_map_to_list_with_nested_maps() {
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
                    vec![Some("key_b_1=value1"), Some("key_b_1=value0")],
                    true,
                ),
                (
                    vec![
                        Some("key_a_1=key0".to_string()),
                        Some("key_a_1=key1".to_string()),
                    ],
                    vec![Some("key_a_1=value0"), Some("key_a_1=value1")],
                    true,
                ),
            ],
            vec![
                (
                    vec![
                        Some("key_b_2=key1".to_string()),
                        Some("key_b_2=key0".to_string()),
                    ],
                    vec![Some("key_b_2=value1"), Some("key_b_2=value0")],
                    true,
                ),
                (
                    vec![
                        Some("key_a_2=key1".to_string()),
                        Some("key_a_2=key0".to_string()),
                    ],
                    vec![Some("key_a_2=value1"), Some("key_a_2=value0")],
                    true,
                ),
            ],
        ];
        let validity = [true, true];

        let map_arr = test_create_nested_map_array!(
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

        let args = vec![ColumnarValue::Array(Arc::new(map_arr.clone()))];
        let result = map_to_list(&args).unwrap();
        verify_result!(StringArray, StringArray, StringArray, result, map_arr);
    }
}
