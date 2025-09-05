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

use crate::map_funcs::COMET_MAP_TO_LIST_FIELD_METADATA_IS_SORTED_KEY;
use arrow::array::{Array, ArrayData, ArrayRef, ListArray, MapArray};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Converts a MapArray to a ListArray of Structs of key-value pairs.
/// It preserves the physical layout, ordering of each map in the array and any metadata associated
/// with the field of the map, such that the original map can be reconstructed back.
///
/// Eg. if the input MapArray with key field named "key" and value field named "value" is:
/// ```text
/// [
///     {"c": 3, "a": 1, "b": 2}
///     {["b", "a", "c"]: 2, ["a", "b", "c"]: 1, ["c", "b", "a"]: 3}
/// ]
/// ```
/// The output ListArray will be:
/// ```text
/// [
///     [
///         {"key": "c", "value": 3},
///         {"key": "a", "value": 1},
///         {"key": "b", "value": 2}
///     ]
///     [
///         {"key": ["b", "a", "c"], "value": 2},
///         {"key": ["a", "b", "c"], "value": 1},
///         {"key": ["c", "b", "a"], "value": 3}
///     ]
/// ]
///
pub fn map_to_list(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("map_to_list expects exactly one argument");
    }

    let arr_arg: ArrayRef = match &args[0] {
        ColumnarValue::Array(array) => Arc::<dyn Array>::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
    };

    let (maps_arg, map_field, map_is_sorted) = match arr_arg.data_type() {
        DataType::Map(field, is_sorted) => (
            arr_arg.as_any().downcast_ref::<MapArray>().unwrap(),
            field,
            is_sorted,
        ),
        _ => return exec_err!("map_to_list expects Map type as argument"),
    };

    // Create the field reference for ListArray, adding map specific metadata. The map specific
    // metadata can be fetch to reconstruct the original map.
    let list_field = {
        let mut field_metadata = map_field.metadata().clone();
        field_metadata.insert(
            COMET_MAP_TO_LIST_FIELD_METADATA_IS_SORTED_KEY.into(),
            map_is_sorted.to_string(),
        );
        (**map_field).clone().with_metadata(field_metadata)
    };

    let maps_data = maps_arg.to_data();

    // A Map only has a single top-level buffer which is the offset buffer.
    let offset_buffer = maps_data.buffers()[0].clone();

    // These are the entries of the map, which is a StructArray of key-value pairs.
    let maps_entries = maps_arg.entries();
    let map_entries_data = maps_entries.to_data();

    // Build a ListArray preserving the same layout as the MapArray.
    let mut list_builder = ArrayData::builder(DataType::List(Arc::new(list_field)))
        .len(maps_arg.len())
        .offset(maps_arg.offset())
        .add_buffer(offset_buffer)
        .child_data(vec![map_entries_data]);

    // Copy the null bitmaps that exist.
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
    use arrow::array::builder::{Int32Builder, MapBuilder, StringBuilder};
    use arrow::array::{
        Int32Array, ListArray, ListBuilder, MapArray, MapFieldNames, StringArray, StructArray,
    };

    use arrow::array::Array;
    use datafusion::common::ScalarValue;
    use std::collections::HashMap;
    use std::sync::Arc;

    macro_rules! build_map {
        (
            $key_builder:expr,
            $value_builder:expr,
            $keys:expr,
            $values:expr,
            $validity:expr,
            $entries_builder_fn:ident
        ) => {{
            let mut map_builder = MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".into(),
                    key: "key".into(),
                    value: "value".into(),
                }),
                $key_builder,
                $value_builder,
            );

            assert_eq!($keys.len(), $values.len());
            assert_eq!($keys.len(), $validity.len());

            let total_maps = $keys.len();
            for map_idx in 0..total_maps {
                let map_keys = &$keys[map_idx];
                let map_values = &$values[map_idx];
                assert_eq!(map_keys.len(), map_values.len());

                let map_entries = map_keys.len();
                for entry_idx in 0..map_entries {
                    let map_key = &map_keys[entry_idx];
                    let map_value = &map_values[entry_idx];
                    $entries_builder_fn!(map_builder, map_key, map_value);
                }

                let is_valid = $validity[map_idx];
                map_builder.append(is_valid).unwrap();
            }

            map_builder.finish()
        }};
    }

    macro_rules! default_map_entries_builder {
        ($map_builder:expr, $key:expr, $value:expr) => {{
            $map_builder.keys().append_value($key.clone());
            $map_builder.values().append_value($value.clone().unwrap());
        }};
    }

    macro_rules! nested_map_entries_builder {
        ($map_builder:expr, $key:expr, $value:expr) => {{
            $map_builder.keys().append_value($key.clone());

            let inner_map_builder = $map_builder.values();

            let (inner_keys, inner_values, inner_valid) = $value;
            assert_eq!(inner_keys.len(), inner_values.len());

            let inner_entries = inner_keys.len();
            for inner_idx in 0..inner_entries {
                let inner_key_val = &inner_keys[inner_idx];
                let inner_value = &inner_values[inner_idx];
                default_map_entries_builder!(inner_map_builder, inner_key_val, inner_value);
            }

            inner_map_builder.append(*inner_valid).unwrap();
        }};
    }

    macro_rules! verify_result {
        (
            $key_type:ty,
            $value_type:ty,
            $result:expr,
            $expected_map_arr:expr,
            $verify_entries_fn:ident
        ) => {{
            match $result {
                ColumnarValue::Array(actual_arr) => {
                    match (actual_arr.data_type(), $expected_map_arr.data_type()) {
                        (
                            DataType::List(actual_field_ref),
                            DataType::Map(expected_field_ref, expected_is_sorted),
                        ) => {
                            let actual_metadata = actual_field_ref.metadata();
                            assert_eq!(actual_metadata.len(), 1, "Unexpected metadata length");

                            // Verify that actual field metadata contains sorted flag.
                            match actual_metadata
                                .get(COMET_MAP_TO_LIST_FIELD_METADATA_IS_SORTED_KEY)
                            {
                                Some(actual_is_sorted) => assert_eq!(
                                    actual_is_sorted.clone(),
                                    expected_is_sorted.to_string(),
                                    "Unexpected sorted flag of the result ListArray"
                                ),
                                _ => panic!("Metadata does not have sorted flag"),
                            }

                            let actual_field_ref_without_metadata =
                                (**actual_field_ref).clone().with_metadata(HashMap::new());
                            let expected_field_ref_without_metadata =
                                (**expected_field_ref).clone().with_metadata(HashMap::new());
                            assert_eq!(
                                actual_field_ref_without_metadata,
                                expected_field_ref_without_metadata,
                                "Mismatched field of the result ListArray"
                            );
                        }
                        _ => panic!("Actual result data type is not List"),
                    }

                    let actual_list = actual_arr.as_any().downcast_ref::<ListArray>().unwrap();

                    assert_eq!(actual_list.len(), $expected_map_arr.len());
                    assert_eq!(actual_list.offset(), $expected_map_arr.offset());

                    let actual_list_data = actual_list.to_data();
                    let expected_map_data = $expected_map_arr.to_data();

                    // Verify that the underlying offset buffer is shared and not copied.
                    assert_eq!(
                        actual_list_data.buffers().len(),
                        expected_map_data.buffers().len(),
                        "Mismatched data buffer length in result"
                    );
                    assert_eq!(
                        actual_list_data.buffers()[0].as_ptr(),
                        expected_map_data.buffers()[0].as_ptr(),
                        "Offsets buffers are not shared"
                    );

                    let actual_offsets: Vec<i32> = actual_list.offsets().iter().copied().collect();
                    let expected_offsets: Vec<i32> =
                        $expected_map_arr.offsets().iter().copied().collect();

                    assert_eq!(actual_offsets, expected_offsets);
                    assert_eq!(actual_list.nulls(), $expected_map_arr.nulls());

                    let actual_entries = actual_list
                        .values()
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap();
                    let expected_entries = $expected_map_arr.entries();

                    assert_eq!(
                        actual_list_data.child_data().len(),
                        1,
                        "Unexpected child data buffers in result"
                    );

                    let actual_struct_data = actual_list_data.child_data()[0].clone();
                    assert_eq!(
                        actual_struct_data.child_data().len(),
                        2,
                        "Unexpected struct buffers in result"
                    );

                    let expected_struct_data = expected_entries.to_data();

                    // Verify that underlying key buffer is shared and not copied.
                    let actual_keys_data = actual_struct_data.child_data()[0].clone();
                    let expected_keys_data = expected_struct_data.child_data()[0].clone();
                    assert_eq!(
                        actual_keys_data.buffers().len(),
                        expected_keys_data.buffers().len()
                    );
                    assert_eq!(
                        actual_keys_data.buffers()[0].as_ptr(),
                        expected_keys_data.buffers()[0].as_ptr(),
                        "Keys buffers are not shared"
                    );

                    // Verify that underlying value buffer is shared and not copied.
                    let actual_values_data = actual_struct_data.child_data()[1].clone();
                    let expected_values_data = expected_struct_data.child_data()[1].clone();
                    assert_eq!(
                        actual_values_data.buffers().len(),
                        expected_values_data.buffers().len()
                    );
                    assert_eq!(
                        actual_values_data.buffers()[0].as_ptr(),
                        expected_values_data.buffers()[0].as_ptr(),
                        "Values buffer are not shared"
                    );

                    assert_eq!(actual_entries.data_type(), expected_entries.data_type());
                    assert_eq!(actual_entries.len(), expected_entries.len());

                    // Verify all keys and values in the result.
                    let actual_keys = actual_entries
                        .column(0)
                        .as_any()
                        .downcast_ref::<$key_type>()
                        .unwrap();
                    let expected_keys = expected_entries
                        .column(0)
                        .as_any()
                        .downcast_ref::<$key_type>()
                        .unwrap();

                    let actual_values = actual_entries
                        .column(1)
                        .as_any()
                        .downcast_ref::<$value_type>()
                        .unwrap();
                    let expected_values = expected_entries
                        .column(1)
                        .as_any()
                        .downcast_ref::<$value_type>()
                        .unwrap();
                    $verify_entries_fn!(
                        expected_entries.len(),
                        actual_keys,
                        expected_keys,
                        actual_values,
                        expected_values
                    );
                }
                unexpected => {
                    panic!("Actual result: {unexpected:?} is not an Array ColumnarValue")
                }
            }
        }};
    }

    macro_rules! default_entries_verifier {
        (
            $entries_len:expr,
            $actual_keys:expr,
            $expected_keys:expr,
            $actual_values:expr,
            $expected_values:expr
        ) => {{
            for idx in 0..$entries_len {
                assert_eq!(
                    $actual_keys.is_null(idx),
                    $expected_keys.is_null(idx),
                    "Unexpected null key at index: {idx}"
                );
                assert_eq!(
                    $actual_keys.value(idx),
                    $expected_keys.value(idx),
                    "Unexpected key at index {idx}"
                );

                assert_eq!(
                    $actual_values.is_null(idx),
                    $expected_values.is_null(idx),
                    "Mismatched nullity at index: {idx}"
                );
                if !$actual_values.is_null(idx) {
                    assert_eq!(
                        $actual_values.value(idx),
                        $expected_values.value(idx),
                        "Unexpected value at index {idx}"
                    );
                }
            }
        }};
    }

    macro_rules! list_entries_verifier {
        (
            $entries_len:expr,
            $actual_keys:expr,
            $expected_keys:expr,
            $actual_values:expr,
            $expected_values:expr
        ) => {{
            for idx in 0..$entries_len {
                let actual_list = $actual_keys.value(idx);
                let expected_list = $expected_keys.value(idx);

                assert!(
                    actual_list.eq(&expected_list),
                    "Unexpected key at index {}: actual={:?}, expected={:?}",
                    idx,
                    actual_list,
                    expected_list
                );

                assert_eq!(
                    $actual_values.value(idx),
                    $expected_values.value(idx),
                    "Unexpected value at index {idx}"
                );
            }
        }};
    }

    #[test]
    fn test_map_to_list_basic() {
        // Input is `MapArray` with 4 maps. Each map has 3 entries with string keys and int values.
        let keys_arg: [Vec<String>; 4] = [
            vec!["c".into(), "a".into(), "b".into()],
            vec!["z".into(), "y".into(), "x".into()],
            vec!["a".into(), "b".into(), "c".into()],
            vec!["fusion".into(), "comet".into(), "data".into()],
        ];
        let values_arg = [
            vec![Some(3), Some(1), Some(2)],
            vec![Some(30), Some(20), Some(10)],
            vec![Some(1), Some(2), Some(3)],
            vec![Some(300), Some(100), Some(200)],
        ];
        let validity = [true, true, true, true];

        let map_arr_arg = build_map!(
            StringBuilder::new(),
            Int32Builder::new(),
            keys_arg,
            values_arg,
            validity,
            default_map_entries_builder
        );

        let expected_map_arr = map_arr_arg.clone();
        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = map_to_list(&args).unwrap();
        verify_result!(
            StringArray,
            Int32Array,
            result,
            expected_map_arr,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_to_list_with_list_keys() {
        // Input is `MapArray` with one maps. The map has 3 entries with integer list keys and
        // string values.
        let keys_arg = [vec![
            vec![Some(3), Some(2)],
            vec![Some(1), Some(2)],
            vec![Some(2), Some(1)],
        ]];
        let values_arg: [Vec<Option<String>>; 1] = [vec![
            Some("three_two".into()),
            Some("one_two".into()),
            Some("two_one".into()),
        ]];
        let validity = [true];

        let map_arr_arg = build_map!(
            ListBuilder::new(Int32Builder::new()),
            StringBuilder::new(),
            keys_arg,
            values_arg,
            validity,
            default_map_entries_builder
        );

        let expected_map_arr = map_arr_arg.clone();
        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = map_to_list(&args).unwrap();
        verify_result!(
            ListArray,
            StringArray,
            result,
            expected_map_arr,
            list_entries_verifier
        );
    }

    #[test]
    fn test_map_to_list_with_nested_maps() {
        // Input is `MapArray` with one maps. The map has 2 entries with string keys and map values.
        // The inner maps have 2 entries each with string keys and string values.
        let outer_keys: [String; 2] = ["outer_k2".into(), "outer_k1".into()];
        let inner_keys: [[String; 2]; 2] = [
            ["outer_k2->inner_k1".into(), "outer_k2->inner_k2".into()],
            ["outer_k1->inner_k1".into(), "outer_k1->inner_k2".into()],
        ];
        let inner_values: [[Option<String>; 2]; 2] = [
            [
                Some("outer_k2->inner_k1->inner_v1".into()),
                Some("outer_k2->inner_k2->inner_v2".into()),
            ],
            [
                Some("outer_k1->inner_k1->inner_v1".into()),
                Some("outer_k1->inner_k2->inner_v2".into()),
            ],
        ];
        let outer_values = [
            (&inner_keys[0], &inner_values[0], true),
            (&inner_keys[1], &inner_values[1], true),
        ];

        let keys_arg = [outer_keys];
        let values_arg = [outer_values];
        let validity = [true];

        let map_arr_arg = build_map!(
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
            keys_arg,
            values_arg,
            validity,
            nested_map_entries_builder
        );

        // For nested maps, only the outer map is sorted by keys, the inner maps remain unchanged.
        let expected_map_arr = map_arr_arg.clone();
        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = map_to_list(&args).unwrap();
        verify_result!(
            StringArray,
            MapArray,
            result,
            expected_map_arr,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_to_list_with_scalar_argument() {
        let map_arr_arg = build_map!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec!["b".to_string(), "a".to_string()]],
            vec![vec![Some(2), Some(1)]],
            vec![true],
            default_map_entries_builder
        );

        let expected_map_arr = map_arr_arg.clone();
        let args = vec![ColumnarValue::Scalar(
            ScalarValue::try_from_array(&map_arr_arg, 0).unwrap(),
        )];
        let result = map_to_list(&args).unwrap();
        verify_result!(
            StringArray,
            Int32Array,
            result,
            expected_map_arr,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_to_list_with_invalid_arguments() {
        let result = map_to_list(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("map_to_list expects exactly one argument"));

        let map_arr_arg = build_map!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec!["a".to_string()]],
            vec![vec![Some(1)]],
            vec![true],
            default_map_entries_builder
        );

        let args = vec![
            ColumnarValue::Array(Arc::new(map_arr_arg.clone())),
            ColumnarValue::Array(Arc::new(map_arr_arg)),
        ];
        let result = map_to_list(&args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("map_to_list expects exactly one argument"));

        let int_arr = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let args = vec![ColumnarValue::Array(int_arr)];

        let result = map_to_list(&args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("map_to_list expects Map type as argument"));
    }
}
