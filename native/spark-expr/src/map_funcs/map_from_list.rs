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
use arrow::array::{Array, ArrayData, ArrayRef, ListArray, MapArray, StructArray};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Converts a ListArray of Struct type with key-value pairs to MapArray.
/// It preserves the physical layout, ordering of each element in the array and apply any map
/// specific field-metadata from the ListArray to the output MapArray.
///
/// Eg. if the input ListArray of Struct type is:
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
/// ```
///
/// The output MapArray will be:
/// ```text
/// [
///      {"c": 3, "a": 1, "b": 2}
///      {["b", "a", "c"]: 2, ["a", "b", "c"]: 1, ["c", "b", "a"]: 3}
/// ]
/// ```
///
pub fn map_from_list(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("map_from_list expects exactly one argument");
    }

    let arr_arg: ArrayRef = match &args[0] {
        ColumnarValue::Array(array) => Arc::<dyn Array>::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
    };

    let (list_arg, list_field_ref) = match arr_arg.data_type() {
        DataType::List(field_ref) => (
            arr_arg.as_any().downcast_ref::<ListArray>().unwrap(),
            field_ref,
        ),
        _ => return exec_err!("map_from_list expects List type as argument"),
    };

    let list_struct_array = list_arg
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("map_from_list expects StructArray values".to_string())
        })?;

    let list_struct_array_data = list_struct_array.to_data();
    let list_data = list_arg.to_data();

    let list_offset_buffer = if list_data.buffers().len() == 1 {
        list_data.buffers()[0].clone()
    } else {
        return exec_err!("map_from_list expects ListArray to have a single offset buffer");
    };

    // If the input ListArray was created from the Map type and if there is any Map specific
    // metadata, then extract those metadata and create a new field reference for the Map type.
    let (map_field_ref, map_is_sorted) = {
        let mut metadata = list_field_ref.metadata().clone();
        let is_sorted = metadata
            .remove(COMET_MAP_TO_LIST_FIELD_METADATA_IS_SORTED_KEY)
            .map(|val| val == "true")
            .unwrap_or(false);
        let field_ref = (**list_field_ref).clone().with_metadata(metadata);
        (Arc::new(field_ref), is_sorted)
    };

    // Build a MapArray preserving the same layout as the ListArray. If the underlying
    // `list_struct_array_data` is malformed, the MapArray construction will fail.
    let mut map_builder = ArrayData::builder(DataType::Map(map_field_ref, map_is_sorted))
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
    use arrow::array::builder::{ArrayBuilder, Int32Builder, StringBuilder};
    use arrow::array::{Int32Array, MapArray, MapBuilder, MapFieldNames, StringArray};
    use arrow::array::{ListBuilder, StructBuilder};
    use arrow::datatypes::{Field, Fields};
    use datafusion::common::ScalarValue;
    use std::collections::HashMap;

    macro_rules! build_list_array {
        (
            $key_type:expr,
            $value_type:expr,
            $key_builder_expr:expr,
            $value_builder_expr:expr,
            $key_builder_type:ty,
            $value_builder_type:ty,
            $keys:expr,
            $values:expr,
            $validity:expr,
            $is_sorted:ident,
            $entries_builder_fn:ident
        ) => {{
            assert_eq!($keys.len(), $values.len());
            assert_eq!($keys.len(), $validity.len());

            let total_lists = $keys.len();

            let key_field = Arc::new(Field::new("keys", $key_type, false));
            let value_field = Arc::new(Field::new("values", $value_type, true));
            let struct_field = Fields::from(vec![key_field.clone(), value_field.clone()]);

            let struct_builder = StructBuilder::new(
                vec![key_field, value_field],
                vec![
                    Box::new($key_builder_expr) as Box<dyn ArrayBuilder>,
                    Box::new($value_builder_expr) as Box<dyn ArrayBuilder>,
                ],
            );

            let mut list_field_metadata = HashMap::new();
            if $is_sorted {
                // Set the sorted flag to the ListArray field metadata.
                list_field_metadata.insert(
                    COMET_MAP_TO_LIST_FIELD_METADATA_IS_SORTED_KEY.into(),
                    "true".to_string(),
                );
            }

            let mut list_builder = ListBuilder::new(struct_builder).with_field(Arc::new(
                Field::new("entries", DataType::Struct(struct_field), true)
                    .with_metadata(list_field_metadata),
            ));

            for list_idx in 0..total_lists {
                if $validity[list_idx] == false {
                    list_builder.append(false);
                    continue;
                }

                let list_keys = &$keys[list_idx];
                let list_values = &$values[list_idx];
                assert_eq!(list_keys.len(), list_values.len());

                let entries = list_keys.len();
                for entry_idx in 0..entries {
                    let struct_builder = list_builder.values();
                    let list_key = &list_keys[entry_idx];
                    let list_value = &list_values[entry_idx];

                    $entries_builder_fn!(
                        struct_builder,
                        $key_builder_type,
                        $value_builder_type,
                        list_key,
                        list_value
                    );

                    struct_builder.append(true);
                }

                list_builder.append(true);
            }

            list_builder.finish()
        }};
    }

    macro_rules! default_struct_entries_builder {
        (
            $struct_builder:expr,
            $key_builder_type:ty,
            $value_builder_type:ty,
            $key:expr,
            $value:expr
        ) => {{
            let key_builder = $struct_builder
                .field_builder::<$key_builder_type>(0)
                .unwrap();
            key_builder.append_value($key.clone());

            let value_builder = $struct_builder
                .field_builder::<$value_builder_type>(1)
                .unwrap();
            value_builder.append_value($value.clone().unwrap());
        }};
    }

    macro_rules! list_key_struct_entries_builder {
        (
            $struct_builder:expr,
            $key_builder_type:ty,
            $value_builder_type:ty,
            $keys:expr,
            $value:expr
        ) => {{
            let key_builder = $struct_builder
                .field_builder::<$key_builder_type>(0)
                .unwrap();
            for key in $keys.iter() {
                key_builder.values().append_value(key.clone());
            }
            key_builder.append(true);

            let value_builder = $struct_builder
                .field_builder::<$value_builder_type>(1)
                .unwrap();
            value_builder.append_value($value.clone().unwrap());
        }};
    }

    macro_rules! nested_struct_entries_builder {
        (
            $struct_builder:expr,
            $key_builder_type:ty,
            $value_builder_type:ty,
            $key:expr,
            $value:expr
        ) => {{
            let key_builder = $struct_builder
                .field_builder::<$key_builder_type>(0)
                .unwrap();
            key_builder.append_value($key.clone());

            let inner_builder = $struct_builder
                .field_builder::<$value_builder_type>(1)
                .unwrap();

            let (inner_keys, inner_values, inner_valid) = $value;
            assert_eq!(inner_keys.len(), inner_values.len());

            let inner_entries = inner_keys.len();
            for inner_idx in 0..inner_entries {
                let inner_key = inner_keys[inner_idx].clone().unwrap();
                let inner_value = inner_values[inner_idx].clone().unwrap();

                inner_builder.keys().append_value(inner_key);
                inner_builder.values().append_value(inner_value);
            }
            inner_builder.append(*inner_valid).unwrap();
        }};
    }

    macro_rules! verify_result {
        (
            $key_type:ty,
            $value_type:ty,
            $result:expr,
            $expected_list_arr:expr,
            $expected_is_sorted:expr,
            $verify_entries_fn:ident
        ) => {{
            match $result {
                ColumnarValue::Array(actual_arr) => {
                    match (actual_arr.data_type(), $expected_list_arr.data_type()) {
                        (
                            DataType::Map(actual_field_ref, actual_is_sorted),
                            DataType::List(expected_field_ref),
                        ) => {
                            // Compare Map field with List field without metadata. Because the
                            // expected_list_arr is a clone of the input argument which will have
                            // sorted flag metadata. The result MapArray will consume that metadata
                            // but will not add back to the metadata.
                            let expected_field_no_meta =
                                (**expected_field_ref).clone().with_metadata(HashMap::new());
                            assert_eq!(
                                **actual_field_ref, expected_field_no_meta,
                                "Mismatched field in the result MapArray"
                            );

                            assert_eq!(
                                *actual_is_sorted, $expected_is_sorted,
                                "Mismatched sorted flag in the result map"
                            );
                        }
                        _ => panic!("Actual result data type is not Map type"),
                    }

                    let actual_map = actual_arr.as_any().downcast_ref::<MapArray>().unwrap();
                    let expected_list = $expected_list_arr
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();

                    assert_eq!(actual_map.len(), expected_list.len());
                    assert_eq!(actual_map.offset(), expected_list.offset());

                    let actual_map_data = actual_map.to_data();
                    let expected_list_data = expected_list.to_data();
                    assert_eq!(
                        actual_map_data.buffers().len(),
                        expected_list_data.buffers().len()
                    );

                    assert_eq!(actual_map.nulls(), expected_list.nulls());

                    let actual_entries = actual_map.entries();
                    let expected_entries = expected_list
                        .values()
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap();

                    assert_eq!(actual_entries.data_type(), expected_entries.data_type());
                    assert_eq!(actual_entries.len(), expected_entries.len());

                    let actual_struct_data = actual_entries.to_data();
                    let expected_struct_data = expected_entries.to_data();

                    // Since the result is a Map type, the struct must have two fields. Its an
                    // invariant.
                    assert_eq!(
                        actual_struct_data.child_data().len(),
                        expected_struct_data.child_data().len(),
                        "Unexpected struct buffers in result"
                    );

                    let actual_keys_data = actual_struct_data.child_data()[0].clone();
                    let expected_keys_data = expected_struct_data.child_data()[0].clone();
                    assert_eq!(
                        actual_keys_data.buffers().len(),
                        expected_keys_data.buffers().len()
                    );

                    // Verify that the underlying key buffers are not copied while constructing the
                    // MapArray.
                    for buffer_idx in 0..actual_keys_data.buffers().len() {
                        assert_eq!(
                            actual_keys_data.buffers()[buffer_idx].as_ptr(),
                            expected_keys_data.buffers()[buffer_idx].as_ptr(),
                            "Keys buffers at index: {buffer_idx} is not shared"
                        );
                    }

                    // Verify that the underlying value buffers are not copies while constructing
                    // the MapArray.
                    let actual_values_data = actual_struct_data.child_data()[1].clone();
                    let expected_values_data = expected_struct_data.child_data()[1].clone();
                    assert_eq!(
                        actual_values_data.buffers().len(),
                        expected_values_data.buffers().len()
                    );
                    assert_eq!(
                        actual_values_data.buffers().is_empty(),
                        expected_values_data.buffers().is_empty()
                    );

                    for buffer_idx in 0..actual_values_data.buffers().len() {
                        assert_eq!(
                            actual_values_data.buffers()[buffer_idx].as_ptr(),
                            expected_values_data.buffers()[buffer_idx].as_ptr(),
                            "Values buffers at index: {buffer_idx} not shared"
                        );
                    }

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
                other => panic!("Actual result: {other:?} is not an Array ColumnarValue"),
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
                let actual_key_list = $actual_keys.value(idx);
                let expected_key_list = $expected_keys.value(idx);

                assert!(
                    actual_key_list.eq(&expected_key_list),
                    "Unexpected list key at index: {idx}",
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
    fn test_map_from_list_basic() {
        // A ListArray with 3 entries of struct type. The third entry is null. The list entries are
        // sorted.
        let keys_arg: [Vec<String>; 3] = [
            vec!["a".into(), "b".into(), "c".into()],
            vec!["first".into(), "second".into(), "third".into()],
            vec![],
        ];
        let values_arg = [
            vec![Some(2), Some(1), Some(3)],
            vec![Some(100), Some(200), Some(300)],
            vec![],
        ];
        let validity_arg = [true, true, false];
        let is_sorted = true;

        let list_arr = build_list_array!(
            DataType::Utf8,
            DataType::Int32,
            StringBuilder::new(),
            Int32Builder::new(),
            StringBuilder,
            Int32Builder,
            keys_arg,
            values_arg,
            validity_arg,
            is_sorted,
            default_struct_entries_builder
        );

        let expected_list_arr = list_arr.clone();
        let args = vec![ColumnarValue::Array(Arc::new(list_arr))];
        let result = map_from_list(&args).unwrap();
        verify_result!(
            StringArray,
            Int32Array,
            result,
            expected_list_arr,
            is_sorted,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_from_list_with_list_keys() {
        // A ListArray with single entry of struct type. The `keys` field of the struct is a list.
        let keys_arg = [vec![vec![1, 2], vec![3, 2], vec![2, 1]]];
        let values_arg: [Vec<Option<String>>; 1] = [vec![
            Some("three_two".into()),
            Some("one_two".into()),
            Some("two_one".into()),
        ]];
        let validity = [true];
        let is_sorted = false;

        let list_arr = build_list_array!(
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::Utf8,
            ListBuilder::new(Int32Builder::new()),
            StringBuilder::new(),
            ListBuilder<Int32Builder>,
            StringBuilder,
            keys_arg,
            values_arg,
            validity,
            is_sorted,
            list_key_struct_entries_builder
        );

        let expected_list_arr = list_arr.clone();
        let args = vec![ColumnarValue::Array(Arc::new(list_arr))];
        let result = map_from_list(&args).unwrap();

        verify_result!(
            ListArray,
            StringArray,
            result,
            expected_list_arr,
            is_sorted,
            list_entries_verifier
        );
    }

    #[test]
    fn test_map_from_list_with_nested_maps() {
        // A ListArray with single entry of struct type. Each struct `values` field is a Map with
        // 2 entries.
        let outer_keys = [vec!["outer_k2".to_string(), "outer_k1".to_string()]];
        let inner_keys = [vec![
            vec![
                Some("outer_k2->inner_k1".to_string()),
                Some("outer_k2->inner_k2".to_string()),
            ],
            vec![
                Some("outer_k1->inner_k1".to_string()),
                Some("outer_k1->inner_k2".to_string()),
            ],
        ]];
        let inner_values = [vec![
            vec![
                Some("outer_k2->inner_k1->inner_v1".to_string()),
                Some("outer_k2->inner_k2->inner_v2".to_string()),
            ],
            vec![
                Some("outer_k1->inner_k1->inner_v1".to_string()),
                Some("outer_k1->inner_k2->inner_v2".to_string()),
            ],
        ]];
        let inner_map_data = [vec![
            (&inner_keys[0][0], &inner_values[0][0], true),
            (&inner_keys[0][1], &inner_values[0][1], true),
        ]];
        let validity = [true];
        let is_sorted = true;

        let map_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        ));

        let list_arr = build_list_array!(
            DataType::Utf8,
            DataType::Map(map_entries_field, false),
            StringBuilder::new(),
            MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".into(),
                    key: "key".into(),
                    value: "value".into()
                }),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            StringBuilder,
            MapBuilder<StringBuilder, StringBuilder>,
            outer_keys,
            inner_map_data,
            validity,
            is_sorted,
            nested_struct_entries_builder
        );

        let expected_list_arr = list_arr.clone();
        let args = vec![ColumnarValue::Array(Arc::new(list_arr))];
        let result = map_from_list(&args).unwrap();

        verify_result!(
            StringArray,
            MapArray,
            result,
            expected_list_arr,
            is_sorted,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_from_list_with_scalar_argument() {
        let list_arr = build_list_array!(
            DataType::Utf8,
            DataType::Int32,
            StringBuilder::new(),
            Int32Builder::new(),
            StringBuilder,
            Int32Builder,
            [vec!["b".to_string(), "a".to_string()]],
            [vec![Some(2), Some(1)]],
            vec![true],
            false,
            default_struct_entries_builder
        );

        let args = vec![ColumnarValue::Scalar(
            ScalarValue::try_from_array(&list_arr, 0).unwrap(),
        )];

        let result = map_from_list(&args).unwrap();
        let expected_list_arr = list_arr;
        verify_result!(
            StringArray,
            Int32Array,
            result,
            expected_list_arr,
            false,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_from_list_with_invalid_arguments() {
        let res = map_from_list(&[]);
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("map_from_list expects exactly one argument"));

        let list_arr = build_list_array!(
            DataType::Utf8,
            DataType::Int32,
            StringBuilder::new(),
            Int32Builder::new(),
            StringBuilder,
            Int32Builder,
            [vec!["b".to_string(), "a".to_string()]],
            [vec![Some(2), Some(1)]],
            vec![true],
            false,
            default_struct_entries_builder
        );
        let args = vec![
            ColumnarValue::Array(Arc::new(list_arr.clone())),
            ColumnarValue::Array(Arc::new(list_arr)),
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
            .contains("map_from_list expects List type as argument"));
    }
}
