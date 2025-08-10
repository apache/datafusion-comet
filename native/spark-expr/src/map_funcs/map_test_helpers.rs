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

#[macro_export]
macro_rules! test_create_map_array {
    ($key_builder:expr, $value_builder:expr, $keys:expr, $values:expr, $validity:expr) => {{
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
                let key_val = &map_keys[entry_idx];
                match key_val {
                    Some(key) => map_builder.keys().append_value(key.clone()),
                    None => panic!("Unexpected None key found"),
                }

                let value = &map_values[entry_idx];
                map_builder.values().append_value(value.clone().unwrap());
            }

            let is_valid = $validity[map_idx];
            map_builder.append(is_valid).unwrap();
        }

        map_builder.finish()
    }};
}

#[macro_export]
macro_rules! test_create_nested_map_array {
    ($key_builder:expr, $value_builder:expr, $keys:expr, $values:expr, $validity:expr) => {{
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
                let key_val = &map_keys[entry_idx];
                match key_val {
                    Some(key) => map_builder.keys().append_value(key.clone()),
                    None => panic!("Unexpected None key found in outer map"),
                }

                let (inner_keys, inner_values, inner_valid) = &map_values[entry_idx];

                let inner_entries = inner_keys.len();
                for inner_idx in 0..inner_entries {
                    let inner_key_val = &inner_keys[inner_idx];
                    match inner_key_val {
                        Some(inner_key) => {
                            map_builder.values().keys().append_value(inner_key.clone())
                        }
                        None => panic!("Unexpected None key found in inner map"),
                    }

                    let inner_value = &inner_values[inner_idx];
                    map_builder
                        .values()
                        .values()
                        .append_value(inner_value.clone().unwrap());
                }

                map_builder.values().append(*inner_valid).unwrap();
            }

            let is_valid = $validity[map_idx];
            map_builder.append(is_valid).unwrap();
        }

        map_builder.finish()
    }};
}

#[macro_export]
macro_rules! test_verify_result_equals_map {
    ($keyType:ty, $valueType:ty, $result:expr, $expected_map_arr:expr) => {{
        match $result {
            ColumnarValue::Array(actual_arr) => {
                let actual_map_arr = actual_arr.as_any().downcast_ref::<MapArray>().unwrap();

                assert_eq!(actual_map_arr.len(), $expected_map_arr.len());
                assert_eq!(actual_map_arr.offsets(), $expected_map_arr.offsets());
                assert_eq!(actual_map_arr.nulls(), $expected_map_arr.nulls());
                assert_eq!(actual_map_arr.data_type(), $expected_map_arr.data_type());

                let actual_entries = actual_map_arr.entries();
                let actual_keys = actual_entries
                    .column(0)
                    .as_any()
                    .downcast_ref::<$keyType>()
                    .unwrap();
                let actual_values = actual_entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<$valueType>()
                    .unwrap();

                let expected_entries = $expected_map_arr.entries();
                let expected_keys = expected_entries
                    .column(0)
                    .as_any()
                    .downcast_ref::<$keyType>()
                    .unwrap();
                let expected_values = expected_entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<$valueType>()
                    .unwrap();

                for idx in 0..actual_entries.len() {
                    assert_eq!(actual_keys.value(idx), expected_keys.value(idx));
                    assert_eq!(actual_values.value(idx), expected_values.value(idx));
                }
            }
            unexpected_arr => {
                panic!("Actual result: {unexpected_arr:?} is not an Array ColumnarValue")
            }
        }
    }};
}

#[macro_export]
macro_rules! test_create_list_array {
    ($key_builder:expr, $value_builder:expr, $keys:expr, $values:expr, $validity:expr) => {{
        use arrow::array::{ArrayRef, GenericListArray, StructArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Field;
        use std::sync::Arc;

        assert_eq!($keys.len(), $values.len());
        assert_eq!($keys.len(), $validity.len());

        let total_lists = $keys.len();

        let mut key_builder = $key_builder;
        let mut value_builder = $value_builder;

        let mut offsets: Vec<i32> = Vec::with_capacity(total_lists + 1);
        offsets.push(0);

        for list_idx in 0..total_lists {
            let list_keys = &$keys[list_idx];
            let list_values = &$values[list_idx];
            assert_eq!(list_keys.len(), list_values.len());

            let entries = list_keys.len();
            for entry_idx in 0..entries {
                let key_val = &list_keys[entry_idx];
                match key_val {
                    Some(key) => key_builder.append_value(key.clone()),
                    None => panic!("Unexpected None key found"),
                }

                let value = &list_values[entry_idx];
                value_builder.append_value(value.clone().unwrap());
            }

            let last = *offsets.last().unwrap();
            offsets.push(last + (entries as i32));
        }

        let keys_array = key_builder.finish();
        let values_array = value_builder.finish();

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("key", keys_array.data_type().clone(), false)),
                Arc::new(keys_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", values_array.data_type().clone(), true)),
                Arc::new(values_array) as ArrayRef,
            ),
        ]);

        let list_field = Arc::new(Field::new(
            "entries",
            struct_array.data_type().clone(),
            true,
        ));

        GenericListArray::<i32>::try_new(
            list_field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(struct_array) as ArrayRef,
            None,
        )
        .unwrap()
    }};
}

#[macro_export]
macro_rules! test_create_nested_list_array {
    ($key_builder:expr, $map_value_builder:expr, $keys:expr, $values:expr, $validity:expr) => {{
        use arrow::array::{ArrayRef, GenericListArray, StructArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Field;
        use std::sync::Arc;

        assert_eq!($keys.len(), $values.len());
        assert_eq!($keys.len(), $validity.len());

        let total_lists = $keys.len();

        let mut key_builder = $key_builder;
        let mut map_builder = $map_value_builder;

        let mut offsets: Vec<i32> = Vec::with_capacity(total_lists + 1);
        offsets.push(0);

        let mut total_entries = 0;
        for list_idx in 0..total_lists {
            let list_keys = &$keys[list_idx];
            let list_values = &$values[list_idx];
            assert_eq!(list_keys.len(), list_values.len());

            let entries = list_keys.len();
            for entry_idx in 0..entries {
                let key_val = &list_keys[entry_idx];
                match key_val {
                    Some(key) => key_builder.append_value(key.clone()),
                    None => panic!("Unexpected None key found in outer list"),
                }

                let (inner_keys, inner_values, inner_valid) = &list_values[entry_idx];
                assert_eq!(inner_keys.len(), inner_values.len());

                for inner_idx in 0..inner_keys.len() {
                    let inner_key_val = &inner_keys[inner_idx];
                    match inner_key_val {
                        Some(inner_key) => map_builder.keys().append_value(inner_key.clone()),
                        None => panic!("Unexpected None key found in inner map"),
                    }

                    let inner_value = &inner_values[inner_idx];
                    map_builder
                        .values()
                        .append_value(inner_value.clone().unwrap());
                }

                map_builder.append(*inner_valid).unwrap();
            }

            total_entries += entries as i32;
            offsets.push(total_entries);
        }

        let keys_array = key_builder.finish();
        let values_array = map_builder.finish();

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("key", keys_array.data_type().clone(), false)),
                Arc::new(keys_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", values_array.data_type().clone(), true)),
                Arc::new(values_array) as ArrayRef,
            ),
        ]);

        let list_field = Arc::new(Field::new(
            "entries",
            struct_array.data_type().clone(),
            true,
        ));

        GenericListArray::<i32>::try_new(
            list_field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(struct_array) as ArrayRef,
            None,
        )
        .unwrap()
    }};
}
