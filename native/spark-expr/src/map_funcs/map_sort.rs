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

use arrow::array::{Array, ArrayRef, MapArray, StructArray, UInt32Array};
use arrow::compute::{sort_to_indices, take, SortOptions};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark compatible `MapSort` implementation.
/// Sorts each entries of a MapArray by keys in ascending order without changing the ordering of the
/// maps in the array.
pub fn spark_map_sort(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("spark_map_sort expects exactly one argument");
    }

    let arr_arg: ArrayRef = match &args[0] {
        ColumnarValue::Array(array) => Arc::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
    };

    let (maps_arg, map_field, is_sorted) = match arr_arg.data_type() {
        DataType::Map(map_field, is_sorted) => {
            let maps_arg = arr_arg
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("invariant: array data type is Map but downcast to MapArray failed");
            (maps_arg, map_field, is_sorted)
        }
        _ => return exec_err!("spark_map_sort expects Map type as argument"),
    };

    // Fast paths: nothing to sort, all maps null, or input already declared sorted.
    if maps_arg.is_empty() || maps_arg.null_count() == maps_arg.len() || *is_sorted {
        return Ok(ColumnarValue::Array(arr_arg));
    }

    let maps_arg_entries = maps_arg.entries();
    let maps_arg_offsets = maps_arg.offsets();

    let sort_options = SortOptions {
        descending: false,
        nulls_first: true,
    };

    // Build one global permutation over the full entries struct, respecting per-map boundaries,
    // then issue a single `take`. This avoids per-map struct copies and a final `concat`.
    let mut global_indices: Vec<u32> = Vec::with_capacity(maps_arg_entries.len());

    for idx in 0..maps_arg.len() {
        let map_start = maps_arg_offsets[idx] as usize;
        let map_end = maps_arg_offsets[idx + 1] as usize;
        if map_end == map_start {
            continue;
        }

        let map_keys = maps_arg_entries
            .column(0)
            .slice(map_start, map_end - map_start);
        let local_indices = sort_to_indices(&map_keys, Some(sort_options), None)?;
        global_indices.extend(local_indices.values().iter().map(|i| map_start as u32 + *i));
    }

    let indices = UInt32Array::from(global_indices);
    let sorted_entries = take(maps_arg_entries, &indices, None)?;
    let sorted_map_struct = sorted_entries
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("invariant: take on StructArray must return StructArray");

    // Preserve the original is_sorted flag to keep schema consistent
    let sorted_map_arr = Arc::new(MapArray::try_new(
        Arc::clone(map_field),
        maps_arg.offsets().clone(),
        sorted_map_struct.clone(),
        maps_arg.nulls().cloned(),
        *is_sorted,
    )?);

    Ok(ColumnarValue::Array(sorted_map_arr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::builder::{Int32Builder, MapBuilder, StringBuilder};
    use arrow::array::{Int32Array, ListArray, ListBuilder, MapFieldNames, StringArray};
    use datafusion::common::ScalarValue;
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
                    let actual_map_arr = actual_arr.as_any().downcast_ref::<MapArray>().unwrap();

                    assert_eq!(actual_map_arr.len(), $expected_map_arr.len());
                    assert_eq!(actual_map_arr.offsets(), $expected_map_arr.offsets());
                    assert_eq!(actual_map_arr.nulls(), $expected_map_arr.nulls());
                    assert_eq!(actual_map_arr.data_type(), $expected_map_arr.data_type());

                    let actual_entries = actual_map_arr.entries();
                    let actual_keys = actual_entries
                        .column(0)
                        .as_any()
                        .downcast_ref::<$key_type>()
                        .unwrap();
                    let actual_values = actual_entries
                        .column(1)
                        .as_any()
                        .downcast_ref::<$value_type>()
                        .unwrap();

                    let expected_entries = $expected_map_arr.entries();
                    let expected_keys = expected_entries
                        .column(0)
                        .as_any()
                        .downcast_ref::<$key_type>()
                        .unwrap();
                    let expected_values = expected_entries
                        .column(1)
                        .as_any()
                        .downcast_ref::<$value_type>()
                        .unwrap();

                    assert_eq!(actual_keys.len(), expected_keys.len());
                    assert_eq!(actual_values.len(), expected_values.len());

                    $verify_entries_fn!(
                        expected_entries.len(),
                        actual_keys,
                        expected_keys,
                        actual_values,
                        expected_values
                    );
                }
                unexpected_arr => {
                    panic!("Actual result: {unexpected_arr:?} is not an Array ColumnarValue")
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
                assert_eq!($actual_keys.value(idx), $expected_keys.value(idx));
                assert_eq!($actual_values.value(idx), $expected_values.value(idx));
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
                assert!(actual_list.eq(&expected_list));
                assert_eq!($actual_values.value(idx), $expected_values.value(idx));
            }
        }};
    }

    #[test]
    fn test_map_sort_with_string_keys() {
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
        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = spark_map_sort(&args).unwrap();

        let expected_keys: [Vec<String>; 4] = [
            vec!["a".into(), "b".into(), "c".into()],
            vec!["x".into(), "y".into(), "z".into()],
            vec!["a".into(), "b".into(), "c".into()],
            vec!["comet".into(), "data".into(), "fusion".into()],
        ];
        let expected_values = [
            vec![Some(1), Some(2), Some(3)],
            vec![Some(10), Some(20), Some(30)],
            vec![Some(1), Some(2), Some(3)],
            vec![Some(100), Some(200), Some(300)],
        ];
        let expected_validity = [true, true, true, true];

        let expected_map_arr = build_map!(
            StringBuilder::new(),
            Int32Builder::new(),
            expected_keys,
            expected_values,
            expected_validity,
            default_map_entries_builder
        );
        verify_result!(
            StringArray,
            Int32Array,
            result,
            expected_map_arr,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_sort_with_int_keys() {
        let keys_arg = [
            vec![3, 2, 1],
            vec![100, 50, 20],
            vec![20, 50, 100],
            vec![-5, 0, -1],
        ];
        let values_arg: [Vec<Option<String>>; 4] = [
            vec![Some("three".into()), Some("two".into()), Some("one".into())],
            vec![
                Some("hundred".into()),
                Some("fifty".into()),
                Some("twenty".into()),
            ],
            vec![
                Some("twenty".into()),
                Some("fifty".into()),
                Some("hundred".into()),
            ],
            vec![
                Some("minus five".into()),
                Some("zero".into()),
                Some("minus one".into()),
            ],
        ];
        let validity = [true, true, true, true];

        let map_arr_arg = build_map!(
            Int32Builder::new(),
            StringBuilder::new(),
            keys_arg,
            values_arg,
            validity,
            default_map_entries_builder
        );
        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = spark_map_sort(&args).unwrap();

        let expected_keys = [
            vec![1, 2, 3],
            vec![20, 50, 100],
            vec![20, 50, 100],
            vec![-5, -1, 0],
        ];
        let expected_values: [Vec<Option<String>>; 4] = [
            vec![Some("one".into()), Some("two".into()), Some("three".into())],
            vec![
                Some("twenty".into()),
                Some("fifty".into()),
                Some("hundred".into()),
            ],
            vec![
                Some("twenty".into()),
                Some("fifty".into()),
                Some("hundred".into()),
            ],
            vec![
                Some("minus five".into()),
                Some("minus one".into()),
                Some("zero".into()),
            ],
        ];
        let expected_validity = [true, true, true, true];

        let expected_map_arr = build_map!(
            Int32Builder::new(),
            StringBuilder::new(),
            expected_keys,
            expected_values,
            expected_validity,
            default_map_entries_builder
        );
        verify_result!(
            Int32Array,
            StringArray,
            result,
            expected_map_arr,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_sort_with_nested_maps() {
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

        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = spark_map_sort(&args).unwrap();

        let expected_outer_keys: [String; 2] = ["outer_k1".into(), "outer_k2".into()];
        let expected_inner_keys: [[String; 2]; 2] = [
            ["outer_k1->inner_k1".into(), "outer_k1->inner_k2".into()],
            ["outer_k2->inner_k1".into(), "outer_k2->inner_k2".into()],
        ];
        let expected_inner_values: [[Option<String>; 2]; 2] = [
            [
                Some("outer_k1->inner_k1->inner_v1".into()),
                Some("outer_k1->inner_k2->inner_v2".into()),
            ],
            [
                Some("outer_k2->inner_k1->inner_v1".into()),
                Some("outer_k2->inner_k2->inner_v2".into()),
            ],
        ];
        let expected_outer_values = [
            (&expected_inner_keys[0], &expected_inner_values[0], true),
            (&expected_inner_keys[1], &expected_inner_values[1], true),
        ];

        let expected_keys_arg = [expected_outer_keys];
        let expected_values_arg = [expected_outer_values];
        let expected_validity = [true];

        let expected_map_arr = build_map!(
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
            expected_keys_arg,
            expected_values_arg,
            expected_validity,
            nested_map_entries_builder
        );

        verify_result!(
            StringArray,
            MapArray,
            result,
            expected_map_arr,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_sort_with_list_int_keys() {
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

        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = spark_map_sort(&args).unwrap();

        let expected_keys = [vec![
            vec![Some(1), Some(2)],
            vec![Some(2), Some(1)],
            vec![Some(3), Some(2)],
        ]];
        let expected_values: [Vec<Option<String>>; 1] = [vec![
            Some("one_two".into()),
            Some("two_one".into()),
            Some("three_two".into()),
        ]];
        let expected_validity = [true];

        let expected_map_arr = build_map!(
            ListBuilder::new(Int32Builder::new()),
            StringBuilder::new(),
            expected_keys,
            expected_values,
            expected_validity,
            default_map_entries_builder
        );

        verify_result!(
            ListArray,
            StringArray,
            result,
            expected_map_arr,
            list_entries_verifier
        );
    }

    #[test]
    fn test_map_sort_with_list_string_keys() {
        let keys_arg: [Vec<Vec<Option<String>>>; 1] = [vec![
            vec![Some("c".into()), Some("b".into())],
            vec![Some("a".into()), Some("b".into())],
            vec![Some("b".into()), Some("a".into())],
        ]];
        let values_arg: [Vec<Option<i32>>; 1] = [vec![Some(32), Some(12), Some(21)]];
        let validity = [true];

        let map_arr_arg = build_map!(
            ListBuilder::new(StringBuilder::new()),
            Int32Builder::new(),
            keys_arg,
            values_arg,
            validity,
            default_map_entries_builder
        );

        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = spark_map_sort(&args).unwrap();

        let expected_keys: [Vec<Vec<Option<String>>>; 1] = [vec![
            vec![Some("a".into()), Some("b".into())],
            vec![Some("b".into()), Some("a".into())],
            vec![Some("c".into()), Some("b".into())],
        ]];
        let expected_values: [Vec<Option<i32>>; 1] = [vec![Some(12), Some(21), Some(32)]];
        let expected_validity = [true];

        let expected_map_arr = build_map!(
            ListBuilder::new(StringBuilder::new()),
            Int32Builder::new(),
            expected_keys,
            expected_values,
            expected_validity,
            default_map_entries_builder
        );

        verify_result!(
            ListArray,
            Int32Array,
            result,
            expected_map_arr,
            list_entries_verifier
        );
    }

    #[test]
    fn test_map_sort_with_scalar_argument() {
        let map_array = build_map!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec!["b".to_string(), "a".to_string()]],
            vec![vec![Some(2), Some(1)]],
            vec![true],
            default_map_entries_builder
        );

        let args = vec![ColumnarValue::Scalar(
            ScalarValue::try_from_array(&map_array, 0).unwrap(),
        )];
        let result = spark_map_sort(&args).unwrap();

        let expected_map_arr = build_map!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec!["a".to_string(), "b".to_string()]],
            vec![vec![Some(1), Some(2)]],
            vec![true],
            default_map_entries_builder
        );
        verify_result!(
            StringArray,
            Int32Array,
            result,
            expected_map_arr,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_sort_with_empty_map() {
        let map_arr_arg = build_map!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![Vec::<String>::new()],
            vec![Vec::<Option<i32>>::new()],
            vec![false],
            default_map_entries_builder
        );

        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = spark_map_sort(&args).unwrap();
        let expected_map_arr = build_map!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![Vec::<String>::new()],
            vec![Vec::<Option<i32>>::new()],
            vec![false],
            default_map_entries_builder
        );
        verify_result!(
            StringArray,
            Int32Array,
            result,
            expected_map_arr,
            default_entries_verifier
        );
    }

    #[test]
    fn test_map_sort_with_invalid_arguments() {
        let result = spark_map_sort(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("spark_map_sort expects exactly one argument"));

        let map_array = build_map!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec!["a".to_string()]],
            vec![vec![Some(1)]],
            vec![true],
            default_map_entries_builder
        );

        let args = vec![
            ColumnarValue::Array(Arc::new(map_array.clone())),
            ColumnarValue::Array(Arc::new(map_array)),
        ];
        let result = spark_map_sort(&args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("spark_map_sort expects exactly one argument"));

        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let args = vec![ColumnarValue::Array(int_array)];

        let result = spark_map_sort(&args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("spark_map_sort expects Map type as argument"));
    }
}
