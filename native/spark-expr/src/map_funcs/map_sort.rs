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

use arrow::array::{Array, ArrayRef, MapArray, StructArray};
use arrow::compute::{concat, sort_to_indices, take, SortOptions};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark compatible `MapSort` implementation.
/// Sorts each entries of a MapArray by keys in ascending order without changing the ordering of the
/// maps in the array.
///
/// For eg. If the input is a MapArray with entries:
/// ```text
/// [
///     {"c": 3, "a": 1, "b": 2}
///     {"x": 1, "z": 3, "y": 2}
///     {"a": 1, "b": 2, "c": 3}
/// ]
/// ```
/// The output will be:
/// ```text
/// [
///     {"a": 1, "b": 2, "c": 3}
///     {"x": 1, "y": 2, "z": 3}
///     {"a": 1, "b": 2, "c": 3}
/// ]
/// ```
pub fn spark_map_sort(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("spark_map_sort expects exactly one argument");
    }

    let arr_arg: ArrayRef = match &args[0] {
        ColumnarValue::Array(array) => Arc::<dyn Array>::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
    };

    let (maps_arg, map_field, is_sorted) = match arr_arg.data_type() {
        DataType::Map(map_field, is_sorted) => {
            let maps_arg = arr_arg.as_any().downcast_ref::<MapArray>().unwrap();
            (maps_arg, map_field, is_sorted)
        }
        _ => return exec_err!("spark_map_sort expects Map type as argument"),
    };

    let maps_arg_entries = maps_arg.entries();
    let maps_arg_offsets = maps_arg.offsets();

    let mut sorted_map_entries_vec: Vec<ArrayRef> = Vec::with_capacity(maps_arg.len());

    // Iterate over each map in the MapArray and build a vector of sorted map entries.
    for idx in 0..maps_arg.len() {
        // Retrieve the start and end of the current map entries from the offset buffer.
        let map_start = maps_arg_offsets[idx] as usize;
        let map_end = maps_arg_offsets[idx + 1] as usize;
        let map_len = map_end - map_start;

        // Get the current map entries.
        let map_entries = maps_arg_entries.slice(map_start, map_len);

        if map_len == 0 {
            sorted_map_entries_vec.push(Arc::new(map_entries));
            continue;
        }

        // Sort the entry-indices of the map by their keys in ascending order.
        let map_keys = map_entries.column(0);
        let sort_options = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let sorted_indices = sort_to_indices(&map_keys, Some(sort_options), None)?;

        // Get the sorted map entries using the sorted indices and add it to the sorted map vector.
        let sorted_map_entries = take(&map_entries, &sorted_indices, None)?;
        sorted_map_entries_vec.push(sorted_map_entries);
    }

    // Flatten the sorted map entries into a single StructArray.
    let sorted_map_entries_arr: Vec<&dyn Array> = sorted_map_entries_vec
        .iter()
        .map(|arr| arr.as_ref())
        .collect();
    let combined_sorted_map_entries = concat(&sorted_map_entries_arr)?;
    let sorted_map_struct = combined_sorted_map_entries
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    // Create a new MapArray with the sorted entries while preserving the original metadata.
    // Note that then even though the map is sorted, the is_sorted flag is been set from the
    // original MapArray which may be false. Although, this might be less efficient, it is has been
    // done to keep the schema consistent.
    let sorted_map_arr = Arc::new(MapArray::try_new(
        Arc::<arrow::datatypes::Field>::clone(map_field),
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
    use crate::{
        test_create_map_array, test_create_nested_map_array, test_verify_result_equals_map,
    };
    use arrow::array::builder::{Int32Builder, MapBuilder, StringBuilder};
    use arrow::array::{Int32Array, MapFieldNames, StringArray};
    use datafusion::common::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_map_sort_with_string_keys() {
        let keys_arg = [
            vec![
                Some("c".to_string()),
                Some("a".to_string()),
                Some("b".to_string()),
            ],
            vec![
                Some("z".to_string()),
                Some("y".to_string()),
                Some("x".to_string()),
            ],
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string()),
            ],
            vec![
                Some("fusion".to_string()),
                Some("comet".to_string()),
                Some("data".to_string()),
            ],
        ];
        let values_arg = [
            vec![Some(3), Some(1), Some(2)],
            vec![Some(30), Some(20), Some(10)],
            vec![Some(1), Some(2), Some(3)],
            vec![Some(300), Some(100), Some(200)],
        ];
        let validity = [true, true, true, true];

        let map_arr_arg = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            keys_arg,
            values_arg,
            validity
        );
        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = spark_map_sort(&args).unwrap();

        let expected_keys = [
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string()),
            ],
            vec![
                Some("x".to_string()),
                Some("y".to_string()),
                Some("z".to_string()),
            ],
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string()),
            ],
            vec![
                Some("comet".to_string()),
                Some("data".to_string()),
                Some("fusion".to_string()),
            ],
        ];
        let expected_values = [
            vec![Some(1), Some(2), Some(3)],
            vec![Some(10), Some(20), Some(30)],
            vec![Some(1), Some(2), Some(3)],
            vec![Some(100), Some(200), Some(300)],
        ];
        let expected_validity = [true, true, true, true];

        let expected_map_arr = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            expected_keys,
            expected_values,
            expected_validity
        );
        test_verify_result_equals_map!(StringArray, Int32Array, result, expected_map_arr);
    }

    #[test]
    fn test_map_sort_with_int_keys() {
        let keys_arg = [
            vec![Some(3), Some(2), Some(1)],
            vec![Some(100), Some(50), Some(20)],
            vec![Some(20), Some(50), Some(100)],
            vec![Some(-5), Some(0), Some(-1)],
        ];
        let values_arg = [
            vec![
                Some("three".to_string()),
                Some("two".to_string()),
                Some("one".to_string()),
            ],
            vec![
                Some("hundred".to_string()),
                Some("fifty".to_string()),
                Some("twenty".to_string()),
            ],
            vec![
                Some("twenty".to_string()),
                Some("fifty".to_string()),
                Some("hundred".to_string()),
            ],
            vec![
                Some("minus five".to_string()),
                Some("zero".to_string()),
                Some("minus one".to_string()),
            ],
        ];
        let validity = [true, true, true, true];

        let map_arr_arg = test_create_map_array!(
            Int32Builder::new(),
            StringBuilder::new(),
            keys_arg,
            values_arg,
            validity
        );
        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = spark_map_sort(&args).unwrap();

        let expected_keys = [
            vec![Some(1), Some(2), Some(3)],
            vec![Some(20), Some(50), Some(100)],
            vec![Some(20), Some(50), Some(100)],
            vec![Some(-5), Some(-1), Some(0)],
        ];
        let expected_values = [
            vec![
                Some("one".to_string()),
                Some("two".to_string()),
                Some("three".to_string()),
            ],
            vec![
                Some("twenty".to_string()),
                Some("fifty".to_string()),
                Some("hundred".to_string()),
            ],
            vec![
                Some("twenty".to_string()),
                Some("fifty".to_string()),
                Some("hundred".to_string()),
            ],
            vec![
                Some("minus five".to_string()),
                Some("minus one".to_string()),
                Some("zero".to_string()),
            ],
        ];
        let expected_validity = [true, true, true, true];

        let expected_map_arr = test_create_map_array!(
            Int32Builder::new(),
            StringBuilder::new(),
            expected_keys,
            expected_values,
            expected_validity
        );
        test_verify_result_equals_map!(Int32Array, StringArray, result, expected_map_arr);
    }

    #[test]
    fn test_map_sort_with_nested_maps() {
        let outer_keys = [
            // Map 1 keys.
            [Some("key_b_1".to_string()), Some("key_a_1".to_string())],
            // Map 2 keys.
            [Some("key_b_2".to_string()), Some("key_a_2".to_string())],
        ];
        let inner_map_data = [
            // Map 1 values, which is another map.
            [
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
            // Map 2 values, which is another map.
            [
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

        let map_arr_arg = test_create_nested_map_array!(
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

        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = spark_map_sort(&args).unwrap();

        let expected_outer_keys = [
            // Map 1 keys.
            [Some("key_a_1".to_string()), Some("key_b_1".to_string())],
            // Map 2 keys.
            [Some("key_a_2".to_string()), Some("key_b_2".to_string())],
        ];
        let expected_inner_map_data = vec![
            // Map 1 values are reordered with the outer keys with getting sorted.
            [
                (
                    vec![
                        Some("key_a_1=key0".to_string()),
                        Some("key_a_1=key1".to_string()),
                    ],
                    vec![Some("key_a_1=value0"), Some("key_a_1=value1")],
                    true,
                ),
                (
                    vec![
                        Some("key_b_1=key1".to_string()),
                        Some("key_b_1=key0".to_string()),
                    ],
                    vec![Some("key_b_1=value1"), Some("key_b_1=value0")],
                    true,
                ),
            ],
            // Map 2 values are reordered with the outer keys with getting sorted.
            [
                (
                    vec![
                        Some("key_a_2=key1".to_string()),
                        Some("key_a_2=key0".to_string()),
                    ],
                    vec![Some("key_a_2=value1"), Some("key_a_2=value0")],
                    true,
                ),
                (
                    vec![
                        Some("key_b_2=key1".to_string()),
                        Some("key_b_2=key0".to_string()),
                    ],
                    vec![Some("key_b_2=value1"), Some("key_b_2=value0")],
                    true,
                ),
            ],
        ];
        let expected_validity = [true, true];

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
            expected_outer_keys,
            expected_inner_map_data,
            expected_validity
        );

        test_verify_result_equals_map!(StringArray, MapArray, result, expected_map_arr);
    }

    #[test]
    fn test_map_sort_with_scalar_argument() {
        let map_array = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec![Some("b".to_string()), Some("a".to_string())]],
            vec![vec![Some(2), Some(1)]],
            vec![true]
        );

        let args = vec![ColumnarValue::Scalar(
            ScalarValue::try_from_array(&map_array, 0).unwrap(),
        )];
        let result = spark_map_sort(&args).unwrap();

        let expected_map_arr = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec![Some("a".to_string()), Some("b".to_string())]],
            vec![vec![Some(1), Some(2)]],
            vec![true]
        );
        test_verify_result_equals_map!(StringArray, Int32Array, result, expected_map_arr);
    }

    #[test]
    fn test_map_sort_with_empty_map() {
        let map_arr_arg = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![Vec::<Option<String>>::new()],
            vec![Vec::<Option<i32>>::new()],
            vec![false]
        );

        let args = vec![ColumnarValue::Array(Arc::new(map_arr_arg))];
        let result = spark_map_sort(&args).unwrap();
        let expected_map_arr = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![Vec::<Option<String>>::new()],
            vec![Vec::<Option<i32>>::new()],
            vec![false]
        );
        test_verify_result_equals_map!(StringArray, Int32Array, result, expected_map_arr);
    }

    #[test]
    fn test_map_sort_with_invalid_arguments() {
        let result = spark_map_sort(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects exactly one argument"));

        let map_array = test_create_map_array!(
            StringBuilder::new(),
            Int32Builder::new(),
            vec![vec![Some("a".to_string())]],
            vec![vec![Some(1)]],
            vec![true]
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
