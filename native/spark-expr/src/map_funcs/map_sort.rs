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
use arrow::compute::{lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

/// Sorts map entries by key in ascending order, matching Spark's `MapSort` semantics.
/// Takes a single map argument and returns a new map with entries sorted by key.
pub fn spark_map_sort(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("map_sort function takes exactly one argument");
    }

    match &args[0] {
        ColumnarValue::Array(array) => {
            let result = sort_map_array(array)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(ScalarValue::Null) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
        ColumnarValue::Scalar(other) => {
            exec_err!("map_sort expects a map type, got: {:?}", other)
        }
    }
}

fn sort_map_array(array: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
    let map_array = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Internal("Expected MapArray".to_string()))?;

    // Extract the entries field from the input data type to preserve field names and metadata
    let (entries_field, ordered) = match map_array.data_type() {
        DataType::Map(field, ordered) => (Arc::clone(field), *ordered),
        other => {
            return exec_err!("Expected Map data type, got: {:?}", other);
        }
    };

    let entries = map_array.entries();
    let keys = entries.column(0);
    let values = entries.column(1);
    let offsets = map_array.offsets();

    let mut sorted_keys_arrays: Vec<ArrayRef> = Vec::new();
    let mut sorted_values_arrays: Vec<ArrayRef> = Vec::new();
    let mut new_offsets = Vec::with_capacity(map_array.len() + 1);
    new_offsets.push(0i32);

    for row_idx in 0..map_array.len() {
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let len = end - start;

        if len == 0 || map_array.is_null(row_idx) {
            new_offsets.push(*new_offsets.last().unwrap());
            continue;
        }

        let row_keys = keys.slice(start, len);
        let row_values = values.slice(start, len);

        if len == 1 {
            sorted_keys_arrays.push(row_keys);
            sorted_values_arrays.push(row_values);
            new_offsets.push(*new_offsets.last().unwrap() + len as i32);
            continue;
        }

        let sort_columns = vec![SortColumn {
            values: Arc::clone(&row_keys),
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        }];

        let indices = lexsort_to_indices(&sort_columns, None)?;
        let sorted_keys = take(&row_keys, &indices, None)?;
        let sorted_values = take(&row_values, &indices, None)?;

        sorted_keys_arrays.push(sorted_keys);
        sorted_values_arrays.push(sorted_values);
        new_offsets.push(*new_offsets.last().unwrap() + len as i32);
    }

    // Reconstruct using the original entries field to preserve field names and metadata
    let (key_field, value_field) = map_array.entries_fields();
    let key_field = Arc::new(key_field.clone());
    let value_field = Arc::new(value_field.clone());

    let (sorted_keys_col, sorted_values_col) = if sorted_keys_arrays.is_empty() {
        (
            arrow::array::new_empty_array(keys.data_type()),
            arrow::array::new_empty_array(values.data_type()),
        )
    } else {
        let keys_refs: Vec<&dyn Array> = sorted_keys_arrays.iter().map(|a| a.as_ref()).collect();
        let values_refs: Vec<&dyn Array> =
            sorted_values_arrays.iter().map(|a| a.as_ref()).collect();
        (
            arrow::compute::concat(&keys_refs)?,
            arrow::compute::concat(&values_refs)?,
        )
    };

    let sorted_entries = StructArray::new(
        vec![key_field, value_field].into(),
        vec![sorted_keys_col, sorted_values_col],
        None,
    );

    Ok(Arc::new(MapArray::new(
        entries_field,
        arrow::buffer::OffsetBuffer::new(new_offsets.into()),
        sorted_entries,
        map_array.nulls().cloned(),
        ordered,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::NullBufferBuilder;
    use arrow::array::{Int32Array, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields};
    use datafusion::logical_expr::ColumnarValue;

    fn make_map_array(
        key_values: &[i32],
        str_values: &[&str],
        offsets: &[i32],
        nulls: Option<Vec<bool>>,
    ) -> ArrayRef {
        let keys = Arc::new(Int32Array::from(key_values.to_vec())) as ArrayRef;
        let values = Arc::new(StringArray::from(
            str_values.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
        )) as ArrayRef;

        let key_field = Arc::new(Field::new("key", DataType::Int32, false));
        let value_field = Arc::new(Field::new("value", DataType::Utf8, true));

        let entries = StructArray::new(
            Fields::from(vec![Arc::clone(&key_field), Arc::clone(&value_field)]),
            vec![keys, values],
            None,
        );

        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entries.fields().clone()),
            false,
        ));

        let null_buffer = nulls.map(|n| {
            let mut builder = NullBufferBuilder::new(n.len());
            for v in n {
                builder.append(v);
            }
            builder.finish().unwrap()
        });

        Arc::new(MapArray::new(
            entries_field,
            OffsetBuffer::new(offsets.to_vec().into()),
            entries,
            null_buffer,
            false,
        ))
    }

    fn get_sorted_keys(result: &ArrayRef) -> Vec<i32> {
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        let entries = map.entries();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        keys.values().to_vec()
    }

    fn get_sorted_values(result: &ArrayRef) -> Vec<String> {
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        let entries = map.entries();
        let vals = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..vals.len()).map(|i| vals.value(i).to_string()).collect()
    }

    #[test]
    fn test_sort_integer_keys() {
        let array = make_map_array(&[3, 1, 2], &["c", "a", "b"], &[0, 3], None);
        let args = vec![ColumnarValue::Array(array)];
        let result = spark_map_sort(&args).unwrap();

        match result {
            ColumnarValue::Array(ref arr) => {
                assert_eq!(get_sorted_keys(arr), vec![1, 2, 3]);
                assert_eq!(get_sorted_values(arr), vec!["a", "b", "c"]);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_sort_multiple_rows() {
        // Row 0: {3->c, 1->a}, Row 1: {5->e, 4->d}
        let array = make_map_array(&[3, 1, 5, 4], &["c", "a", "e", "d"], &[0, 2, 4], None);
        let args = vec![ColumnarValue::Array(array)];
        let result = spark_map_sort(&args).unwrap();

        match result {
            ColumnarValue::Array(ref arr) => {
                assert_eq!(get_sorted_keys(arr), vec![1, 3, 4, 5]);
                assert_eq!(get_sorted_values(arr), vec!["a", "c", "d", "e"]);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_sort_with_empty_map() {
        // Row 0: {2->b, 1->a}, Row 1: empty, Row 2: {3->c}
        let array = make_map_array(&[2, 1, 3], &["b", "a", "c"], &[0, 2, 2, 3], None);
        let args = vec![ColumnarValue::Array(array)];
        let result = spark_map_sort(&args).unwrap();

        match result {
            ColumnarValue::Array(ref arr) => {
                let map = arr.as_any().downcast_ref::<MapArray>().unwrap();
                assert_eq!(map.len(), 3);
                assert_eq!(get_sorted_keys(arr), vec![1, 2, 3]);
                // Verify offsets: row 0 has 2 entries, row 1 has 0, row 2 has 1
                let offsets = map.offsets();
                assert_eq!(offsets.as_ref(), &[0, 2, 2, 3]);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_sort_with_null_map() {
        // Row 0: {2->b, 1->a}, Row 1: null
        let array = make_map_array(&[2, 1], &["b", "a"], &[0, 2, 2], Some(vec![true, false]));
        let args = vec![ColumnarValue::Array(array)];
        let result = spark_map_sort(&args).unwrap();

        match result {
            ColumnarValue::Array(ref arr) => {
                let map = arr.as_any().downcast_ref::<MapArray>().unwrap();
                assert_eq!(map.len(), 2);
                assert!(!map.is_null(0));
                assert!(map.is_null(1));
                assert_eq!(get_sorted_keys(arr), vec![1, 2]);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_sort_single_entry_map() {
        let array = make_map_array(&[42], &["answer"], &[0, 1], None);
        let args = vec![ColumnarValue::Array(array)];
        let result = spark_map_sort(&args).unwrap();

        match result {
            ColumnarValue::Array(ref arr) => {
                assert_eq!(get_sorted_keys(arr), vec![42]);
                assert_eq!(get_sorted_values(arr), vec!["answer"]);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_sort_all_empty_maps() {
        // Two empty maps
        let array = make_map_array(&[], &[], &[0, 0, 0], None);
        let args = vec![ColumnarValue::Array(array)];
        let result = spark_map_sort(&args).unwrap();

        match result {
            ColumnarValue::Array(ref arr) => {
                let map = arr.as_any().downcast_ref::<MapArray>().unwrap();
                assert_eq!(map.len(), 2);
                let offsets = map.offsets();
                assert_eq!(offsets.as_ref(), &[0, 0, 0]);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_sort_preserves_field_names() {
        // Use custom field names to verify preservation
        let keys = Arc::new(Int32Array::from(vec![2, 1])) as ArrayRef;
        let values = Arc::new(StringArray::from(vec!["b", "a"])) as ArrayRef;
        let key_field = Arc::new(Field::new("my_key", DataType::Int32, false));
        let value_field = Arc::new(Field::new("my_value", DataType::Utf8, true));
        let entries = StructArray::new(
            Fields::from(vec![Arc::clone(&key_field), Arc::clone(&value_field)]),
            vec![keys, values],
            None,
        );
        let entries_field = Arc::new(Field::new(
            "my_entries",
            DataType::Struct(entries.fields().clone()),
            false,
        ));
        let map = MapArray::new(
            entries_field,
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        );
        let array: ArrayRef = Arc::new(map);
        let args = vec![ColumnarValue::Array(array)];
        let result = spark_map_sort(&args).unwrap();

        match result {
            ColumnarValue::Array(ref arr) => {
                let map = arr.as_any().downcast_ref::<MapArray>().unwrap();
                let (kf, vf) = map.entries_fields();
                assert_eq!(kf.name(), "my_key");
                assert_eq!(vf.name(), "my_value");
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_sort_null_scalar() {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Null)];
        let result = spark_map_sort(&args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Null) => {}
            _ => panic!("Expected null scalar result"),
        }
    }

    #[test]
    fn test_wrong_arg_count() {
        let array = make_map_array(&[1], &["a"], &[0, 1], None);
        let args = vec![
            ColumnarValue::Array(array.clone()),
            ColumnarValue::Array(array),
        ];
        assert!(spark_map_sort(&args).is_err());
    }
}
