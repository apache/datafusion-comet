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

use arrow::array::{Array, ArrayRef, MapArray};
use arrow::compute::{lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

pub fn spark_map_sort(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("map_sort function takes exactly one argument");
    }

    match &args[0] {
        ColumnarValue::Array(array) => {
            let result = spark_map_sort_array(array)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(scalar) => {
            let result = spark_map_sort_scalar(scalar)?;
            Ok(ColumnarValue::Scalar(result))
        }
    }
}

fn spark_map_sort_array(array: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
    let map_array = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Internal("Expected MapArray".to_string()))?;

    let entries = map_array.entries();
    let struct_array = entries
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .ok_or_else(|| DataFusionError::Internal("Expected StructArray for entries".to_string()))?;

    if struct_array.num_columns() != 2 {
        return exec_err!("Map entries must have exactly 2 columns (keys and values)");
    }

    let keys = struct_array.column(0);
    let values = struct_array.column(1);
    let offsets = map_array.offsets();

    let mut sorted_keys_arrays = Vec::new();
    let mut sorted_values_arrays = Vec::new();
    let mut new_offsets = Vec::with_capacity(map_array.len() + 1);
    new_offsets.push(0i32);

    for row_idx in 0..map_array.len() {
        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let len = end - start;

        if len == 0 {
            new_offsets.push(new_offsets[row_idx]);
            continue;
        }

        let row_keys = keys.slice(start, len);
        let row_values = values.slice(start, len);

        if len == 1 {
            sorted_keys_arrays.push(row_keys);
            sorted_values_arrays.push(row_values);
            new_offsets.push(new_offsets[row_idx] + len as i32);
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
        new_offsets.push(new_offsets[row_idx] + len as i32);
    }

    if sorted_keys_arrays.is_empty() {
        let key_field = Arc::new(Field::new(
            "key",
            keys.data_type().clone(),
            keys.is_nullable(),
        ));
        let value_field = Arc::new(Field::new(
            "value",
            values.data_type().clone(),
            values.is_nullable(),
        ));
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![Arc::clone(&key_field), Arc::clone(&value_field)].into()),
            false,
        ));

        let empty_keys = arrow::array::new_empty_array(keys.data_type());
        let empty_values = arrow::array::new_empty_array(values.data_type());
        let empty_entries = arrow::array::StructArray::new(
            vec![key_field, value_field].into(),
            vec![empty_keys, empty_values],
            None,
        );

        return Ok(Arc::new(MapArray::new(
            entries_field,
            arrow::buffer::OffsetBuffer::new(vec![0i32; map_array.len() + 1].into()),
            empty_entries,
            map_array.nulls().cloned(),
            false,
        )));
    }

    let sorted_keys_refs: Vec<&dyn Array> = sorted_keys_arrays.iter().map(|a| a.as_ref()).collect();
    let sorted_values_refs: Vec<&dyn Array> =
        sorted_values_arrays.iter().map(|a| a.as_ref()).collect();

    let concatenated_keys = arrow::compute::concat(&sorted_keys_refs)?;
    let concatenated_values = arrow::compute::concat(&sorted_values_refs)?;

    let key_field = Arc::new(Field::new(
        "key",
        keys.data_type().clone(),
        keys.is_nullable(),
    ));
    let value_field = Arc::new(Field::new(
        "value",
        values.data_type().clone(),
        values.is_nullable(),
    ));

    let sorted_entries = arrow::array::StructArray::new(
        vec![Arc::clone(&key_field), Arc::clone(&value_field)].into(),
        vec![concatenated_keys, concatenated_values],
        None,
    );

    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(vec![key_field, value_field].into()),
        false,
    ));

    Ok(Arc::new(MapArray::new(
        entries_field,
        arrow::buffer::OffsetBuffer::new(new_offsets.into()),
        sorted_entries,
        map_array.nulls().cloned(),
        false,
    )))
}

fn spark_map_sort_scalar(scalar: &ScalarValue) -> Result<ScalarValue, DataFusionError> {
    match scalar {
        ScalarValue::Null => Ok(ScalarValue::Null),
        _ => exec_err!(
            "map_sort scalar function only supports map types, got: {:?}",
            scalar
        ),
    }
}
