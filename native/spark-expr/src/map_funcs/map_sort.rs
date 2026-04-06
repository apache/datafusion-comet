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

/// Sorts map entries by key in ascending order, matching Spark's MapSort semantics.
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
        let keys_refs: Vec<&dyn Array> =
            sorted_keys_arrays.iter().map(|a| a.as_ref()).collect();
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
