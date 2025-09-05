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

use std::any::Any;
use std::sync::Arc;
use arrow::array::{
    Array, ArrayRef, BooleanArray, MapArray, StructArray, Int32Array, Int64Array,
    Float32Array, Float64Array, StringArray,
};
use arrow::datatypes::{DataType};
use datafusion::common::{Result as DataFusionResult, ScalarValue, DataFusionError, internal_datafusion_err};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Clone, Copy, Debug)]
enum CompareOp {
    Greater,
    Less,
    GreaterEqual,
    LessEqual,
    Equal,
    NotEqual,
}

#[derive(Debug)]
pub struct SparkMapFilter {
    signature: Signature,
}

impl Default for SparkMapFilter {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMapFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_filter"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Internal(
                "map_filter expects exactly 2 arguments".to_string(),
            ));
        }

        match &arg_types[0] {
            DataType::Map(_, _) => Ok(arg_types[0].clone()),
            _ => Err(DataFusionError::Internal(
                "First argument to map_filter must be a map".to_string(),
            )),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let args: [ColumnarValue; 2] = args
            .args
            .try_into()
            .map_err(|_| internal_datafusion_err!("map_filter expects exactly two arguments"))?;
        
        spark_map_filter(&args[0], &args[1])
    }
}


pub fn spark_map_filter(map_arg: &ColumnarValue, lambda_arg: &ColumnarValue) -> DataFusionResult<ColumnarValue> {
    match map_arg {
        ColumnarValue::Array(map_array) => {
            let map_array = map_array.as_any().downcast_ref::<MapArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected MapArray".to_string()))?;
            let filtered = filter_map_with_lambda(map_array, lambda_arg)?;
            Ok(ColumnarValue::Array(Arc::new(filtered)))
        }
        ColumnarValue::Scalar(scalar_value) => {
            match scalar_value {
                ScalarValue::Map(map_array) => {
                    let filtered = filter_map_with_lambda(map_array, lambda_arg)?;
                    
                    // Convert filtered result back to scalar
                    if filtered.len() == 1 {
                        let scalar_map = ScalarValue::try_from_array(&filtered, 0)?;
                        Ok(ColumnarValue::Scalar(scalar_map))
                    } else {
                        // If filtering produces multiple maps, return as array
                        Ok(ColumnarValue::Array(Arc::new(filtered)))
                    }
                }
                ScalarValue::Null => {
                    // Handle null scalar map
                    Ok(ColumnarValue::Scalar(ScalarValue::Null))
                }
                _ => Err(DataFusionError::Internal(
                    "Invalid map data received".to_string()
                )),
            }
        }
    }
}

fn filter_map_with_lambda(map_array: &MapArray, lambda_arg: &ColumnarValue) -> DataFusionResult<MapArray> {
    let lambda_expr = extract_lambda_expression(lambda_arg)?;
    let entries = map_array.entries();
    let entries_str = entries.as_any().downcast_ref::<StructArray>().ok_or_else(||
        DataFusionError::Internal("Expected StructArray for map entries".to_string()))?;

    let keys = entries_str.column(0);
    let values = entries_str.column(1);
    let filter_mask = evaluate_lambda_on_pairs(&lambda_expr, keys, values)?;

    let filtered_entries = filter_struct_array(entries_str, &filter_mask)?;
    let offsets = compute_filtered_offsets(map_array, &filter_mask)?;
    let filtered_map = match map_array.data_type() {
        DataType::Map(field, _) => field.clone(),
        _ => return Err(DataFusionError::Internal(
            "Invalid map datatype".to_string())),
    };

    MapArray::try_new(filtered_map, offsets, filtered_entries, map_array.nulls()
        .cloned(), false,).map_err(|e| DataFusionError::Internal(format!("Arrow error: {}", e)))
}

fn extract_lambda_expression(lambda_arg: &ColumnarValue) -> DataFusionResult<String> {
    // Handle string based lambda expressions
    match lambda_arg {
        ColumnarValue::Scalar(scalar_value) => {
            match scalar_value {
                ScalarValue::Utf8(Some(expr)) => Ok(expr.clone()),
                _ => Err(DataFusionError::Internal("Lambda expression must be a
  string".to_string())),
            }
        }
        _ => Err(DataFusionError::Internal("Lambda expression must be a scalar
  string".to_string())),
    }
}

fn evaluate_lambda_on_pairs(
    lambda_expr: &str,
    keys: &ArrayRef,
    values: &ArrayRef
) -> DataFusionResult<BooleanArray> {
    let mut results = Vec::new();
    let len = keys.len();

    for i in 0..len {
        let result = evaluate_lambda_comparison(lambda_expr, keys, values, i)?;
        results.push(result);
    }

    Ok(BooleanArray::from(results))
}

fn evaluate_lambda_comparison(
    lambda_expr: &str,
    keys: &ArrayRef,
    values: &ArrayRef,
    index: usize
) -> DataFusionResult<Option<bool>> {
    // Handle null values
    if keys.is_null(index) || values.is_null(index) {
        return Ok(Some(false)); // Spark behavior: nulls are filtered out
    }

    // Parse the lambda expression
    if lambda_expr.contains(" >= ") {
        let parts: Vec<&str> = lambda_expr.split(" >= ").collect();
        if parts.len() == 2 {
            return compare_with_constant(keys, values, index, parts[0], parts[1], CompareOp::GreaterEqual);
        }
    } else if lambda_expr.contains(" <= ") {
        let parts: Vec<&str> = lambda_expr.split(" <= ").collect();
        if parts.len() == 2 {
            return compare_with_constant(keys, values, index, parts[0], parts[1], CompareOp::LessEqual);
        }
    } else if lambda_expr.contains(" > ") {
        let parts: Vec<&str> = lambda_expr.split(" > ").collect();
        if parts.len() == 2 {
            return compare_with_constant(keys, values, index, parts[0], parts[1], CompareOp::Greater);
        }
    } else if lambda_expr.contains(" < ") {
        let parts: Vec<&str> = lambda_expr.split(" < ").collect();
        if parts.len() == 2 {
            return compare_with_constant(keys, values, index, parts[0], parts[1], CompareOp::Less);
        }
    } else if lambda_expr.contains(" == ") {
        let parts: Vec<&str> = lambda_expr.split(" == ").collect();
        if parts.len() == 2 {
            return compare_with_constant(keys, values, index, parts[0], parts[1], CompareOp::Equal);
        }
    } else if lambda_expr.contains(" != ") {
        let parts: Vec<&str> = lambda_expr.split(" != ").collect();
        if parts.len() == 2 {
            return compare_with_constant(keys, values, index, parts[0], parts[1], CompareOp::NotEqual);
        }
    }

    // Default: keep all entries for unsupported expressions
    Ok(Some(true))
}

fn compare_with_constant(
    keys: &ArrayRef,
    values: &ArrayRef,
    index: usize,
    left_var: &str,
    right_constant: &str,
    op: CompareOp
) -> DataFusionResult<Option<bool>> {
    let left_var = left_var.trim();
    let right_constant = right_constant.trim();

    let (array_to_compare, data_type) = if left_var == "k" {
        (keys, keys.data_type())
    } else if left_var == "v" {
        (values, values.data_type())
    } else {
        // Unsupported variable, keep entry
        return Ok(Some(true));
    };

    match data_type {
        DataType::Int32 => {
            let arr = array_to_compare.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Internal("Expected Int32Array".to_string()))?;
            let left_val = arr.value(index) as i64;
            if let Ok(right_val) = right_constant.parse::<i64>() {
                Ok(Some(compare_integers(left_val, right_val, op)))
            } else {
                Ok(Some(true)) // Can't parse constant, keep entry
            }
        }
        DataType::Int64 => {
            let arr = array_to_compare.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("Expected Int64Array".to_string()))?;
            let left_val = arr.value(index);
            if let Ok(right_val) = right_constant.parse::<i64>() {
                Ok(Some(compare_integers(left_val, right_val, op)))
            } else {
                Ok(Some(true))
            }
        }
        DataType::Float32 => {
            let arr = array_to_compare.as_any().downcast_ref::<Float32Array>()
                .ok_or_else(|| DataFusionError::Internal("Expected Float32Array".to_string()))?;
            let left_val = arr.value(index) as f64;
            if let Ok(right_val) = right_constant.parse::<f64>() {
                Ok(Some(compare_floats(left_val, right_val, op)))
            } else {
                Ok(Some(true))
            }
        }
        DataType::Float64 => {
            let arr = array_to_compare.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("Expected Float64Array".to_string()))?;
            let left_val = arr.value(index);
            if let Ok(right_val) = right_constant.parse::<f64>() {
                Ok(Some(compare_floats(left_val, right_val, op)))
            } else {
                Ok(Some(true))
            }
        }
        DataType::Utf8 => {
            let arr = array_to_compare.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected StringArray".to_string()))?;
            let left_val = arr.value(index);
            Ok(Some(compare_strings(left_val, right_constant, op)))
        }
        _ => {
            // Unsupported type, keep entry
            Ok(Some(true))
        }
    }
}


// Separate comparison functions for different types
fn compare_integers(left: i64, right: i64, op: CompareOp) -> bool {
    match op {
        CompareOp::Greater => left > right,
        CompareOp::Less => left < right,
        CompareOp::GreaterEqual => left >= right,
        CompareOp::LessEqual => left <= right,
        CompareOp::Equal => left == right,
        CompareOp::NotEqual => left != right,
    }
}

fn compare_floats(left: f64, right: f64, op: CompareOp) -> bool {
    match op {
        CompareOp::Greater => left > right,
        CompareOp::Less => left < right,
        CompareOp::GreaterEqual => left >= right,
        CompareOp::LessEqual => left <= right,
        CompareOp::Equal => (left - right).abs() < f64::EPSILON,
        CompareOp::NotEqual => (left - right).abs() >= f64::EPSILON,
    }
}

fn compare_strings(left: &str, right: &str, op: CompareOp) -> bool {
    match op {
        CompareOp::Greater => left > right,        // Lexicographic comparison
        CompareOp::Less => left < right,
        CompareOp::GreaterEqual => left >= right,
        CompareOp::LessEqual => left <= right,
        CompareOp::Equal => left == right,
        CompareOp::NotEqual => left != right,
    }
}

fn filter_struct_array(struct_array: &StructArray, mask: &BooleanArray) -> DataFusionResult<StructArray> {
    use arrow::compute::filter;

    let mut filtered_columns = Vec::new();
    for column in struct_array.columns() {
        let filtered_column = filter(column, mask)
            .map_err(|e| DataFusionError::Internal(format!("Filter error: {}", e)))?;
        filtered_columns.push(filtered_column);
    }

    StructArray::try_new(
        struct_array.fields().clone(),
        filtered_columns,
        None,
    ).map_err(|e| DataFusionError::Internal(format!("Filter error: {}", e)))
}

fn compute_filtered_offsets(map_array: &MapArray, mask: &BooleanArray) ->
DataFusionResult<arrow::buffer::OffsetBuffer<i32>> {
    let mut new_offsets = vec![0i32];
    let mut current_offset = 0i32;

    for map_index in 0..map_array.len() {
        let start = map_array.value_offsets()[map_index] as usize;
        let end = map_array.value_offsets()[map_index + 1] as usize;

        // Count how many entries pass the filter
        let mut count = 0;
        for entry_index in start..end {
            if mask.value(entry_index) {
                count += 1;
            }
        }

        current_offset += count as i32;
        new_offsets.push(current_offset);
    }
    Ok(arrow::buffer::OffsetBuffer::new(new_offsets.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StructArray, Int32Array, Float32Array, Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use arrow::buffer::OffsetBuffer;
    use datafusion::common::ScalarValue;

    #[test]
    fn test_map_int_int() {
        let keys = Int32Array::from(vec![1, 2, 3]);
        let values = Int32Array::from(vec![10, 20, 30]);
        let entry_struct = StructArray::from(vec![
            (Arc::new(Field::new("key", DataType::Int32, false)), Arc::new(keys) as ArrayRef),
            (Arc::new(Field::new("value", DataType::Int32, false)), Arc::new(values) as ArrayRef),
        ]);
        let map_field = Arc::new(Field::new("entries", entry_struct.data_type().clone(), false));
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let map_array = MapArray::try_new(map_field, offsets, entry_struct, None, false).unwrap();
        
        let map_arg = ColumnarValue::Array(Arc::new(map_array));
        let lambda_arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("k > 1".to_string())));
        let result = spark_map_filter(&map_arg, &lambda_arg).unwrap();
        
        match result {
            ColumnarValue::Array(result_array) => {
                let map_result = result_array.as_any().downcast_ref::<MapArray>().unwrap();
                let entries = map_result.entries().as_any().downcast_ref::<StructArray>().unwrap();
                let keys = entries.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(keys.len(), 2);
                assert_eq!(keys.value(0), 2);
                assert_eq!(keys.value(1), 3);
            }
            _ => panic!("Expected a map array result"),
        }
    }

    #[test]
    fn test_map_int_float() {
        let keys = Int32Array::from(vec![1, 2, 3]);
        let values = Float32Array::from(vec![1.5, 2.5, 3.5]);
        let entry_struct = StructArray::from(vec![
            (Arc::new(Field::new("key", DataType::Int32, false)), Arc::new(keys) as ArrayRef),
            (Arc::new(Field::new("value", DataType::Float32, false)), Arc::new(values) as ArrayRef),
        ]);
        let map_field = Arc::new(Field::new("entries", entry_struct.data_type().clone(), false));
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let map_array = MapArray::try_new(map_field, offsets, entry_struct, None, false).unwrap();
        
        let map_arg = ColumnarValue::Array(Arc::new(map_array));
        let lambda_arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("v > 2.0".to_string())));
        let result = spark_map_filter(&map_arg, &lambda_arg).unwrap();
        
        match result {
            ColumnarValue::Array(result_array) => {
                let map_result = result_array.as_any().downcast_ref::<MapArray>().unwrap();
                let entries = map_result.entries().as_any().downcast_ref::<StructArray>().unwrap();
                let values = entries.column(1).as_any().downcast_ref::<Float32Array>().unwrap();
                assert_eq!(values.len(), 2);
                assert!((values.value(0) - 2.5).abs() < f32::EPSILON);
                assert!((values.value(1) - 3.5).abs() < f32::EPSILON);
            }
            _ => panic!("Expected a map array result"),
        }
    }

    #[test]
    fn test_map_float_float() {
        let keys = Float64Array::from(vec![1.1, 2.2, 3.3]);
        let values = Float64Array::from(vec![10.5, 20.5, 30.5]);
        let entry_struct = StructArray::from(vec![
            (Arc::new(Field::new("key", DataType::Float64, false)), Arc::new(keys) as ArrayRef),
            (Arc::new(Field::new("value", DataType::Float64, false)), Arc::new(values) as ArrayRef),
        ]);
        let map_field = Arc::new(Field::new("entries", entry_struct.data_type().clone(), false));
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let map_array = MapArray::try_new(map_field, offsets, entry_struct, None, false).unwrap();
        
        let map_arg = ColumnarValue::Array(Arc::new(map_array));
        let lambda_arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("k >= 2.2".to_string())));
        let result = spark_map_filter(&map_arg, &lambda_arg).unwrap();
        
        match result {
            ColumnarValue::Array(result_array) => {
                let map_result = result_array.as_any().downcast_ref::<MapArray>().unwrap();
                let entries = map_result.entries().as_any().downcast_ref::<StructArray>().unwrap();
                let keys = entries.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
                assert_eq!(keys.len(), 2);
                assert!((keys.value(0) - 2.2).abs() < f64::EPSILON);
                assert!((keys.value(1) - 3.3).abs() < f64::EPSILON);
            }
            _ => panic!("Expected a map array result"),
        }
    }

    #[test]
    fn test_map_string_float() {
        let keys = StringArray::from(vec!["a", "b", "c"]);
        let values = Float32Array::from(vec![1.0, 2.0, 3.0]);
        let entry_struct = StructArray::from(vec![
            (Arc::new(Field::new("key", DataType::Utf8, false)), Arc::new(keys) as ArrayRef),
            (Arc::new(Field::new("value", DataType::Float32, false)), Arc::new(values) as ArrayRef),
        ]);
        let map_field = Arc::new(Field::new("entries", entry_struct.data_type().clone(), false));
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let map_array = MapArray::try_new(map_field, offsets, entry_struct, None, false).unwrap();
        
        let map_arg = ColumnarValue::Array(Arc::new(map_array));
        let lambda_arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("k == b".to_string())));
        let result = spark_map_filter(&map_arg, &lambda_arg).unwrap();
        
        match result {
            ColumnarValue::Array(result_array) => {
                let map_result = result_array.as_any().downcast_ref::<MapArray>().unwrap();
                let entries = map_result.entries().as_any().downcast_ref::<StructArray>().unwrap();
                let keys = entries.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                let values = entries.column(1).as_any().downcast_ref::<Float32Array>().unwrap();
                assert_eq!(keys.len(), 1);
                assert_eq!(keys.value(0), "b");
                assert!((values.value(0) - 2.0).abs() < f32::EPSILON);
            }
            _ => panic!("Expected a map array result"),
        }
    }

    #[test]
    fn test_scalar_map() {
        let keys = Int32Array::from(vec![1, 2, 3]);
        let values = Int32Array::from(vec![10, 20, 30]);
        let entry_struct = StructArray::from(vec![
            (Arc::new(Field::new("key", DataType::Int32, false)), Arc::new(keys) as ArrayRef),
            (Arc::new(Field::new("value", DataType::Int32, false)), Arc::new(values) as ArrayRef),
        ]);
        let map_field = Arc::new(Field::new("entries", entry_struct.data_type().clone(), false));
        let offsets = OffsetBuffer::new(vec![0, 3].into());
        let map_array = MapArray::try_new(map_field, offsets, entry_struct, None, false).unwrap();
        
        let scalar_map = ColumnarValue::Scalar(ScalarValue::Map(Arc::new(map_array)));
        let lambda_arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("k > 1".to_string())));
        let result = spark_map_filter(&scalar_map, &lambda_arg).unwrap();
        
        match result {
            ColumnarValue::Scalar(ScalarValue::Map(result_map)) => {
                let entries = result_map.entries().as_any().downcast_ref::<StructArray>().unwrap();
                let keys = entries.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                assert_eq!(keys.len(), 2);
                assert_eq!(keys.value(0), 2);
                assert_eq!(keys.value(1), 3);
            }
            _ => panic!("Expected scalar map result"),
        }
    }

    #[test]
    fn test_invalid_arguments() {
        let scalar_int = ColumnarValue::Scalar(ScalarValue::Int32(Some(42)));
        let lambda_arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("k > 1".to_string())));
        let result = spark_map_filter(&scalar_int, &lambda_arg);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid map data received"));
    }
}




