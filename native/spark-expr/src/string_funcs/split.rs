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

use arrow::array::{Array, ArrayRef, GenericStringArray, ListArray};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{
    cast::as_generic_string_array, exec_err, DataFusionError, Result as DataFusionResult,
    ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use regex::Regex;
use std::sync::Arc;

/// Spark-compatible split function
/// Splits a string around matches of a regex pattern with optional limit
///
/// Arguments:
/// - string: The string to split
/// - pattern: The regex pattern to split on
/// - limit (optional): Controls the number of splits
///   - limit > 0: At most limit-1 splits, array length <= limit
///   - limit = 0: As many splits as possible, trailing empty strings removed
///   - limit < 0: As many splits as possible, trailing empty strings kept
pub fn spark_split(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "split expects 2 or 3 arguments (string, pattern, [limit]), got {}",
            args.len()
        );
    }

    // Get limit parameter (default to -1 if not provided)
    let limit = if args.len() == 3 {
        match &args[2] {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(l))) => *l,
            ColumnarValue::Scalar(ScalarValue::Int32(None)) => {
                // NULL limit, return NULL
                return Ok(ColumnarValue::Scalar(ScalarValue::Null));
            }
            _ => {
                return exec_err!("split limit argument must be an Int32 scalar");
            }
        }
    } else {
        -1
    };

    match (&args[0], &args[1]) {
        (ColumnarValue::Array(string_array), ColumnarValue::Scalar(ScalarValue::Utf8(pattern)))
        | (
            ColumnarValue::Array(string_array),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(pattern)),
        ) => {
            if pattern.is_none() {
                // NULL pattern returns NULL
                let null_array = new_null_list_array(string_array.len());
                return Ok(ColumnarValue::Array(null_array));
            }

            let pattern_str = pattern.as_ref().unwrap();
            split_array(string_array.as_ref(), pattern_str, limit)
        }
        (ColumnarValue::Scalar(ScalarValue::Utf8(string)), ColumnarValue::Scalar(pattern_val))
        | (
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(string)),
            ColumnarValue::Scalar(pattern_val),
        ) => {
            if string.is_none() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Null));
            }

            let pattern_str = match pattern_val {
                ScalarValue::Utf8(Some(p)) | ScalarValue::LargeUtf8(Some(p)) => p,
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Null));
                }
                _ => {
                    return exec_err!("split pattern must be a string");
                }
            };

            let result = split_string(string.as_ref().unwrap(), pattern_str, limit)?;
            let string_array = GenericStringArray::<i32>::from(result);
            let list_array = create_list_array(Arc::new(string_array));

            Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(list_array))))
        }
        _ => exec_err!("split expects (array, scalar) or (scalar, scalar) arguments"),
    }
}

fn split_array(
    string_array: &dyn arrow::array::Array,
    pattern: &str,
    limit: i32,
) -> DataFusionResult<ColumnarValue> {
    // Compile regex once for the entire array
    let regex = Regex::new(pattern).map_err(|e| {
        DataFusionError::Execution(format!("Invalid regex pattern '{}': {}", pattern, e))
    })?;

    let string_array = match string_array.data_type() {
        DataType::Utf8 => as_generic_string_array::<i32>(string_array)?,
        DataType::LargeUtf8 => {
            // Convert LargeUtf8 to Utf8 for processing
            let large_array = as_generic_string_array::<i64>(string_array)?;
            return split_large_string_array(&large_array, &regex, limit);
        }
        _ => {
            return exec_err!(
                "split expects Utf8 or LargeUtf8 string array, got {:?}",
                string_array.data_type()
            );
        }
    };

    // Build the result ListArray
    let mut offsets: Vec<i32> = Vec::with_capacity(string_array.len() + 1);
    let mut values: Vec<String> = Vec::new();
    offsets.push(0);

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            // NULL input produces empty array element (maintain position)
            offsets.push(offsets[i]);
        } else {
            let string_val = string_array.value(i);
            let parts = split_with_regex(string_val, &regex, limit);
            values.extend(parts);
            offsets.push(values.len() as i32);
        }
    }

    let values_array = Arc::new(GenericStringArray::<i32>::from(values)) as ArrayRef;
    let field = Arc::new(Field::new("item", DataType::Utf8, false));
    let list_array = ListArray::new(
        field,
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        values_array,
        None, // No nulls at list level
    );

    Ok(ColumnarValue::Array(Arc::new(list_array)))
}

fn split_large_string_array(
    string_array: &GenericStringArray<i64>,
    regex: &Regex,
    limit: i32,
) -> DataFusionResult<ColumnarValue> {
    let mut offsets: Vec<i32> = Vec::with_capacity(string_array.len() + 1);
    let mut values: Vec<String> = Vec::new();
    offsets.push(0);

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            offsets.push(offsets[i]);
        } else {
            let string_val = string_array.value(i);
            let parts = split_with_regex(string_val, regex, limit);
            values.extend(parts);
            offsets.push(values.len() as i32);
        }
    }

    let values_array = Arc::new(GenericStringArray::<i32>::from(values)) as ArrayRef;
    let field = Arc::new(Field::new("item", DataType::Utf8, false));
    let list_array = ListArray::new(
        field,
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        values_array,
        None,
    );

    Ok(ColumnarValue::Array(Arc::new(list_array)))
}

fn split_string(string: &str, pattern: &str, limit: i32) -> DataFusionResult<Vec<String>> {
    let regex = Regex::new(pattern).map_err(|e| {
        DataFusionError::Execution(format!("Invalid regex pattern '{}': {}", pattern, e))
    })?;

    Ok(split_with_regex(string, &regex, limit))
}

fn split_with_regex(string: &str, regex: &Regex, limit: i32) -> Vec<String> {
    if limit == 0 {
        // limit = 0: split as many times as possible, discard trailing empty strings
        let mut parts: Vec<String> = regex.split(string).map(|s| s.to_string()).collect();
        // Remove trailing empty strings
        while parts.last().map_or(false, |s| s.is_empty()) {
            parts.pop();
        }
        if parts.is_empty() {
            vec!["".to_string()]
        } else {
            parts
        }
    } else if limit > 0 {
        // limit > 0: at most limit-1 splits (array length <= limit)
        let mut parts: Vec<String> = Vec::new();
        let mut last_end = 0;
        let mut count = 0;

        for mat in regex.find_iter(string) {
            if count >= limit - 1 {
                break;
            }
            parts.push(string[last_end..mat.start()].to_string());
            last_end = mat.end();
            count += 1;
        }
        // Add the remaining string
        parts.push(string[last_end..].to_string());
        parts
    } else {
        // limit < 0: split as many times as possible, keep trailing empty strings
        regex.split(string).map(|s| s.to_string()).collect()
    }
}

fn create_list_array(values: ArrayRef) -> ListArray {
    let field = Arc::new(Field::new("item", DataType::Utf8, false));
    let offsets = vec![0i32, values.len() as i32];
    ListArray::new(
        field,
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        values,
        None,
    )
}

fn new_null_list_array(len: usize) -> ArrayRef {
    let field = Arc::new(Field::new("item", DataType::Utf8, false));
    let values = Arc::new(GenericStringArray::<i32>::from(Vec::<String>::new())) as ArrayRef;
    let offsets = vec![0i32; len + 1];
    let nulls = arrow::buffer::NullBuffer::new_null(len);

    Arc::new(ListArray::new(
        field,
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        values,
        Some(nulls),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_split_basic() {
        let string_array = Arc::new(StringArray::from(vec!["a,b,c", "x,y,z"])) as ArrayRef;
        let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let args = vec![ColumnarValue::Array(string_array), pattern];

        let result = spark_split(&args).unwrap();
        // Should produce [["a", "b", "c"], ["x", "y", "z"]]
        assert!(matches!(result, ColumnarValue::Array(_)));
    }

    #[test]
    fn test_split_with_limit() {
        let string_array = Arc::new(StringArray::from(vec!["a,b,c,d"])) as ArrayRef;
        let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let limit = ColumnarValue::Scalar(ScalarValue::Int32(Some(2)));
        let args = vec![ColumnarValue::Array(string_array), pattern, limit];

        let result = spark_split(&args).unwrap();
        // Should produce [["a", "b,c,d"]]
        assert!(matches!(result, ColumnarValue::Array(_)));
    }

    #[test]
    fn test_split_regex() {
        let parts = split_string("foo123bar456baz", r"\d+", -1).unwrap();
        assert_eq!(parts, vec!["foo", "bar", "baz"]);
    }

    #[test]
    fn test_split_limit_positive() {
        let parts = split_string("a,b,c,d,e", ",", 3).unwrap();
        assert_eq!(parts, vec!["a", "b", "c,d,e"]);
    }

    #[test]
    fn test_split_limit_zero() {
        let parts = split_string("a,b,c,,", ",", 0).unwrap();
        assert_eq!(parts, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_split_limit_negative() {
        let parts = split_string("a,b,c,,", ",", -1).unwrap();
        assert_eq!(parts, vec!["a", "b", "c", "", ""]);
    }
}
