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

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, GenericListArray, GenericStringArray, GenericStringBuilder,
    ListArray, OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
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

            Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(
                list_array,
            ))))
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

    match string_array.data_type() {
        DataType::Utf8 => {
            split_generic::<i32>(as_generic_string_array::<i32>(string_array)?, &regex, limit)
        }
        DataType::LargeUtf8 => {
            split_generic::<i64>(as_generic_string_array::<i64>(string_array)?, &regex, limit)
        }
        _ => exec_err!(
            "split expects Utf8 or LargeUtf8 string array, got {:?}",
            string_array.data_type()
        ),
    }
}

fn split_generic<O: OffsetSizeTrait>(
    string_array: &GenericStringArray<O>,
    regex: &Regex,
    limit: i32,
) -> DataFusionResult<ColumnarValue> {
    let len = string_array.len();
    let mut offsets: Vec<O> = Vec::with_capacity(len + 1);
    let mut values_builder = GenericStringBuilder::<O>::new();
    offsets.push(O::usize_as(0));

    // Bulk-NULL: output null mask equals input's, so reuse it instead of
    // tracking per-row in a NullBufferBuilder. Null rows contribute no parts
    // (offset does not advance) and the cloned NullBuffer marks them.
    for i in 0..len {
        if !string_array.is_null(i) {
            let s = string_array.value(i);
            push_split_parts(s, regex, limit, &mut values_builder);
        }
        offsets.push(O::usize_as(values_builder.len()));
    }

    let values_array = Arc::new(values_builder.finish()) as ArrayRef;
    let item_type = if O::IS_LARGE {
        DataType::LargeUtf8
    } else {
        DataType::Utf8
    };
    let field = Arc::new(Field::new("item", item_type, false));
    let list_array = GenericListArray::<O>::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values_array,
        string_array.nulls().cloned(),
    );

    Ok(ColumnarValue::Array(Arc::new(list_array)))
}

/// Push the splits of `string` into `builder`. Avoids materializing an
/// intermediate `Vec<String>` — appends each `&str` slice from the regex
/// iterator directly (the builder copies into its own buffer).
fn push_split_parts<O: OffsetSizeTrait>(
    string: &str,
    regex: &Regex,
    limit: i32,
    builder: &mut GenericStringBuilder<O>,
) {
    if limit == 0 {
        // limit = 0: split all, drop trailing empties. Need to know the end
        // before pushing, so collect borrowed slices first (no string copies).
        let mut parts: Vec<&str> = regex.split(string).collect();
        while parts.last().is_some_and(|s| s.is_empty()) {
            parts.pop();
        }
        if parts.is_empty() {
            builder.append_value("");
        } else {
            for p in parts {
                builder.append_value(p);
            }
        }
    } else if limit > 0 {
        // limit > 0: at most limit-1 splits.
        let mut last_end = 0;
        let cap = (limit - 1) as usize;
        for (count, mat) in regex.find_iter(string).enumerate() {
            if count >= cap {
                break;
            }
            builder.append_value(&string[last_end..mat.start()]);
            last_end = mat.end();
        }
        builder.append_value(&string[last_end..]);
    } else {
        // limit < 0: split all, keep trailing empties.
        for p in regex.split(string) {
            builder.append_value(p);
        }
    }
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
        while parts.last().is_some_and(|s| s.is_empty()) {
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

        for (count, mat) in regex.find_iter(string).enumerate() {
            if count >= (limit - 1) as usize {
                break;
            }
            parts.push(string[last_end..mat.start()].to_string());
            last_end = mat.end();
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

    #[test]
    fn test_split_with_nulls() {
        // Test that NULL inputs produce NULL outputs (not empty arrays)
        let string_array = Arc::new(StringArray::from(vec![
            Some("a,b,c"),
            None,
            Some("x,y"),
            None,
        ])) as ArrayRef;
        let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
        let args = vec![ColumnarValue::Array(string_array), pattern];

        let result = spark_split(&args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let list_array = arr.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list_array.len(), 4);
                // First row: valid ["a", "b", "c"]
                assert!(!list_array.is_null(0));
                // Second row: NULL
                assert!(list_array.is_null(1));
                // Third row: valid ["x", "y"]
                assert!(!list_array.is_null(2));
                // Fourth row: NULL
                assert!(list_array.is_null(3));
            }
            _ => panic!("Expected Array result"),
        }
    }

    #[test]
    fn test_split_empty_string() {
        // Test that empty string input produces array with single empty string
        let parts = split_string("", ",", -1).unwrap();
        assert_eq!(parts, vec![""]);
    }
}
