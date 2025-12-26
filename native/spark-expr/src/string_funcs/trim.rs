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
use arrow::array::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

/// Trims whitespace from both ends of a string (Spark's TRIM function)
pub fn spark_trim(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() != 1 {
        return Err(datafusion::common::DataFusionError::Execution(
            format!("trim expects 1 argument, got {}", args.len()),
        ));
    }

    match &args[0] {
        ColumnarValue::Array(array) => {
            let result = trim_array(array, TrimType::Both)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                s.trim().to_string(),
            ))))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(
                s.trim().to_string(),
            ))))
        }
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)))
        }
        _ => Err(datafusion::common::DataFusionError::Execution(
            "trim expects string argument".to_string(),
        )),
    }
}

/// Trims whitespace from the left side of a string (Spark's LTRIM function)
pub fn spark_ltrim(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() != 1 {
        return Err(datafusion::common::DataFusionError::Execution(
            format!("ltrim expects 1 argument, got {}", args.len()),
        ));
    }

    match &args[0] {
        ColumnarValue::Array(array) => {
            let result = trim_array(array, TrimType::Left)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                s.trim_start().to_string(),
            ))))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(
                s.trim_start().to_string(),
            ))))
        }
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)))
        }
        _ => Err(datafusion::common::DataFusionError::Execution(
            "ltrim expects string argument".to_string(),
        )),
    }
}

/// Trims whitespace from the right side of a string (Spark's RTRIM function)
pub fn spark_rtrim(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    if args.len() != 1 {
        return Err(datafusion::common::DataFusionError::Execution(
            format!("rtrim expects 1 argument, got {}", args.len()),
        ));
    }

    match &args[0] {
        ColumnarValue::Array(array) => {
            let result = trim_array(array, TrimType::Right)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                s.trim_end().to_string(),
            ))))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(
                s.trim_end().to_string(),
            ))))
        }
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)))
        }
        _ => Err(datafusion::common::DataFusionError::Execution(
            "rtrim expects string argument".to_string(),
        )),
    }
}

/// Trims whitespace from both ends of a string (alias for trim, Spark's BTRIM function)
pub fn spark_btrim(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    spark_trim(args)
}

#[derive(Debug, Clone, Copy)]
enum TrimType {
    Left,
    Right,
    Both,
}

/// Generic function to trim string arrays
fn trim_array(array: &ArrayRef, trim_type: TrimType) -> DataFusionResult<ArrayRef> {
    let data_type = array.data_type();
    
    match data_type {
        DataType::Utf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "Failed to downcast to StringArray".to_string(),
                    )
                })?;
            Ok(Arc::new(trim_string_array(string_array, trim_type)))
        }
        DataType::LargeUtf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "Failed to downcast to LargeStringArray".to_string(),
                    )
                })?;
            Ok(Arc::new(trim_string_array(string_array, trim_type)))
        }
        _ => Err(datafusion::common::DataFusionError::Execution(format!(
            "trim expects string type, got {:?}",
            data_type
        ))),
    }
}

/// Optimized trim implementation for GenericStringArray
fn trim_string_array<O: OffsetSizeTrait>(
    array: &GenericStringArray<O>,
    trim_type: TrimType,
) -> GenericStringArray<O> {
    // Fast path: Check if any strings actually need trimming
    // If not, we can return a clone of the original array
    let needs_trimming = (0..array.len()).any(|i| {
        if array.is_null(i) {
            false
        } else {
            let s = array.value(i);
            match trim_type {
                TrimType::Left => s.starts_with(|c: char| c.is_whitespace()),
                TrimType::Right => s.ends_with(|c: char| c.is_whitespace()),
                TrimType::Both => {
                    s.starts_with(|c: char| c.is_whitespace())
                        || s.ends_with(|c: char| c.is_whitespace())
                }
            }
        }
    });

    if !needs_trimming {
        // No trimming needed, return a clone of the input
        return array.clone();
    }

    // Slow path: Build new array with trimmed strings
    let mut builder = arrow::array::GenericStringBuilder::<O>::with_capacity(
        array.len(),
        array.get_buffer_memory_size(),
    );

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            let s = array.value(i);
            let trimmed = match trim_type {
                TrimType::Left => s.trim_start(),
                TrimType::Right => s.trim_end(),
                TrimType::Both => s.trim(),
            };
            builder.append_value(trimmed);
        }
    }

    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_trim() {
        let input = StringArray::from(vec![
            Some("  hello  "),
            Some("world"),
            Some("  spaces  "),
            None,
        ]);
        let input_ref: ArrayRef = Arc::new(input);

        let result = trim_array(&input_ref, TrimType::Both).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_array.value(0), "hello");
        assert_eq!(result_array.value(1), "world");
        assert_eq!(result_array.value(2), "spaces");
        assert!(result_array.is_null(3));
    }

    #[test]
    fn test_ltrim() {
        let input = StringArray::from(vec![Some("  hello  "), Some("world  ")]);
        let input_ref: ArrayRef = Arc::new(input);

        let result = trim_array(&input_ref, TrimType::Left).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_array.value(0), "hello  ");
        assert_eq!(result_array.value(1), "world  ");
    }

    #[test]
    fn test_rtrim() {
        let input = StringArray::from(vec![Some("  hello  "), Some("  world")]);
        let input_ref: ArrayRef = Arc::new(input);

        let result = trim_array(&input_ref, TrimType::Right).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_array.value(0), "  hello");
        assert_eq!(result_array.value(1), "  world");
    }

    #[test]
    fn test_trim_no_whitespace_fast_path() {
        // Test the fast path where no trimming is needed
        let input = StringArray::from(vec![
            Some("hello"),
            Some("world"),
            Some("no spaces"),
            None,
        ]);
        let input_ref: ArrayRef = Arc::new(input.clone());

        let result = trim_array(&input_ref, TrimType::Both).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        // Verify values are correct
        assert_eq!(result_array.value(0), "hello");
        assert_eq!(result_array.value(1), "world");
        assert_eq!(result_array.value(2), "no spaces");
        assert!(result_array.is_null(3));
    }

    #[test]
    fn test_ltrim_no_whitespace() {
        // Test ltrim with strings that have no leading whitespace
        let input = StringArray::from(vec![Some("hello  "), Some("world")]);
        let input_ref: ArrayRef = Arc::new(input);

        let result = trim_array(&input_ref, TrimType::Left).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_array.value(0), "hello  ");
        assert_eq!(result_array.value(1), "world");
    }

    #[test]
    fn test_rtrim_no_whitespace() {
        // Test rtrim with strings that have no trailing whitespace
        let input = StringArray::from(vec![Some("  hello"), Some("world")]);
        let input_ref: ArrayRef = Arc::new(input);

        let result = trim_array(&input_ref, TrimType::Right).unwrap();
        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result_array.value(0), "  hello");
        assert_eq!(result_array.value(1), "world");
    }
}
