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

//! String trimming functions

use arrow::array::{Array, ArrayRef, StringArray};
use datafusion::common::ScalarValue;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

/// Trim whitespace from both ends of a string
pub fn spark_trim(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    trim_impl(args, TrimType::Both)
}

/// Trim whitespace from both ends (alias for trim)
pub fn spark_btrim(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    trim_impl(args, TrimType::Both)
}

/// Trim whitespace from the left/start
pub fn spark_ltrim(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    trim_impl(args, TrimType::Left)
}

/// Trim whitespace from the right/end
pub fn spark_rtrim(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    trim_impl(args, TrimType::Right)
}

#[derive(Debug, Clone, Copy)]
enum TrimType {
    Left,
    Right,
    Both,
}

fn trim_impl(args: &[ColumnarValue], trim_type: TrimType) -> DataFusionResult<ColumnarValue> {
    if args.is_empty() || args.len() > 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            format!("trim expects 1 or 2 arguments, got {}", args.len()),
        ));
    }

    // For now, only support single argument (whitespace trimming)
    // TODO: Add support for custom trim characters (2-argument form)
    if args.len() == 2 {
        return Err(datafusion::error::DataFusionError::NotImplemented(
            "trim with custom characters not yet implemented".to_string(),
        ));
    }

    match &args[0] {
        ColumnarValue::Array(array) => {
            let result = trim_array(array, trim_type)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(scalar) => {
            let result = trim_scalar(scalar, trim_type)?;
            Ok(ColumnarValue::Scalar(result))
        }
    }
}

fn trim_array(array: &ArrayRef, trim_type: TrimType) -> DataFusionResult<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution("Expected string array".to_string())
        })?;

    let mut builder = arrow::array::StringBuilder::new();

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            builder.append_null();
        } else {
            let s = string_array.value(i);
            let trimmed = match trim_type {
                TrimType::Left => s.trim_start(),
                TrimType::Right => s.trim_end(),
                TrimType::Both => s.trim(),
            };
            builder.append_value(trimmed);
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn trim_scalar(scalar: &ScalarValue, trim_type: TrimType) -> DataFusionResult<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => {
            let trimmed = match trim_type {
                TrimType::Left => s.trim_start(),
                TrimType::Right => s.trim_end(),
                TrimType::Both => s.trim(),
            };
            Ok(ScalarValue::Utf8(Some(trimmed.to_string())))
        }
        ScalarValue::Utf8(None) => Ok(ScalarValue::Utf8(None)),
        ScalarValue::LargeUtf8(Some(s)) => {
            let trimmed = match trim_type {
                TrimType::Left => s.trim_start(),
                TrimType::Right => s.trim_end(),
                TrimType::Both => s.trim(),
            };
            Ok(ScalarValue::LargeUtf8(Some(trimmed.to_string())))
        }
        ScalarValue::LargeUtf8(None) => Ok(ScalarValue::LargeUtf8(None)),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "trim expects string argument".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim_whitespace() {
        let input = StringArray::from(vec![
            Some("  hello  "),
            Some("world"),
            Some("  spaces  "),
            None,
        ]);
        let input_ref: ArrayRef = Arc::new(input);
        let args = vec![ColumnarValue::Array(input_ref)];

        let result = spark_trim(&args).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let result_array = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(result_array.value(0), "hello");
                assert_eq!(result_array.value(1), "world");
                assert_eq!(result_array.value(2), "spaces");
                assert!(result_array.is_null(3));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_ltrim_whitespace() {
        let input = StringArray::from(vec![Some("  hello  "), Some("world  ")]);
        let input_ref: ArrayRef = Arc::new(input);
        let args = vec![ColumnarValue::Array(input_ref)];

        let result = spark_ltrim(&args).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let result_array = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(result_array.value(0), "hello  ");
                assert_eq!(result_array.value(1), "world  ");
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_rtrim_whitespace() {
        let input = StringArray::from(vec![Some("  hello  "), Some("  world")]);
        let input_ref: ArrayRef = Arc::new(input);
        let args = vec![ColumnarValue::Array(input_ref)];

        let result = spark_rtrim(&args).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let result_array = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(result_array.value(0), "  hello");
                assert_eq!(result_array.value(1), "  world");
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_trim_scalar() {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "  hello  ".to_string(),
        )))];

        let result = spark_trim(&args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s, "hello");
            }
            _ => panic!("Expected scalar result"),
        }
    }
}