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

use std::sync::Arc;

use arrow_array::{Array, OffsetSizeTrait};
use arrow_schema::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{cast::as_generic_string_array, exec_err, DataFusionError, ScalarValue};

fn unhex(string: &str, result: &mut Vec<u8>) -> Result<(), DataFusionError> {
    if string.is_empty() {
        return Ok(());
    }

    // Adjust the string if it has an odd length, and prepare to add a padding byte if needed.
    let needs_padding = string.len() % 2 != 0;
    let adjusted_string = if needs_padding { &string[1..] } else { string };

    let mut iter = adjusted_string.chars().peekable();
    while let (Some(high_char), Some(low_char)) = (iter.next(), iter.next()) {
        let high = high_char
            .to_digit(16)
            .ok_or_else(|| DataFusionError::Internal("Invalid hex character".to_string()))?;
        let low = low_char
            .to_digit(16)
            .ok_or_else(|| DataFusionError::Internal("Invalid hex character".to_string()))?;

        result.push((high << 4 | low) as u8);
    }

    if needs_padding {
        result.push(0);
    }

    Ok(())
}

fn spark_unhex_inner<T: OffsetSizeTrait>(
    array: &ColumnarValue,
    fail_on_error: bool,
) -> Result<ColumnarValue, DataFusionError> {
    match array {
        ColumnarValue::Array(array) => {
            let string_array = as_generic_string_array::<T>(array)?;

            let mut builder = arrow::array::BinaryBuilder::new();
            let mut encoded = Vec::new();

            for i in 0..string_array.len() {
                let string = string_array.value(i);

                if unhex(string, &mut encoded).is_ok() {
                    builder.append_value(encoded.as_slice());
                    encoded.clear();
                } else if fail_on_error {
                    return exec_err!("Input to unhex is not a valid hex string: {string}");
                } else {
                    builder.append_null();
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(string))) => {
            let mut encoded = Vec::new();

            if unhex(string, &mut encoded).is_ok() {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(encoded))))
            } else if fail_on_error {
                exec_err!("Input to unhex is not a valid hex string: {string}")
            } else {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
        }
        _ => {
            exec_err!(
                "The first argument must be a string scalar or array, but got: {:?}",
                array
            )
        }
    }
}

pub(super) fn spark_unhex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() > 2 {
        return exec_err!("unhex takes at most 2 arguments, but got: {}", args.len());
    }

    let val_to_unhex = &args[0];
    let fail_on_error = if args.len() == 2 {
        match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error))) => *fail_on_error,
            _ => {
                return exec_err!(
                    "The second argument must be boolean scalar, but got: {:?}",
                    args[1]
                );
            }
        }
    } else {
        false
    };

    match val_to_unhex.data_type() {
        DataType::Utf8 => spark_unhex_inner::<i32>(val_to_unhex, fail_on_error),
        DataType::LargeUtf8 => spark_unhex_inner::<i64>(val_to_unhex, fail_on_error),
        other => exec_err!(
            "The first argument must be a string scalar or array, but got: {:?}",
            other
        ),
    }
}

#[cfg(test)]
mod test {
    use super::unhex;

    #[test]
    fn test_unhex() -> Result<(), Box<dyn std::error::Error>> {
        let mut result = Vec::new();

        unhex("537061726B2053514C", &mut result)?;
        let result_str = std::str::from_utf8(&result)?;
        assert_eq!(result_str, "Spark SQL");
        result.clear();

        assert!(unhex("hello", &mut result).is_err());
        result.clear();

        unhex("", &mut result)?;
        assert!(result.is_empty());

        Ok(())
    }
}
