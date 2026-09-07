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

use arrow::array::{Array, BinaryBuilder, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_generic_string_array, exec_err, DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;

/// Sentinel stored in `HEX_LUT` for bytes that are not valid hex digits. Its value `0xFF` has all
/// bits set, so OR-ing the two nibbles of a byte and comparing the result against `INVALID_HEX`
/// rejects the byte whenever either nibble is invalid: valid nibbles are `<= 0x0F`, so a valid
/// pair can never OR up to `0xFF`. This lets the decode loop test both nibbles with one comparison.
const INVALID_HEX: u8 = 0xFF;

/// Lookup table mapping each possible input byte to its hex value, or `INVALID_HEX` for bytes
/// that are not `0-9`, `A-F`, or `a-f`. Built at compile time so decoding is a single indexed
/// load per digit rather than a chain of range comparisons.
const HEX_LUT: [u8; 256] = build_hex_lut();

const fn build_hex_lut() -> [u8; 256] {
    let mut lut = [INVALID_HEX; 256];
    let mut i = 0;
    while i < 256 {
        lut[i] = match i as u8 {
            c @ b'0'..=b'9' => c - b'0',
            c @ b'A'..=b'F' => c - b'A' + 10,
            c @ b'a'..=b'f' => c - b'a' + 10,
            _ => INVALID_HEX,
        };
        i += 1;
    }
    lut
}

#[inline]
fn invalid_hex_digit() -> DataFusionError {
    DataFusionError::Execution("Input to unhex is not a valid hex digit".to_string())
}

/// Convert a hex string to binary and store the result in `result`. Returns an error if the input
/// is not a valid hex string.
fn unhex(hex_str: &str, result: &mut Vec<u8>) -> Result<(), DataFusionError> {
    let bytes = hex_str.as_bytes();
    // Each output byte consumes two hex digits (the optional leading nibble rounds up).
    result.reserve(bytes.len().div_ceil(2));

    let mut i = 0;

    if (bytes.len() & 0x01) != 0 {
        let v = HEX_LUT[bytes[0] as usize];
        if v == INVALID_HEX {
            return Err(invalid_hex_digit());
        }
        result.push(v);
        i += 1;
    }

    while i < bytes.len() {
        let first = HEX_LUT[bytes[i] as usize];
        let second = HEX_LUT[bytes[i + 1] as usize];
        if (first | second) == INVALID_HEX {
            return Err(invalid_hex_digit());
        }
        result.push((first << 4) | second);

        i += 2;
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

            let mut encoded = Vec::new();
            // Every two input hex digits produce one output byte, so the decoded data is at most
            // half the total input length. Preallocating both the offset and value buffers avoids
            // the repeated doublings a fresh `BinaryBuilder` would incur across the whole column.
            let mut builder = BinaryBuilder::with_capacity(
                string_array.len(),
                string_array.value_data().len() / 2,
            );

            for item in string_array.iter() {
                if let Some(s) = item {
                    if unhex(s, &mut encoded).is_ok() {
                        builder.append_value(encoded.as_slice());
                    } else if fail_on_error {
                        return exec_err!("Input to unhex is not a valid hex string: {s}");
                    } else {
                        builder.append_null();
                    }
                    encoded.clear();
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
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
        }
        _ => {
            exec_err!(
                "The first argument must be a string scalar or array, but got: {:?}",
                array
            )
        }
    }
}

/// Spark-compatible `unhex` expression
pub fn spark_unhex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
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
            "The first argument must be a Utf8 or LargeUtf8: {:?}",
            other
        ),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::make_array;
    use arrow::array::ArrayData;
    use arrow::array::{BinaryBuilder, StringBuilder};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ColumnarValue;

    use super::unhex;

    #[test]
    fn test_spark_unhex_null() -> Result<(), Box<dyn std::error::Error>> {
        let input = ArrayData::new_null(&arrow::datatypes::DataType::Utf8, 2);
        let output = ArrayData::new_null(&arrow::datatypes::DataType::Binary, 2);

        let input = ColumnarValue::Array(Arc::new(make_array(input)));
        let expected = ColumnarValue::Array(Arc::new(make_array(output)));

        let result = super::spark_unhex(&[input])?;

        match (result, expected) {
            (ColumnarValue::Array(result), ColumnarValue::Array(expected)) => {
                assert_eq!(*result, *expected);
                Ok(())
            }
            _ => Err("Unexpected result type".into()),
        }
    }

    #[test]
    fn test_partial_error() -> Result<(), Box<dyn std::error::Error>> {
        let mut input = StringBuilder::new();

        input.append_value("1CGG"); // 1C is ok, but GG is invalid
        input.append_value("537061726B2053514C"); // followed by valid

        let input = ColumnarValue::Array(Arc::new(input.finish()));
        let fail_on_error = ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)));

        let result = super::spark_unhex(&[input, fail_on_error])?;

        let mut expected = BinaryBuilder::new();
        expected.append_null();
        expected.append_value("Spark SQL".as_bytes());

        match (result, ColumnarValue::Array(Arc::new(expected.finish()))) {
            (ColumnarValue::Array(result), ColumnarValue::Array(expected)) => {
                assert_eq!(*result, *expected);

                Ok(())
            }
            _ => Err("Unexpected result type".into()),
        }
    }

    #[test]
    fn test_unhex_valid() -> Result<(), Box<dyn std::error::Error>> {
        let mut result = Vec::new();

        unhex("537061726B2053514C", &mut result)?;
        let result_str = std::str::from_utf8(&result)?;
        assert_eq!(result_str, "Spark SQL");
        result.clear();

        unhex("1C", &mut result)?;
        assert_eq!(result, vec![28]);
        result.clear();

        unhex("737472696E67", &mut result)?;
        assert_eq!(result, "string".as_bytes());
        result.clear();

        unhex("1", &mut result)?;
        assert_eq!(result, vec![1]);
        result.clear();

        Ok(())
    }

    #[test]
    fn test_odd_length() -> Result<(), Box<dyn std::error::Error>> {
        let mut result = Vec::new();

        unhex("A1B", &mut result)?;
        assert_eq!(result, vec![10, 27]);
        result.clear();

        unhex("0A1B", &mut result)?;
        assert_eq!(result, vec![10, 27]);
        result.clear();

        Ok(())
    }

    #[test]
    fn test_unhex_empty() {
        let mut result = Vec::new();

        // Empty hex string
        unhex("", &mut result).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_unhex_invalid() {
        let mut result = Vec::new();

        // Invalid hex strings
        assert!(unhex("##", &mut result).is_err());
        assert!(unhex("G123", &mut result).is_err());
        assert!(unhex("hello", &mut result).is_err());
        assert!(unhex("\0", &mut result).is_err());
    }
}
