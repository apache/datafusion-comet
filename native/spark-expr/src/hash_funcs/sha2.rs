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

use arrow::array::{Array, StringBuilder};
use datafusion::common::cast::{as_int32_array, as_string_array};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible SHA2 function that supports both Scalar and Array arguments
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSha2 {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkSha2 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSha2 {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkSha2 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[arrow::datatypes::DataType]) -> Result<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("sha2 expects exactly two arguments: input and num_bits");
        }
        spark_sha2(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Main entry point for SHA2 function that handles both Scalar and Array inputs
pub fn spark_sha2(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!("sha2 expects exactly two arguments");
    }

    let input = &args[0];
    let bits = &args[1];

    // Fast path: both arguments are scalars, return a scalar
    if let (ColumnarValue::Scalar(input_scalar), ColumnarValue::Scalar(bits_scalar)) = (input, bits)
    {
        return sha2_scalar(input_scalar, bits_scalar);
    }

    // At least one argument is an array: do full columnar evaluation and always return an array
    sha2_columnar(input, bits)
}

/// Scalar SHA2 evaluation, used only when both arguments are scalars.
fn sha2_scalar(input: &ScalarValue, bits: &ScalarValue) -> Result<ColumnarValue> {
    match (input, bits) {
        (ScalarValue::Utf8(None), _) | (_, ScalarValue::Int32(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        (ScalarValue::Utf8(Some(input_str)), ScalarValue::Int32(Some(num_bits))) => {
            let hash_hex = compute_sha2_hash(input_str.as_bytes(), *num_bits)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(hash_hex)))
        }
        _ => exec_err!(
            "sha2 expects (Utf8, Int32) scalar arguments, got ({:?}, {:?})",
            input,
            bits
        ),
    }
}

/// Columnar SHA2 evaluation with efficient scalar broadcasting.
/// This is used whenever at least one argument is an array and always returns an array.
fn sha2_columnar(input: &ColumnarValue, bits: &ColumnarValue) -> Result<ColumnarValue> {
    // Determine number of rows based on any array argument
    let num_rows = match (input, bits) {
        (ColumnarValue::Array(arr), _) => arr.len(),
        (_, ColumnarValue::Array(arr)) => arr.len(),
        _ => 1,
    };

    let mut builder = StringBuilder::new();

    // Pattern match to avoid unnecessary array allocations
    match (input, bits) {
        // Both are arrays
        (ColumnarValue::Array(input_arr), ColumnarValue::Array(bits_arr)) => {
            let input_str_arr = as_string_array(input_arr)?;
            let bits_i32_arr = as_int32_array(bits_arr)?;

            for i in 0..num_rows {
                if input_str_arr.is_null(i) || bits_i32_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let input_bytes = input_str_arr.value(i).as_bytes();
                    let num_bits = bits_i32_arr.value(i);
                    match compute_sha2_hash(input_bytes, num_bits)? {
                        Some(s) => builder.append_value(&s),
                        None => builder.append_null(),
                    }
                }
            }
        }
        // Input is array, bits is scalar (most common case: sha2(column, 256))
        (ColumnarValue::Array(input_arr), ColumnarValue::Scalar(bits_scalar)) => {
            let input_str_arr = as_string_array(input_arr)?;
            let bits_val = match bits_scalar {
                ScalarValue::Int32(Some(v)) => *v,
                ScalarValue::Int32(None) => {
                    // If bits is NULL, all results are NULL
                    for _ in 0..num_rows {
                        builder.append_null();
                    }
                    return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
                }
                other => {
                    return exec_err!("sha2 expects Int32 bits parameter, got {:?}", other);
                }
            };

            for i in 0..num_rows {
                if input_str_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let input_bytes = input_str_arr.value(i).as_bytes();
                    match compute_sha2_hash(input_bytes, bits_val)? {
                        Some(s) => builder.append_value(&s),
                        None => builder.append_null(),
                    }
                }
            }
        }
        // Input is scalar, bits is array (rare case)
        (ColumnarValue::Scalar(input_scalar), ColumnarValue::Array(bits_arr)) => {
            let bits_i32_arr = as_int32_array(bits_arr)?;
            let input_bytes = match input_scalar {
                ScalarValue::Utf8(Some(s)) => s.as_bytes(),
                ScalarValue::Utf8(None) => {
                    // If input is NULL, all results are NULL
                    for _ in 0..num_rows {
                        builder.append_null();
                    }
                    return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
                }
                other => {
                    return exec_err!("sha2 expects Utf8 input parameter, got {:?}", other);
                }
            };

            for i in 0..num_rows {
                if bits_i32_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let num_bits = bits_i32_arr.value(i);
                    match compute_sha2_hash(input_bytes, num_bits)? {
                        Some(s) => builder.append_value(&s),
                        None => builder.append_null(),
                    }
                }
            }
        }
        // Both scalars should have been handled by sha2_scalar, but be defensive
        (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => {
            return exec_err!("sha2_columnar should not be called with two scalars");
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

/// Compute SHA2 hash and return as hex string (or None for invalid num_bits)
fn compute_sha2_hash(data: &[u8], num_bits: i32) -> Result<Option<String>> {
    match num_bits {
        224 => {
            let mut hasher = Sha224::new();
            hasher.update(data);
            let result = hasher.finalize();
            Ok(Some(hex::encode(result)))
        }
        256 | 0 => {
            // Spark treats 0 as 256
            let mut hasher = Sha256::new();
            hasher.update(data);
            let result = hasher.finalize();
            Ok(Some(hex::encode(result)))
        }
        384 => {
            let mut hasher = Sha384::new();
            hasher.update(data);
            let result = hasher.finalize();
            Ok(Some(hex::encode(result)))
        }
        512 => {
            let mut hasher = Sha512::new();
            hasher.update(data);
            let result = hasher.finalize();
            Ok(Some(hex::encode(result)))
        }
        _ => {
            // Invalid bit length returns NULL in Spark
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use datafusion::common::cast::as_string_array;

    #[test]
    fn test_sha2_hash_256() {
        let result = compute_sha2_hash("hello".as_bytes(), 256)
            .unwrap()
            .unwrap();
        assert_eq!(
            result,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_sha2_hash_zero_as_256() {
        // Spark treats 0 as 256
        let result_256 = compute_sha2_hash("test".as_bytes(), 256).unwrap().unwrap();
        let result_0 = compute_sha2_hash("test".as_bytes(), 0).unwrap().unwrap();
        assert_eq!(result_256, result_0);
    }

    #[test]
    fn test_sha2_hash_invalid_bits() {
        // Invalid bit lengths return None (NULL in Spark)
        assert_eq!(compute_sha2_hash("hello".as_bytes(), 128).unwrap(), None);
        assert_eq!(compute_sha2_hash("hello".as_bytes(), -1).unwrap(), None);
        assert_eq!(compute_sha2_hash("hello".as_bytes(), 1024).unwrap(), None);
    }

    #[test]
    fn test_sha2_different_algorithms() {
        let input = "test".as_bytes();
        
        // SHA-224: 224 bits = 28 bytes = 56 hex chars
        let hash_224 = compute_sha2_hash(input, 224).unwrap().unwrap();
        assert_eq!(hash_224.len(), 56);

        // SHA-256: 256 bits = 32 bytes = 64 hex chars
        let hash_256 = compute_sha2_hash(input, 256).unwrap().unwrap();
        assert_eq!(hash_256.len(), 64);

        // SHA-384: 384 bits = 48 bytes = 96 hex chars
        let hash_384 = compute_sha2_hash(input, 384).unwrap().unwrap();
        assert_eq!(hash_384.len(), 96);

        // SHA-512: 512 bits = 64 bytes = 128 hex chars
        let hash_512 = compute_sha2_hash(input, 512).unwrap().unwrap();
        assert_eq!(hash_512.len(), 128);
    }

    #[test]
    fn test_sha2_scalar_both_scalars() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string())));
        let bits = ColumnarValue::Scalar(ScalarValue::Int32(Some(256)));
        
        let result = spark_sha2(&[input, bits]).unwrap();
        
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(hash))) => {
                assert_eq!(hash, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
            }
            _ => panic!("Expected scalar Utf8 result"),
        }
    }

    #[test]
    fn test_sha2_scalar_null_input() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let bits = ColumnarValue::Scalar(ScalarValue::Int32(Some(256)));
        
        let result = spark_sha2(&[input, bits]).unwrap();
        
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {},
            _ => panic!("Expected NULL scalar result"),
        }
    }

    #[test]
    fn test_sha2_scalar_null_bits() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".to_string())));
        let bits = ColumnarValue::Scalar(ScalarValue::Int32(None));
        
        let result = spark_sha2(&[input, bits]).unwrap();
        
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {},
            _ => panic!("Expected NULL scalar result"),
        }
    }

    #[test]
    fn test_sha2_array_scalar_common_case() {
        // Most common case: sha2(column, 256)
        let input_array = Arc::new(StringArray::from(vec![
            Some("hello"),
            Some("world"),
            None,
            Some("test"),
        ]));
        let input = ColumnarValue::Array(input_array);
        let bits = ColumnarValue::Scalar(ScalarValue::Int32(Some(256)));
        
        let result = spark_sha2(&[input, bits]).unwrap();
        
        match result {
            ColumnarValue::Array(arr) => {
                let string_arr = as_string_array(&arr).unwrap();
                assert_eq!(string_arr.len(), 4);
                assert_eq!(
                    string_arr.value(0),
                    "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                );
                assert_eq!(
                    string_arr.value(1),
                    "486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7"
                );
                assert!(string_arr.is_null(2)); // NULL input -> NULL output
                assert!(!string_arr.is_null(3));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_sha2_array_scalar_invalid_bits() {
        // Invalid bits value should produce all NULLs
        let input_array = Arc::new(StringArray::from(vec![Some("hello"), Some("world")]));
        let input = ColumnarValue::Array(input_array);
        let bits = ColumnarValue::Scalar(ScalarValue::Int32(Some(128))); // Invalid
        
        let result = spark_sha2(&[input, bits]).unwrap();
        
        match result {
            ColumnarValue::Array(arr) => {
                let string_arr = as_string_array(&arr).unwrap();
                assert_eq!(string_arr.len(), 2);
                assert!(string_arr.is_null(0));
                assert!(string_arr.is_null(1));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_sha2_array_array() {
        let input_array = Arc::new(StringArray::from(vec![
            Some("test"),
            Some("hello"),
            Some("world"),
        ]));
        let bits_array = Arc::new(Int32Array::from(vec![Some(256), Some(384), Some(512)]));
        
        let input = ColumnarValue::Array(input_array);
        let bits = ColumnarValue::Array(bits_array);
        
        let result = spark_sha2(&[input, bits]).unwrap();
        
        match result {
            ColumnarValue::Array(arr) => {
                let string_arr = as_string_array(&arr).unwrap();
                assert_eq!(string_arr.len(), 3);
                // Each row uses different algorithm
                assert_eq!(string_arr.value(0).len(), 64);  // SHA-256: 64 hex chars
                assert_eq!(string_arr.value(1).len(), 96);  // SHA-384: 96 hex chars
                assert_eq!(string_arr.value(2).len(), 128); // SHA-512: 128 hex chars
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_sha2_scalar_array() {
        // Rare case: scalar input with array bits
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".to_string())));
        let bits_array = Arc::new(Int32Array::from(vec![Some(224), Some(256), Some(384)]));
        let bits = ColumnarValue::Array(bits_array);
        
        let result = spark_sha2(&[input, bits]).unwrap();
        
        match result {
            ColumnarValue::Array(arr) => {
                let string_arr = as_string_array(&arr).unwrap();
                assert_eq!(string_arr.len(), 3);
                assert_eq!(string_arr.value(0).len(), 56);  // SHA-224
                assert_eq!(string_arr.value(1).len(), 64);  // SHA-256
                assert_eq!(string_arr.value(2).len(), 96);  // SHA-384
            }
            _ => panic!("Expected array result"),
        }
    }
}
