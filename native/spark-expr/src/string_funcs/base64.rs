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

use arrow::array::{Array, BinaryArray, StringBuilder};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use datafusion::common::{cast::as_binary_array, exec_err, DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;

/// Spark-compatible `base64` expression. Encodes the binary input using padded
/// RFC 4648 base64 without line breaks. This matches Spark's behavior when
/// `spark.sql.chunkBase64String.enabled=false`. Spark's default (chunked)
/// output, which inserts CRLF every 76 characters, is not produced here.
pub fn spark_base64(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("base64 expects 1 argument, got {}", args.len());
    }

    match &args[0] {
        ColumnarValue::Array(array) => {
            let binary = as_binary_array(array)?;
            Ok(ColumnarValue::Array(encode_array(binary)))
        }
        ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => Ok(ColumnarValue::Scalar(
            ScalarValue::Utf8(Some(STANDARD.encode(bytes))),
        )),
        ColumnarValue::Scalar(ScalarValue::Binary(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        other => exec_err!(
            "base64 expects a binary scalar or array, but got: {:?}",
            other.data_type()
        ),
    }
}

fn encode_array(array: &BinaryArray) -> Arc<dyn Array> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.value_data().len() * 2);
    for value in array.iter() {
        match value {
            Some(bytes) => builder.append_value(STANDARD.encode(bytes)),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BinaryBuilder;

    #[test]
    fn test_base64_array() {
        let mut builder = BinaryBuilder::new();
        builder.append_value(b"Spark SQL");
        builder.append_null();
        builder.append_value(b"");
        let input = ColumnarValue::Array(Arc::new(builder.finish()));

        let result = spark_base64(&[input]).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let s = arr
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                assert_eq!(s.value(0), "U3BhcmsgU1FM");
                assert!(s.is_null(1));
                assert_eq!(s.value(2), "");
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_base64_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Binary(Some(b"foo".to_vec())));
        let result = spark_base64(&[input]).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "Zm9v"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_base64_null_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Binary(None));
        let result = spark_base64(&[input]).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_base64_padded() {
        // 1 byte of input must produce 4 characters of base64 with two `=` padding chars.
        let mut builder = BinaryBuilder::new();
        builder.append_value(b"a");
        let input = ColumnarValue::Array(Arc::new(builder.finish()));

        let result = spark_base64(&[input]).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let s = arr
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                assert_eq!(s.value(0), "YQ==");
            }
            _ => panic!("expected array"),
        }
    }
}
