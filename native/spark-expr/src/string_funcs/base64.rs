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

use arrow::array::{Array, GenericBinaryArray, OffsetSizeTrait, StringArray};
use arrow::datatypes::DataType;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use datafusion::common::{exec_err, DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;

/// Spark `base64(bin)`: encodes a binary value as a padded base64 string.
///
/// The second argument is a boolean `chunk` flag mirroring Spark's
/// `spark.sql.chunkBase64String.enabled`. When `chunk` is true (Spark's default, and the only
/// behavior on Spark 3.4), the output matches `java.util.Base64.getMimeEncoder()`: lines of at most
/// 76 characters joined by a CRLF (`\r\n`), with no trailing separator. When false, the output is a
/// single unwrapped line, matching `java.util.Base64.getMimeEncoder(-1, [])`.
pub fn spark_base64(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 2 {
        return exec_err!("base64 expects exactly two arguments, got {}", args.len());
    }
    let chunk = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(chunk))) => *chunk,
        other => return exec_err!("base64 expects a boolean chunk flag, got {other:?}"),
    };
    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Binary => Ok(ColumnarValue::Array(Arc::new(encode_array(
                array
                    .as_any()
                    .downcast_ref::<GenericBinaryArray<i32>>()
                    .unwrap(),
                chunk,
            )))),
            DataType::LargeBinary => Ok(ColumnarValue::Array(Arc::new(encode_array(
                array
                    .as_any()
                    .downcast_ref::<GenericBinaryArray<i64>>()
                    .unwrap(),
                chunk,
            )))),
            other => exec_err!("base64 expects a binary argument, got {other}"),
        },
        ColumnarValue::Scalar(ScalarValue::Binary(value))
        | ColumnarValue::Scalar(ScalarValue::LargeBinary(value)) => {
            let encoded = value.as_ref().map(|bytes| encode(bytes, chunk));
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(encoded)))
        }
        ColumnarValue::Scalar(other) => {
            exec_err!("base64 expects a binary argument, got {other}")
        }
    }
}

fn encode_array<O: OffsetSizeTrait>(array: &GenericBinaryArray<O>, chunk: bool) -> StringArray {
    array
        .iter()
        .map(|value| value.map(|bytes| encode(bytes, chunk)))
        .collect()
}

fn encode(bytes: &[u8], chunk: bool) -> String {
    let encoded = BASE64_STANDARD.encode(bytes);
    if chunk {
        chunk_into_lines(&encoded)
    } else {
        encoded
    }
}

/// Wrap a base64 string into lines of at most 76 characters joined by CRLF, with no trailing
/// separator. Matches `java.util.Base64.getMimeEncoder()`. base64 output is pure ASCII, so byte
/// offsets and character offsets coincide.
fn chunk_into_lines(encoded: &str) -> String {
    const LINE_LEN: usize = 76;
    if encoded.len() <= LINE_LEN {
        return encoded.to_string();
    }
    let separators = (encoded.len() - 1) / LINE_LEN;
    let mut out = String::with_capacity(encoded.len() + separators * 2);
    let mut offset = 0;
    while offset < encoded.len() {
        if offset > 0 {
            out.push_str("\r\n");
        }
        let end = (offset + LINE_LEN).min(encoded.len());
        out.push_str(&encoded[offset..end]);
        offset = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unchunked_is_a_single_line() {
        assert_eq!(encode(b"abc", false), "YWJj");
        assert_eq!(
            encode(&[b'a'; 58], false),
            BASE64_STANDARD.encode([b'a'; 58])
        );
        assert_eq!(encode(b"", false), "");
    }

    #[test]
    fn chunked_matches_java_mime_encoder() {
        // Empty and short inputs are returned without a separator.
        assert_eq!(encode(b"", true), "");
        assert_eq!(encode(b"abc", true), "YWJj");
        // 57 input bytes encode to exactly 76 characters: the line limit, so no separator.
        let exactly_76 = encode(&[b'a'; 57], true);
        assert_eq!(exactly_76.len(), 76);
        assert!(!exactly_76.contains("\r\n"));
        // 58 input bytes encode to 80 characters, wrapping once after 76.
        let wrapped_once = encode(&[b'b'; 58], true);
        assert_eq!(wrapped_once.matches("\r\n").count(), 1);
        assert_eq!(wrapped_once.split("\r\n").next().unwrap().len(), 76);
        // 120 input bytes encode to 160 characters, wrapping twice (76 + 76 + 8).
        let wrapped_twice = encode(&[b'c'; 120], true);
        assert_eq!(wrapped_twice.matches("\r\n").count(), 2);
        let lines: Vec<&str> = wrapped_twice.split("\r\n").collect();
        assert_eq!(
            lines.iter().map(|l| l.len()).collect::<Vec<_>>(),
            vec![76, 76, 8]
        );
    }
}
