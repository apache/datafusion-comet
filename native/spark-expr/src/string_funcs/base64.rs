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

use arrow::array::{
    Array, AsArray, GenericBinaryArray, GenericStringBuilder, OffsetSizeTrait, StringArray,
};
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
                array.as_binary::<i32>(),
                chunk,
            )))),
            DataType::LargeBinary => Ok(ColumnarValue::Array(Arc::new(encode_array(
                array.as_binary::<i64>(),
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

const LINE_LEN: usize = 76;

/// Length of the padded base64 encoding of `n` input bytes.
fn base64_encoded_len(n: usize) -> usize {
    n.div_ceil(3) * 4
}

/// Length after CRLF wrapping if `encoded_len` bytes are chunked at `LINE_LEN` chars per line.
fn chunked_len(encoded_len: usize) -> usize {
    if encoded_len == 0 {
        0
    } else {
        encoded_len + ((encoded_len - 1) / LINE_LEN) * 2
    }
}

/// Encodes `bytes` into `out`, wrapping at `LINE_LEN` when `chunk` is true. `out` is reused
/// across rows to avoid per-row heap allocations; the caller clears it before each call.
fn encode_into(bytes: &[u8], chunk: bool, out: &mut String) {
    if !chunk {
        BASE64_STANDARD.encode_string(bytes, out);
        return;
    }
    // Encode into a scratch, then wrap. Two passes are unavoidable because the base64 crate
    // does not emit CRLF for us and computing chunk boundaries mid-encode would require a
    // custom writer that carries per-row state.
    let unwrapped_len = base64_encoded_len(bytes.len());
    if unwrapped_len <= LINE_LEN {
        BASE64_STANDARD.encode_string(bytes, out);
        return;
    }
    // Reuse `out` for the wrapped result: encode into a temporary owned by the outer scratch,
    // then copy CRLF-wrapped chunks in. The temporary is short-lived per row, but the caller's
    // long-lived scratch avoids the per-row allocation the previous implementation had.
    let mut encoded = String::with_capacity(unwrapped_len);
    BASE64_STANDARD.encode_string(bytes, &mut encoded);
    out.reserve(chunked_len(encoded.len()));
    let mut offset = 0;
    while offset < encoded.len() {
        if offset > 0 {
            out.push_str("\r\n");
        }
        let end = (offset + LINE_LEN).min(encoded.len());
        out.push_str(&encoded[offset..end]);
        offset = end;
    }
}

fn encode_array<O: OffsetSizeTrait>(array: &GenericBinaryArray<O>, chunk: bool) -> StringArray {
    // Right-size the value buffer in O(1) from the input's total byte count. When chunking,
    // add the CRLF slack the encoded output will contain so the long-input path does not grow.
    let total_bytes = array.value_data().len();
    let encoded_total = base64_encoded_len(total_bytes);
    let data_capacity = if chunk {
        chunked_len(encoded_total)
    } else {
        encoded_total
    };
    let mut builder = GenericStringBuilder::<i32>::with_capacity(array.len(), data_capacity);
    // Reused across rows so each element pays a single allocation only if it needs to grow.
    let mut buf = String::new();
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
            continue;
        }
        buf.clear();
        encode_into(array.value(i), chunk, &mut buf);
        builder.append_value(&buf);
    }
    builder.finish()
}

fn encode(bytes: &[u8], chunk: bool) -> String {
    let mut out = String::new();
    encode_into(bytes, chunk, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, LargeBinaryArray};

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

    #[test]
    fn encode_array_binary_chunked() {
        // Nulls, empty values, values that wrap, and one on the boundary.
        let a57 = [b'a'; 57];
        let b58 = [b'b'; 58];
        let c120 = [b'c'; 120];
        let input = BinaryArray::from(vec![
            Some(&b""[..]),
            None,
            Some(&a57[..]), // exactly 76 encoded chars: no wrap
            Some(&b58[..]), // wraps once
            None,
            Some(&c120[..]), // wraps twice
        ]);
        let out = encode_array(&input, true);
        assert_eq!(out.len(), 6);
        assert_eq!(out.value(0), "");
        assert!(out.is_null(1));
        assert_eq!(out.value(2), encode(&a57, true));
        assert_eq!(out.value(3), encode(&b58, true));
        assert!(out.is_null(4));
        assert_eq!(out.value(5), encode(&c120, true));
    }

    #[test]
    fn encode_array_large_binary_unchunked() {
        // Exercises the LargeBinaryArray (i64 offsets) instantiation of the generic.
        let input = LargeBinaryArray::from(vec![None, Some(&b"abc"[..]), Some(&b"hi!"[..])]);
        let out = encode_array(&input, false);
        assert_eq!(out.len(), 3);
        assert!(out.is_null(0));
        assert_eq!(out.value(1), "YWJj");
        assert_eq!(out.value(2), BASE64_STANDARD.encode(b"hi!"));
    }
}
