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

use std::fmt::{self, Write};
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

/// Panic message for the `fmt::Write` sinks used here; neither can actually fail.
const INFALLIBLE_SINK: &str = "writing base64 to an in-memory buffer cannot fail";

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

/// Encodes `bytes` as base64 into `out`, wrapping at `LINE_LEN` when `chunk` is true.
///
/// `scratch` receives the unwrapped encoding from a single bulk `encode_string` call. Both
/// `scratch` and `out` are owned by the caller and reused across rows, so encoding a batch performs
/// no per-row heap allocation. Because `out` is a `fmt::Write` sink, the array path can pass the
/// output builder directly and have the wrapped result land in its value buffer, rather than
/// staging each row in a second buffer and copying it in.
///
/// An alternative is to skip `scratch` entirely and encode line-sized windows straight into `out`:
/// MIME wrapping is aligned to base64's block structure, so 57 input bytes encode to exactly
/// `LINE_LEN` chars with no padding, and only the final window can carry padding. That is correct
/// and allocation-free, but measurably slower — the per-call overhead of many small `encode_string`
/// calls exceeds the cost of one bulk encode plus the copy out. See the benchmark discussion on
/// <https://github.com/apache/datafusion-comet/pull/4885>.
fn encode_into<W: Write>(
    bytes: &[u8],
    chunk: bool,
    scratch: &mut String,
    out: &mut W,
) -> fmt::Result {
    scratch.clear();
    BASE64_STANDARD.encode_string(bytes, scratch);
    if !chunk || scratch.len() <= LINE_LEN {
        return out.write_str(scratch);
    }
    let mut offset = 0;
    while offset < scratch.len() {
        if offset > 0 {
            out.write_str("\r\n")?;
        }
        let end = (offset + LINE_LEN).min(scratch.len());
        out.write_str(&scratch[offset..end])?;
        offset = end;
    }
    Ok(())
}

/// O(1) upper bound on the total encoded length of `array`, for sizing the output value buffer.
///
/// Each row pads independently, so the encoded total is `sum ceil(len_i / 3) * 4`, which cannot be
/// derived from the input's total byte count alone: N rows of one byte each encode to `4N`, not to
/// `base64_encoded_len(N)`. Since `sum ceil(x_i) <= ceil(sum x_i) + (N - 1)`, adding `4 * (N - 1)`
/// to the whole-input encoding turns the estimate into a true upper bound while staying O(1) — no
/// pass over the offsets. It over-reserves by at most 4 bytes per row, which matters only for
/// arrays of very short values, where the buffer is small in absolute terms anyway.
fn encoded_capacity<O: OffsetSizeTrait>(array: &GenericBinaryArray<O>, chunk: bool) -> usize {
    let encoded_total =
        base64_encoded_len(array.value_data().len()) + 4 * array.len().saturating_sub(1);
    if chunk {
        chunked_len(encoded_total)
    } else {
        encoded_total
    }
}

fn encode_array<O: OffsetSizeTrait>(array: &GenericBinaryArray<O>, chunk: bool) -> StringArray {
    let mut builder =
        GenericStringBuilder::<i32>::with_capacity(array.len(), encoded_capacity(array, chunk));
    // Reused across rows, so a batch pays no per-row allocation.
    let mut scratch = String::new();
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
            continue;
        }
        encode_into(array.value(i), chunk, &mut scratch, &mut builder).expect(INFALLIBLE_SINK);
        // Finalizes the value written through `fmt::Write` above.
        builder.append_value("");
    }
    builder.finish()
}

fn encode(bytes: &[u8], chunk: bool) -> String {
    let mut scratch = String::new();
    let mut out = String::new();
    encode_into(bytes, chunk, &mut scratch, &mut out).expect(INFALLIBLE_SINK);
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
    fn chunked_matches_encode_then_split_at_every_length() {
        // The window encoding must be byte-for-byte what encoding the whole input and splitting
        // every LINE_LEN chars produces. Sweep every length across three window boundaries so a
        // padding or off-by-one error in the window size cannot hide.
        for n in 0..=(57 * 3 + 4) {
            let input = vec![b'z'; n];
            let unwrapped = BASE64_STANDARD.encode(&input);
            let expected = unwrapped
                .as_bytes()
                .chunks(LINE_LEN)
                .map(|line| std::str::from_utf8(line).unwrap())
                .collect::<Vec<_>>()
                .join("\r\n");
            assert_eq!(encode(&input, true), expected, "length {n}");
            // Every line is full except the last, and no line exceeds the limit.
            let actual = encode(&input, true);
            let lines: Vec<&str> = actual.split("\r\n").collect();
            for (i, line) in lines.iter().enumerate() {
                if i + 1 < lines.len() {
                    assert_eq!(line.len(), LINE_LEN, "length {n}, line {i}");
                } else {
                    assert!(line.len() <= LINE_LEN, "length {n}, last line");
                }
            }
        }
    }

    #[test]
    fn encoded_capacity_is_an_upper_bound() {
        // Regression test for the capacity estimate: rows pad independently, so an estimate
        // derived from the summed input bytes alone under-reserves by ~3x for many tiny rows.
        let shapes: Vec<Vec<usize>> = vec![
            vec![1; 64],                // worst case: every row pads to 4 chars
            vec![2; 64],                //
            vec![3; 64],                // no padding at all
            vec![0; 16],                // all empty
            vec![57; 8],                // exactly one line each
            vec![58; 8],                // wraps once each
            vec![200, 1, 0, 57, 58, 3], // mixed
            vec![4096; 4],              // few large rows
        ];
        for shape in shapes {
            let values: Vec<Option<Vec<u8>>> = shape.iter().map(|&n| Some(vec![b'q'; n])).collect();
            let input = BinaryArray::from_iter(values);
            for chunk in [false, true] {
                let actual: usize = (0..input.len())
                    .map(|i| encode(input.value(i), chunk).len())
                    .sum();
                let estimated = encoded_capacity(&input, chunk);
                assert!(
                    estimated >= actual,
                    "shape {shape:?}, chunk={chunk}: estimate {estimated} < actual {actual}"
                );
            }
        }
    }

    #[test]
    fn encode_array_many_tiny_rows() {
        // The shape that the old capacity estimate under-reserved for. Verifies output
        // correctness independently of the reservation.
        let values: Vec<Option<&[u8]>> = (0..100).map(|_| Some(&b"a"[..])).collect();
        let input = BinaryArray::from(values);
        for chunk in [false, true] {
            let out = encode_array(&input, chunk);
            assert_eq!(out.len(), 100);
            for i in 0..out.len() {
                assert_eq!(out.value(i), "YQ==");
            }
        }
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
