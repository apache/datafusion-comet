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
    Array, AsArray, BinaryArray, BinaryBuilder, GenericStringArray, OffsetSizeTrait,
};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;

/// Spark `unbase64(str)`: decodes a base64 string to binary using JDK's MIME decoder rules.
///
/// Matches `java.util.Base64.getMimeDecoder().decode(str.getBytes(ISO_8859_1))`: every byte
/// outside the base64 alphabet is skipped (so CRLF-wrapped output from Spark's own `base64`
/// round-trips cleanly), and the four terminal-shape error conditions are reproduced with
/// matching messages.
pub fn spark_unbase64(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("unbase64 expects exactly one argument, got {}", args.len());
    }
    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => Ok(ColumnarValue::Array(Arc::new(decode_array(
                array.as_string::<i32>(),
            )?))),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(Arc::new(decode_array(
                array.as_string::<i64>(),
            )?))),
            // Comet's planner coerces string inputs back to Utf8 for well-supported UDF
            // signatures, so Utf8View is not expected to reach us today. Fail loudly if it
            // ever does, rather than silently materialise.
            other => exec_err!("unbase64 expects a string argument, got {other}"),
        },
        ColumnarValue::Scalar(ScalarValue::Utf8(value))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(value)) => {
            let decoded = value
                .as_ref()
                .map(|s| {
                    let mut out = Vec::new();
                    decode(s.as_bytes(), &mut out).map(|_| out)
                })
                .transpose()?;
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(decoded)))
        }
        ColumnarValue::Scalar(other) => {
            exec_err!("unbase64 expects a string argument, got {other}")
        }
    }
}

/// Sentinel in `BASE64_LUT` for bytes that are not in the base64 alphabet.
const INVALID: u8 = 0xFF;
/// Sentinel in `BASE64_LUT` for the padding character `=`.
const PADDING: u8 = 0xFE;

/// Lookup table mapping each possible input byte to its 6-bit base64 value, `PADDING` for `=`,
/// or `INVALID` for every other byte. Built at compile time so the hot loop is a single indexed
/// load per input byte.
const BASE64_LUT: [u8; 256] = build_base64_lut();

const fn build_base64_lut() -> [u8; 256] {
    let mut lut = [INVALID; 256];
    let mut i = 0;
    while i < 26 {
        lut[b'A' as usize + i] = i as u8;
        lut[b'a' as usize + i] = (i + 26) as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 10 {
        lut[b'0' as usize + i] = (i + 52) as u8;
        i += 1;
    }
    lut[b'+' as usize] = 62;
    lut[b'/' as usize] = 63;
    lut[b'=' as usize] = PADDING;
    lut
}

const ERR_WRONG_ENDING: &str = "Input byte array has wrong 4-byte ending unit";
const ERR_NOT_ENOUGH_BITS: &str = "Last unit does not have enough valid bits";

/// Returns `exec_err` if `input_bytes` cannot fit inside a `BinaryArray`'s i32 offset range.
///
/// `GenericByteBuilder::next_offset` in arrow-rs is `.expect("byte array offset overflow")`, so
/// once the running offset would cross `i32::MAX` the append panics. That panic bubbles across
/// the JNI boundary without Comet context; the upfront guard turns it into an `exec_err` with
/// the actual input size, which is what surfaces in the Spark task log.
///
/// The bound is conservative: the decoder produces at most `input_bytes * 3 / 4` output bytes
/// (skipped non-alphabet bytes shrink it further), so an input at this bound decodes to well
/// under `i32::MAX`. Rejecting at input length keeps the check O(1) and independent of the
/// decoder implementation.
fn check_binary_capacity(input_bytes: usize) -> Result<(), DataFusionError> {
    if input_bytes > i32::MAX as usize {
        return exec_err!(
            "unbase64 input of {input_bytes} bytes exceeds BinaryArray capacity ({} bytes)",
            i32::MAX
        );
    }
    Ok(())
}

// Both Utf8 and LargeUtf8 inputs decode to i32-offset `BinaryArray`. Comet's shuffle path
// does not otherwise plumb `LargeBinaryArray`, so widening the return type would ripple
// through every downstream consumer of this column; the `check_binary_capacity` guard on the
// LargeUtf8 path turns the only reachable overflow into a clean `exec_err` instead.
fn decode_array<O: OffsetSizeTrait>(
    array: &GenericStringArray<O>,
) -> Result<BinaryArray, DataFusionError> {
    // Byte span of the slice (last offset − first offset), not the underlying buffer length.
    // `value_data().len()` reports the whole buffer and ignores slicing, which would
    // false-positive the capacity guard on a small slice into a large parent.
    let offsets = array.value_offsets();
    let input_bytes = (offsets[array.len()] - offsets[0]).as_usize();
    if O::IS_LARGE {
        check_binary_capacity(input_bytes)?;
    }
    // Upper bound: N alphabet chars decode to `ceil(N/4)*3` bytes, and alphabet chars <= total
    // input bytes. Over-reserves for inputs padded heavily with skipped bytes (long CRLF-wrapped
    // values), which is acceptable.
    let capacity = input_bytes.div_ceil(4) * 3;
    let mut builder = BinaryBuilder::with_capacity(array.len(), capacity);
    let mut scratch = Vec::new();
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
            continue;
        }
        scratch.clear();
        decode(array.value(i).as_bytes(), &mut scratch)?;
        builder.append_value(&scratch);
    }
    Ok(builder.finish())
}

/// Decodes `src` in MIME mode, appending decoded bytes to `out`.
///
/// Mirrors `java.util.Base64.Decoder.decode0` with `isMIME = true`: every byte outside the base64
/// alphabet is skipped, and the four terminal-shape error conditions are reproduced verbatim so
/// the error messages match Spark's codegen-dispatched path.
fn decode(src: &[u8], out: &mut Vec<u8>) -> Result<(), DataFusionError> {
    out.reserve(src.len().div_ceil(4) * 3);

    let mut bits: u32 = 0;
    // Position, in bits, where the next 6-bit group's high bit lands inside a 24-bit atom.
    // Steps 18 -> 12 -> 6 -> 0 for the four chars of a group; wraps back to 18 after emit.
    let mut shift: i32 = 18;
    let mut sp = 0;
    while sp < src.len() {
        let raw = src[sp];
        sp += 1;
        let v = BASE64_LUT[raw as usize];
        if v == INVALID {
            continue;
        }
        if v == PADDING {
            if shift == 18 {
                // '=' with no data before it in this group.
                return Err(DataFusionError::Execution(ERR_WRONG_ENDING.into()));
            }
            if shift == 6 {
                // `xx=` shape: the immediate next byte must also be `=`. JDK reads it adjacent,
                // without skipping non-alphabet bytes, so we do the same for message parity.
                if sp >= src.len() || src[sp] != b'=' {
                    return Err(DataFusionError::Execution(ERR_WRONG_ENDING.into()));
                }
                sp += 1;
            }
            break;
        }
        bits |= (v as u32) << shift;
        shift -= 6;
        if shift < 0 {
            out.push((bits >> 16) as u8);
            out.push((bits >> 8) as u8);
            out.push(bits as u8);
            shift = 18;
            bits = 0;
        }
    }

    // Finalize the partial group. shift reflects how many chars we consumed in the tail.
    match shift {
        18 => {}
        12 => return Err(DataFusionError::Execution(ERR_NOT_ENOUGH_BITS.into())),
        6 => out.push((bits >> 16) as u8),
        0 => {
            out.push((bits >> 16) as u8);
            out.push((bits >> 8) as u8);
        }
        _ => unreachable!("shift is always one of 18, 12, 6, 0"),
    }

    // After padding, only real alphabet characters are errors. JDK's trailing loop treats any
    // byte with `base64[b] < 0` as skippable, which covers both non-alphabet bytes (INVALID)
    // and additional padding bytes (PADDING) — so e.g. `YQ===` decodes cleanly to `a`.
    while sp < src.len() {
        let b = src[sp];
        sp += 1;
        if BASE64_LUT[b as usize] < PADDING {
            return Err(DataFusionError::Execution(format!(
                "Input byte array has incorrect ending byte at {sp}"
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{LargeStringArray, StringArray};

    fn dec(s: &str) -> Vec<u8> {
        let mut out = Vec::new();
        decode(s.as_bytes(), &mut out).unwrap();
        out
    }

    fn dec_err(s: &str) -> String {
        decode(s.as_bytes(), &mut Vec::new())
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn empty_input() {
        assert_eq!(dec(""), Vec::<u8>::new());
    }

    #[test]
    fn standard_padded() {
        assert_eq!(dec("YQ=="), b"a");
        assert_eq!(dec("YWI="), b"ab");
        assert_eq!(dec("YWJj"), b"abc");
    }

    #[test]
    fn unpadded_accepted() {
        // JDK MIME accepts truncated tails at clean boundaries.
        assert_eq!(dec("YQ"), b"a");
        assert_eq!(dec("YWI"), b"ab");
    }

    #[test]
    fn skips_non_alphabet_bytes() {
        assert_eq!(dec("YW Jj"), b"abc");
        assert_eq!(dec("YWJj?"), b"abc");
        assert_eq!(dec("Y\r\nWJj"), b"abc");
    }

    #[test]
    fn crlf_wrapped_round_trips() {
        // 60-byte input encodes to 80 chars → one CRLF wrap at char 76 (matches spark_base64).
        let raw: Vec<u8> = (0..60).map(|i| i as u8).collect();
        let encoded_flat =
            "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7";
        let encoded_wrapped = format!("{}\r\n{}", &encoded_flat[..76], &encoded_flat[76..]);
        assert_eq!(dec(encoded_flat), raw);
        assert_eq!(dec(&encoded_wrapped), raw);
    }

    #[test]
    fn extra_trailing_padding_after_valid_pair() {
        // Regression: JDK's MIME trailing loop uses `base64[b] < 0`, and '=' maps to -2 (< 0),
        // so additional `=` bytes after the required pair are silently consumed. An earlier
        // implementation compared against the INVALID sentinel only and rejected these.
        assert_eq!(dec("YQ==="), b"a");
        assert_eq!(dec("YQ==\r\n="), b"a");
    }

    #[test]
    fn err_dangling_single_char() {
        assert!(dec_err("YWJjY").contains(ERR_NOT_ENOUGH_BITS));
    }

    #[test]
    fn err_padding_without_data() {
        assert!(dec_err("====").contains(ERR_WRONG_ENDING));
    }

    #[test]
    fn err_missing_second_pad() {
        // "YW=" is shape xx=, the second `=` is missing.
        assert!(dec_err("YW=").contains(ERR_WRONG_ENDING));
    }

    #[test]
    fn err_alphabet_after_padding() {
        let err = dec_err("YQ==Z");
        assert!(err.contains("incorrect ending byte"), "{err}");
    }

    #[test]
    fn decode_array_utf8_with_nulls() {
        let input = StringArray::from(vec![Some("YWJj"), None, Some("YQ=="), Some("")]);
        let out = decode_array(&input).unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), b"abc");
        assert!(out.is_null(1));
        assert_eq!(out.value(2), b"a");
        assert_eq!(out.value(3), b"");
    }

    #[test]
    fn check_binary_capacity_at_boundary() {
        assert!(check_binary_capacity(0).is_ok());
        assert!(check_binary_capacity(i32::MAX as usize).is_ok());
        assert!(check_binary_capacity(i32::MAX as usize + 1).is_err());
    }

    #[test]
    fn spark_unbase64_scalar_utf8() {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "YWJj".into(),
        )))];
        match spark_unbase64(&args).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(v))) => assert_eq!(v, b"abc"),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn spark_unbase64_scalar_null() {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))];
        match spark_unbase64(&args).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Binary(None)) => {}
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn spark_unbase64_wrong_arity() {
        assert!(spark_unbase64(&[]).is_err());
        let two = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("YWJj".into()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("YWJj".into()))),
        ];
        assert!(spark_unbase64(&two).is_err());
    }

    #[test]
    fn spark_unbase64_wrong_scalar_type() {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(42)))];
        assert!(spark_unbase64(&args).is_err());
    }

    #[test]
    fn spark_unbase64_large_utf8_array() {
        let input = LargeStringArray::from(vec![Some("YWJj"), None, Some("YQ==")]);
        match spark_unbase64(&[ColumnarValue::Array(Arc::new(input))]).unwrap() {
            ColumnarValue::Array(out) => {
                let bin = out.as_binary::<i32>();
                assert_eq!(bin.len(), 3);
                assert_eq!(bin.value(0), b"abc");
                assert!(bin.is_null(1));
                assert_eq!(bin.value(2), b"a");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
