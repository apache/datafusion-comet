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

use arrow::array::{
    as_dictionary_array, make_array, Array, ArrayData, ArrayRef, DictionaryArray,
    GenericStringArray, OffsetSizeTrait, StringArray,
};
use arrow::buffer::MutableBuffer;
use arrow::datatypes::{DataType, Int32Type};
use datafusion::common::{exec_err, internal_datafusion_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::{any::Any, sync::Arc};

/// Spark-compatible URL encoding following application/x-www-form-urlencoded format.
/// This matches Java's URLEncoder.encode behavior used by Spark's UrlCodec.encode.
///
/// Key behaviors:
/// - Spaces are encoded as '+' (not '%20')
/// - Alphanumeric characters (a-z, A-Z, 0-9) are not encoded
/// - Special characters '.', '-', '*', '_' are not encoded
/// - All other characters are percent-encoded using UTF-8 bytes
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUrlEncode {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkUrlEncode {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUrlEncode {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkUrlEncode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "url_encode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match &arg_types[0] {
            DataType::Dictionary(key_type, _) => {
                DataType::Dictionary(key_type.clone(), Box::new(DataType::Utf8))
            }
            _ => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(internal_datafusion_err!(
                "url_encode expects exactly one argument, got {}",
                args.args.len()
            ));
        }
        let args: [ColumnarValue; 1] = args
            .args
            .try_into()
            .map_err(|_| internal_datafusion_err!("url_encode expects exactly one argument"))?;
        spark_url_encode(&args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub fn spark_url_encode(args: &[ColumnarValue; 1]) -> Result<ColumnarValue> {
    match args {
        [ColumnarValue::Array(array)] => {
            let result = url_encode_array(array.as_ref())?;
            Ok(ColumnarValue::Array(result))
        }
        [ColumnarValue::Scalar(scalar)] => {
            let result = url_encode_scalar(scalar)?;
            Ok(ColumnarValue::Scalar(result))
        }
    }
}

fn url_encode_array(input: &dyn Array) -> Result<ArrayRef> {
    match input.data_type() {
        DataType::Utf8 => {
            let array = input.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(url_encode_string_array::<i32>(array))
        }
        DataType::LargeUtf8 => {
            let array = input
                .as_any()
                .downcast_ref::<GenericStringArray<i64>>()
                .unwrap();
            Ok(url_encode_string_array::<i64>(array))
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(input);
            let values = url_encode_array(dict.values())?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result))
        }
        other => exec_err!("Unsupported input type for function 'url_encode': {other:?}"),
    }
}

fn url_encode_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => {
            let result = value.as_ref().map(|s| url_encode_string(s));
            Ok(ScalarValue::Utf8(result))
        }
        ScalarValue::Null => Ok(ScalarValue::Utf8(None)),
        other => exec_err!("Unsupported data type {other:?} for function `url_encode`"),
    }
}

fn url_encode_string_array<OffsetSize: OffsetSizeTrait>(
    input: &GenericStringArray<OffsetSize>,
) -> ArrayRef {
    let array_len = input.len();
    let mut offsets = MutableBuffer::new((array_len + 1) * std::mem::size_of::<OffsetSize>());
    let mut values = MutableBuffer::new(input.values().len()); // reasonable initial capacity
    let mut offset_so_far = OffsetSize::zero();
    let null_bit_buffer = input.to_data().nulls().map(|b| b.buffer().clone());

    offsets.push(offset_so_far);

    for i in 0..array_len {
        if !input.is_null(i) {
            let encoded = url_encode_string(input.value(i));
            offset_so_far += OffsetSize::from_usize(encoded.len()).unwrap();
            values.extend_from_slice(encoded.as_bytes());
        }
        offsets.push(offset_so_far);
    }

    let data = unsafe {
        ArrayData::new_unchecked(
            GenericStringArray::<OffsetSize>::DATA_TYPE,
            array_len,
            None,
            null_bit_buffer,
            0,
            vec![offsets.into(), values.into()],
            vec![],
        )
    };
    make_array(data)
}

fn url_encode_length(s: &str) -> usize {
    let mut len = 0;
    for byte in s.bytes() {
        if should_encode(byte) {
            if byte == b' ' {
                len += 1; // space -> '+'
            } else {
                len += 3; // other -> %XX
            }
        } else {
            len += 1;
        }
    }
    len
}

fn url_encode_string(s: &str) -> String {
    let mut buf = Vec::with_capacity(url_encode_length(s));
    for byte in s.bytes() {
        if !should_encode(byte) {
            buf.push(byte);
        } else if byte == b' ' {
            buf.push(b'+');
        } else {
            buf.push(b'%');
            buf.push(HEX_BYTES[(byte >> 4) as usize]);
            buf.push(HEX_BYTES[(byte & 0x0F) as usize]);
        }
    }

    unsafe { String::from_utf8_unchecked(buf) }
}

const HEX_BYTES: [u8; 16] = *b"0123456789ABCDEF";

/// Check if a byte should be encoded
/// Returns true for characters that need to be percent-encoded
fn should_encode(byte: u8) -> bool {
    // Unreserved characters per RFC 3986 that are NOT encoded by URLEncoder:
    // - Alphanumeric: A-Z, a-z, 0-9
    // - Special: '.', '-', '*', '_'
    // Note: '~' is unreserved in RFC 3986 but IS encoded by Java URLEncoder
    !matches!(byte,
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' |
        b'.' | b'-' | b'*' | b'_'
    )
}


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::cast::as_string_array;

    #[test]
    fn test_url_encode_basic() {
        assert_eq!(url_encode_string("Hello World"), "Hello+World");
        assert_eq!(url_encode_string("foo=bar"), "foo%3Dbar");
        assert_eq!(url_encode_string("a+b"), "a%2Bb");
        assert_eq!(url_encode_string(""), "");
    }

    #[test]
    fn test_url_encode_special_chars() {
        assert_eq!(url_encode_string("?"), "%3F");
        assert_eq!(url_encode_string("&"), "%26");
        assert_eq!(url_encode_string("="), "%3D");
        assert_eq!(url_encode_string("#"), "%23");
        assert_eq!(url_encode_string("/"), "%2F");
        assert_eq!(url_encode_string("%"), "%25");
    }

    #[test]
    fn test_url_encode_unreserved_chars() {
        // These should NOT be encoded
        assert_eq!(url_encode_string("abc123"), "abc123");
        assert_eq!(url_encode_string("ABC"), "ABC");
        assert_eq!(url_encode_string("."), ".");
        assert_eq!(url_encode_string("-"), "-");
        assert_eq!(url_encode_string("*"), "*");
        assert_eq!(url_encode_string("_"), "_");
    }

    #[test]
    fn test_url_encode_unicode() {
        // UTF-8 multi-byte characters should be percent-encoded
        assert_eq!(url_encode_string("cafe\u{0301}"), "cafe%CC%81");
        assert_eq!(url_encode_string("\u{00e9}"), "%C3%A9"); // é as single char
    }

    #[test]
    fn test_url_encode_array() {
        let input = StringArray::from(vec![
            Some("Hello World"),
            Some("foo=bar"),
            None,
            Some(""),
        ]);
        let args = ColumnarValue::Array(Arc::new(input));
        match spark_url_encode(&[args]) {
            Ok(ColumnarValue::Array(result)) => {
                let actual = as_string_array(&result).unwrap();
                assert_eq!(actual.value(0), "Hello+World");
                assert_eq!(actual.value(1), "foo%3Dbar");
                assert!(actual.is_null(2));
                assert_eq!(actual.value(3), "");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_url_encode_scalar() {
        let scalar = ScalarValue::Utf8(Some("Hello World".to_string()));
        let result = url_encode_scalar(&scalar).unwrap();
        assert_eq!(result, ScalarValue::Utf8(Some("Hello+World".to_string())));

        let null_scalar = ScalarValue::Utf8(None);
        let null_result = url_encode_scalar(&null_scalar).unwrap();
        assert_eq!(null_result, ScalarValue::Utf8(None));
    }

    #[test]
    fn test_url_encode_tilde() {
        // ~ is unreserved in RFC 3986 but Java URLEncoder encodes it
        assert_eq!(url_encode_string("~"), "%7E");
    }
}
