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

//! String kernels

use std::sync::Arc;

use arrow::{
    array::*,
    buffer::{Buffer, OffsetBuffer, ScalarBuffer},
    compute::kernels::substring::substring as arrow_substring,
    datatypes::{DataType, Int32Type},
};
use datafusion::common::DataFusionError;

pub fn substring(array: &dyn Array, start: i64, length: u64) -> Result<ArrayRef, DataFusionError> {
    match array.data_type() {
        DataType::LargeUtf8 => Ok(substring_by_char(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("A large string is expected"),
            start,
            length,
        )),
        DataType::Utf8 => Ok(substring_by_char(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("A string is expected"),
            start,
            length,
        )),
        DataType::Binary | DataType::LargeBinary => {
            arrow_substring(array, start, Some(length)).map_err(|e| e.into())
        }
        DataType::Dictionary(_, _) => {
            let dict = as_dictionary_array::<Int32Type>(array);
            let values = substring(dict.values(), start, length)?;
            let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
            Ok(Arc::new(result))
        }
        dt => panic!("Unsupported input type for function 'substring': {dt:?}"),
    }
}

/// Byte bounds of the `length`-codepoint window starting at codepoint `start` of an ASCII
/// string, where one codepoint is one byte.
#[inline]
fn ascii_bounds(value: &str, start: i64, length: usize) -> (usize, usize) {
    let byte_len = value.len();
    let begin = if start >= 0 {
        (start as usize).min(byte_len)
    } else {
        byte_len.saturating_sub(start.unsigned_abs() as usize)
    };
    (begin, begin.saturating_add(length).min(byte_len))
}

/// Byte bounds of the `length`-codepoint window starting at codepoint `start` of an
/// arbitrary UTF-8 string, where a negative `start` counts back from the end.
#[inline]
fn utf8_bounds(value: &str, start: i64, length: usize) -> (usize, usize) {
    let byte_len = value.len();
    let begin = if start >= 0 {
        value
            .char_indices()
            .nth(start as usize)
            .map_or(byte_len, |(offset, _)| offset)
    } else {
        value
            .char_indices()
            .nth_back(start.unsigned_abs() as usize - 1)
            .map_or(0, |(offset, _)| offset)
    };

    // A codepoint is at least one byte, so a window at least as wide as the bytes that
    // remain always runs to the end and needs no decoding to place.
    if length >= byte_len - begin {
        return (begin, byte_len);
    }
    let end = value[begin..]
        .char_indices()
        .nth(length)
        .map_or(byte_len, |(offset, _)| begin + offset);
    (begin, end)
}

/// Take `length` codepoints starting at codepoint `start` of each element, where a negative
/// `start` counts back from the end of the element.
fn substring_by_char<O: OffsetSizeTrait>(
    array: &GenericStringArray<O>,
    start: i64,
    length: u64,
) -> ArrayRef {
    let length = usize::try_from(length).unwrap_or(usize::MAX);

    // A whole-buffer ASCII check is a bulk byte scan, and once it succeeds every codepoint
    // index is a byte index, so windows can be sliced without decoding UTF-8 at all. The
    // check is loop-invariant, so pick the bounds function once and let each variant
    // monomorphize into its own loop.
    if array.is_ascii() {
        build(array, length, |value| ascii_bounds(value, start, length))
    } else {
        build(array, length.saturating_mul(4), |value| {
            utf8_bounds(value, start, length)
        })
    }
}

/// Build the result of applying `bounds` to every element, carrying nulls through.
/// `max_element_len` is an upper bound, in bytes, on what one element can contribute.
fn build<O: OffsetSizeTrait, F: Fn(&str) -> (usize, usize)>(
    array: &GenericStringArray<O>,
    max_element_len: usize,
    bounds: F,
) -> ArrayRef {
    // No output can exceed the input it is sliced from, so cap the reservation by that:
    // `length` is unbounded (Spark's 2-arg substring passes u64::MAX) and would otherwise
    // reserve wildly more than any possible result.
    let offsets = array.value_offsets();
    let input_len = offsets[array.len()].as_usize() - offsets[0].as_usize();
    let capacity = input_len.min(array.len().saturating_mul(max_element_len));

    let mut values: Vec<u8> = Vec::with_capacity(capacity);
    let mut new_offsets: Vec<O> = Vec::with_capacity(array.len() + 1);
    new_offsets.push(O::zero());

    for value in array.iter() {
        // Null elements contribute no bytes; the null buffer is reattached below.
        if let Some(value) = value {
            let (from, to) = bounds(value);
            values.extend_from_slice(&value.as_bytes()[from..to]);
        }
        new_offsets.push(O::from_usize(values.len()).unwrap());
    }

    // SAFETY: `new_offsets` is non-decreasing because it only ever records the growing
    // length of `values`, and every window `bounds` returns is cut on a codepoint boundary
    // of a `&str`, so each slice of `values` it delimits is valid UTF-8. Constructing the
    // array checked would re-scan the whole output to rediscover both, which is the decode
    // pass this kernel exists to avoid.
    unsafe {
        Arc::new(GenericStringArray::<O>::new_unchecked(
            OffsetBuffer::new_unchecked(ScalarBuffer::from(new_offsets)),
            Buffer::from_vec(values),
            array.nulls().cloned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf8_substring(values: Vec<Option<&str>>, start: i64, length: u64) -> Vec<Option<String>> {
        let array = StringArray::from(values);
        let result = substring(&array, start, length).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        result.iter().map(|v| v.map(String::from)).collect()
    }

    #[test]
    fn test_ascii() {
        assert_eq!(
            utf8_substring(vec![Some("hello world"), Some("hi"), Some(""), None], 2, 4),
            vec![
                Some("llo ".to_string()),
                Some("".to_string()),
                Some("".to_string()),
                None
            ]
        );
    }

    #[test]
    fn test_zero_length() {
        assert_eq!(
            utf8_substring(vec![Some("hello")], 1, 0),
            vec![Some("".to_string())]
        );
    }

    #[test]
    fn test_length_past_end() {
        assert_eq!(
            utf8_substring(vec![Some("hello")], 3, u64::MAX),
            vec![Some("lo".to_string())]
        );
    }

    #[test]
    fn test_start_past_end() {
        assert_eq!(
            utf8_substring(vec![Some("hello")], 99, 3),
            vec![Some("".to_string())]
        );
    }

    #[test]
    fn test_multibyte() {
        assert_eq!(
            utf8_substring(vec![Some("こんにちは世界"), Some("ab🎉cd")], 2, 3),
            vec![Some("にちは".to_string()), Some("🎉cd".to_string())]
        );
    }

    #[test]
    fn test_negative_start() {
        // matches Arrow's semantics: the start is clamped to the beginning of the element
        assert_eq!(
            utf8_substring(vec![Some("hello"), Some("こんにちは"), Some("ab")], -3, 2),
            vec![
                Some("ll".to_string()),
                Some("にち".to_string()),
                Some("ab".to_string())
            ]
        );
    }

    #[test]
    fn test_large_utf8() {
        let array = LargeStringArray::from(vec![Some("hello world"), None, Some("こんにちは")]);
        let result = substring(&array, 1, 3).unwrap();
        let result = result.as_any().downcast_ref::<LargeStringArray>().unwrap();
        assert_eq!(result.value(0), "ell");
        assert!(result.is_null(1));
        assert_eq!(result.value(2), "んにち");
    }

    #[test]
    fn test_sliced_input() {
        let array = StringArray::from(vec![Some("aaaa"), Some("hello"), Some("úñîçödé"), None]);
        let sliced = array.slice(1, 3);
        let result = substring(&sliced, 1, 3).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "ell");
        assert_eq!(result.value(1), "ñîç");
        assert!(result.is_null(2));
    }

    #[test]
    fn test_dictionary() {
        let array: DictionaryArray<Int32Type> =
            vec![Some("hello"), Some("world"), None, Some("hello")]
                .into_iter()
                .collect();
        let result = substring(&array, 1, 3).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(result.key(0).unwrap()), "ell");
        assert_eq!(values.value(result.key(1).unwrap()), "orl");
        assert!(result.is_null(2));
    }

    #[test]
    fn test_binary() {
        let array = BinaryArray::from(vec![Some([1u8, 2, 3, 4].as_slice()), None]);
        let result = substring(&array, 1, 2).unwrap();
        let result = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(result.value(0), &[2, 3]);
        assert!(result.is_null(1));
    }
}
