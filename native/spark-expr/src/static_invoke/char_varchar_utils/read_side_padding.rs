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

use arrow::array::builder::GenericStringBuilder;
use arrow::array::cast::as_dictionary_array;
use arrow::array::types::Int32Type;
use arrow::array::{make_array, new_null_array, Array, AsArray, DictionaryArray, Int32Array};
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_generic_string_array, DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::fmt::Write;
use std::sync::Arc;

const SPACE: &str = " ";
/// Similar to DataFusion `rpad`, but not to truncate when the string is already longer than length
pub fn spark_read_side_padding(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_read_side_padding2(args, false, false)
}

/// Custom `rpad` because DataFusion's `rpad` has differences in unicode handling
pub fn spark_rpad(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_read_side_padding2(args, true, false)
}

/// Custom `lpad` because DataFusion's `lpad` has differences in unicode handling
pub fn spark_lpad(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_read_side_padding2(args, true, true)
}

fn spark_read_side_padding2(
    args: &[ColumnarValue],
    truncate: bool,
    is_left_pad: bool,
) -> Result<ColumnarValue, DataFusionError> {
    match args {
        [ColumnarValue::Scalar(string @ (ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_))), rest @ ..]
            if matches!(rest,
                [length] | [length, ColumnarValue::Scalar(ScalarValue::Utf8(_))]
                if length.data_type() == DataType::Int32) =>
        {
            // Merged scalar subqueries can reach native padding without being
            // folded by Spark. Borrow the string when lengths vary by row instead
            // of materializing a copy of the entire input for every output row.
            let length_array = rest.first().and_then(|length| match length {
                ColumnarValue::Array(array) => Some(array),
                ColumnarValue::Scalar(_) => None,
            });
            // A runtime NULL must not allocate a padding buffer proportional to
            // the requested length. Preserve the shape and type of the result.
            if string.is_null()
                || matches!(rest, [_, ColumnarValue::Scalar(ScalarValue::Utf8(None))])
                || matches!(
                    rest.first(),
                    Some(ColumnarValue::Scalar(ScalarValue::Int32(None)))
                )
            {
                return Ok(match length_array {
                    Some(array) => {
                        ColumnarValue::Array(new_null_array(&string.data_type(), array.len()))
                    }
                    None => ColumnarValue::Scalar(ScalarValue::try_from(&string.data_type())?),
                });
            }
            let pad = match rest {
                [_, ColumnarValue::Scalar(ScalarValue::Utf8(Some(pad)))] => pad.as_str(),
                _ => SPACE,
            };
            let scalar_lengths;
            let lengths = match length_array {
                Some(lengths) => lengths.as_primitive::<Int32Type>(),
                None => {
                    let ColumnarValue::Scalar(ScalarValue::Int32(Some(length))) = &rest[0] else {
                        unreachable!("non-null Int32 lengths are checked above");
                    };
                    // Match the existing scalar-length kernel: negative lengths
                    // become zero, which keeps read-side padding non-truncating.
                    scalar_lengths = Int32Array::from(vec![(*length).max(0)]);
                    &scalar_lengths
                }
            };
            let result = match string {
                ScalarValue::Utf8(Some(string)) => {
                    spark_pad_scalar_string::<i32>(string, lengths, pad, truncate, is_left_pad)
                }
                ScalarValue::LargeUtf8(Some(string)) => {
                    spark_pad_scalar_string::<i64>(string, lengths, pad, truncate, is_left_pad)
                }
                _ => unreachable!("null strings returned above"),
            }?;
            if length_array.is_some() {
                Ok(result)
            } else {
                let array = result.to_array(1)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &array, 0,
                )?))
            }
        }
        [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(None))]
        | [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(None)), ColumnarValue::Scalar(ScalarValue::Utf8(_))] => {
            Ok(ColumnarValue::Array(new_null_array(
                array.data_type(),
                array.len(),
            )))
        }
        [ColumnarValue::Array(array), length, ColumnarValue::Scalar(ScalarValue::Utf8(None))]
            if length.data_type() == DataType::Int32 =>
        {
            Ok(ColumnarValue::Array(new_null_array(
                array.data_type(),
                array.len(),
            )))
        }
        [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(Some(length)))] => {
            match array.data_type() {
                DataType::Utf8 => spark_read_side_padding_internal::<i32>(
                    array,
                    truncate,
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                    SPACE,
                    is_left_pad,
                ),
                DataType::LargeUtf8 => spark_read_side_padding_internal::<i64>(
                    array,
                    truncate,
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                    SPACE,
                    is_left_pad,
                ),
                // Dictionary support required for SPARK-48498
                DataType::Dictionary(_, value_type) => {
                    let dict = as_dictionary_array::<Int32Type>(array);
                    let col = if value_type.as_ref() == &DataType::Utf8 {
                        spark_read_side_padding_internal::<i32>(
                            dict.values(),
                            truncate,
                            ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                            SPACE,
                            is_left_pad,
                        )?
                    } else {
                        spark_read_side_padding_internal::<i64>(
                            dict.values(),
                            truncate,
                            ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                            SPACE,
                            is_left_pad,
                        )?
                    };
                    // col consists of an array, so arg of to_array() is not used. Can be anything
                    let values = col.to_array(0)?;
                    let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
                    Ok(ColumnarValue::Array(make_array(result.into())))
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for function rpad/read_side_padding",
                ))),
            }
        }
        [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(Some(length))), ColumnarValue::Scalar(ScalarValue::Utf8(Some(string)))] =>
        {
            match array.data_type() {
                DataType::Utf8 => spark_read_side_padding_internal::<i32>(
                    array,
                    truncate,
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                    string,
                    is_left_pad,
                ),
                DataType::LargeUtf8 => spark_read_side_padding_internal::<i64>(
                    array,
                    truncate,
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                    string,
                    is_left_pad,
                ),
                // Dictionary support required for SPARK-48498
                DataType::Dictionary(_, value_type) => {
                    let dict = as_dictionary_array::<Int32Type>(array);
                    let col = if value_type.as_ref() == &DataType::Utf8 {
                        spark_read_side_padding_internal::<i32>(
                            dict.values(),
                            truncate,
                            ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                            SPACE,
                            is_left_pad,
                        )?
                    } else {
                        spark_read_side_padding_internal::<i64>(
                            dict.values(),
                            truncate,
                            ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                            SPACE,
                            is_left_pad,
                        )?
                    };
                    // col consists of an array, so arg of to_array() is not used. Can be anything
                    let values = col.to_array(0)?;
                    let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
                    Ok(ColumnarValue::Array(make_array(result.into())))
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for function rpad/lpad/read_side_padding",
                ))),
            }
        }
        [ColumnarValue::Array(array), ColumnarValue::Array(array_int)] => match array.data_type() {
            DataType::Utf8 => spark_read_side_padding_internal::<i32>(
                array,
                truncate,
                ColumnarValue::Array(Arc::<dyn Array>::clone(array_int)),
                SPACE,
                is_left_pad,
            ),
            DataType::LargeUtf8 => spark_read_side_padding_internal::<i64>(
                array,
                truncate,
                ColumnarValue::Array(Arc::<dyn Array>::clone(array_int)),
                SPACE,
                is_left_pad,
            ),
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {other:?} for function rpad/lpad/read_side_padding",
            ))),
        },
        [ColumnarValue::Array(array), ColumnarValue::Array(array_int), ColumnarValue::Scalar(ScalarValue::Utf8(Some(string)))] => {
            match array.data_type() {
                DataType::Utf8 => spark_read_side_padding_internal::<i32>(
                    array,
                    truncate,
                    ColumnarValue::Array(Arc::<dyn Array>::clone(array_int)),
                    string,
                    is_left_pad,
                ),
                DataType::LargeUtf8 => spark_read_side_padding_internal::<i64>(
                    array,
                    truncate,
                    ColumnarValue::Array(Arc::<dyn Array>::clone(array_int)),
                    string,
                    is_left_pad,
                ),
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for function rpad/read_side_padding",
                ))),
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported arguments {other:?} for function rpad/lpad/read_side_padding",
        ))),
    }
}

/// Pads a borrowed scalar directly into the output for each non-null length.
fn spark_pad_scalar_string<T: OffsetSizeTrait>(
    string: &str,
    lengths: &Int32Array,
    pad_string: &str,
    truncate: bool,
    is_left_pad: bool,
) -> Result<ColumnarValue, DataFusionError> {
    let ascii = string.is_ascii();
    let char_len = if ascii {
        string.len()
    } else {
        string.chars().count()
    };
    let string_char_bytes = if ascii { 1 } else { 4 };
    let pad_char_bytes = if pad_string.is_ascii() { 1 } else { 4 };
    let mut data_capacity = 0usize;
    let mut max_padding = 0usize;
    for length in lengths.iter().flatten().filter(|length| *length >= 0) {
        let length = length as usize;
        // Bound capacity by the characters actually retained, rather than by
        // input bytes times row count or a target that an empty pad cannot fill.
        // UTF-8 uses at most four bytes per character.
        let string_bytes = if truncate {
            string.len().min(length.saturating_mul(string_char_bytes))
        } else {
            string.len()
        };
        let padding = if pad_string.is_empty() {
            0
        } else {
            length.saturating_sub(char_len)
        };
        data_capacity = data_capacity
            .saturating_add(string_bytes)
            .saturating_add(padding.saturating_mul(pad_char_bytes));
        max_padding = max_padding.max(padding);
    }
    let mut builder = GenericStringBuilder::<T>::with_capacity(lengths.len(), data_capacity);
    // Only this prefix can contribute to any output. Avoid copying and indexing
    // an arbitrarily large pattern when the result needs few or no pad characters.
    let pad_end = pad_string
        .char_indices()
        .nth(max_padding)
        .map_or(pad_string.len(), |(offset, _)| offset);
    let padder = Padder {
        pad: PadPattern::new(&pad_string[..pad_end], max_padding),
        ascii,
        truncate,
        is_left_pad,
    };
    for length in lengths {
        match length {
            Some(length) if length >= 0 => padder.append(&mut builder, string, length as usize),
            Some(_) => builder.append_value(""),
            None => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn spark_read_side_padding_internal<T: OffsetSizeTrait>(
    array: &ArrayRef,
    truncate: bool,
    pad_type: ColumnarValue,
    pad_string: &str,
    is_left_pad: bool,
) -> Result<ColumnarValue, DataFusionError> {
    let string_array = as_generic_string_array::<T>(array)?;

    match pad_type {
        ColumnarValue::Array(array_int) => {
            let int_pad_array = array_int.as_primitive::<Int32Type>();

            // Every row is padded to its target length, so the sum of the target
            // lengths sizes the output (exactly, for ASCII input), except when a
            // row is longer than its target and passes through untruncated. Null
            // lengths produce null rows and are skipped: the values under null
            // slots are unspecified and must not size the output.
            let mut data_capacity = 0usize;
            let mut max_length = 0usize;
            for length in int_pad_array.iter().flatten() {
                let length = length.max(0) as usize;
                data_capacity = data_capacity.saturating_add(length);
                max_length = max_length.max(length);
            }
            let mut builder = GenericStringBuilder::<T>::with_capacity(
                string_array.len(),
                data_capacity.max(string_array.value_data().len()),
            );
            let padder = Padder {
                pad: PadPattern::new(pad_string, max_length),
                ascii: string_array.is_ascii(),
                truncate,
                is_left_pad,
            };

            for (string, length) in string_array.iter().zip(int_pad_array) {
                match (string, length) {
                    (Some(string), Some(length)) => {
                        if length >= 0 {
                            padder.append(&mut builder, string, length as usize);
                        } else {
                            builder.append_value("");
                        }
                    }
                    // Spark's StringRPad/StringLPad are null-intolerant: a null
                    // string or a null length yields a null row.
                    _ => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        ColumnarValue::Scalar(const_pad_length) => {
            let length = 0.max(i32::try_from(const_pad_length)?) as usize;

            let mut builder = GenericStringBuilder::<T>::with_capacity(
                string_array.len(),
                string_array.len().saturating_mul(length),
            );
            let padder = Padder {
                pad: PadPattern::new(pad_string, length),
                ascii: string_array.is_ascii(),
                truncate,
                is_left_pad,
            };

            for string in string_array.iter() {
                match string {
                    Some(string) => padder.append(&mut builder, string, length),
                    _ => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
    }
}

/// The padding pattern, materialized as a repeating buffer so that padding a row
/// is a single copy of a slice rather than a character-at-a-time loop.
struct PadPattern<'a> {
    /// One repetition of the pattern.
    pattern: &'a str,
    /// Byte offset of the first `i` characters of one repetition, for every `i` in
    /// `0..=pattern.chars().count()`.
    char_offsets: Vec<usize>,
    /// The pattern repeated enough times to supply the longest padding needed.
    buffer: String,
}

impl<'a> PadPattern<'a> {
    /// Builds a pattern that can supply up to `max_chars` padding characters.
    fn new(pattern: &'a str, max_chars: usize) -> Self {
        let char_offsets: Vec<usize> = pattern
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(pattern.len()))
            .collect();
        let pattern_chars = char_offsets.len() - 1;
        let buffer = if pattern_chars == 0 {
            String::new()
        } else {
            pattern.repeat(max_chars.div_ceil(pattern_chars))
        };
        Self {
            pattern,
            char_offsets,
            buffer,
        }
    }

    /// Returns a slice holding exactly `chars` characters of the repeating pattern,
    /// or an empty slice if the pattern itself is empty. `chars` must not exceed the
    /// `max_chars` the pattern was built with.
    #[inline]
    fn slice(&self, chars: usize) -> &str {
        let pattern_chars = self.char_offsets.len() - 1;
        if pattern_chars == 0 {
            return "";
        }
        let bytes = if self.pattern.len() == pattern_chars {
            // One byte per character (the default single space), so no offset lookup.
            chars
        } else {
            (chars / pattern_chars) * self.pattern.len() + self.char_offsets[chars % pattern_chars]
        };
        &self.buffer[..bytes]
    }
}

/// Pads rows of a single string array, holding the settings that are constant
/// across the array.
struct Padder<'a> {
    pad: PadPattern<'a>,
    /// Whether every value in the array is ASCII, which lets character counts and
    /// truncation points be read off byte offsets directly.
    ascii: bool,
    truncate: bool,
    is_left_pad: bool,
}

impl Padder<'_> {
    /// Appends `string`, padded or truncated to `length` characters, to `builder`.
    #[inline]
    fn append<T: OffsetSizeTrait>(
        &self,
        builder: &mut GenericStringBuilder<T>,
        string: &str,
        length: usize,
    ) {
        // Spark's UTF8String uses char count, not grapheme count
        // https://stackoverflow.com/a/46290728
        let char_len = if self.ascii {
            string.len()
        } else {
            string.chars().count()
        };

        if length <= char_len {
            if self.truncate {
                let idx = if self.ascii {
                    length
                } else {
                    string
                        .char_indices()
                        .nth(length)
                        .map(|(i, _)| i)
                        .unwrap_or(string.len())
                };
                builder.append_value(&string[..idx]);
            } else {
                builder.append_value(string);
            }
            return;
        }

        let padding = self.pad.slice(length - char_len);
        let (first, second) = if self.is_left_pad {
            (padding, string)
        } else {
            (string, padding)
        };
        // Writing through `fmt::Write` appends to the value in progress, so the two
        // pieces are copied once each rather than staged in a temporary `String`.
        let _ = builder.write_str(first);
        let _ = builder.write_str(second);
        builder.append_value("");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};

    fn utf8(values: &[Option<&str>]) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(StringArray::from(values.to_vec())) as ArrayRef)
    }

    fn result_values(col: ColumnarValue) -> Vec<Option<String>> {
        let array = col.to_array(0).unwrap();
        array
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(|s| s.to_string()))
            .collect()
    }

    fn len_scalar(length: i32) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(length)))
    }

    fn pad_scalar(pad: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(pad.to_string())))
    }

    fn len_array(lengths: &[Option<i32>]) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(Int32Array::from(lengths.to_vec())) as ArrayRef)
    }

    fn assert_scalar(result: ColumnarValue, expected: ScalarValue) {
        match result {
            ColumnarValue::Scalar(value) => assert_eq!(value, expected),
            ColumnarValue::Array(_) => panic!("expected scalar padding result"),
        }
    }

    #[test]
    fn scalar_strings_return_scalars() {
        for is_left in [false, true] {
            let pad = if is_left { spark_lpad } else { spark_rpad };
            let expected = if is_left { "  abc" } else { "abc  " };
            assert_scalar(
                pad(&[pad_scalar("abc"), len_scalar(5)]).unwrap(),
                ScalarValue::Utf8(Some(expected.to_string())),
            );

            let expected = if is_left { "öxöúñ" } else { "úñöxö" };
            assert_scalar(
                pad(&[pad_scalar("úñ"), len_scalar(5), pad_scalar("öx")]).unwrap(),
                ScalarValue::Utf8(Some(expected.to_string())),
            );

            for (length, expected) in [(1, "ú"), (0, ""), (-1, "")] {
                assert_scalar(
                    pad(&[pad_scalar("úñ"), len_scalar(length)]).unwrap(),
                    ScalarValue::Utf8(Some(expected.to_string())),
                );
            }
            assert_scalar(
                pad(&[pad_scalar("abc"), len_scalar(5), pad_scalar("")]).unwrap(),
                ScalarValue::Utf8(Some("abc".to_string())),
            );
        }
    }

    #[test]
    fn scalar_strings_broadcast_to_length_array() {
        for is_left in [false, true] {
            let pad = if is_left { spark_lpad } else { spark_rpad };
            for pattern in [None, Some("öx")] {
                let mut args = vec![
                    pad_scalar("úñ"),
                    len_array(&[Some(5), Some(1), None, Some(0), Some(-1)]),
                ];
                if let Some(pattern) = pattern {
                    args.push(pad_scalar(pattern));
                }
                let padded = match (is_left, pattern) {
                    (false, None) => "úñ   ",
                    (true, None) => "   úñ",
                    (false, Some(_)) => "úñöxö",
                    (true, Some(_)) => "öxöúñ",
                };
                assert_eq!(
                    result_values(pad(&args).unwrap()),
                    vec![
                        Some(padded.to_string()),
                        Some("ú".to_string()),
                        None,
                        Some("".to_string()),
                        Some("".to_string()),
                    ]
                );
                args[1] = len_array(&[]);
                assert_eq!(
                    result_values(pad(&args).unwrap()),
                    Vec::<Option<String>>::new()
                );
            }
        }
    }

    #[test]
    fn long_scalar_strings_with_short_and_null_lengths() {
        let string = "é🙂中a".repeat(8192);
        for pad in [spark_lpad, spark_rpad] {
            for input in [
                ScalarValue::Utf8(Some(string.clone())),
                ScalarValue::LargeUtf8(Some(string.clone())),
            ] {
                let args = [
                    ColumnarValue::Scalar(input.clone()),
                    len_array(&[Some(0), Some(1), None, Some(-1), Some(3)]),
                    pad_scalar("öx"),
                ];
                let ColumnarValue::Array(result) = pad(&args).unwrap() else {
                    panic!("expected array padding result");
                };
                assert_eq!(result.data_type(), &input.data_type());
                for (row, value) in [Some(""), Some("é"), None, Some(""), Some("é🙂中")]
                    .into_iter()
                    .enumerate()
                {
                    let value = value.map(str::to_string);
                    let expected = match input {
                        ScalarValue::Utf8(_) => ScalarValue::Utf8(value),
                        _ => ScalarValue::LargeUtf8(value),
                    };
                    assert_eq!(ScalarValue::try_from_array(&result, row).unwrap(), expected);
                }
                // Short outputs must not retain a buffer sized for broadcasting
                // the long input to every row.
                assert!(result.to_data().buffers()[1].capacity() < string.len());
            }
        }
    }

    #[test]
    fn scalar_array_lengths_preserve_unicode_and_nontruncating_semantics() {
        for input in [
            ScalarValue::Utf8(Some("é🙂中".to_string())),
            ScalarValue::LargeUtf8(Some("é🙂中".to_string())),
        ] {
            for (pad, expected) in [spark_lpad, spark_rpad, spark_read_side_padding]
                .into_iter()
                .zip([
                    [Some(""), Some("é"), Some("💠xé🙂中"), None, Some("")],
                    [Some(""), Some("é"), Some("é🙂中💠x"), None, Some("")],
                    [
                        Some("é🙂中"),
                        Some("é🙂中"),
                        Some("é🙂中💠x"),
                        None,
                        Some(""),
                    ],
                ])
            {
                let args = [
                    ColumnarValue::Scalar(input.clone()),
                    len_array(&[Some(0), Some(1), Some(5), None, Some(-1)]),
                    pad_scalar("💠x"),
                ];
                let ColumnarValue::Array(result) = pad(&args).unwrap() else {
                    panic!("expected array padding result");
                };
                assert_eq!(result.data_type(), &input.data_type());
                for (row, value) in expected.into_iter().enumerate() {
                    let value = value.map(str::to_string);
                    let expected = match input {
                        ScalarValue::Utf8(_) => ScalarValue::Utf8(value),
                        _ => ScalarValue::LargeUtf8(value),
                    };
                    assert_eq!(ScalarValue::try_from_array(&result, row).unwrap(), expected);
                }
            }
        }
    }

    #[test]
    fn scalar_empty_pad_capacity_depends_on_output() {
        for pad in [spark_lpad, spark_rpad, spark_read_side_padding] {
            let args = [
                pad_scalar("é🙂中"),
                len_array(&[Some(1_000_000), None, Some(1)]),
                pad_scalar(""),
            ];
            let ColumnarValue::Array(result) = pad(&args).unwrap() else {
                panic!("expected array padding result");
            };
            let strings = result.as_string::<i32>();
            assert_eq!(strings.value(0), "é🙂中");
            assert!(strings.is_null(1));
            // An empty pad cannot fill the requested length, so that length must
            // not reserve a large output buffer.
            assert!(result.to_data().buffers()[1].capacity() < 1024);
        }
    }

    #[test]
    fn scalar_padding_rejects_unsupported_argument_shapes() {
        for pad in [spark_lpad, spark_rpad, spark_read_side_padding] {
            for input in [
                pad_scalar("abc"),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ] {
                for args in [
                    vec![input.clone()],
                    vec![input.clone(), pad_scalar("3")],
                    vec![input.clone(), len_array(&[Some(3)]), utf8(&[Some("x")])],
                    vec![
                        input.clone(),
                        len_array(&[Some(3)]),
                        ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)),
                    ],
                    vec![
                        input.clone(),
                        len_scalar(3),
                        pad_scalar("x"),
                        pad_scalar("y"),
                    ],
                ] {
                    assert!(pad(&args).is_err());
                }
            }
        }
    }

    #[test]
    fn scalar_padding_nulls() {
        for pad in [spark_lpad, spark_rpad] {
            for args in [
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                    len_scalar(5),
                ],
                vec![
                    pad_scalar("abc"),
                    ColumnarValue::Scalar(ScalarValue::Int32(None)),
                ],
                vec![
                    pad_scalar("abc"),
                    len_scalar(5),
                    ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ],
            ] {
                assert_scalar(pad(&args).unwrap(), ScalarValue::Utf8(None));
            }
            let args = [
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                len_array(&[Some(5), None]),
            ];
            assert_eq!(result_values(pad(&args).unwrap()), vec![None, None]);
            let args = [
                pad_scalar("abc"),
                len_array(&[Some(5), None]),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ];
            assert_eq!(result_values(pad(&args).unwrap()), vec![None, None]);
        }
    }

    #[test]
    fn null_scalar_strings_preserve_type_and_shape_with_large_lengths() {
        // Keep the requested length bounded so a regression cannot exhaust the
        // test runner, while exercising a length far larger than the input.
        let large_length = 1_000_000;
        for pad in [spark_lpad, spark_rpad, spark_read_side_padding] {
            for null_string in [ScalarValue::Utf8(None), ScalarValue::LargeUtf8(None)] {
                let scalar = ColumnarValue::Scalar(null_string.clone());
                for pattern in [None, Some("öx")] {
                    let mut args = vec![scalar.clone(), len_scalar(large_length)];
                    if let Some(pattern) = pattern {
                        args.push(pad_scalar(pattern));
                    }
                    assert_scalar(pad(&args).unwrap(), null_string.clone());
                    for lengths in [vec![Some(large_length), None, Some(0)], vec![]] {
                        args[1] = len_array(&lengths);
                        let ColumnarValue::Array(result) = pad(&args).unwrap() else {
                            panic!("expected array padding result for array lengths");
                        };
                        assert_eq!(result.data_type(), &null_string.data_type());
                        assert_eq!(result.len(), lengths.len());
                        assert_eq!(result.null_count(), lengths.len());
                    }
                }
            }
        }
    }

    #[test]
    fn large_utf8_scalar_retains_type() {
        let input = ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("úñ".to_string())));
        assert_scalar(
            spark_lpad(&[input.clone(), len_scalar(4), pad_scalar("ö")]).unwrap(),
            ScalarValue::LargeUtf8(Some("ööúñ".to_string())),
        );
        assert_scalar(
            spark_rpad(&[input, len_scalar(4), pad_scalar("ö")]).unwrap(),
            ScalarValue::LargeUtf8(Some("úñöö".to_string())),
        );
    }

    #[test]
    fn read_side_padding_accepts_scalar_without_truncating() {
        assert_scalar(
            spark_read_side_padding(&[pad_scalar("abcdef"), len_scalar(3)]).unwrap(),
            ScalarValue::Utf8(Some("abcdef".to_string())),
        );
        assert_eq!(
            result_values(
                spark_read_side_padding(&[pad_scalar("abc"), len_array(&[Some(1), Some(5)])])
                    .unwrap()
            ),
            vec![Some("abc".to_string()), Some("abc  ".to_string())]
        );
    }

    #[test]
    fn rpad_default_padding() {
        let args = vec![
            utf8(&[Some("abc"), None, Some(""), Some("abcdef")]),
            len_scalar(5),
        ];
        assert_eq!(
            result_values(spark_rpad(&args).unwrap()),
            vec![
                Some("abc  ".to_string()),
                None,
                Some("     ".to_string()),
                Some("abcde".to_string()),
            ]
        );
    }

    #[test]
    fn lpad_default_padding() {
        let args = vec![utf8(&[Some("abc"), None, Some("abcdef")]), len_scalar(5)];
        assert_eq!(
            result_values(spark_lpad(&args).unwrap()),
            vec![Some("  abc".to_string()), None, Some("abcde".to_string()),]
        );
    }

    #[test]
    fn read_side_padding_does_not_truncate() {
        let args = vec![utf8(&[Some("abcdef")]), len_scalar(3)];
        assert_eq!(
            result_values(spark_read_side_padding(&args).unwrap()),
            vec![Some("abcdef".to_string())]
        );
    }

    #[test]
    fn multi_char_pattern_cycles() {
        let args = vec![utf8(&[Some("x")]), len_scalar(8), pad_scalar("ab")];
        assert_eq!(
            result_values(spark_rpad(&args).unwrap()),
            vec![Some("xabababa".to_string())]
        );
        let args = vec![utf8(&[Some("x")]), len_scalar(8), pad_scalar("ab")];
        assert_eq!(
            result_values(spark_lpad(&args).unwrap()),
            vec![Some("abababax".to_string())]
        );
    }

    #[test]
    fn padding_counts_chars_not_bytes() {
        // multi-byte characters count as one character each
        let args = vec![utf8(&[Some("úñî")]), len_scalar(5), pad_scalar("ö")];
        assert_eq!(
            result_values(spark_rpad(&args).unwrap()),
            vec![Some("úñîöö".to_string())]
        );
        // truncation is by character, not byte
        let args = vec![utf8(&[Some("úñîçö")]), len_scalar(2)];
        assert_eq!(
            result_values(spark_rpad(&args).unwrap()),
            vec![Some("úñ".to_string())]
        );
    }

    #[test]
    fn empty_pad_string_leaves_value_unchanged() {
        let args = vec![utf8(&[Some("abc")]), len_scalar(6), pad_scalar("")];
        assert_eq!(
            result_values(spark_rpad(&args).unwrap()),
            vec![Some("abc".to_string())]
        );
    }

    #[test]
    fn negative_length_yields_empty_string() {
        let lengths = ColumnarValue::Array(Arc::new(Int32Array::from(vec![-1, 4])) as ArrayRef);
        let args = vec![utf8(&[Some("abc"), Some("abc")]), lengths];
        assert_eq!(
            result_values(spark_rpad(&args).unwrap()),
            vec![Some("".to_string()), Some("abc ".to_string())]
        );
    }

    #[test]
    fn length_from_array() {
        let lengths = ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 5, 3])) as ArrayRef);
        let args = vec![utf8(&[Some("abc"), Some("abc"), None]), lengths];
        assert_eq!(
            result_values(spark_lpad(&args).unwrap()),
            vec![Some("a".to_string()), Some("  abc".to_string()), None]
        );
    }

    #[test]
    fn rpad_null_length_yields_null_row() {
        // Spark's StringRPad is null-intolerant: a NULL length makes only that
        // row NULL, the other rows are padded or truncated as usual.
        let strings = [Some("abc"), Some("abc"), Some("abcdef"), Some("abc")];
        let lengths = [Some(5), None, Some(2), Some(-1)];
        // 2 args (default pad of ' ')
        let args = vec![utf8(&strings), len_array(&lengths)];
        assert_eq!(
            result_values(spark_rpad(&args).unwrap()),
            vec![
                Some("abc  ".to_string()),
                None,
                Some("ab".to_string()),
                Some("".to_string()),
            ]
        );
        // 3 args
        let args = vec![utf8(&strings), len_array(&lengths), pad_scalar("xy")];
        assert_eq!(
            result_values(spark_rpad(&args).unwrap()),
            vec![
                Some("abcxy".to_string()),
                None,
                Some("ab".to_string()),
                Some("".to_string()),
            ]
        );
        // read-side padding takes the same path, without truncation
        let args = vec![utf8(&strings), len_array(&lengths)];
        assert_eq!(
            result_values(spark_read_side_padding(&args).unwrap()),
            vec![
                Some("abc  ".to_string()),
                None,
                Some("abcdef".to_string()),
                Some("".to_string()),
            ]
        );
    }

    #[test]
    fn lpad_null_length_yields_null_row() {
        let strings = [Some("abc"), Some("abc"), Some("abcdef"), Some("abc")];
        let lengths = [Some(5), None, Some(2), Some(-1)];
        // 2 args (default pad of ' ')
        let args = vec![utf8(&strings), len_array(&lengths)];
        assert_eq!(
            result_values(spark_lpad(&args).unwrap()),
            vec![
                Some("  abc".to_string()),
                None,
                Some("ab".to_string()),
                Some("".to_string()),
            ]
        );
        // 3 args
        let args = vec![utf8(&strings), len_array(&lengths), pad_scalar("xy")];
        assert_eq!(
            result_values(spark_lpad(&args).unwrap()),
            vec![
                Some("xyabc".to_string()),
                None,
                Some("ab".to_string()),
                Some("".to_string()),
            ]
        );
    }

    #[test]
    fn null_string_and_null_length_yields_null_row() {
        let strings = [None, None, Some("abc")];
        let lengths = [None, Some(4), None];
        let args = vec![utf8(&strings), len_array(&lengths)];
        assert_eq!(
            result_values(spark_rpad(&args).unwrap()),
            vec![None, None, None]
        );
        let args = vec![utf8(&strings), len_array(&lengths), pad_scalar("x")];
        assert_eq!(
            result_values(spark_lpad(&args).unwrap()),
            vec![None, None, None]
        );
    }

    #[test]
    fn all_null_lengths_yield_all_null_rows() {
        let args = vec![utf8(&[Some("abc"), Some("")]), len_array(&[None, None])];
        assert_eq!(result_values(spark_rpad(&args).unwrap()), vec![None, None]);
        let args = vec![utf8(&[Some("abc"), Some("")]), len_array(&[None, None])];
        assert_eq!(result_values(spark_lpad(&args).unwrap()), vec![None, None]);
    }
}
