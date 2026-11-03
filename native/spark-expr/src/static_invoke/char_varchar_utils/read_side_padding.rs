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
use arrow::array::{make_array, new_null_array, Array, AsArray, DictionaryArray};
use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait};
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
        // Spark's StringRPad/StringLPad are null-intolerant, so a NULL length makes every
        // row NULL. A scalar subquery that returns no rows produces such a length.
        [string, ColumnarValue::Scalar(ScalarValue::Int32(None)), ..] => match string {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(new_null_array(
                array.data_type(),
                array.len(),
            ))),
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                scalar.data_type(),
            )?)),
        },
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
        // The string argument can be a scalar even when it references columns, e.g.
        // `IF(p, (SELECT max(s) FROM t), s)` when every row takes the subquery branch.
        // Every row reads the one value rather than an expanded copy, which for a wide string
        // could exceed the 32-bit offsets of the input array.
        [ColumnarValue::Scalar(string), length, rest @ ..] => {
            let pad_string = match rest {
                [] => SPACE,
                [ColumnarValue::Scalar(ScalarValue::Utf8(Some(pad)))] => pad.as_str(),
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported arguments {args:?} for function rpad/lpad/read_side_padding",
                    )))
                }
            };
            let num_rows = match length {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            };
            let len = num_rows.unwrap_or(1);
            let result = match string {
                ScalarValue::Utf8(value) => pad_strings::<i32>(
                    StringInput::Scalar {
                        value: value.as_deref(),
                        len,
                    },
                    truncate,
                    length.clone(),
                    pad_string,
                    is_left_pad,
                )?,
                ScalarValue::LargeUtf8(value) => pad_strings::<i64>(
                    StringInput::Scalar {
                        value: value.as_deref(),
                        len,
                    },
                    truncate,
                    length.clone(),
                    pad_string,
                    is_left_pad,
                )?,
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data type {:?} for function rpad/lpad/read_side_padding",
                        other.data_type()
                    )))
                }
            };
            match num_rows {
                Some(_) => Ok(result),
                // Every argument was a scalar, so the result is one too.
                None => Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result.to_array(1)?,
                    0,
                )?)),
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported arguments {other:?} for function rpad/lpad/read_side_padding",
        ))),
    }
}

fn spark_read_side_padding_internal<T: OffsetSizeTrait>(
    array: &ArrayRef,
    truncate: bool,
    pad_type: ColumnarValue,
    pad_string: &str,
    is_left_pad: bool,
) -> Result<ColumnarValue, DataFusionError> {
    pad_strings(
        StringInput::Array(as_generic_string_array::<T>(array)?),
        truncate,
        pad_type,
        pad_string,
        is_left_pad,
    )
}

/// The strings to pad: a string array, or one value shared by `len` rows.
enum StringInput<'a, T: OffsetSizeTrait> {
    Array(&'a GenericStringArray<T>),
    Scalar { value: Option<&'a str>, len: usize },
}

impl<'a, T: OffsetSizeTrait> StringInput<'a, T> {
    fn len(&self) -> usize {
        match self {
            Self::Array(array) => array.len(),
            Self::Scalar { len, .. } => *len,
        }
    }

    fn is_ascii(&self) -> bool {
        match self {
            Self::Array(array) => array.is_ascii(),
            Self::Scalar { value, .. } => value.is_none_or(str::is_ascii),
        }
    }

    /// Bytes of input values, used to size the output when rows pass through untruncated. A
    /// scalar counts once: reserving it for every row could exceed the output's offsets.
    fn value_bytes(&self) -> usize {
        match self {
            Self::Array(array) => array.value_data().len(),
            Self::Scalar { value, .. } => value.map_or(0, str::len),
        }
    }

    #[inline]
    fn value(&self, i: usize) -> Option<&'a str> {
        match self {
            Self::Array(array) => array.is_valid(i).then(|| array.value(i)),
            Self::Scalar { value, .. } => *value,
        }
    }
}

fn pad_strings<T: OffsetSizeTrait>(
    strings: StringInput<'_, T>,
    truncate: bool,
    pad_type: ColumnarValue,
    pad_string: &str,
    is_left_pad: bool,
) -> Result<ColumnarValue, DataFusionError> {
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
                strings.len(),
                data_capacity.max(strings.value_bytes()),
            );
            let padder = Padder {
                pad: PadPattern::new(pad_string, max_length),
                ascii: strings.is_ascii(),
                truncate,
                is_left_pad,
            };

            for (i, length) in int_pad_array.iter().enumerate() {
                match (strings.value(i), length) {
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
                strings.len(),
                strings.len().saturating_mul(length),
            );
            let padder = Padder {
                pad: PadPattern::new(pad_string, length),
                ascii: strings.is_ascii(),
                truncate,
                is_left_pad,
            };

            for i in 0..strings.len() {
                match strings.value(i) {
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

    fn utf8_scalar(value: Option<&str>) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(value.map(str::to_string)))
    }

    fn scalar_result(col: ColumnarValue) -> ScalarValue {
        match col {
            ColumnarValue::Scalar(value) => value,
            ColumnarValue::Array(array) => panic!("expected a scalar, got {array:?}"),
        }
    }

    // A conditional such as `IF(p, (SELECT max(s) FROM t), s)` evaluates to a
    // scalar when every row in the batch takes the subquery branch (#7245).
    #[test]
    fn scalar_string_with_scalar_length() {
        let args = vec![utf8_scalar(Some("cd")), len_scalar(5), pad_scalar("0")];
        assert_eq!(
            scalar_result(spark_lpad(&args).unwrap()),
            ScalarValue::Utf8(Some("000cd".to_string()))
        );
        assert_eq!(
            scalar_result(spark_rpad(&args).unwrap()),
            ScalarValue::Utf8(Some("cd000".to_string()))
        );
        let args = vec![utf8_scalar(Some("abcdef")), len_scalar(3)];
        assert_eq!(
            scalar_result(spark_rpad(&args).unwrap()),
            ScalarValue::Utf8(Some("abc".to_string()))
        );
        assert_eq!(
            scalar_result(spark_read_side_padding(&args).unwrap()),
            ScalarValue::Utf8(Some("abcdef".to_string()))
        );
    }

    #[test]
    fn scalar_string_with_array_length() {
        let lengths = [Some(3), None, Some(1), Some(-1)];
        let args = vec![utf8_scalar(Some("ab")), len_array(&lengths)];
        assert_eq!(
            result_values(spark_rpad(&args).unwrap()),
            vec![
                Some("ab ".to_string()),
                None,
                Some("a".to_string()),
                Some("".to_string()),
            ]
        );
        let args = vec![
            utf8_scalar(Some("ab")),
            len_array(&lengths),
            pad_scalar("x"),
        ];
        assert_eq!(
            result_values(spark_lpad(&args).unwrap()),
            vec![
                Some("xab".to_string()),
                None,
                Some("a".to_string()),
                Some("".to_string()),
            ]
        );
    }

    #[test]
    fn null_scalar_string_yields_null() {
        let args = vec![utf8_scalar(None), len_scalar(5), pad_scalar("0")];
        assert_eq!(
            scalar_result(spark_lpad(&args).unwrap()),
            ScalarValue::Utf8(None)
        );
        let args = vec![utf8_scalar(None), len_array(&[Some(2), None])];
        assert_eq!(result_values(spark_rpad(&args).unwrap()), vec![None, None]);
    }

    #[test]
    fn large_utf8_scalar_string() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("x".to_string()))),
            len_scalar(3),
            pad_scalar("ab"),
        ];
        assert_eq!(
            scalar_result(spark_lpad(&args).unwrap()),
            ScalarValue::LargeUtf8(Some("abx".to_string()))
        );
    }

    #[test]
    fn all_null_lengths_yield_all_null_rows() {
        let args = vec![utf8(&[Some("abc"), Some("")]), len_array(&[None, None])];
        assert_eq!(result_values(spark_rpad(&args).unwrap()), vec![None, None]);
        let args = vec![utf8(&[Some("abc"), Some("")]), len_array(&[None, None])];
        assert_eq!(result_values(spark_lpad(&args).unwrap()), vec![None, None]);
    }

    // A scalar subquery that returns no rows yields a NULL scalar length (#7452).
    #[test]
    fn null_scalar_length_yields_null() {
        let null_length = ColumnarValue::Scalar(ScalarValue::Int32(None));
        let strings = utf8(&[Some("abc"), None]);
        for args in [
            vec![strings.clone(), null_length.clone()],
            vec![strings, null_length.clone(), pad_scalar("x")],
        ] {
            assert_eq!(result_values(spark_rpad(&args).unwrap()), vec![None, None]);
            assert_eq!(result_values(spark_lpad(&args).unwrap()), vec![None, None]);
            assert_eq!(
                result_values(spark_read_side_padding(&args).unwrap()),
                vec![None, None]
            );
        }
        let args = vec![utf8_scalar(Some("abc")), null_length, pad_scalar("x")];
        assert_eq!(
            scalar_result(spark_lpad(&args).unwrap()),
            ScalarValue::Utf8(None)
        );
    }

    // Expanding a 1 MiB scalar to 4,096 rows would need 4 GiB of input, past the 32-bit
    // offsets of a Utf8 array, for 4 KiB of output.
    #[test]
    fn wide_scalar_string_is_not_expanded() {
        let wide = "x".repeat(1 << 20);
        let rows = 4096;
        let lengths = len_array(&vec![Some(1); rows]);
        for pad in [spark_lpad, spark_rpad] {
            let args = vec![utf8_scalar(Some(&wide)), lengths.clone(), pad_scalar("0")];
            let result = pad(&args).unwrap().to_array(rows).unwrap();
            let result = result.as_string::<i32>();
            assert_eq!(result.len(), rows);
            assert!(result.iter().all(|v| v == Some("x")));
        }
    }

    // A dictionary scalar is rejected rather than routed to the dictionary arm.
    #[test]
    fn dictionary_scalar_string_is_unsupported() {
        let string = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8(Some("ab".to_string()))),
        );
        let args = vec![
            ColumnarValue::Scalar(string),
            len_array(&[Some(4)]),
            pad_scalar("x"),
        ];
        assert!(spark_rpad(&args).is_err());
    }
}
