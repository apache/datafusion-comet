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
use arrow::array::{make_array, Array, AsArray, DictionaryArray};
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
            // row is longer than its target and passes through untruncated.
            let mut data_capacity = 0usize;
            let mut max_length = 0usize;
            for length in int_pad_array.values() {
                let length = (*length).max(0) as usize;
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
                let length = length.unwrap();
                match string {
                    Some(string) => {
                        if length >= 0 {
                            padder.append(&mut builder, string, length as usize);
                        } else {
                            builder.append_value("");
                        }
                    }
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
}
