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

    // Pre-compute pad characters once to avoid repeated iteration
    let pad_chars: Vec<char> = pad_string.chars().collect();

    match pad_type {
        ColumnarValue::Array(array_int) => {
            let int_pad_array = array_int.as_primitive::<Int32Type>();

            let mut builder = GenericStringBuilder::<T>::with_capacity(
                string_array.len(),
                string_array.len() * int_pad_array.len(),
            );

            // Reusable buffer to avoid per-element allocations
            let mut buffer = String::with_capacity(pad_chars.len());

            for (string, length) in string_array.iter().zip(int_pad_array) {
                let length = length.unwrap();
                match string {
                    Some(string) => {
                        if length >= 0 {
                            buffer.clear();
                            write_padded_string(
                                &mut buffer,
                                string,
                                length as usize,
                                truncate,
                                &pad_chars,
                                is_left_pad,
                            );
                            builder.append_value(&buffer);
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
                string_array.len() * length,
            );

            // Reusable buffer to avoid per-element allocations
            let mut buffer = String::with_capacity(length);

            for string in string_array.iter() {
                match string {
                    Some(string) => {
                        buffer.clear();
                        write_padded_string(
                            &mut buffer,
                            string,
                            length,
                            truncate,
                            &pad_chars,
                            is_left_pad,
                        );
                        builder.append_value(&buffer);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
    }
}

/// Writes a padded string to the provided buffer, avoiding allocations.
///
/// The buffer is assumed to be cleared before calling this function.
/// Padding characters are written directly to the buffer without intermediate allocations.
#[inline]
fn write_padded_string(
    buffer: &mut String,
    string: &str,
    length: usize,
    truncate: bool,
    pad_chars: &[char],
    is_left_pad: bool,
) {
    // Spark's UTF8String uses char count, not grapheme count
    // https://stackoverflow.com/a/46290728
    let char_len = string.chars().count();

    if length <= char_len {
        if truncate {
            // Find byte index for the truncation point
            let idx = string
                .char_indices()
                .nth(length)
                .map(|(i, _)| i)
                .unwrap_or(string.len());
            buffer.push_str(&string[..idx]);
        } else {
            buffer.push_str(string);
        }
    } else {
        let pad_needed = length - char_len;

        if is_left_pad {
            // Write padding first, then string
            write_padding_chars(buffer, pad_chars, pad_needed);
            buffer.push_str(string);
        } else {
            // Write string first, then padding
            buffer.push_str(string);
            write_padding_chars(buffer, pad_chars, pad_needed);
        }
    }
}

/// Writes `count` characters from the cycling pad pattern directly to the buffer.
#[inline]
fn write_padding_chars(buffer: &mut String, pad_chars: &[char], count: usize) {
    if pad_chars.is_empty() {
        return;
    }

    // Optimize for the common single-character padding case
    if pad_chars.len() == 1 {
        let ch = pad_chars[0];
        for _ in 0..count {
            buffer.push(ch);
        }
    } else {
        // Multi-character padding: cycle through pad_chars
        let mut remaining = count;
        while remaining > 0 {
            for &ch in pad_chars {
                if remaining == 0 {
                    break;
                }
                buffer.push(ch);
                remaining -= 1;
            }
        }
    }
}
