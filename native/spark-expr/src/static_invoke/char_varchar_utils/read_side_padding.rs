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

/// Similar to DataFusion `rpad`, but not to truncate when the string is already longer than length
pub fn spark_read_side_padding(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_read_side_padding2(args, false)
}

/// Custom `rpad` because DataFusion's `rpad` has differences in unicode handling
pub fn spark_rpad(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_read_side_padding2(args, true)
}

fn spark_read_side_padding2(
    args: &[ColumnarValue],
    truncate: bool,
) -> Result<ColumnarValue, DataFusionError> {
    match args {
        [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(Some(length)))] => {
            match array.data_type() {
                DataType::Utf8 => spark_read_side_padding_internal::<i32>(
                    array,
                    truncate,
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                ),
                DataType::LargeUtf8 => spark_read_side_padding_internal::<i64>(
                    array,
                    truncate,
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                ),
                // Dictionary support required for SPARK-48498
                DataType::Dictionary(_, value_type) => {
                    let dict = as_dictionary_array::<Int32Type>(array);
                    let col = if value_type.as_ref() == &DataType::Utf8 {
                        spark_read_side_padding_internal::<i32>(
                            dict.values(),
                            truncate,
                            ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
                        )?
                    } else {
                        spark_read_side_padding_internal::<i64>(
                            dict.values(),
                            truncate,
                            ColumnarValue::Scalar(ScalarValue::Int32(Some(*length))),
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
        [ColumnarValue::Array(array), ColumnarValue::Array(array_int)] => {
            match array.data_type() {
                DataType::Utf8 => spark_read_side_padding_internal::<i32>(
                    array,
                    truncate,
                    ColumnarValue::Array(Arc::<dyn Array>::clone(array_int)),
                ),
                DataType::LargeUtf8 => spark_read_side_padding_internal::<i64>(
                    array,
                    truncate,
                    ColumnarValue::Array(Arc::<dyn Array>::clone(array_int)),
                ),
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for function rpad/read_side_padding",
                ))),
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported arguments {other:?} for function rpad/read_side_padding",
        ))),
    }
}

fn spark_read_side_padding_internal<T: OffsetSizeTrait>(
    array: &ArrayRef,
    truncate: bool,
    pad_type: ColumnarValue,
) -> Result<ColumnarValue, DataFusionError> {
    let string_array = as_generic_string_array::<T>(array)?;
    match pad_type {
        ColumnarValue::Array(array_int) => {
            let int_pad_array = array_int.as_primitive::<Int32Type>();

            let mut builder = GenericStringBuilder::<T>::with_capacity(
                string_array.len(),
                string_array.len() * int_pad_array.len(),
            );

            for (string, length) in string_array.iter().zip(int_pad_array) {
                match string {
                    Some(string) => builder.append_value(add_padding_string(
                        string.parse().unwrap(),
                        length.unwrap() as usize,
                        truncate,
                    )),
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

            for string in string_array.iter() {
                match string {
                    Some(string) => builder.append_value(add_padding_string(
                        string.parse().unwrap(),
                        length,
                        truncate,
                    )),
                    _ => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
    }
}

fn add_padding_string(string: String, length: usize, truncate: bool) -> String {
    // It looks Spark's UTF8String is closer to chars rather than graphemes
    // https://stackoverflow.com/a/46290728
    let space_string = " ".repeat(length);
    let char_len = string.chars().count();
    if length <= char_len {
        if truncate {
            let idx = string
                .char_indices()
                .nth(length)
                .map(|(i, _)| i)
                .unwrap_or(string.len());
            string[..idx].parse().unwrap()
        } else {
            string
        }
    } else {
        string + &space_string[char_len..]
    }
}
