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

use crate::utils::make_scalar_function;
use arrow::array::builder::GenericStringBuilder;
use arrow::array::types::Int32Type;
use arrow::array::{as_dictionary_array, make_array, Array, AsArray, DictionaryArray};
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_generic_string_array, DataFusionError};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

const SPACE: &str = " ";
/// Similar to DataFusion `rpad`, but not to truncate when the string is already longer than length
pub fn spark_read_side_padding(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    make_scalar_function(spark_read_side_padding_no_truncate)(args)
}

/// Custom `rpad` because DataFusion's `rpad` has differences in unicode handling
pub fn spark_rpad(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    make_scalar_function(spark_read_side_padding_truncate)(args)
}

pub fn spark_read_side_padding_truncate(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    spark_read_side_padding2(args, true)
}

pub fn spark_read_side_padding_no_truncate(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    spark_read_side_padding2(args, false)
}

fn spark_read_side_padding2(
    args: &[ArrayRef],
    truncate: bool,
) -> Result<ArrayRef, DataFusionError> {
    match args {
        [array, array_int] => match array.data_type() {
            DataType::Utf8 => {
                spark_read_side_padding_space_internal::<i32>(array, truncate, array_int)
            }
            DataType::LargeUtf8 => {
                spark_read_side_padding_space_internal::<i64>(array, truncate, array_int)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {other:?} for function rpad/read_side_padding",
            ))),
        },
        [array, array_int, array_pad_string] => {
            match (array.data_type(), array_pad_string.data_type()) {
                (DataType::Utf8, DataType::Utf8) => {
                    spark_read_side_padding_internal::<i32, i32, i32>(
                        array,
                        truncate,
                        array_int,
                        array_pad_string,
                    )
                }
                (DataType::Utf8, DataType::LargeUtf8) => {
                    spark_read_side_padding_internal::<i32, i64, i64>(
                        array,
                        truncate,
                        array_int,
                        array_pad_string,
                    )
                }
                (DataType::LargeUtf8, DataType::Utf8) => {
                    spark_read_side_padding_internal::<i64, i32, i64>(
                        array,
                        truncate,
                        array_int,
                        array_pad_string,
                    )
                }
                (DataType::LargeUtf8, DataType::LargeUtf8) => {
                    spark_read_side_padding_internal::<i64, i64, i64>(
                        array,
                        truncate,
                        array_int,
                        array_pad_string,
                    )
                }
                // Dictionary support required for SPARK-48498
                (DataType::Dictionary(_, value_type), DataType::Utf8) => {
                    let dict = as_dictionary_array::<Int32Type>(array);
                    let values = if value_type.as_ref() == &DataType::Utf8 {
                        spark_read_side_padding_internal::<i32, i32, i32>(
                            dict.values(),
                            truncate,
                            array_int,
                            array_pad_string,
                        )?
                    } else {
                        spark_read_side_padding_internal::<i64, i32, i64>(
                            dict.values(),
                            truncate,
                            array_int,
                            array_pad_string,
                        )?
                    };
                    let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
                    Ok(make_array(result.into()))
                }
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

fn spark_read_side_padding_space_internal<T: OffsetSizeTrait>(
    array: &ArrayRef,
    truncate: bool,
    array_int: &ArrayRef,
) -> Result<ArrayRef, DataFusionError> {
    let string_array = as_generic_string_array::<T>(array)?;
    let int_pad_array = array_int.as_primitive::<Int32Type>();

    let mut builder = GenericStringBuilder::<T>::with_capacity(
        string_array.len(),
        string_array.len() * int_pad_array.len(),
    );

    for (string, length) in string_array.iter().zip(int_pad_array) {
        match (string, length) {
            (Some(string), Some(length)) => builder.append_value(add_padding_string(
                string.parse().unwrap(),
                length as usize,
                truncate,
                SPACE,
            )?),
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn spark_read_side_padding_internal<T: OffsetSizeTrait, O: OffsetSizeTrait, S: OffsetSizeTrait>(
    array: &ArrayRef,
    truncate: bool,
    array_int: &ArrayRef,
    pad_string_array: &ArrayRef,
) -> Result<ArrayRef, DataFusionError> {
    let string_array = as_generic_string_array::<T>(array)?;
    let int_pad_array = array_int.as_primitive::<Int32Type>();
    let pad_string_array = as_generic_string_array::<O>(pad_string_array)?;

    let mut builder = GenericStringBuilder::<S>::with_capacity(
        string_array.len(),
        string_array.len() * int_pad_array.len(),
    );

    for ((string, length), pad_string) in string_array
        .iter()
        .zip(int_pad_array)
        .zip(pad_string_array.iter())
    {
        match (string, length, pad_string) {
            (Some(string), Some(length), Some(pad_string)) => {
                builder.append_value(add_padding_string(
                    string.parse().unwrap(),
                    length as usize,
                    truncate,
                    pad_string,
                )?)
            }
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn add_padding_string(
    string: String,
    length: usize,
    truncate: bool,
    pad_string: &str,
) -> Result<String, DataFusionError> {
    // It looks Spark's UTF8String is closer to chars rather than graphemes
    // https://stackoverflow.com/a/46290728
    let char_len = string.chars().count();
    if length <= char_len {
        if truncate {
            let idx = string
                .char_indices()
                .nth(length)
                .map(|(i, _)| i)
                .unwrap_or(string.len());
            match string[..idx].parse() {
                Ok(string) => Ok(string),
                Err(err) => Err(DataFusionError::Internal(format!(
                    "Failed adding padding string {} error {:}",
                    string, err
                ))),
            }
        } else {
            Ok(string)
        }
    } else {
        let pad_needed = length - char_len;
        let pad: String = pad_string.chars().cycle().take(pad_needed).collect();
        Ok(string + &pad)
    }
}
