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
use datafusion::common::{cast::as_generic_string_array, DataFusionError, HashMap, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::fmt::Write;
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
            let rpad_arg = RPadArgument::ConstLength(*length);
            match array.data_type() {
                DataType::Utf8 => {
                    spark_read_side_padding_internal::<i32>(array, truncate, rpad_arg)
                }
                DataType::LargeUtf8 => {
                    spark_read_side_padding_internal::<i64>(array, truncate, rpad_arg)
                }
                // Dictionary support required for SPARK-48498
                DataType::Dictionary(_, value_type) => {
                    let dict = as_dictionary_array::<Int32Type>(array);
                    let col = if value_type.as_ref() == &DataType::Utf8 {
                        spark_read_side_padding_internal::<i32>(dict.values(), truncate, rpad_arg)?
                    } else {
                        spark_read_side_padding_internal::<i64>(dict.values(), truncate, rpad_arg)?
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
            let rpad_arg = RPadArgument::ColArray(Arc::clone(array_int));
            match array.data_type() {
                DataType::Utf8 => {
                    spark_read_side_padding_internal::<i32>(array, truncate, rpad_arg)
                }
                DataType::LargeUtf8 => {
                    spark_read_side_padding_internal::<i64>(array, truncate, rpad_arg)
                }
                // Dictionary support required for SPARK-48498
                DataType::Dictionary(_, value_type) => {
                    let dict = as_dictionary_array::<Int32Type>(array);
                    let col = if value_type.as_ref() == &DataType::Utf8 {
                        spark_read_side_padding_internal::<i32>(dict.values(), truncate, rpad_arg)?
                    } else {
                        spark_read_side_padding_internal::<i64>(dict.values(), truncate, rpad_arg)?
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
        other => Err(DataFusionError::Internal(format!(
            "Unsupported arguments {other:?} for function rpad/read_side_padding",
        ))),
    }
}

enum RPadArgument {
    ConstLength(i32),
    ColArray(ArrayRef),
}

fn spark_read_side_padding_internal<T: OffsetSizeTrait>(
    array: &ArrayRef,
    truncate: bool,
    rpad_argument: RPadArgument,
) -> Result<ColumnarValue, DataFusionError> {
    let string_array = as_generic_string_array::<T>(array)?;
    match rpad_argument {
        RPadArgument::ColArray(array_int) => {
            let int_pad_array = array_int.as_primitive::<Int32Type>();
            let mut str_pad_value_map = HashMap::new();
            for i in 0..string_array.len() {
                if string_array.is_null(i) || int_pad_array.is_null(i) {
                    continue; // skip nulls
                }
                str_pad_value_map.insert(string_array.value(i), int_pad_array.value(i));
            }

            let mut builder = GenericStringBuilder::<T>::with_capacity(
                str_pad_value_map.len(),
                str_pad_value_map.len() * int_pad_array.len(),
            );

            for string in string_array.iter() {
                match string {
                    Some(string) => {
                        // It looks Spark's UTF8String is closer to chars rather than graphemes
                        // https://stackoverflow.com/a/46290728
                        let char_len = string.chars().count();
                        let length: usize = 0.max(*str_pad_value_map.get(string).unwrap()) as usize;
                        let space_string = " ".repeat(length);
                        if length <= char_len {
                            if truncate {
                                let idx = string
                                    .char_indices()
                                    .nth(length)
                                    .map(|(i, _)| i)
                                    .unwrap_or(string.len());
                                builder.append_value(&string[..idx]);
                            } else {
                                builder.append_value(string);
                            }
                        } else {
                            // write_str updates only the value buffer, not null nor offset buffer
                            // This is convenient for concatenating str(s)
                            builder.write_str(string)?;
                            builder.append_value(&space_string[char_len..]);
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        RPadArgument::ConstLength(length) => {
            let length = 0.max(length) as usize;
            let space_string = " ".repeat(length);

            let mut builder = GenericStringBuilder::<T>::with_capacity(
                string_array.len(),
                string_array.len() * length,
            );

            for string in string_array.iter() {
                match string {
                    Some(string) => {
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
                                builder.append_value(&string[..idx]);
                            } else {
                                builder.append_value(string);
                            }
                        } else {
                            // write_str updates only the value buffer, not null nor offset buffer
                            // This is convenient for concatenating str(s)
                            builder.write_str(string)?;
                            builder.append_value(&space_string[char_len..]);
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
    }
}
