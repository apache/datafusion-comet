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
use arrow::array::{make_array, Array, DictionaryArray};
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion::common::{cast::as_generic_string_array, DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark's charTypeWriteSideCheck: pad if shorter, trim trailing spaces if longer.
/// Throws if string exceeds limit after trimming.
pub fn spark_char_type_write_side_check(
    args: &[ColumnarValue],
) -> Result<ColumnarValue, DataFusionError> {
    write_side_check_impl(args, true)
}

/// Spark's varcharTypeWriteSideCheck: return as-is if within limit, trim trailing spaces if longer.
/// Throws if string exceeds limit after trimming.
pub fn spark_varchar_type_write_side_check(
    args: &[ColumnarValue],
) -> Result<ColumnarValue, DataFusionError> {
    write_side_check_impl(args, false)
}

fn write_side_check_impl(
    args: &[ColumnarValue],
    pad_if_shorter: bool,
) -> Result<ColumnarValue, DataFusionError> {
    match args {
        [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(Some(limit)))] => {
            let limit = *limit as usize;
            match array.data_type() {
                DataType::Utf8 => {
                    write_side_check_internal::<i32>(array, limit, pad_if_shorter)
                }
                DataType::LargeUtf8 => {
                    write_side_check_internal::<i64>(array, limit, pad_if_shorter)
                }
                DataType::Dictionary(_, value_type) => {
                    let dict = as_dictionary_array::<Int32Type>(array);
                    let col = if value_type.as_ref() == &DataType::Utf8 {
                        write_side_check_internal::<i32>(dict.values(), limit, pad_if_shorter)?
                    } else {
                        write_side_check_internal::<i64>(dict.values(), limit, pad_if_shorter)?
                    };
                    let values = col.to_array(0)?;
                    let result = DictionaryArray::try_new(dict.keys().clone(), values)?;
                    Ok(ColumnarValue::Array(make_array(result.into())))
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for write_side_check",
                ))),
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported arguments {other:?} for write_side_check",
        ))),
    }
}

fn write_side_check_internal<T: OffsetSizeTrait>(
    array: &ArrayRef,
    limit: usize,
    pad_if_shorter: bool,
) -> Result<ColumnarValue, DataFusionError> {
    let string_array = as_generic_string_array::<T>(array)?;

    let mut builder =
        GenericStringBuilder::<T>::with_capacity(string_array.len(), string_array.len() * limit);
    let mut buffer = String::with_capacity(limit);

    for string in string_array.iter() {
        match string {
            Some(s) => {
                let char_len = s.chars().count();
                if char_len <= limit {
                    if pad_if_shorter && char_len < limit {
                        // Pad with spaces to reach limit
                        buffer.clear();
                        buffer.push_str(s);
                        for _ in 0..(limit - char_len) {
                            buffer.push(' ');
                        }
                        builder.append_value(&buffer);
                    } else {
                        builder.append_value(s);
                    }
                } else {
                    // Trim trailing spaces
                    let trimmed = s.trim_end_matches(' ');
                    let trimmed_char_len = trimmed.chars().count();
                    if trimmed_char_len > limit {
                        return Err(DataFusionError::Execution(format!(
                            "Exceeds char/varchar type length limitation: {limit}"
                        )));
                    }
                    if pad_if_shorter && trimmed_char_len < limit {
                        // For CHAR type: pad back to limit after trimming
                        buffer.clear();
                        buffer.push_str(trimmed);
                        for _ in 0..(limit - trimmed_char_len) {
                            buffer.push(' ');
                        }
                        builder.append_value(&buffer);
                    } else {
                        builder.append_value(trimmed);
                    }
                }
            }
            None => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}
