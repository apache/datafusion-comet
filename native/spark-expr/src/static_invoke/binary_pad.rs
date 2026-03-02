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

use arrow::array::builder::BinaryBuilder;
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

/// Spark's ByteArray.lpad: left-pad binary array with cyclic pattern.
pub fn spark_binary_lpad(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    binary_pad_impl(args, true)
}

/// Spark's ByteArray.rpad: right-pad binary array with cyclic pattern.
pub fn spark_binary_rpad(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    binary_pad_impl(args, false)
}

fn binary_pad_impl(
    args: &[ColumnarValue],
    is_left_pad: bool,
) -> Result<ColumnarValue, DataFusionError> {
    match args {
        [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(Some(len))), ColumnarValue::Scalar(ScalarValue::Binary(Some(pad)))] =>
        {
            let len = *len;
            match array.data_type() {
                DataType::Binary => {
                    let binary_array = array.as_binary::<i32>();
                    let mut builder = BinaryBuilder::with_capacity(binary_array.len(), 0);

                    for i in 0..binary_array.len() {
                        if binary_array.is_null(i) {
                            builder.append_null();
                        } else {
                            let bytes = binary_array.value(i);
                            let result = pad_bytes(bytes, len as usize, pad, is_left_pad);
                            builder.append_value(&result);
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for binary_pad",
                ))),
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported arguments {other:?} for binary_pad",
        ))),
    }
}

/// Pad bytes to target length using cyclic pad pattern.
/// Matches Spark's ByteArray.lpad/rpad behavior.
fn pad_bytes(bytes: &[u8], len: usize, pad: &[u8], is_left_pad: bool) -> Vec<u8> {
    if len == 0 {
        return Vec::new();
    }

    if pad.is_empty() {
        // Empty pattern: return first `len` bytes or copy of input
        let take = bytes.len().min(len);
        return bytes[..take].to_vec();
    }

    let mut result = vec![0u8; len];
    let min_len = bytes.len().min(len);

    if is_left_pad {
        // Copy input bytes to the right side of result
        result[len - min_len..].copy_from_slice(&bytes[..min_len]);
        // Fill remaining left side with pad pattern
        if bytes.len() < len {
            fill_with_pattern(&mut result, 0, len - bytes.len(), pad);
        }
    } else {
        // Copy input bytes to the left side of result
        result[..min_len].copy_from_slice(&bytes[..min_len]);
        // Fill remaining right side with pad pattern
        if bytes.len() < len {
            fill_with_pattern(&mut result, bytes.len(), len, pad);
        }
    }

    result
}

/// Fill result[first_pos..beyond_pos] with cyclic pad pattern.
fn fill_with_pattern(result: &mut [u8], first_pos: usize, beyond_pos: usize, pad: &[u8]) {
    let mut pos = first_pos;
    while pos < beyond_pos {
        let remaining = beyond_pos - pos;
        let take = pad.len().min(remaining);
        result[pos..pos + take].copy_from_slice(&pad[..take]);
        pos += take;
    }
}
