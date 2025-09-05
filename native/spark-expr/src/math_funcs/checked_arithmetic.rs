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

use arrow::array::{Array, ArrowNativeTypeOp, PrimitiveArray, PrimitiveBuilder};
use arrow::array::{ArrayRef, AsArray};

use arrow::datatypes::{ArrowPrimitiveType, DataType, Int32Type, Int64Type};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

pub fn try_arithmetic_kernel<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: &str,
) -> Result<ArrayRef, DataFusionError>
where
    T: ArrowPrimitiveType,
{
    let len = left.len();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(len);
    match op {
        "checked_add" => {
            for i in 0..len {
                if left.is_null(i) || right.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_option(left.value(i).add_checked(right.value(i)).ok());
                }
            }
        }
        "checked_sub" => {
            for i in 0..len {
                if left.is_null(i) || right.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_option(left.value(i).sub_checked(right.value(i)).ok());
                }
            }
        }
        "checked_mul" => {
            for i in 0..len {
                if left.is_null(i) || right.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_option(left.value(i).mul_checked(right.value(i)).ok());
                }
            }
        }
        "checked_div" => {
            for i in 0..len {
                if left.is_null(i) || right.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_option(left.value(i).div_checked(right.value(i)).ok());
                }
            }
        }
        _ => {
            return Err(DataFusionError::Internal(format!(
                "Unsupported operation: {op:?}"
            )))
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn checked_add(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, "checked_add")
}

pub fn checked_sub(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, "checked_sub")
}

pub fn checked_mul(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, "checked_mul")
}

pub fn checked_div(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, "checked_div")
}

fn checked_arithmetic_internal(
    args: &[ColumnarValue],
    data_type: &DataType,
    op: &str,
) -> Result<ColumnarValue, DataFusionError> {
    let left = &args[0];
    let right = &args[1];

    let (left_arr, right_arr): (ArrayRef, ArrayRef) = match (left, right) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (Arc::clone(l), Arc::clone(r)),
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            (l.to_array_of_size(r.len())?, Arc::clone(r))
        }
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            (Arc::clone(l), r.to_array_of_size(l.len())?)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => (l.to_array()?, r.to_array()?),
    };

    // Rust only supports checked_arithmetic on Int32 and Int64
    let result_array = match data_type {
        DataType::Int32 => try_arithmetic_kernel::<Int32Type>(
            left_arr.as_primitive::<Int32Type>(),
            right_arr.as_primitive::<Int32Type>(),
            op,
        ),
        DataType::Int64 => try_arithmetic_kernel::<Int64Type>(
            left_arr.as_primitive::<Int64Type>(),
            right_arr.as_primitive::<Int64Type>(),
            op,
        ),
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported data type: {data_type:?}"
        ))),
    };

    Ok(ColumnarValue::Array(result_array?))
}
