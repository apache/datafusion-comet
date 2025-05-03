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

use arrow::{array::*, datatypes::DataType};
use datafusion::common::{Result, ScalarValue};
use datafusion::{error::DataFusionError, logical_expr::ColumnarValue};
use std::sync::Arc;

macro_rules! bit_get_scalar_position {
    ($args:expr, $array_type:ty, $pos:expr, $bit_size:expr) => {{
        let args = $args
            .as_any()
            .downcast_ref::<$array_type>()
            .expect("bit_get_scalar_position failed to downcast array");

        let result: Int8Array = args
            .iter()
            .map(|x| x.map(|x| bit_get(x.into(), $pos)))
            .collect();

        Ok(Arc::new(result))
    }};
}

macro_rules! bit_get_array_positions {
    ($args:expr, $array_type:ty, $positions:expr, $bit_size:expr) => {{
        let args = $args
            .as_any()
            .downcast_ref::<$array_type>()
            .expect("bit_get_array_positions failed to downcast args array");

        let positions = $positions
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("bit_get_array_positions failed to downcast positions array");

        let result: Int8Array = args
            .iter()
            .zip(positions.iter())
            .map(|(i, p)| i.and_then(|i| p.map(|p| bit_get(i.into(), p))))
            .collect();

        Ok(Arc::new(result))
    }};
}

pub fn spark_bit_get(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(
            "bit_get expects exactly two arguments".to_string(),
        ));
    }
    match (&args[0], &args[1]) {
        (ColumnarValue::Array(args), ColumnarValue::Array(positions)) => {
            if args.len() != positions.len() {
                return Err(DataFusionError::Execution(format!(
                    "Input arrays must have equal length. Positions array has {} elements, but arguments array has {} elements",
                    positions.len(), args.len()
                )));
            }
            if !matches!(positions.data_type(), DataType::Int32) {
                return Err(DataFusionError::Execution(format!(
                    "Invalid data type for positions array: expected `Int32`, found `{}`",
                    positions.data_type()
                )));
            }
            let result: Result<ArrayRef> = match args.data_type() {
                DataType::Int8 => bit_get_array_positions!(args, Int8Array, positions, i8::BITS),
                DataType::Int16 => bit_get_array_positions!(args, Int16Array, positions, i16::BITS),
                DataType::Int32 => bit_get_array_positions!(args, Int32Array, positions, i32::BITS),
                DataType::Int64 => bit_get_array_positions!(args, Int64Array, positions, i64::BITS),
                _ => Err(DataFusionError::Execution(format!(
                    "Can't be evaluated because the expression's type is {:?}, not signed int",
                    args.data_type(),
                ))),
            };
            result.map(ColumnarValue::Array)
        }
        (ColumnarValue::Array(args), ColumnarValue::Scalar(ScalarValue::Int32(pos))) => {
            let result: Result<ArrayRef> = match args.data_type() {
                DataType::Int8 => {
                    bit_get_scalar_position!(args, Int8Array, pos.unwrap(), i8::BITS)
                }
                DataType::Int16 => {
                    bit_get_scalar_position!(args, Int16Array, pos.unwrap(), i16::BITS)
                }
                DataType::Int32 => {
                    bit_get_scalar_position!(args, Int32Array, pos.unwrap(), i32::BITS)
                }
                DataType::Int64 => {
                    bit_get_scalar_position!(args, Int64Array, pos.unwrap(), i64::BITS)
                }
                _ => Err(DataFusionError::Execution(format!(
                    "Can't be evaluated because the expression's type is {:?}, not signed int",
                    args.data_type(),
                ))),
            };
            result.map(ColumnarValue::Array)
        }
        _ => Err(DataFusionError::Execution(
            "Invalid input to function bit_get. Expected (IntegralType array, Int32Scalar) or \
                    (IntegralType array, Int32Array)"
                .to_string(),
        )),
    }
}

fn bit_get(arg: i64, pos: i32) -> i8 {
    ((arg >> pos) & 1) as i8
}
