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
use datafusion::common::Result;
use datafusion::{error::DataFusionError, logical_expr::ColumnarValue};
use std::sync::Arc;

macro_rules! compute_op {
    ($OPERAND:expr, $DT:ident) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");

        let result: Int32Array = operand
            .iter()
            .map(|x| x.map(|y| bit_count(y.into())))
            .collect();

        Ok(Arc::new(result))
    }};
}

pub fn spark_bit_count(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return Err(DataFusionError::Internal(
            "bit_count expects exactly one argument".to_string(),
        ));
    }
    match &args[0] {
        ColumnarValue::Array(array) => {
            let result: Result<ArrayRef> = match array.data_type() {
                DataType::Int8 | DataType::Boolean => compute_op!(array, Int8Array),
                DataType::Int16 => compute_op!(array, Int16Array),
                DataType::Int32 => compute_op!(array, Int32Array),
                DataType::Int64 => compute_op!(array, Int64Array),
                _ => Err(DataFusionError::Execution(format!(
                    "Can't be evaluated because the expression's type is {:?}, not signed int",
                    array.data_type(),
                ))),
            };
            result.map(ColumnarValue::Array)
        }
        ColumnarValue::Scalar(_) => Err(DataFusionError::Internal(
            "shouldn't go to bit_count scalar path".to_string(),
        )),
    }
}

// Here’s the equivalent Rust implementation of the bitCount function (similar to Apache Spark's bitCount for LongType)
fn bit_count(i: i64) -> i32 {
    let mut u = i as u64;
    u = u - ((u >> 1) & 0x5555555555555555);
    u = (u & 0x3333333333333333) + ((u >> 2) & 0x3333333333333333);
    u = (u + (u >> 4)) & 0x0f0f0f0f0f0f0f0f;
    u = u + (u >> 8);
    u = u + (u >> 16);
    u = u + (u >> 32);
    (u as i32) & 0x7f
}

#[cfg(test)]
mod tests {
    use datafusion::common::{cast::as_int32_array, Result};

    use super::*;

    #[test]
    fn bitwise_count_op() -> Result<()> {
        let args = vec![ColumnarValue::Array(Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(12345),
            Some(89),
            Some(-3456),
        ])))];
        let expected = &Int32Array::from(vec![Some(1), None, Some(6), Some(4), Some(54)]);

        let ColumnarValue::Array(result) = spark_bit_count(&args)? else {
            unreachable!()
        };

        let result = as_int32_array(&result).expect("failed to downcast to In32Array");
        assert_eq!(result, expected);

        Ok(())
    }
}
