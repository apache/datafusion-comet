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

use arrow::array::{Array, ArrowNativeTypeOp, BooleanBufferBuilder, PrimitiveArray};
use arrow::array::{ArrayRef, AsArray};

use crate::{divide_by_zero_error, EvalMode, SparkError};
use arrow::buffer::NullBuffer;
use arrow::compute::kernels::{arity, numeric};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type,
};
use arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;
use array::{Datum, Scalar};
use arrow::array;
use common::ScalarValue;
use datafusion::common;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MathOp {
    Add,
    Sub,
    Mul,
    Div,
}

fn try_arithmetic_kernel<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: MathOp,
) -> Result<ArrayRef, DataFusionError>
where
    T: ArrowPrimitiveType,
{
    match op {
        MathOp::Add => checked_binary(left, right, |l, r| l.add_checked(r)),
        MathOp::Sub => checked_binary(left, right, |l, r| l.sub_checked(r)),
        MathOp::Mul => checked_binary(left, right, |l, r| l.mul_checked(r)),
        MathOp::Div => checked_binary(left, right, |l, r| l.div_checked(r)),
    }
}

fn ansi_arithmetic_kernel(
    left: &ColumnarValue,
    right: &ColumnarValue,
    op: MathOp,
) -> Result<ColumnarValue, DataFusionError> {
    let run_kernel = |l: &dyn Datum, r: &dyn Datum| {
        match op {
            MathOp::Add => numeric::add(l, r),
            MathOp::Sub => numeric::sub(l, r),
            MathOp::Mul => numeric::mul(l, r),
            MathOp::Div => numeric::div(l, r),
        }
    };

    let result_array = match (left, right) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => {
            run_kernel(&l, &r)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            let l_arr = l.to_array()?;
            let l_scalar = Scalar::new(l_arr);
            run_kernel(&l_scalar, &r)
        }
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            let r_arr = r.to_array()?;
            let r_scalar = Scalar::new(r_arr);
            run_kernel(&l, &r_scalar)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => {
            let l_arr = l.to_array()?;
            let r_arr = r.to_array()?;
            let l_scalar = Scalar::new(l_arr);
            let r_scalar = Scalar::new(r_arr);
            let res = run_kernel(&l_scalar, &r_scalar)?;
            let scalar_val = ScalarValue::try_from_array(res.as_ref(), 0)?;
            return Ok(ColumnarValue::Scalar(scalar_val));
        }
    };

    let array = result_array.map_err(|e| match e {
        ArrowError::DivideByZero => divide_by_zero_error().into(),
        _ => DataFusionError::from(SparkError::ArithmeticOverflow {
            from_type: String::from("integer"),
        }),
    })?;

    Ok(ColumnarValue::Array(array))
}

fn ansi_float_div<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<ArrayRef, DataFusionError>
where
    T: ArrowPrimitiveType,
{
    arity::try_binary::<_, _, _, T>(left, right, |l, r| l.div_checked(r))
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|e| match e {
            ArrowError::DivideByZero => divide_by_zero_error().into(),
            _ => DataFusionError::from(SparkError::ArithmeticOverflow {
                from_type: String::from("integer"),
            }),
        })
}

fn checked_binary<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<ArrayRef, DataFusionError>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native, T::Native) -> Result<T::Native, ArrowError>,
{
    let len = left.len();
    let lhs = &left.values()[..len];
    let rhs = &right.values()[..len];
    let nulls = NullBuffer::union(left.nulls(), right.nulls());

    let mut values = vec![T::Native::default(); len];
    let mut overflowed: Vec<usize> = Vec::new();

    for (i, (out, (&l, &r))) in values.iter_mut().zip(lhs.iter().zip(rhs)).enumerate() {
        match op(l, r) {
            Ok(v) => *out = v,
            Err(_) => {
                overflowed.push(i);
            }
        }
    }

    let nulls = if overflowed.is_empty() {
        nulls
    } else {
        let mut validity = BooleanBufferBuilder::new(len);
        match &nulls {
            Some(n) => validity.append_buffer(n.inner()),
            None => validity.append_n(len, true),
        }
        for i in overflowed {
            validity.set_bit(i, false);
        }
        Some(NullBuffer::new(validity.finish()))
    };

    if let Some(n) = &nulls {
        if n.null_count() > 0 {
            for (out, valid) in values.iter_mut().zip(n.iter()) {
                if !valid {
                    *out = T::Native::default();
                }
            }
        }
    }

    Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), nulls)) as ArrayRef)
}

pub fn checked_add(
    args: &[ColumnarValue],
    data_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, MathOp::Add, eval_mode)
}

pub fn checked_sub(
    args: &[ColumnarValue],
    data_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, MathOp::Sub, eval_mode)
}

pub fn checked_mul(
    args: &[ColumnarValue],
    data_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, MathOp::Mul, eval_mode)
}

pub fn checked_div(
    args: &[ColumnarValue],
    data_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    checked_arithmetic_internal(args, data_type, MathOp::Div, eval_mode)
}

fn checked_arithmetic_internal(
    args: &[ColumnarValue],
    data_type: &DataType,
    op: MathOp,
    eval_mode: EvalMode,
) -> Result<ColumnarValue, DataFusionError> {
    let left = &args[0];
    let right = &args[1];

    let is_ansi_mode = match eval_mode {
        EvalMode::Try => false,
        EvalMode::Ansi => true,
        _ => {
            return Err(DataFusionError::Internal(format!(
                "Unsupported mode : {:?}",
                eval_mode
            )))
        }
    };

    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            if is_ansi_mode =>
            {
                ansi_arithmetic_kernel(left, right, op)
            }
        _ => {
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

            let result_array = match data_type {
                DataType::Int8 => try_arithmetic_kernel(
                    left_arr.as_primitive::<Int8Type>(),
                    right_arr.as_primitive::<Int8Type>(),
                    op,
                ),
                DataType::Int16 => try_arithmetic_kernel(
                    left_arr.as_primitive::<Int16Type>(),
                    right_arr.as_primitive::<Int16Type>(),
                    op,
                ),
                DataType::Int32 => try_arithmetic_kernel(
                    left_arr.as_primitive::<Int32Type>(),
                    right_arr.as_primitive::<Int32Type>(),
                    op,
                ),
                DataType::Int64 => try_arithmetic_kernel(
                    left_arr.as_primitive::<Int64Type>(),
                    right_arr.as_primitive::<Int64Type>(),
                    op,
                ),
                DataType::Float16 if op == MathOp::Div => {
                    if is_ansi_mode {
                        ansi_float_div(
                            left_arr.as_primitive::<Float16Type>(),
                            right_arr.as_primitive::<Float16Type>(),
                        )
                    } else {
                        try_arithmetic_kernel(
                            left_arr.as_primitive::<Float16Type>(),
                            right_arr.as_primitive::<Float16Type>(),
                            op,
                        )
                    }
                }
                DataType::Float32 if op == MathOp::Div => {
                    if is_ansi_mode {
                        ansi_float_div(
                            left_arr.as_primitive::<Float32Type>(),
                            right_arr.as_primitive::<Float32Type>(),
                        )
                    } else {
                        try_arithmetic_kernel(
                            left_arr.as_primitive::<Float32Type>(),
                            right_arr.as_primitive::<Float32Type>(),
                            op,
                        )
                    }
                }
                DataType::Float64 if op == MathOp::Div => {
                    if is_ansi_mode {
                        ansi_float_div(
                            left_arr.as_primitive::<Float64Type>(),
                            right_arr.as_primitive::<Float64Type>(),
                        )
                    } else {
                        try_arithmetic_kernel(
                            left_arr.as_primitive::<Float64Type>(),
                            right_arr.as_primitive::<Float64Type>(),
                            op,
                        )
                    }
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported data type: {:?}",
                    data_type
                ))),
            };
            Ok(ColumnarValue::Array(result_array?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array};

    fn int32_args(left: Vec<Option<i32>>, right: Vec<Option<i32>>) -> Vec<ColumnarValue> {
        vec![
            ColumnarValue::Array(Arc::new(Int32Array::from(left))),
            ColumnarValue::Array(Arc::new(Int32Array::from(right))),
        ]
    }

    fn as_int32(value: ColumnarValue) -> Int32Array {
        let ColumnarValue::Array(array) = value else {
            unreachable!()
        };
        array.as_primitive::<Int32Type>().clone()
    }

    #[test]
    fn test_checked_add_propagates_nulls() {
        let args = int32_args(
            vec![Some(1), None, Some(3), None],
            vec![Some(10), Some(20), None, None],
        );
        let result = as_int32(checked_add(&args, &DataType::Int32, EvalMode::Ansi).unwrap());
        assert_eq!(result, Int32Array::from(vec![Some(11), None, None, None]));
    }

    #[test]
    fn test_checked_add_overflow_is_null_in_try_mode() {
        let args = int32_args(vec![Some(i32::MAX), Some(1)], vec![Some(1), Some(1)]);
        let result = as_int32(checked_add(&args, &DataType::Int32, EvalMode::Try).unwrap());
        assert_eq!(result, Int32Array::from(vec![None, Some(2)]));
    }

    #[test]
    fn test_checked_add_overflow_errors_in_ansi_mode() {
        let args = int32_args(vec![Some(i32::MAX)], vec![Some(1)]);
        assert!(checked_add(&args, &DataType::Int32, EvalMode::Ansi).is_err());
    }

    #[test]
    fn test_checked_sub_and_mul_overflow() {
        let args = int32_args(vec![Some(i32::MIN), Some(5)], vec![Some(1), Some(3)]);
        let result = as_int32(checked_sub(&args, &DataType::Int32, EvalMode::Try).unwrap());
        assert_eq!(result, Int32Array::from(vec![None, Some(2)]));

        let args = int32_args(vec![Some(i32::MAX), Some(5)], vec![Some(2), Some(3)]);
        let result = as_int32(checked_mul(&args, &DataType::Int32, EvalMode::Try).unwrap());
        assert_eq!(result, Int32Array::from(vec![None, Some(15)]));
    }

    #[test]
    fn test_checked_div_by_zero() {
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(1.0), Some(8.0)]))),
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(0.0), Some(2.0)]))),
        ];
        let ColumnarValue::Array(array) =
            checked_div(&args, &DataType::Float64, EvalMode::Try).unwrap()
        else {
            unreachable!()
        };
        assert_eq!(
            array.as_primitive::<Float64Type>(),
            &Float64Array::from(vec![None, Some(4.0)])
        );

        assert!(checked_div(&args, &DataType::Float64, EvalMode::Ansi).is_err());
    }

    /// A null slot may hold any value in its underlying buffer (e.g. after a filter or a slice).
    /// Such a row must not overflow-error in ANSI mode, and its result slot must read back as a
    /// null holding the default value.
    #[test]
    fn test_null_row_with_garbage_value_does_not_error_in_ansi_mode() {
        let nulls = NullBuffer::from(vec![false, true]);
        let left = Int32Array::new(vec![i32::MAX, 1].into(), Some(nulls));
        let right = Int32Array::from(vec![Some(1), Some(1)]);
        let args = vec![
            ColumnarValue::Array(Arc::new(left)),
            ColumnarValue::Array(Arc::new(right)),
        ];

        let result = as_int32(checked_add(&args, &DataType::Int32, EvalMode::Ansi).unwrap());
        assert_eq!(result, Int32Array::from(vec![None, Some(2)]));
        assert_eq!(result.values()[0], 0);
    }

    #[test]
    fn test_ansi_integer_div_by_zero() {
        let args = int32_args(vec![Some(10)], vec![Some(0)]);
        let result = checked_div(&args, &DataType::Int32, EvalMode::Ansi);
        assert!(result.is_err());
    }

    #[test]
    fn test_ansi_scalar_operand() {
        let args = vec![
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(10), Some(20)]))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(5))),
        ];
        let result = as_int32(checked_add(&args, &DataType::Int32, EvalMode::Ansi).unwrap());
        assert_eq!(result, Int32Array::from(vec![Some(15), Some(25)]));
    }

    #[test]
    fn test_ansi_int64_overflow() {
        let args = vec![
            ColumnarValue::Array(Arc::new(array::Int64Array::from(vec![Some(
                i64::MAX,
            )]))),
            ColumnarValue::Array(Arc::new(array::Int64Array::from(vec![Some(1)]))),
        ];
        let result = checked_add(&args, &DataType::Int64, EvalMode::Ansi);
        assert!(result.is_err());
    }
}
