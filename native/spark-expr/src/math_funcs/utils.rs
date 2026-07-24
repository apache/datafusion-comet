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

use arrow::array::cast::AsArray;
use arrow::array::types::Decimal128Type;
use arrow::array::{ArrayRef, Decimal128Array};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::physical_plan::ColumnarValue;
use std::sync::Arc;

#[macro_export]
macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $RESULT =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC() as i64);
                Ok(Arc::new(res))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for {}",
                $NAME
            ))),
        }
    }};
}

/// Evaluates `$body` with `$exp` bound as a compile-time constant equal to `$scale`, for every
/// decimal scale whose divisor `10^scale` fits in an `i64`. Larger scales evaluate `$fallback`,
/// which must derive the divisor at runtime.
///
/// Specializing on the scale lets the divisor be folded into the generated code, turning the
/// division by `10^scale` into a multiply-and-shift.
macro_rules! dispatch_pow10 {
    ($scale:expr, $exp:ident => $body:expr, $fallback:expr) => {
        match $scale {
            1 => {
                const $exp: u32 = 1;
                $body
            }
            2 => {
                const $exp: u32 = 2;
                $body
            }
            3 => {
                const $exp: u32 = 3;
                $body
            }
            4 => {
                const $exp: u32 = 4;
                $body
            }
            5 => {
                const $exp: u32 = 5;
                $body
            }
            6 => {
                const $exp: u32 = 6;
                $body
            }
            7 => {
                const $exp: u32 = 7;
                $body
            }
            8 => {
                const $exp: u32 = 8;
                $body
            }
            9 => {
                const $exp: u32 = 9;
                $body
            }
            10 => {
                const $exp: u32 = 10;
                $body
            }
            11 => {
                const $exp: u32 = 11;
                $body
            }
            12 => {
                const $exp: u32 = 12;
                $body
            }
            13 => {
                const $exp: u32 = 13;
                $body
            }
            14 => {
                const $exp: u32 = 14;
                $body
            }
            15 => {
                const $exp: u32 = 15;
                $body
            }
            16 => {
                const $exp: u32 = 16;
                $body
            }
            17 => {
                const $exp: u32 = 17;
                $body
            }
            18 => {
                const $exp: u32 = 18;
                $body
            }
            _ => $fallback,
        }
    };
}

pub(crate) use dispatch_pow10;

#[inline]
pub(crate) fn make_decimal_scalar(
    a: &Option<i128>,
    precision: u8,
    scale: i8,
    f: impl Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let result = ScalarValue::Decimal128(a.map(f), precision, scale);
    Ok(ColumnarValue::Scalar(result))
}

/// Generic over the operation so that it is monomorphized into the element loop rather than
/// called through a vtable for every value.
#[inline]
pub(crate) fn make_decimal_array(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
    f: impl Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let array = array.as_primitive::<Decimal128Type>();
    let result: Decimal128Array = arrow::compute::kernels::arity::unary(array, f);
    let result = result.with_data_type(DataType::Decimal128(precision, scale));
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[inline]
pub(crate) fn get_precision_scale(data_type: &DataType) -> (u8, i8) {
    let DataType::Decimal128(precision, scale) = data_type else {
        unreachable!()
    };
    (*precision, *scale)
}
