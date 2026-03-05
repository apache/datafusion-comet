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

//! Fused wide-decimal binary expression for Decimal128 add/sub/mul that may overflow.
//!
//! Instead of building a 4-node expression tree (Cast→BinaryExpr→Cast→Cast), this performs
//! i256 intermediate arithmetic in a single expression, producing only one output array.

use crate::math_funcs::utils::get_precision_scale;
use crate::EvalMode;
use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array};
use arrow::datatypes::{i256, DataType, Decimal128Type, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::{any::Any, sync::Arc};

/// The arithmetic operation to perform.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum WideDecimalOp {
    Add,
    Subtract,
    Multiply,
}

impl Display for WideDecimalOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WideDecimalOp::Add => write!(f, "+"),
            WideDecimalOp::Subtract => write!(f, "-"),
            WideDecimalOp::Multiply => write!(f, "*"),
        }
    }
}

/// A fused expression that evaluates Decimal128 add/sub/mul using i256 intermediate arithmetic,
/// applies scale adjustment with HALF_UP rounding, checks precision bounds, and outputs
/// a single Decimal128 array.
#[derive(Debug, Eq)]
pub struct WideDecimalBinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    op: WideDecimalOp,
    output_precision: u8,
    output_scale: i8,
    eval_mode: EvalMode,
}

impl Hash for WideDecimalBinaryExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.right.hash(state);
        self.op.hash(state);
        self.output_precision.hash(state);
        self.output_scale.hash(state);
        self.eval_mode.hash(state);
    }
}

impl PartialEq for WideDecimalBinaryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left)
            && self.right.eq(&other.right)
            && self.op == other.op
            && self.output_precision == other.output_precision
            && self.output_scale == other.output_scale
            && self.eval_mode == other.eval_mode
    }
}

impl WideDecimalBinaryExpr {
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        op: WideDecimalOp,
        output_precision: u8,
        output_scale: i8,
        eval_mode: EvalMode,
    ) -> Self {
        Self {
            left,
            right,
            op,
            output_precision,
            output_scale,
            eval_mode,
        }
    }
}

impl Display for WideDecimalBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WideDecimalBinaryExpr [{} {} {}, output: Decimal128({}, {})]",
            self.left, self.op, self.right, self.output_precision, self.output_scale
        )
    }
}

/// Compute `value / divisor` with HALF_UP rounding.
#[inline]
fn div_round_half_up(value: i256, divisor: i256) -> i256 {
    let (quot, rem) = (value / divisor, value % divisor);
    // HALF_UP: if |remainder| * 2 >= |divisor|, round away from zero
    let abs_rem_x2 = if rem < i256::ZERO {
        rem.wrapping_neg()
    } else {
        rem
    }
    .wrapping_mul(i256::from_i128(2));
    let abs_divisor = if divisor < i256::ZERO {
        divisor.wrapping_neg()
    } else {
        divisor
    };
    if abs_rem_x2 >= abs_divisor {
        if (value < i256::ZERO) != (divisor < i256::ZERO) {
            quot.wrapping_sub(i256::ONE)
        } else {
            quot.wrapping_add(i256::ONE)
        }
    } else {
        quot
    }
}

/// i256 constant for 10.
const I256_TEN: i256 = i256::from_i128(10);

/// Compute 10^exp as i256. Panics if exp > 76 (max representable power of 10 in i256).
#[inline]
fn i256_pow10(exp: u32) -> i256 {
    assert!(exp <= 76, "i256_pow10: exponent {exp} exceeds maximum 76");
    let mut result = i256::ONE;
    for _ in 0..exp {
        result = result.wrapping_mul(I256_TEN);
    }
    result
}

/// Maximum i128 value for a given decimal precision (1-indexed).
/// precision p allows values in [-10^p + 1, 10^p - 1].
#[inline]
fn max_for_precision(precision: u8) -> i256 {
    i256_pow10(precision as u32).wrapping_sub(i256::ONE)
}

impl PhysicalExpr for WideDecimalBinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Decimal128(
            self.output_precision,
            self.output_scale,
        ))
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let left_val = self.left.evaluate(batch)?;
        let right_val = self.right.evaluate(batch)?;

        let (left_arr, right_arr): (ArrayRef, ArrayRef) = match (&left_val, &right_val) {
            (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (Arc::clone(l), Arc::clone(r)),
            (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
                (l.to_array_of_size(r.len())?, Arc::clone(r))
            }
            (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
                (Arc::clone(l), r.to_array_of_size(l.len())?)
            }
            (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => (l.to_array()?, r.to_array()?),
        };

        let left = left_arr.as_primitive::<Decimal128Type>();
        let right = right_arr.as_primitive::<Decimal128Type>();
        let (_p1, s1) = get_precision_scale(left.data_type());
        let (_p2, s2) = get_precision_scale(right.data_type());

        let p_out = self.output_precision;
        let s_out = self.output_scale;
        let op = self.op;
        let eval_mode = self.eval_mode;

        let bound = max_for_precision(p_out);
        let neg_bound = i256::ZERO.wrapping_sub(bound);

        let result: Decimal128Array = match op {
            WideDecimalOp::Add | WideDecimalOp::Subtract => {
                let max_scale = std::cmp::max(s1, s2);
                let l_scale_up = i256_pow10((max_scale - s1) as u32);
                let r_scale_up = i256_pow10((max_scale - s2) as u32);
                // After add/sub at max_scale, we may need to rescale to s_out
                let scale_diff = max_scale as i16 - s_out as i16;
                let (need_scale_down, need_scale_up) = (scale_diff > 0, scale_diff < 0);
                let rescale_divisor = if need_scale_down {
                    i256_pow10(scale_diff as u32)
                } else {
                    i256::ONE
                };
                let scale_up_factor = if need_scale_up {
                    i256_pow10((-scale_diff) as u32)
                } else {
                    i256::ONE
                };

                arrow::compute::kernels::arity::try_binary(left, right, |l, r| {
                    let l256 = i256::from_i128(l).wrapping_mul(l_scale_up);
                    let r256 = i256::from_i128(r).wrapping_mul(r_scale_up);
                    let raw = match op {
                        WideDecimalOp::Add => l256.wrapping_add(r256),
                        WideDecimalOp::Subtract => l256.wrapping_sub(r256),
                        _ => unreachable!(),
                    };
                    let result = if need_scale_down {
                        div_round_half_up(raw, rescale_divisor)
                    } else if need_scale_up {
                        raw.wrapping_mul(scale_up_factor)
                    } else {
                        raw
                    };
                    check_overflow_and_convert(result, bound, neg_bound, eval_mode)
                })?
            }
            WideDecimalOp::Multiply => {
                let natural_scale = s1 + s2;
                let need_rescale = s_out < natural_scale;
                let rescale_divisor = if need_rescale {
                    i256_pow10((natural_scale - s_out) as u32)
                } else {
                    i256::ONE
                };

                arrow::compute::kernels::arity::try_binary(left, right, |l, r| {
                    let raw = i256::from_i128(l).wrapping_mul(i256::from_i128(r));
                    let result = if need_rescale {
                        div_round_half_up(raw, rescale_divisor)
                    } else {
                        raw
                    };
                    check_overflow_and_convert(result, bound, neg_bound, eval_mode)
                })?
            }
        };

        let result = if eval_mode != EvalMode::Ansi {
            result.null_if_overflow_precision(p_out)
        } else {
            result
        };
        let result = result.with_data_type(DataType::Decimal128(p_out, s_out));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 2 {
            return Err(datafusion::common::DataFusionError::Internal(format!(
                "WideDecimalBinaryExpr expects 2 children, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(WideDecimalBinaryExpr::new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.op,
            self.output_precision,
            self.output_scale,
            self.eval_mode,
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

/// Check if the i256 result fits in the output precision. In Ansi mode, return an error
/// on overflow. In Legacy/Try mode, return i128::MAX as a sentinel value that will be
/// nullified by `null_if_overflow_precision`.
#[inline]
fn check_overflow_and_convert(
    result: i256,
    bound: i256,
    neg_bound: i256,
    eval_mode: EvalMode,
) -> Result<i128, ArrowError> {
    if result > bound || result < neg_bound {
        if eval_mode == EvalMode::Ansi {
            return Err(ArrowError::ComputeError("Arithmetic overflow".to_string()));
        }
        // Sentinel value — will be nullified by null_if_overflow_precision
        Ok(i128::MAX)
    } else {
        Ok(result.to_i128().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Decimal128Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;

    fn make_batch(
        left_values: Vec<Option<i128>>,
        left_precision: u8,
        left_scale: i8,
        right_values: Vec<Option<i128>>,
        right_precision: u8,
        right_scale: i8,
    ) -> RecordBatch {
        let left_arr = Decimal128Array::from(left_values)
            .with_data_type(DataType::Decimal128(left_precision, left_scale));
        let right_arr = Decimal128Array::from(right_values)
            .with_data_type(DataType::Decimal128(right_precision, right_scale));
        let schema = Schema::new(vec![
            Field::new("left", left_arr.data_type().clone(), true),
            Field::new("right", right_arr.data_type().clone(), true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(left_arr), Arc::new(right_arr)],
        )
        .unwrap()
    }

    fn eval_expr(
        batch: &RecordBatch,
        op: WideDecimalOp,
        output_precision: u8,
        output_scale: i8,
        eval_mode: EvalMode,
    ) -> Result<ArrayRef> {
        let left: Arc<dyn PhysicalExpr> = Arc::new(Column::new("left", 0));
        let right: Arc<dyn PhysicalExpr> = Arc::new(Column::new("right", 1));
        let expr =
            WideDecimalBinaryExpr::new(left, right, op, output_precision, output_scale, eval_mode);
        match expr.evaluate(batch)? {
            ColumnarValue::Array(arr) => Ok(arr),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_add_same_scale() {
        // Decimal128(38, 10) + Decimal128(38, 10) -> Decimal128(38, 10)
        let batch = make_batch(
            vec![Some(1000000000), Some(2500000000)], // 0.1, 0.25 (scale 10 → divide by 10^10 mentally)
            38,
            10,
            vec![Some(2000000000), Some(7500000000)],
            38,
            10,
        );
        let result = eval_expr(&batch, WideDecimalOp::Add, 38, 10, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 3000000000); // 0.1 + 0.2
        assert_eq!(arr.value(1), 10000000000); // 0.25 + 0.75
    }

    #[test]
    fn test_subtract_same_scale() {
        let batch = make_batch(
            vec![Some(5000), Some(1000)],
            38,
            2,
            vec![Some(3000), Some(2000)],
            38,
            2,
        );
        let result = eval_expr(&batch, WideDecimalOp::Subtract, 38, 2, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 2000); // 50.00 - 30.00
        assert_eq!(arr.value(1), -1000); // 10.00 - 20.00
    }

    #[test]
    fn test_add_different_scales() {
        // Decimal128(10, 2) + Decimal128(10, 4) -> output scale 4
        let batch = make_batch(
            vec![Some(150)], // 1.50
            10,
            2,
            vec![Some(2500)], // 0.2500
            10,
            4,
        );
        let result = eval_expr(&batch, WideDecimalOp::Add, 38, 4, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 17500); // 1.5000 + 0.2500 = 1.7500
    }

    #[test]
    fn test_multiply_with_scale_reduction() {
        // Decimal128(20, 5) * Decimal128(20, 5) -> natural scale 10, output scale 6
        // 1.00000 * 2.00000 = 2.000000
        let batch = make_batch(
            vec![Some(100000)], // 1.00000
            20,
            5,
            vec![Some(200000)], // 2.00000
            20,
            5,
        );
        let result = eval_expr(&batch, WideDecimalOp::Multiply, 38, 6, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 2000000); // 2.000000
    }

    #[test]
    fn test_multiply_half_up_rounding() {
        // Test HALF_UP rounding: 1.5 * 1.5 = 2.25, but if output scale=1, should round to 2.3
        // Input: scale 1, values 15 (1.5) * 15 (1.5) = natural scale 2, raw = 225
        // Output scale 1: 225 / 10 = 22 remainder 5 -> HALF_UP rounds to 23
        let batch = make_batch(
            vec![Some(15)], // 1.5
            10,
            1,
            vec![Some(15)], // 1.5
            10,
            1,
        );
        let result = eval_expr(&batch, WideDecimalOp::Multiply, 38, 1, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 23); // 2.3
    }

    #[test]
    fn test_multiply_half_up_rounding_negative() {
        // -1.5 * 1.5 = -2.25, output scale 1: -225/10 => -22 rem -5 -> HALF_UP rounds to -23
        let batch = make_batch(
            vec![Some(-15)], // -1.5
            10,
            1,
            vec![Some(15)], // 1.5
            10,
            1,
        );
        let result = eval_expr(&batch, WideDecimalOp::Multiply, 38, 1, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), -23); // -2.3
    }

    #[test]
    fn test_overflow_legacy_mode_returns_null() {
        // Use precision 1 (max value 9), so 5 + 5 = 10 overflows
        let batch = make_batch(vec![Some(5)], 38, 0, vec![Some(5)], 38, 0);
        let result = eval_expr(&batch, WideDecimalOp::Add, 1, 0, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert!(arr.is_null(0));
    }

    #[test]
    fn test_overflow_ansi_mode_returns_error() {
        let batch = make_batch(vec![Some(5)], 38, 0, vec![Some(5)], 38, 0);
        let result = eval_expr(&batch, WideDecimalOp::Add, 1, 0, EvalMode::Ansi);
        assert!(result.is_err());
    }

    #[test]
    fn test_null_propagation() {
        let batch = make_batch(vec![Some(100), None], 10, 2, vec![None, Some(200)], 10, 2);
        let result = eval_expr(&batch, WideDecimalOp::Add, 38, 2, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
    }

    #[test]
    fn test_zeros() {
        let batch = make_batch(vec![Some(0)], 38, 10, vec![Some(0)], 38, 10);
        let result = eval_expr(&batch, WideDecimalOp::Multiply, 38, 10, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 0);
    }

    #[test]
    fn test_max_precision_values() {
        // Max Decimal128(38,0) value: 10^38 - 1
        let max_val = 10i128.pow(38) - 1;
        let batch = make_batch(vec![Some(max_val)], 38, 0, vec![Some(0)], 38, 0);
        let result = eval_expr(&batch, WideDecimalOp::Add, 38, 0, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), max_val);
    }

    #[test]
    fn test_add_scale_up_to_output() {
        // When s_out > max(s1, s2), result must be scaled UP
        // Decimal128(10, 2) + Decimal128(10, 2) with output scale 4
        // 1.50 + 0.25 = 1.75, at scale 4 = 17500
        let batch = make_batch(
            vec![Some(150)], // 1.50
            10,
            2,
            vec![Some(25)], // 0.25
            10,
            2,
        );
        let result = eval_expr(&batch, WideDecimalOp::Add, 38, 4, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 17500); // 1.7500
    }

    #[test]
    fn test_subtract_scale_up_to_output() {
        // s_out (4) > max(s1, s2) (2) — verify scale-up path for subtract
        let batch = make_batch(
            vec![Some(300)], // 3.00
            10,
            2,
            vec![Some(100)], // 1.00
            10,
            2,
        );
        let result =
            eval_expr(&batch, WideDecimalOp::Subtract, 38, 4, EvalMode::Legacy).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 20000); // 2.0000
    }
}
