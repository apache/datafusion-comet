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

//! Fused decimal rescale + overflow check expression.
//!
//! Replaces the pattern `CheckOverflow(Cast(expr, Decimal128(p2,s2)), Decimal128(p2,s2))`
//! with a single expression that rescales and validates precision in one pass.

use arrow::array::{as_primitive_array, Array, ArrayRef, Decimal128Array};
use arrow::datatypes::{DataType, Decimal128Type, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    sync::Arc,
};

/// A fused expression that rescales a Decimal128 value (changing scale) and checks
/// for precision overflow in a single pass. Replaces the two-step
/// `CheckOverflow(Cast(expr, Decimal128(p,s)))` pattern.
#[derive(Debug, Eq)]
pub struct DecimalRescaleCheckOverflow {
    child: Arc<dyn PhysicalExpr>,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    fail_on_error: bool,
}

impl Hash for DecimalRescaleCheckOverflow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.input_scale.hash(state);
        self.output_precision.hash(state);
        self.output_scale.hash(state);
        self.fail_on_error.hash(state);
    }
}

impl PartialEq for DecimalRescaleCheckOverflow {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.input_scale == other.input_scale
            && self.output_precision == other.output_precision
            && self.output_scale == other.output_scale
            && self.fail_on_error == other.fail_on_error
    }
}

impl DecimalRescaleCheckOverflow {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        input_scale: i8,
        output_precision: u8,
        output_scale: i8,
        fail_on_error: bool,
    ) -> Self {
        Self {
            child,
            input_scale,
            output_precision,
            output_scale,
            fail_on_error,
        }
    }
}

impl Display for DecimalRescaleCheckOverflow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DecimalRescaleCheckOverflow [child: {}, input_scale: {}, output: Decimal128({}, {}), fail_on_error: {}]",
            self.child, self.input_scale, self.output_precision, self.output_scale, self.fail_on_error
        )
    }
}

/// Maximum absolute value for a given decimal precision: 10^p - 1.
/// Precision must be <= 38 (max for Decimal128).
#[inline]
fn precision_bound(precision: u8) -> i128 {
    assert!(
        precision <= 38,
        "precision_bound: precision {precision} exceeds maximum 38"
    );
    10i128.pow(precision as u32) - 1
}

/// Rescale a single i128 value by the given delta (output_scale - input_scale)
/// and check precision bounds. Returns `Ok(value)` or `Ok(i128::MAX)` as sentinel
/// for overflow in legacy mode, or `Err` in ANSI mode.
#[inline]
fn rescale_and_check(
    value: i128,
    delta: i8,
    scale_factor: i128,
    bound: i128,
    fail_on_error: bool,
) -> Result<i128, ArrowError> {
    let rescaled = if delta > 0 {
        // Scale up: multiply. Check for overflow.
        match value.checked_mul(scale_factor) {
            Some(v) => v,
            None => {
                if fail_on_error {
                    return Err(ArrowError::ComputeError(
                        "Decimal overflow during rescale".to_string(),
                    ));
                }
                return Ok(i128::MAX); // sentinel
            }
        }
    } else if delta < 0 {
        // Scale down with HALF_UP rounding
        // divisor = 10^(-delta), half = divisor / 2
        let divisor = scale_factor; // already 10^abs(delta)
        let half = divisor / 2;
        let sign = value.signum();
        (value + sign * half) / divisor
    } else {
        value
    };

    // Precision check
    if rescaled.abs() > bound {
        if fail_on_error {
            return Err(ArrowError::ComputeError(
                "Decimal overflow: value does not fit in precision".to_string(),
            ));
        }
        Ok(i128::MAX) // sentinel for null_if_overflow_precision
    } else {
        Ok(rescaled)
    }
}

impl PhysicalExpr for DecimalRescaleCheckOverflow {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
        Ok(DataType::Decimal128(
            self.output_precision,
            self.output_scale,
        ))
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        let delta = self.output_scale - self.input_scale;
        let abs_delta = delta.unsigned_abs();
        // If abs_delta > 38, the scale factor overflows i128. In that case,
        // any non-zero value will overflow the output precision, so we treat
        // it as an immediate overflow condition.
        if abs_delta > 38 {
            return Err(DataFusionError::Execution(format!(
                "DecimalRescaleCheckOverflow: scale delta {delta} exceeds maximum supported range"
            )));
        }
        let scale_factor = 10i128.pow(abs_delta as u32);
        let bound = precision_bound(self.output_precision);
        let fail_on_error = self.fail_on_error;
        let p_out = self.output_precision;
        let s_out = self.output_scale;

        match arg {
            ColumnarValue::Array(array)
                if matches!(array.data_type(), DataType::Decimal128(_, _)) =>
            {
                let decimal_array = as_primitive_array::<Decimal128Type>(&array);

                let result: Decimal128Array =
                    arrow::compute::kernels::arity::try_unary(decimal_array, |value| {
                        rescale_and_check(value, delta, scale_factor, bound, fail_on_error)
                    })?;

                let result = if !fail_on_error {
                    result.null_if_overflow_precision(p_out)
                } else {
                    result
                };

                let result = result
                    .with_precision_and_scale(p_out, s_out)
                    .map(|a| Arc::new(a) as ArrayRef)?;

                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, _precision, _scale)) => {
                let new_v = match v {
                    Some(val) => {
                        let r = rescale_and_check(val, delta, scale_factor, bound, fail_on_error)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        if r == i128::MAX {
                            None
                        } else {
                            Some(r)
                        }
                    }
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    new_v, p_out, s_out,
                )))
            }
            v => Err(DataFusionError::Execution(format!(
                "DecimalRescaleCheckOverflow expects Decimal128, but found {v:?}"
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "DecimalRescaleCheckOverflow expects 1 child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(DecimalRescaleCheckOverflow::new(
            Arc::clone(&children[0]),
            self.input_scale,
            self.output_precision,
            self.output_scale,
            self.fail_on_error,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Decimal128Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;

    fn make_batch(values: Vec<Option<i128>>, precision: u8, scale: i8) -> RecordBatch {
        let arr =
            Decimal128Array::from(values).with_data_type(DataType::Decimal128(precision, scale));
        let schema = Schema::new(vec![Field::new("col", arr.data_type().clone(), true)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arr)]).unwrap()
    }

    fn eval_expr(
        batch: &RecordBatch,
        input_scale: i8,
        output_precision: u8,
        output_scale: i8,
        fail_on_error: bool,
    ) -> datafusion::common::Result<ArrayRef> {
        let child: Arc<dyn PhysicalExpr> = Arc::new(Column::new("col", 0));
        let expr = DecimalRescaleCheckOverflow::new(
            child,
            input_scale,
            output_precision,
            output_scale,
            fail_on_error,
        );
        match expr.evaluate(batch)? {
            ColumnarValue::Array(arr) => Ok(arr),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_scale_up() {
        // Decimal128(10,2) -> Decimal128(10,4): 1.50 (150) -> 1.5000 (15000)
        let batch = make_batch(vec![Some(150), Some(-300)], 10, 2);
        let result = eval_expr(&batch, 2, 10, 4, false).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 15000); // 1.5000
        assert_eq!(arr.value(1), -30000); // -3.0000
    }

    #[test]
    fn test_scale_down_with_half_up_rounding() {
        // Decimal128(10,4) -> Decimal128(10,2)
        // 1.2350 (12350) -> round to 1.24 (124) with HALF_UP
        // 1.2349 (12349) -> round to 1.23 (123) with HALF_UP
        // -1.2350 (-12350) -> round to -1.24 (-124) with HALF_UP
        let batch = make_batch(vec![Some(12350), Some(12349), Some(-12350)], 10, 4);
        let result = eval_expr(&batch, 4, 10, 2, false).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 124); // 1.24
        assert_eq!(arr.value(1), 123); // 1.23
        assert_eq!(arr.value(2), -124); // -1.24
    }

    #[test]
    fn test_same_scale_precision_check_only() {
        // Same scale, just check precision. Value 999 fits in precision 3, 1000 does not.
        let batch = make_batch(vec![Some(999), Some(1000)], 38, 0);
        let result = eval_expr(&batch, 0, 3, 0, false).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 999);
        assert!(arr.is_null(1)); // overflow -> null in legacy mode
    }

    #[test]
    fn test_overflow_null_in_legacy_mode() {
        // Scale up causes overflow: 10^37 * 100 > i128::MAX range for precision 38
        // Use precision 3, value 10 (which is 10 at scale 0), scale up to scale 2 -> 1000, which overflows precision 3
        let batch = make_batch(vec![Some(10)], 38, 0);
        let result = eval_expr(&batch, 0, 3, 2, false).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert!(arr.is_null(0)); // 10 * 100 = 1000 > 999 (max for precision 3)
    }

    #[test]
    fn test_overflow_error_in_ansi_mode() {
        let batch = make_batch(vec![Some(10)], 38, 0);
        let result = eval_expr(&batch, 0, 3, 2, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_null_propagation() {
        let batch = make_batch(vec![Some(100), None, Some(200)], 10, 2);
        let result = eval_expr(&batch, 2, 10, 4, false).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(!arr.is_null(2));
    }

    #[test]
    fn test_scalar_path() {
        let schema = Schema::new(vec![Field::new("col", DataType::Decimal128(10, 2), true)]);
        let batch = RecordBatch::new_empty(Arc::new(schema));

        let scalar_expr = DecimalRescaleCheckOverflow::new(
            Arc::new(ScalarChild(Some(150), 10, 2)),
            2,
            10,
            4,
            false,
        );
        let result = scalar_expr.evaluate(&batch).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, p, s)) => {
                assert_eq!(v, Some(15000));
                assert_eq!(p, 10);
                assert_eq!(s, 4);
            }
            _ => panic!("expected decimal scalar"),
        }
    }

    /// Helper expression that always returns a Decimal128 scalar.
    #[derive(Debug, Eq, PartialEq, Hash)]
    struct ScalarChild(Option<i128>, u8, i8);

    impl Display for ScalarChild {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "ScalarChild({:?})", self.0)
        }
    }

    impl PhysicalExpr for ScalarChild {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
            Ok(DataType::Decimal128(self.1, self.2))
        }
        fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
            Ok(true)
        }
        fn evaluate(&self, _batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                self.0, self.1, self.2,
            )))
        }
        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self, f)
        }
    }

    #[test]
    fn test_scalar_null() {
        let schema = Schema::new(vec![Field::new("col", DataType::Decimal128(10, 2), true)]);
        let batch = RecordBatch::new_empty(Arc::new(schema));
        let expr =
            DecimalRescaleCheckOverflow::new(Arc::new(ScalarChild(None, 10, 2)), 2, 10, 4, false);
        let result = expr.evaluate(&batch).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, _, _)) => {
                assert_eq!(v, None);
            }
            _ => panic!("expected decimal scalar"),
        }
    }

    #[test]
    fn test_scalar_overflow_legacy() {
        let schema = Schema::new(vec![Field::new("col", DataType::Decimal128(38, 0), true)]);
        let batch = RecordBatch::new_empty(Arc::new(schema));
        let expr = DecimalRescaleCheckOverflow::new(
            Arc::new(ScalarChild(Some(10), 38, 0)),
            0,
            3,
            2,
            false,
        );
        let result = expr.evaluate(&batch).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, _, _)) => {
                assert_eq!(v, None); // 10 * 100 = 1000 > 999
            }
            _ => panic!("expected decimal scalar"),
        }
    }

    #[test]
    fn test_scalar_overflow_ansi_returns_error() {
        // fail_on_error=true must propagate the error, not silently return None
        let schema = Schema::new(vec![Field::new("col", DataType::Decimal128(38, 0), true)]);
        let batch = RecordBatch::new_empty(Arc::new(schema));
        let expr = DecimalRescaleCheckOverflow::new(
            Arc::new(ScalarChild(Some(10), 38, 0)),
            0,
            3,
            2,
            true, // fail_on_error = true
        );
        let result = expr.evaluate(&batch);
        assert!(result.is_err()); // must be error, not Ok(None)
    }

    #[test]
    fn test_large_scale_delta_returns_error() {
        // delta = output_scale - input_scale = 38 - (-1) = 39
        // 10i128.pow(39) would overflow, so we must reject gracefully
        let batch = make_batch(vec![Some(1)], 38, -1);
        let result = eval_expr(&batch, -1, 38, 38, false);
        assert!(result.is_err());
    }
}
