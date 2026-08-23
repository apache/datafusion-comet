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
//! with a single expression backed by Arrow's decimal cast kernel.

use crate::SparkError;
use arrow::array::{as_primitive_array, ArrayRef, Decimal128Array};
use arrow::compute::{rescale_decimal, CastOptions};
use arrow::datatypes::{format_decimal_str, DataType, Decimal128Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{format::DEFAULT_CAST_OPTIONS, DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

/// A fused expression that rescales a Decimal128 value (changing scale) and checks
/// precision through Arrow's decimal cast kernel. Replaces the two-step
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

    fn overflow_error(&self, value: i128, input_scale: i8) -> DataFusionError {
        let unscaled = value.to_string();
        let digits = unscaled.trim_start_matches('-').len();
        DataFusionError::External(Box::new(SparkError::NumericValueOutOfRange {
            value: format_decimal_str(&unscaled, digits, input_scale),
            precision: self.output_precision,
            scale: self.output_scale,
        }))
    }

    fn evaluate_large_scale_delta(
        &self,
        arg: ColumnarValue,
        delta: i16,
        input_scale: i8,
    ) -> datafusion::common::Result<ColumnarValue> {
        match arg {
            ColumnarValue::Array(array) => {
                let input = as_primitive_array::<Decimal128Type>(&array);
                if self.fail_on_error && delta > 0 {
                    if let Some(value) = input.iter().flatten().find(|value| *value != 0) {
                        return Err(self.overflow_error(value, input_scale));
                    }
                }

                let result: Decimal128Array = if delta < 0 || self.fail_on_error {
                    input.unary(|_| 0)
                } else {
                    input.unary_opt(|value| (value == 0).then_some(0))
                };
                let result = result
                    .with_precision_and_scale(self.output_precision, self.output_scale)
                    .map(|array| Arc::new(array) as ArrayRef)?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(ScalarValue::Decimal128(value, _, _)) => {
                let value = match value {
                    Some(value) if delta > 0 && value != 0 && self.fail_on_error => {
                        return Err(self.overflow_error(value, input_scale));
                    }
                    Some(value) if delta > 0 && value != 0 => None,
                    Some(_) => Some(0),
                    None => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    value,
                    self.output_precision,
                    self.output_scale,
                )))
            }
            value => Err(DataFusionError::Execution(format!(
                "DecimalRescaleCheckOverflow expects Decimal128, but found {value:?}"
            ))),
        }
    }

    fn first_overflowing_value(
        &self,
        arg: &ColumnarValue,
        input_precision: u8,
        input_scale: i8,
    ) -> Option<i128> {
        let overflows = |value| {
            rescale_decimal::<Decimal128Type, Decimal128Type>(
                value,
                input_precision,
                input_scale,
                self.output_precision,
                self.output_scale,
            )
            .is_none()
        };

        match arg {
            ColumnarValue::Array(array) => as_primitive_array::<Decimal128Type>(array)
                .iter()
                .flatten()
                .find(|value| overflows(*value)),
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(value), _, _))
                if overflows(*value) =>
            {
                Some(*value)
            }
            _ => None,
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

impl PhysicalExpr for DecimalRescaleCheckOverflow {
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
        let (input_precision, input_scale) = match arg.data_type() {
            DataType::Decimal128(precision, scale) => (precision, scale),
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "DecimalRescaleCheckOverflow expects Decimal128, but found {arg:?}"
                )))
            }
        };

        let delta = self.output_scale as i16 - input_scale as i16;
        if delta.unsigned_abs() > 38 {
            return self.evaluate_large_scale_delta(arg, delta, input_scale);
        }

        let target_type = DataType::Decimal128(self.output_precision, self.output_scale);
        let options = CastOptions {
            safe: !self.fail_on_error,
            ..DEFAULT_CAST_OPTIONS
        };

        match arg.cast_to(&target_type, Some(&options)) {
            Ok(result) => Ok(result),
            Err(error) if self.fail_on_error => {
                match self.first_overflowing_value(&arg, input_precision, input_scale) {
                    Some(value) => Err(self.overflow_error(value, input_scale)),
                    None => Err(error),
                }
            }
            Err(error) => Err(error),
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
    use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array};
    use arrow::datatypes::{Decimal128Type, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::Column;

    fn assert_decimal_values(
        result: &ArrayRef,
        precision: u8,
        scale: i8,
        expected: &[Option<i128>],
    ) {
        assert_eq!(result.data_type(), &DataType::Decimal128(precision, scale));
        assert_eq!(
            result
                .as_primitive::<Decimal128Type>()
                .iter()
                .collect::<Vec<_>>(),
            expected.to_vec()
        );
    }

    fn assert_numeric_value_out_of_range(
        error: DataFusionError,
        expected_value: &str,
        expected_precision: u8,
        expected_scale: i8,
    ) {
        match error {
            DataFusionError::External(error) => match error.downcast_ref::<SparkError>() {
                Some(SparkError::NumericValueOutOfRange {
                    value,
                    precision,
                    scale,
                }) => {
                    assert_eq!(value, expected_value);
                    assert_eq!(*precision, expected_precision);
                    assert_eq!(*scale, expected_scale);
                }
                other => panic!("expected NumericValueOutOfRange, got {other:?}"),
            },
            other => panic!("expected external SparkError, got {other:?}"),
        }
    }

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
        // -1.2349 (-12349) -> round to -1.23 (-123) with HALF_UP
        let batch = make_batch(
            vec![Some(12350), Some(12349), Some(-12350), Some(-12349)],
            10,
            4,
        );
        let result = eval_expr(&batch, 4, 10, 2, false).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 124); // 1.24
        assert_eq!(arr.value(1), 123); // 1.23
        assert_eq!(arr.value(2), -124); // -1.24
        assert_eq!(arr.value(3), -123); // -1.23
    }

    #[test]
    fn test_half_up_at_max_supported_scale_delta() {
        let half = 5 * 10i128.pow(37);
        let batch = make_batch(vec![Some(half), Some(-half), None], 38, 38);
        for fail_on_error in [false, true] {
            let result = eval_expr(&batch, 38, 38, 0, fail_on_error).unwrap();
            assert_decimal_values(&result, 38, 0, &[Some(1), Some(-1), None]);
        }
    }

    #[test]
    fn test_scale_increase_at_max_supported_delta() {
        let batch = make_batch(vec![Some(0), None, Some(1), Some(-1)], 1, 0);
        let result = eval_expr(&batch, 0, 38, 38, false).unwrap();
        assert_decimal_values(&result, 38, 38, &[Some(0), None, None, None]);

        let error = eval_expr(&batch, 0, 38, 38, true).unwrap_err();
        assert_numeric_value_out_of_range(error, "1", 38, 38);
    }

    #[test]
    fn test_precision_is_checked_after_rounding() {
        // 9.95 rounds to 10.0, which no longer fits Decimal(2, 1).
        let batch = make_batch(
            vec![None, Some(994), Some(995), Some(-994), Some(-995)],
            3,
            2,
        );
        let result = eval_expr(&batch, 2, 2, 1, false).unwrap();
        assert_decimal_values(&result, 2, 1, &[None, Some(99), None, Some(-99), None]);

        let error = eval_expr(&batch, 2, 2, 1, true).unwrap_err();
        assert_numeric_value_out_of_range(error, "9.95", 2, 1);
    }

    #[test]
    fn test_spark_check_overflow_compatibility() {
        // Ported from Spark's DecimalExpressionSuite "CheckOverflow" test.
        let batch = make_batch(vec![Some(101), None], 3, 1);
        for (precision, scale, expected) in [(4, 0, 10), (4, 1, 101), (4, 2, 1010)] {
            for fail_on_error in [false, true] {
                let result = eval_expr(&batch, 1, precision, scale, fail_on_error).unwrap();
                assert_decimal_values(&result, precision, scale, &[Some(expected), None]);
            }
        }

        let result = eval_expr(&batch, 1, 4, 3, false).unwrap();
        assert_decimal_values(&result, 4, 3, &[None, None]);
        let error = eval_expr(&batch, 1, 4, 3, true).unwrap_err();
        assert_numeric_value_out_of_range(error, "10.1", 4, 3);

        let null_batch = make_batch(vec![None], 2, 1);
        for fail_on_error in [false, true] {
            let result = eval_expr(&null_batch, 1, 3, 2, fail_on_error).unwrap();
            assert_decimal_values(&result, 3, 2, &[None]);
        }
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
        // Scale up 10 by 100: 1000 overflows precision 3.
        let batch = make_batch(vec![Some(10)], 38, 0);
        let result = eval_expr(&batch, 0, 3, 2, false).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert!(arr.is_null(0)); // 10 * 100 = 1000 > 999 (max for precision 3)
    }

    #[test]
    fn test_overflow_error_in_ansi_mode() {
        let batch = make_batch(vec![None, Some(150), Some(-1000)], 10, 2);
        let error = eval_expr(&batch, 2, 3, 2, true).unwrap_err();
        assert_numeric_value_out_of_range(error, "-10.00", 3, 2);
    }

    #[test]
    fn test_overflow_with_nulls_legacy() {
        // Overflow and input nulls both become null; valid values are preserved.
        let batch = make_batch(vec![Some(150), Some(10_000), None, Some(250)], 10, 2);
        let result = eval_expr(&batch, 2, 4, 2, false).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 150);
        assert!(arr.is_null(1)); // 10000 > 9999 (max for precision 4) -> null
        assert!(arr.is_null(2)); // input null stays null
        assert_eq!(arr.value(3), 250);
    }

    #[test]
    fn test_all_values_overflow_legacy() {
        // Every value overflows, so the whole result is null.
        let batch = make_batch(vec![Some(10_000), Some(20_000), Some(30_000)], 10, 2);
        let result = eval_expr(&batch, 2, 4, 2, false).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert!(arr.is_null(0)); // all > 9999 (max for precision 4) -> null
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_precision_boundary_legacy() {
        // Pins the exact bound the overflow predicate turns on: 10^p - 1 fits, 10^p overflows.
        // Output precision 4, scale 0: 9999 (= 10^4 - 1) fits, 10000 (= 10^4) does not.
        let batch = make_batch(vec![Some(9999), Some(10_000)], 10, 0);
        let result = eval_expr(&batch, 0, 4, 0, false).unwrap();
        let arr = result.as_primitive::<Decimal128Type>();
        assert_eq!(arr.value(0), 9999); // fits precision 4
        assert!(arr.is_null(1)); // overflows precision 4 -> null
    }

    #[test]
    fn test_null_propagation() {
        let batch = make_batch(vec![Some(100), None, Some(200)], 10, 2);
        for fail_on_error in [false, true] {
            let result = eval_expr(&batch, 2, 10, 4, fail_on_error).unwrap();
            let arr = result.as_primitive::<Decimal128Type>();
            assert!(!arr.is_null(0));
            assert!(arr.is_null(1));
            assert!(!arr.is_null(2));
        }
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
        let schema = Schema::new(vec![Field::new("col", DataType::Decimal128(38, 0), true)]);
        let batch = RecordBatch::new_empty(Arc::new(schema));
        let value = 10i128.pow(38) - 1;
        let expr = DecimalRescaleCheckOverflow::new(
            Arc::new(ScalarChild(Some(value), 38, 0)),
            0,
            38,
            1,
            true,
        );
        let error = expr.evaluate(&batch).unwrap_err();
        assert_numeric_value_out_of_range(error, &value.to_string(), 38, 1);
    }

    #[test]
    fn test_large_scale_increase_matches_spark() {
        // Spark's BigDecimal changePrecision path is value-sensitive beyond Arrow's scale table.
        let batch = make_batch(vec![Some(0), None, Some(1), Some(-1)], 38, -1);
        let result = eval_expr(&batch, -1, 38, 38, false).unwrap();
        assert_decimal_values(&result, 38, 38, &[Some(0), None, None, None]);

        let zero_and_null = make_batch(vec![Some(0), None], 38, -1);
        let result = eval_expr(&zero_and_null, -1, 38, 38, true).unwrap();
        assert_decimal_values(&result, 38, 38, &[Some(0), None]);

        let error = eval_expr(&batch, -1, 38, 38, true).unwrap_err();
        assert_numeric_value_out_of_range(error, "10", 38, 38);

        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let eval_scalar = |value, input_scale, output_scale, fail_on_error| {
            DecimalRescaleCheckOverflow::new(
                Arc::new(ScalarChild(value, 38, input_scale)),
                input_scale,
                38,
                output_scale,
                fail_on_error,
            )
            .evaluate(&empty_batch)
        };

        assert!(matches!(
            eval_scalar(Some(1), -1, 38, false).unwrap(),
            ColumnarValue::Scalar(ScalarValue::Decimal128(None, 38, 38))
        ));
        assert!(matches!(
            eval_scalar(Some(0), -1, 38, true).unwrap(),
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(0), 38, 38))
        ));
        let error = eval_scalar(Some(1), -1, 38, true).unwrap_err();
        assert_numeric_value_out_of_range(error, "10", 38, 38);
        for fail_on_error in [false, true] {
            assert!(matches!(
                eval_scalar(Some(-1), 38, -1, fail_on_error).unwrap(),
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(0), 38, -1))
            ));
        }
    }

    #[test]
    fn test_large_scale_reduction_returns_zero() {
        let batch = make_batch(vec![Some(1), None, Some(-1)], 38, 38);
        for fail_on_error in [false, true] {
            let result = eval_expr(&batch, 38, 38, -1, fail_on_error).unwrap();
            assert_decimal_values(&result, 38, -1, &[Some(0), None, Some(0)]);
        }
    }
}
