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

use arrow::datatypes::{DataType, Schema};
use arrow::{
    array::{as_primitive_array, Array, ArrayRef, Decimal128Array},
    datatypes::{Decimal128Type, DecimalType},
    record_batch::RecordBatch,
};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::hash::Hash;

use crate::SparkError;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

/// This is from Spark `CheckOverflow` expression. Spark `CheckOverflow` expression rounds decimals
/// to given scale and check if the decimals can fit in given precision. As `cast` kernel rounds
/// decimals already, Comet `CheckOverflow` expression only checks if the decimals can fit in the
/// precision.
#[derive(Debug, Eq)]
pub struct CheckOverflow {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub fail_on_error: bool,
    pub expr_id: Option<u64>,
    pub query_context: Option<Arc<crate::QueryContext>>,
}

impl Hash for CheckOverflow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.data_type.hash(state);
        self.fail_on_error.hash(state);
    }
}

impl PartialEq for CheckOverflow {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.data_type.eq(&other.data_type)
            && self.fail_on_error.eq(&other.fail_on_error)
    }
}

impl CheckOverflow {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        fail_on_error: bool,
        expr_id: Option<u64>,
        query_context: Option<Arc<crate::QueryContext>>,
    ) -> Self {
        Self {
            child,
            data_type,
            fail_on_error,
            expr_id,
            query_context,
        }
    }
}

impl Display for CheckOverflow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CheckOverflow [datatype: {}, fail_on_error: {}, child: {}]",
            self.data_type, self.fail_on_error, self.child
        )
    }
}

impl PhysicalExpr for CheckOverflow {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array)
                if matches!(array.data_type(), DataType::Decimal128(_, _)) =>
            {
                let (precision, scale) = match &self.data_type {
                    DataType::Decimal128(p, s) => (p, s),
                    dt => {
                        return Err(DataFusionError::Execution(format!(
                            "CheckOverflow expects only Decimal128, but got {dt:?}"
                        )))
                    }
                };

                let decimal_array = as_primitive_array::<Decimal128Type>(&array);

                // Fast path shared by both ANSI and non-ANSI: `is_valid_decimal_precision` is a
                // small, inlined bounds check and `all` short-circuits at the first overflow. When
                // nothing overflows (the common shape for decimal arithmetic in TPC-DS) we reuse the
                // input buffers via `to_data()`, which only clones cheap Arc metadata. This avoids
                // the heavier per-value `validate_decimal_precision` scan (ANSI) or the allocating
                // `null_if_overflow_precision` (non-ANSI) below.
                let no_overflow = decimal_array
                    .iter()
                    .flatten()
                    .all(|v| Decimal128Type::is_valid_decimal_precision(v, *precision));

                let casted_array = if no_overflow {
                    Decimal128Array::from(decimal_array.to_data())
                } else if self.fail_on_error {
                    // ANSI mode with a genuine overflow. Re-run the heavier
                    // `validate_decimal_precision` so it builds the precise Spark error for the
                    // first offending value. This extra scan only runs on the error path, which
                    // aborts the query anyway.
                    decimal_array
                        .validate_decimal_precision(*precision)
                        .map_err(|e| {
                            if matches!(e, arrow::error::ArrowError::InvalidArgumentError(_))
                                && e.to_string().contains("too large to store in a Decimal128") {
                                // Find the first overflowing value
                                let overflow_value = decimal_array
                                    .iter()
                                    .find(|v| {
                                        if let Some(val) = v {
                                            arrow::array::types::Decimal128Type::validate_decimal_precision(
                                                *val, *precision, *scale
                                            ).is_err()
                                        } else {
                                            false
                                        }
                                    })
                                    .and_then(|v| v)
                                    .unwrap_or(0);

                                let spark_error = crate::error::decimal_overflow_error(overflow_value, *precision, *scale);

                                // Wrap with query_context if present
                                if let Some(ctx) = &self.query_context {
                                    DataFusionError::External(Box::new(
                                        crate::SparkErrorWithContext::with_context(spark_error, Arc::clone(ctx))
                                    ))
                                } else {
                                    DataFusionError::External(Box::new(spark_error))
                                }
                            } else {
                                DataFusionError::ArrowError(Box::new(e), None)
                            }
                        })?;
                    // `is_valid_decimal_precision` and `validate_decimal_precision` check identical
                    // precision bounds, so reaching here means `validate_decimal_precision` should
                    // already have returned an error above.
                    return Err(DataFusionError::Internal(
                        "CheckOverflow detected an overflow that validate_decimal_precision did not report".to_string(),
                    ));
                } else {
                    // Non-ANSI: overflowing values become null.
                    decimal_array.null_if_overflow_precision(*precision)
                };

                let new_array = casted_array
                    .with_precision_and_scale(*precision, *scale)
                    .map(|a| Arc::new(a) as ArrayRef)
                    .map_err(|e| {
                        if matches!(e, arrow::error::ArrowError::InvalidArgumentError(_))
                            && e.to_string().contains("too large to store in a Decimal128")
                        {
                            // Fallback error handling
                            let spark_error = SparkError::NumericValueOutOfRange {
                                value: "overflow".to_string(),
                                precision: *precision,
                                scale: *scale,
                            };

                            // Wrap with query_context if present
                            if let Some(ctx) = &self.query_context {
                                DataFusionError::External(Box::new(
                                    crate::SparkErrorWithContext::with_context(
                                        spark_error,
                                        Arc::clone(ctx),
                                    ),
                                ))
                            } else {
                                DataFusionError::External(Box::new(spark_error))
                            }
                        } else {
                            DataFusionError::ArrowError(Box::new(e), None)
                        }
                    })?;

                Ok(ColumnarValue::Array(new_array))
            }
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, precision, scale)) => {
                if self.fail_on_error {
                    if let Some(val) = v {
                        Decimal128Type::validate_decimal_precision(val, precision, scale).map_err(
                            |_| {
                                let spark_error =
                                    crate::error::decimal_overflow_error(val, precision, scale);
                                if let Some(ctx) = &self.query_context {
                                    DataFusionError::External(Box::new(
                                        crate::SparkErrorWithContext::with_context(
                                            spark_error,
                                            Arc::clone(ctx),
                                        ),
                                    ))
                                } else {
                                    DataFusionError::External(Box::new(spark_error))
                                }
                            },
                        )?;
                    }
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        v, precision, scale,
                    )))
                } else {
                    let new_v: Option<i128> = v.and_then(|v| {
                        Decimal128Type::validate_decimal_precision(v, precision, scale)
                            .map(|_| v)
                            .ok()
                    });
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        new_v, precision, scale,
                    )))
                }
            }
            v => Err(DataFusionError::Execution(format!(
                "CheckOverflow's child expression should be decimal array, but found {v:?}"
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
        Ok(Arc::new(CheckOverflow::new(
            Arc::clone(&children[0]),
            self.data_type.clone(),
            self.fail_on_error,
            self.expr_id,
            self.query_context.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::fmt::{Display, Formatter};

    /// Helper that always returns a fixed Decimal128 scalar.
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
        fn evaluate(&self, _: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                self.0, self.1, self.2,
            )))
        }
        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn PhysicalExpr>>,
        ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self, f)
        }
    }

    fn empty_batch() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("x", DataType::Decimal128(38, 0), true)]);
        RecordBatch::new_empty(Arc::new(schema))
    }

    fn make_check_overflow(
        value: Option<i128>,
        precision: u8,
        scale: i8,
        fail_on_error: bool,
    ) -> CheckOverflow {
        CheckOverflow::new(
            Arc::new(ScalarChild(value, precision, scale)),
            DataType::Decimal128(precision, scale),
            fail_on_error,
            None,
            None,
        )
    }

    // --- scalar, fail_on_error = false (legacy mode) ---

    #[test]
    fn test_scalar_no_overflow_legacy() {
        // 999 fits in precision 3, scale 0 → returned as-is
        let expr = make_check_overflow(Some(999), 3, 0, false);
        let result = expr.evaluate(&empty_batch()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) => assert_eq!(v, Some(999)),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_scalar_overflow_returns_null_in_legacy_mode() {
        // 1000 does not fit in precision 3 → null, no error
        let expr = make_check_overflow(Some(1000), 3, 0, false);
        let result = expr.evaluate(&empty_batch()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) => assert_eq!(v, None),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_scalar_null_passthrough_legacy() {
        let expr = make_check_overflow(None, 3, 0, false);
        let result = expr.evaluate(&empty_batch()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) => assert_eq!(v, None),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // --- scalar, fail_on_error = true (ANSI mode) ---

    #[test]
    fn test_scalar_no_overflow_ansi() {
        // 999 fits in precision 3 → returned as-is, no error
        let expr = make_check_overflow(Some(999), 3, 0, true);
        let result = expr.evaluate(&empty_batch()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) => assert_eq!(v, Some(999)),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_scalar_overflow_returns_error_in_ansi_mode() {
        // 1000 does not fit in precision 3 → error, not Ok(None)
        // This is the case that previously panicked with "fail_on_error (ANSI mode) is not
        // supported yet".
        let expr = make_check_overflow(Some(1000), 3, 0, true);
        let result = expr.evaluate(&empty_batch());
        assert!(result.is_err(), "expected error on overflow in ANSI mode");
    }

    #[test]
    fn test_scalar_null_passthrough_ansi() {
        // None input → None output even in ANSI mode (no value to overflow)
        let expr = make_check_overflow(None, 3, 0, true);
        let result = expr.evaluate(&empty_batch()).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, 3, 0)) => assert_eq!(v, None),
            other => panic!("unexpected: {other:?}"),
        }
    }

    // --- array path ---

    fn array_batch(values: Vec<Option<i128>>, in_precision: u8, scale: i8) -> RecordBatch {
        let arr = values
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(in_precision, scale)
            .unwrap();
        let schema = Schema::new(vec![Field::new("d", arr.data_type().clone(), true)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arr)]).unwrap()
    }

    fn array_check_overflow(target_precision: u8, scale: i8, fail_on_error: bool) -> CheckOverflow {
        CheckOverflow::new(
            Arc::new(datafusion::physical_plan::expressions::Column::new("d", 0)),
            DataType::Decimal128(target_precision, scale),
            fail_on_error,
            None,
            None,
        )
    }

    fn eval_array(expr: &CheckOverflow, batch: &RecordBatch) -> Decimal128Array {
        match expr.evaluate(batch).unwrap() {
            ColumnarValue::Array(a) => a
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .clone(),
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_array_no_overflow_legacy_preserves_values_and_type() {
        // No value overflows precision 3, so the fast path reuses the input; values, nulls,
        // and the target precision/scale must all be preserved.
        let batch = array_batch(vec![Some(999), Some(12), None, Some(5)], 38, 0);
        let out = eval_array(&array_check_overflow(3, 0, false), &batch);
        assert_eq!(out.data_type(), &DataType::Decimal128(3, 0));
        assert_eq!(
            out.iter().collect::<Vec<_>>(),
            vec![Some(999), Some(12), None, Some(5)]
        );
    }

    #[test]
    fn test_array_overflow_nulled_legacy() {
        // 1000 does not fit precision 3 → nulled; other values and existing nulls kept.
        let batch = array_batch(vec![Some(999), Some(1000), None, Some(5)], 38, 0);
        let out = eval_array(&array_check_overflow(3, 0, false), &batch);
        assert_eq!(out.data_type(), &DataType::Decimal128(3, 0));
        assert_eq!(
            out.iter().collect::<Vec<_>>(),
            vec![Some(999), None, None, Some(5)]
        );
    }

    #[test]
    fn test_array_no_overflow_ansi_ok() {
        let batch = array_batch(vec![Some(999), None, Some(5)], 38, 0);
        let out = eval_array(&array_check_overflow(3, 0, true), &batch);
        assert_eq!(
            out.iter().collect::<Vec<_>>(),
            vec![Some(999), None, Some(5)]
        );
    }

    #[test]
    fn test_array_overflow_ansi_errors() {
        let batch = array_batch(vec![Some(999), Some(1000)], 38, 0);
        let result = array_check_overflow(3, 0, true).evaluate(&batch);
        assert!(result.is_err(), "expected error on overflow in ANSI mode");
    }
}
