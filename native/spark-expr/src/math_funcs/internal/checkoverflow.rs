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
    datatypes::{
        Decimal128Type, DecimalType, MAX_DECIMAL128_FOR_EACH_PRECISION,
        MIN_DECIMAL128_FOR_EACH_PRECISION,
    },
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

                // Fast path shared by both ANSI and non-ANSI. When nothing overflows (the common
                // shape for decimal arithmetic in TPC-DS) we reuse the input buffers via
                // `to_data()`, which only clones cheap Arc metadata. This avoids the heavier
                // per-value `validate_decimal_precision` scan (ANSI) or the allocating
                // `null_if_overflow_precision` (non-ANSI) below.
                let no_overflow = fits_precision(decimal_array, *precision);

                let casted_array = if no_overflow {
                    Decimal128Array::from(decimal_array.to_data())
                } else if self.fail_on_error {
                    // ANSI mode with a genuine overflow. The fast-path scan already proved an
                    // overflow exists, so locate the first offending value and raise the precise
                    // Spark error directly. `is_valid_decimal_precision` checks both the upper and
                    // lower precision bounds, so this catches negative (underflow) overflow as well
                    // as positive. This branch only runs on the error path, which aborts the query.
                    let overflow_value = decimal_array
                        .iter()
                        .flatten()
                        .find(|v| !Decimal128Type::is_valid_decimal_precision(*v, *precision))
                        .unwrap_or(0);
                    let spark_error =
                        crate::error::decimal_overflow_error(overflow_value, *precision, *scale);
                    return Err(match &self.query_context {
                        Some(ctx) => DataFusionError::External(Box::new(
                            crate::SparkErrorWithContext::with_context(
                                spark_error,
                                Arc::clone(ctx),
                            ),
                        )),
                        None => DataFusionError::External(Box::new(spark_error)),
                    });
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

/// Whether every non-null value of `array` fits `precision`, the check that
/// `Decimal128Type::is_valid_decimal_precision` makes for one value.
///
/// arrow-rs's `PrimitiveArray::validate_decimal_precision` makes the same check slot by slot.
/// <https://github.com/apache/arrow-rs/issues/12368> proposes this scan for it. Once Comet's
/// arrow version has that, this function can call `validate_decimal_precision(precision).is_ok()`
/// if that is as fast.
#[inline]
fn fits_precision(array: &Decimal128Array, precision: u8) -> bool {
    let bounds = (
        MIN_DECIMAL128_FOR_EACH_PRECISION.get(precision as usize),
        MAX_DECIMAL128_FOR_EACH_PRECISION.get(precision as usize),
    );
    let (Some(&min), Some(&max)) = bounds else {
        return array
            .iter()
            .flatten()
            .all(|v| Decimal128Type::is_valid_decimal_precision(v, precision));
    };
    // Scan every slot of the values buffer, null or not, instead of reading the null bitmap per
    // slot. Shifting by `min` turns the two bounds checks into one unsigned comparison, so the
    // loop is one compare per value and a branch that is only taken at a value that doesn't fit.
    let range = max.wrapping_sub(min) as u128;
    let values = array.values();
    let nulls = array.nulls();
    let mut start = 0;
    while let Some(offset) = values[start..]
        .iter()
        .position(|&v| v.wrapping_sub(min) as u128 > range)
    {
        // A null slot can hold any value, so a value that doesn't fit only counts in a valid
        // slot. The null bitmap is read only at such a value, and the scan resumes after it.
        let index = start + offset;
        if !nulls.is_some_and(|nulls| nulls.is_null(index)) {
            return false;
        }
        start = index + 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::NullBuffer;
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

    #[test]
    fn test_array_negative_overflow_nulled_legacy() {
        // -1000 is below the precision-3 lower bound (-999) → nulled; other values kept.
        // Guards the negative (underflow) bound, which is a distinct branch from positive overflow.
        let batch = array_batch(vec![Some(-1000), Some(5)], 38, 0);
        let out = eval_array(&array_check_overflow(3, 0, false), &batch);
        assert_eq!(out.iter().collect::<Vec<_>>(), vec![None, Some(5)]);
    }

    #[test]
    fn test_array_negative_overflow_ansi_errors() {
        // ANSI mode must raise on a negative overflow, not only a positive one. The previous
        // implementation string-matched "too large" and silently missed the "too small" branch.
        let batch = array_batch(vec![Some(5), Some(-1000)], 38, 0);
        let result = array_check_overflow(3, 0, true).evaluate(&batch);
        assert!(
            result.is_err(),
            "expected error on negative overflow in ANSI mode"
        );
    }

    #[test]
    fn test_array_all_null_reuses_input_and_preserves_mask() {
        // The fast-path scan is `flatten().all(...)`, which returns true on an all-null batch
        // (empty after flatten). The input must be reused unchanged with its null mask intact.
        let batch = array_batch(vec![None, None, None], 38, 0);
        let out = eval_array(&array_check_overflow(3, 0, false), &batch);
        assert_eq!(out.data_type(), &DataType::Decimal128(3, 0));
        assert_eq!(out.iter().collect::<Vec<_>>(), vec![None, None, None]);
    }

    #[test]
    fn test_array_all_overflow_nulled_legacy() {
        // Every value overflows precision 3 → every slot nulled in legacy mode.
        let batch = array_batch(vec![Some(1000), Some(5000), Some(-2000)], 38, 0);
        let out = eval_array(&array_check_overflow(3, 0, false), &batch);
        assert_eq!(out.iter().collect::<Vec<_>>(), vec![None, None, None]);
    }

    #[test]
    fn test_array_all_overflow_ansi_errors() {
        let batch = array_batch(vec![Some(1000), Some(5000)], 38, 0);
        let result = array_check_overflow(3, 0, true).evaluate(&batch);
        assert!(result.is_err(), "expected error on overflow in ANSI mode");
    }

    #[test]
    fn test_array_boundary_precision_max_passes_and_over_by_one_overflows() {
        // 999 is exactly the max for precision 3 and must pass; 9999 is over and must be nulled.
        // Pins the off-by-one on the precision bound.
        let batch = array_batch(vec![Some(999), Some(9999)], 38, 0);
        let out = eval_array(&array_check_overflow(3, 0, false), &batch);
        assert_eq!(out.iter().collect::<Vec<_>>(), vec![Some(999), None]);

        // The scan shifts by the precision's minimum and compares unsigned, so also check the
        // values where that shift wraps, at the widest precision.
        let max = MAX_DECIMAL128_FOR_EACH_PRECISION[38];
        let min = MIN_DECIMAL128_FOR_EACH_PRECISION[38];
        let fits = array_batch(vec![Some(max), Some(min), Some(0)], 38, 0);
        let out = eval_array(&array_check_overflow(38, 0, true), &fits);
        assert_eq!(out.null_count(), 0);
        for v in [max + 1, min - 1, i128::MAX, i128::MIN] {
            let batch = array_batch(vec![Some(0), Some(v)], 38, 0);
            let out = eval_array(&array_check_overflow(38, 0, false), &batch);
            assert!(out.is_null(1), "{v}");
            let ansi = array_check_overflow(38, 0, true).evaluate(&batch);
            assert!(ansi.is_err(), "{v}");
        }
    }

    #[test]
    fn test_array_overflow_at_every_position_and_sliced() {
        // The scan stops at the first value that doesn't fit, and a compiled loop can split the
        // batch into an unrolled part and a remainder, so move one value that doesn't fit
        // through every row. 203 rows are not a multiple of any unroll factor.
        for overflow in [1000, -1000] {
            for i in 0..203 {
                let mut values = vec![Some(5); 203];
                values[i] = Some(overflow);
                let batch = array_batch(values, 38, 0);
                let out = eval_array(&array_check_overflow(3, 0, false), &batch);
                assert_eq!(out.null_count(), 1, "{overflow} at {i}");
                assert!(out.is_null(i), "{overflow} at {i}");
                let ansi = array_check_overflow(3, 0, true).evaluate(&batch);
                assert!(ansi.is_err(), "{overflow} at {i}");
            }
        }

        // A sliced array must only be checked within its slice. This one starts right after
        // the overflowing value, so the slice has a non-zero offset.
        let mut values = vec![Some(5); 200];
        values[130] = Some(1000);
        let sliced = array_batch(values, 38, 0).slice(131, 69);
        let out = eval_array(&array_check_overflow(3, 0, true), &sliced);
        assert_eq!(out.len(), 69);
        assert_eq!(out.null_count(), 0);
    }

    /// A Decimal128(38, 0) batch whose null slots keep the values given for them.
    fn array_batch_with_null_slots(values: Vec<i128>, valid: Vec<bool>) -> RecordBatch {
        let arr = Decimal128Array::new(values.into(), Some(NullBuffer::from(valid)))
            .with_precision_and_scale(38, 0)
            .unwrap();
        let schema = Schema::new(vec![Field::new("d", arr.data_type().clone(), true)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arr)]).unwrap()
    }

    #[test]
    fn test_array_null_slot_out_of_range_is_ignored() {
        // A null slot can hold any value. One that doesn't fit the precision must not null out
        // valid values or raise an ANSI error, and the scan must still find a value that doesn't
        // fit in a valid slot after it, next to it or at the end.
        let n = 70;
        for i in 0..n - 1 {
            let mut values = vec![5_i128; n];
            let mut valid = vec![true; n];
            values[i] = 1000;
            valid[i] = false;
            let mut expected = vec![Some(5_i128); n];
            expected[i] = None;
            let batch = array_batch_with_null_slots(values.clone(), valid.clone());
            for fail_on_error in [false, true] {
                let out = eval_array(&array_check_overflow(3, 0, fail_on_error), &batch);
                assert_eq!(out.iter().collect::<Vec<_>>(), expected, "null slot {i}");
            }
            for j in [i + 1, n - 1] {
                let mut values = values.clone();
                values[j] = -1000;
                let batch = array_batch_with_null_slots(values, valid.clone());
                let out = eval_array(&array_check_overflow(3, 0, false), &batch);
                let mut expected = expected.clone();
                expected[j] = None;
                assert_eq!(out.iter().collect::<Vec<_>>(), expected, "{i}, {j}");
                let ansi = array_check_overflow(3, 0, true).evaluate(&batch);
                assert!(ansi.is_err(), "null slot {i}, overflow at {j}");
            }
        }

        // In a slice, the values and the null bitmap both count from the slice's first row.
        let mut values = vec![5_i128; 100];
        let mut valid = vec![true; 100];
        values[40] = 1000;
        values[60] = 1000;
        valid[60] = false;
        let sliced = array_batch_with_null_slots(values, valid).slice(41, 59);
        let out = eval_array(&array_check_overflow(3, 0, true), &sliced);
        assert_eq!(out.null_count(), 1);
        assert!(out.is_null(19));
    }
}
