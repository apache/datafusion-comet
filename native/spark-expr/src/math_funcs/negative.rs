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

use crate::arithmetic_overflow_error;
use crate::SparkError;
use arrow::array::RecordBatch;
use arrow::compute::kernels::numeric::{neg, neg_wrapping};
use arrow::datatypes::IntervalDayTimeType;
use arrow::datatypes::{DataType, IntervalUnit, Schema};
use arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::sort_properties::ExprProperties;
use datafusion::{
    logical_expr::{interval_arithmetic::Interval, ColumnarValue},
    physical_expr::PhysicalExpr,
};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

pub fn create_negate_expr(
    expr: Arc<dyn PhysicalExpr>,
    fail_on_error: bool,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    Ok(Arc::new(NegativeExpr::new(expr, fail_on_error)))
}

/// Negative expression
#[derive(Debug, Eq)]
pub struct NegativeExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
    fail_on_error: bool,
}

impl Hash for NegativeExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
        self.fail_on_error.hash(state);
    }
}

impl PartialEq for NegativeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg) && self.fail_on_error.eq(&other.fail_on_error)
    }
}

impl NegativeExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>, fail_on_error: bool) -> Self {
        Self { arg, fail_on_error }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for NegativeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(- {})", self.arg)
    }
}

fn map_neg_error(err: ArrowError, from_type: &'static str) -> DataFusionError {
    match err {
        ArrowError::ArithmeticOverflow(_) => arithmetic_overflow_error(from_type).into(),
        other => DataFusionError::from(other),
    }
}

impl PhysicalExpr for NegativeExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.arg.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;

        // Overflow checks only apply in ANSI mode, and only the types listed in the
        // match below have a Spark overflow message. Everything else (float, decimal,
        // `Interval(MonthDayNano)`, ...) falls through to `neg_wrapping`.
        match arg {
            ColumnarValue::Array(array) => {
                if !self.fail_on_error {
                    return Ok(ColumnarValue::Array(neg_wrapping(array.as_ref())?));
                }
                // The shims render this as `{from_type} overflow` under
                // `ARITHMETIC_OVERFLOW`. For byte/short that is byte-identical to Spark
                // 4.x, which routes them through `MathUtils.negateExact` ("byte overflow" /
                // "short overflow"). Spark 3.4/3.5 instead throw
                // `_LEGACY_ERROR_TEMP_2043` ("- <sqlValue> caused overflow."); that error
                // class is out of reach here, so no string can match every version.
                let from_type = match array.data_type() {
                    DataType::Int8 => "byte",
                    DataType::Int16 => "short",
                    DataType::Int32 => "integer",
                    DataType::Int64 => "long",
                    // `neg` checks each `DayTime` component, so either `days` or `ms` at
                    // `i32::MIN` overflows; routing intervals here maps the Arrow overflow
                    // error onto Spark's, matching the scalar path below.
                    DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime) => {
                        "interval"
                    }
                    // Everything else falls through to `neg_wrapping`. Note it wraps only
                    // for integers: for any other type, `Interval(MonthDayNano)` included,
                    // it delegates to `neg`, so overflow is still detected -- it just
                    // surfaces as an Arrow error rather than a Spark one.
                    _ => return Ok(ColumnarValue::Array(neg_wrapping(array.as_ref())?)),
                };
                Ok(ColumnarValue::Array(
                    neg(array.as_ref()).map_err(|e| map_neg_error(e, from_type))?,
                ))
            }
            ColumnarValue::Scalar(scalar) => {
                if self.fail_on_error {
                    match scalar {
                        // Keep scalar overflow type names aligned with the array path.
                        ScalarValue::Int8(Some(i8::MIN)) => {
                            return Err(arithmetic_overflow_error("byte").into());
                        }
                        ScalarValue::Int16(Some(i16::MIN)) => {
                            return Err(arithmetic_overflow_error("short").into());
                        }
                        ScalarValue::Int32(Some(i32::MIN)) => {
                            return Err(arithmetic_overflow_error("integer").into());
                        }
                        ScalarValue::Int64(Some(i64::MIN)) => {
                            return Err(arithmetic_overflow_error("long").into());
                        }
                        ScalarValue::IntervalDayTime(value) => {
                            let (days, ms) =
                                IntervalDayTimeType::to_parts(value.unwrap_or_default());
                            if days == i32::MIN || ms == i32::MIN {
                                return Err(arithmetic_overflow_error("interval").into());
                            }
                        }
                        ScalarValue::IntervalYearMonth(Some(i32::MIN)) => {
                            return Err(arithmetic_overflow_error("interval").into());
                        }
                        ScalarValue::IntervalYearMonth(_) => {}
                        _ => {
                            // Overflow checks are not supported for other datatypes
                        }
                    }
                }
                Ok(ColumnarValue::Scalar((scalar.arithmetic_negate())?))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(NegativeExpr::new(
            Arc::clone(&children[0]),
            self.fail_on_error,
        )))
    }

    /// Given the child interval of a NegativeExpr, it calculates the NegativeExpr's interval.
    /// It replaces the upper and lower bounds after multiplying them with -1.
    /// Ex: `(a, b]` => `[-b, -a)`
    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        Interval::try_new(
            children[0].upper().arithmetic_negate()?,
            children[0].lower().arithmetic_negate()?,
        )
    }

    /// Returns a new [`Interval`] of a NegativeExpr  that has the existing `interval` given that
    /// given the input interval is known to be `children`.
    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let child_interval = children[0];

        if child_interval.lower() == &ScalarValue::Int32(Some(i32::MIN))
            || child_interval.upper() == &ScalarValue::Int32(Some(i32::MIN))
            || child_interval.lower() == &ScalarValue::Int64(Some(i64::MIN))
            || child_interval.upper() == &ScalarValue::Int64(Some(i64::MIN))
        {
            return Err(SparkError::ArithmeticOverflow {
                from_type: "long".to_string(),
            }
            .into());
        }

        let negated_interval = Interval::try_new(
            interval.upper().arithmetic_negate()?,
            interval.lower().arithmetic_negate()?,
        )?;

        Ok(child_interval
            .intersect(negated_interval)?
            .map(|result| vec![result]))
    }

    /// The ordering of a [`NegativeExpr`] is simply the reverse of its child.
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let properties = children[0].clone().with_order(children[0].sort_properties);
        Ok(properties)
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::*, buffer::NullBuffer, datatypes::*};
    use datafusion::{
        physical_expr::expressions::{Column, Literal},
        physical_plan::ColumnarValue,
    };

    fn eval_array(array: ArrayRef, fail_on_error: bool) -> Result<ColumnarValue> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
        NegativeExpr::new(Arc::new(Column::new("a", 0)), fail_on_error).evaluate(&batch)
    }

    fn eval_scalar(scalar: ScalarValue, fail_on_error: bool) -> Result<ColumnarValue> {
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        NegativeExpr::new(Arc::new(Literal::new(scalar)), fail_on_error).evaluate(&batch)
    }

    fn assert_spark_overflow(err: DataFusionError, expected_from_type: &str) {
        if let DataFusionError::External(ref e) = err {
            if let Some(SparkError::ArithmeticOverflow { from_type }) =
                e.downcast_ref::<SparkError>()
            {
                assert_eq!(from_type, expected_from_type);
                return;
            }
        }
        panic!(
            "Expected SparkError::ArithmeticOverflow {{ from_type: {:?} }}, got: {:?}",
            expected_from_type, err
        );
    }

    /// Negate `[min, other]` in ANSI mode with slot 0 marked null, and assert the
    /// null slot is skipped instead of raising a spurious overflow.
    fn assert_null_min_slot_is_skipped<T: ArrowPrimitiveType>(
        min: T::Native,
        other: T::Native,
        negated_other: T::Native,
    ) {
        let nulls = NullBuffer::from(vec![false, true]);
        let array: ArrayRef = Arc::new(PrimitiveArray::<T>::new(
            vec![min, other].into(),
            Some(nulls),
        ));
        let ColumnarValue::Array(result) = eval_array(array, true).unwrap() else {
            panic!("expected array result")
        };
        let result = result.as_primitive::<T>();
        assert!(result.is_null(0));
        assert_eq!(result.value(1), negated_other);
    }

    /// A MIN sentinel left behind in a null slot (by filter, slice or FFI) must not raise a
    /// spurious ANSI overflow: the overflow check has to consult the null buffer and skip
    /// invalid slots. Each case below places `MIN` in a null slot.
    #[test]
    fn test_ansi_null_slot_with_min_values_does_not_overflow() {
        assert_null_min_slot_is_skipped::<Int8Type>(i8::MIN, 7, -7);
        assert_null_min_slot_is_skipped::<Int16Type>(i16::MIN, 7, -7);
        assert_null_min_slot_is_skipped::<Int32Type>(i32::MIN, 7, -7);
        assert_null_min_slot_is_skipped::<Int64Type>(i64::MIN, 7, -7);
        assert_null_min_slot_is_skipped::<IntervalYearMonthType>(i32::MIN, 7, -7);
        // `IntervalDayTime::MIN` is both components at `i32::MIN`.
        assert_null_min_slot_is_skipped::<IntervalDayTimeType>(
            IntervalDayTime::MIN,
            IntervalDayTime::new(1, 2),
            IntervalDayTime::new(-1, -2),
        );
    }

    #[test]
    fn test_ansi_valid_min_values_raise_exact_spark_overflow_errors() {
        let arr_i8: ArrayRef = Arc::new(Int8Array::from(vec![i8::MIN]));
        assert_spark_overflow(eval_array(arr_i8, true).unwrap_err(), "byte");

        let arr_i16: ArrayRef = Arc::new(Int16Array::from(vec![i16::MIN]));
        assert_spark_overflow(eval_array(arr_i16, true).unwrap_err(), "short");

        let arr_i32: ArrayRef = Arc::new(Int32Array::from(vec![i32::MIN]));
        assert_spark_overflow(eval_array(arr_i32, true).unwrap_err(), "integer");

        let arr_i64: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN]));
        assert_spark_overflow(eval_array(arr_i64, true).unwrap_err(), "long");

        let arr_ym: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![i32::MIN]));
        assert_spark_overflow(eval_array(arr_ym, true).unwrap_err(), "interval");

        let arr_dt: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::MIN]));
        assert_spark_overflow(eval_array(arr_dt, true).unwrap_err(), "interval");
    }

    /// A single `DayTime` component at `i32::MIN` overflows: `neg` checks each component
    /// (`neg_wrapping` delegates to `neg` for every non-integer type,
    /// `downcast_integer! { ..., _ => neg(array) }`). These surface as Spark overflows like
    /// every other ANSI overflow in this expression.
    #[test]
    fn test_ansi_interval_day_time_component_overflow_maps_to_spark_error() {
        for value in [
            IntervalDayTime::new(i32::MIN, 0),
            IntervalDayTime::new(0, i32::MIN),
        ] {
            let array: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![value]));
            assert_spark_overflow(eval_array(array, true).unwrap_err(), "interval");
        }
    }

    /// Negate `[min, null]` in legacy mode and assert `min` wraps to itself.
    fn assert_legacy_wraps_min<T: ArrowPrimitiveType>(min: T::Native) {
        let array: ArrayRef = Arc::new(PrimitiveArray::<T>::new(
            vec![min, T::Native::default()].into(),
            Some(NullBuffer::from(vec![true, false])),
        ));
        let ColumnarValue::Array(result) = eval_array(array, false).unwrap() else {
            panic!("expected array result")
        };
        let result = result.as_primitive::<T>();
        assert_eq!(result.value(0), min);
        assert!(result.is_null(1));
    }

    #[test]
    fn test_legacy_mode_wraps_min_values() {
        assert_legacy_wraps_min::<Int8Type>(i8::MIN);
        assert_legacy_wraps_min::<Int16Type>(i16::MIN);
        assert_legacy_wraps_min::<Int32Type>(i32::MIN);
        assert_legacy_wraps_min::<Int64Type>(i64::MIN);
    }

    #[test]
    fn test_mixed_ordinary_values() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(-7), Some(0), Some(12), None]));

        // ANSI mode
        let ColumnarValue::Array(res_ansi) = eval_array(Arc::clone(&arr), true).unwrap() else {
            panic!("expected array result")
        };
        assert_eq!(
            res_ansi.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(7), Some(0), Some(-12), None])
        );

        // Legacy mode
        let ColumnarValue::Array(res_legacy) = eval_array(Arc::clone(&arr), false).unwrap() else {
            panic!("expected array result")
        };
        assert_eq!(
            res_legacy.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(7), Some(0), Some(-12), None])
        );
    }

    #[test]
    fn test_interval_month_day_nano_preserves_existing_dispatch() {
        let arr: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(1, 2, 3)),
            None,
        ]));
        let ColumnarValue::Array(res) = eval_array(arr, true).unwrap() else {
            panic!("expected array result")
        };
        let p = res.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(p.value(0), IntervalMonthDayNano::new(-1, -2, -3));
        assert!(p.is_null(1));
    }

    #[test]
    fn test_scalar_negation() {
        // Valid scalar
        let ColumnarValue::Scalar(res_valid) =
            eval_scalar(ScalarValue::Int32(Some(42)), true).unwrap()
        else {
            panic!("expected scalar result")
        };
        assert_eq!(res_valid, ScalarValue::Int32(Some(-42)));

        // Null scalar
        let ColumnarValue::Scalar(res_null) = eval_scalar(ScalarValue::Int32(None), true).unwrap()
        else {
            panic!("expected scalar result")
        };
        assert_eq!(res_null, ScalarValue::Int32(None));

        // MIN scalar overflows in ANSI, with the same messages as the array path
        for (scalar, from_type) in [
            (ScalarValue::Int8(Some(i8::MIN)), "byte"),
            (ScalarValue::Int16(Some(i16::MIN)), "short"),
            (ScalarValue::Int32(Some(i32::MIN)), "integer"),
            (ScalarValue::Int64(Some(i64::MIN)), "long"),
        ] {
            assert_spark_overflow(eval_scalar(scalar, true).unwrap_err(), from_type);
        }
    }

    #[test]
    fn test_map_neg_error_preserves_non_overflow_errors() {
        let err = map_neg_error(
            ArrowError::InvalidArgumentError("test custom error".to_string()),
            "integer",
        );
        assert!(
            matches!(&err, DataFusionError::ArrowError(inner, _)
                if matches!(inner.as_ref(), ArrowError::InvalidArgumentError(msg) if msg == "test custom error")),
            "expected the ArrowError to pass through unchanged, got: {err:?}"
        );
    }
}
