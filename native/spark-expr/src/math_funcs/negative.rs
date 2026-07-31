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
use arrow::datatypes::{DataType, Schema};
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

fn map_neg_error(err: ArrowError, type_name: &'static str) -> DataFusionError {
    match err {
        ArrowError::ArithmeticOverflow(_) => arithmetic_overflow_error(type_name).into(),
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

        // overflow checks only apply in ANSI mode
        // datatypes supported are byte, short, integer, long, float, interval
        match arg {
            ColumnarValue::Array(array) => {
                if self.fail_on_error {
                    match array.data_type() {
                        DataType::Int8 => {
                            let result =
                                neg(array.as_ref()).map_err(|e| map_neg_error(e, "-128 caused"))?;
                            Ok(ColumnarValue::Array(result))
                        }
                        DataType::Int16 => {
                            let result = neg(array.as_ref())
                                .map_err(|e| map_neg_error(e, "-32768 caused"))?;
                            Ok(ColumnarValue::Array(result))
                        }
                        DataType::Int32 => {
                            let result =
                                neg(array.as_ref()).map_err(|e| map_neg_error(e, "integer"))?;
                            Ok(ColumnarValue::Array(result))
                        }
                        DataType::Int64 => {
                            let result =
                                neg(array.as_ref()).map_err(|e| map_neg_error(e, "long"))?;
                            Ok(ColumnarValue::Array(result))
                        }
                        DataType::Interval(value) => match value {
                            arrow::datatypes::IntervalUnit::YearMonth
                            | arrow::datatypes::IntervalUnit::DayTime => {
                                let result = neg(array.as_ref())
                                    .map_err(|e| map_neg_error(e, "interval"))?;
                                Ok(ColumnarValue::Array(result))
                            }
                            arrow::datatypes::IntervalUnit::MonthDayNano => {
                                // Preserve the existing MonthDayNano dispatch.
                                let result = neg_wrapping(array.as_ref())?;
                                Ok(ColumnarValue::Array(result))
                            }
                        },
                        _ => {
                            // Overflow checks are not supported for other datatypes
                            let result = neg_wrapping(array.as_ref())?;
                            Ok(ColumnarValue::Array(result))
                        }
                    }
                } else {
                    let result = neg_wrapping(array.as_ref())?;
                    Ok(ColumnarValue::Array(result))
                }
            }
            ColumnarValue::Scalar(scalar) => {
                if self.fail_on_error {
                    match scalar {
                        ScalarValue::Int8(Some(i8::MIN)) => {
                            return Err(arithmetic_overflow_error(" caused").into());
                        }
                        ScalarValue::Int16(Some(i16::MIN)) => {
                            return Err(arithmetic_overflow_error(" caused").into());
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

    #[test]
    fn test_ansi_null_slot_with_min_values_does_not_overflow() {
        let nulls = NullBuffer::from(vec![false, true]);

        // Int8
        let arr_i8: ArrayRef =
            Arc::new(Int8Array::new(vec![i8::MIN, 7].into(), Some(nulls.clone())));
        let ColumnarValue::Array(res_i8) = eval_array(arr_i8, true).unwrap() else {
            panic!()
        };
        let p_i8 = res_i8.as_primitive::<Int8Type>();
        assert!(p_i8.is_null(0));
        assert_eq!(p_i8.value(1), -7);

        // Int16
        let arr_i16: ArrayRef = Arc::new(Int16Array::new(
            vec![i16::MIN, 7].into(),
            Some(nulls.clone()),
        ));
        let ColumnarValue::Array(res_i16) = eval_array(arr_i16, true).unwrap() else {
            panic!()
        };
        let p_i16 = res_i16.as_primitive::<Int16Type>();
        assert!(p_i16.is_null(0));
        assert_eq!(p_i16.value(1), -7);

        // Int32
        let arr_i32: ArrayRef = Arc::new(Int32Array::new(
            vec![i32::MIN, 7].into(),
            Some(nulls.clone()),
        ));
        let ColumnarValue::Array(res_i32) = eval_array(arr_i32, true).unwrap() else {
            panic!()
        };
        let p_i32 = res_i32.as_primitive::<Int32Type>();
        assert!(p_i32.is_null(0));
        assert_eq!(p_i32.value(1), -7);

        // Int64
        let arr_i64: ArrayRef = Arc::new(Int64Array::new(
            vec![i64::MIN, 7].into(),
            Some(nulls.clone()),
        ));
        let ColumnarValue::Array(res_i64) = eval_array(arr_i64, true).unwrap() else {
            panic!()
        };
        let p_i64 = res_i64.as_primitive::<Int64Type>();
        assert!(p_i64.is_null(0));
        assert_eq!(p_i64.value(1), -7);
    }

    #[test]
    fn test_ansi_valid_min_values_raise_exact_spark_overflow_errors() {
        let arr_i8: ArrayRef = Arc::new(Int8Array::from(vec![i8::MIN]));
        assert_spark_overflow(eval_array(arr_i8, true).unwrap_err(), "-128 caused");

        let arr_i16: ArrayRef = Arc::new(Int16Array::from(vec![i16::MIN]));
        assert_spark_overflow(eval_array(arr_i16, true).unwrap_err(), "-32768 caused");

        let arr_i32: ArrayRef = Arc::new(Int32Array::from(vec![i32::MIN]));
        assert_spark_overflow(eval_array(arr_i32, true).unwrap_err(), "integer");

        let arr_i64: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN]));
        assert_spark_overflow(eval_array(arr_i64, true).unwrap_err(), "long");

        let arr_ym: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![i32::MIN]));
        assert_spark_overflow(eval_array(arr_ym, true).unwrap_err(), "interval");

        let arr_dt: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::MIN]));
        assert_spark_overflow(eval_array(arr_dt, true).unwrap_err(), "interval");
    }

    #[test]
    fn test_ansi_interval_day_time_component_overflow_and_null_controls() {
        let nulls = NullBuffer::from(vec![false, true]);

        // valid (i32::MIN, 0) -> ANSI overflow
        let valid_days_min: ArrayRef =
            Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
                i32::MIN,
                0,
            )]));
        assert_spark_overflow(eval_array(valid_days_min, true).unwrap_err(), "interval");

        // valid (0, i32::MIN) -> ANSI overflow
        let valid_ms_min: ArrayRef =
            Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
                0,
                i32::MIN,
            )]));
        assert_spark_overflow(eval_array(valid_ms_min, true).unwrap_err(), "interval");

        // null-backed (i32::MIN, 0) -> no error, remains null
        let null_days_min: ArrayRef = Arc::new(IntervalDayTimeArray::new(
            vec![
                IntervalDayTime::new(i32::MIN, 0),
                IntervalDayTime::new(1, 2),
            ]
            .into(),
            Some(nulls.clone()),
        ));
        let ColumnarValue::Array(res_days) = eval_array(null_days_min, true).unwrap() else {
            panic!()
        };
        let p_days = res_days.as_primitive::<IntervalDayTimeType>();
        assert!(p_days.is_null(0));
        assert_eq!(p_days.value(1), IntervalDayTime::new(-1, -2));

        // null-backed (0, i32::MIN) -> no error, remains null
        let null_ms_min: ArrayRef = Arc::new(IntervalDayTimeArray::new(
            vec![
                IntervalDayTime::new(0, i32::MIN),
                IntervalDayTime::new(1, 2),
            ]
            .into(),
            Some(nulls),
        ));
        let ColumnarValue::Array(res_ms) = eval_array(null_ms_min, true).unwrap() else {
            panic!()
        };
        let p_ms = res_ms.as_primitive::<IntervalDayTimeType>();
        assert!(p_ms.is_null(0));
        assert_eq!(p_ms.value(1), IntervalDayTime::new(-1, -2));
    }

    #[test]
    fn test_legacy_mode_wraps_min_values() {
        // Int8
        let arr_i8: ArrayRef = Arc::new(Int8Array::from(vec![Some(i8::MIN), None]));
        let ColumnarValue::Array(res_i8) = eval_array(arr_i8, false).unwrap() else {
            panic!()
        };
        let p_i8 = res_i8.as_primitive::<Int8Type>();
        assert_eq!(p_i8.value(0), i8::MIN);
        assert!(p_i8.is_null(1));

        // Int16
        let arr_i16: ArrayRef = Arc::new(Int16Array::from(vec![Some(i16::MIN), None]));
        let ColumnarValue::Array(res_i16) = eval_array(arr_i16, false).unwrap() else {
            panic!()
        };
        let p_i16 = res_i16.as_primitive::<Int16Type>();
        assert_eq!(p_i16.value(0), i16::MIN);
        assert!(p_i16.is_null(1));

        // Int32
        let arr_i32: ArrayRef = Arc::new(Int32Array::from(vec![Some(i32::MIN), None]));
        let ColumnarValue::Array(res_i32) = eval_array(arr_i32, false).unwrap() else {
            panic!()
        };
        let p_i32 = res_i32.as_primitive::<Int32Type>();
        assert_eq!(p_i32.value(0), i32::MIN);
        assert!(p_i32.is_null(1));

        // Int64
        let arr_i64: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MIN), None]));
        let ColumnarValue::Array(res_i64) = eval_array(arr_i64, false).unwrap() else {
            panic!()
        };
        let p_i64 = res_i64.as_primitive::<Int64Type>();
        assert_eq!(p_i64.value(0), i64::MIN);
        assert!(p_i64.is_null(1));
    }

    #[test]
    fn test_mixed_ordinary_values() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(-7), Some(0), Some(12), None]));

        // ANSI mode
        let ColumnarValue::Array(res_ansi) = eval_array(Arc::clone(&arr), true).unwrap() else {
            panic!()
        };
        assert_eq!(
            res_ansi.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(7), Some(0), Some(-12), None])
        );

        // Legacy mode
        let ColumnarValue::Array(res_legacy) = eval_array(Arc::clone(&arr), false).unwrap() else {
            panic!()
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
            panic!()
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
            panic!()
        };
        assert_eq!(res_valid, ScalarValue::Int32(Some(-42)));

        // Null scalar
        let ColumnarValue::Scalar(res_null) = eval_scalar(ScalarValue::Int32(None), true).unwrap()
        else {
            panic!()
        };
        assert_eq!(res_null, ScalarValue::Int32(None));

        // MIN scalar overflow in ANSI
        assert_spark_overflow(
            eval_scalar(ScalarValue::Int32(Some(i32::MIN)), true).unwrap_err(),
            "integer",
        );
    }

    #[test]
    fn test_map_neg_error_preserves_non_overflow_errors() {
        let err = ArrowError::InvalidArgumentError("test custom error".to_string());
        let df_err = map_neg_error(err, "integer");
        match df_err {
            DataFusionError::ArrowError(boxed_err, _) => match *boxed_err {
                ArrowError::InvalidArgumentError(msg) => {
                    assert_eq!(msg, "test custom error");
                }
                _ => panic!("expected InvalidArgumentError, got {:?}", boxed_err),
            },
            _ => panic!("expected ArrowError, got {:?}", df_err),
        }
    }
}
