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

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use arrow::compute::kernels::numeric::neg_wrapping;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use datafusion_common::{Result, ScalarValue};
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_physical_expr::aggregate::utils::down_cast_any_ref;
use datafusion_physical_expr::sort_properties::SortProperties;
use crate::errors::CometError;
use crate::execution::datafusion::expressions::cast::EvalMode;

pub fn create_negate_expr(expr: Arc<dyn PhysicalExpr>, eval_mode: EvalMode) -> Result<Arc<dyn PhysicalExpr>, CometError> {
    Ok(Arc::new(NegativeExpr::new(expr, eval_mode)))
}

/// Negative expression
#[derive(Debug, Hash)]
pub struct NegativeExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
    eval_mode: EvalMode,
}

impl NegativeExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>, eval_mode: EvalMode) -> Self {
        Self { arg, eval_mode }
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

impl PhysicalExpr for NegativeExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.arg.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;        
        match arg {
            ColumnarValue::Array(array) => {
                if self.eval_mode == EvalMode::Ansi {
                    match array.data_type() {
                        DataType::Int8 => {
                            let int_array = array.as_any().downcast_ref::<arrow::array::Int8Array>().unwrap();
                            for i in 0..int_array.len() {
                                if int_array.value(i) <= i8::MIN || int_array.value(i) >= i8::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "integer".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::Int16 => {
                            let int_array = array.as_any().downcast_ref::<arrow::array::Int16Array>().unwrap();
                            for i in 0..int_array.len() {
                                if int_array.value(i) <= i16::MIN || int_array.value(i) >= i16::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "integer".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::Int32 => {
                            let int_array = array.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
                            for i in 0..int_array.len() {
                                if int_array.value(i) <= i32::MIN || int_array.value(i) >= i32::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "integer".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::Int64 => {
                            let int_array = array.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
                            for i in 0..int_array.len() {
                                if int_array.value(i) <= i64::MIN || int_array.value(i) >= i64::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "integer".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::UInt8 => {
                            let int_array = array.as_any().downcast_ref::<arrow::array::UInt8Array>().unwrap();
                            for i in 0..int_array.len() {
                                if int_array.value(i) <= u8::MIN || int_array.value(i) >= u8::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "integer".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::UInt16 => {
                            let int_array = array.as_any().downcast_ref::<arrow::array::UInt16Array>().unwrap();
                            for i in 0..int_array.len() {
                                if int_array.value(i) <= u16::MIN || int_array.value(i) >= u16::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "integer".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::UInt32 => {
                            let int_array = array.as_any().downcast_ref::<arrow::array::UInt32Array>().unwrap();
                            for i in 0..int_array.len() {
                                if int_array.value(i) <= u32::MIN || int_array.value(i) >= u32::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "integer".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::UInt64 => {
                            let int_array = array.as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
                            for i in 0..int_array.len() {
                                if int_array.value(i) <= u64::MIN || int_array.value(i) >= u64::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "integer".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::Float16 => {
                            let float_array = array.as_any().downcast_ref::<arrow::array::Float16Array>().unwrap();
                            for i in 0..float_array.len() {
                                if float_array.value(i) <= half::f16::MIN || float_array.value(i) >= half::f16::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "float".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::Float32 => {
                            let float_array = array.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap();
                            for i in 0..float_array.len() {
                                if float_array.value(i) <= f32::MIN || float_array.value(i) >= f32::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "float".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::Float64 => {
                            let float_array = array.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
                            for i in 0..float_array.len() {
                                if float_array.value(i) <= f64::MIN || float_array.value(i) >= f64::MAX {
                                    return Err(CometError::ArithmeticOverflow{
                                        from_type: "float".to_string(),
                                    }.into());
                                }
                            }
                        }
                        DataType::Interval(value) => {
                            // Downcast the array to the appropriate interval type
                            match value {
                                arrow::datatypes::IntervalUnit::YearMonth => {
                                    let interval_array = array.as_any().downcast_ref::<arrow::array::IntervalYearMonthArray>().unwrap();
                                    for i in 0..interval_array.len() {
                                        if interval_array.value(i) <= i32::MIN || interval_array.value(i) >= i32::MAX {
                                            return Err(CometError::ArithmeticOverflow{
                                                from_type: "interval".to_string(),
                                            }.into());
                                        }
                                    }
                                }
                                arrow::datatypes::IntervalUnit::DayTime => {
                                    let interval_array = array.as_any().downcast_ref::<arrow::array::IntervalDayTimeArray>().unwrap();
                                    for i in 0..interval_array.len() {
                                        if interval_array.value(i) <= i64::MIN || interval_array.value(i) >= i64::MAX {
                                            return Err(CometError::ArithmeticOverflow{
                                                from_type: "interval".to_string(),
                                            }.into());
                                        }
                                    }
                                }
                                arrow::datatypes::IntervalUnit::MonthDayNano => {
                                    let interval_array = array.as_any().downcast_ref::<arrow::array::IntervalMonthDayNanoArray>().unwrap();
                                    for i in 0..interval_array.len() {
                                        if interval_array.value(i) <= i128::MIN || interval_array.value(i) >= i128::MAX {
                                            return Err(CometError::ArithmeticOverflow{
                                                from_type: "interval".to_string(),
                                            }.into());
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            unimplemented!("Overflow error: cannot negate value of type {:?}", array.data_type());
                        }
                    }
                }
                let result = neg_wrapping(array.as_ref())?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                if self.eval_mode == EvalMode::Ansi {
                    match scalar {
                        ScalarValue::Int8(value) => {
                            if value <= Some(i8::MIN) || value >= Some(i8::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "integer".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::Int16(value) => {
                            if value <= Some(i16::MIN) || value >= Some(i16::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "integer".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::Int32(value) => {
                            if value <= Some(i32::MIN) || value >= Some(i32::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "integer".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::Int64(value) => {
                            if value <= Some(i64::MIN) || value >= Some(i64::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "integer".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::UInt8(value) => {
                            if value <= Some(u8::MIN) || value >= Some(u8::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "integer".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::UInt16(value) => {
                            if value <= Some(u16::MIN) || value >= Some(u16::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "integer".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::UInt32(value) => {
                            if value <= Some(u32::MIN) || value >= Some(u32::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "integer".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::UInt64(value) => {
                            if value <= Some(u64::MIN) || value >= Some(u64::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "integer".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::Float32(value) => {
                            if value <= Some(f32::MIN) || value >= Some(f32::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "float".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::Float64(value) => {
                            if value <= Some(f64::MIN) || value >= Some(f64::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "float".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::IntervalDayTime(value) => {
                            if value <= Some(i64::MIN) || value >= Some(i64::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "interval".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::IntervalYearMonth(value) => {
                            if value <= Some(i32::MIN) || value >= Some(i32::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "interval".to_string(),
                                }.into());
                            }
                        }
                        ScalarValue::IntervalMonthDayNano(value) => {
                            if value <= Some(i128::MIN) || value >= Some(i128::MAX) {
                                return Err(CometError::ArithmeticOverflow{
                                    from_type: "interval".to_string(),
                                }.into());
                            }
                        }
                        _ => {
                            unimplemented!("Overflow error: cannot negate value of type {:?}", scalar);
                        }
                    }
                }
                Ok(ColumnarValue::Scalar((scalar.arithmetic_negate())?))
            }
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.arg.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(NegativeExpr::new(children[0].clone(), self.eval_mode)))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
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
        let negated_interval = Interval::try_new(
            interval.upper().arithmetic_negate()?,
            interval.lower().arithmetic_negate()?,
        )?;

        Ok(child_interval
            .intersect(negated_interval)?
            .map(|result| vec![result]))
    }

    /// The ordering of a [`NegativeExpr`] is simply the reverse of its child.
    fn get_ordering(&self, children: &[SortProperties]) -> SortProperties {
        -children[0]
    }
}

impl PartialEq<dyn Any> for NegativeExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.arg.eq(&x.arg))
            .unwrap_or(false)
    }
}