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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::{any::Any, sync::Arc};

use crate::execution::datafusion::expressions::{stats::StatsType, utils::down_cast_any_ref};
use arrow::{
    array::{ArrayRef, Float64Array},
    datatypes::{DataType, Field},
};
use datafusion::logical_expr::Accumulator;
use datafusion_common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion_physical_expr::{expressions::format_state_name, AggregateExpr, PhysicalExpr};

/// VAR_SAMP and VAR_POP aggregate expression
/// The implementation mostly is the same as the DataFusion's implementation. The reason
/// we have our own implementation is that DataFusion has UInt64 for state_field `count`,
/// while Spark has Double for count. Also we have added `null_on_divide_by_zero`
/// to be consistent with Spark's implementation.
#[derive(Debug)]
pub struct Variance {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
}

impl Variance {
    /// Create a new VARIANCE aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
        stats_type: StatsType,
        null_on_divide_by_zero: bool,
    ) -> Self {
        // the result of variance just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr,
            stats_type,
            null_on_divide_by_zero,
        }
    }
}

impl AggregateExpr for Variance {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VarianceAccumulator::try_new(
            self.stats_type,
            self.null_on_divide_by_zero,
        )?))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VarianceAccumulator::try_new(
            self.stats_type,
            self.null_on_divide_by_zero,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "mean"),
                DataType::Float64,
                true,
            ),
            Field::new(format_state_name(&self.name, "m2"), DataType::Float64, true),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Variance {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name && self.expr.eq(&x.expr) && self.stats_type == x.stats_type
            })
            .unwrap_or(false)
    }
}

/// An accumulator to compute variance
#[derive(Debug)]
pub struct VarianceAccumulator {
    m2: f64,
    mean: f64,
    count: f64,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
}

impl VarianceAccumulator {
    /// Creates a new `VarianceAccumulator`
    pub fn try_new(s_type: StatsType, null_on_divide_by_zero: bool) -> Result<Self> {
        Ok(Self {
            m2: 0_f64,
            mean: 0_f64,
            count: 0_f64,
            stats_type: s_type,
            null_on_divide_by_zero,
        })
    }

    pub fn get_count(&self) -> f64 {
        self.count
    }

    pub fn get_mean(&self) -> f64 {
        self.mean
    }

    pub fn get_m2(&self) -> f64 {
        self.m2
    }
}

impl Accumulator for VarianceAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.mean),
            ScalarValue::from(self.m2),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = downcast_value!(&values[0], Float64Array).iter().flatten();

        for value in arr {
            let new_count = self.count + 1.0;
            let delta1 = value - self.mean;
            let new_mean = delta1 / new_count + self.mean;
            let delta2 = value - new_mean;
            let new_m2 = self.m2 + delta1 * delta2;

            self.count += 1.0;
            self.mean = new_mean;
            self.m2 = new_m2;
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = downcast_value!(&values[0], Float64Array).iter().flatten();

        for value in arr {
            let new_count = self.count - 1.0;
            let delta1 = self.mean - value;
            let new_mean = delta1 / new_count + self.mean;
            let delta2 = new_mean - value;
            let new_m2 = self.m2 - delta1 * delta2;

            self.count -= 1.0;
            self.mean = new_mean;
            self.m2 = new_m2;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], Float64Array);
        let means = downcast_value!(states[1], Float64Array);
        let m2s = downcast_value!(states[2], Float64Array);

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0_f64 {
                continue;
            }
            let new_count = self.count + c;
            let new_mean = self.mean * self.count / new_count + means.value(i) * c / new_count;
            let delta = self.mean - means.value(i);
            let new_m2 = self.m2 + m2s.value(i) + delta * delta * self.count * c / new_count;

            self.count = new_count;
            self.mean = new_mean;
            self.m2 = new_m2;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let count = match self.stats_type {
            StatsType::Population => self.count,
            StatsType::Sample => {
                if self.count > 0.0 {
                    self.count - 1.0
                } else {
                    self.count
                }
            }
        };

        Ok(ScalarValue::Float64(match self.count {
            count if count == 0.0 => None,
            count if count == 1.0 => {
                if let StatsType::Population = self.stats_type {
                    Some(0.0)
                } else if self.null_on_divide_by_zero {
                    None
                } else {
                    Some(f64::NAN)
                }
            }
            _ => Some(self.m2 / count),
        }))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
