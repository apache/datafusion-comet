/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::{any::Any, sync::Arc};

use crate::execution::datafusion::expressions::stats::StatsType;
use arrow::{
    array::{ArrayRef, Float64Array},
    compute::cast,
    datatypes::{DataType, Field},
};
use datafusion::logical_expr::Accumulator;
use datafusion_common::{
    downcast_value, unwrap_or_internal_err, DataFusionError, Result, ScalarValue,
};
use datafusion_physical_expr::{
    aggregate::utils::down_cast_any_ref, expressions::format_state_name, AggregateExpr,
    PhysicalExpr,
};

/// COVAR_SAMP and COVAR_POP aggregate expression
/// The implementation mostly is the same as the DataFusion's implementation. The reason
/// we have our own implementation is that DataFusion has UInt64 for state_field count,
/// while Spark has Double for count.
#[derive(Debug, Clone)]
pub struct Covariance {
    name: String,
    expr1: Arc<dyn PhysicalExpr>,
    expr2: Arc<dyn PhysicalExpr>,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
}

impl Covariance {
    /// Create a new COVAR aggregate function
    pub fn new(
        expr1: Arc<dyn PhysicalExpr>,
        expr2: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
        stats_type: StatsType,
        null_on_divide_by_zero: bool,
    ) -> Self {
        // the result of covariance just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr1,
            expr2,
            stats_type,
            null_on_divide_by_zero,
        }
    }
}

impl AggregateExpr for Covariance {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CovarianceAccumulator::try_new(
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
                format_state_name(&self.name, "mean1"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "mean2"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "algo_const"),
                DataType::Float64,
                true,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr1.clone(), self.expr2.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Covariance {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.expr1.eq(&x.expr1)
                    && self.expr2.eq(&x.expr2)
                    && self.stats_type == x.stats_type
                    && self.null_on_divide_by_zero == x.null_on_divide_by_zero
            })
            .unwrap_or(false)
    }
}

/// An accumulator to compute covariance
#[derive(Debug)]
pub struct CovarianceAccumulator {
    algo_const: f64,
    mean1: f64,
    mean2: f64,
    count: f64,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
}

impl CovarianceAccumulator {
    /// Creates a new `CovarianceAccumulator`
    pub fn try_new(s_type: StatsType, null_on_divide_by_zero: bool) -> Result<Self> {
        Ok(Self {
            algo_const: 0_f64,
            mean1: 0_f64,
            mean2: 0_f64,
            count: 0_f64,
            stats_type: s_type,
            null_on_divide_by_zero,
        })
    }

    pub fn get_count(&self) -> f64 {
        self.count
    }

    pub fn get_mean1(&self) -> f64 {
        self.mean1
    }

    pub fn get_mean2(&self) -> f64 {
        self.mean2
    }

    pub fn get_algo_const(&self) -> f64 {
        self.algo_const
    }
}

impl Accumulator for CovarianceAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.mean1),
            ScalarValue::from(self.mean2),
            ScalarValue::from(self.algo_const),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values1 = &cast(&values[0], &DataType::Float64)?;
        let values2 = &cast(&values[1], &DataType::Float64)?;

        let mut arr1 = downcast_value!(values1, Float64Array).iter().flatten();
        let mut arr2 = downcast_value!(values2, Float64Array).iter().flatten();

        for i in 0..values1.len() {
            let value1 = if values1.is_valid(i) {
                arr1.next()
            } else {
                None
            };
            let value2 = if values2.is_valid(i) {
                arr2.next()
            } else {
                None
            };

            if value1.is_none() || value2.is_none() {
                continue;
            }

            let value1 = unwrap_or_internal_err!(value1);
            let value2 = unwrap_or_internal_err!(value2);
            let new_count = self.count + 1.0;
            let delta1 = value1 - self.mean1;
            let new_mean1 = delta1 / new_count + self.mean1;
            let delta2 = value2 - self.mean2;
            let new_mean2 = delta2 / new_count + self.mean2;
            let new_c = delta1 * (value2 - new_mean2) + self.algo_const;

            self.count += 1.0;
            self.mean1 = new_mean1;
            self.mean2 = new_mean2;
            self.algo_const = new_c;
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values1 = &cast(&values[0], &DataType::Float64)?;
        let values2 = &cast(&values[1], &DataType::Float64)?;
        let mut arr1 = downcast_value!(values1, Float64Array).iter().flatten();
        let mut arr2 = downcast_value!(values2, Float64Array).iter().flatten();

        for i in 0..values1.len() {
            let value1 = if values1.is_valid(i) {
                arr1.next()
            } else {
                None
            };
            let value2 = if values2.is_valid(i) {
                arr2.next()
            } else {
                None
            };

            if value1.is_none() || value2.is_none() {
                continue;
            }

            let value1 = unwrap_or_internal_err!(value1);
            let value2 = unwrap_or_internal_err!(value2);

            let new_count = self.count - 1.0;
            let delta1 = self.mean1 - value1;
            let new_mean1 = delta1 / new_count + self.mean1;
            let delta2 = self.mean2 - value2;
            let new_mean2 = delta2 / new_count + self.mean2;
            let new_c = self.algo_const - delta1 * (new_mean2 - value2);

            self.count -= 1.0;
            self.mean1 = new_mean1;
            self.mean2 = new_mean2;
            self.algo_const = new_c;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], Float64Array);
        let means1 = downcast_value!(states[1], Float64Array);
        let means2 = downcast_value!(states[2], Float64Array);
        let cs = downcast_value!(states[3], Float64Array);

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0.0 {
                continue;
            }
            let new_count = self.count + c;
            let new_mean1 = self.mean1 * self.count / new_count + means1.value(i) * c / new_count;
            let new_mean2 = self.mean2 * self.count / new_count + means2.value(i) * c / new_count;
            let delta1 = self.mean1 - means1.value(i);
            let delta2 = self.mean2 - means2.value(i);
            let new_c =
                self.algo_const + cs.value(i) + delta1 * delta2 * self.count * c / new_count;

            self.count = new_count;
            self.mean1 = new_mean1;
            self.mean2 = new_mean2;
            self.algo_const = new_c;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0.0 {
            return Ok(ScalarValue::Float64(None));
        }

        let count = match self.stats_type {
            StatsType::Population => self.count,
            StatsType::Sample if self.count > 1.0 => self.count - 1.0,
            StatsType::Sample => {
                // self.count == 1.0
                return if self.null_on_divide_by_zero {
                    Ok(ScalarValue::Float64(None))
                } else {
                    Ok(ScalarValue::Float64(Some(f64::NAN)))
                };
            }
        };

        Ok(ScalarValue::Float64(Some(self.algo_const / count)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
