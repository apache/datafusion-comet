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

use arrow::compute::{and, filter, is_not_null};

use std::{any::Any, sync::Arc};

use crate::execution::datafusion::expressions::{
    covariance::CovarianceAccumulator, stats::StatsType, stddev::StddevAccumulator,
    utils::down_cast_any_ref,
};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};
use datafusion::logical_expr::Accumulator;
use datafusion_common::{Result, ScalarValue};
use datafusion_physical_expr::{expressions::format_state_name, AggregateExpr, PhysicalExpr};

/// CORR aggregate expression
/// The implementation mostly is the same as the DataFusion's implementation. The reason
/// we have our own implementation is that DataFusion has UInt64 for state_field `count`,
/// while Spark has Double for count. Also we have added `null_on_divide_by_zero`
/// to be consistent with Spark's implementation.
#[derive(Debug)]
pub struct Correlation {
    name: String,
    expr1: Arc<dyn PhysicalExpr>,
    expr2: Arc<dyn PhysicalExpr>,
    null_on_divide_by_zero: bool,
}

impl Correlation {
    pub fn new(
        expr1: Arc<dyn PhysicalExpr>,
        expr2: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
        null_on_divide_by_zero: bool,
    ) -> Self {
        // the result of correlation just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr1,
            expr2,
            null_on_divide_by_zero,
        }
    }
}

impl AggregateExpr for Correlation {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CorrelationAccumulator::try_new(
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
            Field::new(
                format_state_name(&self.name, "m2_1"),
                DataType::Float64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "m2_2"),
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

impl PartialEq<dyn Any> for Correlation {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.expr1.eq(&x.expr1)
                    && self.expr2.eq(&x.expr2)
                    && self.null_on_divide_by_zero == x.null_on_divide_by_zero
            })
            .unwrap_or(false)
    }
}

/// An accumulator to compute correlation
#[derive(Debug)]
pub struct CorrelationAccumulator {
    covar: CovarianceAccumulator,
    stddev1: StddevAccumulator,
    stddev2: StddevAccumulator,
    null_on_divide_by_zero: bool,
}

impl CorrelationAccumulator {
    /// Creates a new `CorrelationAccumulator`
    pub fn try_new(null_on_divide_by_zero: bool) -> Result<Self> {
        Ok(Self {
            covar: CovarianceAccumulator::try_new(StatsType::Population, null_on_divide_by_zero)?,
            stddev1: StddevAccumulator::try_new(StatsType::Population, null_on_divide_by_zero)?,
            stddev2: StddevAccumulator::try_new(StatsType::Population, null_on_divide_by_zero)?,
            null_on_divide_by_zero,
        })
    }
}

impl Accumulator for CorrelationAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.covar.get_count()),
            ScalarValue::from(self.covar.get_mean1()),
            ScalarValue::from(self.covar.get_mean2()),
            ScalarValue::from(self.covar.get_algo_const()),
            ScalarValue::from(self.stddev1.get_m2()),
            ScalarValue::from(self.stddev2.get_m2()),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = if values[0].null_count() != 0 || values[1].null_count() != 0 {
            let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            let values1 = filter(&values[0], &mask)?;
            let values2 = filter(&values[1], &mask)?;

            vec![values1, values2]
        } else {
            values.to_vec()
        };

        if !values[0].is_empty() && !values[1].is_empty() {
            self.covar.update_batch(&values)?;
            self.stddev1.update_batch(&values[0..1])?;
            self.stddev2.update_batch(&values[1..2])?;
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = if values[0].null_count() != 0 || values[1].null_count() != 0 {
            let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            let values1 = filter(&values[0], &mask)?;
            let values2 = filter(&values[1], &mask)?;

            vec![values1, values2]
        } else {
            values.to_vec()
        };

        self.covar.retract_batch(&values)?;
        self.stddev1.retract_batch(&values[0..1])?;
        self.stddev2.retract_batch(&values[1..2])?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states_c = [
            states[0].clone(),
            states[1].clone(),
            states[2].clone(),
            states[3].clone(),
        ];
        let states_s1 = [states[0].clone(), states[1].clone(), states[4].clone()];
        let states_s2 = [states[0].clone(), states[2].clone(), states[5].clone()];

        if states[0].len() > 0 && states[1].len() > 0 && states[2].len() > 0 {
            self.covar.merge_batch(&states_c)?;
            self.stddev1.merge_batch(&states_s1)?;
            self.stddev2.merge_batch(&states_s2)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let covar = self.covar.evaluate()?;
        let stddev1 = self.stddev1.evaluate()?;
        let stddev2 = self.stddev2.evaluate()?;

        match (covar, stddev1, stddev2) {
            (
                ScalarValue::Float64(Some(c)),
                ScalarValue::Float64(Some(s1)),
                ScalarValue::Float64(Some(s2)),
            ) if s1 != 0.0 && s2 != 0.0 => Ok(ScalarValue::Float64(Some(c / (s1 * s2)))),
            _ if self.null_on_divide_by_zero => Ok(ScalarValue::Float64(None)),
            _ => {
                if self.covar.get_count() == 1.0 {
                    return Ok(ScalarValue::Float64(Some(f64::NAN)));
                }
                Ok(ScalarValue::Float64(None))
            }
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.covar) + self.covar.size()
            - std::mem::size_of_val(&self.stddev1)
            + self.stddev1.size()
            - std::mem::size_of_val(&self.stddev2)
            + self.stddev2.size()
    }
}
