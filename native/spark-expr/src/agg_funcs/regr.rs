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

//! Spark-compatible `regr_*` simple linear regression aggregates.
//!
//! For `regr_*(y, x)`, the first argument `y` is the dependent variable and the
//! second argument `x` is the independent variable. Only rows where both `y`
//! and `x` are non-null take part in the aggregation.
//!
//! Each function is composed from Comet's Spark-compatible
//! [`CovarianceAccumulator`] and [`VarianceAccumulator`] so that the partial
//! aggregation state Comet emits matches the buffer layout Spark's planner
//! expects between the partial and final aggregation stages:
//!
//! | function                  | Spark aggregate        | state fields |
//! |---------------------------|------------------------|--------------|
//! | `regr_sxx` / `regr_syy`   | `RegrReplacement`      | 3            |
//! | `regr_sxy`                | `Covariance`           | 4            |
//! | `regr_r2`                 | `PearsonCorrelation`   | 6            |
//! | `regr_slope`/`_intercept` | composite cov + var    | 7            |

use arrow::array::{Array, ArrayRef};
use arrow::compute::{and, filter, is_not_null};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::expressions::format_state_name;
use datafusion::physical_expr::expressions::StatsType;
use std::sync::Arc;

use crate::agg_funcs::covariance::CovarianceAccumulator;
use crate::agg_funcs::variance::VarianceAccumulator;

/// The kind of linear-regression statistic to compute.
///
/// `regr_count`, `regr_avgx` and `regr_avgy` are rewritten by Spark to
/// `Count`/`Average`, so they never reach this accumulator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegrType {
    /// Slope of the least-squares regression line: `cov_pop(x, y) / var_pop(x)`.
    Slope,
    /// Intercept of the regression line: `mean_y - slope * mean_x`.
    Intercept,
    /// Coefficient of determination (R squared).
    R2,
    /// Sum of squares of the independent variable: `m2(x)`.
    SXX,
    /// Sum of squares of the dependent variable: `m2(y)`.
    SYY,
    /// Sum of products of the paired deviations: `ck`.
    SXY,
}

/// Comet's Spark-compatible `regr_*` aggregate UDF.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Regr {
    name: String,
    signature: Signature,
    regr_type: RegrType,
}

impl Regr {
    pub fn new(regr_type: RegrType, name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
            regr_type,
        }
    }

    /// Number of `m2` / covariance sub-states this statistic carries, which
    /// determines the partial aggregation buffer width.
    fn num_state_fields(&self) -> usize {
        match self.regr_type {
            // RegrReplacement (CentralMomentAgg): count, mean, m2
            RegrType::SXX | RegrType::SYY => 3,
            // Covariance: count, mean1, mean2, algo_const
            RegrType::SXY => 4,
            // PearsonCorrelation: count, mean1, mean2, algo_const, m2_1, m2_2
            RegrType::R2 => 6,
            // CovPopulation (4) ++ VariancePop (3)
            RegrType::Slope | RegrType::Intercept => 7,
        }
    }
}

impl AggregateUDFImpl for Regr {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let acc: Box<dyn Accumulator> = match self.regr_type {
            RegrType::SXX | RegrType::SYY => Box::new(RegrMomentAccumulator::try_new()?),
            RegrType::SXY => Box::new(RegrCovAccumulator::try_new()?),
            RegrType::R2 => Box::new(RegrR2Accumulator::try_new()?),
            RegrType::Slope => Box::new(RegrLineAccumulator::try_new(false)?),
            RegrType::Intercept => Box::new(RegrLineAccumulator::try_new(true)?),
        };
        Ok(acc)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let names: &[&str] = match self.num_state_fields() {
            3 => &["count", "mean", "m2"],
            4 => &["count", "mean1", "mean2", "algo_const"],
            6 => &["count", "mean1", "mean2", "algo_const", "m2_1", "m2_2"],
            _ => &[
                "count",
                "mean1",
                "mean2",
                "algo_const",
                "var_count",
                "var_mean",
                "var_m2",
            ],
        };
        Ok(names
            .iter()
            .map(|n| {
                Arc::new(Field::new(
                    format_state_name(args.name, n),
                    DataType::Float64,
                    true,
                ))
            })
            .collect())
    }
}

/// Keep only the rows where both inputs are non-null, mirroring Spark's
/// "regr functions operate on non-null pairs" rule.
fn filter_pairs(values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    if values[0].null_count() == 0 && values[1].null_count() == 0 {
        return Ok(values.to_vec());
    }
    let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
    Ok(vec![filter(&values[0], &mask)?, filter(&values[1], &mask)?])
}

/// `regr_sxx` / `regr_syy`: `m2` of the (already null-filtered) child column.
/// Mirrors Spark's `RegrReplacement` (a `CentralMomentAgg` whose evaluate is
/// `m2`). State layout matches `VarianceAccumulator` (count, mean, m2).
#[derive(Debug)]
struct RegrMomentAccumulator {
    var: VarianceAccumulator,
}

impl RegrMomentAccumulator {
    fn try_new() -> Result<Self> {
        Ok(Self {
            var: VarianceAccumulator::try_new(StatsType::Population, false)?,
        })
    }
}

impl Accumulator for RegrMomentAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.var.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // child1 == child2 (Spark's RegrReplacement is single-arg); use one.
        self.var.update_batch(&values[0..1])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.var.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.var.get_count() == 0.0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some(self.var.get_m2())))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.var) + self.var.size()
    }
}

/// `regr_sxy`: the co-moment `ck` of the non-null pairs. Mirrors Spark's
/// `RegrSXY` (a population `Covariance` whose evaluate is `ck`). State layout
/// matches `CovarianceAccumulator`.
#[derive(Debug)]
struct RegrCovAccumulator {
    covar: CovarianceAccumulator,
}

impl RegrCovAccumulator {
    fn try_new() -> Result<Self> {
        Ok(Self {
            covar: CovarianceAccumulator::try_new(StatsType::Population, false)?,
        })
    }
}

impl Accumulator for RegrCovAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.covar.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // CovarianceAccumulator already skips pairs where either side is null.
        self.covar.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.covar.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.covar.get_count() == 0.0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some(self.covar.get_algo_const())))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.covar) + self.covar.size()
    }
}

/// `regr_r2`: the coefficient of determination. Mirrors Spark's `RegrR2`
/// (a `PearsonCorrelation`). State layout matches `CorrelationAccumulator`:
/// count, mean1, mean2, algo_const, m2(y), m2(x).
///
/// Spark's evaluate differs from DataFusion in one degenerate case: when the
/// dependent variable `y` is constant but `x` varies, Spark returns `1.0`
/// (a horizontal line is a perfect fit) where DataFusion returns `null`.
#[derive(Debug)]
struct RegrR2Accumulator {
    covar: CovarianceAccumulator,
    var_y: VarianceAccumulator,
    var_x: VarianceAccumulator,
}

impl RegrR2Accumulator {
    fn try_new() -> Result<Self> {
        Ok(Self {
            covar: CovarianceAccumulator::try_new(StatsType::Population, false)?,
            var_y: VarianceAccumulator::try_new(StatsType::Population, false)?,
            var_x: VarianceAccumulator::try_new(StatsType::Population, false)?,
        })
    }
}

impl Accumulator for RegrR2Accumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let c = self.covar.state()?;
        Ok(vec![
            c[0].clone(),
            c[1].clone(),
            c[2].clone(),
            c[3].clone(),
            ScalarValue::from(self.var_y.get_m2()),
            ScalarValue::from(self.var_x.get_m2()),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // values[0] = y (dependent), values[1] = x (independent)
        let pairs = filter_pairs(values)?;
        if pairs[0].is_empty() {
            return Ok(());
        }
        self.covar.update_batch(&pairs)?;
        self.var_y.update_batch(&pairs[0..1])?;
        self.var_x.update_batch(&pairs[1..2])?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // state: [count, mean1, mean2, algo_const, m2_y, m2_x]
        let covar_state = [
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[2]),
            Arc::clone(&states[3]),
        ];
        let var_y_state = [
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[4]),
        ];
        let var_x_state = [
            Arc::clone(&states[0]),
            Arc::clone(&states[2]),
            Arc::clone(&states[5]),
        ];
        self.covar.merge_batch(&covar_state)?;
        self.var_y.merge_batch(&var_y_state)?;
        self.var_x.merge_batch(&var_x_state)?;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let count = self.covar.get_count();
        let m2_x = self.var_x.get_m2();
        let m2_y = self.var_y.get_m2();
        if count <= 1.0 || m2_x == 0.0 {
            // independent variable has no spread -> undefined
            Ok(ScalarValue::Float64(None))
        } else if m2_y == 0.0 {
            // dependent variable is constant -> perfect horizontal fit
            Ok(ScalarValue::Float64(Some(1.0)))
        } else {
            let ck = self.covar.get_algo_const();
            Ok(ScalarValue::Float64(Some((ck * ck) / (m2_x * m2_y))))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.covar) + self.covar.size()
            - std::mem::size_of_val(&self.var_y)
            + self.var_y.size()
            - std::mem::size_of_val(&self.var_x)
            + self.var_x.size()
    }
}

/// `regr_slope` / `regr_intercept`. Mirrors Spark's `RegrSlope` / `RegrIntercept`
/// declarative aggregates, whose buffer is `CovPopulation(x, y)` (4 fields)
/// followed by `VariancePop(x)` (3 fields). The covariance is fed `(x, y)` so
/// that `mean1 = mean(x)` and `mean2 = mean(y)`.
#[derive(Debug)]
struct RegrLineAccumulator {
    covar: CovarianceAccumulator,
    var_x: VarianceAccumulator,
    intercept: bool,
}

impl RegrLineAccumulator {
    fn try_new(intercept: bool) -> Result<Self> {
        Ok(Self {
            covar: CovarianceAccumulator::try_new(StatsType::Population, false)?,
            var_x: VarianceAccumulator::try_new(StatsType::Population, false)?,
            intercept,
        })
    }
}

impl Accumulator for RegrLineAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut s = self.covar.state()?;
        s.extend(self.var_x.state()?);
        Ok(s)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // values[0] = y (dependent), values[1] = x (independent)
        let pairs = filter_pairs(values)?;
        if pairs[0].is_empty() {
            return Ok(());
        }
        // Feed covariance as (x, y) so mean1 = mean(x), mean2 = mean(y).
        let cov_input = [Arc::clone(&pairs[1]), Arc::clone(&pairs[0])];
        self.covar.update_batch(&cov_input)?;
        self.var_x.update_batch(&pairs[1..2])?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let covar_state = [
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[2]),
            Arc::clone(&states[3]),
        ];
        let var_x_state = [
            Arc::clone(&states[4]),
            Arc::clone(&states[5]),
            Arc::clone(&states[6]),
        ];
        self.covar.merge_batch(&covar_state)?;
        self.var_x.merge_batch(&var_x_state)?;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let m2_x = self.var_x.get_m2();
        if m2_x == 0.0 {
            // independent variable has no spread (also covers count <= 1)
            return Ok(ScalarValue::Float64(None));
        }
        let slope = self.covar.get_algo_const() / m2_x;
        if self.intercept {
            // mean(y) - slope * mean(x); covar fed (x, y) => mean1 = mean(x), mean2 = mean(y)
            let mean_x = self.covar.get_mean1();
            let mean_y = self.covar.get_mean2();
            Ok(ScalarValue::Float64(Some(mean_y - slope * mean_x)))
        } else {
            Ok(ScalarValue::Float64(Some(slope)))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.covar) + self.covar.size()
            - std::mem::size_of_val(&self.var_x)
            + self.var_x.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;

    fn acc(regr_type: RegrType) -> Box<dyn Accumulator> {
        match regr_type {
            RegrType::SXX | RegrType::SYY => Box::new(RegrMomentAccumulator::try_new().unwrap()),
            RegrType::SXY => Box::new(RegrCovAccumulator::try_new().unwrap()),
            RegrType::R2 => Box::new(RegrR2Accumulator::try_new().unwrap()),
            RegrType::Slope => Box::new(RegrLineAccumulator::try_new(false).unwrap()),
            RegrType::Intercept => Box::new(RegrLineAccumulator::try_new(true).unwrap()),
        }
    }

    fn cols(y: Vec<Option<f64>>, x: Vec<Option<f64>>) -> Vec<ArrayRef> {
        vec![
            Arc::new(Float64Array::from(y)) as ArrayRef,
            Arc::new(Float64Array::from(x)) as ArrayRef,
        ]
    }

    fn eval(regr_type: RegrType, y: Vec<Option<f64>>, x: Vec<Option<f64>>) -> Option<f64> {
        let mut a = acc(regr_type);
        a.update_batch(&cols(y, x)).unwrap();
        match a.evaluate().unwrap() {
            ScalarValue::Float64(v) => v,
            other => panic!("unexpected scalar {other:?}"),
        }
    }

    fn approx(a: Option<f64>, b: f64) {
        assert!(
            a.map(|v| (v - b).abs() < 1e-9).unwrap_or(false),
            "expected ~{b}, got {a:?}"
        );
    }

    fn perfect_line() -> (Vec<Option<f64>>, Vec<Option<f64>>) {
        // y = 2x + 1
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y: Vec<_> = x.iter().map(|v| 2.0 * v + 1.0).collect();
        (
            y.into_iter().map(Some).collect(),
            x.into_iter().map(Some).collect(),
        )
    }

    #[test]
    fn slope_and_intercept_perfect_line() {
        let (y, x) = perfect_line();
        approx(eval(RegrType::Slope, y.clone(), x.clone()), 2.0);
        approx(eval(RegrType::Intercept, y, x), 1.0);
    }

    #[test]
    fn r2_perfect_line_is_one() {
        let (y, x) = perfect_line();
        approx(eval(RegrType::R2, y, x), 1.0);
    }

    #[test]
    fn r2_constant_y_is_one() {
        // Dependent variable constant, independent varies: Spark returns 1.0.
        let y = vec![Some(7.0), Some(7.0), Some(7.0), Some(7.0)];
        let x = vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)];
        approx(eval(RegrType::R2, y, x), 1.0);
    }

    #[test]
    fn constant_x_yields_null() {
        // Independent variable constant: slope/intercept/r2 are all NULL.
        let y = vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)];
        let x = vec![Some(5.0), Some(5.0), Some(5.0), Some(5.0)];
        assert_eq!(eval(RegrType::Slope, y.clone(), x.clone()), None);
        assert_eq!(eval(RegrType::Intercept, y.clone(), x.clone()), None);
        assert_eq!(eval(RegrType::R2, y, x), None);
    }

    #[test]
    fn single_pair_edges() {
        let y = vec![Some(3.0)];
        let x = vec![Some(5.0)];
        // slope/intercept/r2 require >= 2 points
        assert_eq!(eval(RegrType::Slope, y.clone(), x.clone()), None);
        assert_eq!(eval(RegrType::Intercept, y.clone(), x.clone()), None);
        assert_eq!(eval(RegrType::R2, y.clone(), x.clone()), None);
        // moments are 0 for a single point, not null
        approx(eval(RegrType::SXX, y.clone(), x.clone()), 0.0);
        approx(eval(RegrType::SYY, y.clone(), x.clone()), 0.0);
        approx(eval(RegrType::SXY, y, x), 0.0);
    }

    #[test]
    fn empty_input_is_null() {
        for t in [
            RegrType::Slope,
            RegrType::Intercept,
            RegrType::R2,
            RegrType::SXX,
            RegrType::SYY,
            RegrType::SXY,
        ] {
            assert_eq!(eval(t, vec![], vec![]), None);
        }
    }

    #[test]
    fn moments_and_comoment() {
        // y, x with known deviations. mean_x = 2.75, mean_y = 1.75
        let y = vec![Some(1.0), Some(2.0), Some(2.0), Some(2.0)];
        let x = vec![Some(2.0), Some(2.0), Some(3.0), Some(4.0)];
        // The serde duplicates the target column into both slots:
        // regr_sxx -> RegrReplacement(x), regr_syy -> RegrReplacement(y).
        // m2_x = (2-2.75)^2+(2-2.75)^2+(3-2.75)^2+(4-2.75)^2 = 2.75
        approx(eval(RegrType::SXX, x.clone(), x.clone()), 2.75);
        // m2_y = 0.75
        approx(eval(RegrType::SYY, y.clone(), y.clone()), 0.75);
        // ck = sum (x-mx)(y-my) = 0.75
        approx(eval(RegrType::SXY, y, x), 0.75);
    }

    #[test]
    fn null_pairs_are_skipped() {
        // Only (1,2) and (5,10) survive => y = 2x line.
        let y = vec![Some(1.0), None, Some(3.0), Some(5.0)];
        let x = vec![Some(2.0), Some(99.0), None, Some(10.0)];
        approx(eval(RegrType::Slope, y.clone(), x.clone()), 0.5);
        approx(eval(RegrType::R2, y, x), 1.0);
    }

    #[test]
    fn merge_matches_single_batch() {
        let (y, x) = perfect_line();
        // Split into two batches and merge their partial states.
        let mut a1 = acc(RegrType::Slope);
        a1.update_batch(&cols(y[..2].to_vec(), x[..2].to_vec()))
            .unwrap();
        let mut a2 = acc(RegrType::Slope);
        a2.update_batch(&cols(y[2..].to_vec(), x[2..].to_vec()))
            .unwrap();

        let state2 = a2.state().unwrap();
        let state_arrays: Vec<ArrayRef> = state2
            .iter()
            .map(|s| s.to_array_of_size(1).unwrap())
            .collect();
        a1.merge_batch(&state_arrays).unwrap();

        match a1.evaluate().unwrap() {
            ScalarValue::Float64(v) => approx(v, 2.0),
            other => panic!("unexpected {other:?}"),
        }
    }
}
