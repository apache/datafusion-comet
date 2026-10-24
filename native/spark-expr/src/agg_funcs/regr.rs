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
//!
//! Grouped aggregation reuses [`VarianceGroupsAccumulator`],
//! [`CovarianceGroupsAccumulator`], and [`CorrelationGroupsAccumulator`],
//! preserving the same partial-state layouts.

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array};
use arrow::compute::{and, filter, is_not_null};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{not_impl_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::expressions::format_state_name;
use datafusion::physical_expr::expressions::StatsType;
use std::sync::Arc;

use crate::agg_funcs::correlation::CorrelationGroupsAccumulator;
use crate::agg_funcs::covariance::{CovarianceAccumulator, CovarianceGroupsAccumulator};
use crate::agg_funcs::variance::{VarianceAccumulator, VarianceGroupsAccumulator};
use crate::agg_funcs::welford::{r2_value, ZeroDivisor};
use crate::{EvalMode, QueryContextMap};

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
    /// Only consulted for `Slope` / `Intercept`. When `true` (Spark 3.5.2 and
    /// 4.0.0 onward), `VariancePop(x)` counts only rows where both `y` and `x`
    /// are non-null. When `false`, it counts every row where `x` is non-null.
    filter_var_by_pair_nulls: bool,
    /// Only consulted for `R2`. When `true` (Spark 3.5.9, 4.0.3, 4.1.2 and 4.2.0
    /// onward), a constant dependent variable evaluates to `1.0` and a constant
    /// independent variable to `null`. When `false`, those two cases are reversed.
    r2_constant_dependent_is_perfect_fit: bool,
    /// Only consulted for `R2` when the raw moment product underflows to zero.
    zero_divisor: ZeroDivisor,
}

impl Regr {
    pub fn new(
        regr_type: RegrType,
        name: impl Into<String>,
        filter_var_by_pair_nulls: bool,
        r2_constant_dependent_is_perfect_fit: bool,
        eval_mode: EvalMode,
        expr_id: Option<u64>,
        registry: Arc<QueryContextMap>,
    ) -> Self {
        Self {
            name: name.into(),
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
            regr_type,
            filter_var_by_pair_nulls,
            r2_constant_dependent_is_perfect_fit,
            zero_divisor: ZeroDivisor::new(eval_mode, expr_id, registry),
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
            RegrType::R2 => Box::new(RegrR2Accumulator::try_new(
                self.r2_constant_dependent_is_perfect_fit,
                self.zero_divisor.clone(),
            )?),
            RegrType::Slope => Box::new(RegrLineAccumulator::try_new(
                false,
                self.filter_var_by_pair_nulls,
            )?),
            RegrType::Intercept => Box::new(RegrLineAccumulator::try_new(
                true,
                self.filter_var_by_pair_nulls,
            )?),
        };
        Ok(acc)
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(match self.regr_type {
            RegrType::SXX | RegrType::SYY => Box::new(RegrMomentGroupsAccumulator {
                var: VarianceGroupsAccumulator::new(StatsType::Population, false),
            }),
            RegrType::SXY => Box::new(RegrCovGroupsAccumulator {
                covar: CovarianceGroupsAccumulator::new(StatsType::Population, false),
            }),
            RegrType::Slope | RegrType::Intercept => Box::new(RegrLineGroupsAccumulator {
                covar: CovarianceGroupsAccumulator::new(StatsType::Population, false),
                var_x: VarianceGroupsAccumulator::new(StatsType::Population, false),
                intercept: self.regr_type == RegrType::Intercept,
                filter_var_by_pair_nulls: self.filter_var_by_pair_nulls,
            }),
            RegrType::R2 => Box::new(CorrelationGroupsAccumulator::new_regr_r2(
                self.r2_constant_dependent_is_perfect_fit,
                self.zero_divisor.clone(),
            )),
        })
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
/// Spark's degenerate-case handling (see `RegrR2.evaluateExpression`) was
/// swapped by SPARK-55969, which shipped in 3.5.9, 4.0.3, 4.1.2 and 4.2.0. In
/// both eras one degenerate case returns `null` and the other returns `1.0` (a
/// perfect fit), but which is which differs:
/// - before SPARK-55969 (Spark 3.4 and earlier patches of 3.5, 4.0 and 4.1):
///   constant dependent `y` (`m2(y) == 0`) -> `null`; constant independent `x`
///   (`m2(x) == 0`) -> `1.0`.
/// - after: constant dependent `y` -> `1.0`; constant independent `x` -> `null`.
///
/// `m2(y) == 0` also covers fewer than two rows. DataFusion returns `null` in
/// both degenerate cases.
#[derive(Debug)]
struct RegrR2Accumulator {
    covar: CovarianceAccumulator,
    var_y: VarianceAccumulator,
    var_x: VarianceAccumulator,
    /// When `true` (after SPARK-55969), a constant dependent variable yields
    /// `1.0` and a constant independent variable yields `null`; reversed otherwise.
    constant_dependent_is_perfect_fit: bool,
    zero_divisor: ZeroDivisor,
}

impl RegrR2Accumulator {
    fn try_new(constant_dependent_is_perfect_fit: bool, zero_divisor: ZeroDivisor) -> Result<Self> {
        Ok(Self {
            covar: CovarianceAccumulator::try_new(StatsType::Population, false)?,
            var_y: VarianceAccumulator::try_new(StatsType::Population, false)?
                .with_pearson_update(),
            var_x: VarianceAccumulator::try_new(StatsType::Population, false)?
                .with_pearson_update(),
            constant_dependent_is_perfect_fit,
            zero_divisor,
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
        Ok(ScalarValue::Float64(r2_value(
            self.covar.get_algo_const(),
            self.var_y.get_m2(),
            self.var_x.get_m2(),
            self.constant_dependent_is_perfect_fit,
            &self.zero_divisor,
        )?))
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
    /// When `true` (Spark 3.5+), `var_x` counts only rows where both `y` and `x`
    /// are non-null. When `false` (Spark 3.4), `var_x` counts every row where `x`
    /// is non-null, even if `y` is null.
    filter_var_by_pair_nulls: bool,
}

impl RegrLineAccumulator {
    fn try_new(intercept: bool, filter_var_by_pair_nulls: bool) -> Result<Self> {
        Ok(Self {
            covar: CovarianceAccumulator::try_new(StatsType::Population, false)?,
            var_x: VarianceAccumulator::try_new(StatsType::Population, false)?,
            intercept,
            filter_var_by_pair_nulls,
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
        if !pairs[0].is_empty() {
            // Feed covariance as (x, y) so mean1 = mean(x), mean2 = mean(y).
            let cov_input = [Arc::clone(&pairs[1]), Arc::clone(&pairs[0])];
            self.covar.update_batch(&cov_input)?;
        }
        if self.filter_var_by_pair_nulls {
            // Spark 3.5+: VariancePop(x) only over rows where both y and x are non-null.
            if !pairs[0].is_empty() {
                self.var_x.update_batch(&pairs[1..2])?;
            }
        } else {
            // Spark 3.4: VariancePop(x) over every row where x is non-null (the
            // VarianceAccumulator itself skips x nulls), regardless of y.
            self.var_x.update_batch(&values[1..2])?;
        }
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
        Ok(ScalarValue::Float64(line_value(
            self.covar.get_algo_const(),
            self.var_x.get_m2(),
            self.covar.get_mean1(),
            self.covar.get_mean2(),
            self.intercept,
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.covar) + self.covar.size()
            - std::mem::size_of_val(&self.var_x)
            + self.var_x.size()
    }
}

fn line_value(ck: f64, m2_x: f64, mean_x: f64, mean_y: f64, intercept: bool) -> Option<f64> {
    if m2_x == 0.0 {
        // The independent variable has no spread (also covers count <= 1).
        None
    } else {
        let slope = ck / m2_x;
        Some(if intercept {
            mean_y - slope * mean_x
        } else {
            slope
        })
    }
}

/// Grouped raw central moment. Spark has already pair-filtered the selected child.
#[derive(Debug)]
struct RegrMomentGroupsAccumulator {
    var: VarianceGroupsAccumulator,
}

impl GroupsAccumulator for RegrMomentGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.var
            .update_batch(&values[0..1], group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.var
            .merge_batch(values, group_indices, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.var.counts);
        let _ = emit_to.take_needed(&mut self.var.means);
        let m2s = emit_to.take_needed(&mut self.var.m2s);
        Ok(Arc::new(Float64Array::from_iter(
            counts
                .into_iter()
                .zip(m2s)
                .map(|(count, m2)| (count != 0.0).then_some(m2)),
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.var.state(emit_to)
    }

    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
    }

    fn size(&self) -> usize {
        self.var.size()
    }
}

/// Grouped raw co-moment, with the same four state fields as covariance.
#[derive(Debug)]
struct RegrCovGroupsAccumulator {
    covar: CovarianceGroupsAccumulator,
}

impl GroupsAccumulator for RegrCovGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.covar
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.covar
            .merge_batch(values, group_indices, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.covar.counts);
        let _ = emit_to.take_needed(&mut self.covar.mean1s);
        let _ = emit_to.take_needed(&mut self.covar.mean2s);
        let cs = emit_to.take_needed(&mut self.covar.algo_consts);
        Ok(Arc::new(Float64Array::from_iter(
            counts
                .into_iter()
                .zip(cs)
                .map(|(count, c)| (count != 0.0).then_some(c)),
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.covar.state(emit_to)
    }

    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
    }

    fn size(&self) -> usize {
        self.covar.size()
    }
}

/// The covariance and x variance keep independent counts: before SPARK-48719,
/// the variance includes x even when y is null. Preserve the seven-field state.
#[derive(Debug)]
struct RegrLineGroupsAccumulator {
    covar: CovarianceGroupsAccumulator,
    var_x: VarianceGroupsAccumulator,
    intercept: bool,
    filter_var_by_pair_nulls: bool,
}

impl GroupsAccumulator for RegrLineGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let cov_input = [Arc::clone(&values[1]), Arc::clone(&values[0])];
        self.covar
            .update_batch(&cov_input, group_indices, opt_filter, total_num_groups)?;
        // The variance child already skips null x. Modern Spark additionally
        // excludes null y; preserve row positions so group_indices still align.
        let combined = if self.filter_var_by_pair_nulls && values[0].null_count() != 0 {
            let valid_y = is_not_null(&values[0])?;
            Some(match opt_filter {
                Some(filter) => and(filter, &valid_y)?,
                None => valid_y,
            })
        } else {
            None
        };
        self.var_x.update_batch(
            &values[1..2],
            group_indices,
            combined.as_ref().or(opt_filter),
            total_num_groups,
        )
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.covar
            .merge_batch(&values[0..4], group_indices, total_num_groups)?;
        self.var_x
            .merge_batch(&values[4..7], group_indices, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let _ = emit_to.take_needed(&mut self.covar.counts);
        let means_x = emit_to.take_needed(&mut self.covar.mean1s);
        let means_y = emit_to.take_needed(&mut self.covar.mean2s);
        let cs = emit_to.take_needed(&mut self.covar.algo_consts);
        let _ = emit_to.take_needed(&mut self.var_x.counts);
        let _ = emit_to.take_needed(&mut self.var_x.means);
        let m2s = emit_to.take_needed(&mut self.var_x.m2s);
        Ok(Arc::new(Float64Array::from_iter((0..cs.len()).map(|i| {
            line_value(cs[i], m2s[i], means_x[i], means_y[i], self.intercept)
        }))))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut state = self.covar.state(emit_to)?;
        state.extend(self.var_x.state(emit_to)?);
        Ok(state)
    }

    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
    }

    fn size(&self) -> usize {
        self.covar.size() + self.var_x.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{create_query_context_map, QueryContext, SparkErrorWithContext};
    use arrow::array::Float64Array;
    use datafusion::common::DataFusionError;
    use datafusion_comet_common::SparkError;

    fn zero_divisor(eval_mode: EvalMode) -> ZeroDivisor {
        ZeroDivisor::new(eval_mode, None, create_query_context_map())
    }

    fn acc(regr_type: RegrType) -> Box<dyn Accumulator> {
        match regr_type {
            RegrType::SXX | RegrType::SYY => Box::new(RegrMomentAccumulator::try_new().unwrap()),
            RegrType::SXY => Box::new(RegrCovAccumulator::try_new().unwrap()),
            // Default to the post-SPARK-55969 degenerate-case semantics.
            RegrType::R2 => {
                Box::new(RegrR2Accumulator::try_new(true, zero_divisor(EvalMode::Legacy)).unwrap())
            }
            // Existing tests exercise the Spark 3.5+ both-non-null semantics.
            RegrType::Slope => Box::new(RegrLineAccumulator::try_new(false, true).unwrap()),
            RegrType::Intercept => Box::new(RegrLineAccumulator::try_new(true, true).unwrap()),
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

    #[test]
    fn regr_merge_empty_partials() {
        for kind in [
            RegrType::SXX,
            RegrType::SYY,
            RegrType::SXY,
            RegrType::R2,
            RegrType::Slope,
            RegrType::Intercept,
        ] {
            for empty_first in [false, true] {
                let mut merged = acc(kind);
                let counts = if empty_first { [0, 100] } else { [100, 0] };
                for count in counts {
                    let mut partial = acc(kind);
                    let values = vec![Some(1e155); count];
                    partial.update_batch(&cols(values.clone(), values)).unwrap();
                    let state = partial
                        .state()
                        .unwrap()
                        .iter()
                        .map(|v| v.to_array_of_size(1).unwrap())
                        .collect::<Vec<_>>();
                    merged.merge_batch(&state).unwrap();
                }
                let ScalarValue::Float64(result) = merged.evaluate().unwrap() else {
                    panic!("expected a double regression result");
                };
                if !empty_first {
                    assert!(result.unwrap().is_nan(), "{kind:?}");
                } else if matches!(kind, RegrType::SXX | RegrType::SYY | RegrType::SXY) {
                    assert_eq!(result, Some(0.0));
                } else {
                    assert_eq!(result, None);
                }
            }
        }
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

    fn assert_r2_zero_divisor(a: &mut RegrR2Accumulator, eval_mode: EvalMode) {
        // Both moments are nonzero. It is their product, not a constant input,
        // that makes Spark's final division have a zero denominator.
        assert_ne!(a.var_y.get_m2(), 0.0);
        assert_ne!(a.var_x.get_m2(), 0.0);
        assert_eq!(a.var_y.get_m2() * a.var_x.get_m2(), 0.0);
        if eval_mode == EvalMode::Ansi {
            let DataFusionError::External(error) = a.evaluate().unwrap_err() else {
                panic!("expected a structured Spark divide-by-zero error");
            };
            assert!(matches!(
                error.downcast_ref::<SparkError>(),
                Some(SparkError::DivideByZero)
            ));
        } else {
            assert_eq!(a.evaluate().unwrap(), ScalarValue::Float64(None));
        }
    }

    #[test]
    fn r2_ansi_error_carries_query_context() {
        let sql = "SELECT regr_r2(v, v) FROM t";
        let registry = create_query_context_map();
        registry.register(
            7,
            QueryContext::new(sql.to_string(), 7, 19, None, None, 1, 7),
        );
        for perfect_dep in [false, true] {
            let mut a = RegrR2Accumulator::try_new(
                perfect_dep,
                ZeroDivisor::new(EvalMode::Ansi, Some(7), Arc::clone(&registry)),
            )
            .unwrap();
            let v = vec![Some(1e-100), Some(2e-100)];
            a.update_batch(&cols(v.clone(), v)).unwrap();
            let DataFusionError::External(error) = a.evaluate().unwrap_err() else {
                panic!("expected a Spark error with query context");
            };
            let error = error.downcast_ref::<SparkErrorWithContext>().unwrap();
            assert!(matches!(error.error, SparkError::DivideByZero));
            assert_eq!(error.context.as_ref().unwrap().sql_text.as_str(), sql);
        }
    }

    #[test]
    fn r2_moment_product_underflow() {
        for eval_mode in [EvalMode::Legacy, EvalMode::Ansi] {
            for perfect_dep in [false, true] {
                for sign in [-1.0, 1.0] {
                    for merge in [false, true] {
                        let mut a =
                            RegrR2Accumulator::try_new(perfect_dep, zero_divisor(eval_mode))
                                .unwrap();
                        let y = vec![Some(1e-100), None, Some(2e-100)];
                        let x = vec![Some(sign * 1e-100), Some(1e200), Some(sign * 2e-100)];
                        if merge {
                            // Merge two single-row partials, including an unpaired null.
                            for range in [0..2, 2..3] {
                                let mut partial = RegrR2Accumulator::try_new(
                                    perfect_dep,
                                    zero_divisor(eval_mode),
                                )
                                .unwrap();
                                partial
                                    .update_batch(&cols(
                                        y[range.clone()].to_vec(),
                                        x[range].to_vec(),
                                    ))
                                    .unwrap();
                                let state = partial
                                    .state()
                                    .unwrap()
                                    .iter()
                                    .map(|v| v.to_array_of_size(1).unwrap())
                                    .collect::<Vec<_>>();
                                a.merge_batch(&state).unwrap();
                            }
                        } else {
                            a.update_batch(&cols(y, x)).unwrap();
                        }
                        assert_r2_zero_divisor(&mut a, eval_mode);
                    }
                }
            }
        }
    }

    #[test]
    fn r2_zero_covariance_with_underflowed_product() {
        for eval_mode in [EvalMode::Legacy, EvalMode::Ansi] {
            for perfect_dep in [false, true] {
                let mut a =
                    RegrR2Accumulator::try_new(perfect_dep, zero_divisor(eval_mode)).unwrap();
                // A zero co-moment still divides by zero, rather than returning NaN.
                let state = [4.0, 0.0, 0.0, 0.0, 4e-200, 4e-200]
                    .into_iter()
                    .map(|v| Arc::new(Float64Array::from(vec![v])) as ArrayRef)
                    .collect::<Vec<_>>();
                a.merge_batch(&state).unwrap();
                assert_r2_zero_divisor(&mut a, eval_mode);
            }
        }
    }

    #[test]
    fn r2_raw_moment_overflow() {
        for eval_mode in [EvalMode::Legacy, EvalMode::Ansi] {
            for perfect_dep in [false, true] {
                for sign in [-1.0, 1.0] {
                    for (values, nan) in [(vec![1e100, 2e100], false), (vec![1e200, -1e200], true)]
                    {
                        let mut a =
                            RegrR2Accumulator::try_new(perfect_dep, zero_divisor(eval_mode))
                                .unwrap();
                        a.update_batch(&cols(
                            values.iter().copied().map(Some).collect(),
                            values.iter().map(|v| Some(sign * v)).collect(),
                        ))
                        .unwrap();
                        let ScalarValue::Float64(Some(result)) = a.evaluate().unwrap() else {
                            panic!("expected a non-null double result");
                        };
                        if nan {
                            assert!(result.is_nan());
                        } else {
                            assert_eq!(result.to_bits(), 0.0_f64.to_bits());
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn r2_constant_y_is_perfect_fit() {
        // Dependent variable constant: post-SPARK-55969 regr_r2 returns 1.0.
        let y = vec![Some(7.0), Some(7.0), Some(7.0), Some(7.0)];
        let x = vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)];
        approx(eval(RegrType::R2, y, x), 1.0);
    }

    #[test]
    fn r2_degenerate_cases_swapped_by_spark_55969() {
        // SPARK-55969 swapped the two degenerate cases relative to Spark 3.4.
        let const_y = (
            vec![Some(7.0), Some(7.0), Some(7.0), Some(7.0)],
            vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)],
        );
        let const_x = (
            vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)],
            vec![Some(5.0), Some(5.0), Some(5.0), Some(5.0)],
        );

        for eval_mode in [EvalMode::Legacy, EvalMode::Ansi] {
            let r2 = |perfect_dep: bool, y: Vec<Option<f64>>, x: Vec<Option<f64>>| {
                let mut a =
                    RegrR2Accumulator::try_new(perfect_dep, zero_divisor(eval_mode)).unwrap();
                a.update_batch(&cols(y, x)).unwrap();
                match a.evaluate().unwrap() {
                    ScalarValue::Float64(v) => v,
                    other => panic!("unexpected {other:?}"),
                }
            };

            // These guards must run before the zero-product check, even in ANSI mode.
            assert_eq!(r2(true, const_y.0.clone(), const_y.1.clone()), Some(1.0));
            assert_eq!(r2(true, const_x.0.clone(), const_x.1.clone()), None);
            assert_eq!(r2(false, const_y.0.clone(), const_y.1.clone()), None);
            assert_eq!(r2(false, const_x.0.clone(), const_x.1.clone()), Some(1.0));
        }
    }

    #[test]
    fn constant_x_edges() {
        // Independent variable constant, dependent varies: slope/intercept are
        // NULL, and post-SPARK-55969 regr_r2 is NULL too.
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
    fn slope_var_x_null_filtering_differs_by_spark_version() {
        // A row where x is non-null but y is null: (1,2), (2,4), (3,6), (null,10).
        // The co-moment ck = 4 is the same either way (only paired rows contribute).
        // Spark 3.5+ excludes x=10 from VariancePop(x) (m2 over {2,4,6} = 8) -> 0.5.
        // Spark 3.4 includes x=10 (m2 over {2,4,6,10} = 35) -> 4/35.
        let y = vec![Some(1.0), Some(2.0), Some(3.0), None];
        let x = vec![Some(2.0), Some(4.0), Some(6.0), Some(10.0)];

        let mut filtered = RegrLineAccumulator::try_new(false, true).unwrap();
        filtered.update_batch(&cols(y.clone(), x.clone())).unwrap();
        match filtered.evaluate().unwrap() {
            ScalarValue::Float64(v) => approx(v, 0.5),
            other => panic!("unexpected {other:?}"),
        }

        let mut unfiltered = RegrLineAccumulator::try_new(false, false).unwrap();
        unfiltered.update_batch(&cols(y, x)).unwrap();
        match unfiltered.evaluate().unwrap() {
            ScalarValue::Float64(v) => approx(v, 4.0 / 35.0),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn merge_fractional_constant_partials() {
        // #6423: the first merge into an empty final buffer used to round the
        // constant mean away from 0.1. Later merges then produced nonzero M2.
        for (kind, constant_dependent, expected) in [
            (RegrType::Slope, false, None),
            (RegrType::Intercept, false, None),
            (RegrType::R2, false, None),
            (RegrType::R2, true, Some(1.0)),
            (RegrType::SXX, false, Some(0.0)),
            (RegrType::SYY, true, Some(0.0)),
            (RegrType::SXY, false, Some(0.0)),
        ] {
            for reverse in [false, true] {
                let mut merged = acc(kind);
                let starts = if reverse { [3, 0] } else { [0, 3] };
                for start in starts {
                    let varying = (start..start + 3).map(|v| Some(v as f64)).collect();
                    let constant = vec![Some(0.1); 3];
                    let values = match kind {
                        // Spark's RegrReplacement duplicates the selected column.
                        RegrType::SXX | RegrType::SYY => cols(constant.clone(), constant),
                        _ if constant_dependent => cols(constant, varying),
                        _ => cols(varying, constant),
                    };
                    let mut partial = acc(kind);
                    partial.update_batch(&values).unwrap();
                    let state = partial
                        .state()
                        .unwrap()
                        .iter()
                        .map(|v| v.to_array_of_size(1).unwrap())
                        .collect::<Vec<_>>();
                    merged.merge_batch(&state).unwrap();
                }
                // A tolerance would hide the tiny nonzero moments behind the bug.
                assert_eq!(
                    merged.evaluate().unwrap(),
                    ScalarValue::Float64(expected),
                    "{kind:?}, constant_dependent={constant_dependent}, reverse={reverse}"
                );
            }
        }
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

#[cfg(test)]
mod groups_tests {
    use super::*;
    use crate::{create_query_context_map, Correlation, QueryContext, SparkErrorWithContext};
    use arrow::array::AsArray;
    use arrow::datatypes::{Float64Type, Schema};
    use datafusion::common::DataFusionError;
    use datafusion::logical_expr::AggregateUDF;
    use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
    use datafusion::physical_expr::expressions::Column;
    use datafusion_comet_common::SparkError;
    use std::mem::size_of;

    const KINDS: [RegrType; 6] = [
        RegrType::SXX,
        RegrType::SYY,
        RegrType::SXY,
        RegrType::Slope,
        RegrType::Intercept,
        RegrType::R2,
    ];

    fn physical(udf: AggregateUDF) -> AggregateFunctionExpr {
        let schema = Arc::new(Schema::new(vec![
            Field::new("y", DataType::Float64, true),
            Field::new("x", DataType::Float64, true),
        ]));
        let expr = AggregateExprBuilder::new(
            Arc::new(udf),
            vec![Arc::new(Column::new("y", 0)), Arc::new(Column::new("x", 1))],
        )
        .schema(schema)
        .alias("stat")
        .build()
        .unwrap();
        assert!(expr.groups_accumulator_supported());
        expr
    }

    fn expression(kind: RegrType, modern: bool) -> AggregateFunctionExpr {
        physical(AggregateUDF::new_from_impl(Regr::new(
            kind,
            "regr",
            modern,
            modern,
            EvalMode::Legacy,
            None,
            create_query_context_map(),
        )))
    }

    fn inputs(kind: RegrType, y: &[Option<f64>], x: &[Option<f64>]) -> Vec<ArrayRef> {
        if matches!(kind, RegrType::SXX | RegrType::SYY) {
            let selected = y.iter().zip(x).map(|(&y, &x)| {
                y.zip(x)
                    .map(|(y, x)| if kind == RegrType::SXX { x } else { y })
            });
            let column: ArrayRef = Arc::new(Float64Array::from_iter(selected));
            vec![Arc::clone(&column), column]
        } else {
            vec![
                Arc::new(Float64Array::from(y.to_vec())),
                Arc::new(Float64Array::from(x.to_vec())),
            ]
        }
    }

    fn same_value(actual: Option<f64>, expected: Option<f64>) {
        match (actual, expected) {
            (Some(a), Some(b)) if b.is_nan() => assert!(a.is_nan()),
            (Some(a), Some(b)) => assert_eq!(a.to_bits(), b.to_bits(), "{a:?} != {b:?}"),
            _ => assert_eq!(actual, expected),
        }
    }

    fn scalar_value(acc: &mut dyn Accumulator) -> Option<f64> {
        let ScalarValue::Float64(value) = acc.evaluate().unwrap() else {
            panic!("expected f64")
        };
        value
    }

    fn update_scalars(
        scalars: &mut [Box<dyn Accumulator>],
        values: &[ArrayRef],
        groups: &[usize],
        predicate: Option<&BooleanArray>,
    ) {
        for (group, scalar) in scalars.iter_mut().enumerate() {
            let mask = BooleanArray::from_iter(groups.iter().enumerate().map(|(i, &g)| {
                Some(g == group && predicate.is_none_or(|p| p.is_valid(i) && p.value(i)))
            }));
            let selected = values
                .iter()
                .map(|a| filter(a, &mask).unwrap())
                .collect::<Vec<_>>();
            scalar.update_batch(&selected).unwrap();
        }
    }

    fn same_state(state: &[ArrayRef], group: usize, scalar: &mut dyn Accumulator) {
        let expected = scalar.state().unwrap();
        assert_eq!(state.len(), expected.len());
        for (actual, expected) in state.iter().zip(expected) {
            let ScalarValue::Float64(expected) = expected else {
                panic!("expected f64 state")
            };
            let actual = actual.as_primitive::<Float64Type>();
            assert!(actual.is_valid(group));
            same_value(Some(actual.value(group)), expected);
        }
    }

    fn scalar_state(scalar: &mut dyn Accumulator) -> Vec<ArrayRef> {
        scalar
            .state()
            .unwrap()
            .iter()
            .map(|v| v.to_array_of_size(1).unwrap())
            .collect()
    }

    #[test]
    fn grouped_updates_preserve_scalar_state_and_results() {
        for kind in KINDS {
            for modern in [false, true] {
                for nullable in [false, true] {
                    for filtered in [false, true] {
                        let expr = expression(kind, modern);
                        let mut scalars = (0..6)
                            .map(|_| expr.create_accumulator().unwrap())
                            .collect::<Vec<_>>();
                        let mut grouped = expr.create_groups_accumulator().unwrap();
                        let mut for_state = expr.create_groups_accumulator().unwrap();
                        let y: Vec<_> = (0..35)
                            .map(|i| {
                                (i != 0 && i != 34 && (!nullable || i % 7 != 0))
                                    .then_some(i as f64 * 1.5 + 1.0)
                            })
                            .collect();
                        let x: Vec<_> = (0..35)
                            .map(|i| {
                                (i != 0 && i != 34 && (!nullable || i % 11 != 0))
                                    .then_some(i as f64 * 0.75 - 3.0)
                            })
                            .collect();
                        let values = inputs(kind, &y, &x);
                        // Sliced Arrow inputs and predicates retain nonzero offsets.
                        // The dense case has NULLs only outside the sliced range.
                        for start in [1, 12, 23] {
                            let chunk: Vec<_> = values.iter().map(|a| a.slice(start, 11)).collect();
                            let indices: Vec<_> = (start..start + 11).map(|i| i % 5).collect();
                            let predicate = BooleanArray::from_iter((0..35).map(|i| {
                                if i % 13 == 0 {
                                    None
                                } else {
                                    Some(i % 3 != 0)
                                }
                            }))
                            .slice(start, 11);
                            let filter = filtered.then_some(&predicate);
                            // Increase the allocated group count without adding a value
                            // for group 5. FILTER can also leave existing groups empty.
                            let total = if start == 1 { 5 } else { 6 };
                            grouped
                                .update_batch(&chunk, &indices, filter, total)
                                .unwrap();
                            for_state
                                .update_batch(&chunk, &indices, filter, total)
                                .unwrap();
                            update_scalars(&mut scalars, &chunk, &indices, filter);
                        }
                        let states = for_state.state(EmitTo::All).unwrap();
                        let result = grouped.evaluate(EmitTo::All).unwrap();
                        let result = result.as_primitive::<Float64Type>();
                        assert_eq!(result.len(), scalars.len());
                        for (i, scalar) in scalars.iter_mut().enumerate() {
                            same_state(&states, i, scalar.as_mut());
                            same_value(
                                result.is_valid(i).then(|| result.value(i)),
                                scalar_value(scalar.as_mut()),
                            );
                        }
                        assert_eq!(grouped.size(), 0);
                        assert_eq!(for_state.size(), 0);

                        // Final aggregation receives several state rows at once,
                        // with repeated and non-monotonic destination groups.
                        let destinations = [2, 0, 2, 1, 0, 1];
                        let mut merged = expr.create_groups_accumulator().unwrap();
                        let mut expected = (0..3)
                            .map(|_| expr.create_accumulator().unwrap())
                            .collect::<Vec<_>>();
                        for (start, len) in [(0, 6), (1, 4)] {
                            let batch: Vec<_> =
                                states.iter().map(|a| a.slice(start, len)).collect();
                            let indices = &destinations[start..start + len];
                            merged.merge_batch(&batch, indices, 3).unwrap();
                            for_state.merge_batch(&batch, indices, 3).unwrap();
                            for (row, &group) in indices.iter().enumerate() {
                                let state: Vec<_> = batch.iter().map(|a| a.slice(row, 1)).collect();
                                expected[group].merge_batch(&state).unwrap();
                            }
                        }
                        let state = for_state.state(EmitTo::All).unwrap();
                        let result = merged.evaluate(EmitTo::All).unwrap();
                        let result = result.as_primitive::<Float64Type>();
                        assert_eq!(result.len(), expected.len());
                        for (i, scalar) in expected.iter_mut().enumerate() {
                            same_state(&state, i, scalar.as_mut());
                            same_value(
                                result.is_valid(i).then(|| result.value(i)),
                                scalar_value(scalar.as_mut()),
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn grouped_state_round_trips_in_both_directions() {
        for kind in KINDS {
            for modern in [false, true] {
                // Include large adjacent values, signed zero, infinities/NaNs,
                // fractional constants and raw moment underflow/overflow.
                for (y, x) in [
                    (
                        vec![Some(1e16), Some(1e16 + 2.0), None, Some(-0.0)],
                        vec![Some(3.0), Some(9.0), Some(20.0), Some(0.0)],
                    ),
                    (
                        vec![
                            Some(1e200),
                            Some(-1e200),
                            Some(f64::NAN),
                            Some(f64::INFINITY),
                        ],
                        vec![Some(0.1); 4],
                    ),
                    (
                        vec![Some(1e-100), Some(2e-100), None, Some(0.0)],
                        vec![Some(2e-100), Some(-1e-100), Some(1e100), None],
                    ),
                ] {
                    let expr = expression(kind, modern);
                    let mut grouped = expr.create_groups_accumulator().unwrap();
                    let mut expected = expr.create_accumulator().unwrap();
                    let mut from_grouped = expr.create_accumulator().unwrap();
                    let values = inputs(kind, &y, &x);
                    // Empty partials before and after nonempty states must execute
                    // the merge arithmetic, including its NaN propagation.
                    for (start, len) in [(0, 0), (0, 2), (2, 2), (4, 0)] {
                        let chunk = values
                            .iter()
                            .map(|a| a.slice(start, len))
                            .collect::<Vec<_>>();
                        let mut partial = expr.create_accumulator().unwrap();
                        partial.update_batch(&chunk).unwrap();
                        let mut grouped_partial = expr.create_groups_accumulator().unwrap();
                        grouped_partial
                            .update_batch(&chunk, &vec![0; len], None, 1)
                            .unwrap();
                        let gs = grouped_partial.state(EmitTo::All).unwrap();
                        same_state(&gs, 0, partial.as_mut());
                        let ss = scalar_state(partial.as_mut());
                        expected.merge_batch(&ss).unwrap();
                        grouped.merge_batch(&ss, &[0], 1).unwrap();
                        from_grouped.merge_batch(&gs).unwrap();
                    }
                    let states = grouped.state(EmitTo::All).unwrap();
                    same_state(&states, 0, expected.as_mut());
                    same_value(
                        scalar_value(from_grouped.as_mut()),
                        scalar_value(expected.as_mut()),
                    );
                    // Evaluate after one identical additional merge on both sides.
                    grouped.merge_batch(&states, &[0], 1).unwrap();
                    let mut merged = expr.create_accumulator().unwrap();
                    merged.merge_batch(&states).unwrap();
                    let actual = grouped.evaluate(EmitTo::All).unwrap();
                    same_value(
                        actual.as_primitive::<Float64Type>().iter().next().unwrap(),
                        scalar_value(merged.as_mut()),
                    );
                }
            }
        }
    }

    #[test]
    fn grouped_emission_preserves_remaining_groups() {
        for kind in KINDS {
            for modern in [false, true] {
                let expr = expression(kind, modern);
                let mut grouped = expr.create_groups_accumulator().unwrap();
                let mut scalars = (0..4)
                    .map(|_| expr.create_accumulator().unwrap())
                    .collect::<Vec<_>>();
                let values = inputs(
                    kind,
                    &[
                        Some(1.0),
                        Some(4.0),
                        None,
                        Some(9.0),
                        Some(3.0),
                        Some(8.0),
                        Some(5.0),
                        Some(14.0),
                    ],
                    &[
                        Some(2.0),
                        Some(1.0),
                        Some(3.0),
                        None,
                        Some(6.0),
                        Some(4.0),
                        Some(7.0),
                        Some(5.0),
                    ],
                );
                let groups = [0, 1, 2, 3, 0, 1, 2, 3];
                grouped.update_batch(&values, &groups, None, 4).unwrap();
                update_scalars(&mut scalars, &values, &groups, None);
                let vectors = match kind {
                    RegrType::SXX | RegrType::SYY => 3,
                    RegrType::SXY => 4,
                    RegrType::Slope | RegrType::Intercept => 7,
                    RegrType::R2 => 10,
                };
                let allocated = grouped.size();
                assert!(allocated >= 4 * vectors * size_of::<f64>());
                assert!(grouped
                    .state(EmitTo::First(0))
                    .unwrap()
                    .iter()
                    .all(|a| a.is_empty()));
                assert_eq!(grouped.evaluate(EmitTo::First(0)).unwrap().len(), 0);
                let first = grouped.evaluate(EmitTo::First(1)).unwrap();
                same_value(
                    first.as_primitive::<Float64Type>().iter().next().unwrap(),
                    scalar_value(scalars.remove(0).as_mut()),
                );
                // EmitTo keeps the backing allocation when draining a small
                // prefix. Accounting must reflect capacity, not the new length.
                assert_eq!(grouped.size(), allocated);
                let state = grouped.state(EmitTo::First(1)).unwrap();
                same_state(&state, 0, scalars.remove(0).as_mut());
                assert!(grouped.size() >= 2 * vectors * size_of::<f64>());
                scalars.push(expr.create_accumulator().unwrap());
                let more = inputs(
                    kind,
                    &[Some(8.0), None, Some(17.0), Some(12.0)],
                    &[Some(6.0), Some(7.0), Some(9.0), Some(3.0)],
                );
                grouped.update_batch(&more, &[0, 2, 1, 2], None, 3).unwrap();
                update_scalars(&mut scalars, &more, &[0, 2, 1, 2], None);
                let mut partial = expr.create_accumulator().unwrap();
                partial.update_batch(&more).unwrap();
                let partial_state = scalar_state(partial.as_mut());
                grouped.merge_batch(&partial_state, &[1], 3).unwrap();
                scalars[1].merge_batch(&partial_state).unwrap();
                // A majority prefix exercises EmitTo's other allocation path.
                let first = grouped.evaluate(EmitTo::First(2)).unwrap();
                let last = grouped.evaluate(EmitTo::All).unwrap();
                assert_eq!(first.len(), 2);
                assert_eq!(last.len(), 1);
                for (actual, scalar) in first
                    .as_primitive::<Float64Type>()
                    .iter()
                    .chain(last.as_primitive::<Float64Type>().iter())
                    .zip(&mut scalars)
                {
                    same_value(actual, scalar_value(scalar.as_mut()));
                }
                assert_eq!(grouped.size(), 0);
                // All resets the accumulator so group zero can be used again.
                grouped.update_batch(&more, &[0; 4], None, 1).unwrap();
                same_state(&grouped.state(EmitTo::All).unwrap(), 0, partial.as_mut());
            }
        }
    }

    #[test]
    fn r2_factory_selects_regression_result_rules() {
        for modern in [false, true] {
            let expr = expression(RegrType::R2, modern);
            for (y, x, expected) in [
                (vec![Some(1.0)], vec![Some(2.0)], None),
                (
                    vec![Some(f64::INFINITY)],
                    vec![Some(f64::INFINITY)],
                    Some(f64::NAN),
                ),
                (
                    vec![Some(7.0); 2],
                    vec![Some(1.0), Some(3.0)],
                    modern.then_some(1.0),
                ),
                (
                    vec![Some(1.0), Some(3.0)],
                    vec![Some(7.0); 2],
                    (!modern).then_some(1.0),
                ),
            ] {
                let mut grouped = expr.create_groups_accumulator().unwrap();
                grouped
                    .update_batch(&inputs(RegrType::R2, &y, &x), &vec![0; y.len()], None, 1)
                    .unwrap();
                same_value(
                    grouped
                        .evaluate(EmitTo::All)
                        .unwrap()
                        .as_primitive::<Float64Type>()
                        .iter()
                        .next()
                        .unwrap(),
                    expected,
                );
            }
            // Synthetic partial state checks that R2 does not inherit Corr's
            // count==0 shortcut. Empty-partial merge arithmetic produces NaN M2.
            let state: Vec<ArrayRef> = [0.0, f64::NAN, f64::NAN, 0.0, 0.0, 0.0]
                .into_iter()
                .map(|v| Arc::new(Float64Array::from(vec![v])) as ArrayRef)
                .collect();
            let mut grouped = expr.create_groups_accumulator().unwrap();
            grouped.merge_batch(&state, &[0], 1).unwrap();
            same_value(
                grouped
                    .evaluate(EmitTo::All)
                    .unwrap()
                    .as_primitive::<Float64Type>()
                    .iter()
                    .next()
                    .unwrap(),
                Some(f64::NAN),
            );
        }
        let corr = physical(AggregateUDF::new_from_impl(Correlation::new(
            "corr",
            DataType::Float64,
            false,
            EvalMode::Legacy,
            None,
            create_query_context_map(),
        )));
        let mut corr = corr.create_groups_accumulator().unwrap();
        corr.update_batch(
            &inputs(RegrType::R2, &[Some(1.0)], &[Some(2.0)]),
            &[0],
            None,
            1,
        )
        .unwrap();
        same_value(
            corr.evaluate(EmitTo::All)
                .unwrap()
                .as_primitive::<Float64Type>()
                .iter()
                .next()
                .unwrap(),
            Some(f64::NAN),
        );
    }

    #[test]
    fn grouped_r2_underflow_preserves_captured_error_context() {
        let sql = "SELECT regr_r2(y, x) FROM t GROUP BY g";
        for mode in [EvalMode::Legacy, EvalMode::Ansi] {
            for modern in [false, true] {
                for merge in [false, true] {
                    let registry = create_query_context_map();
                    registry.register(
                        7,
                        QueryContext::new(sql.to_string(), 7, 19, None, None, 1, 7),
                    );
                    let expr = physical(AggregateUDF::new_from_impl(Regr::new(
                        RegrType::R2,
                        "regr_r2",
                        true,
                        modern,
                        mode,
                        Some(7),
                        registry,
                    )));
                    let mut grouped = expr.create_groups_accumulator().unwrap();
                    let values = inputs(
                        RegrType::R2,
                        &[Some(1e-100), None, Some(2e-100)],
                        &[Some(-1e-100), Some(1e200), Some(-2e-100)],
                    );
                    if merge {
                        for (start, len) in [(0, 2), (2, 1)] {
                            let mut partial = expr.create_accumulator().unwrap();
                            partial
                                .update_batch(
                                    &values
                                        .iter()
                                        .map(|a| a.slice(start, len))
                                        .collect::<Vec<_>>(),
                                )
                                .unwrap();
                            grouped
                                .merge_batch(&scalar_state(partial.as_mut()), &[0], 1)
                                .unwrap();
                        }
                    } else {
                        grouped.update_batch(&values, &[0; 3], None, 1).unwrap();
                    }
                    if mode == EvalMode::Ansi {
                        let DataFusionError::External(error) =
                            grouped.evaluate(EmitTo::All).unwrap_err()
                        else {
                            panic!("expected structured Spark error")
                        };
                        let error = error.downcast_ref::<SparkErrorWithContext>().unwrap();
                        assert!(matches!(error.error, SparkError::DivideByZero));
                        assert_eq!(error.context.as_ref().unwrap().sql_text.as_str(), sql);
                    } else {
                        assert_eq!(grouped.evaluate(EmitTo::All).unwrap().null_count(), 1);
                    }
                }
            }
        }
    }
}
