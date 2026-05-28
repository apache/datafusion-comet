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

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array};
use arrow::compute::{and, is_not_null};
use arrow::datatypes::{DataType, Field, FieldRef};
use std::sync::Arc;

use crate::agg_funcs::covariance::{CovarianceAccumulator, CovarianceGroupsAccumulator};
use crate::agg_funcs::stddev::StddevAccumulator;
use crate::agg_funcs::variance::VarianceGroupsAccumulator;
use arrow::compute::filter;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::expressions::format_state_name;
use datafusion::physical_expr::expressions::StatsType;

/// CORR aggregate expression
/// The implementation mostly is the same as the DataFusion's implementation. The reason
/// we have our own implementation is that DataFusion has UInt64 for state_field `count`,
/// while Spark has Double for count. Also we have added `null_on_divide_by_zero`
/// to be consistent with Spark's implementation.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Correlation {
    name: String,
    signature: Signature,
    null_on_divide_by_zero: bool,
}

impl Correlation {
    pub fn new(name: impl Into<String>, data_type: DataType, null_on_divide_by_zero: bool) -> Self {
        // the result of correlation just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
            null_on_divide_by_zero,
        }
    }
}

impl AggregateUDFImpl for Correlation {
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
        Ok(Box::new(CorrelationAccumulator::try_new(
            self.null_on_divide_by_zero,
        )?))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(&self.name, "count"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "mean1"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "mean2"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "algo_const"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "m2_1"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "m2_2"),
                DataType::Float64,
                true,
            )),
        ])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(CorrelationGroupsAccumulator::new(
            self.null_on_divide_by_zero,
        )))
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
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[2]),
            Arc::clone(&states[3]),
        ];
        let states_s1 = [
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[4]),
        ];
        let states_s2 = [
            Arc::clone(&states[0]),
            Arc::clone(&states[2]),
            Arc::clone(&states[5]),
        ];

        if !states[0].is_empty() && !states[1].is_empty() && !states[2].is_empty() {
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

        if self.covar.get_count() == 0.0 {
            return Ok(ScalarValue::Float64(None));
        } else if self.covar.get_count() == 1.0 {
            if self.null_on_divide_by_zero {
                return Ok(ScalarValue::Float64(None));
            } else {
                return Ok(ScalarValue::Float64(Some(f64::NAN)));
            }
        }
        match (covar, stddev1, stddev2) {
            (
                ScalarValue::Float64(Some(c)),
                ScalarValue::Float64(Some(s1)),
                ScalarValue::Float64(Some(s2)),
            ) if s1 != 0.0 && s2 != 0.0 => Ok(ScalarValue::Float64(Some(c / (s1 * s2)))),
            _ => Ok(ScalarValue::Float64(None)),
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

/// Grouped correlation accumulator. Mirrors the per-row `CorrelationAccumulator`
/// composition: one population covariance + two population variance sub-
/// accumulators for the per-column m2 values. Combined filter handles
/// "skip rows where either input is null" without filtering `group_indices`.
#[derive(Debug)]
struct CorrelationGroupsAccumulator {
    covar: CovarianceGroupsAccumulator,
    var1: VarianceGroupsAccumulator,
    var2: VarianceGroupsAccumulator,
    null_on_divide_by_zero: bool,
}

impl CorrelationGroupsAccumulator {
    fn new(null_on_divide_by_zero: bool) -> Self {
        // Children run with StatsType::Population, which never hits the
        // count == 1 sample-divide-by-zero branch, so the children's
        // null_on_divide_by_zero is dead code. The top-level evaluate()
        // applies the count <= 1 rule. Pass `false` to children to keep
        // that intent explicit.
        Self {
            covar: CovarianceGroupsAccumulator::new(StatsType::Population, false),
            var1: VarianceGroupsAccumulator::new(StatsType::Population, false),
            var2: VarianceGroupsAccumulator::new(StatsType::Population, false),
            null_on_divide_by_zero,
        }
    }
}

impl GroupsAccumulator for CorrelationGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to update_batch");
        // Fast path: when both inputs are fully non-null, skip the
        // is_not_null/and combination and forward the caller's filter as-is.
        // Mirrors the per-row CorrelationAccumulator short-circuit.
        let dense = values[0].null_count() == 0 && values[1].null_count() == 0;
        let combined: Option<BooleanArray> = if dense {
            None
        } else {
            let null_mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            Some(match opt_filter {
                Some(f) => and(f, &null_mask)?,
                None => null_mask,
            })
        };
        let filter_for_children: Option<&BooleanArray> = match (&combined, opt_filter) {
            (Some(c), _) => Some(c),
            (None, f) => f,
        };

        self.covar
            .update_batch(values, group_indices, filter_for_children, total_num_groups)?;
        self.var1.update_batch(
            &values[0..1],
            group_indices,
            filter_for_children,
            total_num_groups,
        )?;
        self.var2.update_batch(
            &values[1..2],
            group_indices,
            filter_for_children,
            total_num_groups,
        )?;
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 6, "six state columns to merge_batch");
        // state column order: count, mean1, mean2, algo_const, m2_1, m2_2
        let covar_state = [
            Arc::clone(&values[0]),
            Arc::clone(&values[1]),
            Arc::clone(&values[2]),
            Arc::clone(&values[3]),
        ];
        let var1_state = [
            Arc::clone(&values[0]),
            Arc::clone(&values[1]),
            Arc::clone(&values[4]),
        ];
        let var2_state = [
            Arc::clone(&values[0]),
            Arc::clone(&values[2]),
            Arc::clone(&values[5]),
        ];

        self.covar
            .merge_batch(&covar_state, group_indices, opt_filter, total_num_groups)?;
        self.var1
            .merge_batch(&var1_state, group_indices, opt_filter, total_num_groups)?;
        self.var2
            .merge_batch(&var2_state, group_indices, opt_filter, total_num_groups)?;
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        // All three children see the same rows (combined null-mask in
        // update_batch / shared count column on merge), so counts and means
        // are duplicated. We only need: covar.counts, covar.algo_consts,
        // var1.m2s, var2.m2s. Drain everything once and compute correlation
        // inline rather than calling each child's evaluate() (which would
        // allocate three Float64Arrays we'd then unpack and discard).
        let counts = emit_to.take_needed(&mut self.covar.counts);
        let _ = emit_to.take_needed(&mut self.covar.mean1s);
        let _ = emit_to.take_needed(&mut self.covar.mean2s);
        let algo_consts = emit_to.take_needed(&mut self.covar.algo_consts);
        let _ = emit_to.take_needed(&mut self.var1.counts);
        let _ = emit_to.take_needed(&mut self.var1.means);
        let m2_1s = emit_to.take_needed(&mut self.var1.m2s);
        let _ = emit_to.take_needed(&mut self.var2.counts);
        let _ = emit_to.take_needed(&mut self.var2.means);
        let m2_2s = emit_to.take_needed(&mut self.var2.m2s);

        let n = counts.len();
        let mut values = Vec::with_capacity(n);
        let mut validity = Vec::with_capacity(n);
        for i in 0..n {
            let count = counts[i];
            if count == 0.0 {
                values.push(0.0);
                validity.push(false);
                continue;
            }
            if count == 1.0 {
                if self.null_on_divide_by_zero {
                    values.push(0.0);
                    validity.push(false);
                } else {
                    values.push(f64::NAN);
                    validity.push(true);
                }
                continue;
            }
            // Population stats: divide m2 / count, c / count. The 1/count
            // factors cancel in c / (s1 * s2), so we work with raw moments.
            let s1_sq = m2_1s[i];
            let s2_sq = m2_2s[i];
            if s1_sq == 0.0 || s2_sq == 0.0 {
                values.push(0.0);
                validity.push(false);
                continue;
            }
            values.push(algo_consts[i] / (s1_sq * s2_sq).sqrt());
            validity.push(true);
        }

        Ok(Arc::new(Float64Array::new(
            values.into(),
            Some(arrow::buffer::NullBuffer::from(validity)),
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // covar.state -> [count, mean1, mean2, algo_const]
        // var1.state -> [count, mean1, m2_1]
        // var2.state -> [count, mean2, m2_2]
        // Combined state (matches Correlation::state_fields):
        // [count, mean1, mean2, algo_const, m2_1, m2_2]
        let covar_state = self.covar.state(emit_to)?;
        let var1_state = self.var1.state(emit_to)?;
        let var2_state = self.var2.state(emit_to)?;
        Ok(vec![
            Arc::clone(&covar_state[0]),
            Arc::clone(&covar_state[1]),
            Arc::clone(&covar_state[2]),
            Arc::clone(&covar_state[3]),
            Arc::clone(&var1_state[2]),
            Arc::clone(&var2_state[2]),
        ])
    }

    fn size(&self) -> usize {
        self.covar.size() + self.var1.size() + self.var2.size()
    }
}

#[cfg(test)]
mod groups_tests {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::Float64Type;

    fn acc(legacy: bool) -> CorrelationGroupsAccumulator {
        // null_on_divide_by_zero = !legacy
        CorrelationGroupsAccumulator::new(!legacy)
    }

    fn evaluate(a: &mut CorrelationGroupsAccumulator) -> Vec<Option<f64>> {
        a.evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<Float64Type>()
            .iter()
            .collect()
    }

    #[test]
    fn perfectly_correlated_single_group() {
        let mut a = acc(true);
        let v1: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        let v2: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0, 8.0, 10.0]));
        a.update_batch(&[v1, v2], &[0, 0, 0, 0, 0], None, 1)
            .unwrap();
        let r = evaluate(&mut a);
        assert!((r[0].unwrap() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn either_column_null_dropped() {
        let mut a = acc(true);
        let v1: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(3.0),
            Some(5.0),
        ]));
        let v2: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(2.0),
            Some(99.0),
            None,
            Some(10.0),
        ]));
        a.update_batch(&[v1, v2], &[0, 0, 0, 0], None, 1).unwrap();
        // surviving pairs (1,2) and (5,10) lie on y=2x => corr 1.0
        assert!((evaluate(&mut a)[0].unwrap() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn empty_group_yields_null() {
        let mut a = acc(true);
        let v1: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let v2: ArrayRef = Arc::new(Float64Array::from(vec![3.0, 6.0]));
        a.update_batch(&[v1, v2], &[0, 0], None, 2).unwrap();
        assert_eq!(evaluate(&mut a)[1], None);
    }

    #[test]
    fn single_row_legacy_mode_yields_nan() {
        // Correlation always uses Population stats internally. With one row
        // the per-row CorrelationAccumulator returns NaN when in legacy
        // (null_on_divide_by_zero=false) mode and null when the flag is set.
        let mut a = acc(true); // legacy
        let v1: ArrayRef = Arc::new(Float64Array::from(vec![42.0]));
        let v2: ArrayRef = Arc::new(Float64Array::from(vec![7.0]));
        a.update_batch(&[v1, v2], &[0], None, 1).unwrap();
        let r = evaluate(&mut a);
        assert!(r[0].unwrap().is_nan());
    }

    #[test]
    fn single_row_ansi_mode_yields_null() {
        let mut a = acc(false); // null_on_divide_by_zero = true
        let v1: ArrayRef = Arc::new(Float64Array::from(vec![42.0]));
        let v2: ArrayRef = Arc::new(Float64Array::from(vec![7.0]));
        a.update_batch(&[v1, v2], &[0], None, 1).unwrap();
        let r = evaluate(&mut a);
        assert_eq!(r[0], None);
    }
}
