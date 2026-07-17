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

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Float64Array};
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{downcast_value, unwrap_or_internal_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::expressions::format_state_name;
use datafusion::physical_expr::expressions::StatsType;
use std::mem::size_of;
use std::sync::Arc;

/// COVAR_SAMP and COVAR_POP aggregate expression
/// The implementation mostly is the same as the DataFusion's implementation. The reason
/// we have our own implementation is that DataFusion has UInt64 for state_field count,
/// while Spark has Double for count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Covariance {
    name: String,
    signature: Signature,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
}

impl std::hash::Hash for Covariance {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        (self.stats_type as u8).hash(state);
        self.null_on_divide_by_zero.hash(state);
    }
}

impl Covariance {
    /// Create a new COVAR aggregate function
    pub fn new(
        name: impl Into<String>,
        data_type: DataType,
        stats_type: StatsType,
        null_on_divide_by_zero: bool,
    ) -> Self {
        // the result of covariance just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
            stats_type,
            null_on_divide_by_zero,
        }
    }
}

impl AggregateUDFImpl for Covariance {
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
        Ok(Box::new(CovarianceAccumulator::try_new(
            self.stats_type,
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
        ])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(CovarianceGroupsAccumulator::new(
            self.stats_type,
            self.null_on_divide_by_zero,
        )))
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

            let (c, m1, m2, ac) = super::welford::covariance_update(
                self.count,
                self.mean1,
                self.mean2,
                self.algo_const,
                value1,
                value2,
            );
            self.count = c;
            self.mean1 = m1;
            self.mean2 = m2;
            self.algo_const = ac;
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

            let (c, m1, m2, ac) = super::welford::covariance_retract(
                self.count,
                self.mean1,
                self.mean2,
                self.algo_const,
                value1,
                value2,
            );
            self.count = c;
            self.mean1 = m1;
            self.mean2 = m2;
            self.algo_const = ac;
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
            let (new_count, new_mean1, new_mean2, new_c) = super::welford::covariance_merge(
                self.count,
                self.mean1,
                self.mean2,
                self.algo_const,
                c,
                means1.value(i),
                means2.value(i),
                cs.value(i),
            );
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

/// Vectorized grouped covariance accumulator. Counts are `f64` to match the
/// Spark wire format used by the per-row `CovarianceAccumulator`.
#[derive(Debug)]
pub(crate) struct CovarianceGroupsAccumulator {
    pub(super) counts: Vec<f64>,
    pub(super) mean1s: Vec<f64>,
    pub(super) mean2s: Vec<f64>,
    pub(super) algo_consts: Vec<f64>,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
}

impl CovarianceGroupsAccumulator {
    pub(crate) fn new(stats_type: StatsType, null_on_divide_by_zero: bool) -> Self {
        Self {
            counts: Vec::new(),
            mean1s: Vec::new(),
            mean2s: Vec::new(),
            algo_consts: Vec::new(),
            stats_type,
            null_on_divide_by_zero,
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.counts.resize(total_num_groups, 0.0);
        self.mean1s.resize(total_num_groups, 0.0);
        self.mean2s.resize(total_num_groups, 0.0);
        self.algo_consts.resize(total_num_groups, 0.0);
    }

    fn finalize(&mut self, emit_to: EmitTo) -> (Vec<f64>, NullBuffer) {
        let counts = emit_to.take_needed(&mut self.counts);
        let _ = emit_to.take_needed(&mut self.mean1s);
        let _ = emit_to.take_needed(&mut self.mean2s);
        let cs = emit_to.take_needed(&mut self.algo_consts);
        super::welford::finalize_moments(counts, cs, self.stats_type, self.null_on_divide_by_zero)
    }
}

impl GroupsAccumulator for CovarianceGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to update_batch");
        let v1 = cast(&values[0], &DataType::Float64)?;
        let v2 = cast(&values[1], &DataType::Float64)?;
        let v1 = v1.as_primitive::<Float64Type>();
        let v2 = v2.as_primitive::<Float64Type>();

        self.resize(total_num_groups);

        for (idx, &group_index) in group_indices.iter().enumerate() {
            if let Some(f) = opt_filter {
                if !f.is_valid(idx) || !f.value(idx) {
                    continue;
                }
            }
            if v1.is_null(idx) || v2.is_null(idx) {
                continue;
            }
            let (c, m1, m2, ac) = super::welford::covariance_update(
                self.counts[group_index],
                self.mean1s[group_index],
                self.mean2s[group_index],
                self.algo_consts[group_index],
                v1.value(idx),
                v2.value(idx),
            );
            self.counts[group_index] = c;
            self.mean1s[group_index] = m1;
            self.mean2s[group_index] = m2;
            self.algo_consts[group_index] = ac;
        }

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 4, "four arguments to merge_batch");
        let counts = downcast_value!(values[0], Float64Array);
        let means1 = downcast_value!(values[1], Float64Array);
        let means2 = downcast_value!(values[2], Float64Array);
        let cs = downcast_value!(values[3], Float64Array);

        self.resize(total_num_groups);

        for (i, &group_index) in group_indices.iter().enumerate() {
            let partial_count = counts.value(i);
            if partial_count == 0.0 {
                continue;
            }
            let (new_count, new_m1, new_m2, new_c) = super::welford::covariance_merge(
                self.counts[group_index],
                self.mean1s[group_index],
                self.mean2s[group_index],
                self.algo_consts[group_index],
                partial_count,
                means1.value(i),
                means2.value(i),
                cs.value(i),
            );
            self.counts[group_index] = new_count;
            self.mean1s[group_index] = new_m1;
            self.mean2s[group_index] = new_m2;
            self.algo_consts[group_index] = new_c;
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (values, nulls) = self.finalize(emit_to);
        Ok(Arc::new(Float64Array::new(values.into(), Some(nulls))))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let mean1s = emit_to.take_needed(&mut self.mean1s);
        let mean2s = emit_to.take_needed(&mut self.mean2s);
        let cs = emit_to.take_needed(&mut self.algo_consts);
        Ok(vec![
            Arc::new(Float64Array::new(counts.into(), None)),
            Arc::new(Float64Array::new(mean1s.into(), None)),
            Arc::new(Float64Array::new(mean2s.into(), None)),
            Arc::new(Float64Array::new(cs.into(), None)),
        ])
    }

    fn size(&self) -> usize {
        (self.counts.capacity()
            + self.mean1s.capacity()
            + self.mean2s.capacity()
            + self.algo_consts.capacity())
            * size_of::<f64>()
    }
}

#[cfg(test)]
mod groups_tests {
    use super::*;
    use arrow::array::{AsArray, Float64Array};

    fn pop() -> CovarianceGroupsAccumulator {
        CovarianceGroupsAccumulator::new(StatsType::Population, false)
    }

    fn evaluate(acc: &mut CovarianceGroupsAccumulator) -> Vec<Option<f64>> {
        acc.evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<Float64Type>()
            .iter()
            .collect()
    }

    #[test]
    fn pop_covariance_single_group() {
        let mut acc = pop();
        let v1: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        let v2: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0, 8.0, 10.0]));
        acc.update_batch(&[v1, v2], &[0, 0, 0, 0, 0], None, 1)
            .unwrap();
        // pop covariance of x and 2x for x in [1..5] = 4.0
        assert!((evaluate(&mut acc)[0].unwrap() - 4.0).abs() < 1e-12);
    }

    #[test]
    fn null_in_either_column_skipped() {
        let mut acc = pop();
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
        acc.update_batch(&[v1, v2], &[0, 0, 0, 0], None, 1).unwrap();
        // surviving pairs (1,2) and (5,10): mean(x)=3, mean(y)=6
        // pop covar = ((1-3)(2-6) + (5-3)(10-6))/2 = (8+8)/2 = 8.0
        assert!((evaluate(&mut acc)[0].unwrap() - 8.0).abs() < 1e-12);
    }

    #[test]
    fn merge_matches_singleshot() {
        let single = {
            let mut a = pop();
            let v1: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
            let v2: ArrayRef = Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0, 8.0, 10.0, 12.0]));
            a.update_batch(&[v1, v2], &[0; 6], None, 1).unwrap();
            evaluate(&mut a)[0].unwrap()
        };

        let mut left = pop();
        left.update_batch(
            &[
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
                Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0])) as ArrayRef,
            ],
            &[0, 0, 0],
            None,
            1,
        )
        .unwrap();
        let lstate = left.state(EmitTo::All).unwrap();

        let mut right = pop();
        right
            .update_batch(
                &[
                    Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0])) as ArrayRef,
                    Arc::new(Float64Array::from(vec![8.0, 10.0, 12.0])) as ArrayRef,
                ],
                &[0, 0, 0],
                None,
                1,
            )
            .unwrap();
        let rstate = right.state(EmitTo::All).unwrap();

        let mut merged = pop();
        merged.merge_batch(&lstate, &[0], None, 1).unwrap();
        merged.merge_batch(&rstate, &[0], None, 1).unwrap();
        let merged_result = evaluate(&mut merged)[0].unwrap();

        assert!((single - merged_result).abs() < 1e-12);
    }

    #[test]
    fn empty_group_yields_null() {
        let mut acc = pop();
        let v1: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        let v2: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0]));
        acc.update_batch(&[v1, v2], &[0, 0], None, 2).unwrap();
        assert_eq!(evaluate(&mut acc)[1], None);
    }
}
