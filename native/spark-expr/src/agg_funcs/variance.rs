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

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Float64Array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{downcast_value, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature,
};
use datafusion::physical_expr::expressions::format_state_name;
use datafusion::physical_expr::expressions::StatsType;
use std::mem::size_of;
use std::sync::Arc;

/// VAR_SAMP and VAR_POP aggregate expression
/// The implementation mostly is the same as the DataFusion's implementation. The reason
/// we have our own implementation is that DataFusion has UInt64 for state_field `count`,
/// while Spark has Double for count. Also we have added `null_on_divide_by_zero`
/// to be consistent with Spark's implementation.
#[derive(Debug, PartialEq, Eq)]
pub struct Variance {
    name: String,
    signature: Signature,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
}

impl std::hash::Hash for Variance {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        (self.stats_type as u8).hash(state);
        self.null_on_divide_by_zero.hash(state);
    }
}

impl Variance {
    /// Create a new VARIANCE aggregate function
    pub fn new(
        name: impl Into<String>,
        data_type: DataType,
        stats_type: StatsType,
        null_on_divide_by_zero: bool,
    ) -> Self {
        // the result of variance just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            signature: Signature::numeric(1, Immutable),
            stats_type,
            null_on_divide_by_zero,
        }
    }
}

impl AggregateUDFImpl for Variance {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VarianceAccumulator::try_new(
            self.stats_type,
            self.null_on_divide_by_zero,
        )?))
    }

    fn create_sliding_accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VarianceAccumulator::try_new(
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
                format_state_name(&self.name, "mean"),
                DataType::Float64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "m2"),
                DataType::Float64,
                true,
            )),
        ])
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(VarianceGroupsAccumulator::new(
            self.stats_type,
            self.null_on_divide_by_zero,
        )))
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
            let (c, m, m2) = super::welford::variance_update(self.count, self.mean, self.m2, value);
            self.count = c;
            self.mean = m;
            self.m2 = m2;
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = downcast_value!(&values[0], Float64Array).iter().flatten();

        for value in arr {
            let (c, m, m2) =
                super::welford::variance_retract(self.count, self.mean, self.m2, value);
            self.count = c;
            self.mean = m;
            self.m2 = m2;
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
            let (new_count, new_mean, new_m2) = super::welford::variance_merge(
                self.count,
                self.mean,
                self.m2,
                c,
                means.value(i),
                m2s.value(i),
            );
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
            0.0 => None,
            count if count == 1.0 && StatsType::Sample == self.stats_type => {
                if self.null_on_divide_by_zero {
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

/// Vectorized grouped variance accumulator. Mirrors the per-row
/// `VarianceAccumulator` but runs the Welford recurrence per group_index.
#[derive(Debug)]
pub(crate) struct VarianceGroupsAccumulator {
    pub(super) counts: Vec<f64>,
    pub(super) means: Vec<f64>,
    pub(super) m2s: Vec<f64>,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
}

impl VarianceGroupsAccumulator {
    pub(crate) fn new(stats_type: StatsType, null_on_divide_by_zero: bool) -> Self {
        Self {
            counts: Vec::new(),
            means: Vec::new(),
            m2s: Vec::new(),
            stats_type,
            null_on_divide_by_zero,
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.counts.resize(total_num_groups, 0.0);
        self.means.resize(total_num_groups, 0.0);
        self.m2s.resize(total_num_groups, 0.0);
    }

    fn finalize(&mut self, emit_to: EmitTo) -> (Vec<f64>, NullBuffer) {
        let counts = emit_to.take_needed(&mut self.counts);
        let _ = emit_to.take_needed(&mut self.means);
        let m2s = emit_to.take_needed(&mut self.m2s);
        super::welford::finalize_moments(counts, m2s, self.stats_type, self.null_on_divide_by_zero)
    }
}

impl GroupsAccumulator for VarianceGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<Float64Type>();

        self.resize(total_num_groups);

        for (idx, (&group_index, &value)) in
            group_indices.iter().zip(values.values().iter()).enumerate()
        {
            if let Some(f) = opt_filter {
                if !f.is_valid(idx) || !f.value(idx) {
                    continue;
                }
            }
            if values.is_null(idx) {
                continue;
            }
            let (c, m, m2) = super::welford::variance_update(
                self.counts[group_index],
                self.means[group_index],
                self.m2s[group_index],
                value,
            );
            self.counts[group_index] = c;
            self.means[group_index] = m;
            self.m2s[group_index] = m2;
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
        assert_eq!(values.len(), 3, "three arguments to merge_batch");
        let partial_counts = downcast_value!(values[0], Float64Array);
        let partial_means = downcast_value!(values[1], Float64Array);
        let partial_m2s = downcast_value!(values[2], Float64Array);

        self.resize(total_num_groups);

        for (i, &group_index) in group_indices.iter().enumerate() {
            let partial_count = partial_counts.value(i);
            if partial_count == 0.0 {
                continue;
            }
            let (new_count, new_mean, new_m2) = super::welford::variance_merge(
                self.counts[group_index],
                self.means[group_index],
                self.m2s[group_index],
                partial_count,
                partial_means.value(i),
                partial_m2s.value(i),
            );
            self.counts[group_index] = new_count;
            self.means[group_index] = new_mean;
            self.m2s[group_index] = new_m2;
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (values, nulls) = self.finalize(emit_to);
        Ok(Arc::new(Float64Array::new(values.into(), Some(nulls))))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let means = emit_to.take_needed(&mut self.means);
        let m2s = emit_to.take_needed(&mut self.m2s);
        Ok(vec![
            Arc::new(Float64Array::new(counts.into(), None)),
            Arc::new(Float64Array::new(means.into(), None)),
            Arc::new(Float64Array::new(m2s.into(), None)),
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * size_of::<f64>()
            + self.means.capacity() * size_of::<f64>()
            + self.m2s.capacity() * size_of::<f64>()
    }
}

#[cfg(test)]
mod groups_tests {
    use super::*;
    use arrow::array::{AsArray, Float64Array};

    fn pop_acc() -> VarianceGroupsAccumulator {
        VarianceGroupsAccumulator::new(StatsType::Population, false)
    }
    fn sample_acc(null_on_divide_by_zero: bool) -> VarianceGroupsAccumulator {
        VarianceGroupsAccumulator::new(StatsType::Sample, null_on_divide_by_zero)
    }

    fn evaluate(acc: &mut VarianceGroupsAccumulator) -> Vec<Option<f64>> {
        acc.evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<Float64Type>()
            .iter()
            .collect()
    }

    #[test]
    fn pop_variance_single_group() {
        let mut acc = pop_acc();
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        acc.update_batch(&[values], &[0, 0, 0, 0, 0], None, 1)
            .unwrap();
        // population variance of [1..5] = 2.0
        assert_eq!(evaluate(&mut acc), vec![Some(2.0)]);
    }

    #[test]
    fn pop_variance_multi_group() {
        let mut acc = pop_acc();
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 10.0, 20.0, 3.0]));
        acc.update_batch(&[values], &[0, 0, 1, 1, 0], None, 2)
            .unwrap();
        let result = evaluate(&mut acc);
        // group 0: pop var of [1,2,3] = 2/3; group 1: pop var of [10,20] = 25
        assert!((result[0].unwrap() - 2.0_f64 / 3.0).abs() < 1e-12);
        assert!((result[1].unwrap() - 25.0).abs() < 1e-12);
    }

    #[test]
    fn null_values_are_ignored() {
        let mut acc = pop_acc();
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(2.0),
            Some(3.0),
            None,
        ]));
        acc.update_batch(&[values], &[0, 0, 0, 0, 0], None, 1)
            .unwrap();
        // pop var of [1,2,3] = 2/3
        assert!((evaluate(&mut acc)[0].unwrap() - 2.0_f64 / 3.0).abs() < 1e-12);
    }

    #[test]
    fn opt_filter_applied() {
        let mut acc = pop_acc();
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        let filter = BooleanArray::from(vec![true, false, true, false]);
        acc.update_batch(&[values], &[0, 0, 0, 0], Some(&filter), 1)
            .unwrap();
        // pop var of [1,3] = 1.0
        assert!((evaluate(&mut acc)[0].unwrap() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn empty_group_yields_null() {
        let mut acc = pop_acc();
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        acc.update_batch(&[values], &[0, 0], None, 2).unwrap();
        let result = evaluate(&mut acc);
        assert_eq!(result[1], None);
    }

    #[test]
    fn sample_single_row_nan_legacy() {
        let mut acc = sample_acc(false);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![42.0]));
        acc.update_batch(&[values], &[0], None, 1).unwrap();
        let result = evaluate(&mut acc);
        assert!(result[0].unwrap().is_nan());
    }

    #[test]
    fn sample_single_row_null_when_flag_set() {
        let mut acc = sample_acc(true);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![42.0]));
        acc.update_batch(&[values], &[0], None, 1).unwrap();
        assert_eq!(evaluate(&mut acc), vec![None]);
    }

    #[test]
    fn merge_matches_singleshot() {
        let values_full: ArrayRef =
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
        let groups_full: Vec<usize> = vec![0; 6];

        let mut single = pop_acc();
        single
            .update_batch(std::slice::from_ref(&values_full), &groups_full, None, 1)
            .unwrap();
        let single_result = evaluate(&mut single)[0].unwrap();

        let mut left = pop_acc();
        let lvals: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        left.update_batch(&[lvals], &[0, 0, 0], None, 1).unwrap();
        let left_state = left.state(EmitTo::All).unwrap();

        let mut right = pop_acc();
        let rvals: ArrayRef = Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0]));
        right.update_batch(&[rvals], &[0, 0, 0], None, 1).unwrap();
        let right_state = right.state(EmitTo::All).unwrap();

        let mut merged = pop_acc();
        merged.merge_batch(&left_state, &[0], None, 1).unwrap();
        merged.merge_batch(&right_state, &[0], None, 1).unwrap();
        let merged_result = evaluate(&mut merged)[0].unwrap();

        assert!((single_result - merged_result).abs() < 1e-12);
    }
}
