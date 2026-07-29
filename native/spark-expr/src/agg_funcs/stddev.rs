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

use std::sync::Arc;

use crate::agg_funcs::variance::{VarianceAccumulator, VarianceGroupsAccumulator};
use arrow::array::{ArrayRef, AsArray, BooleanArray, Float64Array};
use arrow::datatypes::FieldRef;
use arrow::datatypes::{DataType, Field, Float64Type};
use datafusion::common::types::NativeType;
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Coercion, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::logical_expr_common::signature;
use datafusion::physical_expr::expressions::format_state_name;
use datafusion::physical_expr::expressions::StatsType;

/// STDDEV and STDDEV_SAMP (standard deviation) aggregate expression
/// The implementation mostly is the same as the DataFusion's implementation. The reason
/// we have our own implementation is that DataFusion has UInt64 for state_field `count`,
/// while Spark has Double for count. Also we have added `null_on_divide_by_zero`
/// to be consistent with Spark's implementation.
#[derive(Debug, PartialEq, Eq)]
pub struct Stddev {
    name: String,
    signature: Signature,
    stats_type: StatsType,
    null_on_divide_by_zero: bool,
}

impl std::hash::Hash for Stddev {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        (self.stats_type as u8).hash(state);
        self.null_on_divide_by_zero.hash(state);
    }
}

impl Stddev {
    /// Create a new STDDEV aggregate function
    pub fn new(
        name: impl Into<String>,
        data_type: DataType,
        stats_type: StatsType,
        null_on_divide_by_zero: bool,
    ) -> Self {
        // the result of stddev just support FLOAT64.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            signature: Signature::coercible(
                vec![Coercion::new_exact(signature::TypeSignatureClass::Native(
                    Arc::new(NativeType::Float64),
                ))],
                Volatility::Immutable,
            ),
            stats_type,
            null_on_divide_by_zero,
        }
    }
}

impl AggregateUDFImpl for Stddev {
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
        Ok(Box::new(StddevAccumulator::try_new(
            self.stats_type,
            self.null_on_divide_by_zero,
        )?))
    }

    fn create_sliding_accumulator(
        &self,
        _acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StddevAccumulator::try_new(
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
        Ok(Box::new(StddevGroupsAccumulator::new(
            self.stats_type,
            self.null_on_divide_by_zero,
        )))
    }
}

/// An accumulator to compute the standard deviation
#[derive(Debug)]
pub struct StddevAccumulator {
    variance: VarianceAccumulator,
}

impl StddevAccumulator {
    /// Creates a new `StddevAccumulator`
    pub fn try_new(s_type: StatsType, null_on_divide_by_zero: bool) -> Result<Self> {
        Ok(Self {
            variance: VarianceAccumulator::try_new(s_type, null_on_divide_by_zero)?,
        })
    }

    pub fn get_m2(&self) -> f64 {
        self.variance.get_m2()
    }
}

impl Accumulator for StddevAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.variance.get_count()),
            ScalarValue::from(self.variance.get_mean()),
            ScalarValue::from(self.variance.get_m2()),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.variance.update_batch(values)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.variance.retract_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.variance.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let variance = self.variance.evaluate()?;
        match variance {
            ScalarValue::Float64(Some(e)) => Ok(ScalarValue::Float64(Some(e.sqrt()))),
            ScalarValue::Float64(None) => Ok(ScalarValue::Float64(None)),
            _ => internal_err!("Variance should be f64"),
        }
    }

    fn size(&self) -> usize {
        std::mem::align_of_val(self) - std::mem::align_of_val(&self.variance) + self.variance.size()
    }
}

/// Stddev grouped accumulator: wraps a `VarianceGroupsAccumulator` and applies
/// `sqrt` element-wise on `evaluate`. State is identical to variance.
#[derive(Debug)]
struct StddevGroupsAccumulator {
    inner: VarianceGroupsAccumulator,
}

impl StddevGroupsAccumulator {
    fn new(stats_type: StatsType, null_on_divide_by_zero: bool) -> Self {
        Self {
            inner: VarianceGroupsAccumulator::new(stats_type, null_on_divide_by_zero),
        }
    }
}

impl GroupsAccumulator for StddevGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .merge_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let arr = self.inner.evaluate(emit_to)?;
        // Run sqrt across the buffer in place via PrimitiveArray::unary
        // rather than allocating an intermediate Vec<f64>.
        let sqrted: Float64Array = arr.as_primitive::<Float64Type>().unary(|v| v.sqrt());
        Ok(Arc::new(sqrted))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

#[cfg(test)]
mod groups_tests {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::Float64Type;

    #[test]
    fn pop_stddev_single_group() {
        let mut acc = StddevGroupsAccumulator::new(StatsType::Population, false);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        acc.update_batch(&[values], &[0, 0, 0, 0, 0], None, 1)
            .unwrap();
        // sqrt(2.0)
        let result: Vec<Option<f64>> = acc
            .evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<Float64Type>()
            .iter()
            .collect();
        assert!((result[0].unwrap() - 2.0_f64.sqrt()).abs() < 1e-12);
    }

    #[test]
    fn empty_group_yields_null() {
        let mut acc = StddevGroupsAccumulator::new(StatsType::Population, false);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        acc.update_batch(&[values], &[0, 0], None, 2).unwrap();
        let result: Vec<Option<f64>> = acc
            .evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<Float64Type>()
            .iter()
            .collect();
        assert_eq!(result[1], None);
    }

    #[test]
    fn sample_single_row_nan_legacy() {
        // Stddev wraps variance, but pin the contract here too: legacy mode
        // (null_on_divide_by_zero = false) emits sqrt(NaN) = NaN for a single
        // sample-stddev row.
        let mut acc = StddevGroupsAccumulator::new(StatsType::Sample, false);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![42.0]));
        acc.update_batch(&[values], &[0], None, 1).unwrap();
        let result: Vec<Option<f64>> = acc
            .evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<Float64Type>()
            .iter()
            .collect();
        assert!(result[0].unwrap().is_nan());
    }

    #[test]
    fn sample_single_row_null_when_flag_set() {
        let mut acc = StddevGroupsAccumulator::new(StatsType::Sample, true);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![42.0]));
        acc.update_batch(&[values], &[0], None, 1).unwrap();
        let result: Vec<Option<f64>> = acc
            .evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<Float64Type>()
            .iter()
            .collect();
        assert_eq!(result[0], None);
    }
}
