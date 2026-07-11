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

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Float64Builder, ListArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{internal_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature,
};
use datafusion::physical_expr::expressions::format_state_name;
use std::cmp::Ordering;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkPercentile {
    signature: Signature,
    percentile_bits: u64,
}

impl SparkPercentile {
    pub fn try_new(percentile: f64) -> Result<Self> {
        validate_percentile(percentile)?;
        Ok(Self {
            signature: Signature::user_defined(Immutable),
            percentile_bits: percentile.to_bits(),
        })
    }

    fn percentile(&self) -> f64 {
        f64::from_bits(self.percentile_bits)
    }
}

impl AggregateUDFImpl for SparkPercentile {
    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SparkPercentileAccumulator::new(self.percentile())))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let input_type = args.input_fields[0].data_type();
        if input_type != &DataType::Float64 {
            return internal_err!("SparkPercentile expects Float64 input, got {input_type}");
        }
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, self.name()),
            DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
            true,
        ))])
    }

    fn name(&self) -> &str {
        "percentile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.first() != Some(&DataType::Float64) {
            return internal_err!(
                "SparkPercentile return type expects Float64 input, got {:?}",
                arg_types.first()
            );
        }
        Ok(DataType::Float64)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct && args.expr_fields[0].data_type() == &DataType::Float64
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(SparkPercentileGroupsAccumulator::new(
            self.percentile(),
        )))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn is_nullable(&self) -> bool {
        true
    }
}

fn validate_percentile(percentile: f64) -> Result<()> {
    if !(0.0..=1.0).contains(&percentile) {
        return plan_err!(
            "Percentile value must be between 0.0 and 1.0 inclusive, got {percentile}."
        );
    }
    Ok(())
}

#[derive(Debug)]
struct SparkPercentileAccumulator {
    values: Vec<f64>,
    percentile: f64,
}

impl SparkPercentileAccumulator {
    fn new(percentile: f64) -> Self {
        Self {
            values: vec![],
            percentile,
        }
    }
}

impl Accumulator for SparkPercentileAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, self.values.len() as i32]));
        let values = Float64Array::new(ScalarBuffer::from(self.values.clone()), None);
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            offsets,
            Arc::new(values),
            None,
        );
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.values.reserve(values.len() - values.null_count());
        self.values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states = states[0].as_list::<i32>();
        for state in states.iter().flatten() {
            self.update_batch(&[state])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(spark_percentile(
            self.values.as_mut_slice(),
            self.percentile,
        )))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * size_of::<f64>()
    }
}

#[derive(Debug)]
struct SparkPercentileGroupsAccumulator {
    group_values: Vec<Vec<f64>>,
    percentile: f64,
}

impl SparkPercentileGroupsAccumulator {
    fn new(percentile: f64) -> Self {
        Self {
            group_values: vec![],
            percentile,
        }
    }
}

impl GroupsAccumulator for SparkPercentileGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.group_values.resize(total_num_groups, Vec::new());

        for (row, &group_index) in group_indices.iter().enumerate() {
            if let Some(filter) = opt_filter {
                if !filter.is_valid(row) || !filter.value(row) {
                    continue;
                }
            }
            if values.is_null(row) {
                continue;
            }
            self.group_values[group_index].push(values.value(row));
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
        let input_group_values = values[0].as_list::<i32>();
        self.group_values.resize(total_num_groups, Vec::new());

        for (&group_index, values) in group_indices.iter().zip(input_group_values.iter()) {
            if let Some(values) = values {
                let values = values.as_primitive::<Float64Type>();
                self.group_values[group_index].extend(values.iter().flatten());
            }
        }

        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let emit_group_values = emit_to.take_needed(&mut self.group_values);

        let mut offsets = Vec::with_capacity(emit_group_values.len() + 1);
        offsets.push(0);
        let mut len = 0_i32;
        for values in &emit_group_values {
            len += values.len() as i32;
            offsets.push(len);
        }

        let values = emit_group_values.into_iter().flatten().collect::<Vec<_>>();
        let values = Float64Array::new(ScalarBuffer::from(values), None);
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(values),
            None,
        );
        Ok(vec![Arc::new(list)])
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let mut emit_group_values = emit_to.take_needed(&mut self.group_values);
        let mut builder = Float64Builder::with_capacity(emit_group_values.len());
        for values in &mut emit_group_values {
            builder.append_option(spark_percentile(values.as_mut_slice(), self.percentile));
        }
        Ok(Arc::new(builder.finish()))
    }

    fn size(&self) -> usize {
        self.group_values
            .iter()
            .map(|values| values.capacity() * size_of::<f64>())
            .sum::<usize>()
            + self.group_values.capacity() * size_of::<Vec<f64>>()
    }
}

fn spark_percentile(values: &mut [f64], percentile: f64) -> Option<f64> {
    let len = values.len();
    if len == 0 {
        return None;
    }
    if len == 1 {
        return Some(values[0]);
    }

    let position = (len - 1) as f64 * percentile;
    let lower = position.floor() as usize;
    let higher = position.ceil() as usize;

    let (_, lower_value, _) = values.select_nth_unstable_by(lower, spark_double_cmp);
    let lower_value = *lower_value;
    if lower == higher {
        return Some(lower_value);
    }

    let (_, higher_value, _) = values.select_nth_unstable_by(higher, spark_double_cmp);
    let higher_value = *higher_value;
    if spark_double_cmp(&lower_value, &higher_value) == Ordering::Equal {
        return Some(lower_value);
    }

    Some((higher as f64 - position) * lower_value + (position - lower as f64) * higher_value)
}

fn spark_double_cmp(x: &f64, y: &f64) -> Ordering {
    if x == y || (x.is_nan() && y.is_nan()) {
        Ordering::Equal
    } else if x.is_nan() {
        Ordering::Greater
    } else if y.is_nan() {
        Ordering::Less
    } else {
        x.partial_cmp(y)
            .expect("non-NaN values should be comparable")
    }
}

#[cfg(test)]
mod tests {
    use super::{spark_double_cmp, spark_percentile};
    use std::cmp::Ordering;

    #[test]
    fn interpolates_with_full_spark_precision() {
        let mut values = vec![0.0, 10_000_000.0];
        assert_eq!(
            spark_percentile(&mut values, 0.123456789),
            Some(1_234_567.89)
        );
    }

    #[test]
    fn matches_spark_double_ordering_for_nan_and_zero() {
        assert_eq!(spark_double_cmp(&f64::NAN, &1.0), Ordering::Greater);
        assert_eq!(spark_double_cmp(&1.0, &f64::NAN), Ordering::Less);
        assert_eq!(spark_double_cmp(&f64::NAN, &f64::NAN), Ordering::Equal);
        assert_eq!(spark_double_cmp(&-0.0, &0.0), Ordering::Equal);
    }
}
