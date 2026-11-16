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
    builder::PrimitiveBuilder,
    cast::AsArray,
    types::{Float64Type, Int64Type},
    Array, ArrayRef, ArrowNumericType, BooleanArray, Int64Array, PrimitiveArray,
};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::compute::sum;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{not_impl_err, Result, ScalarValue};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature,
};
use datafusion::physical_expr::expressions::format_state_name;
use std::sync::Arc;

use super::collect::filter_to_nulls;

use arrow::array::ArrowNativeTypeOp;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use DataType::*;

fn avg_return_type(_name: &str, data_type: &DataType) -> Result<DataType> {
    match data_type {
        Float64 => Ok(Float64),
        _ => not_impl_err!("Avg return type for {data_type}"),
    }
}

/// AVG aggregate expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Avg {
    name: String,
    signature: Signature,
    // expr: Arc<dyn PhysicalExpr>,
    input_data_type: DataType,
    result_data_type: DataType,
}

impl Avg {
    /// Create a new AVG aggregate function
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        let result_data_type = avg_return_type("avg", &data_type).unwrap();

        Self {
            name: name.into(),
            signature: Signature::user_defined(Immutable),
            input_data_type: data_type,
            result_data_type,
        }
    }
}

impl AggregateUDFImpl for Avg {
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // All numeric types use Float64 accumulation after casting
        match (&self.input_data_type, &self.result_data_type) {
            (Float64, Float64) => Ok(Box::<AvgAccumulator>::default()),
            _ => not_impl_err!(
                "AvgAccumulator for ({} --> {})",
                self.input_data_type,
                self.result_data_type
            ),
        }
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(&self.name, "sum"),
                self.input_data_type.clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "count"),
                DataType::Int64,
                true,
            )),
        ])
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        match (&self.input_data_type, &self.result_data_type) {
            (Float64, Float64) => Ok(Box::new(AvgGroupsAccumulator::<Float64Type, _>::new(
                &self.input_data_type,
                |sum: f64, count: i64| Ok(sum / count as f64),
            ))),

            _ => not_impl_err!(
                "AvgGroupsAccumulator for ({} --> {})",
                self.input_data_type,
                self.result_data_type
            ),
        }
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        avg_return_type(self.name(), &arg_types[0])
    }
}

/// An accumulator to compute the average
#[derive(Debug, Default)]
pub struct AvgAccumulator {
    /// Spark's partial buffer uses zero for empty input; count determines result nulls.
    sum: f64,
    count: i64,
}

impl Accumulator for AvgAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.sum)),
            ScalarValue::from(self.count),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.count += (values.len() - values.null_count()) as i64;
        if let Some(x) = sum(values) {
            self.sum += x;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[1].as_primitive::<Int64Type>()).unwrap_or_default();

        // sums are summed - no overflow checking in all Eval Modes
        if let Some(x) = sum(states[0].as_primitive::<Float64Type>()) {
            self.sum += x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some(self.sum / self.count as f64)))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// An accumulator to compute the average of `[PrimitiveArray<T>]`.
/// Stores values as native types (
/// no overflow check all eval modes since inf is a perfectly valid value per spark impl)
///
/// F: Function that calculates the average value from a sum of
/// T::Native and a total count
#[derive(Debug)]
struct AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, i64) -> Result<T::Native> + Send,
{
    /// The type of the returned average
    return_data_type: DataType,

    /// Count per group (use i64 to make Int64Array)
    counts: Vec<i64>,

    /// Sums per group, stored as the native type
    sums: Vec<T::Native>,

    /// Function that computes the final average (value / count)
    avg_fn: F,
}

impl<T, F> AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, i64) -> Result<T::Native> + Send,
{
    pub fn new(return_data_type: &DataType, avg_fn: F) -> Self {
        Self {
            return_data_type: return_data_type.clone(),
            counts: vec![],
            sums: vec![],
            avg_fn,
        }
    }
}

impl<T, F> GroupsAccumulator for AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, i64) -> Result<T::Native> + Send + 'static,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();
        let data = values.values();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::default_value());

        let iter = group_indices.iter().zip(data.iter());
        if opt_filter.is_none() && values.null_count() == 0 {
            for (&group_index, &value) in iter {
                let sum = &mut self.sums[group_index];
                // No overflow checking - Infinity is a valid result
                *sum = (*sum).add_wrapping(value);
                self.counts[group_index] += 1;
            }
        } else {
            for (idx, (&group_index, &value)) in iter.enumerate() {
                if let Some(f) = opt_filter {
                    if !f.is_valid(idx) || !f.value(idx) {
                        continue;
                    }
                }
                if values.is_null(idx) {
                    continue;
                }
                let sum = &mut self.sums[group_index];
                *sum = (*sum).add_wrapping(value);

                self.counts[group_index] += 1;
            }
        }

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is partial sums, second is counts
        let partial_sums = values[0].as_primitive::<T>();
        let partial_counts = values[1].as_primitive::<Int64Type>();
        // update counts with partial counts
        self.counts.resize(total_num_groups, 0);
        let iter1 = group_indices.iter().zip(partial_counts.values().iter());
        for (&group_index, &partial_count) in iter1 {
            self.counts[group_index] += partial_count;
        }

        // update sums - no overflow checking (in all eval modes)
        self.sums.resize(total_num_groups, T::default_value());
        let iter2 = group_indices.iter().zip(partial_sums.values().iter());
        for (&group_index, &new_value) in iter2 {
            let sum = &mut self.sums[group_index];
            *sum = sum.add_wrapping(new_value);
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let mut builder = PrimitiveBuilder::<T>::with_capacity(sums.len());
        let iter = sums.into_iter().zip(counts);

        for (sum, count) in iter {
            if count != 0 {
                builder.append_value((self.avg_fn)(sum, count)?)
            } else {
                builder.append_null();
            }
        }
        let array: PrimitiveArray<T> = builder.finish();

        Ok(Arc::new(array))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let counts = Int64Array::new(counts.into(), None);

        let sums = emit_to.take_needed(&mut self.sums);
        let sums = PrimitiveArray::<T>::new(sums.into(), None)
            .with_data_type(self.return_data_type.clone());

        Ok(vec![
            Arc::new(sums) as ArrayRef,
            Arc::new(counts) as ArrayRef,
        ])
    }

    /// Each row's own partial state, as `state` emits it: the value and a count of one, or a
    /// zero sum and count for a null or filtered-out row.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = values[0].as_primitive::<T>();
        let filter_nulls = opt_filter.map(filter_to_nulls);
        let mask = NullBuffer::union(filter_nulls.as_ref(), values.nulls());
        let (sums, counts): (ScalarBuffer<T::Native>, ScalarBuffer<i64>) = match mask {
            // Every row is included, so the input values are the sums as they are.
            None => (values.values().clone(), vec![1; values.len()].into()),
            // `merge_batch` reads the sums' values without their nulls, so an excluded row
            // needs a zero sum as well as a zero count.
            Some(mask) => {
                let (sums, counts): (Vec<_>, Vec<_>) = values
                    .values()
                    .iter()
                    .zip(mask.iter())
                    .map(|(&value, included)| {
                        if included {
                            (value, 1)
                        } else {
                            (T::default_value(), 0)
                        }
                    })
                    .unzip();
                (sums.into(), counts.into())
            }
        };
        let sums =
            PrimitiveArray::<T>::new(sums, None).with_data_type(self.return_data_type.clone());
        Ok(vec![
            Arc::new(sums) as ArrayRef,
            Arc::new(Int64Array::new(counts, None)) as ArrayRef,
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<i64>()
            + self.sums.capacity() * std::mem::size_of::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;

    #[test]
    fn empty_partial_state_matches_spark() -> Result<()> {
        let mut partial = AvgAccumulator::default();
        let empty = vec![ScalarValue::Float64(Some(0.0)), ScalarValue::Int64(Some(0))];
        assert_eq!(partial.state()?, empty);
        assert_eq!(partial.evaluate()?, ScalarValue::Float64(None));

        partial.update_batch(&[Arc::new(Float64Array::from(vec![None, None]))])?;
        assert_eq!(partial.state()?, empty);
        assert_eq!(partial.evaluate()?, ScalarValue::Float64(None));
        Ok(())
    }

    /// Converting rows straight to partial states, as a partial aggregate that stops aggregating
    /// does, gives each row the state that aggregating it alone into a group would.
    #[test]
    fn convert_to_state_matches_one_row_groups() -> Result<()> {
        let new_accumulator = || {
            AvgGroupsAccumulator::<Float64Type, _>::new(&Float64, |sum: f64, count: i64| {
                Ok(sum / count as f64)
            })
        };
        let check = |values: ArrayRef, filter: Option<&BooleanArray>| -> Result<()> {
            let converted = new_accumulator().convert_to_state(&[Arc::clone(&values)], filter)?;
            let groups: Vec<usize> = (0..values.len()).collect();
            let mut grouped = new_accumulator();
            grouped.update_batch(&[values], &groups, filter, groups.len())?;
            assert_eq!(converted, grouped.state(EmitTo::All)?);
            Ok(())
        };

        let values = Float64Array::from(vec![
            Some(1.5),
            None,
            Some(-2.0),
            Some(4.0),
            Some(8.0),
            None,
        ]);
        let no_nulls = Float64Array::from(vec![1.5, -2.0, 4.0, 8.0]);
        // A null predicate, as `FILTER (WHERE d > 1)` gives where `d` is null, excludes its row.
        let filter = BooleanArray::from(vec![
            Some(true),
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(true),
        ]);

        for values in [values.clone(), no_nulls.clone()] {
            let filter = filter.slice(0, values.len());
            check(Arc::new(values.clone()), None)?;
            check(Arc::new(values), Some(&filter))?;
        }
        // Sliced inputs and filters, whose buffers start at an offset.
        let sliced = values.slice(1, 3);
        let sliced_filter = filter.slice(1, 3);
        check(Arc::new(sliced.clone()), None)?;
        check(Arc::new(sliced), Some(&sliced_filter))?;
        check(Arc::new(no_nulls.slice(1, 3)), None)?;
        check(Arc::new(no_nulls.slice(1, 3)), Some(&filter.slice(2, 3)))?;
        Ok(())
    }
}
