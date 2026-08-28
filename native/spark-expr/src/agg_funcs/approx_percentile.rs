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

use super::quantile_summaries::{QuantileSummaries, QuantileSummariesScratch};
use arrow::array::{
    new_empty_array, Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Float64Array,
    ListArray,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{downcast_value, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature,
};
use datafusion::physical_expr::expressions::format_state_name;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

/// Native implementation of Spark's `approx_percentile` / `percentile_approx`,
/// backed by a bit-for-bit `QuantileSummaries` (Greenwald-Khanna) port. The
/// child value is cast to Float64 by the serde; the original `input_type` is
/// carried so results can be cast back to Spark's output type.
#[derive(Debug)]
pub struct ApproxPercentile {
    name: String,
    signature: Signature,
    percentiles: Vec<f64>,
    accuracy: i64,
    input_type: DataType,
    return_array: bool,
}

impl PartialEq for ApproxPercentile {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.percentiles == other.percentiles
            && self.accuracy == other.accuracy
            && self.input_type == other.input_type
            && self.return_array == other.return_array
    }
}
impl Eq for ApproxPercentile {}

impl std::hash::Hash for ApproxPercentile {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.percentiles
            .iter()
            .for_each(|p| p.to_bits().hash(state));
        self.accuracy.hash(state);
        self.input_type.hash(state);
        self.return_array.hash(state);
    }
}

impl ApproxPercentile {
    pub fn new(
        percentiles: Vec<f64>,
        accuracy: i64,
        input_type: DataType,
        return_array: bool,
    ) -> Self {
        Self {
            name: "approx_percentile".to_string(),
            signature: Signature::numeric(1, Immutable),
            percentiles,
            accuracy,
            input_type,
            return_array,
        }
    }
}

impl AggregateUDFImpl for ApproxPercentile {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        if self.return_array {
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                self.input_type.clone(),
                false,
            ))))
        } else {
            Ok(self.input_type.clone())
        }
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ApproxPercentileAccumulator::new(
            self.percentiles.clone(),
            self.accuracy,
            self.input_type.clone(),
            self.return_array,
        )))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(&self.name, "digest"),
            DataType::Binary,
            true,
        ))])
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(ApproxPercentileGroupsAccumulator::new(
            self.percentiles.clone(),
            self.accuracy,
            self.input_type.clone(),
            self.return_array,
        )))
    }
}

#[derive(Debug)]
struct ApproxPercentileAccumulator {
    summary: QuantileSummaries,
    scratch: QuantileSummariesScratch,
    percentiles: Vec<f64>,
    input_type: DataType,
    return_array: bool,
}

impl ApproxPercentileAccumulator {
    fn new(percentiles: Vec<f64>, accuracy: i64, input_type: DataType, return_array: bool) -> Self {
        let relative_error = 1.0 / accuracy as f64;
        Self {
            summary: QuantileSummaries::new(
                QuantileSummaries::DEFAULT_COMPRESS_THRESHOLD,
                relative_error,
            ),
            scratch: QuantileSummariesScratch::default(),
            percentiles,
            input_type,
            return_array,
        }
    }
}

/// Cast a double quantile back to Spark's output type. GK always returns an
/// inserted value, so every supported numeric type round-trips exactly.
fn cast_back(input_type: &DataType, value: f64) -> ScalarValue {
    match input_type {
        DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
        DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
        DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
        DataType::Int64 => ScalarValue::Int64(Some(value as i64)),
        DataType::Float32 => ScalarValue::Float32(Some(value as f32)),
        DataType::Float64 => ScalarValue::Float64(Some(value)),
        // The serde only marks byte/short/int/long/float/double as supported.
        other => unreachable!("unsupported approx_percentile input type: {other}"),
    }
}

fn null_result(input_type: &DataType, return_array: bool) -> Result<ScalarValue> {
    if return_array {
        Ok(ScalarValue::List(Arc::new(ListArray::new_null(
            Arc::new(Field::new("item", input_type.clone(), false)),
            1,
        ))))
    } else {
        Ok(ScalarValue::try_from(input_type)?)
    }
}

fn evaluate_summary(
    summary: &mut QuantileSummaries,
    scratch: &mut QuantileSummariesScratch,
    percentiles: &[f64],
    input_type: &DataType,
    return_array: bool,
) -> Result<ScalarValue> {
    summary.compress(scratch);
    let results = match summary.query(percentiles) {
        Some(results) if !results.is_empty() => results,
        _ => return null_result(input_type, return_array),
    };
    let scalars = results
        .into_iter()
        .map(|value| cast_back(input_type, value));
    if return_array {
        let values = ScalarValue::iter_to_array(scalars)?;
        Ok(SingleRowListArrayBuilder::new(values)
            .with_nullable(false)
            .build_list_scalar())
    } else {
        Ok(scalars.into_iter().next().unwrap())
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = downcast_value!(&values[0], Float64Array);
        self.summary.reserve(arr.len() - arr.null_count());
        if arr.null_count() == 0 {
            // Fast path: no validity checks needed, iterate the raw values.
            for &v in arr.values() {
                self.summary.insert(v, &mut self.scratch);
            }
        } else {
            for v in arr.iter().flatten() {
                self.summary.insert(v, &mut self.scratch);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let digests = downcast_value!(&states[0], BinaryArray);
        self.summary.compress(&mut self.scratch);
        for i in 0..digests.len() {
            if digests.is_null(i) {
                continue;
            }
            let peer = QuantileSummaries::from_bytes(
                QuantileSummaries::DEFAULT_COMPRESS_THRESHOLD,
                digests.value(i),
            );
            if self.summary.count() == 0 {
                // Move the already-owned first digest into the accumulator.
                self.summary = peer;
            } else {
                self.summary.merge(&peer, &mut self.scratch);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.summary.compress(&mut self.scratch);
        Ok(vec![ScalarValue::Binary(Some(self.summary.to_bytes()))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        evaluate_summary(
            &mut self.summary,
            &mut self.scratch,
            &self.percentiles,
            &self.input_type,
            self.return_array,
        )
    }

    fn size(&self) -> usize {
        // Summary and scratch swap buffers during flush/compress/merge, but
        // each allocation is owned by exactly one of them at any time, so the
        // two heap_size terms never double-count.
        size_of_val(self)
            + self.summary.heap_size()
            + self.scratch.heap_size()
            + self.percentiles.capacity() * size_of::<f64>()
    }
}

#[derive(Debug)]
struct ApproxPercentileGroupsAccumulator {
    summaries: Vec<QuantileSummaries>,
    scratch: QuantileSummariesScratch,
    summaries_heap_size: usize,
    relative_error: f64,
    percentiles: Vec<f64>,
    input_type: DataType,
    return_array: bool,
}

impl ApproxPercentileGroupsAccumulator {
    fn new(percentiles: Vec<f64>, accuracy: i64, input_type: DataType, return_array: bool) -> Self {
        Self {
            summaries: Vec::new(),
            scratch: QuantileSummariesScratch::default(),
            summaries_heap_size: 0,
            relative_error: 1.0 / accuracy as f64,
            percentiles,
            input_type,
            return_array,
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        let relative_error = self.relative_error;
        self.summaries.resize_with(total_num_groups, || {
            QuantileSummaries::new(
                QuantileSummaries::DEFAULT_COMPRESS_THRESHOLD,
                relative_error,
            )
        });
    }

    fn take_needed(&mut self, emit_to: EmitTo) -> Vec<QuantileSummaries> {
        let summaries = emit_to.take_needed(&mut self.summaries);
        let emitted_size = summaries
            .iter()
            .map(QuantileSummaries::heap_size)
            .sum::<usize>();
        self.summaries_heap_size = self.summaries_heap_size.saturating_sub(emitted_size);
        summaries
    }

    fn output_type(&self) -> DataType {
        if self.return_array {
            DataType::List(Arc::new(Field::new("item", self.input_type.clone(), false)))
        } else {
            self.input_type.clone()
        }
    }
}

fn selected(filter: Option<&BooleanArray>, row: usize) -> bool {
    match filter {
        Some(filter) => filter.is_valid(row) && filter.value(row),
        None => true,
    }
}

fn adjust_size(total: &mut usize, before: usize, after: usize) {
    if after >= before {
        *total += after - before;
    } else {
        *total = total.saturating_sub(before - after);
    }
}

impl GroupsAccumulator for ApproxPercentileGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let values = downcast_value!(&values[0], Float64Array);
        self.resize(total_num_groups);
        let (summaries, scratch, summaries_heap_size) = (
            &mut self.summaries,
            &mut self.scratch,
            &mut self.summaries_heap_size,
        );
        for (row, &group_index) in group_indices.iter().enumerate() {
            if !selected(opt_filter, row) || values.is_null(row) {
                continue;
            }
            let summary = &mut summaries[group_index];
            let before = summary.heap_size();
            summary.insert(values.value(row), scratch);
            adjust_size(summaries_heap_size, before, summary.heap_size());
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let digests = downcast_value!(&states[0], BinaryArray);
        self.resize(total_num_groups);
        let (summaries, scratch, summaries_heap_size) = (
            &mut self.summaries,
            &mut self.scratch,
            &mut self.summaries_heap_size,
        );
        for (row, &group_index) in group_indices.iter().enumerate() {
            if !selected(opt_filter, row) || digests.is_null(row) {
                continue;
            }
            let peer = QuantileSummaries::from_bytes(
                QuantileSummaries::DEFAULT_COMPRESS_THRESHOLD,
                digests.value(row),
            );
            let summary = &mut summaries[group_index];
            let before = summary.heap_size();
            summary.compress(scratch);
            if summary.count() == 0 {
                *summary = peer;
            } else {
                summary.merge(&peer, scratch);
            }
            adjust_size(summaries_heap_size, before, summary.heap_size());
        }
        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let summaries = self.take_needed(emit_to);
        let mut builder = BinaryBuilder::new();
        for mut summary in summaries {
            summary.compress(&mut self.scratch);
            builder.append_value(summary.to_bytes());
        }
        Ok(vec![Arc::new(builder.finish())])
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let summaries = self.take_needed(emit_to);
        if summaries.is_empty() {
            return Ok(new_empty_array(&self.output_type()));
        }
        let results = summaries
            .into_iter()
            .map(|mut summary| {
                evaluate_summary(
                    &mut summary,
                    &mut self.scratch,
                    &self.percentiles,
                    &self.input_type,
                    self.return_array,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        ScalarValue::iter_to_array(results)
    }

    fn size(&self) -> usize {
        // `summaries_heap_size` is the running sum of every group's
        // `QuantileSummaries::heap_size`; the single shared scratch buffer is
        // counted once here and never inside a summary, so nothing is
        // double-counted when buffers swap between them.
        size_of_val(self)
            + self.summaries.capacity() * size_of::<QuantileSummaries>()
            + self.summaries_heap_size
            + self.scratch.heap_size()
            + self.percentiles.capacity() * size_of::<f64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f64_array(v: Vec<f64>) -> ArrayRef {
        Arc::new(Float64Array::from(v))
    }

    fn grouped_values(start: i32) -> (ArrayRef, Vec<usize>) {
        let values = (start..start + 100)
            .map(|value| value as f64)
            .chain((start + 1000..start + 1100).map(|value| value as f64))
            .collect();
        (
            f64_array(values),
            vec![0; 100].into_iter().chain(vec![1; 100]).collect(),
        )
    }

    #[test]
    fn scalar_median_of_int_column() {
        let mut acc = ApproxPercentileAccumulator::new(vec![0.5], 10000, DataType::Int32, false);
        acc.update_batch(&[f64_array((1..=100).map(|i| i as f64).collect())])
            .unwrap();
        match acc.evaluate().unwrap() {
            ScalarValue::Int32(Some(v)) => assert!((49..=51).contains(&v)),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn array_of_percentiles() {
        let mut acc =
            ApproxPercentileAccumulator::new(vec![0.25, 0.5, 0.75], 10000, DataType::Float64, true);
        acc.update_batch(&[f64_array((1..=1000).map(|i| i as f64).collect())])
            .unwrap();
        match acc.evaluate().unwrap() {
            ScalarValue::List(arr) => assert_eq!(arr.value_length(0), 3),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn empty_input_is_null() {
        let mut acc = ApproxPercentileAccumulator::new(vec![0.5], 10000, DataType::Int64, false);
        assert!(acc.evaluate().unwrap().is_null());
    }

    #[test]
    fn array_output_empty_input_is_null() {
        let mut acc =
            ApproxPercentileAccumulator::new(vec![0.25, 0.5, 0.75], 10000, DataType::Float64, true);
        assert!(acc.evaluate().unwrap().is_null());
    }

    #[test]
    fn empty_percentiles_is_null() {
        // An empty percentage array yields null in Spark even with data present.
        let mut acc = ApproxPercentileAccumulator::new(vec![], 10000, DataType::Float64, true);
        acc.update_batch(&[f64_array((1..=1000).map(|i| i as f64).collect())])
            .unwrap();
        assert!(acc.evaluate().unwrap().is_null());
    }

    #[test]
    fn state_then_merge_matches_single_shot() {
        let mut single =
            ApproxPercentileAccumulator::new(vec![0.5], 10000, DataType::Float64, false);
        single
            .update_batch(&[f64_array((1..=1000).map(|i| i as f64).collect())])
            .unwrap();
        let single_val = single.evaluate().unwrap();

        let mut left = ApproxPercentileAccumulator::new(vec![0.5], 10000, DataType::Float64, false);
        left.update_batch(&[f64_array((1..=500).map(|i| i as f64).collect())])
            .unwrap();
        let left_state = left.state().unwrap();

        let mut right =
            ApproxPercentileAccumulator::new(vec![0.5], 10000, DataType::Float64, false);
        right
            .update_batch(&[f64_array((501..=1000).map(|i| i as f64).collect())])
            .unwrap();
        let right_state = right.state().unwrap();

        let mut merged =
            ApproxPercentileAccumulator::new(vec![0.5], 10000, DataType::Float64, false);
        merged
            .merge_batch(&[ScalarValue::iter_to_array(left_state).unwrap()])
            .unwrap();
        merged
            .merge_batch(&[ScalarValue::iter_to_array(right_state).unwrap()])
            .unwrap();
        let merged_val = merged.evaluate().unwrap();

        // Both within the same accuracy bound of the true median (~500).
        for v in [single_val, merged_val] {
            match v {
                ScalarValue::Float64(Some(x)) => assert!((450.0..=550.0).contains(&x)),
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn grouped_update_filter_and_partial_emit() {
        let mut acc =
            ApproxPercentileGroupsAccumulator::new(vec![0.5], 10000, DataType::Float64, false);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(3.0),
            None,
            Some(10.0),
            Some(20.0),
        ]));
        let filter = BooleanArray::from(vec![true, true, true, true, false]);
        acc.update_batch(&[values], &[0, 0, 1, 2, 2], Some(&filter), 4)
            .unwrap();

        let first = acc.evaluate(EmitTo::First(2)).unwrap();
        let first = first.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((1.0..=3.0).contains(&first.value(0)));
        assert!(first.is_null(1));

        let rest = acc.evaluate(EmitTo::All).unwrap();
        let rest = rest.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(rest.value(0), 10.0);
        assert!(rest.is_null(1));
    }

    #[test]
    fn grouped_merge_reuses_one_scratch_buffer() {
        let mut left =
            ApproxPercentileGroupsAccumulator::new(vec![0.5], 10000, DataType::Float64, false);
        let (values, groups) = grouped_values(1);
        left.update_batch(&[values], &groups, None, 2).unwrap();
        let left_state = left.state(EmitTo::All).unwrap();

        let mut right =
            ApproxPercentileGroupsAccumulator::new(vec![0.5], 10000, DataType::Float64, false);
        let (values, groups) = grouped_values(101);
        right.update_batch(&[values], &groups, None, 2).unwrap();
        let right_state = right.state(EmitTo::All).unwrap();

        let mut merged =
            ApproxPercentileGroupsAccumulator::new(vec![0.5], 10000, DataType::Float64, false);
        merged.merge_batch(&left_state, &[0, 1], None, 2).unwrap();
        let filter = BooleanArray::from(vec![Some(true), None]);
        merged
            .merge_batch(&right_state, &[0, 1], Some(&filter), 2)
            .unwrap();

        assert!(merged.scratch.heap_size() > 0);
        assert_eq!(
            merged.summaries_heap_size,
            merged
                .summaries
                .iter()
                .map(QuantileSummaries::heap_size)
                .sum::<usize>()
        );

        let result = merged.evaluate(EmitTo::All).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((90.0..=110.0).contains(&result.value(0)));
        assert!((1040.0..=1060.0).contains(&result.value(1)));
    }

    #[test]
    fn grouped_array_result() {
        let mut acc = ApproxPercentileGroupsAccumulator::new(
            vec![0.25, 0.75],
            10000,
            DataType::Float64,
            true,
        );
        acc.update_batch(&[f64_array(vec![1.0, 2.0, 3.0])], &[0, 0, 0], None, 1)
            .unwrap();
        let result = acc.evaluate(EmitTo::All).unwrap();
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(result.value_length(0), 2);
    }
}
