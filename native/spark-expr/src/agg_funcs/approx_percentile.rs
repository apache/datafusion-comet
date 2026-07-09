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

use super::quantile_summaries::QuantileSummaries;
use arrow::array::{Array, ArrayRef, BinaryArray, Float64Array, ListArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{downcast_value, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature};
use datafusion::physical_expr::expressions::format_state_name;
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

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }
}

#[derive(Debug)]
struct ApproxPercentileAccumulator {
    summary: QuantileSummaries,
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
            percentiles,
            input_type,
            return_array,
        }
    }

    /// Cast a double quantile back to Spark's output type. GK always returns an
    /// actual inserted value (never an interpolation), so for the supported
    /// numeric types this round-trips exactly and is always in range.
    fn cast_back(&self, d: f64) -> ScalarValue {
        match &self.input_type {
            DataType::Int8 => ScalarValue::Int8(Some(d as i8)),
            DataType::Int16 => ScalarValue::Int16(Some(d as i16)),
            DataType::Int32 => ScalarValue::Int32(Some(d as i32)),
            DataType::Int64 => ScalarValue::Int64(Some(d as i64)),
            DataType::Float32 => ScalarValue::Float32(Some(d as f32)),
            DataType::Float64 => ScalarValue::Float64(Some(d)),
            // The serde only marks byte/short/int/long/float/double as
            // supported, so no other type reaches the accumulator.
            other => unreachable!("unsupported approx_percentile input type: {other}"),
        }
    }

    /// The null Spark produces for an empty result: a typed null scalar, or a
    /// null list when the call returns an array of percentiles.
    fn null_result(&self) -> Result<ScalarValue> {
        if self.return_array {
            Ok(ScalarValue::List(Arc::new(ListArray::new_null(
                Arc::new(Field::new("item", self.input_type.clone(), false)),
                1,
            ))))
        } else {
            Ok(ScalarValue::try_from(&self.input_type)?)
        }
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = downcast_value!(&values[0], Float64Array);
        self.summary.reserve(arr.len() - arr.null_count());
        if arr.null_count() == 0 {
            // Fast path: no validity checks needed, iterate the raw values.
            for &v in arr.values() {
                self.summary.insert(v);
            }
        } else {
            for v in arr.iter().flatten() {
                self.summary.insert(v);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let digests = downcast_value!(&states[0], BinaryArray);
        self.summary.compress();
        for i in 0..digests.len() {
            if digests.is_null(i) {
                continue;
            }
            let peer = QuantileSummaries::from_bytes(
                QuantileSummaries::DEFAULT_COMPRESS_THRESHOLD,
                digests.value(i),
            );
            if self.summary.count() == 0 {
                // Empty self: `merge` would return a clone of the (potentially
                // large) peer, so move the owned peer in and skip the clone.
                self.summary = peer;
            } else {
                self.summary = self.summary.merge(&peer);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.summary.compress();
        Ok(vec![ScalarValue::Binary(Some(self.summary.to_bytes()))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.summary.compress();
        // Spark returns null whenever the result would be empty, i.e. no rows
        // were aggregated (`query` returns `None`) or the percentage argument
        // was an empty array (`query` returns `Some([])`).
        let results = match self.summary.query(&self.percentiles) {
            Some(r) if !r.is_empty() => r,
            _ => return self.null_result(),
        };
        let scalars: Vec<ScalarValue> = results.into_iter().map(|d| self.cast_back(d)).collect();
        if self.return_array {
            let values = ScalarValue::iter_to_array(scalars)?;
            Ok(SingleRowListArrayBuilder::new(values)
                .with_nullable(false)
                .build_list_scalar())
        } else {
            Ok(scalars.into_iter().next().unwrap())
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.summary.heap_size()
            + self.percentiles.capacity() * std::mem::size_of::<f64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f64_array(v: Vec<f64>) -> ArrayRef {
        Arc::new(Float64Array::from(v))
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
}
