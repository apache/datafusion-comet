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

//! Spark-compatible Percentile aggregate function.
//!
//! This implementation matches Spark's `Percentile` class intermediate state format,
//! which uses a serialized map of (value -> frequency) stored as BinaryType.

use arrow::array::{Array, ArrayRef, BinaryArray, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature};
use datafusion::physical_expr::expressions::format_state_name;
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;

/// Spark-compatible Percentile aggregate function.
///
/// Stores intermediate state as BinaryType containing serialized (value, count) pairs,
/// matching Spark's `TypedAggregateWithHashMapAsBuffer` format.
#[derive(Debug, Clone, PartialEq)]
pub struct Percentile {
    name: String,
    signature: Signature,
    /// Percentile value stored as bits for Hash/Eq
    percentile_bits: u64,
    reverse: bool,
    /// The return data type
    return_type: DataType,
}

impl std::hash::Hash for Percentile {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.percentile_bits.hash(state);
        self.reverse.hash(state);
    }
}

impl Eq for Percentile {}

impl Percentile {
    pub fn new(name: impl Into<String>, percentile: f64, reverse: bool, return_type: DataType) -> Self {
        Self {
            name: name.into(),
            signature: Signature::any(1, Immutable),
            percentile_bits: percentile.to_bits(),
            reverse,
            return_type,
        }
    }

    fn percentile(&self) -> f64 {
        f64::from_bits(self.percentile_bits)
    }
}

impl AggregateUDFImpl for Percentile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Match Spark's BinaryType state format
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "counts"),
            DataType::Binary,
            true,
        ))])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(PercentileAccumulator::new(
            self.percentile(),
            self.reverse,
            self.return_type.clone(),
        )))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }
}

/// Accumulator for Percentile that stores (value -> count) map.
/// Values are stored as i64 regardless of input type to simplify the implementation.
#[derive(Debug)]
pub struct PercentileAccumulator {
    /// Map of value (as i64 bits) -> frequency count (using BTreeMap for sorted iteration)
    counts: BTreeMap<i64, i64>,
    /// The percentile to compute (0.0 to 1.0)
    percentile: f64,
    /// Whether to reverse the order (for DESC)
    reverse: bool,
    /// The return data type
    return_type: DataType,
}

impl PercentileAccumulator {
    pub fn new(percentile: f64, reverse: bool, return_type: DataType) -> Self {
        Self {
            counts: BTreeMap::new(),
            percentile,
            reverse,
            return_type,
        }
    }

    /// Serialize the counts map to Spark's binary format.
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        for (&key, &count) in &self.counts {
            // Each entry: [size: i32][key: i64][count: i64]
            // Size = 8 (i64) + 8 (i64) = 16 bytes
            let size: i32 = 16;
            buf.extend_from_slice(&size.to_be_bytes());
            buf.extend_from_slice(&key.to_be_bytes());
            buf.extend_from_slice(&count.to_be_bytes());
        }

        // End marker
        buf.extend_from_slice(&(-1i32).to_be_bytes());
        buf
    }

    /// Deserialize counts map from Spark's binary format.
    fn deserialize(bytes: &[u8]) -> Result<BTreeMap<i64, i64>> {
        let mut counts = BTreeMap::new();
        let mut offset = 0;

        while offset + 4 <= bytes.len() {
            let size = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;

            if size < 0 {
                // End marker
                break;
            }

            if offset + 16 > bytes.len() {
                break;
            }

            let key = i64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let count = i64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;

            counts.insert(key, count);
        }

        Ok(counts)
    }

    /// Compute the percentile from the accumulated counts.
    /// Returns the result as i64 bits (can be interpreted as f64 bits or interval value).
    fn compute_percentile_i64(&self) -> Option<i64> {
        if self.counts.is_empty() {
            return None;
        }

        // Collect entries and sort by actual f64 value (not bit pattern).
        // We can't rely on BTreeMap's i64 ordering because f64 bit patterns
        // don't preserve numeric ordering for negative values.
        let mut entries: Vec<(f64, i64)> = self
            .counts
            .iter()
            .map(|(&bits, &count)| (f64::from_bits(bits as u64), count))
            .collect();

        // Sort by f64 value
        entries.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        if self.reverse {
            entries.reverse();
        }

        // Convert back to (bits, count) for the rest of the computation
        let sorted_counts: Vec<(i64, i64)> = entries
            .into_iter()
            .map(|(f, count)| (f.to_bits() as i64, count))
            .collect();

        // Compute accumulated counts
        let mut accumulated: Vec<(i64, i64)> = Vec::with_capacity(sorted_counts.len());
        let mut total: i64 = 0;
        for (value, count) in sorted_counts {
            total += count;
            accumulated.push((value, total));
        }

        let total_count = total;
        if total_count == 0 {
            return None;
        }

        // Position in the distribution (0-indexed)
        let position = (total_count - 1) as f64 * self.percentile;
        let lower = position.floor() as i64;
        let higher = position.ceil() as i64;

        // Binary search for lower and higher indices
        let lower_idx = Self::binary_search_count(&accumulated, lower + 1);
        let higher_idx = Self::binary_search_count(&accumulated, higher + 1);

        let lower_key = accumulated[lower_idx].0;

        if higher == lower {
            // No interpolation needed
            return Some(lower_key);
        }

        let higher_key = accumulated[higher_idx].0;

        if lower_key == higher_key {
            // Same key, no interpolation needed
            return Some(lower_key);
        }

        // Linear interpolation
        let fraction = position - lower as f64;

        // Handle interpolation based on return type
        match &self.return_type {
            DataType::Float64 => {
                // Interpret i64 bits as f64
                let lower_f = f64::from_bits(lower_key as u64);
                let higher_f = f64::from_bits(higher_key as u64);
                let result = (1.0 - fraction) * lower_f + fraction * higher_f;
                Some(result.to_bits() as i64)
            }
            _ => Some(lower_key),
        }
    }

    /// Binary search to find the index where accumulated count >= target
    fn binary_search_count(accumulated: &[(i64, i64)], target: i64) -> usize {
        match accumulated.binary_search_by(|(_, count)| count.cmp(&target)) {
            Ok(idx) => idx,
            Err(idx) => idx.min(accumulated.len() - 1),
        }
    }
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        let values = array.as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..values.len() {
            if values.is_null(i) {
                continue;
            }
            let key = values.value(i).to_bits() as i64;
            *self.counts.entry(key).or_insert(0) += 1;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let binary_array = states[0].as_any().downcast_ref::<BinaryArray>().unwrap();

        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                continue;
            }
            let bytes = binary_array.value(i);
            let other_counts = Self::deserialize(bytes)?;

            for (key, count) in other_counts {
                *self.counts.entry(key).or_insert(0) += count;
            }
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let bytes = self.serialize();
        Ok(vec![ScalarValue::Binary(Some(bytes))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.compute_percentile_i64() {
            Some(value) => Ok(ScalarValue::Float64(Some(f64::from_bits(value as u64)))),
            None => Ok(ScalarValue::Float64(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.counts.len() * (std::mem::size_of::<i64>() * 2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use std::sync::Arc;

    #[test]
    fn test_percentile_median() {
        let mut acc = PercentileAccumulator::new(0.5, false, DataType::Float64);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![0.0, 10.0, 20.0, 30.0, 40.0]));
        acc.update_batch(&[values]).unwrap();

        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Float64(Some(20.0)));
    }

    #[test]
    fn test_percentile_25th() {
        let mut acc = PercentileAccumulator::new(0.25, false, DataType::Float64);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![0.0, 10.0, 20.0, 30.0, 40.0]));
        acc.update_batch(&[values]).unwrap();

        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Float64(Some(10.0)));
    }

    #[test]
    fn test_percentile_serialize_deserialize() {
        let mut acc = PercentileAccumulator::new(0.5, false, DataType::Float64);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        acc.update_batch(&[values]).unwrap();

        let state = acc.state().unwrap();
        let bytes = match &state[0] {
            ScalarValue::Binary(Some(b)) => b.clone(),
            _ => panic!("Expected Binary state"),
        };

        let deserialized = PercentileAccumulator::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.len(), 3);
    }

    #[test]
    fn test_percentile_reverse() {
        // With DESC ordering, 25th percentile should equal 75th percentile of ASC
        let mut acc_asc = PercentileAccumulator::new(0.75, false, DataType::Float64);
        let mut acc_desc = PercentileAccumulator::new(0.25, true, DataType::Float64);

        let values: ArrayRef = Arc::new(Float64Array::from(vec![0.0, 10.0, 20.0, 30.0, 40.0]));
        acc_asc.update_batch(&[values.clone()]).unwrap();
        acc_desc.update_batch(&[values]).unwrap();

        let result_asc = acc_asc.evaluate().unwrap();
        let result_desc = acc_desc.evaluate().unwrap();
        assert_eq!(result_asc, result_desc);
    }

    #[test]
    fn test_percentile_negative_values() {
        // Test that negative values are sorted correctly
        // Values: -100, -50, 50, 100
        // Sorted: -100, -50, 50, 100
        // Median (50th percentile) with 4 values:
        // position = (4-1) * 0.5 = 1.5
        // lower_idx = 1 (-50), upper_idx = 2 (50)
        // result = 0.5 * (-50) + 0.5 * 50 = 0
        let mut acc = PercentileAccumulator::new(0.5, false, DataType::Float64);
        let values: ArrayRef =
            Arc::new(Float64Array::from(vec![-100.0, -50.0, 50.0, 100.0]));
        acc.update_batch(&[values]).unwrap();

        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Float64(Some(0.0)));
    }

    #[test]
    fn test_percentile_all_negative() {
        // Test all negative values
        // Values: -50, -20, 0, 10, 30
        // Sorted: -50, -20, 0, 10, 30
        // Median (50th percentile) with 5 values:
        // position = (5-1) * 0.5 = 2
        // result = value at index 2 = 0
        let mut acc = PercentileAccumulator::new(0.5, false, DataType::Float64);
        let values: ArrayRef =
            Arc::new(Float64Array::from(vec![-50.0, -20.0, 0.0, 10.0, 30.0]));
        acc.update_batch(&[values]).unwrap();

        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Float64(Some(0.0)));
    }

    #[test]
    fn test_percentile_negative_25th() {
        // Test 25th percentile with negative values
        // Values: -100, -50, 50, 100
        // position = (4-1) * 0.25 = 0.75
        // lower_idx = 0 (-100), upper_idx = 1 (-50)
        // result = 0.25 * (-100) + 0.75 * (-50) = -25 + -37.5 = -62.5
        // Actually: (1-0.75)*(-100) + 0.75*(-50) = 0.25*(-100) + 0.75*(-50) = -25 - 37.5 = -62.5
        let mut acc = PercentileAccumulator::new(0.25, false, DataType::Float64);
        let values: ArrayRef =
            Arc::new(Float64Array::from(vec![-100.0, -50.0, 50.0, 100.0]));
        acc.update_batch(&[values]).unwrap();

        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Float64(Some(-62.5)));
    }
}
