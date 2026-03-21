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

use arrow::array::{Array, ArrayRef, BinaryArray, Float64Array, IntervalDayTimeArray, IntervalYearMonthArray};
use arrow::datatypes::{DataType, Field, FieldRef, IntervalDayTimeType, IntervalUnit};
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
        match &self.return_type {
            DataType::Float64 => Ok(ScalarValue::Float64(None)),
            DataType::Interval(IntervalUnit::YearMonth) => Ok(ScalarValue::IntervalYearMonth(None)),
            DataType::Interval(IntervalUnit::DayTime) => Ok(ScalarValue::IntervalDayTime(None)),
            _ => Ok(ScalarValue::Float64(None)),
        }
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

        // Get sorted (value, accumulated_count) pairs
        let sorted_counts: Vec<(i64, i64)> = if self.reverse {
            self.counts.iter().rev().map(|(&k, &v)| (k, v)).collect()
        } else {
            self.counts.iter().map(|(&k, &v)| (k, v)).collect()
        };

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
            DataType::Interval(IntervalUnit::YearMonth) => {
                // Values are i32 months stored as i64
                let lower_months = lower_key as i32;
                let higher_months = higher_key as i32;
                let result = (1.0 - fraction) * (lower_months as f64) + fraction * (higher_months as f64);
                Some(result.round() as i64)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                // Values are packed as (days << 32) | milliseconds
                let lower_days = (lower_key >> 32) as i32;
                let lower_ms = lower_key as i32;
                let higher_days = (higher_key >> 32) as i32;
                let higher_ms = higher_key as i32;

                // Convert to total milliseconds for interpolation
                let lower_total_ms = (lower_days as i64) * 86_400_000 + (lower_ms as i64);
                let higher_total_ms = (higher_days as i64) * 86_400_000 + (higher_ms as i64);
                let result_ms = ((1.0 - fraction) * (lower_total_ms as f64) + fraction * (higher_total_ms as f64)).round() as i64;

                // Convert back to days and milliseconds
                let result_days = (result_ms / 86_400_000) as i32;
                let result_remaining_ms = (result_ms % 86_400_000) as i32;

                Some(((result_days as i64) << 32) | (result_remaining_ms as i64 & 0xFFFFFFFF))
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

        match array.data_type() {
            DataType::Float64 => {
                let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..values.len() {
                    if values.is_null(i) {
                        continue;
                    }
                    let key = values.value(i).to_bits() as i64;
                    *self.counts.entry(key).or_insert(0) += 1;
                }
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                let values = array.as_any().downcast_ref::<IntervalYearMonthArray>().unwrap();
                for i in 0..values.len() {
                    if values.is_null(i) {
                        continue;
                    }
                    let key = values.value(i) as i64;
                    *self.counts.entry(key).or_insert(0) += 1;
                }
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let values = array.as_any().downcast_ref::<IntervalDayTimeArray>().unwrap();
                for i in 0..values.len() {
                    if values.is_null(i) {
                        continue;
                    }
                    // Convert IntervalDayTime struct to packed i64: (days << 32) | milliseconds
                    let (days, ms) = IntervalDayTimeType::to_parts(values.value(i));
                    let key = ((days as i64) << 32) | (ms as i64 & 0xFFFFFFFF);
                    *self.counts.entry(key).or_insert(0) += 1;
                }
            }
            _ => {
                // Fallback: try to treat as Float64
                if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
                    for i in 0..values.len() {
                        if values.is_null(i) {
                            continue;
                        }
                        let key = values.value(i).to_bits() as i64;
                        *self.counts.entry(key).or_insert(0) += 1;
                    }
                }
            }
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
            Some(value) => match &self.return_type {
                DataType::Float64 => Ok(ScalarValue::Float64(Some(f64::from_bits(value as u64)))),
                DataType::Interval(IntervalUnit::YearMonth) => {
                    Ok(ScalarValue::IntervalYearMonth(Some(value as i32)))
                }
                DataType::Interval(IntervalUnit::DayTime) => {
                    // Unpack i64 to (days, milliseconds) and create IntervalDayTime struct
                    let days = (value >> 32) as i32;
                    let ms = value as i32;
                    Ok(ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(days, ms))))
                }
                _ => Ok(ScalarValue::Float64(Some(f64::from_bits(value as u64)))),
            },
            None => match &self.return_type {
                DataType::Float64 => Ok(ScalarValue::Float64(None)),
                DataType::Interval(IntervalUnit::YearMonth) => Ok(ScalarValue::IntervalYearMonth(None)),
                DataType::Interval(IntervalUnit::DayTime) => Ok(ScalarValue::IntervalDayTime(None)),
                _ => Ok(ScalarValue::Float64(None)),
            },
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
    fn test_percentile_year_month_interval() {
        let mut acc = PercentileAccumulator::new(0.5, false, DataType::Interval(IntervalUnit::YearMonth));
        let values: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![0, 10, 20, 30, 40]));
        acc.update_batch(&[values]).unwrap();

        let result = acc.evaluate().unwrap();
        assert_eq!(result, ScalarValue::IntervalYearMonth(Some(20)));
    }

    #[test]
    fn test_percentile_day_time_interval() {
        let mut acc = PercentileAccumulator::new(0.5, false, DataType::Interval(IntervalUnit::DayTime));
        // Create intervals: 1 day, 2 days, 3 days, 4 days, 5 days (no milliseconds)
        let values: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(1, 0),
            IntervalDayTimeType::make_value(2, 0),
            IntervalDayTimeType::make_value(3, 0),
            IntervalDayTimeType::make_value(4, 0),
            IntervalDayTimeType::make_value(5, 0),
        ]));
        acc.update_batch(&[values]).unwrap();

        let result = acc.evaluate().unwrap();
        // Median should be 3 days
        assert_eq!(result, ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(3, 0))));
    }
}
