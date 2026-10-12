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

use crate::float_semantics::compare_floats;
use crate::hash_funcs::murmur3::spark_compatible_murmur3_hash;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Float64Builder, ListArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{internal_err, not_impl_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature,
};
use datafusion::physical_expr::expressions::format_state_name;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkPercentile {
    signature: Signature,
    percentile_bits: u64,
    /// Whether Spark's input was `FLOAT` before the serde cast it to `DOUBLE`. Spark hashes the
    /// values as floats then, which decides which signed zero it reaches first.
    float_input: bool,
}

impl SparkPercentile {
    pub fn try_new(percentile: f64, float_input: bool) -> Result<Self> {
        validate_percentile(percentile)?;
        Ok(Self {
            signature: Signature::user_defined(Immutable),
            percentile_bits: percentile.to_bits(),
            float_input,
        })
    }

    fn percentile(&self) -> f64 {
        f64::from_bits(self.percentile_bits)
    }
}

impl AggregateUDFImpl for SparkPercentile {
    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SparkPercentileAccumulator::new(
            self.percentile(),
            self.float_input,
        )))
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
            self.float_input,
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
    float_input: bool,
}

impl SparkPercentileAccumulator {
    fn new(percentile: f64, float_input: bool) -> Self {
        Self {
            values: vec![],
            percentile,
            float_input,
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
            self.float_input,
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
    float_input: bool,
}

impl SparkPercentileGroupsAccumulator {
    fn new(percentile: f64, float_input: bool) -> Self {
        Self {
            group_values: vec![],
            percentile,
            float_input,
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

        // `len` is the total number of values, which `flatten().collect()` has no size hint for.
        let mut values = Vec::with_capacity(len as usize);
        for group in emit_group_values {
            values.extend_from_slice(&group);
        }
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
            builder.append_option(spark_percentile(
                values.as_mut_slice(),
                self.percentile,
                self.float_input,
            ));
        }
        Ok(Arc::new(builder.finish()))
    }

    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
    }

    fn size(&self) -> usize {
        self.group_values
            .iter()
            .map(|values| values.capacity() * size_of::<f64>())
            .sum::<usize>()
            + self.group_values.capacity() * size_of::<Vec<f64>>()
    }
}

fn spark_percentile(values: &mut [f64], percentile: f64, float_input: bool) -> Option<f64> {
    let len = values.len();
    if len == 0 {
        return None;
    }
    if len == 1 {
        return Some(values[0]);
    }
    // Read before the selection below reorders the values.
    let zeros = SignedZeros::find(values, float_input);

    let position = (len - 1) as f64 * percentile;
    let lower = position.floor() as usize;
    let higher = position.ceil() as usize;

    let (_, lower_value, _) = values.select_nth_unstable_by(lower, |x, y| compare_floats(*x, *y));
    let lower_value = zeros.resolve(lower, *lower_value);
    if lower == higher {
        return Some(lower_value);
    }

    let (_, higher_value, _) = values.select_nth_unstable_by(higher, |x, y| compare_floats(*x, *y));
    let higher_value = zeros.resolve(higher, *higher_value);
    if compare_floats(lower_value, higher_value).is_eq() {
        return Some(lower_value);
    }

    Some((higher as f64 - position) * lower_value + (position - lower as f64) * higher_value)
}

/// Where `0.0` and `-0.0` sit in Spark's sorted order, when the values hold both.
///
/// Spark counts the values in an `OpenHashMap`, which keeps the two zeros as separate keys
/// (SPARK-45599), then sorts the keys with a stable sort under SQL ordering, where they are
/// equal. So the zero that Spark's hash table iterates first takes the lower ranks, and which
/// one that is depends on the table's capacity and collisions.
#[derive(Debug, Default)]
struct SignedZeros {
    /// The rank of the first zero, `None` unless both signs are present.
    start: Option<usize>,
    /// How many values have the sign Spark reaches first.
    first_count: usize,
    first_negative: bool,
}

impl SignedZeros {
    fn find(values: &[f64], float_input: bool) -> Self {
        if !values.iter().any(|v| v.to_bits() == NEGATIVE_ZERO_BITS) {
            return Self::default();
        }
        let (mut below, mut negative, mut positive) = (0, 0, 0);
        for &v in values {
            if v == 0.0 {
                if v.is_sign_negative() {
                    negative += 1;
                } else {
                    positive += 1;
                }
            } else if v < 0.0 {
                below += 1;
            }
        }
        if positive == 0 {
            return Self::default();
        }
        let first_negative = spark_iterates_negative_zero_first(values, float_input);
        Self {
            start: Some(below),
            first_count: if first_negative { negative } else { positive },
            first_negative,
        }
    }

    /// The zero Spark has at `rank`, or `value` itself when it is not a zero.
    fn resolve(&self, rank: usize, value: f64) -> f64 {
        match self.start {
            Some(start) if value == 0.0 => {
                if (rank < start + self.first_count) == self.first_negative {
                    -0.0
                } else {
                    0.0
                }
            }
            _ => value,
        }
    }
}

const NEGATIVE_ZERO_BITS: u64 = 0x8000_0000_0000_0000;

/// Whether `-0.0` comes before `0.0` in Spark's final percentile buffer.
///
/// Spark inserts the rows into the partial buffer, serializes it in iteration order,
/// deserializes it into a new map, and merges that into the final buffer, again in iteration
/// order. This replays those three tables. Values merged from several partial buffers are
/// treated as one, since even Spark's answer then depends on the order they arrive in.
fn spark_iterates_negative_zero_first(values: &[f64], float_input: bool) -> bool {
    let mut table = SparkKeySet::new(float_input);
    values.iter().for_each(|&v| table.add(v));
    for _ in 0..2 {
        let mut next = SparkKeySet::new(float_input);
        table.keys().for_each(|v| next.add(v));
        table = next;
    }
    let first_zero = table.keys().find(|&v| v == 0.0);
    first_zero.is_some_and(f64::is_sign_negative)
}

/// The slot layout of Spark's `OpenHashSet` with the default capacity and load factor, holding
/// boxed `Double` or `Float` keys.
struct SparkKeySet {
    slots: Vec<Option<f64>>,
    size: usize,
    float_input: bool,
}

impl SparkKeySet {
    const INITIAL_CAPACITY: usize = 64;
    const LOAD_FACTOR: f64 = 0.7;

    fn new(float_input: bool) -> Self {
        Self {
            slots: vec![None; Self::INITIAL_CAPACITY],
            size: 0,
            float_input,
        }
    }

    /// `Double.doubleToLongBits` or `Float.floatToIntBits`, which `equals` and `hashCode` use:
    /// the bits of the value, with every NaN turned into the same one.
    fn key_bits(&self, v: f64) -> u64 {
        match (self.float_input, v.is_nan()) {
            (true, true) => f32::NAN.to_bits() as u64,
            (true, false) => (v as f32).to_bits() as u64,
            (false, true) => f64::NAN.to_bits(),
            (false, false) => v.to_bits(),
        }
    }

    /// `Double.hashCode` or `Float.hashCode`, mixed with murmur3 as `OpenHashSet.hashcode` does.
    fn slot(&self, v: f64, mask: usize) -> usize {
        let bits = self.key_bits(v);
        let hash = (bits ^ (bits >> 32)) as i32;
        spark_compatible_murmur3_hash(hash.to_le_bytes(), 0) as usize & mask
    }

    fn add(&mut self, v: f64) {
        let mask = self.slots.len() - 1;
        let bits = self.key_bits(v);
        let mut pos = self.slot(v, mask);
        let mut delta = 1;
        while let Some(existing) = self.slots[pos] {
            if self.key_bits(existing) == bits {
                return;
            }
            pos = (pos + delta) & mask;
            delta += 1;
        }
        self.slots[pos] = Some(v);
        self.size += 1;
        if self.size > (Self::LOAD_FACTOR * self.slots.len() as f64) as usize {
            self.grow();
        }
    }

    fn grow(&mut self) {
        let capacity = self.slots.len() * 2;
        let old = std::mem::replace(&mut self.slots, vec![None; capacity]);
        let mask = self.slots.len() - 1;
        for v in old.into_iter().flatten() {
            let mut pos = self.slot(v, mask);
            let mut delta = 1;
            while self.slots[pos].is_some() {
                pos = (pos + delta) & mask;
                delta += 1;
            }
            self.slots[pos] = Some(v);
        }
    }

    fn keys(&self) -> impl Iterator<Item = f64> + '_ {
        self.slots.iter().flatten().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::spark_percentile;

    #[test]
    fn interpolates_with_full_spark_precision() {
        let mut values = vec![0.0, 10_000_000.0];
        assert_eq!(
            spark_percentile(&mut values, 0.123456789, false),
            Some(1_234_567.89)
        );
    }

    #[test]
    fn orders_nan_above_every_other_value() {
        // A NaN with the sign bit set, which IEEE 754 total order would put first.
        let mut values = vec![f64::from_bits(0xfff8_0000_0000_0000), 2.0, 1.0];
        assert_eq!(spark_percentile(&mut values, 0.0, false), Some(1.0));
        assert!(spark_percentile(&mut values, 1.0, false).unwrap().is_nan());
    }

    // The rows of Spark's SPARK-45599 test, for which Spark returns -0.0.
    fn spark_45599_rows() -> Vec<f64> {
        vec![
            0.0,
            2.0,
            153.0,
            168.0,
            3252411229536261.0,
            7.205759403792794e16,
            f64::MAX,
            0.25,
            f64::NAN,
            f64::NAN,
            -0.0,
            -128.0,
            f64::NAN,
            f64::NAN,
        ]
    }

    #[test]
    fn picks_the_zero_spark_iterates_first() {
        for float_input in [false, true] {
            let mut values = spark_45599_rows();
            let result = spark_percentile(&mut values, 0.1, float_input).unwrap();
            assert_eq!(result.to_bits(), (-0.0_f64).to_bits());
        }
    }

    #[test]
    fn follows_the_table_as_it_grows() {
        // -0.0 comes first in Spark's 64-slot table, and 0.0 once it has grown to 1024 slots,
        // which it does past 358 keys.
        for (keys, first) in [(10, -0.0_f64), (500, 0.0_f64)] {
            let mut values: Vec<f64> = (1..=keys).map(f64::from).collect();
            values.extend([-0.0, 0.0]);
            let result = spark_percentile(&mut values, 0.0, false).unwrap();
            assert_eq!(result.to_bits(), first.to_bits());
        }
    }

    #[test]
    fn keeps_the_sign_of_a_lone_zero() {
        let mut values = vec![-1.0, -0.0, 1.0];
        let result = spark_percentile(&mut values, 0.5, false).unwrap();
        assert_eq!(result.to_bits(), (-0.0_f64).to_bits());
    }
}
