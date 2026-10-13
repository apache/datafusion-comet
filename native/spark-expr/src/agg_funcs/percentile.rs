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
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Float64Builder, Int64Array, ListArray,
    StructArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type, Int64Type};
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
    zero_order: ZeroOrder,
}

impl SparkPercentile {
    pub fn try_new(percentile: f64, float_input: bool, signed_zero_keys: bool) -> Result<Self> {
        validate_percentile(percentile)?;
        Ok(Self {
            signature: Signature::user_defined(Immutable),
            percentile_bits: percentile.to_bits(),
            zero_order: ZeroOrder {
                float_input,
                signed_zero_keys,
            },
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
            self.zero_order,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let input_type = args.input_fields[0].data_type();
        if input_type != &DataType::Float64 {
            return internal_err!("SparkPercentile expects Float64 input, got {input_type}");
        }
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, self.name()),
            DataType::Struct(state_struct_fields()),
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
            self.zero_order,
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

/// Fields of the single struct state column `{values: list<f64>, partial_ends: list<i64>}`.
/// `partial_ends` keeps the boundaries `spark_percentile` replays, so a final aggregate that
/// spills its state and merges it back still sees the partials it merged.
fn state_struct_fields() -> Fields {
    let list_of = |data_type| DataType::List(Arc::new(Field::new_list_field(data_type, true)));
    Fields::from(vec![
        Field::new("values", list_of(DataType::Float64), false),
        Field::new("partial_ends", list_of(DataType::Int64), false),
    ])
}

/// One state row per group. Values past a group's last partial end came from input rows, and
/// the state hands them on as one more partial, as Spark's partial aggregate does.
fn build_state(group_values: &[Vec<f64>], group_partial_ends: &[Vec<usize>]) -> StructArray {
    let num_values = group_values.iter().map(Vec::len).sum();
    let num_ends = group_partial_ends.iter().map(|ends| ends.len() + 1).sum();
    let mut values = Vec::with_capacity(num_values);
    let mut ends = Vec::with_capacity(num_ends);
    let mut value_offsets = Vec::with_capacity(group_values.len() + 1);
    let mut end_offsets = Vec::with_capacity(group_values.len() + 1);
    value_offsets.push(0);
    end_offsets.push(0);
    for (group, group_ends) in group_values.iter().zip(group_partial_ends) {
        ends.extend(group_ends.iter().map(|&end| end as i64));
        if group_ends.last().copied().unwrap_or(0) < group.len() {
            ends.push(group.len() as i64);
        }
        values.extend_from_slice(group);
        value_offsets.push(values.len() as i32);
        end_offsets.push(ends.len() as i32);
    }
    let list = |offsets: Vec<i32>, data_type, values: ArrayRef| -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(data_type, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values,
            None,
        ))
    };
    StructArray::new(
        state_struct_fields(),
        vec![
            list(
                value_offsets,
                DataType::Float64,
                Arc::new(Float64Array::from(values)),
            ),
            list(
                end_offsets,
                DataType::Int64,
                Arc::new(Int64Array::from(ends)),
            ),
        ],
        None,
    )
}

/// Appends state row `row` to `values`, recording where each of its partials ends.
fn merge_state_row(
    states: &StructArray,
    row: usize,
    values: &mut Vec<f64>,
    partial_ends: &mut Vec<usize>,
) {
    if states.is_null(row) {
        return;
    }
    let start = values.len();
    let state_values = states.column(0).as_list::<i32>().value(row);
    values.extend_from_slice(state_values.as_primitive::<Float64Type>().values());
    let state_ends = states.column(1).as_list::<i32>().value(row);
    let state_ends = state_ends.as_primitive::<Int64Type>().values();
    partial_ends.extend(state_ends.iter().map(|&end| start + end as usize));
}

#[derive(Debug)]
struct SparkPercentileAccumulator {
    values: Vec<f64>,
    /// Where each merged partial state ends in `values`. See `spark_percentile`.
    partial_ends: Vec<usize>,
    percentile: f64,
    zero_order: ZeroOrder,
}

impl SparkPercentileAccumulator {
    fn new(percentile: f64, zero_order: ZeroOrder) -> Self {
        Self {
            values: vec![],
            partial_ends: vec![],
            percentile,
            zero_order,
        }
    }
}

impl Accumulator for SparkPercentileAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let state = build_state(
            std::slice::from_ref(&self.values),
            std::slice::from_ref(&self.partial_ends),
        );
        Ok(vec![ScalarValue::Struct(Arc::new(state))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.values.reserve(values.len() - values.null_count());
        self.values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states = states[0].as_struct();
        for row in 0..states.len() {
            merge_state_row(states, row, &mut self.values, &mut self.partial_ends);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(spark_percentile(
            self.values.as_mut_slice(),
            &self.partial_ends,
            self.percentile,
            self.zero_order,
        )))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.values.capacity() * size_of::<f64>()
            + self.partial_ends.capacity() * size_of::<usize>()
    }
}

#[derive(Debug)]
struct SparkPercentileGroupsAccumulator {
    group_values: Vec<Vec<f64>>,
    /// Where each merged partial state ends in the group's values. See `spark_percentile`.
    group_partial_ends: Vec<Vec<usize>>,
    percentile: f64,
    zero_order: ZeroOrder,
}

impl SparkPercentileGroupsAccumulator {
    fn new(percentile: f64, zero_order: ZeroOrder) -> Self {
        Self {
            group_values: vec![],
            group_partial_ends: vec![],
            percentile,
            zero_order,
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
        self.group_partial_ends.resize(total_num_groups, Vec::new());

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
        let states = values[0].as_struct();
        self.group_values.resize(total_num_groups, Vec::new());
        self.group_partial_ends.resize(total_num_groups, Vec::new());

        for (row, &group_index) in group_indices.iter().enumerate() {
            merge_state_row(
                states,
                row,
                &mut self.group_values[group_index],
                &mut self.group_partial_ends[group_index],
            );
        }

        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let emit_group_values = emit_to.take_needed(&mut self.group_values);
        let emit_partial_ends = emit_to.take_needed(&mut self.group_partial_ends);
        Ok(vec![Arc::new(build_state(
            &emit_group_values,
            &emit_partial_ends,
        ))])
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let mut emit_group_values = emit_to.take_needed(&mut self.group_values);
        let emit_partial_ends = emit_to.take_needed(&mut self.group_partial_ends);
        let mut builder = Float64Builder::with_capacity(emit_group_values.len());
        for (values, partial_ends) in emit_group_values.iter_mut().zip(&emit_partial_ends) {
            builder.append_option(spark_percentile(
                values.as_mut_slice(),
                partial_ends,
                self.percentile,
                self.zero_order,
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
            + self
                .group_partial_ends
                .iter()
                .map(|ends| ends.capacity() * size_of::<usize>())
                .sum::<usize>()
            + self.group_partial_ends.capacity() * size_of::<Vec<usize>>()
    }
}

/// `partial_ends` marks where each partial state merged into `values` ends. Values past the last
/// one came straight from input rows, as in Spark's `Complete` mode.
fn spark_percentile(
    values: &mut [f64],
    partial_ends: &[usize],
    percentile: f64,
    zero_order: ZeroOrder,
) -> Option<f64> {
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

    // Read before the selection below reorders the values.
    let zeros = SignedZeros::find(values, partial_ends, [lower, higher], zero_order);

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

/// What decides where Spark ranks `0.0` against `-0.0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ZeroOrder {
    /// Whether Spark's input was `FLOAT` before the serde cast it to `DOUBLE`. Spark hashes the
    /// values as floats then, which decides which signed zero it reaches first.
    float_input: bool,
    /// Whether Spark keeps `0.0` and `-0.0` as separate keys, which it does from 3.5.2
    /// (SPARK-45599). Earlier releases can merge the two and drop one's count, which this does
    /// not reproduce.
    signed_zero_keys: bool,
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
    /// Only works out the order when one of `ranks` lands on a zero, since replaying Spark's
    /// tables is far slower than the selection itself.
    fn find(
        values: &[f64],
        partial_ends: &[usize],
        ranks: [usize; 2],
        zero_order: ZeroOrder,
    ) -> Self {
        if !zero_order.signed_zero_keys || !values.iter().any(|v| v.to_bits() == NEGATIVE_ZERO_BITS)
        {
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
        let zero_ranks = below..below + negative + positive;
        if positive == 0 || !ranks.iter().any(|rank| zero_ranks.contains(rank)) {
            return Self::default();
        }
        let first_negative =
            spark_iterates_negative_zero_first(values, partial_ends, zero_order.float_input);
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
/// Each partition inserts its rows into a partial buffer and serializes it in iteration order.
/// The final aggregation deserializes each one into a new map and merges that into its buffer,
/// again in iteration order, one partial after another. This replays those tables, taking the
/// partials in the order Comet merged them. Spark merges them in the order its shuffle delivers
/// them, so when that changes the zero Spark reaches first, Spark's own answer varies too. A
/// distinct aggregate's PartialMerge hands on the partials it merged where Spark hands on one
/// merged buffer, so there the order can also differ from Spark's.
fn spark_iterates_negative_zero_first(
    values: &[f64],
    partial_ends: &[usize],
    float_input: bool,
) -> bool {
    let build = |values: &mut dyn Iterator<Item = f64>| {
        let mut table = SparkKeySet::new(float_input);
        values.for_each(|v| table.add(v));
        table
    };
    let mut table = SparkKeySet::new(float_input);
    let mut start = 0;
    for &end in partial_ends {
        let partial = build(&mut values[start..end].iter().copied());
        let deserialized = build(&mut partial.keys());
        deserialized.keys().for_each(|v| table.add(v));
        start = end;
    }
    values[start..].iter().for_each(|&v| table.add(v));
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
    use super::*;

    fn zero_order(float_input: bool) -> ZeroOrder {
        ZeroOrder {
            float_input,
            signed_zero_keys: true,
        }
    }

    #[test]
    fn interpolates_with_full_spark_precision() {
        let mut values = vec![0.0, 10_000_000.0];
        assert_eq!(
            spark_percentile(&mut values, &[], 0.123456789, zero_order(false)),
            Some(1_234_567.89)
        );
    }

    #[test]
    fn orders_nan_above_every_other_value() {
        // A NaN with the sign bit set, which IEEE 754 total order would put first.
        let mut values = vec![f64::from_bits(0xfff8_0000_0000_0000), 2.0, 1.0];
        assert_eq!(
            spark_percentile(&mut values, &[], 0.0, zero_order(false)),
            Some(1.0)
        );
        assert!(spark_percentile(&mut values, &[], 1.0, zero_order(false))
            .unwrap()
            .is_nan());
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
            let ends = [values.len()];
            let result =
                spark_percentile(&mut values, &ends, 0.1, zero_order(float_input)).unwrap();
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
            let ends = [values.len()];
            let result = spark_percentile(&mut values, &ends, 0.0, zero_order(false)).unwrap();
            assert_eq!(result.to_bits(), first.to_bits());
        }
    }

    #[test]
    fn keeps_the_sign_of_a_lone_zero() {
        let mut values = vec![-1.0, -0.0, 1.0];
        let result = spark_percentile(&mut values, &[], 0.5, zero_order(false)).unwrap();
        assert_eq!(result.to_bits(), (-0.0_f64).to_bits());
    }

    // Two partitions for which Spark ranks -0.0 first whichever arrives first, but which rank
    // 0.0 first when the second is merged first and the two are replayed as one partial.
    fn two_partials() -> [Vec<f64>; 2] {
        [
            vec![
                369513.0, 998311.0, 8278.0, 156933.0, 286454.0, 349279.0, 125841.0, 796806.0,
                556996.0, 0.0, 667190.0, 624668.0, 335166.0, 220654.0, 315604.0,
            ],
            vec![223815.0, 201661.0, 68860.0, 928422.0, -0.0],
        ]
    }

    // The states of partial aggregates, one per partition.
    fn states(partials: &[Vec<f64>]) -> ArrayRef {
        Arc::new(build_state(partials, &vec![vec![]; partials.len()]))
    }

    fn is_negative(result: ArrayRef) -> bool {
        result
            .as_primitive::<Float64Type>()
            .value(0)
            .is_sign_negative()
    }

    #[test]
    fn replays_each_partial_separately() {
        let partials = two_partials();

        let mut flattened = partials.concat();
        let ends = [flattened.len()];
        let result = spark_percentile(&mut flattened, &ends, 0.0, zero_order(false)).unwrap();
        assert_eq!(result.to_bits(), 0.0_f64.to_bits());

        let mut accumulator = SparkPercentileAccumulator::new(0.0, zero_order(false));
        accumulator.merge_batch(&[states(&partials)]).unwrap();
        let ScalarValue::Float64(Some(result)) = accumulator.evaluate().unwrap() else {
            panic!("expected a non-null Float64")
        };
        assert!(result.is_sign_negative());

        let mut accumulator = SparkPercentileGroupsAccumulator::new(0.0, zero_order(false));
        accumulator
            .merge_batch(&[states(&partials)], &[0, 0], 1)
            .unwrap();
        assert!(is_negative(accumulator.evaluate(EmitTo::All).unwrap()));
    }

    #[test]
    fn keeps_the_partials_through_a_spill() {
        let mut accumulator = SparkPercentileGroupsAccumulator::new(0.0, zero_order(false));
        accumulator
            .merge_batch(&[states(&two_partials())], &[0, 0], 1)
            .unwrap();
        let spilled = accumulator.state(EmitTo::All).unwrap();

        let mut restored = SparkPercentileGroupsAccumulator::new(0.0, zero_order(false));
        restored.merge_batch(&spilled, &[0], 1).unwrap();
        assert!(is_negative(restored.evaluate(EmitTo::All).unwrap()));
    }

    #[test]
    fn replays_only_when_a_rank_lands_on_a_zero() {
        let values = [-1.0, -0.0, 0.0, 1.0, 2.0];
        let ends = [values.len()];
        let find = |ranks, zero_order| SignedZeros::find(&values, &ends, ranks, zero_order);
        assert_eq!(find([1, 2], zero_order(false)).start, Some(1));
        assert_eq!(find([0, 3], zero_order(false)).start, None);
        let old_spark = ZeroOrder {
            float_input: false,
            signed_zero_keys: false,
        };
        assert_eq!(find([1, 2], old_spark).start, None);
    }
}
