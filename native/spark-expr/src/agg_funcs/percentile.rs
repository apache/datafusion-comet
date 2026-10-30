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
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Float64Builder, Int32Array, Int64Array,
    ListArray, StructArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Float64Type, Int32Type, Int64Type};
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
    pub fn try_new(percentile: f64, zero_order: ZeroOrder) -> Result<Self> {
        validate_percentile(percentile)?;
        Ok(Self {
            signature: Signature::user_defined(Immutable),
            percentile_bits: percentile.to_bits(),
            zero_order,
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

/// What decides where Spark ranks `0.0` against `-0.0` when the values hold both.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ZeroOrder {
    /// Whether Spark's input was `FLOAT` before the serde cast it to `DOUBLE`. Spark hashes the
    /// values as floats then, which decides which signed zero it reaches first.
    pub float_input: bool,
    /// Whether Spark keeps `0.0` and `-0.0` as separate keys, which it does from 3.5.2
    /// (SPARK-45599) for `FLOAT` and `DOUBLE` input. Earlier releases can merge the two and drop
    /// one's count, which this does not reproduce, and an integral or decimal input is never
    /// `-0.0`. The accumulators only track Spark's partials when this is set.
    pub signed_zero_keys: bool,
    /// Whether this aggregate is a Spark `PartialMerge`, which serializes the buffer it merged its
    /// input into. Its state then hands on the partials it merged nested in one.
    pub merge_stage: bool,
}

/// Fields of the single struct state column
/// `{values: list<f64>, partial_ends: list<i64>, partial_depths: list<i32>}`.
///
/// `partial_ends` and `partial_depths` describe the [`Partial`]s in `values`, so a final aggregate
/// that spills its state and merges it back still sees the partials it merged. Both are empty
/// unless [`ZeroOrder::signed_zero_keys`].
fn state_struct_fields() -> Fields {
    let list_of = |data_type| DataType::List(Arc::new(Field::new_list_field(data_type, true)));
    Fields::from(vec![
        Field::new("values", list_of(DataType::Float64), false),
        Field::new("partial_ends", list_of(DataType::Int64), false),
        Field::new("partial_depths", list_of(DataType::Int32), false),
    ])
}

/// A buffer Spark serialized on its way to the final aggregate: a task's partial, holding its
/// rows, or a merge stage's, holding the partials it merged nested one level deeper. A group's
/// partials are kept in preorder, each ending where its values end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Partial {
    end: usize,
    depth: u32,
}

/// One state row per group. Values past a group's last partial came from input rows, and the
/// state hands them on as one more partial, as Spark's partial aggregate does. A merge stage hands
/// on all of a group's partials nested in one, as Spark serializes the buffer it merged them into.
fn build_state(
    group_values: &[Vec<f64>],
    group_partials: &[Vec<Partial>],
    zero_order: ZeroOrder,
) -> StructArray {
    let num_values = group_values.iter().map(Vec::len).sum();
    let mut values = Vec::with_capacity(num_values);
    let mut ends = Vec::new();
    let mut depths = Vec::new();
    let mut value_offsets = Vec::with_capacity(group_values.len() + 1);
    let mut partial_offsets = Vec::with_capacity(group_values.len() + 1);
    value_offsets.push(0);
    partial_offsets.push(0);
    for (group, partials) in group_values.iter().zip(group_partials) {
        if zero_order.signed_zero_keys {
            let nested = i32::from(zero_order.merge_stage);
            if zero_order.merge_stage && !group.is_empty() {
                ends.push(group.len() as i64);
                depths.push(0);
            }
            for partial in partials {
                ends.push(partial.end as i64);
                depths.push(partial.depth as i32 + nested);
            }
            if partials.last().is_none_or(|last| last.end < group.len()) {
                ends.push(group.len() as i64);
                depths.push(nested);
            }
        }
        values.extend_from_slice(group);
        value_offsets.push(values.len() as i32);
        partial_offsets.push(ends.len() as i32);
    }
    let list = |offsets: OffsetBuffer<i32>, data_type, values: ArrayRef| -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(data_type, true)),
            offsets,
            values,
            None,
        ))
    };
    let partial_offsets = OffsetBuffer::new(ScalarBuffer::from(partial_offsets));
    StructArray::new(
        state_struct_fields(),
        vec![
            list(
                OffsetBuffer::new(ScalarBuffer::from(value_offsets)),
                DataType::Float64,
                Arc::new(Float64Array::from(values)),
            ),
            list(
                partial_offsets.clone(),
                DataType::Int64,
                Arc::new(Int64Array::from(ends)),
            ),
            list(
                partial_offsets,
                DataType::Int32,
                Arc::new(Int32Array::from(depths)),
            ),
        ],
        None,
    )
}

/// Appends state row `row` to `values`, and its partials to `partials`.
fn merge_state_row(
    states: &StructArray,
    row: usize,
    values: &mut Vec<f64>,
    partials: &mut Vec<Partial>,
    zero_order: ZeroOrder,
) {
    if states.is_null(row) {
        return;
    }
    let start = values.len();
    let state_values = states.column(0).as_list::<i32>().value(row);
    values.extend_from_slice(state_values.as_primitive::<Float64Type>().values());
    if !zero_order.signed_zero_keys {
        return;
    }
    let ends = states.column(1).as_list::<i32>().value(row);
    let depths = states.column(2).as_list::<i32>().value(row);
    let ends = ends.as_primitive::<Int64Type>().values();
    let depths = depths.as_primitive::<Int32Type>().values();
    partials.extend(ends.iter().zip(depths).map(|(&end, &depth)| Partial {
        end: start + end as usize,
        depth: depth as u32,
    }));
}

#[derive(Debug)]
struct SparkPercentileAccumulator {
    values: Vec<f64>,
    /// The partials merged into `values`. See `spark_percentile`.
    partials: Vec<Partial>,
    percentile: f64,
    zero_order: ZeroOrder,
}

impl SparkPercentileAccumulator {
    fn new(percentile: f64, zero_order: ZeroOrder) -> Self {
        Self {
            values: vec![],
            partials: vec![],
            percentile,
            zero_order,
        }
    }
}

impl Accumulator for SparkPercentileAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let state = build_state(
            std::slice::from_ref(&self.values),
            std::slice::from_ref(&self.partials),
            self.zero_order,
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
            merge_state_row(
                states,
                row,
                &mut self.values,
                &mut self.partials,
                self.zero_order,
            );
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(spark_percentile(
            self.values.as_mut_slice(),
            &self.partials,
            self.percentile,
            self.zero_order,
        )))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.values.capacity() * size_of::<f64>()
            + self.partials.capacity() * size_of::<Partial>()
    }
}

#[derive(Debug)]
struct SparkPercentileGroupsAccumulator {
    group_values: Vec<Vec<f64>>,
    /// The partials merged into each group's values. See `spark_percentile`.
    group_partials: Vec<Vec<Partial>>,
    percentile: f64,
    zero_order: ZeroOrder,
}

impl SparkPercentileGroupsAccumulator {
    fn new(percentile: f64, zero_order: ZeroOrder) -> Self {
        Self {
            group_values: vec![],
            group_partials: vec![],
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
        self.group_partials.resize(total_num_groups, Vec::new());

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
        self.group_partials.resize(total_num_groups, Vec::new());

        for (row, &group_index) in group_indices.iter().enumerate() {
            merge_state_row(
                states,
                row,
                &mut self.group_values[group_index],
                &mut self.group_partials[group_index],
                self.zero_order,
            );
        }

        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let emit_group_values = emit_to.take_needed(&mut self.group_values);
        let emit_partials = emit_to.take_needed(&mut self.group_partials);
        Ok(vec![Arc::new(build_state(
            &emit_group_values,
            &emit_partials,
            self.zero_order,
        ))])
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let mut emit_group_values = emit_to.take_needed(&mut self.group_values);
        let emit_partials = emit_to.take_needed(&mut self.group_partials);
        let mut builder = Float64Builder::with_capacity(emit_group_values.len());
        for (values, partials) in emit_group_values.iter_mut().zip(&emit_partials) {
            builder.append_option(spark_percentile(
                values.as_mut_slice(),
                partials,
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
                .group_partials
                .iter()
                .map(|partials| partials.capacity() * size_of::<Partial>())
                .sum::<usize>()
            + self.group_partials.capacity() * size_of::<Vec<Partial>>()
    }
}

/// `partials` are the partial states merged into `values`, in preorder. Values past the last one
/// came straight from input rows, as in Spark's `Complete` mode.
fn spark_percentile(
    values: &mut [f64],
    partials: &[Partial],
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
    let zeros = SignedZeros::find(values, partials, [lower, higher], zero_order);

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
    /// Only works out the order when one of `ranks` lands on a zero, since replaying Spark's
    /// tables is far slower than the selection itself.
    fn find(
        values: &[f64],
        partials: &[Partial],
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
            spark_iterates_negative_zero_first(values, partials, zero_order.float_input);
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
/// Each task inserts its rows into a partial buffer and serializes it in iteration order. Each
/// later stage deserializes every partial it receives into a new table, merges that into its
/// buffer in iteration order, one partial after another, and, unless it is the final aggregate,
/// serializes the result again. This replays those tables, taking the partials in the order Comet
/// merged them. Spark merges them in the order its shuffle delivers them, so when that changes the
/// zero Spark reaches first, Spark's own answer varies too.
///
/// It does not model what either engine does when it runs short of memory. Spark's
/// `ObjectHashAggregateExec` falls back to sort-based aggregation once a task holds
/// `spark.sql.objectHashAggregate.sortBased.fallbackThreshold` groups, and then builds each
/// group's buffer from the rows that came after the fallback before merging in the serialized
/// buffer from before it. DataFusion's partial aggregate emits the groups it holds and starts
/// again, which splits a task's partial, and its other aggregates spill their state, which nests
/// the partials a merge stage spilled one level deeper.
fn spark_iterates_negative_zero_first(
    values: &[f64],
    partials: &[Partial],
    float_input: bool,
) -> bool {
    let mut table = SparkKeySet::new(float_input);
    let mut next = 0;
    let mut start = 0;
    while next < partials.len() {
        let end = partials[next].end;
        let deserialized = deserialize(values, partials, &mut next, start, float_input);
        deserialized.keys().for_each(|v| table.add(v));
        start = end;
    }
    values[start..].iter().for_each(|&v| table.add(v));
    let first_zero = table.keys().find(|&v| v == 0.0);
    first_zero.is_some_and(f64::is_sign_negative)
}

/// The table a stage merges from the partial at `partials[*next]`, which starts at
/// `values[start]`: Spark deserializes the partial's serialized keys into a fresh table, in the
/// order the stage that built it iterated them. Advances `next` past the partials nested in it.
fn deserialize(
    values: &[f64],
    partials: &[Partial],
    next: &mut usize,
    start: usize,
    float_input: bool,
) -> SparkKeySet {
    let partial = partials[*next];
    *next += 1;
    let mut buffer = SparkKeySet::new(float_input);
    let mut nested_start = start;
    while *next < partials.len() && partials[*next].depth > partial.depth {
        let nested_end = partials[*next].end;
        let nested = deserialize(values, partials, next, nested_start, float_input);
        nested.keys().for_each(|v| buffer.add(v));
        nested_start = nested_end;
    }
    // A task's partial holds its rows; a merge stage's holds only the partials it merged.
    values[nested_start..partial.end]
        .iter()
        .for_each(|&v| buffer.add(v));
    let mut deserialized = SparkKeySet::new(float_input);
    buffer.keys().for_each(|v| deserialized.add(v));
    deserialized
}

/// The slot layout of Spark's `OpenHashSet` with the default capacity and load factor, holding
/// boxed `Double` or `Float` keys. Each slot holds a key's bits, or `EMPTY`, which no key has:
/// a float key fits in 32 bits, and a double key's NaN is always the canonical one.
struct SparkKeySet {
    slots: Vec<u64>,
    size: usize,
    float_input: bool,
}

impl SparkKeySet {
    const INITIAL_CAPACITY: usize = 64;
    const LOAD_FACTOR: f64 = 0.7;
    const EMPTY: u64 = u64::MAX;

    fn new(float_input: bool) -> Self {
        Self {
            slots: vec![Self::EMPTY; Self::INITIAL_CAPACITY],
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

    fn key(&self, bits: u64) -> f64 {
        if self.float_input {
            f32::from_bits(bits as u32) as f64
        } else {
            f64::from_bits(bits)
        }
    }

    /// `Double.hashCode` or `Float.hashCode`, mixed with murmur3 as `OpenHashSet.hashcode` does.
    fn slot(bits: u64, mask: usize) -> usize {
        let hash = (bits ^ (bits >> 32)) as i32;
        spark_compatible_murmur3_hash(hash.to_le_bytes(), 0) as usize & mask
    }

    fn add(&mut self, v: f64) {
        let mask = self.slots.len() - 1;
        let bits = self.key_bits(v);
        let mut pos = Self::slot(bits, mask);
        let mut delta = 1;
        while self.slots[pos] != Self::EMPTY {
            if self.slots[pos] == bits {
                return;
            }
            pos = (pos + delta) & mask;
            delta += 1;
        }
        self.slots[pos] = bits;
        self.size += 1;
        if self.size > (Self::LOAD_FACTOR * self.slots.len() as f64) as usize {
            self.grow();
        }
    }

    fn grow(&mut self) {
        let capacity = self.slots.len() * 2;
        let old = std::mem::replace(&mut self.slots, vec![Self::EMPTY; capacity]);
        let mask = self.slots.len() - 1;
        for bits in old.into_iter().filter(|&bits| bits != Self::EMPTY) {
            let mut pos = Self::slot(bits, mask);
            let mut delta = 1;
            while self.slots[pos] != Self::EMPTY {
                pos = (pos + delta) & mask;
                delta += 1;
            }
            self.slots[pos] = bits;
        }
    }

    fn keys(&self) -> impl Iterator<Item = f64> + '_ {
        self.slots
            .iter()
            .filter(|&&bits| bits != Self::EMPTY)
            .map(|&bits| self.key(bits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zero_order(float_input: bool) -> ZeroOrder {
        ZeroOrder {
            float_input,
            signed_zero_keys: true,
            merge_stage: false,
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

    // Two partitions for which Spark ranks -0.0 first whichever arrives first, but which rank
    // 0.0 first when they are replayed as one partial with the first of them ahead.
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
        Arc::new(build_state(
            partials,
            &vec![vec![]; partials.len()],
            zero_order(false),
        ))
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
        let ends = [Partial {
            end: flattened.len(),
            depth: 0,
        }];
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
    fn nests_the_partials_of_each_merge_stage() {
        // Spark's distinct-aggregate plan merges the percentile twice before the final aggregate,
        // and each merge stage serializes its buffer, which reorders the keys the next table
        // receives. For these rows Spark ranks 0.0 first after two stages, and -0.0 after none.
        let rows = vec![
            572803.0, 866096.0, 866879.0, 433001.0, 33996.0, 75441.0, 418281.0, 919347.0, 710172.0,
            746536.0, 68317.0, 830168.0, 127901.0, 662299.0, 132114.0, 986230.0, 624172.0,
            784519.0, 199287.0, 200265.0, 0.0, -0.0,
        ];
        let merge_stage = ZeroOrder {
            merge_stage: true,
            ..zero_order(false)
        };
        let after_stages = |stages: usize| {
            let mut state = states(std::slice::from_ref(&rows));
            for stage in 0..stages {
                state = if stage % 2 == 0 {
                    let mut accumulator = SparkPercentileAccumulator::new(0.0, merge_stage);
                    accumulator.merge_batch(&[state]).unwrap();
                    let ScalarValue::Struct(state) = accumulator.state().unwrap().remove(0) else {
                        panic!("expected a struct state")
                    };
                    state
                } else {
                    let mut accumulator = SparkPercentileGroupsAccumulator::new(0.0, merge_stage);
                    accumulator.merge_batch(&[state], &[0], 1).unwrap();
                    accumulator.state(EmitTo::All).unwrap().remove(0)
                };
            }
            let mut accumulator = SparkPercentileGroupsAccumulator::new(0.0, zero_order(false));
            accumulator.merge_batch(&[state], &[0], 1).unwrap();
            is_negative(accumulator.evaluate(EmitTo::All).unwrap())
        };
        assert!(after_stages(0));
        assert!(!after_stages(2));
    }

    #[test]
    fn tracks_no_partials_unless_spark_keeps_both_zeros() {
        let old_spark = ZeroOrder {
            signed_zero_keys: false,
            ..zero_order(false)
        };
        let partials = two_partials();
        let state = build_state(&partials, &[vec![], vec![]], old_spark);
        assert_eq!(state.column(1).as_list::<i32>().values().len(), 0);
        assert_eq!(state.column(2).as_list::<i32>().values().len(), 0);

        let mut accumulator = SparkPercentileGroupsAccumulator::new(0.0, old_spark);
        accumulator
            .merge_batch(&[states(&partials)], &[0, 0], 1)
            .unwrap();
        assert!(accumulator.group_partials[0].is_empty());
    }

    #[test]
    fn replays_only_when_a_rank_lands_on_a_zero() {
        let values = [-1.0, -0.0, 0.0, 1.0, 2.0];
        let partials = [Partial {
            end: values.len(),
            depth: 0,
        }];
        let find = |ranks, zero_order| SignedZeros::find(&values, &partials, ranks, zero_order);
        assert_eq!(find([1, 2], zero_order(false)).start, Some(1));
        assert_eq!(find([0, 3], zero_order(false)).start, None);
        let old_spark = ZeroOrder {
            signed_zero_keys: false,
            ..zero_order(false)
        };
        assert_eq!(find([1, 2], old_spark).start, None);
    }
}
