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

use crate::float_semantics::{float_gt, float_lt};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::NullBuffer;
use arrow::compute::{not, nullif, prep_null_mask_filter};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::expressions::format_state_name;
use num::Float;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

/// Spark's `max` or `min` over Float32 or Float64 values.
///
/// Spark orders floats with `SQLOrderingUtil.compareDoubles`, in which NaN is larger than every
/// other value and `-0.0` equals `0.0`. Its `Max` updates the buffer with `greatest(max, input)`,
/// which replaces the buffer only with a strictly larger value, so of equal values the first one
/// seen is kept: `max` over `-0.0` and then `0.0` returns `-0.0`. `Min` does the same with
/// `least`. Every accumulator here follows that rule, including over a sliding window frame.
///
/// DataFusion's `max` and `min` order floats by IEEE 754 total order in their batch kernels, in
/// which a NaN with the sign bit set is the smallest value. In DataFusion 55 the grouped versions
/// also start each group at the most negative (or positive) finite value, so a group holding only
/// `-Infinity` returns that finite value; apache/datafusion#24433 fixes that upstream.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMinMax {
    signature: Signature,
    is_max: bool,
}

impl SparkMinMax {
    pub fn new(is_max: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            is_max,
        }
    }
}

impl AggregateUDFImpl for SparkMinMax {
    fn name(&self) -> &str {
        if self.is_max {
            "max"
        } else {
            "min"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match args.return_field.data_type() {
            DataType::Float32 => Ok(Box::new(MinMaxAccumulator::<Float32Type>::new(self.is_max))),
            DataType::Float64 => Ok(Box::new(MinMaxAccumulator::<Float64Type>::new(self.is_max))),
            other => internal_err!("Spark {} expects a float, got {other}", self.name()),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, self.name()),
            args.return_field.data_type().clone(),
            true,
        ))])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        match args.return_field.data_type() {
            DataType::Float32 => Ok(Box::new(MinMaxGroupsAccumulator::<Float32Type>::new(
                self.is_max,
            ))),
            DataType::Float64 => Ok(Box::new(MinMaxGroupsAccumulator::<Float64Type>::new(
                self.is_max,
            ))),
            other => internal_err!("Spark {} expects a float, got {other}", self.name()),
        }
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match args.return_field.data_type() {
            DataType::Float32 => Ok(Box::new(SlidingMinMaxAccumulator::<Float32Type>::new(
                self.is_max,
            ))),
            DataType::Float64 => Ok(Box::new(SlidingMinMaxAccumulator::<Float64Type>::new(
                self.is_max,
            ))),
            other => internal_err!("Spark {} expects a float, got {other}", self.name()),
        }
    }
}

/// Whether `candidate` replaces a running maximum `current`: the same as [`float_gt`], so equal
/// values do not and the first one seen is kept. Written so that one comparison decides the usual
/// answer, which is no. That makes the grouped and sliding loops faster, which cannot vectorize,
/// while `float_gt` is faster in a loop over a slice that can.
// The negated comparison is the point: `!(candidate <= current)` also holds when either value
// is NaN, in one comparison. The `partial_cmp` form clippy suggests makes grouped `max` about 50%
// slower.
#[allow(clippy::neg_cmp_op_on_partial_ord)]
#[inline]
fn replaces_max<T: Float>(candidate: T, current: T) -> bool {
    // Only a NaN `current` must be kept, since NaN is the largest value.
    !(candidate <= current) && !current.is_nan()
}

/// Whether `candidate` replaces a running minimum `current`: the same as [`float_lt`]. See
/// [`replaces_max`].
#[allow(clippy::neg_cmp_op_on_partial_ord)]
#[inline]
fn replaces_min<T: Float>(candidate: T, current: T) -> bool {
    // Only a NaN `candidate` must be passed over.
    !(candidate >= current) && !candidate.is_nan()
}

/// Whether `max` (with `is_max`) or `min` replaces `current` with `candidate`.
#[inline]
fn replaces<T: Float>(is_max: bool, candidate: T, current: T) -> bool {
    if is_max {
        replaces_max(candidate, current)
    } else {
        replaces_min(candidate, current)
    }
}

/// `max` or `min` without grouping.
#[derive(Debug)]
struct MinMaxAccumulator<T: ArrowPrimitiveType> {
    value: Option<T::Native>,
    is_max: bool,
}

impl<T: ArrowPrimitiveType> MinMaxAccumulator<T> {
    fn new(is_max: bool) -> Self {
        Self {
            value: None,
            is_max,
        }
    }
}

impl<T> Accumulator for MinMaxAccumulator<T>
where
    T: ArrowPrimitiveType + Debug,
    T::Native: Float,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.value = match values.nulls() {
            None => fold(self.value, values.values().iter().copied(), self.is_max),
            Some(_) => fold(self.value, values.iter().flatten(), self.is_max),
        };
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

/// Folds `values` into `current`, keeping the first of equal values.
#[inline]
fn fold<T: Float>(current: Option<T>, values: impl Iterator<Item = T>, is_max: bool) -> Option<T> {
    // Monomorphized per direction.
    fn run<T: Float>(
        current: Option<T>,
        mut values: impl Iterator<Item = T>,
        replaces: impl Fn(T, T) -> bool,
    ) -> Option<T> {
        let mut best = current.or_else(|| values.next())?;
        for value in values {
            if replaces(value, best) {
                best = value;
            }
        }
        Some(best)
    }
    // `float_gt` and `float_lt` rather than `replaces_max` and `replaces_min`: this loop over a
    // slice vectorizes with them and is several times faster.
    if is_max {
        run(current, values, float_gt)
    } else {
        run(current, values, float_lt)
    }
}

/// `max` or `min` per group. A group keeps the first value it sees as is, and then only strictly
/// preferred ones.
#[derive(Debug)]
struct MinMaxGroupsAccumulator<T: ArrowPrimitiveType> {
    values: Vec<T::Native>,
    /// Whether each group has seen a value. A group that has not evaluates to null.
    seen: Vec<bool>,
    is_max: bool,
}

impl<T: ArrowPrimitiveType> MinMaxGroupsAccumulator<T>
where
    T::Native: Float,
{
    fn new(is_max: bool) -> Self {
        Self {
            values: vec![],
            seen: vec![],
            is_max,
        }
    }

    /// Monomorphized per direction, with a tight loop for the common case of no nulls and no
    /// filter.
    fn update<F>(
        &mut self,
        values: &PrimitiveArray<T>,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        replaces: F,
    ) where
        F: Fn(T::Native, T::Native) -> bool,
    {
        let (values_by_group, seen_by_group) = (&mut self.values, &mut self.seen);
        debug_assert!(group_indices
            .iter()
            .all(|&group| group < values_by_group.len()));
        let mut accumulate = |group: usize, value: T::Native| {
            // SAFETY: `update_batch` sized both vectors to `total_num_groups`, and every group
            // index is below it, as `GroupsAccumulator` requires. DataFusion's own grouped `max`
            // relies on the same guarantee; checking each index costs a third of this loop.
            let (current, seen) = unsafe {
                (
                    values_by_group.get_unchecked_mut(group),
                    seen_by_group.get_unchecked_mut(group),
                )
            };
            // `|` rather than `||`, so that the usual case takes no extra branch.
            if !*seen | replaces(value, *current) {
                *current = value;
            }
            *seen = true;
        };
        if values.null_count() == 0 && opt_filter.is_none() {
            for (&group, &value) in group_indices.iter().zip(values.values().iter()) {
                accumulate(group, value);
            }
            return;
        }
        for (row, &group) in group_indices.iter().enumerate() {
            let filtered_out =
                opt_filter.is_some_and(|filter| !filter.is_valid(row) || !filter.value(row));
            if !filtered_out && values.is_valid(row) {
                accumulate(group, values.value(row));
            }
        }
    }
}

impl<T> GroupsAccumulator for MinMaxGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Debug + Send + Sync,
    T::Native: Float,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // A new group takes its first value whatever it holds, since it has not been seen.
        self.values.resize(total_num_groups, T::Native::default());
        self.seen.resize(total_num_groups, false);
        let values = values[0].as_primitive::<T>();
        if self.is_max {
            self.update(values, group_indices, opt_filter, replaces_max);
        } else {
            self.update(values, group_indices, opt_filter, replaces_min);
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.update_batch(values, group_indices, None, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.values);
        let seen = emit_to.take_needed(&mut self.seen);
        Ok(Arc::new(PrimitiveArray::<T>::new(
            values.into(),
            Some(NullBuffer::from(seen)),
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = Arc::clone(&values[0]);
        let Some(filter) = opt_filter else {
            return Ok(vec![values]);
        };
        // A row the filter drops, or whose filter value is null, contributes nothing.
        let dropped = not(&prep_null_mask_filter(filter))?;
        Ok(vec![nullif(&values, &dropped)?])
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.values.capacity() * size_of::<T::Native>()
            + self.seen.capacity() * size_of::<bool>()
    }
}

/// `max` or `min` over a sliding window frame, which retracts values in the order it added them.
///
/// A deque holds the values that can still become the result, oldest first. Adding a value drops
/// the newer values that it is strictly preferred to, but not equal ones, so the front is always
/// the first of the best values in the frame.
#[derive(Debug)]
struct SlidingMinMaxAccumulator<T: ArrowPrimitiveType> {
    /// Candidates, each with the position at which it was added.
    candidates: VecDeque<(u64, T::Native)>,
    added: u64,
    retracted: u64,
    is_max: bool,
}

impl<T: ArrowPrimitiveType> SlidingMinMaxAccumulator<T>
where
    T::Native: Float,
{
    fn new(is_max: bool) -> Self {
        Self {
            candidates: VecDeque::new(),
            added: 0,
            retracted: 0,
            is_max,
        }
    }

    fn add(&mut self, value: T::Native) {
        while self
            .candidates
            .back()
            .is_some_and(|&(_, newest)| replaces(self.is_max, value, newest))
        {
            self.candidates.pop_back();
        }
        self.candidates.push_back((self.added, value));
        self.added += 1;
    }

    fn retract_oldest(&mut self) {
        if self.retracted == self.added {
            return;
        }
        if self
            .candidates
            .front()
            .is_some_and(|&(position, _)| position == self.retracted)
        {
            self.candidates.pop_front();
        }
        self.retracted += 1;
    }
}

impl<T> Accumulator for SlidingMinMaxAccumulator<T>
where
    T: ArrowPrimitiveType + Debug,
    T::Native: Float,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for value in values[0].as_primitive::<T>().iter().flatten() {
            self.add(value);
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Nulls were never added, so only the valid values leave the frame.
        for _ in 0..values[0].len() - values[0].null_count() {
            self.retract_oldest();
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = self.candidates.front().map(|&(_, value)| value);
        ScalarValue::new_primitive::<T>(value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.candidates.capacity() * size_of::<(u64, T::Native)>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::float_semantics::{spark_extreme, EDGE_VALUES};
    use arrow::array::{Float32Array, Float64Array};

    /// Pseudo-random sequences of edge values, the same on every run.
    fn sequences() -> Vec<Vec<Option<f64>>> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        (0..300)
            .map(|i| {
                (0..i % 23)
                    .map(|_| EDGE_VALUES[(next() % EDGE_VALUES.len() as u64) as usize])
                    .collect()
            })
            .collect()
    }

    fn bits(value: &ScalarValue) -> Option<u64> {
        match value {
            ScalarValue::Float64(v) => v.map(f64::to_bits),
            ScalarValue::Float32(v) => v.map(|v| u64::from(v.to_bits())),
            other => panic!("unexpected {other:?}"),
        }
    }

    fn array(values: &[Option<f64>]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    fn float_array(values: &[Option<f32>]) -> ArrayRef {
        Arc::new(Float32Array::from(values.to_vec()))
    }

    /// The loops' predicates agree with the shared ones on every pair of edge values.
    #[test]
    fn replacement_predicates_match_float_semantics() {
        for &left in EDGE_VALUES.iter().flatten() {
            for &right in EDGE_VALUES.iter().flatten() {
                assert_eq!(
                    replaces_max(left, right),
                    float_gt(left, right),
                    "{left} {right}"
                );
                assert_eq!(
                    replaces_min(left, right),
                    float_lt(left, right),
                    "{left} {right}"
                );
            }
        }
    }

    /// The result, down to its bits, including which zero or which NaN was kept, over a whole
    /// sequence fed in several batches, and over two partial states merged in order.
    #[test]
    fn accumulator_matches_spark() -> Result<()> {
        for values in sequences() {
            for is_max in [true, false] {
                let expected = spark_extreme(&values, is_max).map(f64::to_bits);
                let mut acc = MinMaxAccumulator::<Float64Type>::new(is_max);
                for batch in values.chunks(4) {
                    acc.update_batch(&[array(batch)])?;
                }
                assert_eq!(bits(&acc.evaluate()?), expected, "{values:?} max={is_max}");

                let (first, second) = values.split_at(values.len() / 2);
                let mut merged = MinMaxAccumulator::<Float64Type>::new(is_max);
                for part in [first, second] {
                    let mut partial = MinMaxAccumulator::<Float64Type>::new(is_max);
                    partial.update_batch(&[array(part)])?;
                    let state = partial.state()?[0].to_array()?;
                    merged.merge_batch(&[state])?;
                }
                assert_eq!(
                    bits(&merged.evaluate()?),
                    expected,
                    "{values:?} max={is_max}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn float32_accumulator_matches_spark() -> Result<()> {
        for values in sequences() {
            let values: Vec<Option<f32>> = values.iter().map(|v| v.map(|v| v as f32)).collect();
            let as_f64: Vec<Option<f64>> = values.iter().map(|v| v.map(f64::from)).collect();
            for is_max in [true, false] {
                let mut acc = MinMaxAccumulator::<Float32Type>::new(is_max);
                acc.update_batch(&[float_array(&values)])?;
                let expected =
                    spark_extreme(&as_f64, is_max).map(|v| u64::from((v as f32).to_bits()));
                assert_eq!(bits(&acc.evaluate()?), expected, "{values:?} max={is_max}");
            }
        }
        Ok(())
    }

    /// Rows go to three groups, and are emitted in two parts to exercise `EmitTo::First`. Each
    /// sequence runs twice: with its nulls and a filter that drops every fifth row, and without
    /// nulls or a filter, which takes the accumulator's faster loop.
    #[test]
    fn groups_accumulator_matches_spark() -> Result<()> {
        for values in sequences() {
            let without_nulls: Vec<Option<f64>> =
                values.iter().copied().filter(Option::is_some).collect();
            for (values, filtered) in [(values, true), (without_nulls, false)] {
                let groups: Vec<usize> = (0..values.len()).map(|row| row % 3).collect();
                let keep = |row: usize| !filtered || row % 5 != 4;
                let filter = BooleanArray::from(
                    (0..values.len())
                        .map(|row| keep(row).then_some(true))
                        .collect::<Vec<_>>(),
                );
                for is_max in [true, false] {
                    let mut acc = MinMaxGroupsAccumulator::<Float64Type>::new(is_max);
                    let opt_filter = filtered.then_some(&filter);
                    acc.update_batch(&[array(&values)], &groups, opt_filter, 3)?;
                    let first = acc.evaluate(EmitTo::First(1))?;
                    let rest = acc.evaluate(EmitTo::All)?;
                    let results = first
                        .as_primitive::<Float64Type>()
                        .iter()
                        .chain(rest.as_primitive::<Float64Type>().iter());
                    for (group, actual) in results.enumerate() {
                        let rows: Vec<Option<f64>> = (0..values.len())
                            .filter(|&row| groups[row] == group && keep(row))
                            .map(|row| values[row])
                            .collect();
                        let expected = spark_extreme(&rows, is_max).map(f64::to_bits);
                        assert_eq!(
                            actual.map(f64::to_bits),
                            expected,
                            "{values:?} group={group} max={is_max} filtered={filtered}"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Merging partial states keeps the first of equal values across states too.
    #[test]
    fn groups_accumulator_merges_in_order() -> Result<()> {
        let mut acc = MinMaxGroupsAccumulator::<Float64Type>::new(true);
        acc.merge_batch(&[array(&[Some(-0.0), None])], &[0, 1], 2)?;
        acc.merge_batch(&[array(&[Some(0.0), Some(f64::INFINITY)])], &[0, 1], 2)?;
        let result = acc.evaluate(EmitTo::All)?;
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0).to_bits(), (-0.0f64).to_bits());
        assert_eq!(result.value(1), f64::INFINITY);
        Ok(())
    }

    /// DataFusion's grouped `max` starts a group at `f64::MIN`, so a group holding only
    /// `-Infinity` came back as `f64::MIN`. `min` had the same problem with `Infinity`.
    #[test]
    fn groups_of_only_an_infinity() -> Result<()> {
        for (is_max, value) in [(true, f64::NEG_INFINITY), (false, f64::INFINITY)] {
            let mut acc = MinMaxGroupsAccumulator::<Float64Type>::new(is_max);
            acc.update_batch(&[array(&[Some(value)])], &[0], None, 1)?;
            let result = acc.evaluate(EmitTo::All)?;
            assert_eq!(result.as_primitive::<Float64Type>().value(0), value);
        }
        Ok(())
    }

    #[test]
    fn convert_to_state_drops_filtered_rows() -> Result<()> {
        let acc = MinMaxGroupsAccumulator::<Float64Type>::new(true);
        let values = array(&[Some(1.0), Some(2.0), Some(3.0), None]);
        let filter = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
        let state = acc.convert_to_state(&[Arc::clone(&values)], Some(&filter))?;
        let state = state[0].as_primitive::<Float64Type>();
        assert_eq!(
            state.iter().collect::<Vec<_>>(),
            vec![Some(1.0), None, None, None]
        );
        assert!(Arc::ptr_eq(
            &acc.convert_to_state(&[Arc::clone(&values)], None)?[0],
            &values
        ));
        Ok(())
    }

    /// A frame of `width` rows ending at each row, as `ROWS BETWEEN width - 1 PRECEDING AND
    /// CURRENT ROW` evaluates it: add the new row, then retract the row that left the frame. Each
    /// sequence runs as `DOUBLE` and as `FLOAT`.
    #[test]
    fn sliding_accumulator_matches_spark() -> Result<()> {
        for values in sequences() {
            // The `FLOAT` expectation folds the values that accumulator sees, widened back.
            let floats: Vec<Option<f32>> = values.iter().map(|v| v.map(|v| v as f32)).collect();
            let widened: Vec<Option<f64>> = floats.iter().map(|v| v.map(f64::from)).collect();
            for width in 1..5 {
                for is_max in [true, false] {
                    let mut doubles = SlidingMinMaxAccumulator::<Float64Type>::new(is_max);
                    let mut singles = SlidingMinMaxAccumulator::<Float32Type>::new(is_max);
                    for row in 0..values.len() {
                        doubles.update_batch(&[array(&values[row..=row])])?;
                        singles.update_batch(&[float_array(&floats[row..=row])])?;
                        if let Some(left) = row.checked_sub(width) {
                            doubles.retract_batch(&[array(&values[left..=left])])?;
                            singles.retract_batch(&[float_array(&floats[left..=left])])?;
                        }
                        let start = (row + 1).saturating_sub(width);
                        let frame = &values[start..=row];
                        let expected = spark_extreme(frame, is_max).map(f64::to_bits);
                        assert_eq!(
                            bits(&doubles.evaluate()?),
                            expected,
                            "{frame:?} width={width} max={is_max}"
                        );
                        let frame = &widened[start..=row];
                        let expected =
                            spark_extreme(frame, is_max).map(|v| u64::from((v as f32).to_bits()));
                        assert_eq!(
                            bits(&singles.evaluate()?),
                            expected,
                            "FLOAT {frame:?} width={width} max={is_max}"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Each accumulator fails the same way when its type is wrong, so check the dispatch once.
    #[test]
    fn rejects_other_types() {
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_expr::PhysicalExpr;
        let schema = arrow::datatypes::Schema::new(vec![Field::new("i", DataType::Int32, true)]);
        let return_field = Arc::new(Field::new("i", DataType::Int32, true));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("i", 0));
        let exprs = [expr];
        let args = AccumulatorArgs {
            return_field,
            schema: &schema,
            expr_fields: &[],
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "max",
            is_distinct: false,
            exprs: &exprs,
        };
        assert!(SparkMinMax::new(true).accumulator(args).is_err());
    }
}
