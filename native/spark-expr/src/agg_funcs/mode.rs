/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Int64Type};
use datafusion::common::{internal_datafusion_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::expressions::format_state_name;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

/// Spark's `mode` aggregate: returns the most frequent value within a group, ignoring NULLs.
///
/// Spark breaks ties on the default `mode(col)` form non-deterministically (the value is chosen
/// by JVM `OpenHashMap` iteration order), which a native hash map cannot reproduce bit-for-bit.
/// Comet resolves ties deterministically by returning the smallest value, so this function is
/// registered as `Incompatible` on the Scala side and is opt-in via `allowIncompatible`.
///
/// # Float keys
///
/// Spark keys the frequency map on the boxed input value and compares keys with
/// `OpenHashSet`'s `_data(pos) equals k` (`core/.../util/collection/OpenHashSet.scala:122`), i.e.
/// `java.lang.Double.equals`, which is defined via `doubleToLongBits`. That collapses every `NaN`
/// bit pattern to one key but keeps `-0.0` and `0.0` apart. Note that
/// `NormalizeFloatingNumbers` does *not* apply here: its `apply` only rewrites `WINDOW` and
/// `JOIN` patterns, so an aggregate's argument reaches `Mode` un-normalized.
///
/// Spark 4.2.0 changed this. SPARK-57329 ("mode() returns incorrect result when input contains
/// both -0.0 and 0.0") treats the split `-0.0`/`0.0` counts as a bug and normalizes the key at
/// update time, so from 4.2.0 on the two fold into a single key. `normalize_neg_zero` therefore
/// tracks the Spark version Comet is running against: it is `false` for Spark 3.4 through 4.1 and
/// `true` for 4.2.0+. `NaN` canonicalization is unconditional because every supported version
/// collapses `NaN` via `doubleToLongBits`.
///
/// Do not "simplify" this to always normalize: `max_by`/`min_by` need the opposite treatment,
/// because they compare the ordering column with `SQLOrderingUtil.compareDoubles`, which ties
/// `-0.0 == 0.0` on every version.
///
/// Spark's `Mode` is a `TypedImperativeAggregate` with a single aggregation-buffer attribute, so
/// the intermediate state is a single struct field `{ values: list<T>, counts: list<i64> }` (a
/// parallel-array encoding of the frequency map) to keep the partial/final buffer schemas aligned
/// with Spark.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Mode {
    name: String,
    signature: Signature,
    data_type: DataType,
    /// Whether `-0.0` folds into `0.0` before being used as a key (Spark 4.2.0+; SPARK-57329).
    normalize_neg_zero: bool,
}

impl Mode {
    pub fn new(data_type: DataType, normalize_neg_zero: bool) -> Self {
        Self {
            name: "mode".to_string(),
            signature: Signature::any(1, Volatility::Immutable),
            data_type,
            normalize_neg_zero,
        }
    }
}

/// Fields of the single struct state column `{values: list<T>, counts: list<i64>}`.
fn state_struct_fields(data_type: &DataType) -> Fields {
    let values_list = DataType::List(Arc::new(Field::new_list_field(data_type.clone(), true)));
    let counts_list = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
    Fields::from(vec![
        Field::new("values", values_list, false),
        Field::new("counts", counts_list, false),
    ])
}

/// Build the single-column struct state array holding one `{values, counts}` row per map.
fn build_state(data_type: &DataType, maps: &[&HashMap<ScalarValue, i64>]) -> Result<StructArray> {
    let mut value_lists = Vec::with_capacity(maps.len());
    let mut count_lists = Vec::with_capacity(maps.len());
    for map in maps {
        let mut values = Vec::with_capacity(map.len());
        let mut counts = Vec::with_capacity(map.len());
        for (value, &count) in map.iter() {
            values.push(value.clone());
            counts.push(ScalarValue::Int64(Some(count)));
        }
        value_lists.push(ScalarValue::List(ScalarValue::new_list(
            &values, data_type, true,
        )));
        count_lists.push(ScalarValue::List(ScalarValue::new_list(
            &counts,
            &DataType::Int64,
            true,
        )));
    }
    let values = ScalarValue::iter_to_array(value_lists)?;
    let counts = ScalarValue::iter_to_array(count_lists)?;
    Ok(StructArray::new(
        state_struct_fields(data_type),
        vec![values, counts],
        None,
    ))
}

impl AggregateUDFImpl for Mode {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ModeAccumulator::new(
            self.data_type.clone(),
            self.normalize_neg_zero,
        )))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(&self.name, "freq"),
            DataType::Struct(state_struct_fields(&self.data_type)),
            false,
        ))])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(ModeGroupsAccumulator::new(
            self.data_type.clone(),
            self.normalize_neg_zero,
        )))
    }
}

/// Canonicalize a float key so that map lookups reproduce Spark's key equality.
///
/// `ScalarValue`'s `PartialEq`/`Hash` for `Float32`/`Float64` are both defined on `to_bits()`, so
/// distinct `NaN` bit patterns would otherwise be distinct keys and `-0.0` is naturally kept apart
/// from `0.0`. Collapsing `NaN` to one canonical value therefore reproduces `doubleToLongBits`
/// equality, which is what Spark's `OpenHashSet` uses. `-0.0` is folded into `0.0` only when
/// `normalize_neg_zero` is set, i.e. only on Spark 4.2.0+ (SPARK-57329); see [`Mode`].
fn normalize_key(value: ScalarValue, normalize_neg_zero: bool) -> ScalarValue {
    macro_rules! normalize_float {
        ($variant:path, $f:expr, $nan:expr) => {
            if $f.is_nan() {
                $variant(Some($nan))
            } else if normalize_neg_zero && $f == 0.0 {
                // `-0.0 == 0.0` in IEEE 754, so this catches negative zero only.
                $variant(Some(0.0))
            } else {
                $variant(Some($f))
            }
        };
    }
    match value {
        ScalarValue::Float32(Some(f)) => normalize_float!(ScalarValue::Float32, f, f32::NAN),
        ScalarValue::Float64(Some(f)) => normalize_float!(ScalarValue::Float64, f, f64::NAN),
        other => other,
    }
}

/// Add each non-null value in `array` to `map`, canonicalizing float keys.
///
/// The map is intentionally keyed on the type-generic `ScalarValue` rather than a monomorphized
/// `HashMap<Hashable<T::Native>, _>`: `mode` supports every primitive type plus decimal, string and
/// the temporal types, so one generic map is simpler than a kernel per type. Revisit if the hot
/// primitive paths ever show up in a profile.
fn count_values(
    map: &mut HashMap<ScalarValue, i64>,
    array: &ArrayRef,
    idx: usize,
    normalize_neg_zero: bool,
) -> Result<()> {
    if array.is_null(idx) {
        return Ok(());
    }
    let key = normalize_key(ScalarValue::try_from_array(array, idx)?, normalize_neg_zero);
    *map.entry(key).or_insert(0) += 1;
    Ok(())
}

/// Fold row `row` of the struct-state columns (`{values, counts}`) into `map`.
fn merge_state_row(
    map: &mut HashMap<ScalarValue, i64>,
    values_list: &arrow::array::ListArray,
    counts_list: &arrow::array::ListArray,
    row: usize,
    normalize_neg_zero: bool,
) -> Result<()> {
    if values_list.is_null(row) {
        return Ok(());
    }
    let values = values_list.value(row);
    let counts = counts_list.value(row);
    let counts = counts
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| internal_datafusion_err!("mode state counts must be Int64"))?;
    for i in 0..values.len() {
        if values.is_null(i) {
            continue;
        }
        let key = normalize_key(ScalarValue::try_from_array(&values, i)?, normalize_neg_zero);
        *map.entry(key).or_insert(0) += counts.value(i);
    }
    Ok(())
}

/// Pick the mode from a frequency map: the value with the highest count, breaking ties by the
/// smallest value. Returns a null scalar of `data_type` when the map is empty.
fn eval_mode(counts: &HashMap<ScalarValue, i64>, data_type: &DataType) -> Result<ScalarValue> {
    let mut best: Option<(&ScalarValue, i64)> = None;
    for (value, &count) in counts.iter() {
        let wins = match best {
            None => true,
            Some((best_value, best_count)) => {
                count > best_count
                    || (count == best_count
                        && value.partial_cmp(best_value) == Some(Ordering::Less))
            }
        };
        if wins {
            best = Some((value, count));
        }
    }
    match best {
        Some((value, _)) => Ok(value.clone()),
        None => ScalarValue::try_from(data_type),
    }
}

/// Heap bytes held by the frequency map's keys, on top of the map's own slot allocation.
///
/// `HashMap::capacity` only accounts for the inline `(ScalarValue, i64)` slots, which misses the
/// `String`/`Vec<u8>`/boxed-decimal payloads behind variable-length keys. Under-reporting those
/// would hide real memory from the pool that drives spill decisions.
fn map_size(map: &HashMap<ScalarValue, i64>) -> usize {
    map.capacity() * size_of::<(ScalarValue, i64)>()
        + map
            .keys()
            .map(|k| k.size().saturating_sub(size_of::<ScalarValue>()))
            .sum::<usize>()
}

/// Non-grouped accumulator backing global `mode` aggregation.
#[derive(Debug)]
pub struct ModeAccumulator {
    counts: HashMap<ScalarValue, i64>,
    data_type: DataType,
    normalize_neg_zero: bool,
}

impl ModeAccumulator {
    fn new(data_type: DataType, normalize_neg_zero: bool) -> Self {
        Self {
            counts: HashMap::new(),
            data_type,
            normalize_neg_zero,
        }
    }
}

impl Accumulator for ModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        for i in 0..array.len() {
            count_values(&mut self.counts, array, i, self.normalize_neg_zero)?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let structs = states[0].as_struct();
        let values_list = structs.column(0).as_list::<i32>();
        let counts_list = structs.column(1).as_list::<i32>();
        for row in 0..structs.len() {
            merge_state_row(
                &mut self.counts,
                values_list,
                counts_list,
                row,
                self.normalize_neg_zero,
            )?;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let array = build_state(&self.data_type, &[&self.counts])?;
        Ok(vec![ScalarValue::Struct(Arc::new(array))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        eval_mode(&self.counts, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + map_size(&self.counts)
    }
}

/// Vectorized grouped accumulator: one frequency map per group.
#[derive(Debug)]
pub struct ModeGroupsAccumulator {
    groups: Vec<HashMap<ScalarValue, i64>>,
    data_type: DataType,
    normalize_neg_zero: bool,
}

impl ModeGroupsAccumulator {
    fn new(data_type: DataType, normalize_neg_zero: bool) -> Self {
        Self {
            groups: Vec::new(),
            data_type,
            normalize_neg_zero,
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        if self.groups.len() < total_num_groups {
            self.groups.resize_with(total_num_groups, HashMap::new);
        }
    }
}

impl GroupsAccumulator for ModeGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize(total_num_groups);
        let array = &values[0];
        for (idx, &group_index) in group_indices.iter().enumerate() {
            if let Some(f) = opt_filter {
                if !f.is_valid(idx) || !f.value(idx) {
                    continue;
                }
            }
            count_values(
                &mut self.groups[group_index],
                array,
                idx,
                self.normalize_neg_zero,
            )?;
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
        self.resize(total_num_groups);
        let structs = values[0].as_struct();
        let values_list = structs.column(0).as_list::<i32>();
        let counts_list = structs.column(1).as_list::<i32>();
        for (row, &group_index) in group_indices.iter().enumerate() {
            merge_state_row(
                &mut self.groups[group_index],
                values_list,
                counts_list,
                row,
                self.normalize_neg_zero,
            )?;
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let emitted = emit_to.take_needed(&mut self.groups);
        // `ScalarValue::iter_to_array` errors on an empty iterator. The grouped-aggregate stream
        // never emits zero groups, so this is unreachable; assert it rather than leaving the
        // dependency implicit.
        debug_assert!(!emitted.is_empty(), "mode: evaluate called with no groups");
        let mut results = Vec::with_capacity(emitted.len());
        for map in &emitted {
            results.push(eval_mode(map, &self.data_type)?);
        }
        ScalarValue::iter_to_array(results)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let emitted = emit_to.take_needed(&mut self.groups);
        // As in `evaluate`: `build_state` funnels into `ScalarValue::iter_to_array`, which needs a
        // non-empty iterator.
        debug_assert!(!emitted.is_empty(), "mode: state called with no groups");
        let refs: Vec<&HashMap<ScalarValue, i64>> = emitted.iter().collect();
        Ok(vec![Arc::new(build_state(&self.data_type, &refs)?)])
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.groups.iter().map(map_size).sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array};
    use arrow::datatypes::Int32Type;

    fn i32_array(values: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from(values))
    }

    fn eval_acc(acc: &mut ModeAccumulator) -> ScalarValue {
        acc.evaluate().unwrap()
    }

    #[test]
    fn most_frequent_value() {
        let mut acc = ModeAccumulator::new(DataType::Int32, false);
        acc.update_batch(&[i32_array(vec![Some(0), Some(10), Some(10)])])
            .unwrap();
        assert_eq!(eval_acc(&mut acc), ScalarValue::Int32(Some(10)));
    }

    #[test]
    fn nulls_are_ignored() {
        let mut acc = ModeAccumulator::new(DataType::Int32, false);
        acc.update_batch(&[i32_array(vec![
            Some(10),
            None,
            None,
            None,
            Some(10),
            Some(7),
        ])])
        .unwrap();
        assert_eq!(eval_acc(&mut acc), ScalarValue::Int32(Some(10)));
    }

    #[test]
    fn empty_input_is_null() {
        let mut acc = ModeAccumulator::new(DataType::Int32, false);
        acc.update_batch(&[i32_array(vec![None, None])]).unwrap();
        assert_eq!(eval_acc(&mut acc), ScalarValue::Int32(None));
    }

    #[test]
    fn ties_break_to_smallest() {
        let mut acc = ModeAccumulator::new(DataType::Int32, false);
        // 10 and 20 each appear twice; Comet returns the smallest tied value.
        acc.update_batch(&[i32_array(vec![Some(20), Some(10), Some(10), Some(20)])])
            .unwrap();
        assert_eq!(eval_acc(&mut acc), ScalarValue::Int32(Some(10)));
    }

    /// Turn an accumulator's `Vec<ScalarValue>` state into the state arrays `merge_batch` consumes.
    fn state_arrays(acc: &mut ModeAccumulator) -> Vec<ArrayRef> {
        acc.state()
            .unwrap()
            .into_iter()
            .map(|s| ScalarValue::iter_to_array(vec![s]).unwrap())
            .collect()
    }

    #[test]
    fn merge_matches_single_shot() {
        let single = {
            let mut a = ModeAccumulator::new(DataType::Int32, false);
            a.update_batch(&[i32_array(vec![
                Some(1),
                Some(1),
                Some(2),
                Some(3),
                Some(3),
                Some(3),
            ])])
            .unwrap();
            eval_acc(&mut a)
        };

        let mut left = ModeAccumulator::new(DataType::Int32, false);
        left.update_batch(&[i32_array(vec![Some(1), Some(1), Some(3)])])
            .unwrap();
        let lstate = state_arrays(&mut left);

        let mut right = ModeAccumulator::new(DataType::Int32, false);
        right
            .update_batch(&[i32_array(vec![Some(2), Some(3), Some(3)])])
            .unwrap();
        let rstate = state_arrays(&mut right);

        let mut merged = ModeAccumulator::new(DataType::Int32, false);
        merged.merge_batch(&lstate).unwrap();
        merged.merge_batch(&rstate).unwrap();
        assert_eq!(eval_acc(&mut merged), single);
    }

    /// Input from the SPARK-57329 report: `-0.0` x2, `0.0` x2, `5.0` x3. The winner differs
    /// depending on whether the two zeros share a key, so it pins each version's behaviour
    /// without depending on how `-0.0` and `0.0` compare.
    fn signed_zero_input() -> ArrayRef {
        Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(-0.0),
            Some(0.0),
            Some(0.0),
            Some(5.0),
            Some(5.0),
            Some(5.0),
        ]))
    }

    #[test]
    fn signed_zeros_are_distinct_keys_before_spark_42() {
        // Spark 3.4-4.1 key on `java.lang.Double.equals`, so counts are -0.0:2, 0.0:2, 5.0:3 and
        // 5.0 wins outright.
        let mut acc = ModeAccumulator::new(DataType::Float64, false);
        acc.update_batch(&[signed_zero_input()]).unwrap();
        assert_eq!(eval_acc(&mut acc), ScalarValue::Float64(Some(5.0)));
    }

    #[test]
    fn signed_zeros_share_a_key_from_spark_42() {
        // Spark 4.2.0+ normalizes the key (SPARK-57329), so counts are 0.0:4, 5.0:3 and the
        // zero wins.
        let mut acc = ModeAccumulator::new(DataType::Float64, true);
        acc.update_batch(&[signed_zero_input()]).unwrap();
        assert_eq!(eval_acc(&mut acc), ScalarValue::Float64(Some(0.0)));
    }

    #[test]
    fn nan_collapses_on_every_version() {
        // `doubleToLongBits` maps every NaN to one key on all supported versions, so the two NaNs
        // outvote the single 1.0 regardless of the -0.0 setting.
        for normalize_neg_zero in [false, true] {
            let mut acc = ModeAccumulator::new(DataType::Float64, normalize_neg_zero);
            let arr: ArrayRef = Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(-f64::NAN),
                Some(1.0),
            ]));
            acc.update_batch(&[arr]).unwrap();
            match eval_acc(&mut acc) {
                ScalarValue::Float64(Some(v)) => assert!(
                    v.is_nan(),
                    "expected NaN with normalize_neg_zero={normalize_neg_zero}, got {v}"
                ),
                other => panic!("expected Float64(NaN), got {other:?}"),
            }
        }
    }

    #[test]
    fn signed_zero_key_survives_merge() {
        // The partial/final split must not lose the distinction: each side sees one -0.0 and one
        // 0.0, and 5.0 only wins if they stay separate through the merge.
        let mut left = ModeAccumulator::new(DataType::Float64, false);
        left.update_batch(&[
            Arc::new(Float64Array::from(vec![Some(-0.0), Some(0.0), Some(5.0)])) as ArrayRef,
        ])
        .unwrap();
        let lstate = state_arrays(&mut left);

        let mut right = ModeAccumulator::new(DataType::Float64, false);
        right
            .update_batch(&[Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(5.0),
                Some(5.0),
            ])) as ArrayRef])
            .unwrap();
        let rstate = state_arrays(&mut right);

        let mut merged = ModeAccumulator::new(DataType::Float64, false);
        merged.merge_batch(&lstate).unwrap();
        merged.merge_batch(&rstate).unwrap();
        assert_eq!(eval_acc(&mut merged), ScalarValue::Float64(Some(5.0)));
    }

    #[test]
    fn groups_accumulator_per_group_mode() {
        let mut acc = ModeGroupsAccumulator::new(DataType::Int32, false);
        let values = i32_array(vec![Some(5), Some(5), Some(9), Some(9), Some(9)]);
        acc.update_batch(&[values], &[0, 0, 1, 1, 1], None, 2)
            .unwrap();
        let result = acc.evaluate(EmitTo::All).unwrap();
        let result = result.as_primitive::<Int32Type>();
        assert_eq!(result.value(0), 5);
        assert_eq!(result.value(1), 9);
    }

    #[test]
    fn groups_accumulator_merge_roundtrip() {
        // Partial over two groups, then merge its state into a fresh accumulator.
        let mut partial = ModeGroupsAccumulator::new(DataType::Int32, false);
        let values = i32_array(vec![Some(5), Some(5), Some(7), Some(9), Some(9), Some(9)]);
        partial
            .update_batch(&[values], &[0, 0, 0, 1, 1, 1], None, 2)
            .unwrap();
        let state = partial.state(EmitTo::All).unwrap();

        let mut final_acc = ModeGroupsAccumulator::new(DataType::Int32, false);
        final_acc.merge_batch(&state, &[0, 1], None, 2).unwrap();
        let result = final_acc.evaluate(EmitTo::All).unwrap();
        let result = result.as_primitive::<Int32Type>();
        assert_eq!(result.value(0), 5);
        assert_eq!(result.value(1), 9);
    }
}
