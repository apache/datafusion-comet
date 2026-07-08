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
/// Float keys are normalized before counting (`-0.0` becomes `0.0` and every `NaN` becomes a
/// canonical `NaN`) to match Spark's `NormalizeFloatingNumbers` behaviour so that counts agree.
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
}

impl Mode {
    pub fn new(data_type: DataType) -> Self {
        Self {
            name: "mode".to_string(),
            signature: Signature::any(1, Volatility::Immutable),
            data_type,
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

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(&self.data_type)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ModeAccumulator::new(self.data_type.clone())))
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
        Ok(Box::new(ModeGroupsAccumulator::new(self.data_type.clone())))
    }
}

/// Normalize a scalar key so that Spark's floating-point normalization is honoured: `-0.0` and
/// `0.0` collapse to the same key and all `NaN` bit patterns collapse to a canonical `NaN`.
fn normalize_key(value: ScalarValue) -> ScalarValue {
    /// Collapse `-0.0`/`0.0` and every `NaN` to a canonical form for one float variant.
    macro_rules! normalize_float {
        ($variant:path, $f:expr, $nan:expr) => {
            if $f == 0.0 {
                $variant(Some(0.0))
            } else if $f.is_nan() {
                $variant(Some($nan))
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

/// Add each non-null value in `array` to `map`, normalizing float keys.
fn count_values(map: &mut HashMap<ScalarValue, i64>, array: &ArrayRef, idx: usize) -> Result<()> {
    if array.is_null(idx) {
        return Ok(());
    }
    let key = normalize_key(ScalarValue::try_from_array(array, idx)?);
    *map.entry(key).or_insert(0) += 1;
    Ok(())
}

/// Fold row `row` of the struct-state columns (`{values, counts}`) into `map`.
fn merge_state_row(
    map: &mut HashMap<ScalarValue, i64>,
    values_list: &arrow::array::ListArray,
    counts_list: &arrow::array::ListArray,
    row: usize,
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
        let key = normalize_key(ScalarValue::try_from_array(&values, i)?);
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

/// Non-grouped accumulator backing global `mode` aggregation.
#[derive(Debug)]
pub struct ModeAccumulator {
    counts: HashMap<ScalarValue, i64>,
    data_type: DataType,
}

impl ModeAccumulator {
    fn new(data_type: DataType) -> Self {
        Self {
            counts: HashMap::new(),
            data_type,
        }
    }
}

impl Accumulator for ModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        for i in 0..array.len() {
            count_values(&mut self.counts, array, i)?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let structs = states[0].as_struct();
        let values_list = structs.column(0).as_list::<i32>();
        let counts_list = structs.column(1).as_list::<i32>();
        for row in 0..structs.len() {
            merge_state_row(&mut self.counts, values_list, counts_list, row)?;
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
        size_of_val(self) + self.counts.capacity() * size_of::<(ScalarValue, i64)>()
    }
}

/// Vectorized grouped accumulator: one frequency map per group.
#[derive(Debug)]
pub struct ModeGroupsAccumulator {
    groups: Vec<HashMap<ScalarValue, i64>>,
    data_type: DataType,
}

impl ModeGroupsAccumulator {
    fn new(data_type: DataType) -> Self {
        Self {
            groups: Vec::new(),
            data_type,
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
            count_values(&mut self.groups[group_index], array, idx)?;
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
            merge_state_row(&mut self.groups[group_index], values_list, counts_list, row)?;
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let emitted = emit_to.take_needed(&mut self.groups);
        let mut results = Vec::with_capacity(emitted.len());
        for map in &emitted {
            results.push(eval_mode(map, &self.data_type)?);
        }
        ScalarValue::iter_to_array(results)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let emitted = emit_to.take_needed(&mut self.groups);
        let refs: Vec<&HashMap<ScalarValue, i64>> = emitted.iter().collect();
        Ok(vec![Arc::new(build_state(&self.data_type, &refs)?)])
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self
                .groups
                .iter()
                .map(|m| m.capacity() * size_of::<(ScalarValue, i64)>())
                .sum::<usize>()
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
        let mut acc = ModeAccumulator::new(DataType::Int32);
        acc.update_batch(&[i32_array(vec![Some(0), Some(10), Some(10)])])
            .unwrap();
        assert_eq!(eval_acc(&mut acc), ScalarValue::Int32(Some(10)));
    }

    #[test]
    fn nulls_are_ignored() {
        let mut acc = ModeAccumulator::new(DataType::Int32);
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
        let mut acc = ModeAccumulator::new(DataType::Int32);
        acc.update_batch(&[i32_array(vec![None, None])]).unwrap();
        assert_eq!(eval_acc(&mut acc), ScalarValue::Int32(None));
    }

    #[test]
    fn ties_break_to_smallest() {
        let mut acc = ModeAccumulator::new(DataType::Int32);
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
            let mut a = ModeAccumulator::new(DataType::Int32);
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

        let mut left = ModeAccumulator::new(DataType::Int32);
        left.update_batch(&[i32_array(vec![Some(1), Some(1), Some(3)])])
            .unwrap();
        let lstate = state_arrays(&mut left);

        let mut right = ModeAccumulator::new(DataType::Int32);
        right
            .update_batch(&[i32_array(vec![Some(2), Some(3), Some(3)])])
            .unwrap();
        let rstate = state_arrays(&mut right);

        let mut merged = ModeAccumulator::new(DataType::Int32);
        merged.merge_batch(&lstate).unwrap();
        merged.merge_batch(&rstate).unwrap();
        assert_eq!(eval_acc(&mut merged), single);
    }

    #[test]
    fn float_zero_and_nan_normalized() {
        let mut acc = ModeAccumulator::new(DataType::Float64);
        // -0.0 and 0.0 must count as one key.
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(0.0),
            Some(1.5),
        ]));
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(eval_acc(&mut acc), ScalarValue::Float64(Some(0.0)));
    }

    #[test]
    fn groups_accumulator_per_group_mode() {
        let mut acc = ModeGroupsAccumulator::new(DataType::Int32);
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
        let mut partial = ModeGroupsAccumulator::new(DataType::Int32);
        let values = i32_array(vec![Some(5), Some(5), Some(7), Some(9), Some(9), Some(9)]);
        partial
            .update_batch(&[values], &[0, 0, 0, 1, 1, 1], None, 2)
            .unwrap();
        let state = partial.state(EmitTo::All).unwrap();

        let mut final_acc = ModeGroupsAccumulator::new(DataType::Int32);
        final_acc.merge_batch(&state, &[0, 1], None, 2).unwrap();
        let result = final_acc.evaluate(EmitTo::All).unwrap();
        let result = result.as_primitive::<Int32Type>();
        assert_eq!(result.value(0), 5);
        assert_eq!(result.value(1), 9);
    }
}
