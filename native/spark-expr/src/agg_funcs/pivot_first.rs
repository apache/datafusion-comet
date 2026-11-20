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

//! Spark's `PivotFirst` aggregate. Used only by the second phase of the optimized pivot plan
//! that the analyzer rule `ResolvePivot` builds. For each group, `PivotFirst` maintains one slot
//! per pivot value; on each input row it looks the pivot column up in the pivot list, and writes
//! the value column into that slot when a match is found and the value is non-null. Rows with
//! unmatched pivot values are ignored; matched rows with a null value column leave the slot
//! unchanged (matches Spark).
//!
//! State layout is one column per pivot slot, matching Spark's `aggBufferAttributes` (which
//! declares `indexSize` `AttributeReference`s, one per pivot value). This keeps the shuffle
//! schema between Partial and Final consistent with what Spark catalyst declared; otherwise
//! the shuffle exchange rejects the batch. Spark sizes that buffer from its index of the pivot
//! values, so the serde falls back when the list holds duplicates, and the native slot count is
//! always the list length. The result reassembles the slots into a `ListArray` matching
//! `PivotFirst.dataType = ArrayType(value_type)`.
//!
//! The optimized pivot plan groups by every column of the pivoted relation other than the pivot
//! and aggregated columns, so the aggregate runs grouped, through [`PivotFirstGroupsAccumulator`],
//! unless the relation has no other column. That case uses [`PivotFirstAccumulator`], which is
//! the same accumulator holding a single group.

use crate::float_semantics::ieee_equality_key;
use arrow::array::{
    new_null_array, Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray,
    BooleanBufferBuilder, ListArray, PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Field, FieldRef, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, TimeUnit, TimestampMicrosecondType,
};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{internal_err, not_impl_err, HashMap, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature,
};
use datafusion::physical_expr::expressions::format_state_name;
use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

/// UDAF implementation of Spark's `PivotFirst`.
///
/// `pivot_values` is a fixed, plan-time list of the pivot column values that occupy each
/// output slot, and `pivot_index` maps an input row's pivot column value to its slot. The index
/// is wrapped in `Arc` so that every accumulator shares it rather than rebuilding it.
#[derive(Debug)]
pub struct SparkPivotFirst {
    signature: Signature,
    value_type: DataType,
    // Kept for `PartialEq`/`Hash` (identity of the aggregate for plan comparison) and for the
    // slot count `state_fields` needs.
    pivot_values: Vec<ScalarValue>,
    pivot_index: Arc<PivotIndex>,
}

impl PartialEq for SparkPivotFirst {
    fn eq(&self, other: &Self) -> bool {
        self.value_type == other.value_type && self.pivot_values == other.pivot_values
    }
}

impl Eq for SparkPivotFirst {}

impl std::hash::Hash for SparkPivotFirst {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value_type.hash(state);
        self.pivot_values.hash(state);
    }
}

impl SparkPivotFirst {
    /// `pivot_values` are the pivot list in output order. The serde only sends a non-empty list
    /// without duplicates, of a pivot column type that [`PivotIndex`] supports.
    pub fn try_new(value_type: DataType, pivot_values: Vec<ScalarValue>) -> Result<Self> {
        let pivot_index = Arc::new(PivotIndex::try_new(&pivot_values)?);
        Ok(Self {
            signature: Signature::user_defined(Immutable),
            value_type,
            pivot_values,
            pivot_index,
        })
    }
}

impl AggregateUDFImpl for SparkPivotFirst {
    fn name(&self) -> &str {
        "pivot_first"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            self.value_type.clone(),
            true,
        ))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // One field per pivot slot, matching Spark's aggBufferAttributes so the shuffle
        // exchange sees the same schema catalyst declared. `format_state_name` is the same
        // helper other aggregates in this crate use (see `avg.rs`, `stddev.rs`).
        Ok((0..self.pivot_values.len())
            .map(|i| {
                Arc::new(Field::new(
                    format_state_name(args.name, &i.to_string()),
                    self.value_type.clone(),
                    true,
                ))
            })
            .collect())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(PivotFirstAccumulator {
            groups: self.groups_accumulator()?,
        }))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        // The ungrouped accumulator is built on the grouped one, so it cannot stand in for a
        // value type that the grouped one does not store.
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(self.groups_accumulator()?)
    }
}

impl SparkPivotFirst {
    /// The grouped accumulator, for any value type in Spark's `PivotFirst.supportsDataType`.
    fn groups_accumulator(&self) -> Result<Box<dyn PivotFirstGroups>> {
        let index = Arc::clone(&self.pivot_index);
        let num_slots = self.pivot_values.len();
        let value_type = self.value_type.clone();
        Ok(match &self.value_type {
            // Booleans are stored as 0/1 bytes, so that one primitive implementation covers
            // every value type.
            DataType::Boolean | DataType::Int8 => Box::new(
                PivotFirstGroupsAccumulator::<Int8Type>::new(value_type, index, num_slots),
            ),
            DataType::Int16 => Box::new(PivotFirstGroupsAccumulator::<Int16Type>::new(
                value_type, index, num_slots,
            )),
            DataType::Int32 => Box::new(PivotFirstGroupsAccumulator::<Int32Type>::new(
                value_type, index, num_slots,
            )),
            DataType::Int64 => Box::new(PivotFirstGroupsAccumulator::<Int64Type>::new(
                value_type, index, num_slots,
            )),
            DataType::Float32 => Box::new(PivotFirstGroupsAccumulator::<Float32Type>::new(
                value_type, index, num_slots,
            )),
            DataType::Float64 => Box::new(PivotFirstGroupsAccumulator::<Float64Type>::new(
                value_type, index, num_slots,
            )),
            DataType::Decimal128(_, _) => Box::new(
                PivotFirstGroupsAccumulator::<Decimal128Type>::new(value_type, index, num_slots),
            ),
            other => return internal_err!("PivotFirst does not support value type {other}"),
        })
    }
}

/// Marks a row whose pivot column value is in no slot.
const NO_SLOT: u32 = u32::MAX;

/// Maps a pivot column value to its output slot the way Spark's `PivotFirst` matches it.
///
/// For the pivot column types the serde admits, Spark looks the value up in a Scala
/// `HashMap[Any, Int]` built from the pivot list. Matching therefore goes through Scala's `==`
/// on the boxed Catalyst value, which is plain value equality except for floats, where it is
/// IEEE 754 equality: `-0.0` and `0.0` are one key, and NaN matches nothing, not even a NaN in
/// the pivot list. A null pivot column value matches a null in the pivot list, which is what
/// Spark's `pivotIndex.getOrElse(null, -1)` does.
#[derive(Debug)]
struct PivotIndex {
    /// The Arrow type of the pivot values. A pivot column with another layout, such as a
    /// dictionary-encoded string column, is cast to it before the lookup.
    key_type: DataType,
    keys: PivotKeys,
    null_slot: Option<u32>,
}

/// The slot of each non-null pivot value.
#[derive(Debug)]
enum PivotKeys {
    /// Slots of `false` and `true`.
    Boolean([Option<u32>; 2]),
    /// Integers, dates, timestamps and decimals, keyed on their value as an `i128`.
    Integer(HashMap<i128, u32>),
    /// Floats, keyed on the bits of their [`ieee_equality_key`] as an `f64`. A NaN in the pivot
    /// list has no key, so its slot is never written.
    Float(HashMap<u64, u32>),
    Utf8(HashMap<Box<str>, u32>),
}

impl PivotIndex {
    fn try_new(pivot_values: &[ScalarValue]) -> Result<Self> {
        let Some(first) = pivot_values.first() else {
            return internal_err!("PivotFirst requires at least one pivot value");
        };
        let key_type = first.data_type();
        let mut keys = match &key_type {
            DataType::Boolean => PivotKeys::Boolean([None, None]),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Decimal128(_, _) => PivotKeys::Integer(HashMap::new()),
            DataType::Float32 | DataType::Float64 => PivotKeys::Float(HashMap::new()),
            DataType::Utf8 => PivotKeys::Utf8(HashMap::new()),
            other => return internal_err!("PivotFirst does not support pivot column type {other}"),
        };
        if pivot_values.len() >= NO_SLOT as usize {
            return internal_err!("PivotFirst supports fewer than {NO_SLOT} pivot values");
        }
        let mut null_slot = None;
        for (slot, value) in pivot_values.iter().enumerate() {
            let slot = slot as u32;
            // `Some(true)` for a new key, `Some(false)` for a repeated one, `None` for a NaN.
            let inserted = match (&mut keys, value) {
                (_, value) if value.is_null() => Some(null_slot.replace(slot).is_none()),
                (PivotKeys::Boolean(slots), ScalarValue::Boolean(Some(b))) => {
                    Some(slots[*b as usize].replace(slot).is_none())
                }
                (PivotKeys::Integer(map), value) => match integer_key(value) {
                    Some(key) => Some(map.insert(key, slot).is_none()),
                    None => return mismatched_pivot_value(value, &key_type),
                },
                (PivotKeys::Float(map), ScalarValue::Float32(Some(v))) => {
                    float_key(*v).map(|key| map.insert(key, slot).is_none())
                }
                (PivotKeys::Float(map), ScalarValue::Float64(Some(v))) => {
                    float_key(*v).map(|key| map.insert(key, slot).is_none())
                }
                (PivotKeys::Utf8(map), ScalarValue::Utf8(Some(s))) => {
                    Some(map.insert(s.as_str().into(), slot).is_none())
                }
                (_, value) => return mismatched_pivot_value(value, &key_type),
            };
            if inserted == Some(false) {
                // Spark keeps a repeated value's last position and sizes its buffer from the
                // distinct values, so the slot layout here would no longer match its schema.
                return internal_err!(
                    "PivotFirst pivot value {value} is repeated; the serde should have fallen back"
                );
            }
        }
        Ok(Self {
            key_type,
            keys,
            null_slot,
        })
    }

    /// The slot of every row of `pivots`, or [`NO_SLOT`] for a row whose value is not in the
    /// pivot list.
    fn slots(&self, pivots: &ArrayRef) -> Result<Vec<u32>> {
        // A timestamp's value does not depend on its time zone label, so only the unit has to
        // agree for the column to be read as it is.
        let same_layout = match (pivots.data_type(), &self.key_type) {
            (DataType::Timestamp(a, _), DataType::Timestamp(b, _)) => a == b,
            (a, b) => a == b,
        };
        let pivots = if same_layout {
            Arc::clone(pivots)
        } else {
            cast(pivots, &self.key_type)?
        };
        let null_slot = self.null_slot.unwrap_or(NO_SLOT);
        Ok(match &self.keys {
            PivotKeys::Boolean(slots) => {
                lookup(pivots.as_boolean().iter(), null_slot, |b| slots[b as usize])
            }
            PivotKeys::Integer(map) => match pivots.data_type() {
                DataType::Int8 => integer_slots::<Int8Type>(&pivots, null_slot, map),
                DataType::Int16 => integer_slots::<Int16Type>(&pivots, null_slot, map),
                DataType::Int32 => integer_slots::<Int32Type>(&pivots, null_slot, map),
                DataType::Int64 => integer_slots::<Int64Type>(&pivots, null_slot, map),
                DataType::Date32 => integer_slots::<Date32Type>(&pivots, null_slot, map),
                DataType::Timestamp(_, _) => {
                    integer_slots::<TimestampMicrosecondType>(&pivots, null_slot, map)
                }
                DataType::Decimal128(_, _) => {
                    integer_slots::<Decimal128Type>(&pivots, null_slot, map)
                }
                other => return internal_err!("PivotFirst cannot read pivot column type {other}"),
            },
            PivotKeys::Float(map) => match pivots.data_type() {
                DataType::Float32 => float_slots::<Float32Type>(&pivots, null_slot, map),
                DataType::Float64 => float_slots::<Float64Type>(&pivots, null_slot, map),
                other => return internal_err!("PivotFirst cannot read pivot column type {other}"),
            },
            PivotKeys::Utf8(map) => lookup(pivots.as_string::<i32>().iter(), null_slot, |s| {
                map.get(s).copied()
            }),
        })
    }
}

fn mismatched_pivot_value<T>(value: &ScalarValue, key_type: &DataType) -> Result<T> {
    internal_err!("PivotFirst pivot value {value:?} does not have the pivot column type {key_type}")
}

fn integer_key(value: &ScalarValue) -> Option<i128> {
    Some(match value {
        ScalarValue::Int8(Some(v)) => i128::from(*v),
        ScalarValue::Int16(Some(v)) => i128::from(*v),
        ScalarValue::Int32(Some(v)) | ScalarValue::Date32(Some(v)) => i128::from(*v),
        ScalarValue::Int64(Some(v)) | ScalarValue::TimestampMicrosecond(Some(v), _) => {
            i128::from(*v)
        }
        ScalarValue::Decimal128(Some(v), _, _) => *v,
        _ => return None,
    })
}

fn float_key(v: impl Into<f64>) -> Option<u64> {
    ieee_equality_key(v.into()).map(f64::to_bits)
}

fn lookup<T>(
    values: impl Iterator<Item = Option<T>>,
    null_slot: u32,
    slot_of: impl Fn(T) -> Option<u32>,
) -> Vec<u32> {
    values
        .map(|v| match v {
            Some(v) => slot_of(v).unwrap_or(NO_SLOT),
            None => null_slot,
        })
        .collect()
}

fn integer_slots<T>(pivots: &ArrayRef, null_slot: u32, map: &HashMap<i128, u32>) -> Vec<u32>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,
{
    lookup(pivots.as_primitive::<T>().iter(), null_slot, |v| {
        map.get(&v.into()).copied()
    })
}

fn float_slots<T>(pivots: &ArrayRef, null_slot: u32, map: &HashMap<u64, u32>) -> Vec<u32>
where
    T: ArrowPrimitiveType,
    T::Native: Into<f64>,
{
    lookup(pivots.as_primitive::<T>().iter(), null_slot, |v| {
        float_key(v).and_then(|key| map.get(&key).copied())
    })
}

/// Grouped `PivotFirst` state. Each group owns `num_slots` cells, stored group-major in one flat
/// buffer, so that a group's cells are contiguous and `evaluate` hands the buffer to the output
/// list as it is.
#[derive(Debug)]
struct PivotFirstGroupsAccumulator<T: ArrowPrimitiveType> {
    /// The value type Spark declared. `T` stores it, as `Int8` for `Boolean`.
    value_type: DataType,
    pivot_index: Arc<PivotIndex>,
    num_slots: usize,
    /// `values[group * num_slots + slot]`. Only the cells set in `is_set` hold a value.
    values: Vec<T::Native>,
    /// Which cells have been written. A cell nothing has written to is null, as in Spark.
    is_set: BooleanBufferBuilder,
}

impl<T: ArrowPrimitiveType> PivotFirstGroupsAccumulator<T> {
    fn new(value_type: DataType, pivot_index: Arc<PivotIndex>, num_slots: usize) -> Self {
        Self {
            value_type,
            pivot_index,
            num_slots,
            values: Vec::new(),
            is_set: BooleanBufferBuilder::new(0),
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        let num_cells = total_num_groups * self.num_slots;
        if num_cells > self.values.len() {
            self.values.resize(num_cells, T::Native::default());
            self.is_set.append_n(num_cells - self.is_set.len(), false);
        }
    }

    /// Writes `values[row]` into `cell` for each `(row, cell)` pair, skipping a null value, which
    /// Spark never writes. A later row overwrites an earlier one, as Spark's `update` and `merge`
    /// do.
    fn write(
        &mut self,
        values: &ArrayRef,
        writes: impl Iterator<Item = (usize, usize)>,
    ) -> Result<()> {
        let values = if self.value_type == DataType::Boolean {
            cast(values, &DataType::Int8)?
        } else {
            Arc::clone(values)
        };
        let Some(values) = values.as_primitive_opt::<T>() else {
            return internal_err!(
                "PivotFirst expected {} values, got {}",
                self.value_type,
                values.data_type()
            );
        };
        for (row, cell) in writes {
            if values.is_valid(row) {
                self.values[cell] = values.value(row);
                self.is_set.set_bit(cell, true);
            }
        }
        Ok(())
    }

    /// Removes the cells of the groups that `emit_to` covers, returning their values and which of
    /// them are set. The remaining groups move to the front.
    fn take_cells(&mut self, emit_to: EmitTo) -> (Vec<T::Native>, BooleanBuffer) {
        // `take_needed` copies out whichever part is smaller, so emitting a few groups does not
        // hand the output the buffer that still holds all the others. It counts cells, and each
        // group has `num_slots` of them.
        let cells = match emit_to {
            EmitTo::All => EmitTo::All,
            EmitTo::First(n) => EmitTo::First(n * self.num_slots),
        };
        let values = cells.take_needed(&mut self.values);
        let num_cells = values.len();
        let is_set = self.is_set.finish();
        self.is_set
            .append_buffer(&is_set.slice(num_cells, is_set.len() - num_cells));
        (values, is_set.slice(0, num_cells))
    }

    /// Gives stored cells the declared value type: the precision and scale of a decimal, or
    /// booleans in place of their `Int8` storage.
    fn to_value_type(&self, cells: PrimitiveArray<T>) -> Result<ArrayRef> {
        if self.value_type == DataType::Boolean {
            Ok(cast(&cells, &DataType::Boolean)?)
        } else {
            Ok(Arc::new(cells.with_data_type(self.value_type.clone())))
        }
    }
}

impl<T: ArrowPrimitiveType> GroupsAccumulator for PivotFirstGroupsAccumulator<T> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let [pivots, values] = values else {
            return internal_err!(
                "PivotFirst expects 2 inputs (pivot, value); got {}",
                values.len()
            );
        };
        self.resize(total_num_groups);
        let slots = self.pivot_index.slots(pivots)?;
        let num_slots = self.num_slots;
        let writes = group_indices
            .iter()
            .zip(slots)
            .enumerate()
            .filter(|&(row, (_, slot))| {
                slot != NO_SLOT && opt_filter.is_none_or(|f| f.is_valid(row) && f.value(row))
            })
            .map(|(row, (&group, slot))| (row, group * num_slots + slot as usize));
        self.write(values, writes)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        if values.len() != self.num_slots {
            return internal_err!(
                "PivotFirst merge expects {} state columns; got {}",
                self.num_slots,
                values.len()
            );
        }
        self.resize(total_num_groups);
        // Each column is one slot, and any non-null cell overwrites the group's slot (matches
        // Spark's `PivotFirst.merge`). A group's state holds only the pivot values its rows had,
        // so the columns are mostly null, and only their valid rows are visited.
        let num_slots = self.num_slots;
        for (slot, column) in values.iter().enumerate() {
            let cell = |row: usize| (row, group_indices[row] * num_slots + slot);
            match column.logical_nulls() {
                Some(nulls) => self.write(column, nulls.valid_indices().map(cell))?,
                None => self.write(column, (0..column.len()).map(cell))?,
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (values, is_set) = self.take_cells(emit_to);
        let num_groups = values.len() / self.num_slots;
        let cells = PrimitiveArray::new(values.into(), Some(NullBuffer::new(is_set)));
        let offsets =
            OffsetBuffer::<i32>::from_lengths(std::iter::repeat_n(self.num_slots, num_groups));
        let field = Arc::new(Field::new_list_field(self.value_type.clone(), true));
        Ok(Arc::new(ListArray::try_new(
            field,
            offsets,
            self.to_value_type(cells)?,
            None,
        )?))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (values, is_set) = self.take_cells(emit_to);
        let num_slots = self.num_slots;
        let num_groups = values.len() / num_slots;
        (0..num_slots)
            .map(|slot| {
                let column: ScalarBuffer<T::Native> = (0..num_groups)
                    .map(|group| values[group * num_slots + slot])
                    .collect();
                let nulls = BooleanBuffer::collect_bool(num_groups, |group| {
                    is_set.value(group * num_slots + slot)
                });
                self.to_value_type(PrimitiveArray::new(column, Some(NullBuffer::new(nulls))))
            })
            .collect()
    }

    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.values.capacity() * size_of::<T::Native>()
            + self.is_set.capacity() / 8
    }
}

/// A [`PivotFirstGroupsAccumulator`] of any value type, as [`PivotFirstAccumulator`] uses it.
trait PivotFirstGroups: GroupsAccumulator + Sync + Debug {
    /// The slots of `group` in the value type, left in place. The slots of a group that no row
    /// has reached are null.
    fn group_slots(&self, group: usize) -> Result<ArrayRef>;
}

impl<T: ArrowPrimitiveType + Debug> PivotFirstGroups for PivotFirstGroupsAccumulator<T> {
    fn group_slots(&self, group: usize) -> Result<ArrayRef> {
        let first = group * self.num_slots;
        let cells = first..first + self.num_slots;
        if cells.end > self.values.len() {
            return Ok(new_null_array(&self.value_type, self.num_slots));
        }
        let is_set =
            BooleanBuffer::collect_bool(self.num_slots, |slot| self.is_set.get_bit(first + slot));
        let values = PrimitiveArray::new(
            self.values[cells].to_vec().into(),
            Some(NullBuffer::new(is_set)),
        );
        self.to_value_type(values)
    }
}

/// Ungrouped `PivotFirst`: a [`PivotFirstGroupsAccumulator`] whose rows all belong to one group.
#[derive(Debug)]
struct PivotFirstAccumulator {
    groups: Box<dyn PivotFirstGroups>,
}

impl Accumulator for PivotFirstAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let num_rows = values.first().map_or(0, |v| v.len());
        self.groups
            .update_batch(values, &vec![0; num_rows], None, 1)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Reads the slots rather than emitting them, because a window aggregate can evaluate
        // the same accumulator more than once.
        let slots = self.groups.group_slots(0)?;
        Ok(SingleRowListArrayBuilder::new(slots).build_list_scalar())
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.groups.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // One scalar per slot, matching `state_fields`.
        let slots = self.groups.group_slots(0)?;
        (0..slots.len())
            .map(|slot| ScalarValue::try_from_array(&slots, slot))
            .collect()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let num_rows = states.first().map_or(0, |s| s.len());
        self.groups.merge_batch(states, &vec![0; num_rows], 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Date32Array, Decimal128Array, DictionaryArray, Float32Array, Float64Array, Int32Array,
        Int64Array, Int8Array, StringArray, StringViewArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::Int32Type as DictKey;
    use arrow::datatypes::Schema;

    fn s(v: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(v.to_string()))
    }
    fn f64s(values: &[f64]) -> Vec<ScalarValue> {
        values
            .iter()
            .map(|v| ScalarValue::Float64(Some(*v)))
            .collect()
    }

    fn index(pivot_values: &[ScalarValue]) -> Arc<PivotIndex> {
        Arc::new(PivotIndex::try_new(pivot_values).unwrap())
    }

    fn slots(pivot_values: &[ScalarValue], pivots: ArrayRef) -> Vec<u32> {
        index(pivot_values).slots(&pivots).unwrap()
    }

    /// `SparkPivotFirst` ignores the accumulator arguments, so any well-formed set will do.
    fn args(schema: &Schema) -> AccumulatorArgs<'_> {
        AccumulatorArgs {
            return_field: Arc::new(Field::new("pivot_first", DataType::Null, true)),
            schema,
            expr_fields: &[],
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "pivot_first",
            is_distinct: false,
            exprs: &[],
        }
    }

    fn grouped_acc(
        value_type: DataType,
        pivot_values: &[ScalarValue],
    ) -> Box<dyn GroupsAccumulator> {
        SparkPivotFirst::try_new(value_type, pivot_values.to_vec())
            .unwrap()
            .create_groups_accumulator(args(&Schema::empty()))
            .unwrap()
    }

    fn ungrouped_acc(value_type: DataType, pivot_values: &[ScalarValue]) -> Box<dyn Accumulator> {
        SparkPivotFirst::try_new(value_type, pivot_values.to_vec())
            .unwrap()
            .accumulator(args(&Schema::empty()))
            .unwrap()
    }

    /// The lists `evaluate` returned, one `Vec` of slot values per group.
    fn int32_lists(lists: &ArrayRef) -> Vec<Vec<Option<i32>>> {
        let lists = lists.as_list::<i32>();
        (0..lists.len())
            .map(|i| {
                let list = lists.value(i);
                list.as_primitive::<Int32Type>().iter().collect()
            })
            .collect()
    }

    /// The list an ungrouped accumulator's `evaluate` returned.
    fn int32_list(value: ScalarValue) -> Vec<Option<i32>> {
        let ScalarValue::List(list) = value else {
            panic!("expected a list, got {value:?}");
        };
        let list: ArrayRef = list;
        int32_lists(&list).remove(0)
    }

    #[test]
    fn index_maps_values_to_slots() {
        let pivots: ArrayRef = Arc::new(StringArray::from(vec![
            Some("c"),
            Some("z"),
            None,
            Some("a"),
        ]));
        // `z` is not in the list, and nor is null, so neither row has a slot.
        assert_eq!(
            slots(&[s("a"), s("b"), s("c")], pivots),
            vec![2, NO_SLOT, NO_SLOT, 0]
        );
    }

    #[test]
    fn null_pivot_value_matches_null_rows() {
        let pivots: ArrayRef = Arc::new(StringArray::from(vec![None, Some("a")]));
        let pivot_values = [s("a"), ScalarValue::Utf8(None)];
        assert_eq!(slots(&pivot_values, pivots), vec![1, 0]);
    }

    #[test]
    fn signed_zero_pivot_values_are_one_key() {
        // Spark keys the pivot list on the boxed Catalyst value, where `-0.0 == 0.0`, so a
        // `-0.0` pivot value matches rows carrying either sign of zero, and the other way round.
        let zeros: ArrayRef = Arc::new(Float64Array::from(vec![0.0, -0.0, 1.0]));
        assert_eq!(
            slots(&f64s(&[-0.0, 1.0]), Arc::clone(&zeros)),
            vec![0, 0, 1]
        );
        assert_eq!(slots(&f64s(&[0.0, 1.0]), zeros), vec![0, 0, 1]);
    }

    #[test]
    fn nan_pivot_value_matches_nothing() {
        // Scala's `==` on Double is IEEE, so `pivotIndex.getOrElse(NaN, -1)` never hits even
        // when NaN is in the pivot list, and so does a NaN with another payload.
        let nans: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::NAN,
            f64::from_bits(0xfff8_0000_0000_0000),
            2.0,
        ]));
        assert_eq!(
            slots(&f64s(&[f64::NAN, 2.0]), nans),
            vec![NO_SLOT, NO_SLOT, 1]
        );
    }

    #[test]
    fn float32_signed_zero_and_nan_follow_the_same_rules() {
        let pivot_values = [
            ScalarValue::Float32(Some(-0.0)),
            ScalarValue::Float32(Some(f32::NAN)),
        ];
        let pivots: ArrayRef = Arc::new(Float32Array::from(vec![0.0, f32::NAN]));
        assert_eq!(slots(&pivot_values, pivots), vec![0, NO_SLOT]);
    }

    #[test]
    fn integer_like_pivot_columns() {
        let pivots: ArrayRef = Arc::new(Int8Array::from(vec![Some(3), Some(4), None]));
        let pivot_values = [ScalarValue::Int8(Some(4)), ScalarValue::Int8(Some(3))];
        assert_eq!(slots(&pivot_values, pivots), vec![1, 0, NO_SLOT]);

        let pivots: ArrayRef = Arc::new(Date32Array::from(vec![19723, 19724]));
        let pivot_values = [ScalarValue::Date32(Some(19724))];
        assert_eq!(slots(&pivot_values, pivots), vec![NO_SLOT, 0]);

        let pivots: ArrayRef = Arc::new(
            Decimal128Array::from(vec![150, 225])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let pivot_values = [ScalarValue::Decimal128(Some(225), 10, 2)];
        assert_eq!(slots(&pivot_values, pivots), vec![NO_SLOT, 0]);

        let pivots: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
        let pivot_values = [
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Boolean(None),
        ];
        assert_eq!(slots(&pivot_values, pivots), vec![NO_SLOT, 1, 0]);
    }

    #[test]
    fn timestamp_time_zone_label_does_not_matter() {
        // Comet labels TimestampType values UTC, but the value is the instant either way.
        let pivots: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![10, 20]).with_timezone("Asia/Tokyo"));
        let pivot_values = [ScalarValue::TimestampMicrosecond(
            Some(20),
            Some("UTC".into()),
        )];
        assert_eq!(slots(&pivot_values, pivots), vec![NO_SLOT, 0]);
    }

    #[test]
    fn string_column_in_another_layout_is_cast() {
        let pivot_values = [s("a"), s("b")];
        let dictionary: DictionaryArray<DictKey> = vec!["b", "x", "a"].into_iter().collect();
        assert_eq!(
            slots(&pivot_values, Arc::new(dictionary)),
            vec![1, NO_SLOT, 0]
        );
        let view: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b"]));
        assert_eq!(slots(&pivot_values, view), vec![0, 1]);
    }

    #[test]
    fn repeated_pivot_values_are_rejected() {
        // Spark collapses these into fewer buffer slots, so the serde falls back for them. Two
        // NaNs are distinct keys for Spark, so they are allowed.
        assert!(PivotIndex::try_new(&[s("a"), s("b"), s("a")]).is_err());
        assert!(PivotIndex::try_new(&f64s(&[0.0, -0.0])).is_err());
        assert!(PivotIndex::try_new(&[ScalarValue::Utf8(None), ScalarValue::Utf8(None)]).is_err());
        assert!(PivotIndex::try_new(&f64s(&[f64::NAN, f64::NAN])).is_ok());
        assert!(PivotIndex::try_new(&[]).is_err());
    }

    #[test]
    fn unmatchable_pivot_value_keeps_its_state_slot() {
        // A NaN entry is left out of the index, but it still has a slot, or `state()` would
        // return fewer columns than `state_fields` declares and the shuffle exchange would
        // reject the batch.
        let mut grouped = grouped_acc(DataType::Int32, &f64s(&[f64::NAN, 0.0]));
        let pivots: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN, -0.0]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        grouped
            .update_batch(&[pivots, values], &[0, 0], None, 1)
            .unwrap();
        let state = grouped.state(EmitTo::All).unwrap();
        assert_eq!(state.len(), 2);
        assert!(state[0].is_null(0));
        assert_eq!(state[1].as_primitive::<Int32Type>().value(0), 20);

        let mut ungrouped = ungrouped_acc(DataType::Int32, &f64s(&[f64::NAN, 0.0]));
        assert_eq!(ungrouped.state().unwrap().len(), 2);
    }

    #[test]
    fn grouped_update_writes_each_group_slot() {
        let mut acc = grouped_acc(DataType::Int32, &[s("a"), s("b"), s("c")]);
        let pivots: ArrayRef = Arc::new(StringArray::from(vec!["a", "c", "b", "z", "a", "a"]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(3),
            Some(2),
            Some(99),
            Some(10),
            None,
        ]));
        // Group 1 gets 'a' twice: the later 10 wins, and the null after it changes nothing.
        acc.update_batch(&[pivots, values], &[0, 0, 1, 1, 1, 1], None, 2)
            .unwrap();
        assert_eq!(
            int32_lists(&acc.evaluate(EmitTo::All).unwrap()),
            vec![vec![Some(1), None, Some(3)], vec![Some(10), Some(2), None]]
        );
    }

    #[test]
    fn grouped_update_honors_filter() {
        let mut acc = grouped_acc(DataType::Int32, &[s("a"), s("b")]);
        let pivots: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let filter = BooleanArray::from(vec![Some(false), None]);
        acc.update_batch(&[pivots, values], &[0, 0], Some(&filter), 1)
            .unwrap();
        assert_eq!(
            int32_lists(&acc.evaluate(EmitTo::All).unwrap()),
            vec![vec![None, None]]
        );
    }

    #[test]
    fn grouped_state_merges_across_partials() {
        let pivot_values = [s("a"), s("b"), s("c")];
        let mut left = grouped_acc(DataType::Int32, &pivot_values);
        let mut right = grouped_acc(DataType::Int32, &pivot_values);
        left.update_batch(
            &[
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
            &[0, 1],
            None,
            2,
        )
        .unwrap();
        right
            .update_batch(
                &[
                    Arc::new(StringArray::from(vec!["c", "a"])),
                    Arc::new(Int32Array::from(vec![3, 4])),
                ],
                &[0, 0],
                None,
                1,
            )
            .unwrap();

        let left_state = left.state(EmitTo::All).unwrap();
        assert_eq!(left_state.len(), 3);
        assert!(left_state.iter().all(|c| c.data_type() == &DataType::Int32));

        // Right's group 0 is the final group 1, and its 'a' overwrites nothing there, while
        // left's group 0 keeps its 'a'.
        let mut merged = grouped_acc(DataType::Int32, &pivot_values);
        merged.merge_batch(&left_state, &[0, 1], 2).unwrap();
        merged
            .merge_batch(&right.state(EmitTo::All).unwrap(), &[1], 2)
            .unwrap();
        assert_eq!(
            int32_lists(&merged.evaluate(EmitTo::All).unwrap()),
            vec![vec![Some(1), None, None], vec![Some(4), Some(2), Some(3)]]
        );
    }

    #[test]
    fn grouped_merge_reads_state_without_a_null_buffer() {
        // A shuffle reader can hand over a state column with no null buffer at all.
        let mut acc = grouped_acc(DataType::Int32, &[s("a"), s("b")]);
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None]));
        assert!(a.nulls().is_none());
        acc.merge_batch(&[a, b], &[1, 0], 2).unwrap();
        assert_eq!(
            int32_lists(&acc.evaluate(EmitTo::All).unwrap()),
            vec![vec![Some(2), None], vec![Some(1), Some(3)]]
        );
    }

    #[test]
    fn grouped_emit_first_keeps_the_remaining_groups() {
        let mut acc = grouped_acc(DataType::Int32, &[s("a"), s("b")]);
        acc.update_batch(
            &[
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
            &[0, 1, 2],
            None,
            3,
        )
        .unwrap();
        assert_eq!(
            int32_lists(&acc.evaluate(EmitTo::First(2)).unwrap()),
            vec![vec![Some(1), None], vec![None, Some(2)]]
        );
        // The old group 2 is now group 0.
        acc.update_batch(
            &[
                Arc::new(StringArray::from(vec!["b"])),
                Arc::new(Int32Array::from(vec![4])),
            ],
            &[0],
            None,
            1,
        )
        .unwrap();
        let state = acc.state(EmitTo::All).unwrap();
        assert_eq!(state[0].as_primitive::<Int32Type>().value(0), 3);
        assert_eq!(state[1].as_primitive::<Int32Type>().value(0), 4);
        assert_eq!(acc.evaluate(EmitTo::All).unwrap().len(), 0);
    }

    #[test]
    fn grouped_emit_first_copies_out_only_the_emitted_groups() {
        // The output of a partial emit must not hold the buffer that keeps the other groups.
        let num_groups = 1000;
        let mut acc = grouped_acc(DataType::Int32, &[s("a"), s("b")]);
        acc.update_batch(
            &[
                Arc::new(StringArray::from(vec!["b"; num_groups])),
                Arc::new(Int32Array::from_iter_values(0..num_groups as i32)),
            ],
            &(0..num_groups).collect::<Vec<_>>(),
            None,
            num_groups,
        )
        .unwrap();
        let lists = acc.evaluate(EmitTo::First(10)).unwrap();
        let cells = lists.as_list::<i32>().values().as_primitive::<Int32Type>();
        assert_eq!(cells.len(), 20);
        assert_eq!(
            cells.values().inner().capacity(),
            cells.len() * size_of::<i32>()
        );
        assert_eq!(
            int32_lists(&acc.evaluate(EmitTo::All).unwrap())[0],
            vec![None, Some(10)]
        );
    }

    #[test]
    fn grouped_boolean_values_round_trip() {
        let pivot_values = [s("a"), s("b"), s("c")];
        let mut partial = grouped_acc(DataType::Boolean, &pivot_values);
        partial
            .update_batch(
                &[
                    Arc::new(StringArray::from(vec!["a", "b"])),
                    Arc::new(BooleanArray::from(vec![true, false])),
                ],
                &[0, 0],
                None,
                1,
            )
            .unwrap();
        let state = partial.state(EmitTo::All).unwrap();
        assert!(state.iter().all(|c| c.data_type() == &DataType::Boolean));

        let mut fin = grouped_acc(DataType::Boolean, &pivot_values);
        fin.merge_batch(&state, &[0], 1).unwrap();
        let lists = fin.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            lists.data_type(),
            &DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true)))
        );
        let list = lists.as_list::<i32>().value(0);
        assert_eq!(
            list.as_boolean().iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), None]
        );
    }

    #[test]
    fn grouped_decimal_values_keep_precision_and_scale() {
        let value_type = DataType::Decimal128(10, 2);
        let mut acc = grouped_acc(value_type.clone(), &[s("a")]);
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![1234])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        acc.update_batch(
            &[Arc::new(StringArray::from(vec!["a"])), values],
            &[0],
            None,
            1,
        )
        .unwrap();
        let lists = acc.evaluate(EmitTo::All).unwrap();
        let list = lists.as_list::<i32>().value(0);
        assert_eq!(list.data_type(), &value_type);
        assert_eq!(list.as_primitive::<Decimal128Type>().value(0), 1234);
    }

    #[test]
    fn ungrouped_accumulator_is_one_group() {
        let pivot_values = [
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(None),
        ];
        let mut partial = ungrouped_acc(DataType::Int32, &pivot_values);
        partial
            .update_batch(&[
                Arc::new(Int64Array::from(vec![
                    Some(2),
                    None,
                    Some(7),
                    Some(1),
                    Some(2),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(20),
                    Some(30),
                    Some(70),
                    None,
                    Some(21),
                ])),
            ])
            .unwrap();
        // 7 is in no slot, the null value leaves slot 0 empty, the later 21 wins slot 1, and
        // the null pivot value collects the null row. Evaluating leaves the slots in place, so
        // a second call agrees with the first.
        for _ in 0..2 {
            assert_eq!(
                int32_list(partial.evaluate().unwrap()),
                vec![None, Some(21), Some(30)]
            );
        }
        let state = partial.state().unwrap();
        assert_eq!(
            state,
            [None, Some(21), Some(30)].map(ScalarValue::Int32).to_vec()
        );

        // A final accumulator merges one row per partial state, and a later non-null slot wins.
        let other = [Some(1), None, Some(31)].map(ScalarValue::Int32);
        let columns: Vec<ArrayRef> = state
            .into_iter()
            .zip(other)
            .map(|(a, b)| ScalarValue::iter_to_array([a, b]).unwrap())
            .collect();
        let mut fin = ungrouped_acc(DataType::Int32, &pivot_values);
        fin.merge_batch(&columns).unwrap();
        assert_eq!(
            int32_list(fin.evaluate().unwrap()),
            vec![Some(1), Some(21), Some(31)]
        );
    }

    #[test]
    fn ungrouped_accumulator_without_rows_is_all_null() {
        // A pivot with no grouping column returns one row of nulls over an empty input.
        let value_type = DataType::Decimal128(10, 2);
        let mut acc = ungrouped_acc(value_type.clone(), &[s("a"), s("b")]);
        assert_eq!(
            acc.state().unwrap(),
            vec![ScalarValue::Decimal128(None, 10, 2); 2]
        );
        let ScalarValue::List(list) = acc.evaluate().unwrap() else {
            panic!("expected a list");
        };
        let slots = list.value(0);
        assert_eq!(slots.data_type(), &value_type);
        assert_eq!((slots.len(), slots.null_count()), (2, 2));
    }
}
