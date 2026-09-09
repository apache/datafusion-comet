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

//! Spark `collect_list` / `collect_set`, wrapping `datafusion_spark`'s implementations with a
//! `GroupsAccumulator` so grouped aggregation does not go through DataFusion's
//! `GroupsAccumulatorAdapter`.
//!
//! The adapter keeps one boxed `Accumulator` per group and slices every input batch into a
//! per-group `update_batch` call, which dominates the runtime once a query groups by a
//! high-cardinality key. See <https://github.com/apache/datafusion-comet/issues/5797>.

use arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, BooleanArray, ListArray, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{cast, concat, interleave, take};
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::row::{RowConverter, SortField};
use datafusion::common::{internal_datafusion_err, internal_err, Result as DFResult, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature,
};
use datafusion_spark::function::aggregate::collect::{SparkCollectList, SparkCollectSet};
use std::mem::{size_of, take as take_field};
use std::sync::Arc;
use twox_hash::XxHash64;

/// Spark's `collect_list`.
///
/// Everything except grouped aggregation is delegated to [`SparkCollectList`]; this type only
/// adds the [`CollectListGroupsAccumulator`].
#[derive(Debug, PartialEq, Eq, Hash, Default)]
pub struct CometCollectList {
    inner: SparkCollectList,
}

impl CometCollectList {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AggregateUDFImpl for CometCollectList {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.inner.return_type(arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    fn default_value(&self, data_type: &DataType) -> DFResult<ScalarValue> {
        self.inner.default_value(data_type)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct && args.order_bys.is_empty()
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(CollectListGroupsAccumulator::new(
            args.expr_fields[0].data_type().clone(),
        )))
    }
}

/// Spark's `collect_set`.
///
/// Everything except grouped aggregation is delegated to [`SparkCollectSet`].
#[derive(Debug, PartialEq, Eq, Hash, Default)]
pub struct CometCollectSet {
    inner: SparkCollectSet,
}

impl CometCollectSet {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AggregateUDFImpl for CometCollectSet {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.inner.return_type(arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    fn default_value(&self, data_type: &DataType) -> DFResult<ScalarValue> {
        self.inner.default_value(data_type)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct && args.order_bys.is_empty()
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(CollectSetGroupsAccumulator::try_new(
            args.expr_fields[0].data_type().clone(),
        )?))
    }
}

/// Casts `array` to `datatype` when the two have drifted apart, which a dictionary-encoded input
/// or nested field nullability surviving a shuffle round trip can both cause. The cast is
/// metadata-only when only nullability differs.
fn coerce(array: &ArrayRef, datatype: &DataType) -> DFResult<ArrayRef> {
    if array.data_type() == datatype {
        Ok(Arc::clone(array))
    } else {
        Ok(cast(array.as_ref(), datatype)?)
    }
}

/// The list field a `collect_list` / `collect_set` state or result is built from. Spark's element
/// type is always nullable, and the group's own list entry is never null.
fn list_field(element_type: &DataType) -> FieldRef {
    Arc::new(Field::new_list_field(element_type.clone(), true))
}

/// `GroupsAccumulator` for Spark's `collect_list`.
///
/// Input arrays are retained rather than copied per group; each one is paired with the
/// `(group, row range)` contributions it makes. `evaluate` then rearranges those contributions
/// into group order with a counting sort and gathers the values in a single pass, so the cost is
/// proportional to the data rather than to the number of groups.
#[derive(Debug)]
struct CollectListGroupsAccumulator {
    /// The element type as declared by the plan.
    datatype: DataType,
    /// Source arrays: input arrays from `update_batch`, list values arrays from `merge_batch`.
    batches: Vec<ArrayRef>,
    /// Contributions of each retained batch, in the order they were seen.
    ranges: Vec<Vec<RowRange>>,
    /// Running total of what `batches` and `ranges` hold. `size()` is called once per input
    /// batch, so it cannot afford to walk the retained batches to work this out.
    retained_bytes: usize,
    num_groups: usize,
}

/// A run of consecutive rows of one batch that all belong to `group`.
#[derive(Debug, Clone, Copy, Default)]
struct RowRange {
    group: u32,
    start: u32,
    len: u32,
}

impl RowRange {
    fn end(&self) -> u32 {
        self.start + self.len
    }
}

/// Below this many rows per range, gathering with `interleave` beats slicing and concatenating.
/// Scattered single rows (a high-cardinality `update_batch`) sit at 1 range per row; merging
/// partial states back together sits at one range per group per batch, which is far longer.
const CONCAT_MIN_RANGE_LEN: usize = 4;

impl CollectListGroupsAccumulator {
    fn new(datatype: DataType) -> Self {
        Self {
            datatype,
            batches: Vec::new(),
            ranges: Vec::new(),
            retained_bytes: 0,
            num_groups: 0,
        }
    }

    /// Appends `row` of the batch being accumulated into `ranges`, extending the previous range
    /// when the row continues it.
    fn push_row(ranges: &mut Vec<RowRange>, group: u32, row: u32) {
        match ranges.last_mut() {
            Some(last) if last.group == group && last.end() == row => last.len += 1,
            _ => ranges.push(RowRange {
                group,
                start: row,
                len: 1,
            }),
        }
    }

    fn retain(&mut self, batch: ArrayRef, mut ranges: Vec<RowRange>) {
        if ranges.is_empty() {
            return;
        }
        // `ranges` is sized for one range per row; run coalescing can leave most of that unused,
        // and the excess would be retained for as long as the batch is.
        if ranges.len() * 2 <= ranges.capacity() {
            ranges.shrink_to_fit();
        }
        self.retained_bytes +=
            batch.get_array_memory_size() + ranges.capacity() * size_of::<RowRange>();
        self.batches.push(batch);
        self.ranges.push(ranges);
    }

    fn clear(&mut self) {
        // `size()` reports capacity rather than length, so release the buffers outright.
        self.batches = Vec::new();
        self.ranges = Vec::new();
        self.retained_bytes = 0;
        self.num_groups = 0;
    }

    /// Drops the contributions of groups `0..emit_groups` and renumbers what is left to start at
    /// 0. Batches that no longer contribute anything are released, and batches only partly
    /// retained are compacted so their emitted rows stop being pinned.
    fn compact(&mut self, emit_groups: u32) -> DFResult<()> {
        let batches = take_field(&mut self.batches);
        let ranges = take_field(&mut self.ranges);
        self.retained_bytes = 0;
        self.num_groups -= emit_groups as usize;

        for (batch, ranges) in batches.into_iter().zip(ranges) {
            if ranges.iter().all(|r| r.group >= emit_groups) {
                // Nothing was emitted from this batch, so only the group numbering changes and
                // the batch itself can be kept as it is.
                let mut ranges = ranges;
                for range in &mut ranges {
                    range.group -= emit_groups;
                }
                self.retain(batch, ranges);
                continue;
            }
            let mut retained: Vec<RowRange> = Vec::new();
            let mut retained_rows: Vec<u32> = Vec::new();
            for range in ranges {
                if range.group < emit_groups {
                    continue;
                }
                retained.push(RowRange {
                    group: range.group - emit_groups,
                    start: retained_rows.len() as u32,
                    len: range.len,
                });
                retained_rows.extend(range.start..range.end());
            }
            if retained.is_empty() {
                continue;
            }
            // Compact the batch so the rows this emit released stop being pinned.
            let batch = take(batch.as_ref(), &UInt32Array::from(retained_rows), None)?;
            self.retain(batch, retained);
        }
        Ok(())
    }
}

impl GroupsAccumulator for CollectListGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.num_groups = self.num_groups.max(total_num_groups);
        let input = coerce(&values[0], &self.datatype)?;
        // Spark's collect_list drops null inputs.
        let nulls = input.logical_nulls();
        let mut ranges = Vec::with_capacity(group_indices.len());
        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if let Some(filter) = opt_filter {
                if filter.is_null(row_idx) || !filter.value(row_idx) {
                    continue;
                }
            }
            if nulls.as_ref().is_some_and(|n| n.is_null(row_idx)) {
                continue;
            }
            Self::push_row(&mut ranges, group_idx as u32, row_idx as u32);
        }
        self.retain(input, ranges);
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.num_groups = self.num_groups.max(total_num_groups);
        let Some(lists) = values[0].as_list_opt::<i32>() else {
            return internal_err!(
                "collect_list expected a List state, got {:?}",
                values[0].data_type()
            );
        };
        let elements = coerce(lists.values(), &self.datatype)?;
        let offsets = lists.offsets();
        let mut ranges = Vec::with_capacity(group_indices.len());
        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            let (start, end) = (offsets[row_idx], offsets[row_idx + 1]);
            if lists.is_null(row_idx) || end == start {
                continue;
            }
            ranges.push(RowRange {
                group: group_idx as u32,
                start: start as u32,
                len: (end - start) as u32,
            });
        }
        self.retain(elements, ranges);
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let emit_groups = match emit_to {
            EmitTo::All => self.num_groups,
            EmitTo::First(n) => n,
        };

        // Counting sort of the retained ranges into group order: count, prefix sum, scatter.
        // Rows and ranges are counted separately: rows give the output list offsets, ranges
        // decide how the values are gathered.
        let mut range_counts = vec![0u32; emit_groups];
        let mut row_counts = vec![0u32; emit_groups];
        let mut num_ranges = 0usize;
        let mut num_rows = 0usize;
        for ranges in &self.ranges {
            for range in ranges {
                if (range.group as usize) < emit_groups {
                    range_counts[range.group as usize] += 1;
                    row_counts[range.group as usize] += range.len;
                    num_ranges += 1;
                    num_rows += range.len as usize;
                }
            }
        }
        let offsets = OffsetBuffer::<i32>::from_lengths(row_counts.iter().map(|c| *c as usize));

        let values = if num_rows == 0 {
            new_empty_array(&self.datatype)
        } else if num_rows >= num_ranges * CONCAT_MIN_RANGE_LEN {
            // Long runs: scatter the ranges themselves into group order, then copy each one
            // whole. `concat` preallocates and does a single copy per run.
            let mut cursors = prefix_sum(&range_counts);
            let mut ordered = vec![(0u32, RowRange::default()); num_ranges];
            for (batch_idx, ranges) in self.ranges.iter().enumerate() {
                for range in ranges {
                    let group = range.group as usize;
                    if group < emit_groups {
                        ordered[cursors[group] as usize] = (batch_idx as u32, *range);
                        cursors[group] += 1;
                    }
                }
            }
            let slices: Vec<ArrayRef> = ordered
                .iter()
                .map(|(batch_idx, range)| {
                    self.batches[*batch_idx as usize]
                        .slice(range.start as usize, range.len as usize)
                })
                .collect();
            let slices: Vec<&dyn Array> = slices.iter().map(|s| s.as_ref()).collect();
            concat(&slices)?
        } else {
            // Scattered rows: scatter straight into the gather indices and take them all in one
            // `interleave` call.
            let mut cursors = prefix_sum(&row_counts);
            let mut indices = vec![(0usize, 0usize); num_rows];
            for (batch_idx, ranges) in self.ranges.iter().enumerate() {
                for range in ranges {
                    let group = range.group as usize;
                    if group < emit_groups {
                        let mut cursor = cursors[group] as usize;
                        for row in range.start..range.end() {
                            indices[cursor] = (batch_idx, row as usize);
                            cursor += 1;
                        }
                        cursors[group] = cursor as u32;
                    }
                }
            }
            let sources: Vec<&dyn Array> = self.batches.iter().map(|b| b.as_ref()).collect();
            interleave(&sources, &indices)?
        };

        match emit_to {
            EmitTo::All => self.clear(),
            EmitTo::First(n) => self.compact(n as u32)?,
        }

        // A group that collected nothing gets an empty list, not a null one, matching Spark.
        Ok(Arc::new(ListArray::new(
            list_field(&self.datatype),
            offsets,
            values,
            None,
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> DFResult<Vec<ArrayRef>> {
        single_row_lists(&values[0], opt_filter, &self.datatype)
    }

    fn size(&self) -> usize {
        self.retained_bytes
            + self.batches.capacity() * size_of::<ArrayRef>()
            + self.ranges.capacity() * size_of::<Vec<RowRange>>()
            + self.datatype.size()
    }
}

/// Exclusive prefix sum of `counts`, giving each group the position its first item is written to.
fn prefix_sum(counts: &[u32]) -> Vec<u32> {
    let mut cursors = Vec::with_capacity(counts.len());
    let mut total = 0u32;
    for count in counts {
        cursors.push(total);
        total += count;
    }
    cursors
}

/// A `NullBuffer` that is null wherever `filter` does not select the row, so that a filtered row
/// can be represented as a null list entry. Mirrors DataFusion's `filter_to_nulls`, which lives
/// in `datafusion-functions-aggregate-common` and is not re-exported by the `datafusion` facade.
fn filter_to_nulls(filter: &BooleanArray) -> NullBuffer {
    let selected = match filter.nulls() {
        Some(nulls) => filter.values() & nulls.inner(),
        None => filter.values().clone(),
    };
    NullBuffer::new(selected)
}

/// Turns every row of `input` into its own single-element list, which is what
/// `GroupsAccumulator::convert_to_state` has to produce for both collect functions. Null and
/// filtered-out rows become null list entries, which `merge_batch` skips, so the input array
/// itself can back the lists without being copied.
fn single_row_lists(
    input: &ArrayRef,
    opt_filter: Option<&BooleanArray>,
    datatype: &DataType,
) -> DFResult<Vec<ArrayRef>> {
    let filter_nulls = opt_filter.map(filter_to_nulls);
    let nulls = NullBuffer::union(filter_nulls.as_ref(), input.logical_nulls().as_ref());
    Ok(vec![Arc::new(ListArray::new(
        list_field(datatype),
        OffsetBuffer::from_repeated_length(1, input.len()),
        Arc::clone(input),
        nulls,
    ))])
}

/// One distinct `(group, value)` pair. `start` and `len` address the value's row-encoded bytes
/// in [`CollectSetGroupsAccumulator::arena`], and `hash` is of those bytes alone, so it survives
/// the group renumbering [`CollectSetGroupsAccumulator::compact`] does.
#[derive(Debug, Clone, Copy)]
struct SetEntry {
    group: u32,
    start: u32,
    len: u32,
    hash: u64,
}

/// `GroupsAccumulator` for Spark's `collect_set`.
///
/// Values are kept row-encoded (`arrow::row`) in one shared arena, deduplicated on insert against
/// an open-addressed index keyed by `(group, encoded value)`. Encoding is what the per-group
/// `Accumulator` did as well, so which values count as duplicates is unchanged; what changes is
/// that a batch is encoded once for all of its groups instead of once per group, and that the
/// state is flat rather than one hash table plus one `IndexSet` per group.
#[derive(Debug)]
struct CollectSetGroupsAccumulator {
    /// The element type as declared by the plan.
    datatype: DataType,
    converter: RowConverter,
    /// Row-encoded bytes of every distinct `(group, value)` pair, in insertion order.
    arena: Vec<u8>,
    entries: Vec<SetEntry>,
    index: RowIndex,
    num_groups: usize,
}

impl CollectSetGroupsAccumulator {
    fn try_new(datatype: DataType) -> DFResult<Self> {
        let converter = RowConverter::new(vec![SortField::new(datatype.clone())])?;
        Ok(Self {
            datatype,
            converter,
            arena: Vec::new(),
            entries: Vec::new(),
            index: RowIndex::default(),
            num_groups: 0,
        })
    }

    /// Records `(group, encoded)` unless the group already holds that value. `hash` is of
    /// `encoded` alone, so a caller that already has it (`compact`) need not recompute it.
    fn insert(&mut self, group: u32, hash: u64, encoded: &[u8]) {
        let Self {
            arena,
            entries,
            index,
            ..
        } = self;
        let key = key_hash(group, hash);
        let Some(slot) = index.vacant_slot(key, |entry_idx| {
            let entry = &entries[entry_idx as usize];
            entry.group == group
                && entry.hash == hash
                && &arena[entry.start as usize..(entry.start + entry.len) as usize] == encoded
        }) else {
            return;
        };
        let entry_idx = entries.len() as u32;
        entries.push(SetEntry {
            group,
            start: arena.len() as u32,
            len: encoded.len() as u32,
            hash,
        });
        arena.extend_from_slice(encoded);
        index.fill(slot, entry_idx);
        if index.is_full(entries.len()) {
            index.grow(entries.iter().map(|e| key_hash(e.group, e.hash)));
        }
    }

    fn entry_bytes(&self, entry: &SetEntry) -> &[u8] {
        &self.arena[entry.start as usize..(entry.start + entry.len) as usize]
    }

    /// Drops the entries of groups `0..emit_groups` and renumbers what is left to start at 0.
    fn compact(&mut self, emit_groups: u32) {
        let old_entries = take_field(&mut self.entries);
        let old_arena = take_field(&mut self.arena);
        self.index = RowIndex::default();
        self.num_groups -= emit_groups as usize;
        for entry in old_entries {
            if entry.group < emit_groups {
                continue;
            }
            let bytes = &old_arena[entry.start as usize..(entry.start + entry.len) as usize];
            self.insert(entry.group - emit_groups, entry.hash, bytes);
        }
    }

    fn clear(&mut self) {
        // `size()` reports capacity rather than length, so release the buffers outright.
        self.arena = Vec::new();
        self.entries = Vec::new();
        self.index = RowIndex::default();
        self.num_groups = 0;
    }
}

impl GroupsAccumulator for CollectSetGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.num_groups = self.num_groups.max(total_num_groups);
        let input = coerce(&values[0], &self.datatype)?;
        let rows = self.converter.convert_columns(&[Arc::clone(&input)])?;
        // Spark's collect_set drops null inputs.
        let nulls = input.logical_nulls();
        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if let Some(filter) = opt_filter {
                if filter.is_null(row_idx) || !filter.value(row_idx) {
                    continue;
                }
            }
            if nulls.as_ref().is_some_and(|n| n.is_null(row_idx)) {
                continue;
            }
            let row = rows.row(row_idx);
            self.insert(group_idx as u32, value_hash(row.as_ref()), row.as_ref());
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.num_groups = self.num_groups.max(total_num_groups);
        let Some(lists) = values[0].as_list_opt::<i32>() else {
            return internal_err!(
                "collect_set expected a List state, got {:?}",
                values[0].data_type()
            );
        };
        let elements = coerce(lists.values(), &self.datatype)?;
        let rows = self.converter.convert_columns(&[elements])?;
        let offsets = lists.offsets();
        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if lists.is_null(row_idx) {
                continue;
            }
            for pos in offsets[row_idx] as usize..offsets[row_idx + 1] as usize {
                let row = rows.row(pos);
                self.insert(group_idx as u32, value_hash(row.as_ref()), row.as_ref());
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let emit_groups = match emit_to {
            EmitTo::All => self.num_groups,
            EmitTo::First(n) => n,
        };

        // Counting sort of the entries into group order: count, prefix sum, scatter.
        let mut counts = vec![0u32; emit_groups];
        let mut total = 0u32;
        for entry in &self.entries {
            if (entry.group as usize) < emit_groups {
                counts[entry.group as usize] += 1;
                total += 1;
            }
        }
        let offsets = OffsetBuffer::<i32>::from_lengths(counts.iter().map(|c| *c as usize));
        let mut cursors = prefix_sum(&counts);

        let values = if total == 0 {
            new_empty_array(&self.datatype)
        } else {
            let mut ordered = vec![0u32; total as usize];
            for (entry_idx, entry) in self.entries.iter().enumerate() {
                let group = entry.group as usize;
                if group < emit_groups {
                    ordered[cursors[group] as usize] = entry_idx as u32;
                    cursors[group] += 1;
                }
            }
            let parser = self.converter.parser();
            let decoded = self.converter.convert_rows(
                ordered
                    .iter()
                    .map(|&i| parser.parse(self.entry_bytes(&self.entries[i as usize]))),
            )?;
            let decoded = decoded
                .into_iter()
                .next()
                .ok_or_else(|| internal_datafusion_err!("collect_set decoded no columns"))?;
            // `RowConverter` always decodes to the physical type, so a dictionary element type
            // has to be restored.
            coerce(&decoded, &self.datatype)?
        };

        match emit_to {
            EmitTo::All => self.clear(),
            EmitTo::First(n) => self.compact(n as u32),
        }

        Ok(Arc::new(ListArray::new(
            list_field(&self.datatype),
            offsets,
            values,
            None,
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> DFResult<Vec<ArrayRef>> {
        // A single-element list is trivially deduplicated; `merge_batch` does the real work.
        single_row_lists(&values[0], opt_filter, &self.datatype)
    }

    fn size(&self) -> usize {
        self.arena.capacity()
            + self.entries.capacity() * size_of::<SetEntry>()
            + self.index.size()
            + self.datatype.size()
    }
}

/// Hash of a row-encoded value, independent of the group that holds it.
fn value_hash(encoded: &[u8]) -> u64 {
    XxHash64::oneshot(0, encoded)
}

/// Mixes a group index into a value hash. Keeping the two separate lets an entry's value hash be
/// reused when [`CollectSetGroupsAccumulator::compact`] renumbers its group.
fn key_hash(group: u32, value_hash: u64) -> u64 {
    value_hash ^ (group as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Open-addressed index from a hash to an entry position in
/// [`CollectSetGroupsAccumulator::entries`], with linear probing and a load factor of 0.5.
///
/// Slots hold `entry index + 1` so that `0` means empty.
#[derive(Debug, Default)]
struct RowIndex {
    slots: Vec<u32>,
}

impl RowIndex {
    const INITIAL_SLOTS: usize = 1024;

    /// The slot `hash` belongs in, or `None` when `eq` matched an entry already indexed there.
    fn vacant_slot(&mut self, hash: u64, eq: impl Fn(u32) -> bool) -> Option<usize> {
        if self.slots.is_empty() {
            self.slots = vec![0; Self::INITIAL_SLOTS];
        }
        let mask = self.slots.len() - 1;
        let mut slot = hash as usize & mask;
        loop {
            match self.slots[slot] {
                0 => return Some(slot),
                v if eq(v - 1) => return None,
                _ => slot = (slot + 1) & mask,
            }
        }
    }

    fn fill(&mut self, slot: usize, entry_idx: u32) {
        self.slots[slot] = entry_idx + 1;
    }

    fn is_full(&self, num_entries: usize) -> bool {
        num_entries * 2 >= self.slots.len()
    }

    fn grow(&mut self, hashes: impl Iterator<Item = u64>) {
        let mask = self.slots.len() * 2 - 1;
        let mut slots = vec![0u32; mask + 1];
        for (entry_idx, hash) in hashes.enumerate() {
            let mut slot = hash as usize & mask;
            while slots[slot] != 0 {
                slot = (slot + 1) & mask;
            }
            slots[slot] = entry_idx as u32 + 1;
        }
        self.slots = slots;
    }

    fn size(&self) -> usize {
        self.slots.capacity() * size_of::<u32>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int32Array, Int32Builder, ListBuilder, StringArray, StringBuilder, StructArray,
    };
    use arrow::datatypes::{Fields, Int32Type};

    /// The collected values of every group, which are never null lists.
    fn int_groups(array: &ArrayRef) -> Vec<Vec<Option<i32>>> {
        let list = array.as_list::<i32>();
        assert_eq!(list.null_count(), 0, "collected lists are never null");
        (0..list.len())
            .map(|i| {
                let values = list.value(i);
                let values = values.as_primitive::<Int32Type>();
                (0..values.len())
                    .map(|j| (!values.is_null(j)).then(|| values.value(j)))
                    .collect()
            })
            .collect()
    }

    fn string_groups(array: &ArrayRef) -> Vec<Vec<String>> {
        let list = array.as_list::<i32>();
        (0..list.len())
            .map(|i| {
                let values = list.value(i);
                let values = values.as_string::<i32>();
                (0..values.len())
                    .map(|j| values.value(j).to_string())
                    .collect()
            })
            .collect()
    }

    fn ints(values: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from(values))
    }

    /// A `List<Int32>` state array, one list per row.
    fn int_lists(rows: Vec<Option<Vec<i32>>>) -> ArrayRef {
        let mut builder = ListBuilder::new(Int32Builder::new())
            .with_field(Field::new_list_field(DataType::Int32, true));
        for row in rows {
            match row {
                Some(values) => {
                    for value in values {
                        builder.values().append_value(value);
                    }
                    builder.append(true);
                }
                None => builder.append(false),
            }
        }
        Arc::new(builder.finish())
    }

    fn collect_list() -> CollectListGroupsAccumulator {
        CollectListGroupsAccumulator::new(DataType::Int32)
    }

    fn collect_set() -> CollectSetGroupsAccumulator {
        CollectSetGroupsAccumulator::try_new(DataType::Int32).unwrap()
    }

    #[test]
    fn collect_list_keeps_input_order_per_group_and_drops_nulls() {
        let mut acc = collect_list();
        acc.update_batch(
            &[ints(vec![Some(1), None, Some(2), Some(3), Some(4)])],
            &[0, 1, 0, 2, 1],
            None,
            3,
        )
        .unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            int_groups(&emitted),
            vec![vec![Some(1), Some(2)], vec![Some(4)], vec![Some(3)],]
        );
    }

    #[test]
    fn collect_list_emits_empty_lists_for_groups_that_collected_nothing() {
        let mut acc = collect_list();
        // Group 0 only ever sees a null, group 2 is never seen at all.
        acc.update_batch(&[ints(vec![None, Some(7)])], &[0, 1], None, 3)
            .unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(int_groups(&emitted), vec![vec![], vec![Some(7)], vec![]]);
    }

    #[test]
    fn collect_list_honours_the_filter() {
        let mut acc = collect_list();
        let filter = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
        acc.update_batch(
            &[ints(vec![Some(1), Some(2), Some(3), Some(4)])],
            &[0, 0, 0, 1],
            Some(&filter),
            2,
        )
        .unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(int_groups(&emitted), vec![vec![Some(1)], vec![Some(4)]]);
    }

    #[test]
    fn collect_list_merges_partial_states() {
        let mut acc = collect_list();
        acc.merge_batch(
            &[int_lists(vec![Some(vec![1, 2]), Some(vec![3])])],
            &[0, 1],
            2,
        )
        .unwrap();
        acc.merge_batch(
            &[int_lists(vec![Some(vec![4]), None, Some(vec![])])],
            &[1, 0, 2],
            3,
        )
        .unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            int_groups(&emitted),
            vec![vec![Some(1), Some(2)], vec![Some(3), Some(4)], vec![]]
        );
    }

    /// `evaluate` picks its gather by average run length: two runs of 32 take the `concat`
    /// path, 64 alternating rows the `interleave` one. Both have to collect the right values.
    #[test]
    fn collect_list_gathers_runs_and_scattered_rows_alike() {
        let values = || ints((0..64).map(Some).collect());

        let mut clustered = collect_list();
        let groups: Vec<usize> = (0..64).map(|i| i / 32).collect();
        clustered
            .update_batch(&[values()], &groups, None, 2)
            .unwrap();
        assert_eq!(
            int_groups(&clustered.evaluate(EmitTo::All).unwrap()),
            vec![
                (0..32).map(Some).collect::<Vec<_>>(),
                (32..64).map(Some).collect::<Vec<_>>(),
            ]
        );

        let mut scattered = collect_list();
        let groups: Vec<usize> = (0..64).map(|i| i % 2).collect();
        scattered
            .update_batch(&[values()], &groups, None, 2)
            .unwrap();
        assert_eq!(
            int_groups(&scattered.evaluate(EmitTo::All).unwrap()),
            vec![
                (0..64).step_by(2).map(Some).collect::<Vec<_>>(),
                (1..64).step_by(2).map(Some).collect::<Vec<_>>(),
            ]
        );
    }

    #[test]
    fn collect_list_emit_first_keeps_the_remaining_groups() {
        let mut acc = collect_list();
        acc.update_batch(
            &[ints(vec![Some(1), Some(2), Some(3), Some(4)])],
            &[0, 1, 2, 0],
            None,
            3,
        )
        .unwrap();
        let emitted = acc.evaluate(EmitTo::First(2)).unwrap();
        assert_eq!(
            int_groups(&emitted),
            vec![vec![Some(1), Some(4)], vec![Some(2)]]
        );

        // Group 2 is renumbered to 0, and a further batch lands on the renumbered groups.
        acc.update_batch(&[ints(vec![Some(5), Some(6)])], &[0, 1], None, 2)
            .unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            int_groups(&emitted),
            vec![vec![Some(3), Some(5)], vec![Some(6)]]
        );
    }

    #[test]
    fn collect_list_converts_rows_to_single_element_states() {
        let acc = collect_list();
        let filter = BooleanArray::from(vec![true, true, false]);
        let states = acc
            .convert_to_state(&[ints(vec![Some(1), None, Some(3)])], Some(&filter))
            .unwrap();
        let mut merged = collect_list();
        merged.merge_batch(&states, &[0, 0, 0], 1).unwrap();
        assert_eq!(
            int_groups(&merged.evaluate(EmitTo::All).unwrap()),
            vec![vec![Some(1)]]
        );
    }

    #[test]
    fn collect_set_deduplicates_per_group_and_drops_nulls() {
        let mut acc = collect_set();
        acc.update_batch(
            &[ints(vec![Some(1), Some(1), None, Some(2), Some(1)])],
            &[0, 0, 0, 0, 1],
            None,
            2,
        )
        .unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        // The same value in a different group is a different entry.
        assert_eq!(
            int_groups(&emitted),
            vec![vec![Some(1), Some(2)], vec![Some(1)]]
        );
    }

    #[test]
    fn collect_set_emits_empty_lists_for_groups_that_collected_nothing() {
        let mut acc = collect_set();
        acc.update_batch(&[ints(vec![None, Some(7)])], &[0, 1], None, 3)
            .unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(int_groups(&emitted), vec![vec![], vec![Some(7)], vec![]]);
    }

    #[test]
    fn collect_set_deduplicates_across_merged_states() {
        let mut acc = collect_set();
        acc.merge_batch(
            &[int_lists(vec![Some(vec![1, 2]), Some(vec![3])])],
            &[0, 1],
            2,
        )
        .unwrap();
        acc.merge_batch(&[int_lists(vec![Some(vec![2, 4]), None])], &[0, 1], 2)
            .unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            int_groups(&emitted),
            vec![vec![Some(1), Some(2), Some(4)], vec![Some(3)]]
        );
    }

    #[test]
    fn collect_set_emit_first_keeps_the_remaining_groups() {
        let mut acc = collect_set();
        acc.update_batch(
            &[ints(vec![Some(1), Some(2), Some(3), Some(1)])],
            &[0, 1, 2, 0],
            None,
            3,
        )
        .unwrap();
        let emitted = acc.evaluate(EmitTo::First(2)).unwrap();
        assert_eq!(int_groups(&emitted), vec![vec![Some(1)], vec![Some(2)]]);

        acc.update_batch(&[ints(vec![Some(3), Some(5)])], &[0, 0], None, 1)
            .unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        // Group 2 became group 0 and already held 3, so the repeat is dropped.
        assert_eq!(int_groups(&emitted), vec![vec![Some(3), Some(5)]]);
    }

    #[test]
    fn collect_set_grows_its_index_past_the_initial_capacity() {
        let mut acc = collect_set();
        let values: Vec<Option<i32>> = (0..4096).map(Some).collect();
        let groups: Vec<usize> = (0..4096).map(|i| i % 4).collect();
        acc.update_batch(&[ints(values)], &groups, None, 4).unwrap();
        // Every value is distinct, so nothing is deduplicated away.
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        let collected = int_groups(&emitted);
        assert_eq!(collected.iter().map(Vec::len).sum::<usize>(), 4096);
        assert_eq!(collected[0][..2], [Some(0), Some(4)]);
    }

    #[test]
    fn collect_set_deduplicates_strings() {
        let mut acc = CollectSetGroupsAccumulator::try_new(DataType::Utf8).unwrap();
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("a"),
            None,
            Some("b"),
        ]));
        acc.update_batch(&[values], &[0, 0, 0, 0, 1], None, 2)
            .unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            string_groups(&emitted),
            vec![
                vec!["a".to_string(), "b".to_string()],
                vec!["b".to_string()]
            ]
        );
    }

    #[test]
    fn collect_set_deduplicates_structs() {
        let fields: Fields = vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Int32, true)),
        ]
        .into();
        let mut names = StringBuilder::new();
        let mut numbers = Int32Builder::new();
        for (name, number) in [("x", 1), ("x", 1), ("x", 2)] {
            names.append_value(name);
            numbers.append_value(number);
        }
        let values: ArrayRef = Arc::new(StructArray::new(
            fields.clone(),
            vec![Arc::new(names.finish()), Arc::new(numbers.finish())],
            None,
        ));

        let mut acc =
            CollectSetGroupsAccumulator::try_new(DataType::Struct(fields.clone())).unwrap();
        acc.update_batch(&[values], &[0, 0, 0], None, 1).unwrap();
        let emitted = acc.evaluate(EmitTo::All).unwrap();
        let list = emitted.as_list::<i32>();
        assert_eq!(list.len(), 1);
        // `{x, 1}` collapses into one entry, `{x, 2}` stays separate.
        assert_eq!(list.value(0).len(), 2);
        assert_eq!(list.value(0).data_type(), &DataType::Struct(fields));
    }

    /// The two UDFs forward the rest of `AggregateUDFImpl` to `datafusion_spark`, and the
    /// planner and the `GroupsAccumulator` both depend on what those forwards report.
    #[test]
    fn collect_functions_forward_their_shape_to_datafusion_spark() {
        for udf in [
            Box::new(CometCollectList::new()) as Box<dyn AggregateUDFImpl>,
            Box::new(CometCollectSet::new()),
        ] {
            let name = udf.name().to_string();
            assert!(name == "collect_list" || name == "collect_set", "{name}");
            assert_eq!(
                udf.return_type(&[DataType::Int32]).unwrap(),
                DataType::List(list_field(&DataType::Int32))
            );
        }
    }
}
