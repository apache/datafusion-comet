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

//! Which columns of a native Iceberg data file are dictionary-encoded.
//!
//! iceberg-java writes Parquet through parquet-mr, which judges dictionary encoding on the first
//! data page of every column chunk (`FallbackValuesWriter`): unless the page's dictionary-encoded
//! values plus its dictionary come out smaller than the page's plain encoding, the chunk is written
//! plain from its first page on and has no dictionary page. parquet-rs never makes that
//! comparison. It keeps a column dictionary-encoded until the dictionary reaches
//! `dictionary_page_size_limit`, then falls back to plain for the rest of the chunk but still
//! writes the dictionary page. A high-cardinality column therefore carries a dictionary page of up
//! to `write.parquet.dict-size-bytes` that every selective read of the chunk has to fetch, where
//! iceberg-java writes none (apache/datafusion-comet#6114).
//!
//! parquet-rs cannot change a column's encoding once a file is open, so the native writer takes
//! parquet-mr's decision before opening one: [`DictionaryChooser`] replays parquet-mr's size
//! accounting over the rows each column's first data page would hold, and turns dictionary encoding
//! off for the columns parquet-mr would write plain. The accounting follows `FallbackValuesWriter`,
//! `DictionaryValuesWriter` and `RunLengthBitPackingHybridEncoder` in parquet-column, and the page
//! boundary follows `ColumnWriteStoreBase.sizeCheck`.

use arrow::array::{downcast_primitive_array, Array, AsArray, OffsetSizeTrait, RecordBatch};
use arrow::datatypes::{DataType, Fields, Schema as ArrowSchema, ToByteSlice};
use datafusion::common::HashSet;
use datafusion::error::{DataFusionError, Result as DFResult};
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::Type as PhysicalType;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

/// The row count of parquet-mr's first page size check. Iceberg passes
/// `write.parquet.row-group-check-min-record-count` to parquet-mr as the minimum row count between
/// page size checks, and the native writer declines tables that change it from this default, so no
/// first page ends before this row even when `write.parquet.page-row-limit` is lower.
const FIRST_PAGE_SIZE_CHECK_ROWS: usize = 100;

/// parquet-mr ends a page once less than this share of the page size is left
/// (`ColumnWriteStoreBase.THRESHOLD_TOLERANCE_RATIO`).
const PAGE_SIZE_TOLERANCE_RATIO: f32 = 0.1;

/// Decides, from the rows a partition's data starts with, which of its columns keep dictionary
/// encoding.
///
/// The choice is made once per partition and applies to every file and row group the task writes
/// for it, where parquet-mr decides again for every column chunk. A column's first data page is
/// the table's `write.parquet.page-row-limit` rows unless the page size ends it sooner. parquet-mr
/// only notices a full page at its periodic size checks, whose timing depends on every column, so
/// where the page size decides, this ends the page at the first row past parquet-mr's threshold and
/// can judge a column on somewhat fewer rows than parquet-mr would.
pub(super) struct DictionaryChooser {
    /// The table's writer properties, which every choice starts from.
    base: WriterProperties,
    /// The fields every batch is written with.
    fields: Fields,
    /// The leaf columns there is a choice to make for, in Parquet schema order. parquet-rs never
    /// dictionary-encodes booleans, nor fixed-length byte arrays under Parquet format v1, and a
    /// column whose dictionary is already off stays off, so none of those is here and their values
    /// are never read.
    candidates: Vec<Candidate>,
    /// Rows the first data page of every column holds at most.
    page_rows: usize,
    /// Plain-encoded bytes at which parquet-mr's size check ends a page.
    page_bytes: u64,
    /// Memory at which a partition stops holding rows back, however few it has: the row group
    /// size, since parquet-mr's first page never outlasts its first row group.
    max_held_bytes: Option<usize>,
}

/// A leaf column whose dictionary encoding is up for choosing.
struct Candidate {
    path: ColumnPath,
    /// Where the column's values are in a batch.
    leaf: Leaf,
    /// Plain-encoded width of one value, or `None` for a byte array, which parquet-mr accounts as
    /// a 4-byte length plus the bytes.
    width: Option<u64>,
    /// `write.parquet.dict-size-bytes`. parquet-mr abandons a dictionary as soon as it grows past
    /// this, which on a first page leaves no dictionary page at all.
    dictionary_limit: u64,
}

/// The way from a batch down to one leaf column: a top-level column, then a step into each nested
/// level.
#[derive(Clone)]
struct Leaf {
    column: usize,
    steps: Vec<Step>,
}

#[derive(Clone, Copy)]
enum Step {
    /// Into a struct's field.
    Field(usize),
    /// Into a list's elements.
    Element,
    /// Into a map's keys.
    Key,
    /// Into a map's values.
    Value,
}

impl DictionaryChooser {
    /// `schema` is the Arrow schema every batch is written with, which fixes the Parquet leaf
    /// columns the same way the parquet writer derives them.
    pub(super) fn try_new(base: WriterProperties, schema: &ArrowSchema) -> DFResult<Self> {
        let parquet_schema = ArrowSchemaConverter::new()
            .with_coerce_types(base.coerce_types())
            .convert(schema)
            .map_err(DataFusionError::from)?;
        let mut leaves = Vec::new();
        for (column, field) in schema.fields().iter().enumerate() {
            let mut leaf = Leaf {
                column,
                steps: Vec::new(),
            };
            collect_leaves(field.data_type(), &mut leaf, &mut leaves);
        }
        // Both lists come from the same depth-first walk of the schema, so they line up one to one.
        // Should they ever not, there is no telling which values belong to which column, and every
        // column is left as configured.
        let candidates = if leaves.len() == parquet_schema.num_columns() {
            parquet_schema
                .columns()
                .iter()
                .zip(leaves)
                .filter_map(|(descr, leaf)| {
                    let path = descr.path();
                    let width = match descr.physical_type() {
                        PhysicalType::INT32 | PhysicalType::FLOAT => Some(4),
                        PhysicalType::INT64 | PhysicalType::DOUBLE => Some(8),
                        PhysicalType::BYTE_ARRAY => None,
                        // INT96 is not an Iceberg type.
                        PhysicalType::BOOLEAN
                        | PhysicalType::FIXED_LEN_BYTE_ARRAY
                        | PhysicalType::INT96 => return None,
                    };
                    base.dictionary_enabled(path).then(|| Candidate {
                        path: path.clone(),
                        leaf,
                        width,
                        dictionary_limit: base.column_dictionary_page_size_limit(path) as u64,
                    })
                })
                .collect()
        } else {
            Vec::new()
        };
        let page_size = base.data_page_size_limit();
        let tolerance = (page_size as f32 * PAGE_SIZE_TOLERANCE_RATIO) as usize;
        Ok(Self {
            fields: schema.fields().clone(),
            candidates,
            page_rows: base
                .data_page_row_count_limit()
                .max(FIRST_PAGE_SIZE_CHECK_ROWS),
            page_bytes: page_size.saturating_sub(tolerance) as u64,
            max_held_bytes: base.max_row_group_bytes(),
            base,
        })
    }

    /// The table's writer properties, before any choice.
    pub(super) fn base(&self) -> &WriterProperties {
        &self.base
    }

    /// Whether a partition that has shown `rows` rows, held in `bytes` of memory, should hold back
    /// more before choosing.
    pub(super) fn wants_more(&self, rows: usize, bytes: usize) -> bool {
        !self.candidates.is_empty() && rows < self.page_rows && self.may_hold(bytes)
    }

    /// Whether rows taking `bytes` of memory may stay held back.
    pub(super) fn may_hold(&self, bytes: usize) -> bool {
        self.max_held_bytes.is_none_or(|max| bytes < max)
    }

    /// The writer properties for a partition whose rows start with `sample`: the table's, with
    /// dictionary encoding turned off for every column parquet-mr would write plain.
    ///
    /// Columns are judged one at a time, and each is read only as far as its first page reaches,
    /// so what this keeps at any moment is one column's dictionary, which cannot outgrow
    /// `write.parquet.dict-size-bytes` by more than a value.
    pub(super) fn choose(&self, sample: &[RecordBatch]) -> WriterProperties {
        if sample
            .iter()
            .any(|batch| batch.schema_ref().fields() != &self.fields)
        {
            // There is no telling which values belong to which column.
            return self.base.clone();
        }
        let mut plain = Vec::new();
        for candidate in &self.candidates {
            let mut page = FirstPage::new(candidate, self.page_bytes);
            for batch in sample {
                if page.is_closed() || page.rows == self.page_rows {
                    break;
                }
                let rows = batch.num_rows().min(self.page_rows - page.rows);
                let top: Entries = Box::new((0..rows).map(|row| (row as u32, row)));
                let Some((array, entries)) = candidate.leaf.entries(batch, top) else {
                    // Cannot happen once the fields match, but a guess is worse than no choice.
                    return self.base.clone();
                };
                page.extend(array, entries, rows);
            }
            if page.rows > 0 && !page.keeps_dictionary() {
                plain.push(&candidate.path);
            }
        }
        if plain.is_empty() {
            return self.base.clone();
        }
        plain
            .into_iter()
            .fold(self.base.clone().into_builder(), |builder, path| {
                builder.set_column_dictionary_enabled(path.clone(), false)
            })
            .build()
    }
}

/// Appends the way to every leaf under a column of `data_type` that `leaf` leads to, in the
/// depth-first order Parquet numbers leaf columns in.
fn collect_leaves(data_type: &DataType, leaf: &mut Leaf, leaves: &mut Vec<Leaf>) {
    match data_type {
        DataType::Struct(fields) => {
            for (field, child) in fields.iter().enumerate() {
                descend(Step::Field(field), child.data_type(), leaf, leaves);
            }
        }
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => {
            descend(Step::Element, element.data_type(), leaf, leaves)
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                descend(Step::Key, fields[0].data_type(), leaf, leaves);
                descend(Step::Value, fields[1].data_type(), leaf, leaves);
            }
            _ => leaves.push(leaf.clone()),
        },
        _ => leaves.push(leaf.clone()),
    }
}

fn descend(step: Step, data_type: &DataType, leaf: &mut Leaf, leaves: &mut Vec<Leaf>) {
    leaf.steps.push(step);
    collect_leaves(data_type, leaf, leaves);
    leaf.steps.pop();
}

/// `(row, index)` pairs, one per entry of an array that the column chunk stores: its row of the
/// batch, and its index into the array.
type Entries<'a> = Box<dyn Iterator<Item = (u32, usize)> + 'a>;

impl Leaf {
    /// The leaf's array in `batch`, with the entries of it that lie under the rows `top` yields and
    /// whose ancestors are all present, in the order the column chunk stores them. Nothing is
    /// expanded ahead: each entry is produced when the page asks for it. `None` when `batch` does
    /// not have the shape the schema promised.
    fn entries<'a>(
        &self,
        batch: &'a RecordBatch,
        top: Entries<'a>,
    ) -> Option<(&'a dyn Array, Entries<'a>)> {
        let mut array = batch.columns().get(self.column)?.as_ref();
        let mut entries = top;
        for step in &self.steps {
            let present = present(array, entries);
            (array, entries) = match (step, array.data_type()) {
                (Step::Field(field), DataType::Struct(_)) => {
                    (array.as_struct().columns().get(*field)?.as_ref(), present)
                }
                (Step::Element, DataType::List(_)) => {
                    let list = array.as_list::<i32>();
                    (
                        list.values().as_ref(),
                        children(present, list.value_offsets()),
                    )
                }
                (Step::Element, DataType::LargeList(_)) => {
                    let list = array.as_list::<i64>();
                    (
                        list.values().as_ref(),
                        children(present, list.value_offsets()),
                    )
                }
                (Step::Element, DataType::FixedSizeList(_, size)) => {
                    let size = *size as usize;
                    let elements: Entries<'a> = Box::new(present.flat_map(move |(row, index)| {
                        (index * size..(index + 1) * size).map(move |element| (row, element))
                    }));
                    (array.as_fixed_size_list().values().as_ref(), elements)
                }
                (Step::Key, DataType::Map(_, _)) => {
                    let map = array.as_map();
                    (map.keys().as_ref(), children(present, map.value_offsets()))
                }
                (Step::Value, DataType::Map(_, _)) => {
                    let map = array.as_map();
                    (
                        map.values().as_ref(),
                        children(present, map.value_offsets()),
                    )
                }
                _ => return None,
            };
        }
        Some((array, entries))
    }
}

/// The entries whose value in `array` is not null.
fn present<'a>(array: &'a dyn Array, entries: Entries<'a>) -> Entries<'a> {
    match array.nulls() {
        Some(nulls) => Box::new(entries.filter(move |&(_, index)| nulls.is_valid(index))),
        None => entries,
    }
}

/// The entries of the children of every list or map entry in `entries`.
fn children<'a, O: OffsetSizeTrait>(entries: Entries<'a>, offsets: &'a [O]) -> Entries<'a> {
    Box::new(entries.flat_map(move |(row, index)| {
        (offsets[index].as_usize()..offsets[index + 1].as_usize()).map(move |child| (row, child))
    }))
}

type ValueBytes<'a> = Box<dyn Fn(usize) -> &'a [u8] + 'a>;

/// The bytes that tell `array`'s values apart: a fixed-width value's native bytes, or a string's or
/// binary's content. `None` for a type an Iceberg schema cannot produce.
fn value_bytes<'a>(array: &'a dyn Array) -> Option<ValueBytes<'a>> {
    let bytes: ValueBytes<'a> = downcast_primitive_array!(
        array => Box::new(move |i| array.values()[i].to_byte_slice()),
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            Box::new(move |i| array.value(i).as_bytes())
        }
        DataType::LargeUtf8 => {
            let array = array.as_string::<i64>();
            Box::new(move |i| array.value(i).as_bytes())
        }
        DataType::Utf8View => {
            let array = array.as_string_view();
            Box::new(move |i| array.value(i).as_bytes())
        }
        DataType::Binary => {
            let array = array.as_binary::<i32>();
            Box::new(move |i| array.value(i))
        }
        DataType::LargeBinary => {
            let array = array.as_binary::<i64>();
            Box::new(move |i| array.value(i))
        }
        DataType::BinaryView => {
            let array = array.as_binary_view();
            Box::new(move |i| array.value(i))
        }
        _ => return None,
    );
    Some(bytes)
}

/// What parquet-mr's dictionary writer has seen of one column chunk's first data page.
struct FirstPage<'a> {
    width: Option<u64>,
    dictionary_limit: u64,
    page_bytes: u64,
    /// Every value seen so far, once each: the dictionary.
    dictionary: HashSet<&'a [u8]>,
    /// The value before the one being added.
    previous: Option<&'a [u8]>,
    /// The runs the page's dictionary ids would be encoded in.
    ids: HybridRuns,
    plain_bytes: u64,
    dictionary_bytes: u64,
    /// Rows the page has taken so far.
    rows: usize,
    /// The page ended at a size check.
    full: bool,
    /// The dictionary outgrew `dictionary_limit` before the page ended.
    overflowed: bool,
    /// The column's arrays are of a type there is no reading, so it is left as configured.
    unreadable: bool,
}

impl<'a> FirstPage<'a> {
    fn new(candidate: &Candidate, page_bytes: u64) -> Self {
        Self {
            width: candidate.width,
            dictionary_limit: candidate.dictionary_limit,
            page_bytes,
            dictionary: HashSet::default(),
            previous: None,
            ids: HybridRuns::default(),
            plain_bytes: 0,
            dictionary_bytes: 0,
            rows: 0,
            full: false,
            overflowed: false,
            unreadable: false,
        }
    }

    fn is_closed(&self) -> bool {
        self.full || self.overflowed || self.unreadable
    }

    /// Adds `rows` rows of one batch, given as the entries of the leaf's `array` they hold. Stops
    /// taking entries as soon as the page ends.
    fn extend(
        &mut self,
        array: &'a dyn Array,
        entries: impl Iterator<Item = (u32, usize)>,
        rows: usize,
    ) {
        if self.is_closed() {
            return;
        }
        let Some(value) = value_bytes(array) else {
            self.unreadable = true;
            return;
        };
        let nulls = array.nulls();
        let mut entries = entries.peekable();
        for row in 0..rows as u32 {
            while let Some((_, index)) = entries.next_if(|&(entry_row, _)| entry_row == row) {
                if nulls.is_none_or(|nulls| nulls.is_valid(index)) {
                    self.add(value(index));
                    if self.overflowed {
                        return;
                    }
                }
            }
            self.rows += 1;
            if self.rows >= FIRST_PAGE_SIZE_CHECK_ROWS && self.plain_bytes >= self.page_bytes {
                self.full = true;
                return;
            }
        }
    }

    /// `DictionaryValuesWriter.write*` and `FallbackValuesWriter`'s raw size, for one value.
    fn add(&mut self, value: &'a [u8]) {
        let size = self.width.unwrap_or(4 + value.len() as u64);
        self.plain_bytes += size;
        if self.dictionary.insert(value) {
            self.dictionary_bytes += size;
        }
        // Ids are handed out in order of first appearance, so an id repeats the one before it
        // exactly when its value does.
        self.ids.write(self.previous == Some(value));
        self.previous = Some(value);
        // `DictionaryValuesWriter.shouldFallBack`, checked after every value.
        if self.dictionary_bytes > self.dictionary_limit {
            self.overflowed = true;
        }
    }

    /// `FallbackValuesWriter.getBytes` on the first page: the dictionary stays only if the page's
    /// dictionary-encoded bytes (a bit-width byte, then the ids) plus the dictionary are fewer than
    /// the page's plain bytes (`DictionaryValuesWriter.isCompressionSatisfying`). A page of nulls
    /// only is plain, since nothing is fewer than zero bytes.
    fn keeps_dictionary(&self) -> bool {
        if self.unreadable {
            return true;
        }
        if self.overflowed {
            return false;
        }
        let max_id = self.dictionary.len().saturating_sub(1) as u32;
        let bit_width = u32::BITS - max_id.leading_zeros();
        1 + self.ids.len(bit_width) + self.dictionary_bytes < self.plain_bytes
    }
}

/// `RunLengthBitPackingHybridEncoder`'s run-splitting state machine, counting what it would write
/// instead of writing it. A value repeated 8 or more times from the start of a group of 8 becomes
/// an RLE run; everything else is bit-packed 8 values at a time, up to 63 groups per run header.
///
/// Where the encoder cuts runs depends only on whether each id repeats the one before it, so that
/// is all this is told. The bit width, which the dictionary's final size decides, only comes in
/// when the length is read off.
#[derive(Clone, Default)]
struct HybridRuns {
    repeat_count: u64,
    buffered: u32,
    /// Groups in the bit-packed run that is open.
    groups: u32,
    bit_packed_run_open: bool,
    /// One header byte each.
    bit_packed_runs: u64,
    /// `bit_width` bytes each: eight values of `bit_width` bits.
    bit_packed_groups: u64,
    /// The value, padded to whole bytes, each.
    rle_runs: u64,
    /// The varint headers of the RLE runs, which hold the run lengths.
    rle_header_bytes: u64,
}

impl HybridRuns {
    /// `writeInt` for an id that does or does not repeat the one before it. The encoder starts from
    /// a previous value of 0, which is always the first id, and either way a first value starts a
    /// run of one.
    fn write(&mut self, repeat: bool) {
        if repeat {
            self.repeat_count += 1;
            if self.repeat_count >= 8 {
                return;
            }
        } else {
            if self.repeat_count >= 8 {
                self.write_rle_run();
            }
            self.repeat_count = 1;
        }
        self.buffered += 1;
        if self.buffered == 8 {
            self.write_bit_packed_group();
        }
    }

    fn write_bit_packed_group(&mut self) {
        if self.groups >= 63 {
            self.end_bit_packed_run();
        }
        if !self.bit_packed_run_open {
            self.bit_packed_runs += 1;
            self.bit_packed_run_open = true;
        }
        self.bit_packed_groups += 1;
        self.buffered = 0;
        self.repeat_count = 0;
        self.groups += 1;
    }

    fn end_bit_packed_run(&mut self) {
        self.bit_packed_run_open = false;
        self.groups = 0;
    }

    fn write_rle_run(&mut self) {
        self.end_bit_packed_run();
        self.rle_runs += 1;
        self.rle_header_bytes += varint_len(self.repeat_count << 1);
        self.repeat_count = 0;
        self.buffered = 0;
    }

    /// The bytes `toBytes()` would return at `bit_width`, including its flush of what is still
    /// buffered, whose last group is padded to 8 values.
    fn len(&self, bit_width: u32) -> u64 {
        let mut finished = self.clone();
        if finished.repeat_count >= 8 {
            finished.write_rle_run();
        } else if finished.buffered > 0 {
            finished.write_bit_packed_group();
        }
        let bit_width = bit_width as u64;
        finished.bit_packed_runs
            + finished.bit_packed_groups * bit_width
            + finished.rle_header_bytes
            + finished.rle_runs * bit_width.div_ceil(8)
    }
}

fn varint_len(value: u64) -> u64 {
    (u64::BITS - value.leading_zeros()).div_ceil(7).max(1) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::Arc;

    use arrow::array::builder::{
        Int32Builder, Int64Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
    };
    use arrow::array::{
        ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
        Float64Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Fields};

    /// splitmix64, the generator the parquet-mr ground truth below was recorded with.
    fn mix(x: u64) -> u64 {
        let mut x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    const CARDINALITIES: [u64; 14] = [
        1000, 2780, 5000, 12000, 13000, 14000, 14500, 15000, 15500, 16000, 17000, 20000, 30000,
        38000,
    ];
    const CYCLE_UNIQUE: [u64; 9] = [60, 65, 68, 69, 70, 71, 72, 75, 80];
    const WIDE_CARDINALITIES: [u64; 4] = [3000, 5000, 7000, 9000];

    /// Rows `rows` of a table built to sit on both sides of parquet-mr's cut-off: random columns
    /// of rising cardinality in each physical type, runs, cycles that mix repeats with unique
    /// values, nulls, wide strings whose first page the page size ends, and nested leaves.
    fn corpus(rows: std::ops::Range<u64>) -> RecordBatch {
        let ints = |f: &dyn Fn(u64) -> i32| -> ArrayRef {
            Arc::new(Int32Array::from_iter_values(rows.clone().map(f)))
        };
        let longs = |f: &dyn Fn(u64) -> i64| -> ArrayRef {
            Arc::new(Int64Array::from_iter_values(rows.clone().map(f)))
        };
        let strings = |f: &dyn Fn(u64) -> String| -> ArrayRef {
            Arc::new(StringArray::from_iter_values(rows.clone().map(f)))
        };
        let mut columns: Vec<(String, ArrayRef)> = vec![
            ("unique_long".into(), longs(&|i| i as i64)),
            ("mod10_int".into(), ints(&|i| (i % 10) as i32)),
        ];
        for c in CARDINALITIES {
            columns.push((format!("rand_int_{c}"), ints(&|i| (mix(i) % c) as i32)));
        }
        for c in CARDINALITIES {
            columns.push((
                format!("rand_long_{c}"),
                longs(&|i| (mix(i + 7) % c) as i64),
            ));
        }
        for c in CARDINALITIES {
            columns.push((
                format!("rand_str_{c}"),
                strings(&|i| format!("s{}", mix(i + 13) % c)),
            ));
        }
        columns.push((
            "unique_str".into(),
            strings(&|i| format!("{:016x}", mix(i))),
        ));
        columns.push((
            "all_null".into(),
            Arc::new(Int32Array::from_iter(rows.clone().map(|_| None::<i32>))),
        ));
        columns.push((
            "half_null_unique".into(),
            Arc::new(Int64Array::from_iter(
                rows.clone().map(|i| (i % 2 == 0).then_some(i as i64)),
            )),
        ));
        columns.push(("runs_of_3".into(), longs(&|i| (i / 3) as i64)));
        columns.push(("runs_of_10".into(), ints(&|i| (i / 10) as i32)));
        for u in CYCLE_UNIQUE {
            columns.push((
                format!("cycle_unique_{u}"),
                ints(&|i| if i % 100 < u { (1 + i) as i32 } else { 0 }),
            ));
        }
        columns.push((
            "unique_double".into(),
            Arc::new(Float64Array::from_iter_values(
                rows.clone().map(|i| f64::from_bits(mix(i) >> 2)),
            )),
        ));
        columns.push((
            "low_float".into(),
            Arc::new(Float32Array::from_iter_values(
                rows.clone().map(|i| (i % 100) as f32),
            )),
        ));
        columns.push((
            "wide_unique_str".into(),
            strings(&|i| format!("{:016x}", mix(i + 99)).repeat(12)),
        ));
        columns.push((
            "wide_low_str".into(),
            strings(&|i| format!("{}{}", "x".repeat(200), i % 20)),
        ));
        for c in WIDE_CARDINALITIES {
            columns.push((
                format!("wide_rand_str_{c}"),
                strings(&|i| format!("{}{}", "y".repeat(120), mix(i + 31) % c)),
            ));
        }
        columns.push((
            "bool".into(),
            Arc::new(BooleanArray::from_iter(
                rows.clone().map(|i| Some(i % 3 == 0)),
            )),
        ));
        columns.push((
            "dec_low".into(),
            Arc::new(
                Decimal128Array::from_iter_values(rows.clone().map(|i| (i % 1000) as i128))
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ));
        columns.push((
            "date_low".into(),
            Arc::new(Date32Array::from_iter_values(
                rows.clone().map(|i| 18000 + (i % 365) as i32),
            )),
        ));
        let struct_fields = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int32, true),
        ]);
        columns.push((
            "st".into(),
            Arc::new(StructArray::new(
                struct_fields,
                vec![longs(&|i| mix(i + 5) as i64), ints(&|i| (i % 7) as i32)],
                None,
            )),
        ));
        let element = |data_type| Arc::new(Field::new("element", data_type, true));
        let mut list_low =
            ListBuilder::new(Int32Builder::new()).with_field(element(DataType::Int32));
        let mut list_unique =
            ListBuilder::new(Int64Builder::new()).with_field(element(DataType::Int64));
        let mut map = MapBuilder::new(
            Some(MapFieldNames {
                entry: "key_value".into(),
                key: "key".into(),
                value: "value".into(),
            }),
            StringBuilder::new(),
            Int64Builder::new(),
        );
        for i in rows.clone() {
            list_low.values().append_value((i % 5) as i32);
            list_low.values().append_value((i % 3) as i32);
            list_low.values().append_null();
            list_low.append(true);
            for k in 0..3 {
                list_unique.values().append_value((3 * i + k) as i64);
            }
            list_unique.append(true);
            map.keys().append_value(format!("k{}", i % 4));
            map.values().append_value(mix(i + 11) as i64);
            map.keys().append_value(format!("z{}", i % 2));
            map.values().append_value((i % 9) as i64);
            map.append(true).unwrap();
        }
        columns.push(("list_low".into(), Arc::new(list_low.finish())));
        columns.push(("list_unique".into(), Arc::new(list_unique.finish())));
        columns.push(("m".into(), Arc::new(map.finish())));
        RecordBatch::try_from_iter(columns).unwrap()
    }

    /// `rows` rows of [`corpus`] in batches of `batch_rows`.
    fn corpus_batches(rows: u64, batch_rows: u64) -> Vec<RecordBatch> {
        (0..rows)
            .step_by(batch_rows as usize)
            .map(|start| corpus(start..(start + batch_rows).min(rows)))
            .collect()
    }

    fn properties(page_rows: usize, page_size: usize, dictionary_limit: usize) -> WriterProperties {
        WriterProperties::builder()
            .set_data_page_row_count_limit(page_rows)
            .set_data_page_size_limit(page_size)
            .set_dictionary_page_size_limit(dictionary_limit)
            .set_max_row_group_bytes(Some(128 * 1024 * 1024))
            .set_max_row_group_row_count(None)
            .build()
    }

    /// The leaf columns that keep dictionary encoding under the properties chosen for `sample`,
    /// leaving out the types parquet-rs never dictionary-encodes.
    fn dictionary_columns(properties: WriterProperties, sample: &[RecordBatch]) -> Vec<String> {
        let schema = sample[0].schema();
        let chosen = DictionaryChooser::try_new(properties, &schema)
            .unwrap()
            .choose(sample);
        ArrowSchemaConverter::new()
            .convert(&schema)
            .unwrap()
            .columns()
            .iter()
            .filter(|descr| {
                !matches!(
                    descr.physical_type(),
                    PhysicalType::BOOLEAN | PhysicalType::FIXED_LEN_BYTE_ARRAY
                ) && chosen.dictionary_enabled(descr.path())
            })
            .map(|descr| descr.path().string())
            .collect()
    }

    // The expected columns below are parquet-mr's own answers. They were recorded by writing the
    // same rows through iceberg-java's `Parquet.write` with `GenericParquetWriter`, with the same
    // `write.parquet.page-row-limit`, `page-size-bytes` and `dict-size-bytes`, and listing the
    // column chunks of the first row group whose encoding stats include a dictionary page. Iceberg
    // 1.5.2, 1.8.1, 1.10.0 and 1.11.0 (parquet-mr 1.13.1, 1.15.0, 1.16.0 and 1.17.1) all agree.

    #[test]
    fn matches_parquet_mr_at_iceberg_defaults() {
        let sample = corpus_batches(24_576, 8192);
        assert_eq!(
            dictionary_columns(properties(20_000, 1024 * 1024, 2 * 1024 * 1024), &sample),
            [
                "mod10_int",
                "rand_int_1000",
                "rand_int_2780",
                "rand_int_5000",
                "rand_int_12000",
                "rand_int_13000",
                "rand_int_14000",
                "rand_int_14500",
                "rand_int_15000",
                "rand_long_1000",
                "rand_long_2780",
                "rand_long_5000",
                "rand_long_12000",
                "rand_long_13000",
                "rand_long_14000",
                "rand_long_14500",
                "rand_long_15000",
                "rand_long_15500",
                "rand_long_16000",
                "rand_long_17000",
                "rand_long_20000",
                "rand_long_30000",
                "rand_str_1000",
                "rand_str_2780",
                "rand_str_5000",
                "rand_str_12000",
                "rand_str_13000",
                "rand_str_14000",
                "rand_str_14500",
                "rand_str_15000",
                "rand_str_15500",
                "rand_str_16000",
                "rand_str_17000",
                "rand_str_20000",
                "rand_str_30000",
                "rand_str_38000",
                "runs_of_3",
                "runs_of_10",
                "cycle_unique_60",
                "cycle_unique_65",
                "low_float",
                "wide_low_str",
                "wide_rand_str_3000",
                "wide_rand_str_5000",
                "wide_rand_str_7000",
                "wide_rand_str_9000",
                "dec_low",
                "date_low",
                "st.b",
                "list_low.list.element",
                "m.key_value.key",
                "m.key_value.value",
            ]
        );
    }

    /// Pages that end at 16 KiB, before the 2000-row limit, for all but the narrowest columns: the
    /// settings of `CometIcebergNativeSuite`'s page skipping test.
    #[test]
    fn matches_parquet_mr_when_the_page_size_ends_the_first_page() {
        let sample = corpus_batches(8192, 8192);
        assert_eq!(
            dictionary_columns(properties(2000, 16 * 1024, 2 * 1024 * 1024), &sample),
            [
                "mod10_int",
                "rand_int_1000",
                "rand_long_1000",
                "rand_long_2780",
                "rand_str_1000",
                "rand_str_2780",
                "rand_str_5000",
                "runs_of_3",
                "runs_of_10",
                "cycle_unique_60",
                "cycle_unique_65",
                "cycle_unique_68",
                "cycle_unique_69",
                "cycle_unique_70",
                "cycle_unique_71",
                "cycle_unique_72",
                "low_float",
                "wide_low_str",
                "wide_rand_str_3000",
                "wide_rand_str_5000",
                "wide_rand_str_7000",
                "wide_rand_str_9000",
                "dec_low",
                "date_low",
                "st.b",
                "list_low.list.element",
                "m.key_value.key",
                "m.key_value.value",
            ]
        );
    }

    /// parquet-mr does not check page sizes before row 100, so a lower row limit still gives
    /// 100-row first pages.
    #[test]
    fn matches_parquet_mr_when_the_row_limit_is_below_the_first_size_check() {
        let sample = corpus_batches(1000, 1000);
        assert_eq!(
            dictionary_columns(properties(50, 1024 * 1024, 2 * 1024 * 1024), &sample),
            [
                "mod10_int",
                "runs_of_3",
                "runs_of_10",
                "cycle_unique_60",
                "cycle_unique_65",
                "cycle_unique_68",
                "cycle_unique_69",
                "cycle_unique_70",
                "cycle_unique_71",
                "cycle_unique_72",
                "cycle_unique_75",
                "cycle_unique_80",
                "wide_low_str",
                "wide_rand_str_3000",
                "wide_rand_str_5000",
                "wide_rand_str_7000",
                "wide_rand_str_9000",
                "st.b",
                "list_low.list.element",
                "m.key_value.key",
                "m.key_value.value",
            ]
        );
    }

    /// A partition with fewer rows than a page is judged on all of them, as parquet-mr judges a
    /// column chunk that ends within its first page.
    #[test]
    fn matches_parquet_mr_when_the_rows_end_within_the_first_page() {
        let sample = corpus_batches(5000, 2048);
        assert_eq!(
            dictionary_columns(properties(20_000, 1024 * 1024, 2 * 1024 * 1024), &sample),
            [
                "mod10_int",
                "rand_int_1000",
                "rand_int_2780",
                "rand_long_1000",
                "rand_long_2780",
                "rand_long_5000",
                "rand_str_1000",
                "rand_str_2780",
                "rand_str_5000",
                "rand_str_13000",
                "runs_of_3",
                "runs_of_10",
                "cycle_unique_60",
                "cycle_unique_65",
                "cycle_unique_68",
                "cycle_unique_69",
                "cycle_unique_70",
                "cycle_unique_71",
                "low_float",
                "wide_low_str",
                "wide_rand_str_3000",
                "wide_rand_str_5000",
                "wide_rand_str_7000",
                "wide_rand_str_9000",
                "dec_low",
                "date_low",
                "st.b",
                "list_low.list.element",
                "m.key_value.key",
                "m.key_value.value",
            ]
        );
    }

    /// A dictionary that outgrows `write.parquet.dict-size-bytes` on the first page is dropped,
    /// however much it would have saved.
    #[test]
    fn matches_parquet_mr_when_the_dictionary_outgrows_its_limit() {
        let sample = corpus_batches(24_576, 8192);
        assert_eq!(
            dictionary_columns(properties(20_000, 1024 * 1024, 4096), &sample),
            [
                "mod10_int",
                "rand_int_1000",
                "low_float",
                "date_low",
                "st.b",
                "list_low.list.element",
                "m.key_value.key",
            ]
        );
    }

    /// Where a partition's rows were cut into batches does not change its choice.
    #[test]
    fn batch_boundaries_do_not_change_the_choice() {
        let properties = || properties(20_000, 1024 * 1024, 2 * 1024 * 1024);
        let whole = dictionary_columns(properties(), &corpus_batches(20_000, 20_000));
        for batch_rows in [777, 1000, 8192] {
            assert_eq!(
                dictionary_columns(properties(), &corpus_batches(20_000, batch_rows)),
                whole,
                "batches of {batch_rows} rows"
            );
        }
    }

    #[test]
    fn hybrid_len_matches_parquet_mr() {
        fn sequence(len: usize, value: impl Fn(usize) -> u32) -> Vec<u32> {
            (0..len).map(value).collect()
        }
        // What `RunLengthBitPackingHybridEncoder.toBytes()` returned for the same values, at the
        // bit width of their maximum.
        let cases: Vec<(&str, Vec<u32>, u64)> = vec![
            ("empty", vec![], 0),
            ("one zero", vec![0], 1),
            ("20000 zeros", vec![0; 20_000], 3),
            ("ascending", sequence(20_000, |i| i as u32), 37_540),
            ("seven ones", vec![1; 7], 2),
            ("eight ones", vec![1; 8], 2),
            ("nine ones", vec![1; 9], 2),
            ("runs of 3", sequence(20_000, |i| (i / 3) as u32), 32_540),
            ("runs of 8", sequence(20_000, |i| (i / 8) as u32), 7_500),
            ("runs of 10", sequence(20_000, |i| (i / 10) as u32), 6_000),
            (
                "runs of 11 starting mid-group",
                sequence(20_000, |i| ((i + 3) / 11) as u32),
                5_466,
            ),
            (
                "random under 5000",
                sequence(20_000, |i| (mix(i as u64) % 5000) as u32),
                32_540,
            ),
            (
                "cycles of 70 unique and 30 repeats",
                sequence(20_000, |i| if i % 100 < 70 { 1 + i as u32 } else { 0 }),
                27_800,
            ),
            (
                "runs of 9 broken by one value",
                sequence(1000, |i| if i % 10 < 9 { 5 } else { i as u32 }),
                1_100,
            ),
            (
                "scattered repeats",
                sequence(5000, |i| {
                    if mix(i as u64).is_multiple_of(3) {
                        i as u32
                    } else {
                        7
                    }
                }),
                7_794,
            ),
            (
                "504 ascending, 63 groups in one run",
                sequence(504, |i| i as u32),
                568,
            ),
            ("505 ascending", sequence(505, |i| i as u32), 578),
            (
                "512 ascending, 64 groups in two runs",
                sequence(512, |i| i as u32),
                578,
            ),
            ("513 ascending", sequence(513, |i| i as u32), 652),
            ("600 ascending", sequence(600, |i| i as u32), 752),
        ];
        for (name, values, expected) in cases {
            let mut runs = HybridRuns::default();
            for (i, value) in values.iter().enumerate() {
                runs.write(i > 0 && values[i - 1] == *value);
            }
            let max = values.iter().copied().max().unwrap_or(0);
            assert_eq!(
                runs.len(u32::BITS - max.leading_zeros()),
                expected,
                "{name}"
            );
        }
    }

    /// The dictionary has to come out strictly smaller. Here the ids (a bit-width byte and one
    /// bit-packed group, 5 bytes) plus the dictionary (7 values, 35 bytes) exactly match the plain
    /// page (8 values, 40 bytes), and parquet-mr writes the column plain.
    #[test]
    fn a_dictionary_that_only_breaks_even_is_dropped() {
        let values = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "a"]);
        let batch = RecordBatch::try_from_iter([("s", Arc::new(values) as ArrayRef)]).unwrap();
        assert!(dictionary_columns(properties(20_000, 1024 * 1024, 1024), &[batch]).is_empty());
    }

    #[test]
    fn wants_rows_until_a_page_or_a_row_group_is_held() {
        let schema = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
        let chooser =
            DictionaryChooser::try_new(properties(20_000, 1024 * 1024, 1024), &schema).unwrap();
        assert!(chooser.wants_more(19_999, 1024));
        assert!(!chooser.wants_more(20_000, 1024));
        assert!(!chooser.wants_more(10, 128 * 1024 * 1024));
    }

    /// parquet-rs never dictionary-encodes booleans or fixed-length binary, and a column whose
    /// dictionary is already off stays off, so none of them is worth holding rows for.
    #[test]
    fn holds_no_rows_when_there_is_nothing_to_choose() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("flag", DataType::Boolean, false),
            Field::new("uuid", DataType::FixedSizeBinary(16), false),
            Field::new("id", DataType::Int64, false),
        ]));
        let properties = properties(20_000, 1024 * 1024, 1024)
            .into_builder()
            .set_column_dictionary_enabled(ColumnPath::from("id"), false)
            .build();
        let chooser = DictionaryChooser::try_new(properties, &schema).unwrap();
        assert!(!chooser.wants_more(0, 0));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([[0u8; 16], [1u8; 16]].into_iter())
                        .unwrap(),
                ),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();
        assert!(!chooser
            .choose(&[batch])
            .dictionary_enabled(&ColumnPath::from("id")));
    }

    /// Only the columns there is a choice for are read. The boolean lists here, 1000 rows of 8192
    /// entries each in the batch below, are never walked, and a schema with nothing else holds no
    /// rows back and chooses without reading anything.
    #[test]
    fn columns_with_no_choice_are_never_read() {
        let flags = || DataType::List(Arc::new(Field::new("element", DataType::Boolean, true)));
        let schema = ArrowSchema::new(vec![
            Field::new("flags", flags(), true),
            Field::new(
                "s",
                DataType::Struct(Fields::from(vec![
                    Field::new("more_flags", flags(), true),
                    Field::new("n", DataType::Int64, true),
                ])),
                true,
            ),
            Field::new("id", DataType::Int64, false),
        ]);
        let chooser =
            DictionaryChooser::try_new(properties(20_000, 1024 * 1024, 1024), &schema).unwrap();
        let read: Vec<String> = chooser
            .candidates
            .iter()
            .map(|candidate| candidate.path.string())
            .collect();
        assert_eq!(read, ["s.n", "id"]);

        let list = ListArray::new(
            Arc::new(Field::new("element", DataType::Boolean, true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(8192, 1000)),
            Arc::new(BooleanArray::from(vec![true; 8192 * 1000])),
            None,
        );
        let batch = RecordBatch::try_from_iter([("flags", Arc::new(list) as ArrayRef)]).unwrap();
        let chooser =
            DictionaryChooser::try_new(properties(20_000, 1024 * 1024, 1024), batch.schema_ref())
                .unwrap();
        assert!(chooser.candidates.is_empty());
        assert!(!chooser.wants_more(0, 0));
        assert!(chooser
            .choose(&[batch])
            .dictionary_enabled(&ColumnPath::from("flags")));
    }

    /// A column is read only as far as its first page reaches, one entry at a time. Each row here
    /// lists 8192 ints, so the page is full at row 100, parquet-mr's first size check, and the
    /// entries of the other 300 rows are never produced.
    #[test]
    fn a_column_is_read_only_as_far_as_its_first_page() {
        let rows = 400;
        let list = ListArray::new(
            Arc::new(Field::new("element", DataType::Int32, true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(8192, rows)),
            Arc::new(Int32Array::from(vec![0; 8192 * rows])),
            None,
        );
        let batch = RecordBatch::try_from_iter([("l", Arc::new(list) as ArrayRef)]).unwrap();
        let chooser = DictionaryChooser::try_new(
            properties(20_000, 1024 * 1024, 2 * 1024 * 1024),
            batch.schema_ref(),
        )
        .unwrap();
        let candidate = &chooser.candidates[0];
        let rows_walked = Cell::new(0);
        let top: Entries = Box::new((0..rows).map(|row| {
            rows_walked.set(rows_walked.get() + 1);
            (row as u32, row)
        }));
        let (array, entries) = candidate.leaf.entries(&batch, top).unwrap();
        let mut page = FirstPage::new(candidate, chooser.page_bytes);
        page.extend(array, entries, rows);
        assert!(page.full);
        assert_eq!(page.rows, 100);
        // The 100 rows the page took, and the next, whose first entry told it row 100 was done.
        assert_eq!(rows_walked.get(), 101);
    }

    /// A null list can still span child values. They are not part of the column chunk, so they
    /// must not count toward its first page.
    #[test]
    fn values_under_a_null_list_are_not_counted() {
        // Row 0 is a null list spanning 1000 unique values, which would overflow the 1 KiB
        // dictionary if they were counted. Rows 1 to 1000 hold one 7 each, so the page the column
        // chunk actually has is 1000 repeats of one value.
        let mut offsets = vec![0i32, 1000];
        offsets.extend((1..=1000).map(|row| 1000 + row));
        let list = ListArray::new(
            Arc::new(Field::new("element", DataType::Int64, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(Int64Array::from_iter_values((0..2000).map(|i| {
                if i < 1000 {
                    i
                } else {
                    7
                }
            }))),
            Some(NullBuffer::from_iter((0..=1000).map(|row| row != 0))),
        );
        let batch = RecordBatch::try_from_iter([("l", Arc::new(list) as ArrayRef)]).unwrap();
        assert_eq!(
            dictionary_columns(properties(20_000, 1024 * 1024, 1024), &[batch]),
            ["l.list.element"]
        );
    }

    /// A batch whose leaves do not line up with the schema is not guessed at.
    #[test]
    fn leaves_every_column_as_configured_when_the_batch_does_not_match_the_schema() {
        let schema = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
        let chooser =
            DictionaryChooser::try_new(properties(20_000, 1024 * 1024, 1024), &schema).unwrap();
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..100)) as ArrayRef,
            ),
            (
                "extra",
                Arc::new(Int64Array::from_iter_values(0..100)) as ArrayRef,
            ),
        ])
        .unwrap();
        assert!(chooser
            .choose(&[batch])
            .dictionary_enabled(&ColumnPath::from("id")));
    }
}
