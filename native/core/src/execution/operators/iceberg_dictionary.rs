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

use std::hash::{Hash, Hasher};

use arrow::array::{downcast_primitive, Array, ArrayAccessor, AsArray, RecordBatch};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Fields, Schema as ArrowSchema, ToByteSlice,
};
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

/// The most values a column's dictionary makes room for up front. A first page of a nested column
/// can hold many more values than it has distinct ones, so past this the set grows only as
/// distinct values arrive.
const MAX_RESERVED_VALUES: usize = 1 << 16;

/// Decides, from the rows a partition's data starts with, which of its columns keep dictionary
/// encoding.
///
/// The choice is made once per partition and applies to every file and row group the task writes
/// for it, where parquet-mr decides again for every column chunk. A column's first data page is
/// the table's `write.parquet.page-row-limit` rows unless the page size ends it sooner. parquet-mr
/// only notices a full page at its periodic size checks, whose timing depends on every column, so
/// where the page size decides, this ends the page at the first row past parquet-mr's threshold and
/// can judge a column on somewhat fewer rows than parquet-mr would.
pub struct DictionaryChooser {
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
    /// The Arrow type of the column's values.
    data_type: DataType,
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
    pub fn try_new(base: WriterProperties, schema: &ArrowSchema) -> DFResult<Self> {
        let parquet_schema = ArrowSchemaConverter::new()
            .with_coerce_types(base.coerce_types())
            .convert(schema)
            .map_err(DataFusionError::from)?;
        let mut leaves = Vec::with_capacity(parquet_schema.num_columns());
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
                .filter_map(|(descr, (leaf, data_type))| {
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
                        data_type,
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
    pub fn choose(&self, sample: &[RecordBatch]) -> WriterProperties {
        if sample
            .iter()
            .any(|batch| batch.schema_ref().fields() != &self.fields)
        {
            // There is no telling which values belong to which column.
            return self.base.clone();
        }
        let mut plain = Vec::new();
        for candidate in &self.candidates {
            let Some(keeps) = self.judge(candidate, sample) else {
                // Cannot happen once the fields match, but a guess is worse than no choice.
                return self.base.clone();
            };
            if !keeps {
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

    /// Whether `candidate` keeps its dictionary on the first page `sample` gives it, or `None`
    /// when a batch does not have the shape the schema promised. A column of a type there is no
    /// reading is left as configured.
    fn judge(&self, candidate: &Candidate, sample: &[RecordBatch]) -> Option<bool> {
        macro_rules! primitive {
            ($t:ty) => {
                self.judge_primitive::<$t>(candidate, sample)
            };
        }
        downcast_primitive! {
            &candidate.data_type => (primitive),
            DataType::Utf8 => {
                self.judge_values(candidate, sample, |a| a.as_string_opt::<i32>().map(bytes))
            }
            DataType::LargeUtf8 => {
                self.judge_values(candidate, sample, |a| a.as_string_opt::<i64>().map(bytes))
            }
            DataType::Utf8View => {
                self.judge_values(candidate, sample, |a| a.as_string_view_opt().map(bytes))
            }
            DataType::Binary => {
                self.judge_values(candidate, sample, |a| a.as_binary_opt::<i32>().map(bytes))
            }
            DataType::LargeBinary => {
                self.judge_values(candidate, sample, |a| a.as_binary_opt::<i64>().map(bytes))
            }
            DataType::BinaryView => {
                self.judge_values(candidate, sample, |a| a.as_binary_view_opt().map(bytes))
            }
            _ => Some(true),
        }
    }

    /// [`Self::judge`] for a column of `T` values, told apart by their bits where those fit in a
    /// `u128` and by their bytes where not.
    fn judge_primitive<'a, T: ArrowPrimitiveType>(
        &self,
        candidate: &Candidate,
        sample: &'a [RecordBatch],
    ) -> Option<bool> {
        let values = |array: &'a dyn Array| Some(array.as_primitive_opt::<T>()?.values().as_ref());
        // Cannot be missing for a fixed-width type, but a guess is worse than no choice.
        let width = candidate.width?;
        if size_of::<T::Native>() <= size_of::<u64>() {
            self.judge_values(candidate, sample, |array| {
                values(array).map(|values| bits::<_, u64>(values, width))
            })
        } else if size_of::<T::Native>() <= size_of::<u128>() {
            self.judge_values(candidate, sample, |array| {
                values(array).map(|values| bits::<_, u128>(values, width))
            })
        } else {
            self.judge_values(candidate, sample, |array| {
                values(array).map(|values| native_bytes(values, width))
            })
        }
    }

    /// [`Self::judge`] for a column whose values `read` reads from each batch's leaf array: what
    /// tells each value apart, and how many bytes it takes in the plain encoding.
    fn judge_values<'a, K: Copy + Eq + Hash, R: Fn(usize) -> (K, u64)>(
        &self,
        candidate: &Candidate,
        sample: &'a [RecordBatch],
        read: impl Fn(&'a dyn Array) -> Option<R>,
    ) -> Option<bool> {
        // The way to the leaf in each batch the page reaches, with the rows of it the page can
        // take, and how many leaf entries lie under those rows.
        let mut batches = Vec::with_capacity(sample.len());
        let mut entries = 0;
        let mut rows_left = self.page_rows;
        for batch in sample {
            if rows_left == 0 {
                break;
            }
            let rows = batch.num_rows().min(rows_left);
            let path = candidate.leaf.path(batch)?;
            entries += path.entries(rows);
            rows_left -= rows;
            batches.push((path, rows));
        }
        let mut page = FirstPage::<K>::new(candidate, self.page_bytes, entries);
        for (path, rows) in &batches {
            if page.is_closed() {
                break;
            }
            page.extend(path, &read(path.leaf)?, *rows);
        }
        Some(page.rows == 0 || page.keeps_dictionary())
    }
}

/// Appends the way to every leaf under a column of `data_type` that `leaf` leads to, with the
/// leaf's type, in the depth-first order Parquet numbers leaf columns in.
fn collect_leaves(data_type: &DataType, leaf: &mut Leaf, leaves: &mut Vec<(Leaf, DataType)>) {
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
            _ => leaves.push((leaf.clone(), data_type.clone())),
        },
        _ => leaves.push((leaf.clone(), data_type.clone())),
    }
}

fn descend(step: Step, data_type: &DataType, leaf: &mut Leaf, leaves: &mut Vec<(Leaf, DataType)>) {
    leaf.steps.push(step);
    collect_leaves(data_type, leaf, leaves);
    leaf.steps.pop();
}

impl Leaf {
    /// The arrays of `batch` the way to the leaf passes through, or `None` when `batch` does not
    /// have the shape the schema promised.
    fn path<'a>(&self, batch: &'a RecordBatch) -> Option<Path<'a>> {
        let mut array = batch.columns().get(self.column)?.as_ref();
        let mut levels = Vec::with_capacity(self.steps.len());
        for step in &self.steps {
            let (children, next) = match (step, array.data_type()) {
                (Step::Field(field), DataType::Struct(_)) => {
                    (Children::Same, array.as_struct().columns().get(*field)?)
                }
                (Step::Element, DataType::List(_)) => {
                    let list = array.as_list::<i32>();
                    (Children::Offsets(list.value_offsets()), list.values())
                }
                (Step::Element, DataType::LargeList(_)) => {
                    let list = array.as_list::<i64>();
                    (Children::LargeOffsets(list.value_offsets()), list.values())
                }
                (Step::Element, DataType::FixedSizeList(_, size)) => (
                    Children::Fixed(*size as usize),
                    array.as_fixed_size_list().values(),
                ),
                (Step::Key, DataType::Map(_, _)) => {
                    let map = array.as_map();
                    (Children::Offsets(map.value_offsets()), map.keys())
                }
                (Step::Value, DataType::Map(_, _)) => {
                    let map = array.as_map();
                    (Children::Offsets(map.value_offsets()), map.values())
                }
                _ => return None,
            };
            // A null buffer with no nulls in it is as good as none, and a struct without nulls
            // leads every entry straight to its field's, so neither needs a level.
            let nulls = present_nulls(array);
            if nulls.is_some() || !matches!(children, Children::Same) {
                levels.push(Level { nulls, children });
            }
            array = next.as_ref();
        }
        Some(Path {
            levels,
            leaf: array,
        })
    }
}

/// The arrays the way from a batch's column down to one leaf column passes through.
struct Path<'a> {
    /// One for each step, from the batch's column down.
    levels: Vec<Level<'a>>,
    /// The leaf column's array.
    leaf: &'a dyn Array,
}

/// An array on the way to a leaf, and how its entries lead into the next one.
struct Level<'a> {
    /// A null entry has nothing under it in the column chunk, even where a null list spans child
    /// values.
    nulls: Option<&'a NullBuffer>,
    children: Children<'a>,
}

/// The entries of the next array that lie under entry `i` of a level.
#[derive(Clone, Copy)]
enum Children<'a> {
    /// A struct's: entry `i` of its field.
    Same,
    /// A list's or a map's: entries `offsets[i]` up to `offsets[i + 1]` of its elements, keys or
    /// values.
    Offsets(&'a [i32]),
    /// A large list's.
    LargeOffsets(&'a [i64]),
    /// A fixed-size list's: the `size` elements from `i * size`.
    Fixed(usize),
}

impl Children<'_> {
    /// Where the entries under entry `index` start in the next array. Those under the entries
    /// before `index` end there too, so this also holds for one past the last entry.
    fn start(self, index: usize) -> usize {
        match self {
            Children::Same => index,
            Children::Offsets(offsets) => offsets[index] as usize,
            Children::LargeOffsets(offsets) => offsets[index] as usize,
            Children::Fixed(size) => index * size,
        }
    }
}

impl Path<'_> {
    /// How many entries of the leaf array lie under the batch's first `rows` rows, null or not.
    fn entries(&self, rows: usize) -> usize {
        let (start, end) = self.levels.iter().fold((0, rows), |(start, end), level| {
            (level.children.start(start), level.children.start(end))
        });
        end - start
    }

    /// Calls `f` with the leaf index of every entry under the batch's row `row` whose ancestors
    /// are all present, in the order the column chunk stores them, until `f` returns `false`.
    /// Returns whether every entry was visited. Nothing is expanded ahead, so a page that ends
    /// partway through a batch reads no further.
    fn visit(&self, row: usize, f: &mut impl FnMut(usize) -> bool) -> bool {
        match self.levels.split_first() {
            Some((level, below)) => visit(level, below, row, f),
            // A leaf with no levels above it has one entry per row: the row itself.
            None => f(row),
        }
    }
}

/// `array`'s nulls, or `None` when it has none.
fn present_nulls(array: &dyn Array) -> Option<&NullBuffer> {
    array.nulls().filter(|nulls| nulls.null_count() > 0)
}

/// [`Path::visit`] for entry `index` of `level`, with `below` the levels under it.
fn visit(level: &Level, below: &[Level], index: usize, f: &mut impl FnMut(usize) -> bool) -> bool {
    if level.nulls.is_some_and(|nulls| nulls.is_null(index)) {
        return true;
    }
    let mut children = level.children.start(index)..level.children.start(index + 1);
    // The leaf's entries are visited here rather than one call deeper each.
    match below.split_first() {
        Some((next, below)) => children.all(|child| visit(next, below, child, &mut *f)),
        None => children.all(f),
    }
}

/// Reads fixed-width values by their bits, zero-extended to `K`, which is at least as wide. Each
/// takes `width` bytes in the plain encoding.
fn bits<T: ArrowNativeType, K: Bits>(values: &[T], width: u64) -> impl Fn(usize) -> (K, u64) + '_ {
    move |index| (K::of(values[index]), width)
}

/// Reads fixed-width values too wide for [`bits`] by their native bytes. Each takes `width` bytes
/// in the plain encoding.
fn native_bytes<'a, T: ArrowNativeType>(
    values: &'a [T],
    width: u64,
) -> impl Fn(usize) -> (Bytes<'a>, u64) {
    move |index| (Bytes(values[index].to_byte_slice()), width)
}

/// Reads a string or binary array's values by their content. Each takes a 4-byte length and then
/// its bytes in the plain encoding.
fn bytes<'a, A, V>(array: A) -> impl Fn(usize) -> (Bytes<'a>, u64)
where
    A: ArrayAccessor<Item = &'a V>,
    V: AsRef<[u8]> + ?Sized + 'a,
{
    move |index| {
        let value = array.value(index).as_ref();
        (Bytes(value), 4 + value.len() as u64)
    }
}

/// The bits of a fixed-width value, zero-extended to fill this type.
trait Bits: Copy + Eq + Hash {
    fn of<T: ArrowNativeType>(value: T) -> Self;
}

impl Bits for u64 {
    fn of<T: ArrowNativeType>(value: T) -> Self {
        let mut bits = [0; 8];
        let bytes = value.to_byte_slice();
        bits[..bytes.len()].copy_from_slice(bytes);
        u64::from_ne_bytes(bits)
    }
}

impl Bits for u128 {
    fn of<T: ArrowNativeType>(value: T) -> Self {
        let mut bits = [0; 16];
        let bytes = value.to_byte_slice();
        bits[..bytes.len()].copy_from_slice(bytes);
        u128::from_ne_bytes(bits)
    }
}

/// A value's bytes. Two of the same length whose last bytes differ are told apart without
/// comparing the rest, which spares comparing most distinct values with the one before them.
/// Keyed by plain `&[u8]` instead, the `maps` and `nested` shapes of the
/// `iceberg_dictionary_chooser` benchmark take about 25% longer and `primitives` and `arrays`
/// about 12% longer, mostly for the comparison. Only `sorted`, whose values mostly repeat the one
/// before them, is faster that way, by 2%.
#[derive(Clone, Copy)]
struct Bytes<'a>(&'a [u8]);

impl PartialEq for Bytes<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len() && self.0.last() == other.0.last() && self.0 == other.0
    }
}

impl Eq for Bytes<'_> {}

impl Hash for Bytes<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // A value is always hashed alone, so its bytes need no length in front of them.
        state.write(self.0);
    }
}

/// What parquet-mr's dictionary writer has seen of one column chunk's first data page.
struct FirstPage<K> {
    dictionary_limit: u64,
    page_bytes: u64,
    /// Every value seen so far, once each: the dictionary.
    dictionary: HashSet<K>,
    /// The value before the one being added.
    previous: Option<K>,
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
}

impl<K: Copy + Eq + Hash> FirstPage<K> {
    /// A page that can see at most `entries` values, whose dictionary starts with room for as
    /// many of them as it can hold.
    fn new(candidate: &Candidate, page_bytes: u64, entries: usize) -> Self {
        // A dictionary never holds more values than the page takes, nor more than one past the
        // most that fit within its limit, since a value takes at least its width, or a 4-byte
        // length.
        let most = (candidate.dictionary_limit / candidate.width.unwrap_or(4) + 1) as usize;
        Self {
            dictionary_limit: candidate.dictionary_limit,
            page_bytes,
            dictionary: HashSet::with_capacity(entries.min(most).min(MAX_RESERVED_VALUES)),
            previous: None,
            ids: HybridRuns::default(),
            plain_bytes: 0,
            dictionary_bytes: 0,
            rows: 0,
            full: false,
            overflowed: false,
        }
    }

    fn is_closed(&self) -> bool {
        self.full || self.overflowed
    }

    /// Adds `rows` rows of one batch, whose way down to the leaf is `path` and whose leaf values
    /// `read` reads. Stops reading as soon as the page ends.
    fn extend(&mut self, path: &Path<'_>, read: &impl Fn(usize) -> (K, u64), rows: usize) {
        if self.is_closed() {
            return;
        }
        let nulls = present_nulls(path.leaf);
        for row in 0..rows {
            let whole_row = path.visit(row, &mut |index| {
                if nulls.is_some_and(|nulls| nulls.is_null(index)) {
                    return true;
                }
                let (value, size) = read(index);
                self.add(value, size);
                !self.overflowed
            });
            if !whole_row {
                return;
            }
            self.rows += 1;
            if self.rows >= FIRST_PAGE_SIZE_CHECK_ROWS && self.plain_bytes >= self.page_bytes {
                self.full = true;
                return;
            }
        }
    }

    /// `DictionaryValuesWriter.write*` and `FallbackValuesWriter`'s raw size, for one value that
    /// takes `size` bytes in the plain encoding.
    fn add(&mut self, value: K, size: u64) {
        self.plain_bytes += size;
        // Ids are handed out in order of first appearance, so an id repeats the one before it
        // exactly when its value does, and a repeated value is in the dictionary already.
        let repeat = self.previous == Some(value);
        if !repeat && self.dictionary.insert(value) {
            self.dictionary_bytes += size;
        }
        self.ids.write(repeat);
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
        ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeListArray,
        ListArray, MapArray, StringArray, StructArray, UInt32Array,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::compute::{cast, take_record_batch};
    use arrow::datatypes::{Field, Fields, Int32Type, TimeUnit};

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

    /// Where a partition's rows were cut into batches does not change its choice, whether each
    /// batch was built on its own or sliced from a larger one, as the clustered writer slices a
    /// batch that spans partitions.
    #[test]
    fn batch_boundaries_do_not_change_the_choice() {
        let properties = || properties(20_000, 1024 * 1024, 2 * 1024 * 1024);
        let batch = corpus(0..20_000);
        let whole = dictionary_columns(properties(), std::slice::from_ref(&batch));
        for batch_rows in [777, 1000, 8192] {
            assert_eq!(
                dictionary_columns(properties(), &corpus_batches(20_000, batch_rows as u64)),
                whole,
                "batches of {batch_rows} rows"
            );
            let slices: Vec<RecordBatch> = (0..batch.num_rows())
                .step_by(batch_rows)
                .map(|start| batch.slice(start, batch_rows.min(batch.num_rows() - start)))
                .collect();
            assert_eq!(
                dictionary_columns(properties(), &slices),
                whole,
                "slices of {batch_rows} rows"
            );
        }
    }

    /// A batch sliced at an odd offset, as the clustered writer slices one that spans
    /// partitions, makes the same choice as the same rows gathered into arrays of their own, as
    /// the fanout writer gathers them.
    #[test]
    fn a_sliced_batch_is_read_only_within_its_slice() {
        let rows = 3000;
        let (offset, len) = (1001, 999);
        // Every list, map and struct here is null on odd rows. Only the entries within the slice
        // that are present and have nothing null above them hold one of four values. Every other
        // entry holds a value of its own, so reading one past the slice, or a null or anything
        // under one, overflows the 1 KiB dictionary.
        let value = |row: usize, entry: usize, counted: bool| -> i64 {
            if counted && (offset..offset + len).contains(&row) {
                (entry % 4) as i64
            } else {
                (1_000_000 + 4 * row + entry) as i64
            }
        };
        let even = |row: usize| row.is_multiple_of(2);
        let row_nulls = || Some(NullBuffer::from_iter((0..rows).map(even)));
        // Two entries per row, as the children of a list or a map.
        let pairs = |first_entry: usize| -> ArrayRef {
            Arc::new(Int64Array::from_iter_values(
                (0..2 * rows).map(|i| value(i / 2, first_entry + i % 2, even(i / 2))),
            ))
        };
        let element = || Arc::new(Field::new("element", DataType::Int64, true));
        let map_entries = StructArray::new(
            Fields::from(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, true),
            ]),
            vec![pairs(0), pairs(2)],
            None,
        );
        // `s.n` and `s.inner` are also null on every other even row.
        let quarter = |row: usize| row.is_multiple_of(4);
        let quarter_nulls = || Some(NullBuffer::from_iter((0..rows).map(|row| row % 4 != 2)));
        let x = Fields::from(vec![Field::new("x", DataType::Int64, true)]);
        let s = StructArray::new(
            Fields::from(vec![
                Field::new("n", DataType::Int64, true),
                Field::new("inner", DataType::Struct(x.clone()), true),
            ]),
            vec![
                Arc::new(Int64Array::new(
                    (0..rows).map(|row| value(row, 0, quarter(row))).collect(),
                    quarter_nulls(),
                )),
                Arc::new(StructArray::new(
                    x,
                    vec![Arc::new(Int64Array::from_iter_values(
                        (0..rows).map(|row| value(row, 1, quarter(row))),
                    ))],
                    quarter_nulls(),
                )),
            ],
            row_nulls(),
        );
        // A list of two structs per row, the second of them null.
        let k = Fields::from(vec![Field::new("k", DataType::Int64, true)]);
        let ls = ListArray::new(
            Arc::new(Field::new("element", DataType::Struct(k.clone()), true)),
            OffsetBuffer::from_repeated_length(2, rows),
            Arc::new(StructArray::new(
                k,
                vec![Arc::new(Int64Array::from_iter_values(
                    (0..2 * rows).map(|i| value(i / 2, i % 2, even(i / 2) && even(i))),
                ))],
                Some(NullBuffer::from_iter((0..2 * rows).map(even))),
            )),
            row_nulls(),
        );
        let columns: Vec<(&str, ArrayRef)> = vec![
            // Distinct within the slice too, so plain either way.
            ("id", Arc::new(Int64Array::from_iter_values(0..rows as i64))),
            (
                "flat",
                Arc::new(Int64Array::new(
                    (0..rows).map(|row| value(row, 0, even(row))).collect(),
                    row_nulls(),
                )),
            ),
            (
                "l",
                Arc::new(ListArray::new(
                    element(),
                    OffsetBuffer::from_repeated_length(2, rows),
                    pairs(0),
                    row_nulls(),
                )),
            ),
            (
                "ll",
                Arc::new(LargeListArray::new(
                    element(),
                    OffsetBuffer::from_repeated_length(2, rows),
                    pairs(0),
                    row_nulls(),
                )),
            ),
            (
                "fl",
                Arc::new(FixedSizeListArray::new(element(), 2, pairs(0), row_nulls())),
            ),
            (
                "m",
                Arc::new(MapArray::new(
                    Arc::new(Field::new(
                        "key_value",
                        map_entries.data_type().clone(),
                        false,
                    )),
                    OffsetBuffer::from_repeated_length(2, rows),
                    map_entries,
                    row_nulls(),
                    false,
                )),
            ),
            ("s", Arc::new(s)),
            ("ls", Arc::new(ls)),
        ];
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let properties = || properties(20_000, 1024 * 1024, 1024);
        // Read whole, the batch overflows every dictionary.
        assert!(dictionary_columns(properties(), std::slice::from_ref(&batch)).is_empty());

        let slice = batch.slice(offset, len);
        let gathered =
            take_record_batch(&slice, &UInt32Array::from_iter_values(0..len as u32)).unwrap();
        let expected = [
            "flat",
            "l.list.element",
            "ll.list.element",
            "fl.list.element",
            "m.key_value.key",
            "m.key_value.value",
            "s.n",
            "s.inner.x",
            "ls.list.element.k",
        ];
        assert_eq!(dictionary_columns(properties(), &[slice]), expected);
        assert_eq!(dictionary_columns(properties(), &[gathered]), expected);
    }

    /// A leaf column of any type is read. In each, a column of ten values keeps its dictionary and
    /// a column of distinct values does not.
    #[test]
    fn reads_every_leaf_type() {
        let rows = 1000;
        let longs = |value: fn(usize) -> i64| -> ArrayRef {
            Arc::new(Int64Array::from_iter(
                (0..rows).map(|i| (i % 7 != 3).then(|| value(i))),
            ))
        };
        let low = longs(|i| (i % 10) as i64);
        let unique = longs(|i| i as i64 + 1_000_000);
        let types = [
            ("int", DataType::Int32),
            ("long", DataType::Int64),
            ("float", DataType::Float32),
            ("double", DataType::Float64),
            ("date", DataType::Date32),
            ("time", DataType::Time64(TimeUnit::Microsecond)),
            (
                "timestamptz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            ),
            (
                "timestamp_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            ("decimal_9", DataType::Decimal128(9, 2)),
            ("decimal_18", DataType::Decimal128(18, 2)),
            ("decimal256_18", DataType::Decimal256(18, 2)),
            ("string", DataType::Utf8),
            ("large_string", DataType::LargeUtf8),
            ("string_view", DataType::Utf8View),
            ("binary", DataType::Binary),
            ("large_binary", DataType::LargeBinary),
            ("binary_view", DataType::BinaryView),
        ];
        let mut columns: Vec<(String, ArrayRef)> = Vec::new();
        let mut expected = Vec::new();
        for (name, data_type) in &types {
            // arrow casts longs to neither dates nor binaries.
            let via = match data_type {
                DataType::Date32 => DataType::Int32,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView => DataType::Utf8,
                _ => data_type.clone(),
            };
            let convert = |longs: &ArrayRef| cast(&cast(longs, &via).unwrap(), data_type).unwrap();
            columns.push((format!("low_{name}"), convert(&low)));
            columns.push((format!("unique_{name}"), convert(&unique)));
            expected.push(format!("low_{name}"));
        }
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        assert_eq!(
            dictionary_columns(properties(20_000, 1024 * 1024, 2 * 1024 * 1024), &[batch]),
            expected
        );
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

    /// A column is read only as far as its first page reaches. Each row here lists 8192 ints, so
    /// the page is full at row 100, parquet-mr's first size check, and the values of the other 300
    /// rows are never read.
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
        let path = candidate.leaf.path(&batch).unwrap();
        let read = bits::<_, u64>(path.leaf.as_primitive::<Int32Type>().values(), 4);
        let reads = Cell::new(0);
        let counted = |index| {
            reads.set(reads.get() + 1);
            read(index)
        };
        let mut page = FirstPage::new(candidate, chooser.page_bytes, 0);
        page.extend(&path, &counted, rows);
        assert!(page.full);
        assert_eq!(page.rows, 100);
        assert_eq!(reads.get(), 100 * 8192);
    }

    /// A null list can still span child values. They are not part of the column chunk, so they
    /// must not count toward its first page.
    #[test]
    fn values_under_a_null_list_are_not_counted() {
        // Row 1 is a null list spanning 1000 unique values, which would overflow the 1 KiB
        // dictionary if they were counted. Every other row holds one 7, so the page the column
        // chunk actually has is 1001 repeats of one value. Row 0 comes first so that an overflow
        // would end a page that has rows, which is then judged.
        let mut offsets = vec![0i32, 1, 1001];
        offsets.extend((2..=1001).map(|row| 1000 + row));
        let list = ListArray::new(
            Arc::new(Field::new("element", DataType::Int64, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(Int64Array::from_iter_values((0..2001).map(|i| {
                if (1..=1000).contains(&i) {
                    i
                } else {
                    7
                }
            }))),
            Some(NullBuffer::from_iter((0..=1001).map(|row| row != 1))),
        );
        let batch = RecordBatch::try_from_iter([("l", Arc::new(list) as ArrayRef)]).unwrap();
        assert_eq!(
            dictionary_columns(properties(20_000, 1024 * 1024, 1024), &[batch]),
            ["l.list.element"]
        );
    }

    /// The fields of a null struct are not part of the column chunk either.
    #[test]
    fn values_under_a_null_struct_are_not_counted() {
        // Every odd row's struct is null over a value no other row has, which would overflow the
        // 1 KiB dictionary if they were counted. Every even row holds 7, so the page the column
        // chunk actually has is 1000 repeats of one value.
        let values =
            Int64Array::from_iter_values((0..2000).map(|i| if i % 2 == 1 { i } else { 7 }));
        let st = StructArray::new(
            Fields::from(vec![Field::new("n", DataType::Int64, true)]),
            vec![Arc::new(values) as ArrayRef],
            Some(NullBuffer::from_iter((0..2000).map(|i| i % 2 == 0))),
        );
        let batch = RecordBatch::try_from_iter([("s", Arc::new(st) as ArrayRef)]).unwrap();
        assert_eq!(
            dictionary_columns(properties(20_000, 1024 * 1024, 1024), &[batch]),
            ["s.n"]
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
