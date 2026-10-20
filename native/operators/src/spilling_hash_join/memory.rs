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

//! Memory accounting for [`super::SpillingHashJoinExec`].
//!
//! The operator reserves, in the task's memory pool, everything an inner `HashJoinExec` holds,
//! including what `HashJoinExec` itself allocates without reserving. The estimates here follow
//! `collect_left_input` in DataFusion 55.2 and have to be re-checked on a DataFusion upgrade.

use std::fmt::{Display, Formatter};
use std::mem::size_of;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::{max, min};
use arrow::datatypes::{
    DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use arrow::record_batch::RecordBatch;
use datafusion::common::utils::memory::estimate_memory_size;
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result};
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_expr_common::utils::evaluate_expressions_to_arrays;
use datafusion::physical_plan::joins::join_hash_map::{JoinHashMapU32, JoinHashMapU64};
use datafusion::physical_plan::metrics::Count;
use parking_lot::Mutex;

/// Whether `error` is a memory pool refusing a request, rather than any other failure. A Spark
/// acquire that fails, or an interrupted task, surfaces as a different error and must not be
/// treated as a reason to spill.
pub(super) fn is_refusal(error: &DataFusionError) -> bool {
    matches!(error.find_root(), DataFusionError::ResourcesExhausted(_))
}

/// Grows `reservation` by `bytes`. Returns false if the pool refuses.
pub(super) fn try_reserve(reservation: &MemoryReservation, bytes: usize) -> Result<bool> {
    match reservation.try_grow(bytes) {
        Ok(()) => Ok(true),
        Err(e) if is_refusal(&e) => Ok(false),
        Err(e) => Err(e),
    }
}

/// The memory pool of the inner `HashJoinExec`s.
///
/// An inner join reserves through this pool rather than the task's, so the task's pool sees one
/// consumer, the operator's, as it would for a plain `HashJoinExec`. Before it starts an inner
/// join, the operator reserves the inner join's budget in the task's pool, so requests within the
/// budget always succeed. A request beyond the budget goes to `overflow`, a reservation of the
/// operator's consumer in the task's pool. A low estimate is therefore still accounted, and the
/// inner join is refused only when the task's pool refuses.
///
/// Only one inner join runs at a time, and each one releases everything it reserved before the
/// next one starts.
#[derive(Debug)]
pub(super) struct BudgetPool {
    state: Mutex<BudgetState>,
    /// Bytes reserved beyond the budget, which mean an estimate was too low.
    overflow_bytes: Count,
}

#[derive(Debug)]
struct BudgetState {
    budget: usize,
    used: usize,
    overflow: MemoryReservation,
}

impl BudgetPool {
    pub(super) fn new(overflow: MemoryReservation, overflow_bytes: Count) -> Self {
        Self {
            state: Mutex::new(BudgetState {
                budget: 0,
                used: 0,
                overflow,
            }),
            overflow_bytes,
        }
    }

    /// Sets the budget of the next inner join.
    pub(super) fn set_budget(&self, budget: usize) {
        let mut state = self.state.lock();
        debug_assert_eq!(state.used, 0, "an inner join still holds memory");
        state.budget = budget;
    }
}

impl BudgetState {
    fn excess(&self) -> usize {
        self.used.saturating_sub(self.budget)
    }

    fn release_overflow(&mut self) {
        let excess = self.excess();
        if self.overflow.size() > excess {
            self.overflow.shrink(self.overflow.size() - excess);
        }
    }
}

impl Display for BudgetPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        write!(
            f,
            "BudgetPool(budget={}, used={}, overflow={})",
            state.budget,
            state.used,
            state.overflow.size()
        )
    }
}

impl MemoryPool for BudgetPool {
    fn name(&self) -> &str {
        "SpillingHashJoinBudgetPool"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        state.used = state.used.saturating_add(additional);
        let excess = state.excess();
        if excess > state.overflow.size() {
            let missing = excess - state.overflow.size();
            state.overflow.grow(missing);
            self.overflow_bytes.add(missing);
        }
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock();
        state.used = state.used.saturating_sub(shrink);
        state.release_overflow();
    }

    fn try_grow(&self, _reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock();
        let used = state.used.saturating_add(additional);
        let excess = used.saturating_sub(state.budget);
        if excess > state.overflow.size() {
            let missing = excess - state.overflow.size();
            state.overflow.try_grow(missing)?;
            self.overflow_bytes.add(missing);
        }
        state.used = used;
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.state.lock().used
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Infinite
    }
}

/// The memory an inner `HashJoinExec` needs for its build side, besides the build batches.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct BuildMemory {
    /// What the inner join reserves through its pool: the lookup table and, for join types that
    /// emit unmatched build rows, the bitmap of visited rows.
    pub(super) reserved: usize,
    /// What it allocates for as long as it runs without reserving it: the hash map's chain of row
    /// indices, and join keys computed from expressions rather than read from columns.
    pub(super) unreserved: usize,
    /// What it allocates without reserving only until its build completes: the single batch it
    /// concatenates the build batches into, while the batches themselves are still held.
    pub(super) transient: usize,
}

/// The settings of DataFusion's perfect hash join, `ArrayMap`, which a single integer key uses
/// instead of a hash map when its values are dense.
#[derive(Debug, Clone, Copy)]
pub(super) struct PerfectHashConfig {
    pub(super) small_build_threshold: usize,
    pub(super) min_key_density: f64,
}

/// Estimates [`BuildMemory`] for the build sides of one join.
#[derive(Debug, Clone)]
pub(super) struct BuildMemoryEstimator {
    build_keys: Vec<PhysicalExprRef>,
    computed_keys: Vec<PhysicalExprRef>,
    /// Whether the key is a single integer, which `HashJoinExec` may look up with an `ArrayMap`.
    integer_key: bool,
    visited_bitmap: bool,
    null_equality: NullEquality,
    perfect_hash: PerfectHashConfig,
}

impl BuildMemoryEstimator {
    pub(super) fn try_new(
        build_keys: &[PhysicalExprRef],
        build_schema: &arrow::datatypes::Schema,
        join_type: JoinType,
        null_equality: NullEquality,
        perfect_hash: PerfectHashConfig,
    ) -> Result<Self> {
        let integer_key =
            build_keys.len() == 1 && is_array_map_type(&build_keys[0].data_type(build_schema)?);
        let computed_keys = build_keys
            .iter()
            .filter(|key| key.downcast_ref::<Column>().is_none())
            .cloned()
            .collect();
        Ok(Self {
            build_keys: build_keys.to_vec(),
            computed_keys,
            integer_key,
            visited_bitmap: needs_visited_bitmap(join_type),
            null_equality,
            perfect_hash,
        })
    }

    /// For build batches held in memory, whose key range is known. `batch_bytes` is what the
    /// inner join will count for the batches.
    pub(super) fn for_batches(
        &self,
        batches: &[RecordBatch],
        batch_bytes: usize,
    ) -> Result<BuildMemory> {
        let rows = batches.iter().map(|batch| batch.num_rows()).sum();
        let array_map = if self.integer_key {
            self.array_map_bytes(batches, rows)?
        } else {
            None
        };
        let mut computed_key_bytes = 0;
        if !self.computed_keys.is_empty() {
            for batch in batches {
                for array in evaluate_expressions_to_arrays(&self.computed_keys, batch)? {
                    computed_key_bytes += array.get_array_memory_size();
                }
            }
        }
        self.estimate(rows, batch_bytes, array_map, computed_key_bytes)
    }

    /// For a build side that is on disk, whose key range is unknown, so an `ArrayMap` has to be
    /// assumed at the largest size `HashJoinExec` would build one.
    pub(super) fn for_rows(&self, rows: usize, batch_bytes: usize) -> Result<BuildMemory> {
        // About what a computed key costs per row, whether fixed width or not.
        let computed_key_bytes = rows.saturating_mul(32 * self.computed_keys.len());
        let (table, chain) = hash_map_bytes(rows)?;
        // Either structure may be built, so assume the larger.
        let array_map = if self.integer_key {
            self.largest_array_map_bytes(rows)
                .filter(|bytes| *bytes > table.saturating_add(chain))
        } else {
            None
        };
        self.estimate(rows, batch_bytes, array_map, computed_key_bytes)
    }

    /// An upper bound of what one build batch adds to an inner join besides the batch itself,
    /// for taking batches into a chunk before the chunk's key range is known.
    ///
    /// A hash map holds a (u32 or u64, u64) entry and a control byte per slot, in a table of at
    /// most 16/7 slots per row, plus up to 8 bytes of chain per row: under 48 bytes a row. An
    /// `ArrayMap` that `HashJoinExec` chooses is smaller per row, apart from a range of up to the
    /// small build threshold, which the constant covers. Then the bitmap, computed keys, and the
    /// batch's share of the concatenated copy.
    pub(super) fn chunk_bound(&self, batch: &RecordBatch, batch_bytes: usize) -> usize {
        let rows = batch.num_rows();
        let per_row = 48 + 32 * self.computed_keys.len();
        rows.saturating_mul(per_row)
            .saturating_add(rows.div_ceil(8))
            .saturating_add(self.perfect_hash.small_build_threshold.saturating_mul(4))
            .saturating_add(batch_bytes)
    }

    fn estimate(
        &self,
        rows: usize,
        batch_bytes: usize,
        array_map: Option<usize>,
        computed_key_bytes: usize,
    ) -> Result<BuildMemory> {
        let (table, chain) = match array_map {
            // An `ArrayMap` reserves its chain of row indices along with its slots.
            Some(bytes) => (bytes, 0),
            None => hash_map_bytes(rows)?,
        };
        let bitmap = if self.visited_bitmap {
            rows.div_ceil(8)
        } else {
            0
        };
        Ok(BuildMemory {
            reserved: table.saturating_add(bitmap),
            unreserved: chain.saturating_add(computed_key_bytes),
            transient: batch_bytes,
        })
    }

    /// The bytes `HashJoinExec` reserves for an `ArrayMap` over `batches`, or None when it would
    /// use a hash map. Follows `try_create_array_map`.
    fn array_map_bytes(&self, batches: &[RecordBatch], rows: usize) -> Result<Option<usize>> {
        let mut bounds: Option<(i128, i128)> = None;
        let mut has_nulls = false;
        for batch in batches {
            let keys = evaluate_expressions_to_arrays(&self.build_keys, batch)?;
            let key = &keys[0];
            has_nulls |= key.null_count() > 0;
            if let Some((lo, hi)) = integer_bounds(key) {
                bounds = Some(match bounds {
                    Some((min_so_far, max_so_far)) => (min_so_far.min(lo), max_so_far.max(hi)),
                    None => (lo, hi),
                });
            }
        }
        if self.null_equality == NullEquality::NullEqualsNull && has_nulls {
            return Ok(None);
        }
        let Some((lo, hi)) = bounds else {
            return Ok(None);
        };
        // `ArrayMap::key_to_u64` casts each value to u64, and the range wraps.
        let range = (hi as u64).wrapping_sub(lo as u64);
        if rows >= u32::MAX as usize || range == usize::MAX as u64 {
            return Ok(None);
        }
        let dense_ratio = rows as f64 / (range as f64 + 1.0);
        if range >= self.perfect_hash.small_build_threshold as u64
            && dense_ratio <= self.perfect_hash.min_key_density
        {
            return Ok(None);
        }
        Ok(Some(array_map_estimate(range, rows)))
    }

    /// The largest `ArrayMap` that `HashJoinExec` would build over `rows` rows of unknown range.
    /// Its range is either below the small build threshold, or small enough that the keys are
    /// denser than the minimum density.
    fn largest_array_map_bytes(&self, rows: usize) -> Option<usize> {
        if rows >= u32::MAX as usize {
            return None;
        }
        let dense_range = if self.perfect_hash.min_key_density > 0.0 {
            (rows as f64 / self.perfect_hash.min_key_density) as u64
        } else {
            // Then any range qualifies and there is no bound, so leave a larger table to the
            // overflow reservation.
            0
        };
        let small_range = self.perfect_hash.small_build_threshold as u64;
        Some(array_map_estimate(dense_range.max(small_range), rows))
    }
}

/// `ArrayMap::estimate_memory_size`: one u32 slot per key value in the range and one u32 chain
/// entry per row.
fn array_map_estimate(range: u64, rows: usize) -> usize {
    let slots = usize::try_from(range)
        .unwrap_or(usize::MAX)
        .saturating_add(1);
    slots
        .saturating_mul(size_of::<u32>())
        .saturating_add(rows.saturating_mul(size_of::<u32>()))
}

/// The bytes `HashJoinExec` reserves for its hash map, and the bytes of the chain of row indices
/// it allocates without reserving them.
fn hash_map_bytes(rows: usize) -> Result<(usize, usize)> {
    if rows > u32::MAX as usize {
        Ok((
            estimate_memory_size::<(u64, u64)>(rows, size_of::<JoinHashMapU64>())?,
            rows.saturating_mul(size_of::<u64>()),
        ))
    } else {
        Ok((
            estimate_memory_size::<(u32, u64)>(rows, size_of::<JoinHashMapU32>())?,
            rows.saturating_mul(size_of::<u32>()),
        ))
    }
}

/// `need_produce_result_in_final`: the join types whose `HashJoinExec` keeps a bitmap of the build
/// rows that found a match.
pub(super) fn needs_visited_bitmap(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left
            | JoinType::LeftAnti
            | JoinType::LeftSemi
            | JoinType::LeftMark
            | JoinType::Full
    )
}

/// `ArrayMap::is_supported_type`.
fn is_array_map_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

/// The minimum and maximum non-null value of an integer array, or None if it has none.
fn integer_bounds(array: &ArrayRef) -> Option<(i128, i128)> {
    macro_rules! bounds {
        ($t:ty) => {{
            let array = array.as_primitive::<$t>();
            min(array)
                .zip(max(array))
                .map(|(lo, hi)| (lo as i128, hi as i128))
        }};
    }
    match array.data_type() {
        DataType::Int8 => bounds!(Int8Type),
        DataType::Int16 => bounds!(Int16Type),
        DataType::Int32 => bounds!(Int32Type),
        DataType::Int64 => bounds!(Int64Type),
        DataType::UInt8 => bounds!(UInt8Type),
        DataType::UInt16 => bounds!(UInt16Type),
        DataType::UInt32 => bounds!(UInt32Type),
        DataType::UInt64 => bounds!(UInt64Type),
        _ => None,
    }
}
