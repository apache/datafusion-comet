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

//! Delta Lake deletion-vector decoding and translation into DataFusion
//! [`ParquetAccessPlan`]s (feature = "delta").
//!
//! Wire formats implemented here (from delta-spark's `DeletionVectorStore` /
//! `RoaringBitmapArray`, v3.3.2):
//! - On-disk DV file: 1 version byte at the start of the file; at
//!   `descriptor.offset`: `[i32 BE size][data: size bytes][i32 BE CRC32(data)]`.
//! - `data`: `[i32 LE magic]` then either
//!   - magic 1681511376 ("native"): `[i32 LE count]`, then per bitmap
//!     `[i32 LE size][standard 32-bit RoaringBitmap]`, keys implicit (index);
//!   - magic 1681511377 ("portable", the spec's 64-bit extension): `[i64 LE
//!     count]`, then per bitmap `[i32 LE key][standard 32-bit RoaringBitmap]`
//!     with keys ascending -- exactly [`RoaringTreemap`]'s serialized form.

use std::mem::size_of;
use std::sync::Arc;

use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::execution::operators::ExecutionError;
use crate::execution::operators::ExecutionError::GeneralError;
use datafusion_comet_proto::spark_operator::DeltaSparkDvDescriptor;

const NATIVE_MAGIC: i32 = 1681511376;
const PORTABLE_MAGIC: i32 = 1681511377;

/// Unframe a DV blob read from `descriptor.offset` of a DV file:
/// `[i32 BE size][data][i32 BE crc]`. Verifies both the size against the
/// descriptor's `size_in_bytes` and the CRC32 checksum.
pub fn unframe_dv_blob(blob: &[u8], expected_size: usize) -> Result<&[u8], ExecutionError> {
    if blob.len() < 8 {
        return Err(GeneralError(format!(
            "Deletion vector blob too short: {} bytes",
            blob.len()
        )));
    }
    let size = i32::from_be_bytes(blob[0..4].try_into().unwrap());
    if size < 0 || size as usize != expected_size {
        return Err(GeneralError(format!(
            "Deletion vector size mismatch: file says {size}, descriptor says {expected_size}"
        )));
    }
    let end = 4 + size as usize;
    if blob.len() < end + 4 {
        return Err(GeneralError(format!(
            "Deletion vector blob truncated: need {} bytes, have {}",
            end + 4,
            blob.len()
        )));
    }
    let data = &blob[4..end];
    let expected_crc = i32::from_be_bytes(blob[end..end + 4].try_into().unwrap());
    let actual_crc = crc32fast::hash(data) as i32;
    if expected_crc != actual_crc {
        return Err(GeneralError(
            "Deletion vector checksum mismatch".to_string(),
        ));
    }
    Ok(data)
}

/// Deserialize the magic-prefixed RoaringBitmapArray into a 64-bit treemap of
/// deleted row indexes.
pub fn deserialize_dv_bitmap(data: &[u8]) -> Result<RoaringTreemap, ExecutionError> {
    if data.len() < 4 {
        return Err(GeneralError(
            "Deletion vector bitmap too short for magic number".to_string(),
        ));
    }
    let magic = i32::from_le_bytes(data[0..4].try_into().unwrap());
    let rest = &data[4..];
    match magic {
        PORTABLE_MAGIC => RoaringTreemap::deserialize_from(rest)
            .map_err(|e| GeneralError(format!("Invalid portable deletion vector bitmap: {e}"))),
        NATIVE_MAGIC => {
            if rest.len() < 4 {
                return Err(GeneralError(
                    "Native deletion vector bitmap missing count".to_string(),
                ));
            }
            let count = i32::from_le_bytes(rest[0..4].try_into().unwrap());
            if count < 0 {
                return Err(GeneralError(format!(
                    "Invalid RoaringBitmapArray length ({count} < 0)"
                )));
            }
            let mut pos = 4usize;
            let mut treemap = RoaringTreemap::new();
            for key in 0..count as u64 {
                if rest.len() < pos + 4 {
                    return Err(GeneralError(
                        "Native deletion vector bitmap truncated".to_string(),
                    ));
                }
                let size = i32::from_le_bytes(rest[pos..pos + 4].try_into().unwrap());
                pos += 4;
                if size < 0 || rest.len() < pos + size as usize {
                    return Err(GeneralError(
                        "Native deletion vector bitmap truncated".to_string(),
                    ));
                }
                let bitmap = RoaringBitmap::deserialize_from(&rest[pos..pos + size as usize])
                    .map_err(|e| {
                        GeneralError(format!("Invalid deletion vector sub-bitmap: {e}"))
                    })?;
                pos += size as usize;
                for value in bitmap {
                    treemap.insert((key << 32) | value as u64);
                }
            }
            Ok(treemap)
        }
        other => Err(GeneralError(format!(
            "Unexpected RoaringBitmapArray magic number {other}"
        ))),
    }
}

/// Translate deleted row indexes into a [`ParquetAccessPlan`]: fully-deleted
/// row groups become `Skip`, untouched groups stay `Scan`, and partially
/// deleted groups get a `RowSelection` selecting the complement of the deleted
/// rows. Page-index pruning later INTERSECTS with these selections, so DV
/// skips and page skips compose.
pub fn build_access_plan(
    row_group_row_counts: &[i64],
    deleted: &RoaringTreemap,
) -> Result<ParquetAccessPlan, ExecutionError> {
    let mut plan = ParquetAccessPlan::new_all(row_group_row_counts.len());
    // Single sweep over the (sorted) deleted row indexes, bucketing by row group.
    let mut deleted_iter = deleted.iter().peekable();
    let mut group_start = 0u64;
    for (idx, &num_rows) in row_group_row_counts.iter().enumerate() {
        let num_rows = num_rows as u64;
        let group_end = group_start + num_rows;
        let mut selectors: Vec<RowSelector> = Vec::new();
        let mut cursor = group_start;
        let mut deleted_in_group = 0u64;
        while let Some(&row) = deleted_iter.peek() {
            if row >= group_end {
                break;
            }
            deleted_iter.next();
            deleted_in_group += 1;
            if row > cursor {
                selectors.push(RowSelector::select((row - cursor) as usize));
            }
            // Merge runs of consecutive deleted rows into one skip.
            match selectors.last_mut() {
                Some(last) if last.skip => last.row_count += 1,
                _ => selectors.push(RowSelector::skip(1)),
            }
            cursor = row + 1;
        }
        if deleted_in_group == num_rows && num_rows > 0 {
            plan.skip(idx);
        } else if deleted_in_group > 0 {
            if group_end > cursor {
                selectors.push(RowSelector::select((group_end - cursor) as usize));
            }
            plan.scan_selection(idx, RowSelection::from(selectors));
        }
        group_start = group_end;
    }
    // A deleted index beyond the file's total row count means the DV does not
    // belong to this file (stale or corrupted metadata); silently dropping it
    // would under-apply deletions.
    if let Some(&row) = deleted_iter.peek() {
        return Err(GeneralError(format!(
            "Deletion vector marks row {row} but the file only has {group_start} rows"
        )));
    }
    Ok(plan)
}

/// Verify a decoded deletion vector's row count matches the descriptor's
/// declared `cardinality`, mirroring Delta's JVM reader
/// (`StoredBitmap.validateCardinality`). The CRC and framing checks catch
/// corruption but not a stale, otherwise well-formed bitmap whose row count
/// no longer matches the descriptor -- that would silently under- or
/// over-delete rows.
fn validate_cardinality(
    file_path: &str,
    expected: i64,
    deleted: &RoaringTreemap,
) -> Result<(), ExecutionError> {
    let actual = deleted.len();
    if actual != expected as u64 {
        return Err(GeneralError(format!(
            "Deletion vector for {file_path} has cardinality mismatch: descriptor says {expected}, decoded bitmap has {actual} deleted rows"
        )));
    }
    Ok(())
}

/// One data file plus everything needed to apply its deletion vector. The
/// file's size comes from `file.object_meta.size` (built by the planner from
/// the proto's `file_size`).
///
/// `data_store` and `dv_store` are resolved by the caller *before* entering
/// the async `attach_access_plans` runtime (see its doc comment): building an
/// object store is sync I/O that, for a cold S3 authority, internally issues
/// its own `Handle::block_on` calls, which panics if nested inside another
/// `block_on`. Resolving up front means this module never constructs a
/// store itself.
pub struct DvScanFile {
    pub file: PartitionedFile,
    /// Full URL of the data file (proto `file_path`).
    pub file_path: String,
    pub dv: Option<DeltaSparkDvDescriptor>,
    /// Object store for `file_path`, pre-resolved by the caller. Only read
    /// when `dv` is `Some` (files without a deletion vector never open their
    /// footer here), but every file carries one so the struct's shape
    /// doesn't depend on whether a deletion vector is present.
    pub data_store: Arc<dyn ObjectStore>,
    /// Store and within-store path for an on-disk deletion vector's absolute
    /// path, pre-resolved by the caller. `None` when the file has no
    /// deletion vector or the deletion vector is stored inline.
    pub dv_store: Option<(Arc<dyn ObjectStore>, Path)>,
}

/// Execution-memory-pool reservation covering one file's expanded DV row selectors across
/// their *entire* lifetime attached to a scan -- from `build_access_plan`'s construction
/// through DataFusion 54.1's reader normalizing the attached [`ParquetAccessPlan`]
/// (`create_initial_plan`'s deep clone plus `into_overall_row_selection`'s combined
/// `RowSelection`; see [`reader_peak_bytes`]) -- attached to the file's [`PartitionedFile`]
/// extensions alongside its [`ParquetAccessPlan`]. The reservation's lifetime is tied to the
/// `PartitionedFile` it is attached to, so it is released back to the pool exactly when the
/// plan is dropped (query completion or an early-terminated scan), never held open longer.
/// Newtype-wrapped so it occupies its own slot in the multi-slot, type-keyed `extensions` map
/// (`datafusion_common::extensions::Extensions`) alongside the plan, rather than a bare
/// `MemoryReservation` colliding with one some other extension might attach.
pub struct DvAccessPlanReservation(pub MemoryReservation);

/// Total number of [`RowSelector`]s materialized across `plan`'s per-row-group
/// selections (`RowGroupAccess::Selection`); `Scan`/`Skip` row groups
/// contribute none. An alternating deleted/retained bitmap produces one
/// non-coalescing selector per row (see [`reader_peak_bytes`]'s doc comment
/// for the worst-case accounting), so this count -- not the deletion
/// vector's cardinality -- is the thing that must be bounded and reserved
/// against the execution memory pool.
fn total_selectors(plan: &ParquetAccessPlan) -> usize {
    plan.inner()
        .iter()
        .map(|access| match access {
            RowGroupAccess::Selection(selection) => selection.iter().count(),
            _ => 0,
        })
        .sum()
}

/// Multiplier bounding the peak allocation live *during construction* of one
/// file's [`RowSelection`]s, relative to the conservative selector-count
/// bound `S = 2 * cardinality + num_row_groups` (one non-coalescing selector
/// per deleted row in the worst-case alternating pattern, doubled, plus up to
/// one extra boundary selector per row group). Split `S` into `r`, the
/// selectors already retained from row groups `build_access_plan` has
/// finished, and `c`, the selectors accumulated so far in the current row
/// group's source `Vec`; `r` and `c` partition the selectors counted toward
/// `S`, so `r + c <= S` always. While the current group is being built, the
/// `Vec`'s doubling growth strategy can leave its backing allocation at up to
/// `2 * c` (the next power-of-two capacity above `c`). Once the group
/// finishes, `RowSelection::from(Vec)` (parquet's `FromIterator` impl,
/// `with_capacity` + copy) builds a second, separate `Vec` of size `c` from
/// that source while the source is still alive, so at the moment the copy
/// begins, the retained selectors, the current group's doubled source `Vec`,
/// and the copy are all live simultaneously: `r + 2c + c = r + 3c`. Since
/// `r >= 0`, `r + 3c <= 3r + 3c = 3(r + c) <= 3S`. 3x covers that peak.
const CONSTRUCTION_PEAK_FACTOR: usize = 3;

/// Upper bound on how much larger a `Vec`'s backing allocation can be than its element count
/// after being built by repeated pushes: `std`'s doubling growth strategy never leaves a `Vec`
/// of `n` elements with a backing allocation larger than the next power of two above `n`, which
/// is at most `2 * n` for any `n >= 1`.
const VEC_GROWTH_CAPACITY_FACTOR: usize = 2;

/// `RawVec`'s minimum non-zero capacity for element sizes `<= 1024` bytes ([`RowSelector`] is
/// 16 bytes on 64-bit platforms: a `usize` row count plus a padded `bool`). Applied once per
/// row group (or per contiguous run of row groups) a fresh `from_fn`/`FlatMap`-driven `Vec`
/// gets built for (see [`reader_peak_bytes`]), so even a group or run whose true selector count
/// is tiny still pays this floor.
const MIN_VEC_CAPACITY_SELECTORS: usize = 4;

/// Conservative upper bound, in bytes, on the peak allocation live while DataFusion 54.1's
/// reader normalizes one file's attached [`ParquetAccessPlan`] -- the allocation this module's
/// steady-state reservation must cover, not merely the plan's own retained selector bytes.
/// THREE allocations can be live simultaneously by the time `into_overall_row_selection`
/// returns, not two -- the clone is only exact when page-index pruning never touches it:
///
/// 1. **Attached original** (`selectors`, exact): `create_initial_plan` deep-clones the
///    attached plan while the original remains reachable from the file's `extensions` until
///    the scan consumes it. The ORIGINAL's own selector `Vec`s are exact -- a coalesced
///    [`RowSelection`] built via `RowSelection::from(Vec<RowSelector>)` (what
///    `build_access_plan` uses) has no excess capacity, because that conversion is a plain
///    `with_capacity(len)` copy, not a `size_hint`-blind fold.
/// 2. **The clone, possibly capacity-inflated** (`<= VEC_GROWTH_CAPACITY_FACTOR * selectors +
///    MIN_VEC_CAPACITY_SELECTORS * num_row_groups`): if page-index pruning fires
///    (`PagePruningAccessPlanFilter`; `access_plan.rs`'s `scan_selection` on a row group that
///    already carries a `RowGroupAccess::Selection` calls `existing.intersection(&page_derived)`
///    -- `RowSelection::intersection` -> `intersect_row_selections`), it replaces the CLONE's
///    per-row-group selection with that intersection's output. `intersect_row_selections` is
///    ANOTHER `from_fn` generator with `size_hint() == (0, None)`, so each intersected row
///    group's backing `Vec` starts at `with_capacity(0)` and doubles as it grows, independent
///    of whatever capacity the pre-intersection selection had. This inflated clone is still
///    live when `into_overall_row_selection` later moves its buffer. Term 1's exactness
///    guarantee holds for the ORIGINAL always, and for the clone only when page-index pruning
///    never fires against it -- once it does, the clone must be charged at the SAME
///    growth-capped bound as a fresh combined-selection `Vec` (term 3), summed once per row
///    group rather than once per run, since each row group's `Selection` is intersected
///    independently.
/// 3. **Per-run combined-selection allocation** (`<= VEC_GROWTH_CAPACITY_FACTOR * (selectors +
///    num_row_groups) + MIN_VEC_CAPACITY_SELECTORS * num_row_groups`): `into_overall_row_selection`
///    collects each contiguous run of row groups' selectors into a *new* `RowSelection` via a
///    `FlatMap` whose `size_hint().0 == 0`, so that run's `Vec` starts at `with_capacity(0)`
///    and doubles as it grows -- capping its backing allocation at
///    `max(MIN_VEC_CAPACITY_SELECTORS, next_power_of_two(len))`, which is at most
///    `MIN_VEC_CAPACITY_SELECTORS + VEC_GROWTH_CAPACITY_FACTOR * len` for a run of `len`
///    selectors. `len` is at most that run's share of `selectors` plus one boundary selector
///    per `RowGroupAccess::Scan` row group in the run (`Scan` always contributes exactly one
///    `RowSelector::select(num_rows)`; see `access_plan.rs`'s `into_overall_row_selection`).
///    Summing across at most `num_row_groups` runs (each spans >= 1 row group) bounds the total
///    at `VEC_GROWTH_CAPACITY_FACTOR * selectors + (MIN_VEC_CAPACITY_SELECTORS +
///    VEC_GROWTH_CAPACITY_FACTOR) * num_row_groups`.
///
/// Summing all three terms and converting to bytes: `((1 + 2 * VEC_GROWTH_CAPACITY_FACTOR) *
/// selectors + (2 * MIN_VEC_CAPACITY_SELECTORS + VEC_GROWTH_CAPACITY_FACTOR) * num_row_groups)
/// * size_of::<RowSelector>()` -- with the constants above, `(5 * selectors + 10 *
///   num_row_groups) * size_of::<RowSelector>()`. Checked against two measured worst cases:
///
/// - No page-index pruning (the original P2 report; term 2 stays exact): one 2,000,000-row
///   group, 1,000,000 alternating deletions, `selectors = 2,000,000`. Measured allocator peak
///   97,554,457 B; the byte-for-byte accounting for the attached original plus the (here,
///   exact) clone plus the inflated combined selection explains 97,554,432 B of that, a 25 B
///   residue we did not attribute. This bound gives 160,000,160 B -- much looser here because
///   it must also cover the next case, where the clone is NOT exact.
/// - Page-index pruning fires against the clone: one 1,048,577-row group, `selectors =
///   1,048,577`. Measured peak 83,886,096 B; this bound gives 83,886,320 B (a 224 B, <1%
///   margin -- deliberately tight, since this is the case that drives the bound).
///
/// Uses checked arithmetic throughout: a selector or row-group count large enough to overflow
/// `usize` indicates a corrupted or malicious input, reported as a clean error rather than
/// panicking.
fn reader_peak_bytes(selectors: usize, num_row_groups: usize) -> Result<usize, ExecutionError> {
    let overflow = || {
        GeneralError(format!(
            "Deletion vector reader-peak bound overflowed for {selectors} selectors and \
             {num_row_groups} row groups"
        ))
    };
    // Term 1: the attached original -- exact, untouched by page-index pruning (only the clone
    // is ever intersected; see the doc comment above).
    let attached_term = selectors;
    // Term 2: the clone, bounded as if page-index pruning DID fire against every row group
    // (safe even when it doesn't: term 2's bound is always >= `selectors`, so it never
    // undershoots the exact case either).
    let clone_growth = selectors
        .checked_mul(VEC_GROWTH_CAPACITY_FACTOR)
        .ok_or_else(overflow)?;
    let clone_floor = num_row_groups
        .checked_mul(MIN_VEC_CAPACITY_SELECTORS)
        .ok_or_else(overflow)?;
    let clone_term = clone_growth.checked_add(clone_floor).ok_or_else(overflow)?;
    // Term 3: into_overall_row_selection's per-run combined-selection allocation.
    let combined_growth = selectors
        .checked_mul(VEC_GROWTH_CAPACITY_FACTOR)
        .ok_or_else(overflow)?;
    let combined_floor = num_row_groups
        .checked_mul(MIN_VEC_CAPACITY_SELECTORS + VEC_GROWTH_CAPACITY_FACTOR)
        .ok_or_else(overflow)?;
    let combined_term = combined_growth
        .checked_add(combined_floor)
        .ok_or_else(overflow)?;

    let selector_bound = attached_term
        .checked_add(clone_term)
        .and_then(|sum| sum.checked_add(combined_term))
        .ok_or_else(overflow)?;
    selector_bound
        .checked_mul(size_of::<RowSelector>())
        .ok_or_else(overflow)
}

/// Upper bound, in [`RowSelector`]s, on how many extra selectors the parquet reader's
/// page-index pruning can add on top of the deletion vector's own selection when normalizing
/// one file, from that file's already-fetched [`ParquetMetaData`].
///
/// `intersect_row_selections` (parquet's `selection.rs`), which combines a page-pruning
/// selection with the deletion vector's selection, is a `from_fn` generator whose
/// `size_hint()` is `(0, None)`: for inputs of length `a` and `b`, its output can have up to
/// `a + b` selectors -- longer than either input. Bounding the page-pruning side of that sum
/// requires knowing how many selectors a page-index-derived selection could produce: at most
/// two per data page (one skip, one select, in the worst case of alternating page-level
/// pruning decisions), summed over every column of every row group.
///
/// Returns `0` when `metadata` carries no offset index (`metadata.offset_index()` is `None`).
/// This is provably safe, not merely a convenient default: page-index pruning cannot produce a
/// page-level selection without the offset index to locate pages by, so there are no
/// page-pruning selectors to bound. The offset index is fetched with
/// `PageIndexPolicy::Optional` from the same `FileMetadataCache` entry the scan's reader later
/// reopens (see [`attach_access_plan`]'s footer-fetch comment), so this function observes
/// exactly what the reader will see.
///
/// Uses checked arithmetic throughout for the same reason as [`admission_bound_bytes`].
fn page_selection_bound_selectors(metadata: &ParquetMetaData) -> Result<usize, ExecutionError> {
    let Some(offset_index) = metadata.offset_index() else {
        return Ok(0);
    };
    let overflow = || {
        GeneralError(
            "Deletion vector page-selection bound overflowed while summing offset-index page \
             locations"
                .to_string(),
        )
    };
    let mut total_page_locations = 0usize;
    for row_group in offset_index {
        for column in row_group {
            total_page_locations = total_page_locations
                .checked_add(column.page_locations().len())
                .ok_or_else(overflow)?;
        }
    }
    total_page_locations.checked_mul(2).ok_or_else(overflow)
}

/// Execution-memory-pool admission bound, in bytes, for one file's deletion-vector access
/// plan -- reserved *before* calling `build_access_plan` (see [`attach_access_plan`]'s
/// pre-reserve call site) to cover the larger of two peaks live at different points in the
/// plan's lifetime. In practice the reader-normalization peak below dominates the construction
/// peak unconditionally for any non-trivial input (`reader_peak_bytes(S, G) = (5S + 10G) *
/// size_of::<RowSelector>()` always exceeds `CONSTRUCTION_PEAK_FACTOR * S *
/// size_of::<RowSelector>() = 3S * size_of::<RowSelector>()` once `S >= 1`, since the `5S` term
/// alone already exceeds `3S`); the construction term is retained as a documented floor rather
/// than dropped, since it is cheap to compute and keeps this bound correct even if the reader's
/// growth factors ever shrink below construction's.
///
/// - **Construction peak** (`CONSTRUCTION_PEAK_FACTOR * S`, see that constant's doc comment):
///   live while `build_access_plan` builds the plan's `RowSelection`s. Construction's
///   transient allocations fully unwind before `build_access_plan` returns, so this peak never
///   overlaps the reader-normalization peak below.
/// - **Reader-normalization peak** (`reader_peak_bytes(S + page_bound_selectors,
///   num_row_groups)`, see that function): live later, once DataFusion's reader normalizes the
///   attached plan. `S = 2 * cardinality + num_row_groups` is the same conservative bound on
///   the plan's final retained selector count used for the construction peak -- it provably
///   bounds `R = total_selectors(&plan)` (`R <= S`, from `build_access_plan`'s
///   one-non-coalescing-selector-per-deleted-row worst case plus one boundary selector per row
///   group), so `S + page_bound_selectors` bounds `R` after page-index inflation the same way
///   `S` bounds `R` before it.
///
/// These two peaks never overlap in time, so `max` -- not `sum` -- is the correct combinator:
/// reserving their sum would over-reserve for no safety benefit.
///
/// Deliberately not clamped by the file's total row count here, unlike the reader-peak target
/// `attach_access_plan` resizes down to after construction (see that call site): `S`'s
/// `+ num_row_groups` boundary term is a worst-case padding margin that can legitimately exceed
/// the total row count for a small, heavily-deleted file, and admission sizing has no actual
/// retained-selector count yet to clamp against -- only after construction, once `R` is known,
/// is clamping to the total row count both meaningful and strictly tighter. Leaving this bound
/// unclamped only ever makes admission more conservative, never less safe.
///
/// Uses checked arithmetic throughout: a cardinality, row-group count, or page bound large
/// enough to overflow `usize` while computing this bound indicates a corrupted or malicious
/// descriptor, reported as a clean error rather than panicking.
fn admission_bound_bytes(
    cardinality: i64,
    num_row_groups: usize,
    page_bound_selectors: usize,
) -> Result<usize, ExecutionError> {
    let overflow = || {
        GeneralError(format!(
            "Deletion vector admission bound overflowed for cardinality {cardinality}, \
             {num_row_groups} row groups, and page bound {page_bound_selectors} selectors"
        ))
    };
    let cardinality_usize = usize::try_from(cardinality).map_err(|_| overflow())?;
    // S: the conservative bound on the plan's final *retained* selector count (what
    // `total_selectors(&plan)` cannot exceed) -- unchanged from the pre-existing
    // construction-only bound this function replaces.
    let s = cardinality_usize
        .checked_mul(2)
        .and_then(|doubled| doubled.checked_add(num_row_groups))
        .ok_or_else(overflow)?;

    let construction_bytes = s
        .checked_mul(size_of::<RowSelector>())
        .and_then(|bytes| bytes.checked_mul(CONSTRUCTION_PEAK_FACTOR))
        .ok_or_else(overflow)?;

    let s_plus_page = s.checked_add(page_bound_selectors).ok_or_else(overflow)?;
    let reader_bytes = reader_peak_bytes(s_plus_page, num_row_groups)?;

    Ok(construction_bytes.max(reader_bytes))
}

/// Upper bound on concurrent DV-blob and footer fetches per partition. Both
/// are small ranged reads, so a modest fan-out hides object-store latency
/// without flooding the store client.
const DV_FETCH_CONCURRENCY: usize = 8;

/// Called via `block_on` at plan-creation time on the executor task: DV blobs
/// are small ranged reads and footers are needed to learn row-group
/// boundaries. Files are fetched concurrently (bounded by
/// [`DV_FETCH_CONCURRENCY`]) with input order preserved. Footer fetches go
/// through the scan's shared FileMetadataCache, so the scan's subsequent open
/// of the same file is served from cache. That reuse relies on each input
/// [`PartitionedFile`] being returned as-is (only `with_extension` applied),
/// never rebuilt: the cache entry is keyed by this exact `object_meta` and the
/// scan later looks it up through the same struct.
///
/// Deliberately takes no object-store options map and imports no
/// store-construction helper: every [`DvScanFile`] arrives with its stores
/// already resolved by the caller (see its doc comment), so this async path
/// structurally cannot build an object store -- only `runtime_env` is still
/// threaded through, for the shared `FileMetadataCache` and (per file) the
/// execution `MemoryPool` each expanded access plan's row selectors are
/// reserved against -- see [`DvAccessPlanReservation`].
pub async fn attach_access_plans(
    runtime_env: Arc<RuntimeEnv>,
    files: Vec<DvScanFile>,
) -> Result<Vec<PartitionedFile>, ExecutionError> {
    futures::stream::iter(files)
        .map(|scan_file| attach_access_plan(Arc::clone(&runtime_env), scan_file))
        .buffered(DV_FETCH_CONCURRENCY)
        .try_collect()
        .await
}

/// Resolve one file's deletion vector into an attached [`ParquetAccessPlan`];
/// files without a DV pass through untouched.
async fn attach_access_plan(
    runtime_env: Arc<RuntimeEnv>,
    scan_file: DvScanFile,
) -> Result<PartitionedFile, ExecutionError> {
    let DvScanFile {
        file,
        file_path,
        dv,
        data_store,
        dv_store,
    } = scan_file;
    let dv = match dv {
        Some(dv) => dv,
        None => return Ok(file),
    };
    // Delta's canonical `DeletionVectorDescriptor.EMPTY`: inline storage, empty
    // payload, size 0, cardinality 0. Spark's reader returns all rows for it;
    // decoding would fail (the empty payload is too short for a magic
    // number), so pass the file through unchanged before attempting to read it.
    if dv.cardinality == 0 && dv.size_in_bytes == 0 {
        return Ok(file);
    }
    if dv.size_in_bytes < 0 {
        return Err(GeneralError(format!(
            "Deletion vector for {file_path} has negative size {}",
            dv.size_in_bytes
        )));
    }
    if dv.cardinality < 0 {
        return Err(GeneralError(format!(
            "Deletion vector for {file_path} has negative cardinality {}",
            dv.cardinality
        )));
    }

    let data: Vec<u8> = if let Some(inline) = dv.inline_data {
        inline
    } else if let Some(dv_path) = &dv.absolute_path {
        let offset = dv
            .offset
            .ok_or_else(|| GeneralError("On-disk deletion vector missing offset".into()))?;
        if offset < 0 {
            return Err(GeneralError(format!(
                "Deletion vector for {file_path} has negative offset {offset}"
            )));
        }
        let offset = offset as u64;
        // [i32 BE size][data: size_in_bytes][i32 BE crc]
        let framed_len = 4 + dv.size_in_bytes as u64 + 4;
        let (store, dv_store_path) = dv_store.ok_or_else(|| {
            GeneralError(format!(
                "Deletion vector for {file_path} has an absolute path but no pre-resolved object store"
            ))
        })?;
        let blob = store
            .get_range(&dv_store_path, offset..offset + framed_len)
            .await
            .map_err(|e| GeneralError(format!("Failed to read deletion vector {dv_path}: {e}")))?;
        unframe_dv_blob(&blob, dv.size_in_bytes as usize)?.to_vec()
    } else {
        return Err(GeneralError(
            "Deletion vector descriptor has neither inline data nor a path".into(),
        ));
    };
    let deleted = deserialize_dv_bitmap(&data)
        .map_err(|e| GeneralError(format!("Invalid deletion vector for {file_path}: {e}")))?;
    validate_cardinality(&file_path, dv.cardinality, &deleted)?;

    // Row-group boundaries come from the data file's footer, fetched through the scan's
    // shared FileMetadataCache with the page index loaded eagerly and the scan's metadata
    // size hint (mirroring EagerPageIndexReaderFactory): the one fetch here also serves the
    // subsequent data-file open, so DV files pay no extra footer round-trip. Keyed by
    // `file.object_meta`, the exact ObjectMeta the scan's reader factory will look up.
    let metadata_cache = runtime_env.cache_manager.get_file_metadata_cache();
    let metadata = DFParquetMetadata::new(data_store.as_ref(), &file.object_meta)
        .with_file_metadata_cache(Some(metadata_cache))
        .with_page_index_policy(Some(PageIndexPolicy::Optional))
        .with_metadata_size_hint(Some(crate::parquet::parquet_exec::METADATA_SIZE_HINT))
        .fetch_metadata()
        .await
        .map_err(|e| GeneralError(format!("Failed to read parquet footer of {file_path}: {e}")))?;
    let row_counts: Vec<i64> = metadata
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .collect();

    // Pre-reserve the admission bound *before* calling build_access_plan: this bound covers
    // both construction's own transient peak AND the larger peak DataFusion's reader hits
    // later while normalizing the attached plan (`create_initial_plan`'s deep clone plus
    // `into_overall_row_selection`'s combined RowSelection) -- see admission_bound_bytes and
    // reader_peak_bytes. Reserving first means a rejection happens before any large `Vec` is
    // allocated, not after -- see reader_peak_bytes's doc comment for the measured worst
    // cases. The error message names this as a construction-phase rejection (contains
    // "construct"), textually distinct from the steady-state message below, so callers/logs
    // can tell which phase failed.
    let page_bound_selectors = page_selection_bound_selectors(&metadata)?;
    let admission_bytes =
        admission_bound_bytes(dv.cardinality, row_counts.len(), page_bound_selectors)?;
    let reservation =
        MemoryConsumer::new("DeltaDeletionVectorAccessPlan").register(&runtime_env.memory_pool);
    reservation.try_grow(admission_bytes).map_err(|e| {
        GeneralError(format!(
            "Deletion vector access plan for {file_path} needs up to {admission_bytes} \
             bytes to construct, exceeding the execution memory pool: {e}"
        ))
    })?;

    let plan = build_access_plan(&row_counts, &deleted)
        .map_err(|e| GeneralError(format!("Invalid deletion vector for {file_path}: {e}")))?;

    // Shrink the reservation to the reader-lifecycle steady state now that construction's
    // transient peak has passed: the peak DataFusion's reader hits later while normalizing
    // this file's attached plan (see reader_peak_bytes), not merely the plan's own retained
    // selector bytes. `Rp_bound` bounds the selector count the reader will see after
    // page-index pruning inflates the deletion vector's own selection: this plan's actual
    // retained selector count (`R = total_selectors(&plan)`) plus `page_bound_selectors`,
    // clamped to the file's total row count -- a RowSelection can never carry more than one
    // selector per row, so `total_rows` independently bounds the reader's true selector count
    // regardless of how loose `R + page_bound_selectors` is.
    //
    // NEVER-GROWS PROOF (this call always shrinks -- never fails): `R <= S` (established by
    // `build_access_plan`'s worst case, the same invariant `admission_bound_bytes` relies on
    // for its own `S`), so `Rp_bound = min(R + page_bound_selectors, total_rows) <=
    // R + page_bound_selectors <= S + page_bound_selectors` -- the exact quantity
    // `admission_bound_bytes` fed into `reader_peak_bytes` when computing the reservation
    // already made above. `reader_peak_bytes` is monotone non-decreasing in its first
    // argument (all three terms of its sum scale with `selectors`, `num_row_groups`, or
    // both), so
    // `reader_peak_bytes(Rp_bound, num_row_groups) <=
    // reader_peak_bytes(S + page_bound_selectors, num_row_groups) <= admission_bytes`.
    // `try_resize` is still used (rather than the infallible `resize`) so a violation of that
    // invariant surfaces as a clean error instead of an internal panic.
    let selector_count = total_selectors(&plan);
    let total_rows: usize = row_counts
        .iter()
        .try_fold(0usize, |sum, &n| {
            usize::try_from(n).ok().and_then(|n| sum.checked_add(n))
        })
        .ok_or_else(|| {
            GeneralError(format!(
                "Deletion vector total row count negative or overflowed usize for {file_path}"
            ))
        })?;
    let reader_selector_bound = selector_count
        .checked_add(page_bound_selectors)
        .ok_or_else(|| {
            GeneralError(format!(
                "Deletion vector reader-peak bound overflowed for {file_path} while adding the \
                 page-index inflation term"
            ))
        })?
        .min(total_rows);
    let retained_bytes_bound = reader_peak_bytes(reader_selector_bound, row_counts.len())?;
    reservation.try_resize(retained_bytes_bound).map_err(|e| {
        GeneralError(format!(
            "Deletion vector access plan for {file_path} retains {selector_count} row \
             selectors, needing up to {retained_bytes_bound} bytes at the reader's \
             normalization peak, exceeding the execution memory pool: {e}"
        ))
    })?;

    // Keyed by concrete type: the parquet opener looks up
    // `extensions.get::<ParquetAccessPlan>()`, so the plan must be stored
    // as ParquetAccessPlan itself, NOT wrapped in an Arc (which would key
    // it as Arc<ParquetAccessPlan> and silently skip DV application). The
    // reservation occupies its own slot (`DvAccessPlanReservation`, keyed
    // separately by its own concrete type) alongside it -- `extensions` is
    // a multi-slot, type-keyed map (`datafusion_common::extensions`), not a
    // single-slot table, so the two coexist without conflict and are
    // dropped together.
    Ok(file
        .with_extension(plan)
        .with_extension(DvAccessPlanReservation(reservation)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::ParquetMetaDataReader;
    use parquet::file::properties::WriterProperties;

    /// Mirror the pre-resolution `plan_delta_spark_scan` does before entering
    /// `attach_access_plans`: resolve `url`'s object store and within-store
    /// path via the same helper the production code path uses, outside any
    /// async runtime, exactly as `DvScanFile` requires.
    fn resolve_store(runtime_env: &Arc<RuntimeEnv>, url: &str) -> (Arc<dyn ObjectStore>, Path) {
        use crate::parquet::parquet_support::prepare_object_store_with_configs;
        let (store_url, path) = prepare_object_store_with_configs(
            Arc::clone(runtime_env),
            url.to_string(),
            &std::collections::HashMap::new(),
        )
        .unwrap();
        let store = runtime_env.object_store(&store_url).unwrap();
        (store, path)
    }

    /// Build a one-column (`id: Int64`), `num_rows`-row batch (values `0..num_rows`), shared by
    /// every parquet-writing helper below.
    fn sequential_int64_batch(num_rows: i64) -> (Arc<Schema>, RecordBatch) {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field};
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..num_rows))],
        )
        .unwrap();
        (schema, batch)
    }

    /// Write a one-column parquet file with rows 0..num_rows using explicit `props`; returns
    /// its size.
    fn write_parquet_with_properties(
        path: &std::path::Path,
        num_rows: i64,
        props: WriterProperties,
    ) -> i64 {
        let (schema, batch) = sequential_int64_batch(num_rows);
        let out = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(out, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        std::fs::metadata(path).unwrap().len() as i64
    }

    /// Write a one-column parquet file with rows 0..num_rows; returns its size.
    fn write_parquet(path: &std::path::Path, num_rows: i64) -> i64 {
        write_parquet_with_properties(path, num_rows, WriterProperties::default())
    }

    /// Write a two-row-group parquet file (`2 * rows_per_group` total rows, split evenly via
    /// an explicit `max_row_group_size`); returns its size. Used by tests exercising
    /// `into_overall_row_selection`'s per-`Scan`-group boundary-selector term.
    fn write_two_row_groups(path: &std::path::Path, rows_per_group: i64) -> i64 {
        write_parquet_with_properties(
            path,
            rows_per_group * 2,
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(rows_per_group as usize))
                .build(),
        )
    }

    /// Read `path`'s full [`ParquetMetaData`], including the page index, exactly as this
    /// module's own footer fetch does (`PageIndexPolicy::Optional`) -- synchronously, for test
    /// setup that needs the real metadata before entering `attach_access_plans`' async path.
    fn read_metadata_with_page_index(path: &std::path::Path) -> ParquetMetaData {
        let file = std::fs::File::open(path).unwrap();
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&file)
            .unwrap()
    }

    /// End-to-end over local files: inline and on-disk DVs resolve to attached
    /// access plans, non-DV files pass through untouched, and the output keeps
    /// the input's file order (which concurrent fetching must preserve).
    #[tokio::test]
    async fn attach_access_plans_resolves_dvs_and_preserves_order() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        let inline_deleted: RoaringTreemap = [0u64].into_iter().collect();
        let inline_data = portable_bytes(&inline_deleted);

        // On-disk DV file: 1 version byte, then the framed blob at offset 1.
        let ondisk_deleted: RoaringTreemap = [1u64].into_iter().collect();
        let ondisk_data = portable_bytes(&ondisk_deleted);
        let dv_file = dir.join("dv.bin");
        let mut dv_bytes = vec![1u8];
        dv_bytes.extend(frame(&ondisk_data));
        std::fs::write(&dv_file, &dv_bytes).unwrap();

        let dv_for = |name: &str| match name {
            "f0" => Some(DeltaSparkDvDescriptor {
                storage_type: "i".to_string(),
                absolute_path: None,
                inline_data: Some(inline_data.clone()),
                offset: None,
                size_in_bytes: inline_data.len() as i32,
                cardinality: 1,
            }),
            "f2" => Some(DeltaSparkDvDescriptor {
                storage_type: "p".to_string(),
                absolute_path: Some(format!("file://{}", dv_file.display())),
                inline_data: None,
                offset: Some(1),
                size_in_bytes: ondisk_data.len() as i32,
                cardinality: 1,
            }),
            // Delta's `DeletionVectorDescriptor.EMPTY`: inline storage, empty
            // payload, size 0, cardinality 0. Must pass through unchanged
            // without attempting to decode the (empty) payload.
            "f4" => Some(DeltaSparkDvDescriptor {
                storage_type: "i".to_string(),
                absolute_path: None,
                inline_data: Some(vec![]),
                offset: None,
                size_in_bytes: 0,
                cardinality: 0,
            }),
            _ => None,
        };

        let runtime_env = Arc::new(RuntimeEnv::default());
        let names = ["f0", "f1", "f2", "f3", "f4"];
        let files: Vec<DvScanFile> = names
            .iter()
            .map(|name| {
                let path = dir.join(format!("{name}.parquet"));
                let size = write_parquet(&path, 10);
                let file_path = format!("file://{}", path.display());
                let (data_store, _) = resolve_store(&runtime_env, &file_path);
                let dv = dv_for(name);
                let dv_store = dv
                    .as_ref()
                    .and_then(|d| d.absolute_path.as_deref())
                    .map(|dv_path| resolve_store(&runtime_env, dv_path));
                DvScanFile {
                    file: PartitionedFile::new(path.display().to_string(), size as u64),
                    file_path,
                    dv,
                    data_store,
                    dv_store,
                }
            })
            .collect();

        let out = attach_access_plans(Arc::clone(&runtime_env), files)
            .await
            .unwrap();

        assert_eq!(out.len(), names.len());
        for (file, name) in out.iter().zip(names) {
            assert!(
                file.object_meta
                    .location
                    .as_ref()
                    .ends_with(&format!("{name}.parquet")),
                "output order broken: expected {name}, got {}",
                file.object_meta.location
            );
            let plan = file.extensions.get::<ParquetAccessPlan>();
            match name {
                "f0" | "f2" => {
                    let plan = plan.unwrap_or_else(|| panic!("{name} should carry an access plan"));
                    let skipped_row = if name == "f0" { 1 } else { 2 };
                    match &plan.inner()[0] {
                        RowGroupAccess::Selection(sel) => {
                            let selectors: Vec<RowSelector> = sel.clone().into();
                            let expected = if skipped_row == 1 {
                                vec![RowSelector::skip(1), RowSelector::select(9)]
                            } else {
                                vec![
                                    RowSelector::select(1),
                                    RowSelector::skip(1),
                                    RowSelector::select(8),
                                ]
                            };
                            assert_eq!(selectors, expected, "{name}");
                        }
                        other => panic!("{name}: expected selection, got {other:?}"),
                    }
                }
                _ => assert!(plan.is_none(), "{name} should have no access plan"),
            }
        }

        // Footer reads must go through the shared FileMetadataCache so the scan's
        // subsequent open of the same file is served from cache instead of paying a
        // second footer round-trip. Files without a DV read no footer at all.
        let cache = runtime_env.cache_manager.get_file_metadata_cache();
        for (file, name) in out.iter().zip(names) {
            let cached = cache.get(&file.object_meta.location);
            match name {
                "f0" | "f2" => assert!(
                    cached.is_some(),
                    "{name}: DV footer read should populate the shared metadata cache"
                ),
                _ => assert!(
                    cached.is_none(),
                    "{name}: no-DV file should not have fetched a footer"
                ),
            }
        }
    }

    /// Serialize a treemap in Delta's portable RoaringBitmapArray format
    /// (magic + RoaringTreemap wire form).
    fn portable_bytes(deleted: &RoaringTreemap) -> Vec<u8> {
        let mut data = PORTABLE_MAGIC.to_le_bytes().to_vec();
        deleted.serialize_into(&mut data).unwrap();
        data
    }

    /// Serialize values in Delta's "native" RoaringBitmapArray format.
    fn native_bytes(values: &[u64]) -> Vec<u8> {
        use std::collections::BTreeMap;
        let mut by_key: BTreeMap<u32, RoaringBitmap> = BTreeMap::new();
        for v in values {
            by_key
                .entry((v >> 32) as u32)
                .or_default()
                .insert(*v as u32);
        }
        let max_key = by_key.keys().max().copied().unwrap_or(0);
        let mut data = NATIVE_MAGIC.to_le_bytes().to_vec();
        data.extend(((max_key + 1) as i32).to_le_bytes());
        for key in 0..=max_key {
            let bitmap = by_key.remove(&key).unwrap_or_default();
            let mut bytes = Vec::new();
            bitmap.serialize_into(&mut bytes).unwrap();
            data.extend((bytes.len() as i32).to_le_bytes());
            data.extend(bytes);
        }
        data
    }

    fn frame(data: &[u8]) -> Vec<u8> {
        let mut blob = (data.len() as i32).to_be_bytes().to_vec();
        blob.extend_from_slice(data);
        blob.extend((crc32fast::hash(data) as i32).to_be_bytes());
        blob
    }

    #[test]
    fn portable_roundtrip_through_framing() {
        let deleted: RoaringTreemap = [1u64, 5, 6, 7, 1000, (3u64 << 32) + 42]
            .into_iter()
            .collect();
        let blob = frame(&portable_bytes(&deleted));
        let data = unframe_dv_blob(&blob, blob.len() - 8).unwrap();
        let decoded = deserialize_dv_bitmap(data).unwrap();
        assert_eq!(decoded, deleted);
    }

    #[test]
    fn native_format_decodes() {
        let values = [0u64, 2, 3, 100, (1u64 << 32) + 7];
        let decoded = deserialize_dv_bitmap(&native_bytes(&values)).unwrap();
        let expected: RoaringTreemap = values.into_iter().collect();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn framing_rejects_bad_size_and_crc() {
        let deleted: RoaringTreemap = [1u64, 2].into_iter().collect();
        let blob = frame(&portable_bytes(&deleted));
        let err = unframe_dv_blob(&blob, 3).unwrap_err();
        assert!(format!("{err}").contains("size mismatch"));

        let mut corrupted = blob.clone();
        let mid = corrupted.len() / 2;
        corrupted[mid] ^= 0xFF;
        let err = unframe_dv_blob(&corrupted, blob.len() - 8).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("checksum") || msg.contains("size mismatch"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn cardinality_mismatch_is_rejected() {
        let deleted: RoaringTreemap = [1u64].into_iter().collect();
        let bytes = portable_bytes(&deleted);
        let decoded = deserialize_dv_bitmap(&bytes).unwrap();

        let err = validate_cardinality("f.parquet", 2, &decoded).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("cardinality"), "unexpected: {msg}");

        validate_cardinality("f.parquet", 1, &decoded).unwrap();
    }

    #[test]
    fn access_plan_scan_skip_and_selection() {
        // Three row groups of 10 rows: group 0 untouched, group 1 fully
        // deleted, group 2 rows 21..24 deleted (local 1..4).
        let deleted: RoaringTreemap = (10u64..20).chain(21u64..24).collect();
        let plan = build_access_plan(&[10, 10, 10], &deleted).unwrap();
        assert_eq!(&plan.inner()[0], &RowGroupAccess::Scan);
        assert_eq!(&plan.inner()[1], &RowGroupAccess::Skip);
        match &plan.inner()[2] {
            RowGroupAccess::Selection(sel) => {
                let selectors: Vec<RowSelector> = sel.clone().into();
                assert_eq!(
                    selectors,
                    vec![
                        RowSelector::select(1),
                        RowSelector::skip(3),
                        RowSelector::select(6)
                    ]
                );
            }
            other => panic!("expected selection, got {other:?}"),
        }
    }

    #[test]
    fn access_plan_rejects_out_of_range_rows() {
        let deleted: RoaringTreemap = [5u64, 25].into_iter().collect();
        let err = build_access_plan(&[10, 10], &deleted).unwrap_err();
        assert!(format!("{err}").contains("only has 20 rows"));
    }

    #[test]
    fn access_plan_selects_complement_row_count() {
        // Random-ish pattern in one 100-row group: every 7th row deleted.
        let deleted: RoaringTreemap = (0u64..100).filter(|i| i % 7 == 0).collect();
        let plan = build_access_plan(&[100], &deleted).unwrap();
        match &plan.inner()[0] {
            RowGroupAccess::Selection(sel) => {
                let selected: usize = sel.iter().filter(|s| !s.skip).map(|s| s.row_count).sum();
                let skipped: usize = sel.iter().filter(|s| s.skip).map(|s| s.row_count).sum();
                assert_eq!(selected + skipped, 100);
                assert_eq!(skipped, deleted.len() as usize);
            }
            other => panic!("expected selection, got {other:?}"),
        }
    }

    /// The maintainer's confirmed worst case: deleting every even row leaves
    /// no adjacent skips or selects to merge, so `build_access_plan` emits
    /// one non-coalescing `RowSelector` per row of the group.
    fn alternating_deleted(num_rows: u64) -> RoaringTreemap {
        (0..num_rows).step_by(2).collect()
    }

    #[test]
    fn total_selectors_counts_one_per_row_for_alternating_bitmap() {
        let deleted = alternating_deleted(1024);
        let plan = build_access_plan(&[1024], &deleted).unwrap();
        assert_eq!(total_selectors(&plan), 1024);
    }

    #[test]
    fn total_selectors_ignores_scan_and_skip_row_groups() {
        // Group 0 untouched (Scan), group 1 fully deleted (Skip): neither
        // carries a RowSelection, so both must contribute zero selectors.
        let deleted: RoaringTreemap = (10u64..20).collect();
        let plan = build_access_plan(&[10, 10], &deleted).unwrap();
        assert_eq!(total_selectors(&plan), 0);
    }

    /// Writes one file's on-disk parquet data for a full-file, alternating-bitmap deletion
    /// vector, returning its path, byte size, and deleted-row bitmap so callers needing the
    /// file's on-disk metadata (to size a memory pool exactly, or to replay the real reader
    /// path) can inspect it before building a [`DvScanFile`] from it.
    fn write_alternating_parquet(
        dir: &std::path::Path,
        num_rows: i64,
    ) -> (std::path::PathBuf, i64, RoaringTreemap) {
        let deleted = alternating_deleted(num_rows as u64);
        let path = dir.join("alternating.parquet");
        let size = write_parquet(&path, num_rows);
        (path, size, deleted)
    }

    /// Builds a [`DvScanFile`] with an inline deletion vector for an already-written parquet
    /// file at `path`.
    fn dv_scan_file_for_alternating(
        runtime_env: &Arc<RuntimeEnv>,
        path: &std::path::Path,
        size: i64,
        deleted: &RoaringTreemap,
    ) -> DvScanFile {
        let inline_data = portable_bytes(deleted);
        let file_path = format!("file://{}", path.display());
        let (data_store, _) = resolve_store(runtime_env, &file_path);
        DvScanFile {
            file: PartitionedFile::new(path.display().to_string(), size as u64),
            file_path,
            dv: Some(DeltaSparkDvDescriptor {
                storage_type: "i".to_string(),
                absolute_path: None,
                inline_data: Some(inline_data.clone()),
                offset: None,
                size_in_bytes: inline_data.len() as i32,
                cardinality: deleted.len() as i64,
            }),
            data_store,
            dv_store: None,
        }
    }

    /// Builds one file's [`DvScanFile`] carrying an inline, alternating-bitmap
    /// deletion vector over `num_rows` -- enough retained selectors to make
    /// the reservation's byte count non-trivial without needing an on-disk DV
    /// file. Used by the memory-accounting tests below.
    fn alternating_dv_scan_file(
        runtime_env: &Arc<RuntimeEnv>,
        dir: &std::path::Path,
        num_rows: i64,
    ) -> DvScanFile {
        let (path, size, deleted) = write_alternating_parquet(dir, num_rows);
        dv_scan_file_for_alternating(runtime_env, &path, size, &deleted)
    }

    /// A pool too small for even one `RowSelector` must reject the file's
    /// access plan with a clean, file-naming error instead of the caller
    /// materializing the selectors unbounded and risking an executor OOM.
    #[tokio::test]
    async fn attach_access_plans_rejects_oversized_dv_against_tiny_pool() {
        let tmp = tempfile::tempdir().unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()
                .unwrap(),
        );
        let scan_file = alternating_dv_scan_file(&runtime_env, tmp.path(), 1024);

        let err = attach_access_plans(Arc::clone(&runtime_env), vec![scan_file])
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("alternating.parquet"),
            "error should name the file: {msg}"
        );
        assert!(
            msg.contains("Resources exhausted") || msg.contains("exceeding"),
            "error should surface pool exhaustion: {msg}"
        );
        assert!(
            msg.to_lowercase().contains("construct"),
            "a pool too small even for the construction-phase bound should fail with a \
             construction-phase message: {msg}"
        );
        assert_eq!(
            pool.reserved(),
            0,
            "a rejected reservation must not leak bytes into the pool"
        );
    }

    /// A pool with room for the plan succeeds, reserves exactly the reader-lifecycle peak
    /// bound (`reader_peak_bytes`, never a hardcoded constant) once construction's transient
    /// peak has passed, attaches the reservation alongside the access plan, and releases it
    /// back to the pool when the returned files are dropped.
    #[tokio::test]
    async fn attach_access_plans_reserves_and_releases_selector_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1_000_000));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()
                .unwrap(),
        );
        let scan_file = alternating_dv_scan_file(&runtime_env, tmp.path(), 1024);
        // A full-file alternating bitmap retains exactly one selector per row (1024), which
        // equals the file's total row count -- so the reader-peak clamp collapses to exactly
        // this file's retained selector count regardless of its real page-index bound.
        let expected_bytes = reader_peak_bytes(1024, 1).unwrap();

        let out = attach_access_plans(Arc::clone(&runtime_env), vec![scan_file])
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            pool.reserved(),
            expected_bytes,
            "plan bytes should be reserved against the pool"
        );

        let reservation = out[0]
            .extensions
            .get::<DvAccessPlanReservation>()
            .expect("reservation extension should be attached alongside the access plan");
        assert_eq!(reservation.0.size(), expected_bytes);

        drop(out);
        assert_eq!(
            pool.reserved(),
            0,
            "dropping the files should release the reservation back to the pool"
        );
    }

    /// Multi-file variant of `attach_access_plans_reserves_and_releases_selector_bytes`: two
    /// files with distinct alternating deletion vectors (different row counts, so distinct
    /// selector byte counts) must have their reservations summed in the pool while the returned
    /// files are alive, and released in full once every returned file is dropped.
    #[tokio::test]
    async fn attach_access_plans_reserves_and_releases_selector_bytes_for_multiple_files() {
        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10_000_000));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()
                .unwrap(),
        );
        let scan_file_a = alternating_dv_scan_file(&runtime_env, tmp_a.path(), 1024);
        let scan_file_b = alternating_dv_scan_file(&runtime_env, tmp_b.path(), 512);
        // Per-file sum: each full-file alternating bitmap's reader-peak bound is independent of
        // the other file's row count (unlike a naive shared-factor formula would suggest).
        let expected_bytes =
            reader_peak_bytes(1024, 1).unwrap() + reader_peak_bytes(512, 1).unwrap();

        let out = attach_access_plans(Arc::clone(&runtime_env), vec![scan_file_a, scan_file_b])
            .await
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(
            pool.reserved(),
            expected_bytes,
            "reserved bytes should be the SUM of both files' selector bytes while the files \
             are alive"
        );

        drop(out);
        assert_eq!(
            pool.reserved(),
            0,
            "dropping the files should release every file's reservation back to the pool"
        );
    }

    /// A pool sized to fit only the larger of two files' selector bytes must reject the whole
    /// batch -- regardless of which file's reservation attempt happens to run first under
    /// `buffered`'s bounded concurrency -- and must not leave an earlier, transiently successful
    /// file's reservation stranded in the pool once the batch's error propagates: `try_collect`
    /// drops the whole in-flight `Vec<PartitionedFile>` (including any already-resolved file's
    /// attached `DvAccessPlanReservation`) as soon as any one file errors.
    #[tokio::test]
    async fn attach_access_plans_rejects_multi_file_batch_without_leaking_earlier_reservation() {
        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();

        // Write file A up front (rather than via `alternating_dv_scan_file`) so its on-disk
        // metadata -- and thus its exact page-selection bound -- is available here, before the
        // pool exists, to size `pool_capacity` using the exact same admission bound the
        // production code computes.
        let (path_a, size_a, deleted_a) = write_alternating_parquet(tmp_a.path(), 1024);
        let metadata_a = read_metadata_with_page_index(&path_a);
        let page_bound_a = page_selection_bound_selectors(&metadata_a).unwrap();

        // Sized to exactly fit the larger file's (1024 rows, cardinality 512) admission bound
        // alone -- derived, never hardcoded, so it tracks CONSTRUCTION_PEAK_FACTOR,
        // reader_peak_bytes, and size_of::<RowSelector>() across changes. Whichever of the two
        // files reserves first (the FIRST reservation each file makes) fits alone, but the
        // combined requirement (both files' admission bounds together) never does, so the
        // batch fails no matter the scheduling order under `buffered`'s bounded concurrency.
        let pool_capacity = admission_bound_bytes(512, 1, page_bound_a).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_capacity));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()
                .unwrap(),
        );
        let scan_file_a = dv_scan_file_for_alternating(&runtime_env, &path_a, size_a, &deleted_a);
        let scan_file_b = alternating_dv_scan_file(&runtime_env, tmp_b.path(), 512);

        let err = attach_access_plans(Arc::clone(&runtime_env), vec![scan_file_a, scan_file_b])
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Resources exhausted") || msg.contains("exceeding"),
            "error should surface pool exhaustion: {msg}"
        );
        assert_eq!(
            pool.reserved(),
            0,
            "a rejected multi-file batch must not leak bytes from any file's reservation, \
             including one that transiently succeeded before the batch as a whole failed"
        );
    }

    /// A pool sized to fit only the STEADY-STATE reservation (`reader_peak_bytes` at this
    /// file's actual retained selector count) but not the larger admission bound must still be
    /// rejected: the pre-reserve step runs before `build_access_plan`, so undersizing only for
    /// steady state is not enough to admit a file whose transient admission-phase peak the pool
    /// cannot actually hold. The error must be textually distinguishable from a steady-state
    /// rejection (contains "construct").
    #[tokio::test]
    async fn construction_bound_rejects_before_building_the_plan() {
        let num_rows = 1024i64;
        let cardinality = 512i64; // alternating_deleted(1024).len()
        let num_row_groups = 1usize;

        let tmp = tempfile::tempdir().unwrap();
        let (path, size, deleted) = write_alternating_parquet(tmp.path(), num_rows);
        let metadata = read_metadata_with_page_index(&path);
        let page_bound = page_selection_bound_selectors(&metadata).unwrap();

        // A full-file alternating bitmap's actual retained selector count equals its total row
        // count, so its reader-peak-clamped steady state is exactly reader_peak_bytes(num_rows,
        // 1). This is strictly smaller than the admission bound below: S = 2 * cardinality +
        // num_row_groups (1025) is strictly larger than num_rows == R (1024) for this file
        // (S's one-selector row-group boundary padding), and the file's real page bound `P`
        // (from its default-written offset index, `page_bound` above) further inflates the
        // admission side via `S + P` -- so the true gap is
        // `reader_peak_bytes(S + page_bound, 1) - reader_peak_bytes(num_rows, 1) ==
        // 5 * (S + page_bound - num_rows) * size_of::<RowSelector>() ==
        // 5 * (1 + page_bound) * size_of::<RowSelector>()`, not merely the 1-selector S/R
        // difference alone. Deliberately near-tight, and NOT hardcoded to a specific byte
        // count: `page_bound` is measured from the real file, not assumed to be zero.
        let steady_state_bytes = reader_peak_bytes(num_rows as usize, num_row_groups).unwrap();
        let admission_bytes =
            admission_bound_bytes(cardinality, num_row_groups, page_bound).unwrap();
        assert!(
            steady_state_bytes < admission_bytes,
            "test setup invariant: steady state ({steady_state_bytes}) must be smaller than the \
             admission bound ({admission_bytes}) for this rejection to be meaningful"
        );

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(steady_state_bytes));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()
                .unwrap(),
        );
        let scan_file = dv_scan_file_for_alternating(&runtime_env, &path, size, &deleted);

        let err = attach_access_plans(Arc::clone(&runtime_env), vec![scan_file])
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("construct"),
            "rejection at the pre-reserve step should carry a construction-phase message: {msg}"
        );
        assert_eq!(
            pool.reserved(),
            0,
            "a rejected construction-phase reservation must not leak bytes into the pool"
        );
    }

    /// Directly verifies the reader-peak invariant end to end: after `attach_access_plans`
    /// completes, the attached reservation's steady-state size must equal `reader_peak_bytes`
    /// evaluated at this file's actual retained selector count and row-group count --
    /// computed independently here via `build_access_plan`/`total_selectors`, never hardcoded
    /// -- not the larger admission bound that was reserved up front.
    #[tokio::test]
    async fn steady_state_reservation_covers_reader_peak() {
        let tmp = tempfile::tempdir().unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10_000_000));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()
                .unwrap(),
        );
        let num_rows = 300i64;
        let deleted = alternating_deleted(num_rows as u64);
        let scan_file = alternating_dv_scan_file(&runtime_env, tmp.path(), num_rows);

        let out = attach_access_plans(Arc::clone(&runtime_env), vec![scan_file])
            .await
            .unwrap();

        let plan = build_access_plan(&[num_rows], &deleted).unwrap();
        let expected_bytes = reader_peak_bytes(total_selectors(&plan), 1).unwrap();

        let reservation = out[0]
            .extensions
            .get::<DvAccessPlanReservation>()
            .expect("reservation extension should be attached alongside the access plan");
        assert_eq!(reservation.0.size(), expected_bytes);
        assert_eq!(pool.reserved(), expected_bytes);
    }

    #[test]
    fn admission_bound_bytes_derives_from_cardinality_and_row_groups() {
        let sel = size_of::<RowSelector>();

        // Zero cardinality, zero page bound: the reader term dominates in both cases below
        // (reader_peak_bytes(S, G) = (5S + 10G) * sel always exceeds CONSTRUCTION_PEAK_FACTOR
        // * S * sel = 3S * sel for S >= 1, since 5S alone already exceeds 3S).
        assert_eq!(
            admission_bound_bytes(0, 1, 0).unwrap(),
            (CONSTRUCTION_PEAK_FACTOR * sel).max(reader_peak_bytes(1, 1).unwrap())
        );
        // Many row groups, still zero cardinality.
        assert_eq!(
            admission_bound_bytes(0, 1_000, 0).unwrap(),
            (CONSTRUCTION_PEAK_FACTOR * 1_000 * sel).max(reader_peak_bytes(1_000, 1_000).unwrap())
        );
        // Typical case: cardinality dominates over a single row group, with a non-zero page
        // bound feeding only the reader-normalization term.
        let cardinality = 512usize;
        let num_row_groups = 1usize;
        let page_bound = 7usize;
        let s = 2 * cardinality + num_row_groups;
        assert_eq!(
            admission_bound_bytes(cardinality as i64, num_row_groups, page_bound).unwrap(),
            (CONSTRUCTION_PEAK_FACTOR * s * sel)
                .max(reader_peak_bytes(s + page_bound, num_row_groups).unwrap())
        );

        // Overflow anywhere in the derivation must produce a clean GeneralError, never a panic.
        let err = admission_bound_bytes(0, usize::MAX, 0).unwrap_err();
        assert!(matches!(err, GeneralError(_)), "unexpected error: {err:?}");
    }

    /// Replays DataFusion 54.1's REAL reader-normalization path (not a reimplementation of
    /// it): clones the attached plan exactly as `create_initial_plan` does, calls the actual,
    /// public `ParquetAccessPlan::into_overall_row_selection` DataFusion will call from
    /// `build_stream`, and recovers the resulting `RowSelection`'s TRUE backing `Vec` capacity
    /// (not its length) -- the same quantity `reader_peak_bytes` bounds. Exercising the real
    /// dependency rather than a model of it means this test keeps working (or fails loudly)
    /// across future `datafusion`/`parquet` upgrades that change either crate's growth
    /// strategy.
    ///
    /// This test (and `..._with_a_scan_row_group` below) covers the NO-page-index-pruning
    /// path only: neither ever calls `scan_selection` on the clone, so `retained_selectors`
    /// (from `total_selectors`, i.e. length, not capacity) is exact for BOTH the attached
    /// original and the clone here -- see `reader_path_peak_fits_the_reservation_with_page_pruning`
    /// for the case where the clone's own capacity can exceed its length. Also note the
    /// assertion below is purely arithmetic: `attached_plan`, `cloned_plan`, and `combined`
    /// are not necessarily all simultaneously resident in this process's memory at one program
    /// point (Rust may reuse `cloned_plan`'s allocation once `into_overall_row_selection`
    /// consumes it, before `combined` is bound) -- this test checks that the byte counts the
    /// real dependency reports add up within the reservation, not that three buffers are
    /// observed live at once via a profiler.
    #[tokio::test]
    async fn reader_path_peak_fits_the_reservation() {
        let tmp = tempfile::tempdir().unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10_000_000));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()
                .unwrap(),
        );
        let (path, size, deleted) = write_alternating_parquet(tmp.path(), 1024);
        let scan_file = dv_scan_file_for_alternating(&runtime_env, &path, size, &deleted);

        let out = attach_access_plans(Arc::clone(&runtime_env), vec![scan_file])
            .await
            .unwrap();
        let attached_plan = out[0]
            .extensions
            .get::<ParquetAccessPlan>()
            .expect("attach_access_plans should have attached a plan")
            .clone();
        let reservation = out[0]
            .extensions
            .get::<DvAccessPlanReservation>()
            .expect("reservation extension should be attached alongside the access plan");

        let retained_selectors = total_selectors(&attached_plan);

        // Mirror create_initial_plan's deep clone: the original (still reachable via
        // `out[0]`'s extensions) and the clone are live at once, exactly like the real reader.
        let cloned_plan = attached_plan.clone();
        let metadata = read_metadata_with_page_index(&path);
        let combined = cloned_plan
            .into_overall_row_selection(metadata.row_groups())
            .unwrap()
            .expect("a fully-alternating file should produce a combined RowSelection");
        // `From<RowSelection> for Vec<RowSelector>` moves the RowSelection's backing Vec, so
        // this preserves its TRUE allocated capacity -- not merely its length.
        let combined_selectors: Vec<RowSelector> = combined.into();
        let combined_capacity = combined_selectors.capacity();

        let peak_bytes = (retained_selectors + retained_selectors + combined_capacity)
            * size_of::<RowSelector>();
        assert!(
            peak_bytes <= reservation.0.size(),
            "the real DataFusion/parquet reader path's peak ({peak_bytes} bytes: \
             {retained_selectors} retained selectors x 2 live plan copies + \
             {combined_capacity} combined-selection Vec capacity) must fit the reservation \
             ({} bytes)",
            reservation.0.size()
        );
    }

    /// Same replay as `reader_path_peak_fits_the_reservation`, but with a two-row-group file
    /// where only the first group has any deletions -- the second stays `RowGroupAccess::Scan`
    /// (no `RowSelection`), exercising `into_overall_row_selection`'s one-`select`-per-
    /// `Scan`-group term that a naive `k * total_selectors` bound would miss entirely. Like
    /// that test, this one never calls `scan_selection` on the clone, so it exercises the
    /// NO-page-index-pruning path only (clone length == clone capacity here); see the doc
    /// comment there for why `retained_selectors` is exact in this test and why the assertion
    /// below is arithmetic rather than a live-memory observation.
    #[tokio::test]
    async fn reader_path_peak_fits_the_reservation_with_a_scan_row_group() {
        let tmp = tempfile::tempdir().unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10_000_000));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()
                .unwrap(),
        );

        // Two 500-row groups: only the first has any deletions, so the second stays a `Scan`
        // row group in the resulting ParquetAccessPlan.
        let rows_per_group = 500i64;
        let deleted: RoaringTreemap = alternating_deleted(rows_per_group as u64);
        let path = tmp.path().join("two_groups.parquet");
        let size = write_two_row_groups(&path, rows_per_group);
        let scan_file = dv_scan_file_for_alternating(&runtime_env, &path, size, &deleted);

        let out = attach_access_plans(Arc::clone(&runtime_env), vec![scan_file])
            .await
            .unwrap();
        let attached_plan = out[0]
            .extensions
            .get::<ParquetAccessPlan>()
            .expect("attach_access_plans should have attached a plan")
            .clone();
        assert_eq!(
            &attached_plan.inner()[1],
            &RowGroupAccess::Scan,
            "the second, untouched row group must stay Scan"
        );
        let reservation = out[0]
            .extensions
            .get::<DvAccessPlanReservation>()
            .expect("reservation extension should be attached alongside the access plan");

        let retained_selectors = total_selectors(&attached_plan);
        let cloned_plan = attached_plan.clone();
        let metadata = read_metadata_with_page_index(&path);
        let combined = cloned_plan
            .into_overall_row_selection(metadata.row_groups())
            .unwrap()
            .expect("a plan with a Selection row group should produce a combined RowSelection");
        let combined_selectors: Vec<RowSelector> = combined.into();
        let combined_capacity = combined_selectors.capacity();

        let peak_bytes = (retained_selectors + retained_selectors + combined_capacity)
            * size_of::<RowSelector>();
        assert!(
            peak_bytes <= reservation.0.size(),
            "the real reader path's peak with a Scan row group present ({peak_bytes} bytes) \
             must fit the reservation ({} bytes)",
            reservation.0.size()
        );
    }

    /// Replays the page-index-pruning path the HIGH-severity review finding measured: clones
    /// the attached plan (mirroring `create_initial_plan`), then intersects the clone's
    /// row-group `Selection` with a synthetic, all-selecting page `RowSelection` via
    /// `ParquetAccessPlan::scan_selection` -- the EXACT call `access_plan.rs`'s row-group
    /// intersection makes when `PagePruningAccessPlanFilter` fires
    /// (`existing_selection.intersection(&page_derived)` -> `RowSelection::intersection` ->
    /// `intersect_row_selections`, ANOTHER `from_fn` generator with `size_hint() == (0,
    /// None)`). The synthetic selection selects every row of the row group (a no-op filter --
    /// it changes nothing about which rows are scanned), included ONLY to drive the clone
    /// through the SAME capacity-inflating intersection path real page pruning takes, so the
    /// recovered capacity reflects the real dependency's growth strategy, not a model of it.
    /// `num_rows` is chosen just above a power of two (mirroring the review's own
    /// `1,048,577`-row example at test scale) so the intersection's `next_power_of_two`
    /// capacity jump is real and visible, not accidentally exact.
    ///
    /// Recovers BOTH the intersected clone's TRUE capacity and the subsequent combined
    /// selection's TRUE capacity (each via `into_inner()` / pattern-matching by value and
    /// `Into<Vec<RowSelector>>`, never `.clone()` -- cloning a `RowSelection` resets capacity
    /// to length, since `Vec::clone` allocates exactly `with_capacity(len)`), and asserts
    /// `attached_len + clone_capacity + combined_capacity` fits the reservation. Unlike
    /// `reader_path_peak_fits_the_reservation`, this test does NOT model the clone as exact --
    /// it is the one that would have caught the original under-count.
    #[tokio::test]
    async fn reader_path_peak_fits_the_reservation_with_page_pruning() {
        let tmp = tempfile::tempdir().unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100_000_000));
        let runtime_env = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()
                .unwrap(),
        );
        let num_rows = 1025i64; // 2^10 + 1: next_power_of_two(1025) == 2048, a real jump.
        let (path, size, deleted) = write_alternating_parquet(tmp.path(), num_rows);
        let scan_file = dv_scan_file_for_alternating(&runtime_env, &path, size, &deleted);

        let out = attach_access_plans(Arc::clone(&runtime_env), vec![scan_file])
            .await
            .unwrap();
        let attached_plan = out[0]
            .extensions
            .get::<ParquetAccessPlan>()
            .expect("attach_access_plans should have attached a plan")
            .clone();
        let reservation = out[0]
            .extensions
            .get::<DvAccessPlanReservation>()
            .expect("reservation extension should be attached alongside the access plan");
        let attached_len = total_selectors(&attached_plan);

        let all_select = RowSelection::from(vec![RowSelector::select(num_rows as usize)]);

        // Mirror create_initial_plan's clone, then simulate PagePruningAccessPlanFilter firing
        // against it.
        let mut clone_for_capacity = attached_plan.clone();
        clone_for_capacity.scan_selection(0, all_select.clone());
        // Recover the intersected clone's TRUE capacity: `into_inner()` moves the
        // `Vec<RowGroupAccess>` out without cloning, and pattern-matching by value on the
        // result moves the `RowSelection` out the same way -- neither step clones it.
        let clone_selection = match clone_for_capacity.into_inner().into_iter().next().unwrap() {
            RowGroupAccess::Selection(sel) => sel,
            other => panic!(
                "expected row group 0 to carry a Selection after scan_selection, got {other:?}"
            ),
        };
        let clone_selectors: Vec<RowSelector> = clone_selection.into();
        let clone_capacity = clone_selectors.capacity();
        assert!(
            clone_capacity > attached_len,
            "test setup invariant: the intersection must actually inflate the clone's capacity \
             past its length ({attached_len}) for this test to exercise the fix -- got \
             {clone_capacity}"
        );

        // A second, independently-reconstructed intersected clone (identical content, so the
        // SAME deterministic capacity) feeds into_overall_row_selection, mirroring how the
        // real reader calls it on the plan AFTER page pruning has already mutated it in place.
        let mut clone_for_combining = attached_plan.clone();
        clone_for_combining.scan_selection(0, all_select);
        let metadata = read_metadata_with_page_index(&path);
        let combined = clone_for_combining
            .into_overall_row_selection(metadata.row_groups())
            .unwrap()
            .expect("a plan with a Selection row group should produce a combined RowSelection");
        let combined_selectors: Vec<RowSelector> = combined.into();
        let combined_capacity = combined_selectors.capacity();

        let peak_bytes =
            (attached_len + clone_capacity + combined_capacity) * size_of::<RowSelector>();
        assert!(
            peak_bytes <= reservation.0.size(),
            "the real reader path's peak WITH page-index pruning firing against the clone \
             ({peak_bytes} bytes: {attached_len} attached selectors + {clone_capacity} \
             intersected-clone Vec capacity + {combined_capacity} combined-selection Vec \
             capacity) must fit the reservation ({} bytes)",
            reservation.0.size()
        );
    }

    /// Property check over a grid of `(cardinality, num_row_groups, page_bound)` combinations,
    /// each checked at several `R <= S`: the reader-lifecycle steady-state bound can never
    /// exceed the admission bound reserved up front -- the resize at the end of
    /// `attach_access_plan` must never need to GROW the reservation, only shrink it.
    #[test]
    fn resize_never_grows() {
        for cardinality in [0i64, 1, 5, 100, 1_000, 10_000] {
            for num_row_groups in [1usize, 2, 5, 100] {
                for page_bound in [0usize, 1, 3, 50] {
                    let s = 2 * cardinality as usize + num_row_groups;
                    let admission =
                        admission_bound_bytes(cardinality, num_row_groups, page_bound).unwrap();
                    // Sample the real invariant `R <= S` at both extremes and the midpoint --
                    // reader_peak_bytes is monotone in its first argument, so checking a few
                    // representative points is sufficient to catch a regression.
                    for &r in &[0usize, s / 2, s] {
                        let rp_bound = r + page_bound;
                        let reader_bytes = reader_peak_bytes(rp_bound, num_row_groups).unwrap();
                        assert!(
                            reader_bytes <= admission,
                            "reader_peak_bytes({rp_bound}, {num_row_groups}) = {reader_bytes} \
                             must not exceed admission_bound_bytes({cardinality}, \
                             {num_row_groups}, {page_bound}) = {admission} for R={r} <= S={s}"
                        );
                    }
                }
            }
        }
    }

    /// `page_selection_bound_selectors` must return exactly `0` when the file's metadata
    /// carries no offset index (the `unwrap_or(0)` this module's doc comment claims is
    /// provably safe, not merely a convenient default), and the shared
    /// `PageIndexPolicy::Optional` fetch used throughout this module must actually populate the
    /// offset index when the file has one -- otherwise every other test in this file exercising
    /// `page_selection_bound_selectors` indirectly would be silently testing against `0`
    /// instead of a real page-index bound.
    #[test]
    fn page_selection_bound_selectors_reflects_offset_index_presence() {
        let tmp = tempfile::tempdir().unwrap();

        // A file written with the offset index explicitly disabled: no page locations to bound.
        let no_index_path = tmp.path().join("no_page_index.parquet");
        write_parquet_with_properties(
            &no_index_path,
            1024,
            WriterProperties::builder()
                .set_offset_index_disabled(true)
                .build(),
        );
        let metadata_without_index = read_metadata_with_page_index(&no_index_path);
        assert!(
            metadata_without_index.offset_index().is_none(),
            "test setup invariant: this file must have no offset index"
        );
        assert_eq!(
            page_selection_bound_selectors(&metadata_without_index).unwrap(),
            0
        );

        // A file written with default properties: the offset index is written by default, and
        // the PageIndexPolicy::Optional fetch this module uses must actually populate it.
        let indexed_path = tmp.path().join("with_page_index.parquet");
        write_parquet(&indexed_path, 1024);
        let metadata_with_index = read_metadata_with_page_index(&indexed_path);
        assert!(
            metadata_with_index.offset_index().is_some(),
            "a default-written file should carry an offset index -- if this fails, the \
             Optional page-index fetch policy stopped populating it, and \
             page_selection_bound_selectors would be silently under-bounding"
        );
        assert!(
            page_selection_bound_selectors(&metadata_with_index).unwrap() > 0,
            "a file with pages and an offset index should have a positive page-selection bound"
        );
    }
}
