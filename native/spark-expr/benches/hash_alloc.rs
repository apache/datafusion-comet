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

//! Allocation cost of the murmur3 hash kernel, on the same shapes `hash.rs` times.
//!
//! The nested-element list path gathers elements with `arrow::compute::take`, which allocates an
//! index array per pass and copies the selected element payloads. A timing benchmark cannot show
//! that cost, and it is the part that scales with payload width rather than with element count, so
//! it is reported separately here.
//!
//! Two numbers per shape:
//!
//! - **total** allocated-plus-growth bytes over the whole hash call: every `alloc` size, plus the
//!   growth part of every `realloc` that grew. It counts a temporary even if it is freed
//!   immediately, and it does not re-count the bytes a `realloc` carried over. This is the
//!   throughput-relevant figure: what the allocator has to service.
//! - **peak** live bytes: the high-water mark of bytes *requested* and not yet freed, counting only
//!   allocations made inside the measured window. It is a logical figure, not RSS, and it cannot see
//!   a transient inside `System.realloc` that briefly holds the old and new blocks at once. This is the footprint-relevant figure. Successive
//!   passes do not add up, because each frees its gather before the next allocates, but nesting
//!   levels do: an outer gather stays live while the recursion below it builds its own, so a deep
//!   element type can peak at the sum down the nesting.
//!
//! The two move in opposite directions for the batched gather, which is why both are reported.
//! Slicing one element at a time allocates a small array per element, so its total is large while
//! its peak stays near one element. Gathering a whole pass allocates once per pass for all
//! surviving rows, so its total is much smaller while its peak is a gathered pass. For a wide or
//! deeply nested child the gather copies more than the elements it hashes, so both figures can be
//! worse than the per-element path; `GATHER_ELIGIBLE_CHILD_BYTES` in the kernel is what keeps those shapes
//! on the slice path, and `null_parent_struct_64kb_x4` and `deep_singleton_list_5_deep` are here to
//! keep that guard measured.
//!
//! Run with `cargo bench --bench hash_alloc`. It prints a table rather than asserting a bound: the
//! numbers are for comparing two builds, and a threshold would either be too loose to catch a
//! regression or too tight to survive an Arrow upgrade.

use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[path = "common/hash_shapes.rs"]
mod hash_shapes;
use hash_shapes::*;

/// Counting allocator.
///
/// Only allocations made inside the measured window are tracked, and they are tracked by pointer.
/// Without that, a buffer allocated before the window and freed inside it would be subtracted from
/// `live` and hide part of the peak, and a shrinking `realloc` would never give its bytes back.
/// Clamping the subtraction instead would only paper over both.
///
/// The pointer set makes the allocator non-reentrant, so `IN_HOOK` guards against the set's own
/// allocations recursing back in.
struct Counting;

static TOTAL: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ON: AtomicUsize = AtomicUsize::new(0);

/// What the window knows about one live allocation.
///
/// The two fields are not the same number, and conflating them was a bug: a block allocated inside
/// the window is charged in full, while a block that existed *before* the window and was then grown
/// is charged only for the growth. A second `realloc` of either has to adjust by what this window
/// actually counted, not by the block's physical size.
#[derive(Clone, Copy)]
struct Tracked {
    /// Current physical size of the block.
    physical: usize,
    /// How many bytes of it this window has counted toward `live`.
    counted: usize,
}

/// Live allocations the window knows about, keyed by address. A plain `Mutex<HashMap>` is enough:
/// the benchmark is single-threaded, and correctness here matters more than speed.
static TRACKED: Mutex<Option<HashMap<usize, Tracked>>> = Mutex::new(None);

thread_local! {
    /// Set while inside the allocator hook, so the bookkeeping's own allocations are not counted
    /// and cannot recurse.
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

fn counting() -> bool {
    ON.load(Ordering::Relaxed) == 1 && !IN_HOOK.with(|f| f.get())
}

/// Runs `f` with the hook flag set, so allocations it makes itself are ignored.
fn in_hook<R>(f: impl FnOnce(&mut HashMap<usize, Tracked>) -> R) -> Option<R> {
    IN_HOOK.with(|flag| {
        if flag.get() {
            return None;
        }
        flag.set(true);
        let mut guard = TRACKED.lock().unwrap();
        let map = guard.get_or_insert_with(HashMap::new);
        let out = f(map);
        drop(guard);
        flag.set(false);
        Some(out)
    })
}

/// Records a block allocated inside the window: physical size and counted size are the same.
fn record_alloc(ptr: *mut u8, size: usize) {
    let entry = Tracked {
        physical: size,
        counted: size,
    };
    if in_hook(|map| map.insert(ptr as usize, entry)).is_some() {
        TOTAL.fetch_add(size, Ordering::Relaxed);
        let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

/// Records a block that existed before the window and has now grown: only the growth is this
/// window's, so that is what `counted` holds even though `physical` is the whole block.
fn adopt_grown(ptr: *mut u8, physical: usize, grew: usize) {
    let entry = Tracked {
        physical,
        counted: grew,
    };
    if in_hook(|map| map.insert(ptr as usize, entry)).is_some() {
        TOTAL.fetch_add(grew, Ordering::Relaxed);
        let live = LIVE.fetch_add(grew, Ordering::Relaxed) + grew;
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

/// Drops `ptr` from the tracked set, returning what the window knew about it. `None` means the
/// window never counted it, so it must not touch `live`.
fn forget_alloc(ptr: *mut u8) -> Option<Tracked> {
    in_hook(|map| map.remove(&(ptr as usize))).flatten()
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && counting() {
            record_alloc(ptr, layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if counting() {
            if let Some(t) = forget_alloc(ptr) {
                LIVE.fetch_sub(t.counted, Ordering::Relaxed);
            }
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if new_ptr.is_null() || !counting() {
            return new_ptr;
        }
        match forget_alloc(ptr) {
            // A block this window knows about. Release what the window counted before recording the
            // new size: recording first would leave both live at once and push `peak` to old + new,
            // which no moment of the program holds -- `realloc` either extends in place or frees as
            // it copies.
            Some(t) => {
                LIVE.fetch_sub(t.counted, Ordering::Relaxed);
                // Newly requested bytes are measured against the block's PHYSICAL size, since that
                // is what already existed. The window's `counted` may be smaller -- a block adopted
                // from before the window carries only its growth -- so using it here would charge
                // the pre-existing bytes again on every subsequent realloc.
                let newly_requested = new_size.saturating_sub(t.physical);
                // Shrinking gives bytes back. Subtract the physical reduction rather than clamping
                // to `new_size`: a block adopted from before the window counts less than its
                // physical size, so clamping would leave counted bytes that no longer exist. An
                // adopted 100-byte block grown to 200 and shrunk to 150 has counted 100 and must
                // end at 50, not at `min(100, 150)`.
                let released = t.physical.saturating_sub(new_size);
                let entry = Tracked {
                    physical: new_size,
                    counted: t
                        .counted
                        .saturating_add(newly_requested)
                        .saturating_sub(released)
                        .min(new_size),
                };
                if in_hook(|map| map.insert(new_ptr as usize, entry)).is_some() {
                    TOTAL.fetch_add(newly_requested, Ordering::Relaxed);
                    let live = LIVE.fetch_add(entry.counted, Ordering::Relaxed) + entry.counted;
                    PEAK.fetch_max(live, Ordering::Relaxed);
                }
            }
            // A block from before the window. Count only the growth, and start tracking it so a
            // later shrink or free has something to release -- but record its physical size too, so
            // a *second* realloc adjusts against the right number.
            None => {
                let grew = new_size.saturating_sub(layout.size());
                if grew > 0 {
                    adopt_grown(new_ptr, new_size, grew);
                }
            }
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Hashes `array` once with counting on, returning `(total, peak, hashes)`.
///
/// The hash buffer is allocated before counting starts, so the numbers describe the kernel's own
/// temporaries rather than the caller's output buffer.
/// Opens a measurement window: counters AND tracked records are reset together.
///
/// Resetting only the counters leaves the previous window's live allocations in the map, and freeing
/// one of them inside this window subtracts from a `live` that never counted it, wrapping it toward
/// `usize::MAX` and poisoning `peak`.
fn begin_window() {
    in_hook(|map| map.clear());
    TOTAL.store(0, Ordering::SeqCst);
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ON.store(1, Ordering::SeqCst);
}

/// Closes the window and returns `(total, peak)`. Anything still tracked is dropped with the window,
/// so it cannot affect a later one.
fn end_window() -> (usize, usize) {
    ON.store(0, Ordering::SeqCst);
    let total = TOTAL.load(Ordering::SeqCst);
    let peak = PEAK.load(Ordering::SeqCst);
    in_hook(|map| map.clear());
    (total, peak)
}

fn measure(array: &arrow::array::ArrayRef) -> (usize, usize, Vec<u32>) {
    let mut hashes = vec![42u32; array.len()];

    begin_window();
    create_murmur3_hashes(std::slice::from_ref(array), &mut hashes).unwrap();
    let (total, peak) = end_window();

    (total, peak, hashes)
}

/// The counters are the instrument, so they are checked before the numbers they produce are quoted.
/// Every case below corresponds to a bug this allocator actually had: a clamped subtraction that
/// never released a shrink, a pre-window free that offset in-window bytes, a second `realloc` of an
/// adopted block adjusting by the wrong field, and a window that inherited the previous window's
/// records.
///
/// Uses raw `alloc`/`realloc`/`dealloc` so each step is exact and nothing depends on how `Vec`
/// chooses capacity. Every pointer goes through `black_box`: these allocations are otherwise dead,
/// and the optimiser is entitled to delete them, which silently makes the whole check vacuous.
/// `alloc_zeroed` routes through the default `GlobalAlloc` implementation, which calls `alloc`, so
/// it would be counted too.
fn self_check() {
    let l = |n: usize| Layout::from_size_align(n, 8).unwrap();

    // A plain allocation is counted in full, and freeing it inside the window releases it.
    unsafe {
        begin_window();
        let p = std::hint::black_box(std::alloc::alloc(l(100)));
        assert_eq!(
            (TOTAL.load(Ordering::SeqCst), LIVE.load(Ordering::SeqCst)),
            (100, 100),
            "a plain allocation must be counted in full"
        );
        std::alloc::dealloc(p, l(100));
        assert_eq!(LIVE.load(Ordering::SeqCst), 0, "a free must release it");
        let (total, peak) = end_window();
        assert_eq!((total, peak), (100, 100));
    }

    // A shrinking realloc has to give its bytes back, or peak keeps the pre-shrink size.
    unsafe {
        begin_window();
        let p = std::hint::black_box(std::alloc::alloc(l(100)));
        let p = std::hint::black_box(std::alloc::realloc(p, l(100), 10));
        assert_eq!(
            LIVE.load(Ordering::SeqCst),
            10,
            "a shrink must release the difference"
        );
        let q = std::hint::black_box(std::alloc::alloc(l(100)));
        let (total, peak) = end_window();
        std::alloc::dealloc(p, l(10));
        std::alloc::dealloc(q, l(100));
        // 100 allocated, shrunk to 10 (no new bytes), then 100 more: peak is 10 + 100.
        assert_eq!((total, peak), (200, 110), "shrink then grow accounting");
    }

    // Freeing a block from before the window must not offset the window's own live bytes.
    unsafe {
        let before = std::hint::black_box(std::alloc::alloc(l(100)));
        begin_window();
        let a = std::hint::black_box(std::alloc::alloc(l(100)));
        std::alloc::dealloc(before, l(100));
        let b = std::hint::black_box(std::alloc::alloc(l(100)));
        let (_, peak) = end_window();
        std::alloc::dealloc(a, l(100));
        std::alloc::dealloc(b, l(100));
        assert_eq!(
            peak, 200,
            "a pre-window free must not offset in-window live bytes"
        );
    }

    // A pre-window block grown twice inside the window: only growth counts, and the second grow
    // must adjust against what the window counted, not the block's physical size.
    unsafe {
        let before = std::hint::black_box(std::alloc::alloc(l(100)));
        begin_window();
        let p = std::hint::black_box(std::alloc::realloc(before, l(100), 200));
        assert_eq!(
            (TOTAL.load(Ordering::SeqCst), LIVE.load(Ordering::SeqCst)),
            (100, 100),
            "growing a pre-window block counts only the growth"
        );
        let p = std::hint::black_box(std::alloc::realloc(p, l(200), 300));
        let (total, live) = (TOTAL.load(Ordering::SeqCst), LIVE.load(Ordering::SeqCst));
        let (_, _) = end_window();
        std::alloc::dealloc(p, l(300));
        assert_eq!(
            (total, live),
            (200, 200),
            "a second grow adds only its own growth"
        );
    }

    // An adopted block grown then shrunk: the growth counts, the shrink gives back the physical
    // reduction, not a clamp to the new size.
    unsafe {
        let before = std::hint::black_box(std::alloc::alloc(l(100)));
        begin_window();
        let p = std::hint::black_box(std::alloc::realloc(before, l(100), 200));
        let p = std::hint::black_box(std::alloc::realloc(p, l(200), 150));
        let live_after_shrink = LIVE.load(Ordering::SeqCst);
        let q = std::hint::black_box(std::alloc::alloc(l(100)));
        let (total, peak) = end_window();
        std::alloc::dealloc(p, l(150));
        std::alloc::dealloc(q, l(100));
        assert_eq!(
            live_after_shrink, 50,
            "an adopted block grown to 200 then shrunk to 150 must count 50"
        );
        assert_eq!(
            (total, peak),
            (200, 150),
            "growth counts once; peak is 50 live plus the later 100"
        );
    }

    // A window must not inherit the previous window's records, or freeing an older block wraps
    // `live` toward usize::MAX.
    unsafe {
        begin_window();
        let stale = std::hint::black_box(std::alloc::alloc(l(100)));
        let _ = end_window();

        begin_window();
        std::alloc::dealloc(stale, l(100));
        let live = LIVE.load(Ordering::SeqCst);
        let (_, peak) = end_window();
        assert_eq!(
            live, 0,
            "freeing a block tracked by an earlier window must not underflow live"
        );
        assert_eq!(peak, 0, "and must not poison peak");
    }
}

fn main() {
    self_check();

    // Same shapes and sizes as `hash.rs`, so the timing and allocation tables line up. The
    // gather-heavy ones are the point; the primitive-element and non-list shapes are controls that
    // must not move.
    let cases: Vec<(&str, arrow::array::ArrayRef)> = vec![
        ("int32", primitive(NUM_ROWS)),
        ("list_of_int32_x10", list_of_primitive(NUM_ROWS, 10)),
        ("list_of_struct_x10", list_of_struct(NUM_ROWS, 10)),
        (
            "list_of_struct_skewed_x1024",
            skewed_list_of_struct(NUM_ROWS, 1024),
        ),
        ("list_of_list_x5x5", list_of_list(NUM_ROWS, 5, 5)),
        (
            "list_of_struct_of_map_x5x5",
            list_of_struct_of_map(NUM_ROWS, 5, 5),
        ),
        (
            "list_of_struct_1kb_string_x4",
            list_of_struct_big_string(2048, 4, 1024),
        ),
        (
            "list_of_struct_half_null_x10",
            list_of_struct_half_null(NUM_ROWS, 10),
        ),
        (
            "list_of_struct_long_tail_x1024",
            list_of_struct_long_tail(2, 1024),
        ),
        // Adverse shapes: the gather copies bytes the hash never reads, and nested levels hold
        // their gathers at the same time.
        (
            "null_parent_struct_64kb_x4",
            null_parent_struct_big_string(128, 4, 65536),
        ),
        (
            "deep_singleton_list_5_deep",
            deep_singleton_list_of_struct_big_string(2048, 5, 4096),
        ),
        // The gather shares a dictionary's values instead of copying them, so a cost estimate
        // taken from the child's total size can abandon batching for payload that never moves.
        (
            "struct_of_dict_unreferenced_8mb",
            struct_of_dict_unreferenced_big_value(8192, 8 * 1024 * 1024),
        ),
        // An average width is diluted by short elements, and a slice keeps its child's buffers, so
        // these two are where a width-based estimate under-reads the gather.
        (
            "width_skewed_8mb_first",
            width_skewed_list_of_struct(8 * 1024 * 1024),
        ),
        (
            "sliced_list_retaining_10m_ints",
            sliced_list_retaining_big_child(2, 10_000_000),
        ),
        // Eligible but with nothing to batch across, directly and via recursion: the scheduling
        // buffers must not be allocated for a batch of one.
        (
            "single_row_list_of_struct_x10",
            single_row_list_of_struct(10),
        ),
        (
            "deep_singleton_narrow_5_deep",
            deep_singleton_list_narrow(2048, 5),
        ),
    ];

    println!(
        "{:<32} {:>6} {:>14} {:>14} {:>12}",
        "shape", "rows", "total bytes", "peak bytes", "hash"
    );
    for (name, array) in &cases {
        // Warm up first: the first call through a shape can allocate one-off caches that are not
        // part of the per-call cost, and counting them would make the first row an outlier.
        let (_, _, warm) = measure(array);
        let (total, peak, hashes) = measure(array);
        // The allocation numbers are only meaningful for a kernel that still produces the right
        // hashes, so keep a check in the same run rather than trusting a separate one. Comparing
        // two builds compares this digest too.
        assert_eq!(warm, hashes, "{name}: hashing is not deterministic");
        let digest = hashes.iter().fold(0u64, |a, h| {
            a.wrapping_mul(1_000_003).wrapping_add(*h as u64)
        });
        println!(
            "{:<32} {:>6} {:>14} {:>14} {:>12}",
            name,
            array.len(),
            total,
            peak,
            digest % 1_000_000_007
        );
    }
}
