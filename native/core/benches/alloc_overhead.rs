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

//! Measures the cost the `alloc-accounting` global-allocator wrapper adds per allocation.
//!
//! Run the same benchmark with and without the feature and compare:
//!
//! ```shell
//! cargo bench --bench alloc_overhead -- --save-baseline off
//! cargo bench --bench alloc_overhead --features alloc-accounting -- --baseline off
//! ```
//!
//! `churn` allocates and frees untouched blocks, so the allocator call is most of the work and the
//! wrapper's share is largest. Sizes below the 64 KiB settle threshold only ever touch the
//! thread-local path; a loop of exactly 64 KiB blocks flushes to the shared atomic on every alloc
//! and every free, and its parallel variant does that from every core at once, so the gap between
//! the single-threaded and parallel 64 KiB numbers is the cost of contention on that cacheline.
//! `arrow_sized_churn` and `growth_churn` are closer to what Comet does, where filling a
//! batch-sized buffer or growing a builder dwarfs the bookkeeping.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;
use std::sync::Once;
use std::thread;
use std::time::Instant;

/// Mirrors `alloc_accounting::SETTLE_THRESHOLD`: a thread flushes to the shared balance once its
/// un-flushed delta reaches this.
const SETTLE_THRESHOLD: usize = 64 * 1024;

/// Fails the run if the allocator being measured is not the one the feature set asked for, since
/// a number measured against the wrong allocator would be worse than no number. Every benchmark
/// function calls this, so a filtered run cannot skip it.
fn assert_allocators_are_live() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Which backend is in effect is `lib.rs`'s decision (with `jemalloc,mimalloc` together it
        // falls back to the system allocator), so ask it rather than re-deriving the answer from
        // the feature set. Naming `comet::ALLOCATOR_BACKEND` is also what links the `comet` rlib,
        // and with it the `#[global_allocator]` it installs, into this binary: an `--extern` crate
        // that nothing names is dropped from the crate graph along with its allocator.
        eprintln!(
            "alloc_overhead: measuring the `{}` allocator backend",
            comet::ALLOCATOR_BACKEND
        );
        if comet::ALLOCATOR_BACKEND == "jemalloc" {
            assert_jemalloc_is_live();
        }
        assert_accounting_is_live();
    });
}

/// jemalloc keeps its own count of bytes it has served; if it is not the global allocator of this
/// binary that count stays at zero.
#[cfg(feature = "jemalloc")]
fn assert_jemalloc_is_live() {
    use tikv_jemalloc_ctl::{epoch, stats};
    let held: Vec<u8> = black_box(vec![1u8; 8 * 1024 * 1024]);
    black_box(&held);
    epoch::advance().expect("jemalloc epoch");
    let allocated = stats::allocated::read().expect("jemalloc stats.allocated");
    assert!(
        allocated >= 8 * 1024 * 1024,
        "the library selected jemalloc but jemalloc is not the global allocator of this binary \
         (stats.allocated = {allocated}); the numbers below would be meaningless"
    );
    drop(held);
}

/// Without the feature the library cannot have selected jemalloc, so this is never reached.
#[cfg(not(feature = "jemalloc"))]
fn assert_jemalloc_is_live() {
    unreachable!("the library reports the jemalloc backend but the feature is not enabled");
}

/// If the wrapper were not installed in this binary, every "with the feature" number would
/// silently be a second baseline run.
#[cfg(feature = "alloc-accounting")]
fn assert_accounting_is_live() {
    let before = comet::alloc_accounting::current_balance();
    // `black_box` is load-bearing: in release mode LLVM elides an allocation whose contents are
    // never observed, and the check would then fail against a wrapper that is in fact working.
    let held: Vec<u8> = black_box(vec![1u8; 8 * 1024 * 1024]);
    black_box(&held);
    let during = comet::alloc_accounting::current_balance();
    assert!(
        during >= before + 4 * 1024 * 1024,
        "alloc-accounting is enabled but the allocator is not installed in this binary \
         (balance {before} -> {during}); the numbers below would be meaningless"
    );
    drop(held);
}

#[cfg(not(feature = "alloc-accounting"))]
fn assert_accounting_is_live() {}

/// Alloc/free of an untouched block.
fn alloc_free(size: usize) {
    let v: Vec<u8> = Vec::with_capacity(black_box(size));
    black_box(&v);
}

/// Alloc/free loops from well below the settle threshold up to exactly on it, single-threaded, and
/// either side of the threshold from every core at once.
///
/// A loop of one size never accumulates drift, so the threshold decides everything: below it the
/// alloc and the free cancel inside the thread-local cell and the shared counter is never touched,
/// while at exactly 64 KiB every alloc and every free flushes. The 64 KiB parallel case is
/// therefore the upper bound on shared-counter contention: `available_parallelism()` threads each
/// doing two atomic read-modify-writes per iteration on the same cacheline, with nothing else in
/// between. Times are per alloc/free pair per thread, so a parallel number equal to its
/// single-threaded counterpart means the threads did not slow each other down at all.
fn churn(c: &mut Criterion) {
    assert_allocators_are_live();
    let threads = thread::available_parallelism().map_or(4, |n| n.get());
    let mut group = c.benchmark_group("alloc_overhead");
    group.throughput(Throughput::Elements(1));
    for size in [16usize, 256, 4096, SETTLE_THRESHOLD / 2, SETTLE_THRESHOLD] {
        // The two sizes either side of the threshold are the ones where the parallel variant
        // says something.
        let near_threshold = size >= SETTLE_THRESHOLD / 2;
        let label = if near_threshold {
            format!("{}kb", size / 1024)
        } else {
            format!("{size}b")
        };
        group.bench_function(format!("alloc_free_{label}"), |b| {
            b.iter(|| alloc_free(size));
        });
        if near_threshold {
            group.bench_function(format!("parallel_alloc_free_{label}_x{threads}"), |b| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    thread::scope(|scope| {
                        for _ in 0..threads {
                            scope.spawn(move || {
                                for _ in 0..iters {
                                    alloc_free(size);
                                }
                            });
                        }
                    });
                    start.elapsed()
                });
            });
        }
    }
    group.finish();
}

/// A batch-sized buffer, filled so the pages are actually touched. This is the shape of allocation
/// Comet does in bulk.
fn arrow_sized_churn(c: &mut Criterion) {
    assert_allocators_are_live();
    let mut group = c.benchmark_group("alloc_overhead");
    group.throughput(Throughput::Bytes(64 * 1024));
    group.bench_function("alloc_fill_free_64kb", |b| {
        b.iter(|| {
            let v: Vec<u8> = vec![1u8; black_box(64 * 1024)];
            black_box(v.len())
        });
    });
    group.finish();
}

/// Repeated growth, which is the `realloc` path: a builder doubling its buffer.
fn growth_churn(c: &mut Criterion) {
    assert_allocators_are_live();
    let mut group = c.benchmark_group("alloc_overhead");
    group.bench_function("grow_vec_to_64kb", |b| {
        b.iter(|| {
            let mut v: Vec<u8> = Vec::new();
            for _ in 0..(64 * 1024) {
                v.push(black_box(1u8));
            }
            black_box(v.len())
        });
    });
    group.finish();
}

criterion_group!(benches, churn, arrow_sized_churn, growth_churn);
criterion_main!(benches);
