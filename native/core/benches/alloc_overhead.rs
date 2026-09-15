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
//! The benchmark relies on the `#[global_allocator]` that `lib.rs` installs, which reaches this
//! binary through the `rlib`. That only happens if the crate is actually linked, and an `--extern`
//! crate that nothing names is dropped from the crate graph along with its allocator, so the
//! `extern crate` below is load-bearing: without it a baseline run that never touches `comet`
//! silently measures the system allocator instead of jemalloc. The two liveness checks fail the run
//! if either the selected backend or the wrapper is somehow not in effect, because a number
//! measured against the wrong allocator would be worse than no number.
//!
//! `small_churn` is the worst case for the thread-local path: allocations so small that the
//! wrapper's bookkeeping is a meaningful fraction of the allocator's own work. `threshold_churn` is
//! the worst case for the shared counter: an alloc/free loop at exactly the 64 KiB settle threshold
//! flushes to the process-wide atomic on every call, and the parallel variant does that from every
//! core at once, so the gap between the single-threaded and parallel numbers is the cost of
//! contention on that cacheline. `arrow_sized_churn` is closer to what Comet actually does, where
//! a batch-sized buffer dwarfs the bookkeeping. Real query workloads sit at or below
//! `arrow_sized_churn`, because they do actual work between allocations.

// Pulls `comet`, and with it the `#[global_allocator]` selected by its feature set, into this
// binary even when the feature set leaves nothing here that names the crate.
extern crate comet;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::hint::black_box;
use std::thread;
use std::time::Instant;

/// Guards against measuring the wrong allocator, and says which one is being measured.
///
/// Which backend is in effect is `lib.rs`'s decision, not this crate's feature flags': with
/// `jemalloc,mimalloc` together the library deliberately falls back to the system allocator, and
/// jemalloc on MSVC is not selected at all. So the check asks the library which backend it chose
/// rather than re-deriving that from the feature set, and can never disagree with the selection it
/// is meant to verify.
fn assert_backend_is_live() {
    static ANNOUNCE: std::sync::Once = std::sync::Once::new();
    ANNOUNCE.call_once(|| {
        eprintln!(
            "alloc_overhead: measuring the `{}` allocator backend",
            comet::ALLOCATOR_BACKEND
        )
    });
    if comet::ALLOCATOR_BACKEND == "jemalloc" {
        assert_jemalloc_is_live();
    }
}

/// jemalloc keeps its own count of bytes it has served; if it is not the global allocator of this
/// binary that count stays at zero, and a "jemalloc" baseline would in fact be the system
/// allocator.
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

/// Guards against measuring nothing. If the wrapper were not actually installed in the benchmark
/// binary, every "with the feature" number would silently be a second baseline run.
#[cfg(feature = "alloc-accounting")]
fn assert_accounting_is_live() {
    let before = comet::alloc_accounting::current_balance();
    // `black_box` is load-bearing: benchmarks build in release mode, where LLVM will happily
    // elide an allocation whose contents are never observed, and the check would then fail
    // against a wrapper that is in fact working.
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

/// Allocation sizes that stay under the 64 KiB settle threshold, so most iterations exercise only
/// the thread-local fast path rather than the atomic flush.
fn small_churn(c: &mut Criterion) {
    assert_backend_is_live();
    assert_accounting_is_live();
    let mut group = c.benchmark_group("alloc_overhead");
    for size in [16usize, 256, 4096] {
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("alloc_free_{size}b"), |b| {
            b.iter(|| {
                let v: Vec<u8> = Vec::with_capacity(black_box(size));
                black_box(&v);
            });
        });
    }
    group.finish();
}

/// A batch-sized buffer, filled so the pages are actually touched. This is the shape of allocation
/// Comet does in bulk.
fn arrow_sized_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("alloc_overhead");
    group.throughput(Throughput::Bytes(64 * 1024));
    group.bench_function("alloc_fill_free_64kb", |b| {
        b.iter_batched(
            || (),
            |()| {
                let v: Vec<u8> = vec![1u8; black_box(64 * 1024)];
                black_box(v.len())
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Repeated growth, which is the `realloc` path: a builder doubling its buffer.
fn growth_churn(c: &mut Criterion) {
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

/// Alloc/free of an untouched block, so the allocator call itself is most of the work and the
/// wrapper's share is largest.
fn alloc_free(size: usize) {
    let v: Vec<u8> = Vec::with_capacity(black_box(size));
    black_box(&v);
}

/// Alloc/free loops either side of the 64 KiB settle threshold, single-threaded and from every
/// core at once.
///
/// A loop of one size never accumulates drift, so the threshold decides everything: at 32 KiB the
/// alloc and the free cancel inside the thread-local cell and the shared counter is never touched,
/// while at 64 KiB every alloc and every free flushes. The 64 KiB parallel case is therefore the
/// upper bound on shared-counter contention: `available_parallelism()` threads each doing two
/// atomic read-modify-writes per iteration on the same cacheline, with nothing else in between.
///
/// Times are reported per alloc/free pair per thread, so a parallel number equal to its
/// single-threaded counterpart means the threads did not slow each other down at all.
fn threshold_churn(c: &mut Criterion) {
    assert_backend_is_live();
    assert_accounting_is_live();
    let threads = thread::available_parallelism().map_or(4, |n| n.get());
    let mut group = c.benchmark_group("alloc_overhead");
    group.throughput(Throughput::Elements(1));
    for size in [32 * 1024usize, 64 * 1024] {
        let kb = size / 1024;
        group.bench_function(format!("alloc_free_{kb}kb"), |b| {
            b.iter(|| alloc_free(size));
        });
        group.bench_function(format!("parallel_alloc_free_{kb}kb_x{threads}"), |b| {
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
    group.finish();
}

criterion_group!(
    benches,
    small_churn,
    threshold_churn,
    arrow_sized_churn,
    growth_churn
);
criterion_main!(benches);
