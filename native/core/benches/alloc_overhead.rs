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

//! Measures the cost the accounting wrapper around the global allocator adds per allocation.
//!
//! The wrapper is always installed, so there is no build without it to compare against. Instead
//! each case runs twice in one binary, calling the backend allocator the build selected directly
//! (`direct`) and through an `AccountingAllocator` wrapping it (`accounted`). Both call the
//! allocator explicitly rather than through `#[global_allocator]`, and the accounted calls update
//! the same process-wide balance the installed wrapper does, so the difference between the two is
//! the wrapper's cost.
//!
//! ```shell
//! cargo bench --bench alloc_overhead
//! cargo bench --bench alloc_overhead --features jemalloc
//! ```
//!
//! `alloc_free` allocates and frees untouched blocks, so the allocator call is most of the work
//! and the wrapper's share is largest. Sizes below the 64 KiB settle threshold only ever touch the
//! thread-local path; a loop of exactly 64 KiB blocks flushes to the shared atomic on every alloc
//! and every free, and its parallel variant does that from every core at once, so the gap between
//! the single-threaded and parallel 64 KiB numbers is the cost of contention on that cacheline.
//! `alloc_fill_free` and `grow_to_64kb` are closer to what Comet does, where filling a batch-sized
//! buffer or growing a builder dwarfs the bookkeeping.

use comet::alloc_accounting::AccountingAllocator;
use comet::{AllocatorBackend, ALLOCATOR_BACKEND, BACKEND_ALLOCATOR};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::alloc::{GlobalAlloc, Layout};
use std::hint::black_box;
use std::sync::Once;
use std::thread;
use std::time::Instant;

/// Mirrors `alloc_accounting::SETTLE_THRESHOLD`: a thread flushes to the shared balance once its
/// un-flushed delta reaches this.
const SETTLE_THRESHOLD: usize = 64 * 1024;

static DIRECT: AllocatorBackend = BACKEND_ALLOCATOR;
static ACCOUNTED: AccountingAllocator<AllocatorBackend> =
    AccountingAllocator::new(BACKEND_ALLOCATOR);

/// The two allocators every case is measured against.
fn variants() -> [(&'static str, &'static (dyn GlobalAlloc + Sync)); 2] {
    static ANNOUNCE: Once = Once::new();
    ANNOUNCE.call_once(|| {
        eprintln!("alloc_overhead: measuring the `{ALLOCATOR_BACKEND}` allocator backend");
    });
    [("direct", &DIRECT), ("accounted", &ACCOUNTED)]
}

/// Alloc/free of an untouched block.
fn alloc_free(allocator: &dyn GlobalAlloc, size: usize) {
    let layout = Layout::from_size_align(black_box(size), 8).unwrap();
    // SAFETY: the layout is valid and non-zero, and the block is freed through the allocator that
    // produced it.
    unsafe {
        let ptr = allocator.alloc(layout);
        assert!(!ptr.is_null());
        black_box(ptr);
        allocator.dealloc(ptr, layout);
    }
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
        for (variant, allocator) in variants() {
            group.bench_function(format!("alloc_free_{label}/{variant}"), |b| {
                b.iter(|| alloc_free(allocator, size));
            });
            if near_threshold {
                group.bench_function(
                    format!("parallel_alloc_free_{label}_x{threads}/{variant}"),
                    |b| {
                        b.iter_custom(|iters| {
                            let start = Instant::now();
                            thread::scope(|scope| {
                                for _ in 0..threads {
                                    scope.spawn(move || {
                                        for _ in 0..iters {
                                            alloc_free(allocator, size);
                                        }
                                    });
                                }
                            });
                            start.elapsed()
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

/// A batch-sized buffer, filled so the pages are actually touched. This is the shape of allocation
/// Comet does in bulk.
fn arrow_sized_churn(c: &mut Criterion) {
    const SIZE: usize = 64 * 1024;
    let mut group = c.benchmark_group("alloc_overhead");
    group.throughput(Throughput::Bytes(SIZE as u64));
    for (variant, allocator) in variants() {
        group.bench_function(format!("alloc_fill_free_64kb/{variant}"), |b| {
            let layout = Layout::from_size_align(SIZE, 8).unwrap();
            b.iter(|| {
                // SAFETY: the layout is valid and non-zero, the fill stays within the block, and
                // the block is freed through the allocator that produced it.
                unsafe {
                    let ptr = allocator.alloc(layout);
                    assert!(!ptr.is_null());
                    ptr.write_bytes(1, black_box(SIZE));
                    black_box(ptr);
                    allocator.dealloc(ptr, layout);
                }
            });
        });
    }
    group.finish();
}

/// Repeated growth, which is the `realloc` path: a builder doubling its buffer from 64 bytes to
/// 64 KiB.
fn growth_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("alloc_overhead");
    for (variant, allocator) in variants() {
        group.bench_function(format!("grow_to_64kb/{variant}"), |b| {
            b.iter(|| {
                // SAFETY: each `realloc` passes the block's current layout, and the block is freed
                // at its final size through the allocator that produced it.
                unsafe {
                    let mut layout = Layout::from_size_align(64, 8).unwrap();
                    let mut ptr = allocator.alloc(layout);
                    assert!(!ptr.is_null());
                    while layout.size() < 64 * 1024 {
                        let new_size = layout.size() * 2;
                        ptr = allocator.realloc(ptr, layout, new_size);
                        assert!(!ptr.is_null());
                        layout = Layout::from_size_align(new_size, 8).unwrap();
                        black_box(ptr);
                    }
                    allocator.dealloc(ptr, layout);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, churn, arrow_sized_churn, growth_churn);
criterion_main!(benches);
