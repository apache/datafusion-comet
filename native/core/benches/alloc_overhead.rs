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
//! The benchmark relies on the `#[global_allocator]` that `lib.rs` installs, which is linked in
//! through the `rlib`. `assert_accounting_is_live` fails the run if the wrapper is somehow not in
//! effect — without it, a "with the feature" run could silently be a second baseline.
//!
//! `small_churn` is the worst case: allocations so small that the wrapper's bookkeeping is a
//! meaningful fraction of the allocator's own work. `arrow_sized_churn` is closer to what Comet
//! actually does, where a batch-sized buffer dwarfs the bookkeeping. Real query workloads sit at
//! or below `arrow_sized_churn`, because they do actual work between allocations.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use std::hint::black_box;

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

criterion_group!(benches, small_churn, arrow_sized_churn, growth_churn);
criterion_main!(benches);
