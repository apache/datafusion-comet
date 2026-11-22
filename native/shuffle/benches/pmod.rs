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

//! Benchmarks the mapping from row hash to shuffle partition.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_comet_shuffle::bench_support::hash_partition_ids;
use std::hint::black_box;

const NUM_ROWS: usize = 8192;

/// Pseudo-random hashes from a fixed-seed xorshift, so runs are comparable.
fn random_hashes() -> Vec<u32> {
    let mut x: u32 = 0x9e37_79b9;
    (0..NUM_ROWS)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x
        })
        .collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    let random = random_hashes();
    // Clearing the sign bit makes every hash non-negative as an i32, so the sign is predictable.
    let non_negative: Vec<u32> = random.iter().map(|h| h & 0x7fff_ffff).collect();
    let mut partition_ids = vec![0u32; NUM_ROWS];

    let mut group = c.benchmark_group("pmod");
    for num_partitions in [16, 200] {
        for (shape, hashes) in [("random", &random), ("non_negative", &non_negative)] {
            group.bench_with_input(
                BenchmarkId::new(shape, num_partitions),
                hashes,
                |b, hashes| {
                    b.iter(|| {
                        hash_partition_ids(
                            black_box(hashes),
                            black_box(num_partitions),
                            &mut partition_ids,
                        );
                        black_box(&partition_ids);
                    })
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
