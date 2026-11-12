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

//! `sort_row_partitions`, the sort of a JVM shuffle spill's packed record pointers by partition
//! id, over 1M records spread uniformly across 1, 200, 4000 and 100,000 partitions, and skewed
//! with nine records in ten in one of 200 partitions.

use std::hint::black_box;

use comet::execution::row_partition_sort::sort_row_partitions;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

const NUM_RECORDS: usize = 1 << 20;

/// splitmix64.
fn mix(x: u64) -> u64 {
    let mut x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// A pointer array as `SpillSorter` hands it over: the packed record pointers, partition id in
/// the upper 24 bits, followed by as many longs of scratch space.
fn pointer_array(partition_of: impl Fn(u64) -> u64) -> Vec<i64> {
    let mut array = vec![0_i64; 2 * NUM_RECORDS];
    for (i, pointer) in array[..NUM_RECORDS].iter_mut().enumerate() {
        *pointer = ((partition_of(mix(i as u64)) << 40) | i as u64) as i64;
    }
    array
}

fn criterion_benchmark(c: &mut Criterion) {
    let cases: [(&str, Vec<i64>); 5] = [
        ("uniform_1", pointer_array(|_| 0)),
        ("uniform_200", pointer_array(|r| r % 200)),
        ("uniform_4000", pointer_array(|r| r % 4000)),
        ("uniform_100000", pointer_array(|r| r % 100_000)),
        (
            "skewed_200",
            pointer_array(|r| if r % 10 == 0 { (r >> 8) % 200 } else { 7 }),
        ),
    ];

    let mut group = c.benchmark_group("sort_row_partitions");
    group.throughput(Throughput::Elements(NUM_RECORDS as u64));
    for (name, array) in &cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), array, |b, array| {
            b.iter_batched_ref(
                || array.clone(),
                |array| sort_row_partitions(black_box(array), NUM_RECORDS),
                BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
