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

#[path = "common.rs"]
mod common;

use arrow_array::ArrayRef;
use comet::execution::kernels::hash;
use common::*;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;

const BATCH_SIZE: usize = 1024 * 8;
const NUM_ITER: usize = 10;
const NULL_FRACTION: f32 = 0.1;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash");

    let a1: ArrayRef = Arc::new(create_int64_array(BATCH_SIZE, 0.0, 0, BATCH_SIZE as i64));
    let a2: ArrayRef = Arc::new(create_int64_array(BATCH_SIZE, 0.0, 0, BATCH_SIZE as i64));
    let a3: ArrayRef = Arc::new(create_int64_array(
        BATCH_SIZE,
        NULL_FRACTION,
        0,
        BATCH_SIZE as i64,
    ));
    let a4: ArrayRef = Arc::new(create_int64_array(
        BATCH_SIZE,
        NULL_FRACTION,
        0,
        BATCH_SIZE as i64,
    ));

    group.bench_function(
        BenchmarkId::new("hash_i64_single_nonnull", BATCH_SIZE),
        |b| {
            let input = vec![a1.clone()];
            let mut dst = vec![0; BATCH_SIZE];

            b.iter(|| {
                for _ in 0..NUM_ITER {
                    hash(&input, &mut dst);
                }
            });
        },
    );
    group.bench_function(BenchmarkId::new("hash_i64_single_null", BATCH_SIZE), |b| {
        let input = vec![a3.clone()];
        let mut dst = vec![0; BATCH_SIZE];

        b.iter(|| {
            for _ in 0..NUM_ITER {
                hash(&input, &mut dst);
            }
        });
    });
    group.bench_function(
        BenchmarkId::new("hash_i64_multiple_nonnull", BATCH_SIZE),
        |b| {
            let input = vec![a1.clone(), a2.clone()];
            let mut dst = vec![0; BATCH_SIZE];

            b.iter(|| {
                for _ in 0..NUM_ITER {
                    hash(&input, &mut dst);
                }
            });
        },
    );
    group.bench_function(
        BenchmarkId::new("hash_i64_multiple_null", BATCH_SIZE),
        |b| {
            let input = vec![a3.clone(), a4.clone()];
            let mut dst = vec![0; BATCH_SIZE];

            b.iter(|| {
                for _ in 0..NUM_ITER {
                    hash(&input, &mut dst);
                }
            });
        },
    );
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
