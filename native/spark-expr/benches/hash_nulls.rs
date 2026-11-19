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

//! Benchmarks hashing fixed-width columns with nulls, the case shuffle hash partitioning hits on
//! nullable keys such as TPC-DS foreign keys.

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array};
use arrow::buffer::NullBuffer;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use datafusion_comet_spark_expr::spark_xxhash64;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 8192;

/// Pseudo-random values from a fixed-seed xorshift, so runs are comparable.
fn xorshift(seed: u64) -> impl FnMut() -> u64 {
    let mut x = seed;
    move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    }
}

/// Each row is null with probability `null_fraction`, independently.
fn random_nulls(null_fraction: f64) -> NullBuffer {
    let mut next = xorshift(0x2545_f491_4f6c_dd1d);
    NullBuffer::from(
        (0..NUM_ROWS)
            .map(|_| (next() as f64 / u64::MAX as f64) >= null_fraction)
            .collect::<Vec<_>>(),
    )
}

/// Every tenth row is null, a pattern the branch predictor learns.
fn stride_nulls() -> NullBuffer {
    NullBuffer::from((0..NUM_ROWS).map(|i| i % 10 != 0).collect::<Vec<_>>())
}

fn int32(nulls: Option<NullBuffer>) -> ArrayRef {
    let mut next = xorshift(1);
    Arc::new(Int32Array::new(
        (0..NUM_ROWS).map(|_| next() as i32).collect(),
        nulls,
    ))
}

fn int64(nulls: Option<NullBuffer>) -> ArrayRef {
    let mut next = xorshift(2);
    Arc::new(Int64Array::new(
        (0..NUM_ROWS).map(|_| next() as i64).collect(),
        nulls,
    ))
}

fn float64(nulls: Option<NullBuffer>) -> ArrayRef {
    let mut next = xorshift(3);
    Arc::new(Float64Array::new(
        (0..NUM_ROWS).map(|_| next() as f64 * 1e-6).collect(),
        nulls,
    ))
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut murmur3 = |name: &str, array: ArrayRef| {
        let arrays = [array];
        let mut hashes = vec![42u32; NUM_ROWS];
        c.bench_function(&format!("murmur3/{name}"), |b| {
            b.iter(|| {
                create_murmur3_hashes(black_box(&arrays), &mut hashes).unwrap();
                black_box(&hashes);
            })
        });
    };
    // No nulls, which this path doesn't reach, to show the run's noise.
    murmur3("int32/no_nulls", int32(None));
    murmur3("int32/stride_10%", int32(Some(stride_nulls())));
    for percent in [3, 10, 50, 90, 99] {
        let nulls = random_nulls(percent as f64 / 100.0);
        murmur3(&format!("int32/random_{percent}%"), int32(Some(nulls)));
    }
    for percent in [3, 50] {
        let nulls = random_nulls(percent as f64 / 100.0);
        murmur3(
            &format!("int64/random_{percent}%"),
            int64(Some(nulls.clone())),
        );
        murmur3(&format!("float64/random_{percent}%"), float64(Some(nulls)));
    }

    // A seed other than 42 keeps xxhash64 on Comet's implementation.
    for percent in [3, 50, 99] {
        let nulls = random_nulls(percent as f64 / 100.0);
        let args = vec![
            ColumnarValue::Array(int64(Some(nulls))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(7))),
        ];
        c.bench_function(&format!("xxhash64/int64/random_{percent}%"), |b| {
            b.iter(|| black_box(spark_xxhash64(black_box(&args)).unwrap()))
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
