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

//! Benchmarks for the scalar HLL kernels: `hll_sketch_estimate` (Binary sketch -> Long estimate)
//! and `hll_union` (two Binary sketch columns + an `allowDifferentLgConfigK` flag -> Binary).
//! Both deserialize a sketch per row, so inputs are real sketch bytes minted up front with
//! `SparkHllSketch`.

use arrow::array::{ArrayRef, BinaryArray, BooleanArray};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_hll_sketch_estimate, spark_hll_union, SparkHllSketch};
use std::hint::black_box;
use std::sync::Arc;

const LG_CONFIG_K: u8 = 12;

/// Build a Binary column of `rows` sketches, each covering a distinct run of 100 i64 values
/// offset by `base` so the two columns in a union cover different ranges.
fn sketch_column(rows: usize, base: i64) -> ArrayRef {
    let sketches: Vec<Vec<u8>> = (0..rows)
        .map(|i| {
            let mut s = SparkHllSketch::new(LG_CONFIG_K);
            let start = base + (i as i64) * 100;
            for v in start..start + 100 {
                s.update_i64(v);
            }
            s.to_sketch_bytes()
        })
        .collect();
    Arc::new(BinaryArray::from_iter_values(sketches))
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;
    let left = sketch_column(rows, 0);
    let right = sketch_column(rows, 1_000_000);
    let allow: ArrayRef = Arc::new(BooleanArray::from(vec![false; rows]));

    let mut group = c.benchmark_group("hll_scalar");

    group.bench_function("hll_sketch_estimate", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&left))];
        b.iter(|| black_box(spark_hll_sketch_estimate(black_box(&args)).unwrap()))
    });

    group.bench_function("hll_union", |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(&left)),
            ColumnarValue::Array(Arc::clone(&right)),
            ColumnarValue::Array(Arc::clone(&allow)),
        ];
        b.iter(|| black_box(spark_hll_union(black_box(&args)).unwrap()))
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
