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

//! `xxhash64` is the alternative Spark hash (e.g. `xxhash64()` and bucketing). Same shape as the
//! murmur3 benchmark: a representative multi-column key across row counts and null ratios.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_xxhash64;
use std::hint::black_box;

#[path = "common/mod.rs"]
mod common;
use common::{f64_array, i64_array, string_array, NULL_RATIOS, ROW_COUNTS};

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spark_xxhash64");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            // Trailing Int64 scalar is the seed; preceding columns are the key being hashed.
            let args = vec![
                ColumnarValue::Array(i64_array(rows, null_ratio, |i| i as i64)),
                ColumnarValue::Array(string_array(rows, null_ratio, |i| format!("k{}", i % 1024))),
                ColumnarValue::Array(f64_array(rows, null_ratio, |i| i as f64 * 1.5)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(42))),
            ];
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &args,
                |b, args| b.iter(|| black_box(spark_xxhash64(black_box(args)).unwrap())),
            );
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
