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

use arrow::array::{ArrayRef, Date32Array};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_comet_spark_expr::date_trunc_dyn;
use std::{hint::black_box, sync::Arc};

fn criterion_benchmark(c: &mut Criterion) {
    let modern_dates: ArrayRef = Arc::new(create_modern_date_array());
    let mixed_range_null_dates: ArrayRef = Arc::new(create_mixed_range_null_date_array());

    let mut group = c.benchmark_group("date_trunc");

    // Benchmark each truncation format
    for format in ["YEAR", "QUARTER", "MONTH", "WEEK"] {
        group.bench_function(format!("date_trunc_{}", format.to_lowercase()), |b| {
            b.iter(|| {
                black_box(date_trunc_dyn(black_box(&modern_dates), format.to_string()).unwrap())
            });
        });
        group.bench_function(
            format!("date_trunc_{}_mixed_range_null", format.to_lowercase()),
            |b| {
                b.iter(|| {
                    black_box(
                        date_trunc_dyn(black_box(&mixed_range_null_dates), format.to_string())
                            .unwrap(),
                    )
                });
            },
        );
    }

    group.finish();
}

fn create_modern_date_array() -> Date32Array {
    // Create 10000 dates spanning several years (more realistic workload)
    // Days since Unix epoch: range from 0 (1970-01-01) to ~19000 (2022)
    let dates: Vec<i32> = (0..10000).map(|i| (i * 2) % 19000).collect();
    Date32Array::from(dates)
}

fn create_mixed_range_null_date_array() -> Date32Array {
    // Mix ordinary modern dates with dates around year 3333, which are outside
    // DataFusion's TimestampNanosecond range, and nulls.
    let dates: Vec<Option<i32>> = (0..10000)
        .map(|i| match i % 10 {
            0 | 1 => None,
            2 | 3 => Some(497_826 + (i * 17) % 365),
            _ => Some((i * 2) % 19000),
        })
        .collect();
    Date32Array::from(dates)
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
