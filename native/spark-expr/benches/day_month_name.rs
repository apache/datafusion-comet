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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::logical_expr::ColumnarValue;
use datafusion_comet_spark_expr::{spark_day_name, spark_month_name};
use std::hint::black_box;

#[path = "common/mod.rs"]
mod common;
use common::{date32_array, NULL_RATIOS, ROW_COUNTS};

fn bench_fn(
    c: &mut Criterion,
    name: &str,
    f: fn(&[ColumnarValue]) -> datafusion::common::Result<ColumnarValue>,
) {
    let mut group = c.benchmark_group(name);
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let args = vec![ColumnarValue::Array(date32_array(rows, null_ratio, |i| {
                19_000 + (i % 5000) as i32
            }))];
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &args,
                |b, args| b.iter(|| black_box(f(black_box(args)).unwrap())),
            );
        }
    }
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_fn(c, "spark_day_name", spark_day_name);
    bench_fn(c, "spark_month_name", spark_month_name);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
