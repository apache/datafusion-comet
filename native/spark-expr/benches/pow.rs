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
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_pow;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{f64_array, NULL_RATIOS, ROW_COUNTS};

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spark_pow");
    for rows in ROW_COUNTS {
        let base = f64_array(rows, 0.0, |_| 2.0);
        for (null_ratio, tag) in NULL_RATIOS {
            let exp = f64_array(rows, null_ratio, |i| (i % 10) as f64 + 1.0);
            let args = vec![
                ColumnarValue::Array(Arc::clone(&base)),
                ColumnarValue::Array(exp),
            ];
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &args,
                |b, args| b.iter(|| black_box(spark_pow(black_box(args)).unwrap())),
            );
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
