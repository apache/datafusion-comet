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

use arrow::array::StringArray;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_to_time;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("to_time");

    let hh_mm = create_string_array(10000, |i| format!("{}:{:02}", i % 24, i % 60));
    group.bench_function("hh_mm", |b| {
        b.iter(|| spark_to_time(std::slice::from_ref(&hh_mm), true).unwrap());
    });

    let hh_mm_ss =
        create_string_array(10000, |i| format!("{}:{:02}:{:02}", i % 24, i % 60, i % 60));
    group.bench_function("hh_mm_ss", |b| {
        b.iter(|| spark_to_time(std::slice::from_ref(&hh_mm_ss), true).unwrap());
    });

    let fractional = create_string_array(10000, |i| {
        format!(
            "{}:{:02}:{:02}.{:06}",
            i % 24,
            i % 60,
            i % 60,
            i * 7 % 1000000
        )
    });
    group.bench_function("fractional", |b| {
        b.iter(|| spark_to_time(std::slice::from_ref(&fractional), true).unwrap());
    });

    let am_pm = create_string_array(10000, |i| {
        let hour = (i % 12) + 1;
        let suffix = if i % 2 == 0 { "AM" } else { "PM" };
        format!("{}:{:02}:{:02} {}", hour, i % 60, i % 60, suffix)
    });
    group.bench_function("am_pm", |b| {
        b.iter(|| spark_to_time(std::slice::from_ref(&am_pm), true).unwrap());
    });

    group.finish();
}

fn create_string_array(size: usize, f: impl Fn(usize) -> String) -> ColumnarValue {
    let values: Vec<String> = (0..size).map(&f).collect();
    let array = StringArray::from(values);
    ColumnarValue::Array(Arc::new(array))
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
