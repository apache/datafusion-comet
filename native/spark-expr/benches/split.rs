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

use arrow::array::{ArrayRef, StringArray};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_split, spark_split_sql};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8192;

fn create_csv(rows: usize, null_every: usize) -> ArrayRef {
    let arr: StringArray = (0..rows)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else {
                Some(format!("{},{},{}", i % 100, (i + 1) % 100, (i + 2) % 100))
            }
        })
        .collect();
    Arc::new(arr)
}

fn bench_split(c: &mut Criterion) {
    let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
    let mut bench = |name: &str, arr: &ArrayRef| {
        let args = vec![ColumnarValue::Array(Arc::clone(arr)), pattern.clone()];
        c.bench_function(name, |b| {
            b.iter(|| black_box(spark_split(black_box(&args)).unwrap()))
        });
    };
    bench("spark_split: no nulls", &create_csv(ROWS, 0));
    bench("spark_split: sparse nulls", &create_csv(ROWS, 10));
    bench("spark_split: dense nulls", &create_csv(ROWS, 2));
}

fn bench_split_sql(c: &mut Criterion) {
    let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));
    let mut bench = |name: &str, arr: &ArrayRef| {
        let args = vec![ColumnarValue::Array(Arc::clone(arr)), delimiter.clone()];
        c.bench_function(name, |b| {
            b.iter(|| black_box(spark_split_sql(black_box(&args)).unwrap()))
        });
    };
    bench("spark_split_sql: no nulls", &create_csv(ROWS, 0));
    bench("spark_split_sql: sparse nulls", &create_csv(ROWS, 10));
    bench("spark_split_sql: dense nulls", &create_csv(ROWS, 2));
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_split(c);
    bench_split_sql(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
