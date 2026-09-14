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

use arrow::array::{ArrayRef, Decimal128Array};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_unscaled_value;
use std::hint::black_box;
use std::sync::Arc;

/// Build a Decimal128(20, 2) column of `rows` rows, with every `null_every`-th
/// row null (`null_every == 0` means no nulls).
fn create_decimal_array(rows: usize, null_every: usize) -> ArrayRef {
    let arr: Decimal128Array = (0..rows)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else {
                Some((i as i128 % 100_000) * 100)
            }
        })
        .collect::<Decimal128Array>()
        .with_precision_and_scale(20, 2)
        .unwrap();
    Arc::new(arr)
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;

    let mut bench = |name: &str, arr: &ArrayRef| {
        let args = vec![ColumnarValue::Array(Arc::clone(arr))];
        c.bench_function(name, |b| {
            b.iter(|| black_box(spark_unscaled_value(black_box(&args)).unwrap()))
        });
    };

    let no_nulls = create_decimal_array(rows, 0);
    let sparse_nulls = create_decimal_array(rows, 10);
    let dense_nulls = create_decimal_array(rows, 2);

    bench("spark_unscaled_value: no nulls", &no_nulls);
    bench("spark_unscaled_value: sparse nulls", &sparse_nulls);
    bench("spark_unscaled_value: dense nulls", &dense_nulls);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
