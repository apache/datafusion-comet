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

use arrow::array::Decimal128Array;
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_ceil;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: i128 = 8192;

fn decimal_args(precision: u8, scale: i8, spread: i128) -> Vec<ColumnarValue> {
    let values: Vec<i128> = (0..NUM_ROWS)
        .map(|i| (i - NUM_ROWS / 2) * spread + i % 7)
        .collect();
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .unwrap();
    vec![ColumnarValue::Array(Arc::new(array))]
}

fn criterion_benchmark(c: &mut Criterion) {
    // decimal(18, 4): unscaled values fit comfortably in 64 bits
    let narrow = decimal_args(18, 4, 1_000_003);
    // decimal(38, 6): unscaled values exceed the 64-bit range
    let wide = decimal_args(38, 6, 1_000_000_000_000_000_003);

    let mut group = c.benchmark_group("ceil");
    group.bench_function("ceil_decimal_18_4", |b| {
        b.iter(|| {
            black_box(spark_ceil(
                black_box(&narrow),
                black_box(&DataType::Decimal128(18, 0)),
            ))
        })
    });
    group.bench_function("ceil_decimal_38_6", |b| {
        b.iter(|| {
            black_box(spark_ceil(
                black_box(&wide),
                black_box(&DataType::Decimal128(38, 0)),
            ))
        })
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
