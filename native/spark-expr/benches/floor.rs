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

use arrow::array::{ArrayRef, Decimal128Array, Float64Array};
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_floor;
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8192;

/// Unscaled decimal values that fit in 64 bits, which is the common case, with every 10th row
/// null.
fn decimal_array(precision: u8, scale: i8) -> ArrayRef {
    let values: Vec<Option<i128>> = (0..ROWS)
        .map(|i| {
            if i % 10 == 0 {
                None
            } else {
                let v = (i as i128) * 987_654_321 % 100_000_000_000_000_000;
                Some(if i % 2 == 0 { v } else { -v })
            }
        })
        .collect();
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

/// Unscaled decimal values too wide for 64 bits, exercising the 128-bit fallback.
fn wide_decimal_array(precision: u8, scale: i8) -> ArrayRef {
    let values: Vec<Option<i128>> = (0..ROWS)
        .map(|i| {
            if i % 10 == 0 {
                None
            } else {
                let v = (i as i128 + 1) * 10_000_000_000_000_000_000_000_000_000;
                Some(if i % 2 == 0 { v } else { -v })
            }
        })
        .collect();
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn float_array() -> ArrayRef {
    Arc::new(Float64Array::from(
        (0..ROWS)
            .map(|i| i as f64 * 1.5 - 4096.0)
            .collect::<Vec<_>>(),
    ))
}

fn criterion_benchmark(c: &mut Criterion) {
    let dec_2 = decimal_array(38, 2);
    c.bench_function("spark_floor: decimal128(38,2)", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&dec_2))];
        b.iter(|| black_box(spark_floor(black_box(&args), &DataType::Decimal128(37, 0)).unwrap()))
    });

    let dec_18 = decimal_array(38, 18);
    c.bench_function("spark_floor: decimal128(38,18)", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&dec_18))];
        b.iter(|| black_box(spark_floor(black_box(&args), &DataType::Decimal128(21, 0)).unwrap()))
    });

    let wide = wide_decimal_array(38, 6);
    c.bench_function("spark_floor: decimal128(38,6) wide values", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&wide))];
        b.iter(|| black_box(spark_floor(black_box(&args), &DataType::Decimal128(33, 0)).unwrap()))
    });

    let floats = float_array();
    c.bench_function("spark_floor: float64", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&floats))];
        b.iter(|| black_box(spark_floor(black_box(&args), &DataType::Float64).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
