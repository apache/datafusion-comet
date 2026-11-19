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

use arrow::array::builder::Decimal128Builder;
use arrow::array::{ArrayRef, Decimal128Array};
use arrow::compute::cast;
use arrow::compute::kernels::numeric;
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_decimal_div, spark_decimal_integral_div, EvalMode};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

/// A batch of nonzero values in `[-max, max]`.
fn decimal_array(rng: &mut StdRng, max: i128, precision: u8, scale: i8) -> ArrayRef {
    let values = (0..BATCH_SIZE).map(|_| {
        let v = rng.random_range(1..=max);
        if rng.random_bool(0.5) {
            v
        } else {
            -v
        }
    });
    Arc::new(
        Decimal128Array::from_iter_values(values)
            .with_data_type(DataType::Decimal128(precision, scale)),
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    // create input data
    let mut c1 = Decimal128Builder::new();
    let mut c2 = Decimal128Builder::new();
    for i in 0..1000 {
        c1.append_value(99999999 + i);
        c2.append_value(88888888 - i);
    }
    let c1 = Arc::new(c1.finish());
    let c2 = Arc::new(c2.finish());

    let c1_type = DataType::Decimal128(10, 4);
    let c1 = cast(c1.as_ref(), &c1_type).unwrap();
    let c2_type = DataType::Decimal128(10, 3);
    let c2 = cast(c2.as_ref(), &c2_type).unwrap();

    let args = [
        ColumnarValue::Array(Arc::clone(&c1)),
        ColumnarValue::Array(Arc::clone(&c2)),
    ];

    let mut group = c.benchmark_group("decimal div");
    group.bench_function("decimal_div", |b| {
        b.iter(|| {
            black_box(spark_decimal_div(
                black_box(&args),
                black_box(&DataType::Decimal128(10, 4)),
                EvalMode::Legacy,
            ))
        })
    });

    group.bench_function("decimal_integral_div", |b| {
        b.iter(|| {
            black_box(spark_decimal_integral_div(
                black_box(&args),
                black_box(&DataType::Decimal128(10, 4)),
                EvalMode::Legacy,
                false,
            ))
        })
    });

    // The cases below use Spark's result types and a full batch.
    let mut rng = StdRng::seed_from_u64(42);

    // Decimal(10, 4) / Decimal(10, 3) is Decimal(24, 15). The dividend is scaled past 64 bits.
    let args = [
        ColumnarValue::Array(decimal_array(&mut rng, 10_i128.pow(10) - 1, 10, 4)),
        ColumnarValue::Array(decimal_array(&mut rng, 10_i128.pow(10) - 1, 10, 3)),
    ];
    group.bench_function("decimal_div_10_4_by_10_3", |b| {
        b.iter(|| {
            black_box(spark_decimal_div(
                black_box(&args),
                &DataType::Decimal128(24, 15),
                EvalMode::Legacy,
            ))
        })
    });
    // Decimal(10, 4) div Decimal(10, 3) is Decimal(9, 0).
    group.bench_function("decimal_integral_div_10_4_by_10_3", |b| {
        b.iter(|| {
            black_box(spark_decimal_integral_div(
                black_box(&args),
                &DataType::Decimal128(9, 0),
                EvalMode::Legacy,
                false,
            ))
        })
    });

    // Decimal(38, 10) / Decimal(38, 10) is Decimal(38, 6), which takes the BigInt branch.
    let args = [
        ColumnarValue::Array(decimal_array(&mut rng, 10_i128.pow(20), 38, 10)),
        ColumnarValue::Array(decimal_array(&mut rng, 10_i128.pow(15), 38, 10)),
    ];
    group.bench_function("decimal_div_38_10_bigint", |b| {
        b.iter(|| {
            black_box(spark_decimal_div(
                black_box(&args),
                &DataType::Decimal128(38, 6),
                EvalMode::Legacy,
            ))
        })
    });

    // Integer `div` casts both operands to Decimal(19, 0).
    for (name, max) in [
        ("integral_div_long_small", 1_000_000),
        ("integral_div_long_random", i64::MAX as i128),
    ] {
        let args = [
            ColumnarValue::Array(decimal_array(&mut rng, max, 19, 0)),
            ColumnarValue::Array(decimal_array(&mut rng, max, 19, 0)),
        ];
        group.bench_function(name, |b| {
            b.iter(|| {
                black_box(spark_decimal_integral_div(
                    black_box(&args),
                    &DataType::Decimal128(19, 0),
                    EvalMode::Legacy,
                    false,
                ))
            })
        });
    }

    // Arrow's decimal division, which this kernel doesn't call. Its change is the run's noise.
    group.bench_function("control_arrow_decimal_div", |b| {
        b.iter(|| black_box(numeric::div(black_box(&c1), black_box(&c2))))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
