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

use arrow::array::{builder::Int32Builder, Decimal128Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    let batch = create_int32_batch();
    let expr = Arc::new(Column::new("a", 0));
    let spark_cast_options = SparkCastOptions::new_without_timezone(EvalMode::Legacy, false);
    let cast_i32_to_i8 = Cast::new(
        expr.clone(),
        DataType::Int8,
        spark_cast_options.clone(),
        None,
        None,
    );
    let cast_i32_to_i16 = Cast::new(
        expr.clone(),
        DataType::Int16,
        spark_cast_options.clone(),
        None,
        None,
    );
    let cast_i32_to_i64 = Cast::new(expr, DataType::Int64, spark_cast_options, None, None);

    let mut group = c.benchmark_group("cast_int_to_int");
    group.bench_function("cast_i32_to_i8", |b| {
        b.iter(|| cast_i32_to_i8.evaluate(&batch).unwrap());
    });
    group.bench_function("cast_i32_to_i16", |b| {
        b.iter(|| cast_i32_to_i16.evaluate(&batch).unwrap());
    });
    group.bench_function("cast_i32_to_i64", |b| {
        b.iter(|| cast_i32_to_i64.evaluate(&batch).unwrap());
    });
    group.finish();

    let decimal_cast = |data_type| {
        Cast::new(
            Arc::new(Column::new("a", 0)),
            data_type,
            SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
            None,
            None,
        )
    };
    let decimal_to_f64 = decimal_cast(DataType::Float64);
    let decimal_to_f32 = decimal_cast(DataType::Float32);
    let cases = [
        (
            "decimal18_to_f64",
            create_decimal128_batch(18, 0, 1_i128 << 53),
            &decimal_to_f64,
        ),
        (
            "decimal18_to_f64_nulls",
            create_decimal128_batch(18, 5, 1_i128 << 53),
            &decimal_to_f64,
        ),
        (
            "decimal38_to_f64",
            create_decimal128_batch(38, 0, 10_i128.pow(37)),
            &decimal_to_f64,
        ),
        (
            "decimal38_to_f64_nulls",
            create_decimal128_batch(38, 5, 10_i128.pow(37)),
            &decimal_to_f64,
        ),
        (
            "decimal12_to_f32",
            create_decimal128_batch(12, 0, 1_i128 << 24),
            &decimal_to_f32,
        ),
        (
            "decimal12_to_f32_nulls",
            create_decimal128_batch(12, 5, 1_i128 << 24),
            &decimal_to_f32,
        ),
    ];

    let mut group = c.benchmark_group("cast_decimal_scale_zero");
    for (name, batch, cast) in cases {
        group.bench_function(name, |b| {
            b.iter(|| black_box(cast.evaluate(black_box(&batch)).unwrap()))
        });
    }
    group.finish();
}

fn create_int32_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let mut b = Int32Builder::new();
    for i in 0..1000 {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(rand::random::<i32>());
        }
    }
    let array = b.finish();

    RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
}

fn create_decimal128_batch(precision: u8, null_every: usize, base: i128) -> RecordBatch {
    let array: Decimal128Array = (0..NUM_ROWS)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else {
                let magnitude = base + i as i128;
                Some(if i % 2 == 0 { magnitude } else { -magnitude })
            }
        })
        .collect::<Decimal128Array>()
        .with_precision_and_scale(precision, 0)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Decimal128(precision, 0),
        true,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
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
