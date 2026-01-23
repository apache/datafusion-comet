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

use arrow::array::{builder::StringBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let small_int_batch = create_small_int_string_batch();
    let int_batch = create_int_string_batch();
    let decimal_batch = create_decimal_string_batch();
    let expr = Arc::new(Column::new("a", 0));

    for (mode, mode_name) in [
        (EvalMode::Legacy, "legacy"),
        (EvalMode::Ansi, "ansi"),
        (EvalMode::Try, "try"),
    ] {
        let spark_cast_options = SparkCastOptions::new(mode, "", false);
        let cast_to_i8 = Cast::new(expr.clone(), DataType::Int8, spark_cast_options.clone());
        let cast_to_i16 = Cast::new(expr.clone(), DataType::Int16, spark_cast_options.clone());
        let cast_to_i32 = Cast::new(expr.clone(), DataType::Int32, spark_cast_options.clone());
        let cast_to_i64 = Cast::new(expr.clone(), DataType::Int64, spark_cast_options);

        let mut group = c.benchmark_group(format!("cast_string_to_int/{}", mode_name));
        group.bench_function("i8", |b| {
            b.iter(|| cast_to_i8.evaluate(&small_int_batch).unwrap());
        });
        group.bench_function("i16", |b| {
            b.iter(|| cast_to_i16.evaluate(&small_int_batch).unwrap());
        });
        group.bench_function("i32", |b| {
            b.iter(|| cast_to_i32.evaluate(&int_batch).unwrap());
        });
        group.bench_function("i64", |b| {
            b.iter(|| cast_to_i64.evaluate(&int_batch).unwrap());
        });
        group.finish();
    }

    // Benchmark decimal truncation (Legacy mode only)
    let spark_cast_options = SparkCastOptions::new(EvalMode::Legacy, "", false);
    let cast_to_i32 = Cast::new(expr.clone(), DataType::Int32, spark_cast_options.clone());
    let cast_to_i64 = Cast::new(expr.clone(), DataType::Int64, spark_cast_options);

    let mut group = c.benchmark_group("cast_string_to_int/legacy_decimals");
    group.bench_function("i32", |b| {
        b.iter(|| cast_to_i32.evaluate(&decimal_batch).unwrap());
    });
    group.bench_function("i64", |b| {
        b.iter(|| cast_to_i64.evaluate(&decimal_batch).unwrap());
    });
    group.finish();
}

/// Create batch with small integer strings that fit in i8 range (for i8/i16 benchmarks)
fn create_small_int_string_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let mut b = StringBuilder::new();
    for i in 0..1000 {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("{}", rand::random::<i8>()));
        }
    }
    let array = b.finish();
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

/// Create batch with valid integer strings (works for all eval modes)
fn create_int_string_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let mut b = StringBuilder::new();
    for i in 0..1000 {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("{}", rand::random::<i32>()));
        }
    }
    let array = b.finish();
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

/// Create batch with decimal strings (for Legacy mode decimal truncation)
fn create_decimal_string_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let mut b = StringBuilder::new();
    for i in 0..1000 {
        if i % 10 == 0 {
            b.append_null();
        } else {
            // Generate integers with decimal portions to test truncation
            let int_part: i32 = rand::random();
            let dec_part: u32 = rand::random::<u32>() % 1000;
            b.append_value(format!("{}.{}", int_part, dec_part));
        }
    }
    let array = b.finish();
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
