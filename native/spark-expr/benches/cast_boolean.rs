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

use arrow::array::{BooleanBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let expr = Arc::new(Column::new("a", 0));
    let boolean_batch = create_boolean_batch();
    let spark_cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
    Cast::new(expr.clone(), DataType::Int8, spark_cast_options.clone());
    let cast_to_i8 = Cast::new(expr.clone(), DataType::Int8, spark_cast_options.clone());
    let cast_to_i16 = Cast::new(expr.clone(), DataType::Int16, spark_cast_options.clone());
    let cast_to_i32 = Cast::new(expr.clone(), DataType::Int32, spark_cast_options.clone());
    let cast_to_i64 = Cast::new(expr.clone(), DataType::Int64, spark_cast_options.clone());
    let cast_to_f32 = Cast::new(expr.clone(), DataType::Float32, spark_cast_options.clone());
    let cast_to_f64 = Cast::new(expr.clone(), DataType::Float64, spark_cast_options.clone());
    let cast_to_str = Cast::new(expr, DataType::Utf8, spark_cast_options);

    let mut group = c.benchmark_group(format!("cast_bool_to_int"));
    group.bench_function("i8", |b| {
        b.iter(|| cast_to_i8.evaluate(&boolean_batch).unwrap());
    });
    group.bench_function("i16", |b| {
        b.iter(|| cast_to_i16.evaluate(&boolean_batch).unwrap());
    });
    group.bench_function("i32", |b| {
        b.iter(|| cast_to_i32.evaluate(&boolean_batch).unwrap());
    });
    group.bench_function("i64", |b| {
        b.iter(|| cast_to_i64.evaluate(&boolean_batch).unwrap());
    });
    group.bench_function("f32", |b| {
        b.iter(|| cast_to_f32.evaluate(&boolean_batch).unwrap());
    });
    group.bench_function("f64", |b| {
        b.iter(|| cast_to_f64.evaluate(&boolean_batch).unwrap());
    });
    group.bench_function("str", |b| {
        b.iter(|| cast_to_str.evaluate(&boolean_batch).unwrap());
    });
}

fn create_boolean_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
    let mut b = BooleanBuilder::with_capacity(1000);
    for i in 0..1000 {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(rand::random::<bool>());
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
