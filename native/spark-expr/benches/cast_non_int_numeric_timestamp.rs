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

use arrow::array::builder::{BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    let spark_cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
    let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

    let mut group = c.benchmark_group("cast_non_int_numeric_to_timestamp");

    // Float32 -> Timestamp
    let batch_f32 = create_float32_batch();
    let expr_f32 = Arc::new(Column::new("a", 0));
    let cast_f32_to_ts = Cast::new(expr_f32, timestamp_type.clone(), spark_cast_options.clone());
    group.bench_function("cast_f32_to_timestamp", |b| {
        b.iter(|| cast_f32_to_ts.evaluate(&batch_f32).unwrap());
    });

    // Float64 -> Timestamp
    let batch_f64 = create_float64_batch();
    let expr_f64 = Arc::new(Column::new("a", 0));
    let cast_f64_to_ts = Cast::new(expr_f64, timestamp_type.clone(), spark_cast_options.clone());
    group.bench_function("cast_f64_to_timestamp", |b| {
        b.iter(|| cast_f64_to_ts.evaluate(&batch_f64).unwrap());
    });

    // Boolean -> Timestamp
    let batch_bool = create_boolean_batch();
    let expr_bool = Arc::new(Column::new("a", 0));
    let cast_bool_to_ts = Cast::new(
        expr_bool,
        timestamp_type.clone(),
        spark_cast_options.clone(),
    );
    group.bench_function("cast_bool_to_timestamp", |b| {
        b.iter(|| cast_bool_to_ts.evaluate(&batch_bool).unwrap());
    });

    // Decimal128 -> Timestamp
    let batch_decimal = create_decimal128_batch();
    let expr_decimal = Arc::new(Column::new("a", 0));
    let cast_decimal_to_ts = Cast::new(
        expr_decimal,
        timestamp_type.clone(),
        spark_cast_options.clone(),
    );
    group.bench_function("cast_decimal_to_timestamp", |b| {
        b.iter(|| cast_decimal_to_ts.evaluate(&batch_decimal).unwrap());
    });

    group.finish();
}

fn create_float32_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));
    let mut b = Float32Builder::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(rand::random::<f32>());
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(b.finish())]).unwrap()
}

fn create_float64_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
    let mut b = Float64Builder::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(rand::random::<f64>());
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(b.finish())]).unwrap()
}

fn create_boolean_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
    let mut b = BooleanBuilder::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(rand::random::<bool>());
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(b.finish())]).unwrap()
}

fn create_decimal128_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Decimal128(18, 6),
        true,
    )]));
    let mut b = Decimal128Builder::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(i as i128 * 1_000_000);
        }
    }
    let array = b.finish().with_precision_and_scale(18, 6).unwrap();
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
