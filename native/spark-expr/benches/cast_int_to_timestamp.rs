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

use arrow::array::builder::{Int16Builder, Int32Builder, Int64Builder, Int8Builder};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    // Test with UTC timezone
    let spark_cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
    let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

    let mut group = c.benchmark_group("cast_int_to_timestamp");

    // Int8 -> Timestamp
    let batch_i8 = create_int8_batch();
    let expr_i8 = Arc::new(Column::new("a", 0));
    let cast_i8_to_ts = Cast::new(
        expr_i8,
        timestamp_type.clone(),
        spark_cast_options.clone(),
        None,
        None,
    );
    group.bench_function("cast_i8_to_timestamp", |b| {
        b.iter(|| cast_i8_to_ts.evaluate(&batch_i8).unwrap());
    });

    // Int16 -> Timestamp
    let batch_i16 = create_int16_batch();
    let expr_i16 = Arc::new(Column::new("a", 0));
    let cast_i16_to_ts = Cast::new(
        expr_i16,
        timestamp_type.clone(),
        spark_cast_options.clone(),
        None,
        None,
    );
    group.bench_function("cast_i16_to_timestamp", |b| {
        b.iter(|| cast_i16_to_ts.evaluate(&batch_i16).unwrap());
    });

    // Int32 -> Timestamp
    let batch_i32 = create_int32_batch();
    let expr_i32 = Arc::new(Column::new("a", 0));
    let cast_i32_to_ts = Cast::new(
        expr_i32,
        timestamp_type.clone(),
        spark_cast_options.clone(),
        None,
        None,
    );
    group.bench_function("cast_i32_to_timestamp", |b| {
        b.iter(|| cast_i32_to_ts.evaluate(&batch_i32).unwrap());
    });

    // Int64 -> Timestamp
    let batch_i64 = create_int64_batch();
    let expr_i64 = Arc::new(Column::new("a", 0));
    let cast_i64_to_ts = Cast::new(
        expr_i64,
        timestamp_type.clone(),
        spark_cast_options.clone(),
        None,
        None,
    );
    group.bench_function("cast_i64_to_timestamp", |b| {
        b.iter(|| cast_i64_to_ts.evaluate(&batch_i64).unwrap());
    });

    group.finish();
}

fn create_int8_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, true)]));
    let mut b = Int8Builder::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(rand::random::<i8>());
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(b.finish())]).unwrap()
}

fn create_int16_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int16, true)]));
    let mut b = Int16Builder::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(rand::random::<i16>());
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(b.finish())]).unwrap()
}

fn create_int32_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let mut b = Int32Builder::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(rand::random::<i32>());
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(b.finish())]).unwrap()
}

fn create_int64_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let mut b = Int64Builder::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(rand::random::<i64>());
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(b.finish())]).unwrap()
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
