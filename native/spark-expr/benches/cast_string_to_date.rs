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

const BATCH_SIZE: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    let expr = Arc::new(Column::new("a", 0));
    let cast_to_date = Cast::new(
        expr,
        DataType::Date32,
        SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        None,
        None,
    );

    let canonical =
        create_batch(|i| format!("{:04}-{:02}-{:02}", 1970 + i % 60, i % 12 + 1, i % 28 + 1));
    let mixed = create_batch(|i| match i % 5 {
        0 => format!("{:04}-{:02}-{:02}", 1970 + i % 60, i % 12 + 1, i % 28 + 1),
        1 => format!("{}-{}-{} 12:34:56", 1970 + i % 60, i % 12 + 1, i % 28 + 1),
        2 => format!(
            "  {:04}-{:02}-{:02}  ",
            1900 + i % 200,
            i % 12 + 1,
            i % 28 + 1
        ),
        3 => format!("{:04}", 1970 + i % 60),
        _ => "not a date".to_string(),
    });

    let mut group = c.benchmark_group("cast_string_to_date");
    group.bench_function("canonical", |b| {
        b.iter(|| cast_to_date.evaluate(&canonical).unwrap());
    });
    group.bench_function("mixed", |b| {
        b.iter(|| cast_to_date.evaluate(&mixed).unwrap());
    });
    group.finish();
}

fn create_batch(f: impl Fn(usize) -> String) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let mut builder = StringBuilder::new();
    for i in 0..BATCH_SIZE {
        if i % 17 == 0 {
            builder.append_null();
        } else {
            builder.append_value(f(i));
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap()
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
