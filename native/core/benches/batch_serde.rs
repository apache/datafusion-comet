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

use arrow_array::{builder::StringBuilder, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use comet::execution::shuffle::batch_serde::{
    read_batch_fast, read_batch_ipc, write_batch_fast, write_batch_ipc,
};
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_serde");
    group.bench_function("batch_serde: write_batch_fast (with schema serde)", |b| {
        let batch = create_batch();
        let mut buffer = vec![];
        b.iter(|| {
            buffer.clear();
            criterion::black_box(write_batch_fast(&batch, &mut buffer, true).unwrap())
        });
    });
    group.bench_function("batch_serde: read_batch_fast (with schema serde)", |b| {
        let batch = create_batch();
        let mut buffer = vec![];
        write_batch_fast(&batch, &mut buffer, true).unwrap();
        b.iter(|| criterion::black_box(read_batch_fast(&buffer, None).unwrap()));
    });
    group.bench_function("batch_serde: write_batch_fast (no schema serde)", |b| {
        let batch = create_batch();
        let mut buffer = vec![];
        b.iter(|| {
            buffer.clear();
            criterion::black_box(write_batch_fast(&batch, &mut buffer, false).unwrap())
        });
    });
    group.bench_function("batch_serde: read_batch_fast (no schema serde)", |b| {
        let batch = create_batch();
        let mut buffer = vec![];
        write_batch_fast(&batch, &mut buffer, false).unwrap();
        b.iter(|| criterion::black_box(read_batch_fast(&buffer, Some(batch.schema())).unwrap()));
    });
    group.bench_function("batch_serde: write_batch_ipc", |b| {
        let batch = create_batch();
        let mut buffer = vec![];
        b.iter(|| {
            buffer.clear();
            criterion::black_box(write_batch_ipc(&batch, &mut buffer).unwrap())
        });
    });
    group.bench_function("batch_serde: read_batch_ipc", |b| {
        let batch = create_batch();
        let mut buffer = vec![];
        write_batch_ipc(&batch, &mut buffer).unwrap();
        b.iter(|| criterion::black_box(read_batch_ipc(&buffer).unwrap()));
    });
}

fn create_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Utf8, true),
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Utf8, true),
    ]));
    let mut array = StringBuilder::new();
    for i in 0..8192 {
        array.append_value(format!("{i}"));
    }
    let array: ArrayRef = Arc::new(array.finish());
    let array = Arc::new(array);
    RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::clone(&array), Arc::clone(&array), array],
    )
    .unwrap()
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
