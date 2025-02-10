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

use arrow_array::builder::Int32Builder;
use arrow_array::{builder::StringBuilder, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use comet::execution::shuffle::{write_ipc_compressed, CompressionCodec, ShuffleWriterExec};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::metrics::Time;
use datafusion::{
    physical_plan::{common::collect, memory::MemoryExec, ExecutionPlan},
    prelude::SessionContext,
};
use datafusion_physical_expr::{expressions::Column, Partitioning};
use std::io::Cursor;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_writer");
    group.bench_function("shuffle_writer: encode (no compression))", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = vec![];
        let ipc_time = Time::default();
        b.iter(|| {
            buffer.clear();
            let mut cursor = Cursor::new(&mut buffer);
            write_ipc_compressed(&batch, &mut cursor, &CompressionCodec::None, &ipc_time)
        });
    });
    group.bench_function("shuffle_writer: encode and compress (snappy)", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = vec![];
        let ipc_time = Time::default();
        b.iter(|| {
            buffer.clear();
            let mut cursor = Cursor::new(&mut buffer);
            write_ipc_compressed(&batch, &mut cursor, &CompressionCodec::Snappy, &ipc_time)
        });
    });
    group.bench_function("shuffle_writer: encode and compress (lz4)", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = vec![];
        let ipc_time = Time::default();
        b.iter(|| {
            buffer.clear();
            let mut cursor = Cursor::new(&mut buffer);
            write_ipc_compressed(&batch, &mut cursor, &CompressionCodec::Lz4Frame, &ipc_time)
        });
    });
    group.bench_function("shuffle_writer: encode and compress (zstd level 1)", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = vec![];
        let ipc_time = Time::default();
        b.iter(|| {
            buffer.clear();
            let mut cursor = Cursor::new(&mut buffer);
            write_ipc_compressed(&batch, &mut cursor, &CompressionCodec::Zstd(1), &ipc_time)
        });
    });
    group.bench_function("shuffle_writer: encode and compress (zstd level 6)", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = vec![];
        let ipc_time = Time::default();
        b.iter(|| {
            buffer.clear();
            let mut cursor = Cursor::new(&mut buffer);
            write_ipc_compressed(&batch, &mut cursor, &CompressionCodec::Zstd(6), &ipc_time)
        });
    });
    group.bench_function("shuffle_writer: end to end", |b| {
        let ctx = SessionContext::new();
        let exec = create_shuffle_writer_exec(CompressionCodec::Zstd(1));
        b.iter(|| {
            let task_ctx = ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let rt = Runtime::new().unwrap();
            criterion::black_box(rt.block_on(collect(stream)).unwrap());
        });
    });
}

fn create_shuffle_writer_exec(compression_codec: CompressionCodec) -> ShuffleWriterExec {
    let batches = create_batches(8192, 10);
    let schema = batches[0].schema();
    let partitions = &[batches];
    ShuffleWriterExec::try_new(
        Arc::new(MemoryExec::try_new(partitions, schema, None).unwrap()),
        Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 16),
        compression_codec,
        "/tmp/data.out".to_string(),
        "/tmp/index.out".to_string(),
    )
    .unwrap()
}

fn create_batches(size: usize, count: usize) -> Vec<RecordBatch> {
    let batch = create_batch(size, true);
    let mut batches = Vec::new();
    for _ in 0..count {
        batches.push(batch.clone());
    }
    batches
}

fn create_batch(num_rows: usize, allow_nulls: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, true),
        Field::new("c1", DataType::Utf8, true),
    ]));
    let mut a = Int32Builder::new();
    let mut b = StringBuilder::new();
    for i in 0..num_rows {
        a.append_value(i as i32);
        if allow_nulls && i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("this is string number {i}"));
        }
    }
    let a = a.finish();
    let b = b.finish();
    RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap()
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
