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

use arrow_array::builder::{Date32Builder, Decimal128Builder, Int32Builder};
use arrow_array::{builder::StringBuilder, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use comet::execution::shuffle::{CompressionCodec, ShuffleBlockWriter, ShuffleWriterExec};
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
    let batch = create_batch(8192, true);
    let mut group = c.benchmark_group("shuffle_writer");
    for compression_codec in &[
        CompressionCodec::None,
        CompressionCodec::Lz4Frame,
        CompressionCodec::Snappy,
        CompressionCodec::Zstd(1),
        CompressionCodec::Zstd(6),
    ] {
        for enable_fast_encoding in [true, false] {
            let name = format!("shuffle_writer: write encoded (enable_fast_encoding={enable_fast_encoding}, compression={compression_codec:?})");
            group.bench_function(name, |b| {
                let mut buffer = vec![];
                let ipc_time = Time::default();
                let w = ShuffleBlockWriter::try_new(
                    &batch.schema(),
                    enable_fast_encoding,
                    compression_codec.clone(),
                )
                .unwrap();
                b.iter(|| {
                    buffer.clear();
                    let mut cursor = Cursor::new(&mut buffer);
                    w.write_batch(&batch, &mut cursor, &ipc_time).unwrap();
                });
            });
        }
    }

    for compression_codec in [
        CompressionCodec::None,
        CompressionCodec::Lz4Frame,
        CompressionCodec::Zstd(1),
    ] {
        group.bench_function(
            format!("shuffle_writer: end to end (compression = {compression_codec:?}"),
            |b| {
                let ctx = SessionContext::new();
                let exec = create_shuffle_writer_exec(compression_codec.clone());
                b.iter(|| {
                    let task_ctx = ctx.task_ctx();
                    let stream = exec.execute(0, task_ctx).unwrap();
                    let rt = Runtime::new().unwrap();
                    rt.block_on(collect(stream)).unwrap();
                });
            },
        );
    }
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
        true,
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
        Field::new("c2", DataType::Date32, true),
        Field::new("c3", DataType::Decimal128(11, 2), true),
    ]));
    let mut a = Int32Builder::new();
    let mut b = StringBuilder::new();
    let mut c = Date32Builder::new();
    let mut d = Decimal128Builder::new()
        .with_precision_and_scale(11, 2)
        .unwrap();
    for i in 0..num_rows {
        a.append_value(i as i32);
        c.append_value(i as i32);
        d.append_value((i * 1000000) as i128);
        if allow_nulls && i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("this is string number {i}"));
        }
    }
    let a = a.finish();
    let b = b.finish();
    let c = c.finish();
    let d = d.finish();
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(d)],
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
