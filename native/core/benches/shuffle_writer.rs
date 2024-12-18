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
use comet::execution::shuffle::{calculate_partition_ids, write_ipc_compressed, ShuffleWriterExec};
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
    let mut batches = Vec::new();
    for _ in 0..10 {
        batches.push(batch.clone());
    }
    let partitions = &[batches];
    let exec = ShuffleWriterExec::try_new(
        Arc::new(MemoryExec::try_new(partitions, batch.schema(), None).unwrap()),
        Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 16),
        "/tmp/data.out".to_string(),
        "/tmp/index.out".to_string(),
    )
    .unwrap();

    let mut group = c.benchmark_group("shuffle_writer");
    group.bench_function("shuffle_writer: calculate partition ids", |b| {
        let batch = create_batch(8192, true);
        let arrays = batch.columns().to_vec();
        let mut hashes_buf = vec![0; batch.num_rows()];
        let mut partition_ids = vec![0; batch.num_rows()];
        b.iter(|| {
            calculate_partition_ids(&arrays, 200, &mut hashes_buf, &mut partition_ids).unwrap();
        });
    });
    group.bench_function("shuffle_writer: encode and compress", |b| {
        let batch = create_batch(8192, true);
        let mut buffer = vec![];
        let mut cursor = Cursor::new(&mut buffer);
        let ipc_time = Time::default();
        b.iter(|| write_ipc_compressed(&batch, &mut cursor, &ipc_time));
    });
    group.bench_function("shuffle_writer: end to end", |b| {
        let ctx = SessionContext::new();
        b.iter(|| {
            let task_ctx = ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let rt = Runtime::new().unwrap();
            criterion::black_box(rt.block_on(collect(stream)).unwrap());
        });
    });
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
