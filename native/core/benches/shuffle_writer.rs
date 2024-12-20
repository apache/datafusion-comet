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

use arrow_array::{builder::StringBuilder, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use comet::execution::shuffle::ShuffleWriterExec;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::{
    physical_plan::{common::collect, memory::MemoryExec, ExecutionPlan},
    prelude::SessionContext,
};
use datafusion_physical_expr::{expressions::Column, Partitioning};
use std::sync::Arc;
use tokio::runtime::Runtime;

fn criterion_benchmark(c: &mut Criterion) {
    let batch = create_batch();
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
    group.bench_function("shuffle_writer", |b| {
        let ctx = SessionContext::new();
        b.iter(|| {
            let task_ctx = ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let rt = Runtime::new().unwrap();
            criterion::black_box(rt.block_on(collect(stream)).unwrap());
        });
    });
}

fn create_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let mut b = StringBuilder::new();
    for i in 0..8192 {
        if i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("{i}"));
        }
    }
    let array = b.finish();

    RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap()
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
