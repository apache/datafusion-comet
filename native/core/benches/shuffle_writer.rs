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

use arrow::array::builder::{Date32Builder, Decimal128Builder, Int32Builder};
use arrow::array::{builder::StringBuilder, Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::row::{RowConverter, SortField};
use comet::execution::shuffle::{
    CometPartitioning, CompressionCodec, ShuffleBlockWriter, ShuffleWriterExec,
};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::{col, Column};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::metrics::Time;
use datafusion::{
    physical_plan::{common::collect, ExecutionPlan},
    prelude::SessionContext,
};
use itertools::Itertools;
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
        let name = format!("shuffle_writer: write encoded (compression={compression_codec:?})");
        group.bench_function(name, |b| {
            let mut buffer = vec![];
            let ipc_time = Time::default();
            let w =
                ShuffleBlockWriter::try_new(&batch.schema(), compression_codec.clone()).unwrap();
            b.iter(|| {
                buffer.clear();
                let mut cursor = Cursor::new(&mut buffer);
                w.write_batch(&batch, &mut cursor, &ipc_time).unwrap();
            });
        });
    }

    for compression_codec in [
        CompressionCodec::None,
        CompressionCodec::Lz4Frame,
        CompressionCodec::Snappy,
        CompressionCodec::Zstd(1),
        CompressionCodec::Zstd(6),
    ] {
        group.bench_function(
            format!("shuffle_writer: end to end (compression = {compression_codec:?})"),
            |b| {
                let ctx = SessionContext::new();
                let exec = create_shuffle_writer_exec(
                    compression_codec.clone(),
                    CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], 16),
                );
                b.iter(|| {
                    let task_ctx = ctx.task_ctx();
                    let stream = exec.execute(0, task_ctx).unwrap();
                    let rt = Runtime::new().unwrap();
                    rt.block_on(collect(stream)).unwrap();
                });
            },
        );
    }

    let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col("c0", batch.schema().as_ref()).unwrap(),
    )])
    .unwrap();

    let sort_fields: Vec<SortField> = batch
        .columns()
        .iter()
        .zip(&lex_ordering)
        .map(|(array, sort_expr)| {
            SortField::new_with_options(array.data_type().clone(), sort_expr.options)
        })
        .collect();
    let row_converter = RowConverter::new(sort_fields).unwrap();

    // These are hard-coded values based on the benchmark params of 8192 rows per batch, and 16
    // partitions. If these change, these values need to be recalculated, or bring over the
    // bounds-finding logic from shuffle_write_test in shuffle_writer.rs.
    let bounds_ints = vec![
        512, 1024, 1536, 2048, 2560, 3072, 3584, 4096, 4608, 5120, 5632, 6144, 6656, 7168, 7680,
    ];
    let bounds_array: Arc<dyn Array> = Arc::new(Int32Array::from(bounds_ints));
    let bounds_rows = row_converter
        .convert_columns(vec![bounds_array].as_slice())
        .unwrap();

    let owned_rows = bounds_rows.iter().map(|row| row.owned()).collect_vec();

    for partitioning in [
        CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], 16),
        CometPartitioning::RangePartitioning(lex_ordering, 16, Arc::new(row_converter), owned_rows),
    ] {
        let compression_codec = CompressionCodec::None;
        group.bench_function(
            format!("shuffle_writer: end to end (partitioning={partitioning:?})"),
            |b| {
                let ctx = SessionContext::new();
                let exec =
                    create_shuffle_writer_exec(compression_codec.clone(), partitioning.clone());
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

fn create_shuffle_writer_exec(
    compression_codec: CompressionCodec,
    partitioning: CometPartitioning,
) -> ShuffleWriterExec {
    let batches = create_batches(8192, 10);
    let schema = batches[0].schema();
    let partitions = &[batches];
    ShuffleWriterExec::try_new(
        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None).unwrap(),
        ))),
        partitioning,
        compression_codec,
        "/tmp/data.out".to_string(),
        "/tmp/index.out".to_string(),
        false,
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
