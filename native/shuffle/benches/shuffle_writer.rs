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
use arrow::ipc::writer::CompressionContext;
use arrow::row::{RowConverter, SortField};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::expressions::{col, Column};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, Time};
use datafusion::{
    physical_plan::{common::collect, ExecutionPlan},
    prelude::SessionContext,
};
use datafusion_comet_shuffle::{
    CometPartitioning, CompressionCodec, LocalPartitionWriter, MultiPartitionShuffleRepartitioner,
    RoundRobinStrategy, ShuffleBlockWriter, ShufflePartitioner, ShufflePartitionerMetrics,
    ShuffleWriterExec,
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
            let mut compression_context = CompressionContext::default();
            b.iter(|| {
                buffer.clear();
                let mut cursor = Cursor::new(&mut buffer);
                w.write_batch(&batch, &mut cursor, &mut compression_context, &ipc_time)
                    .unwrap();
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
                    create_batches(8192, 10),
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
                let exec = create_shuffle_writer_exec(
                    compression_codec.clone(),
                    partitioning.clone(),
                    create_batches(8192, 10),
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

    // Single-partition writes, varying only how the input is chunked relative to the
    // session `batch_size` (8192). The two cases exercise different paths through the
    // writer's `BatchCoalescer`:
    //
    // - `rows_per_batch=8192` divides `batch_size` evenly, so each batch is handed to the
    //   coalescer at exactly `batch_size` and takes the zero-copy passthrough.
    // - `rows_per_batch=3000` does not, so batches accumulate in the coalescer's builders
    //   and are copied there before being emitted.
    //
    // Total rows are held roughly constant so the two are comparable.
    for (rows_per_batch, num_batches) in [(8192usize, 10usize), (3000, 27)] {
        group.bench_function(
            format!("shuffle_writer: end to end (partitioning=SinglePartition, rows_per_batch={rows_per_batch})"),
            |b| {
                let ctx = SessionContext::new();
                let exec = create_shuffle_writer_exec(
                    CompressionCodec::None,
                    CometPartitioning::SinglePartition,
                    create_batches(rows_per_batch, num_batches),
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

    // RoundRobin on a wide nested schema: compares the row-level hash-all-columns strategy
    // against the whole-batch strategy. The nested schema is where the win is largest:
    // `create_murmur3_hashes` recurses into every Struct child per row on the row-level path,
    // while the whole-batch path does one mod per batch and never inspects row contents.
    // 40 top-level struct columns of shallow depth reflect the real-world shape where this
    // optimization matters (event log workload was ~194 nested columns).
    let num_partitions = 50usize;
    let wide_batches: Vec<RecordBatch> = (0..8).map(|_| nested_schema_batch(8192, 40, 2)).collect();
    let wide_schema = wide_batches[0].schema();
    let round_robin_strategies = [
        (
            "hash_all_columns",
            RoundRobinStrategy::HashAll {
                max_hash_columns: 0,
            },
        ),
        ("whole_batch", RoundRobinStrategy::WholeBatch),
    ];
    for (label, strategy) in &round_robin_strategies {
        group.bench_function(
            format!("shuffle_writer: RoundRobin nested schema (strategy={label})"),
            |b| {
                let ctx = SessionContext::new();
                let rt = Runtime::new().unwrap();
                let exec = create_shuffle_writer_exec(
                    CompressionCodec::None,
                    CometPartitioning::RoundRobin(num_partitions, strategy.clone()),
                    wide_batches.clone(),
                );
                b.iter(|| {
                    let task_ctx = ctx.task_ctx();
                    let stream = exec.execute(0, task_ctx).unwrap();
                    rt.block_on(collect(stream)).unwrap();
                });
            },
        );
    }

    // Partitioning-only microbench. `insert_batch` runs partition assignment + index
    // buffering but never flushes to disk, so the two RoundRobin strategies can be compared
    // without the IPC encode + disk write cost that dominates the end-to-end bench above.
    for (label, strategy) in &round_robin_strategies {
        group.bench_function(
            format!("partitioning_only: RoundRobin nested schema (strategy={label})"),
            |b| {
                let rt = Runtime::new().unwrap();
                let batches = wide_batches.clone();
                let runtime_env = Arc::new(
                    RuntimeEnvBuilder::new()
                        .with_memory_limit(1024 * 1024 * 1024, 1.0)
                        .build()
                        .unwrap(),
                );
                let dir = tempfile::tempdir().unwrap();
                let data_path = dir.path().join("data.out").to_str().unwrap().to_string();
                let index_path = dir.path().join("index.out").to_str().unwrap().to_string();
                b.iter(|| {
                    let block_writer =
                        ShuffleBlockWriter::try_new(wide_schema.as_ref(), CompressionCodec::None)
                            .unwrap();
                    let writer = LocalPartitionWriter::try_new(
                        data_path.clone(),
                        index_path.clone(),
                        block_writer,
                        num_partitions,
                        8192,
                        1024 * 1024,
                        Arc::clone(&runtime_env),
                    )
                    .unwrap();
                    let metrics =
                        ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
                    let mut repartitioner = MultiPartitionShuffleRepartitioner::try_new(
                        0,
                        writer,
                        CometPartitioning::RoundRobin(num_partitions, strategy.clone()),
                        metrics,
                        Arc::clone(&runtime_env),
                        8192,
                        false,
                        None,
                    )
                    .unwrap();
                    rt.block_on(async {
                        for batch in &batches {
                            repartitioner.insert_batch(batch.clone()).await.unwrap();
                        }
                    });
                });
            },
        );
    }

    // End-to-end bench on a real on-disk parquet dataset at
    // /tmp/clickstream_data_smoke (Spark-written, snappy-compressed, wide nested
    // clickstream schema — the shape this optimization targets). Loaded once per
    // process; each iteration streams the batches through `ShuffleWriterExec`
    // for both RoundRobin strategies. Skipped if the dataset is absent.
    let clickstream_batches = load_parquet_dir_batches("/tmp/clickstream_data_smoke");
    if let Some(batches) = clickstream_batches {
        let clickstream_num_partitions = 50usize;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let num_cols = batches[0].num_columns();
        eprintln!(
            "clickstream bench: {} batches, {} rows total, {} columns",
            batches.len(),
            total_rows,
            num_cols,
        );
        for (label, strategy) in &round_robin_strategies {
            group.bench_function(
                format!("shuffle_writer: RoundRobin clickstream parquet (strategy={label})"),
                |b| {
                    let ctx = SessionContext::new();
                    let rt = Runtime::new().unwrap();
                    let exec = create_shuffle_writer_exec(
                        CompressionCodec::Lz4Frame,
                        CometPartitioning::RoundRobin(clickstream_num_partitions, strategy.clone()),
                        batches.clone(),
                    );
                    b.iter(|| {
                        let task_ctx = ctx.task_ctx();
                        let stream = exec.execute(0, task_ctx).unwrap();
                        rt.block_on(collect(stream)).unwrap();
                    });
                },
            );
        }
    } else {
        eprintln!("clickstream bench: /tmp/clickstream_data_smoke not found; skipping");
    }
}

/// Loads every `*.parquet` file under `dir` (recursively, one level deep for
/// partitioned datasets like `date=…/part-*.parquet`) and returns their record
/// batches at 8192 rows per batch. Returns `None` if the directory is missing
/// or contains no parquet files.
fn load_parquet_dir_batches(dir: &str) -> Option<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let root = std::path::Path::new(dir);
    if !root.exists() {
        return None;
    }

    let mut files: Vec<std::path::PathBuf> = Vec::new();
    let mut stack: Vec<std::path::PathBuf> = vec![root.to_path_buf()];
    while let Some(p) = stack.pop() {
        let entries = match std::fs::read_dir(&p) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("parquet"))
                .unwrap_or(false)
            {
                files.push(path);
            }
        }
    }
    files.sort();
    if files.is_empty() {
        return None;
    }

    let mut all = Vec::new();
    for path in files {
        let file = std::fs::File::open(&path).unwrap_or_else(|e| {
            panic!("failed to open {}: {e}", path.display());
        });
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap_or_else(|e| panic!("parquet builder for {}: {e}", path.display()))
            .with_batch_size(8192)
            .build()
            .unwrap_or_else(|e| panic!("parquet build for {}: {e}", path.display()));
        for batch in reader {
            all.push(batch.unwrap());
        }
    }
    Some(all)
}

fn create_shuffle_writer_exec(
    compression_codec: CompressionCodec,
    partitioning: CometPartitioning,
    batches: Vec<RecordBatch>,
) -> ShuffleWriterExec {
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
        1024 * 1024,
        None,
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

/// Benchmarks the per-block IPC encoding cost (schema + record batch) in isolation, using the
/// `None` codec so that compression does not obscure the schema-encoding cost. Covers a wide flat
/// schema and a deeply nested schema, where the schema flatbuffer is largest.
fn schema_encoding_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_block_schema_encoding");

    for (name, batch) in [
        ("flat", flat_schema_batch(8192)),
        ("nested", nested_schema_batch(8192, 4, 6)),
    ] {
        let writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::None).unwrap();
        let ipc_time = Time::default();
        let mut compression_context = CompressionContext::default();
        group.bench_function(format!("write_batch ({name} schema)"), |b| {
            let mut buffer = vec![];
            b.iter(|| {
                buffer.clear();
                let mut cursor = Cursor::new(&mut buffer);
                writer
                    .write_batch(&batch, &mut cursor, &mut compression_context, &ipc_time)
                    .unwrap();
            });
        });
    }

    group.finish();
}

/// A wide flat schema of primitive columns.
fn flat_schema_batch(num_rows: usize) -> RecordBatch {
    let num_cols = 50;
    let fields: Vec<Field> = (0..num_cols)
        .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let columns: Vec<Arc<dyn Array>> = (0..num_cols)
        .map(|i| {
            let values: Vec<i32> = (0..num_rows as i32).map(|r| r + i).collect();
            Arc::new(Int32Array::from(values)) as Arc<dyn Array>
        })
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

/// A schema of `num_cols` struct columns, each nested `depth` levels deep.
fn nested_schema_batch(num_rows: usize, num_cols: usize, depth: usize) -> RecordBatch {
    let mut fields: Vec<Field> = Vec::with_capacity(num_cols);
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(num_cols);
    for col in 0..num_cols {
        let array = nested_struct_array(num_rows, depth);
        fields.push(Field::new(
            format!("col{col}"),
            array.data_type().clone(),
            false,
        ));
        columns.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).unwrap()
}

/// Builds a struct array with a multi-field leaf, wrapped in `depth` single-field structs.
fn nested_struct_array(num_rows: usize, depth: usize) -> Arc<dyn Array> {
    use arrow::array::{Float64Array, Int64Array, StringArray, StructArray};

    // Leaf: struct<a: int64, b: utf8, c: float64>
    let mut array: Arc<dyn Array> = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![1_i64; num_rows])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("b", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["x"; num_rows])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("c", DataType::Float64, false)),
            Arc::new(Float64Array::from(vec![1.0_f64; num_rows])) as Arc<dyn Array>,
        ),
    ]));

    for level in 0..depth {
        let field = Arc::new(Field::new(
            format!("s{level}"),
            array.data_type().clone(),
            false,
        ));
        array = Arc::new(StructArray::from(vec![(field, array)]));
    }
    array
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark, schema_encoding_benchmark
}
criterion_main!(benches);
