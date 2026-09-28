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
use criterion::{criterion_group, criterion_main, BatchSize, Bencher, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::{col, Column};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::metrics::Time;
use datafusion::{
    physical_plan::{common::collect, ExecutionPlan},
    prelude::SessionContext,
};
use datafusion_comet_shuffle::{
    bench_support::BenchRepartitioner, CometPartitioning, CompressionCodec, RoundRobinStrategy,
    ShuffleBlockWriter, ShuffleCodecContext, ShuffleWriterExec,
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
            let mut codec_context = ShuffleCodecContext::default();
            b.iter(|| {
                buffer.clear();
                let mut cursor = Cursor::new(&mut buffer);
                w.write_batch(&batch, &mut cursor, &mut codec_context, &ipc_time)
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
                bench_end_to_end(b, || {
                    create_shuffle_writer_exec(
                        compression_codec.clone(),
                        CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], 16),
                        8192,
                        10,
                    )
                })
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
                bench_end_to_end(b, || {
                    create_shuffle_writer_exec(
                        compression_codec.clone(),
                        partitioning.clone(),
                        8192,
                        10,
                    )
                })
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
                bench_end_to_end(b, || {
                    create_shuffle_writer_exec(
                        CompressionCodec::None,
                        CometPartitioning::SinglePartition,
                        rows_per_batch,
                        num_batches,
                    )
                })
            },
        );
    }
    group.finish();

    // High partition counts stress the per-partition write path (one short-lived
    // buffered writer per partition), which low counts barely exercise; compression
    // is disabled to isolate it. Few samples: each iteration is a full end-to-end
    // write across thousands of partitions.
    let mut high_partition_group = c.benchmark_group("shuffle_writer_high_partition");
    high_partition_group.sample_size(10);
    for num_partitions in [200usize, 2000, 8000] {
        high_partition_group.bench_function(
            format!("shuffle_writer: end to end (partitions={num_partitions}, compression=None)"),
            |b| {
                bench_end_to_end(b, || {
                    create_shuffle_writer_exec(
                        CompressionCodec::None,
                        CometPartitioning::Hash(
                            vec![Arc::new(Column::new("a", 0))],
                            num_partitions,
                        ),
                        8192,
                        10,
                    )
                })
            },
        );
    }
    high_partition_group.finish();
}

/// Times one execution of a freshly built writer per iteration. A `ShuffleWriterExec`
/// publishes its partition offsets once, so it cannot be re-executed; building it is
/// setup and stays outside the measurement.
fn bench_end_to_end(b: &mut Bencher, make_exec: impl Fn() -> ShuffleWriterExec) {
    let ctx = SessionContext::new();
    b.iter_batched(
        make_exec,
        |exec| {
            let stream = exec.execute(0, ctx.task_ctx()).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(collect(stream)).unwrap();
        },
        BatchSize::LargeInput,
    );
}

fn create_shuffle_writer_exec(
    compression_codec: CompressionCodec,
    partitioning: CometPartitioning,
    rows_per_batch: usize,
    num_batches: usize,
) -> ShuffleWriterExec {
    let batches = create_batches(rows_per_batch, num_batches);
    let schema = batches[0].schema();
    let partitions = &[batches];
    ShuffleWriterExec::try_new(
        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None).unwrap(),
        ))),
        partitioning,
        compression_codec,
        "/tmp/data.out".to_string(),
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
    create_batch_from(0, num_rows, allow_nulls)
}

/// [`create_batch`] with every value offset by `first_row`, so that successive batches differ.
fn create_batch_from(first_row: usize, num_rows: usize, allow_nulls: bool) -> RecordBatch {
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
    for i in first_row..first_row + num_rows {
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

/// Round robin placement in isolation, and then the gather it implies on flush.
///
/// The end-to-end benches above spend most of their time in IPC encoding and the file write,
/// which on a loaded disk swamps the difference between two placement strategies entirely.
/// These stop short of both. `place` times only the strategy and the per-partition index
/// buffering; `place+gather` adds the flush through a writer that discards its batches, which is
/// where a run-indexed partitioner diverges from a row-indexed one — the row-level scatter makes
/// `interleave_record_batch` walk every column and child again, and a run can instead be sliced,
/// or handed through untouched when it covers a whole buffered batch.
///
/// 8192 rows per batch into 50 output partitions, so `RowGroups(auto)` uses the 163-row group the
/// driver derives by default (`CometShuffleExchangeExec.resolvePositionalGroupRows`). `RowGroups(8192)` is the opposite extreme, one whole input batch per group,
/// where every run covers a buffered batch end to end and the gather copies nothing at all.
fn partitioning_benchmark(c: &mut Criterion) {
    const BATCH_SIZE: usize = 8192;
    const NUM_BATCHES: usize = 8;
    const NUM_PARTITIONS: usize = 50;

    let strategies = [
        (
            "HashAll",
            RoundRobinStrategy::HashAll {
                max_hash_columns: 0,
            },
        ),
        // Hashing only the leading column: not a candidate strategy, but it separates the cost
        // of recursing through every struct child from the cost of hashing at all.
        (
            "HashAll{1}",
            RoundRobinStrategy::HashAll {
                max_hash_columns: 1,
            },
        ),
        (
            "RowGroups(auto)",
            RoundRobinStrategy::RowGroups {
                start_partition: 0,
                group_rows: BATCH_SIZE / NUM_PARTITIONS,
                max_hash_columns: 0,
            },
        ),
        (
            "RowGroups(8192)",
            RoundRobinStrategy::RowGroups {
                start_partition: 0,
                group_rows: BATCH_SIZE,
                max_hash_columns: 0,
            },
        ),
    ];

    // `plain` is the flat schema the end-to-end benches use. `nested` is the shape that motivates
    // positional placement: 40 struct columns over a three-field leaf, so 120 leaf arrays for a
    // hash to recurse into and for a gather to walk. Every batch is built separately, rather than
    // cloned, so that none of them share buffers: a clone would be charged nothing by the
    // reservation, and the gather would keep rereading one cache-resident batch.
    let fixtures = [
        (
            "plain",
            (0..NUM_BATCHES)
                .map(|b| create_batch_from(b * BATCH_SIZE, BATCH_SIZE, true))
                .collect::<Vec<_>>(),
        ),
        (
            "nested",
            nested_batches(BATCH_SIZE, NUM_BATCHES, 40, 2, Fill::PerRow),
        ),
    ];

    let mut group = c.benchmark_group("shuffle_partitioning");
    for (schema, batches) in &fixtures {
        for (label, strategy) in &strategies {
            let partitioning = CometPartitioning::RoundRobin(NUM_PARTITIONS, strategy.clone());
            group.bench_function(format!("place ({schema}, {label})"), |b| {
                b.iter_batched(
                    || BenchRepartitioner::try_new(partitioning.clone(), BATCH_SIZE).unwrap(),
                    |mut repartitioner| {
                        repartitioner.place(batches).unwrap();
                        // Returned so that freeing the buffered batches and the partition index
                        // lands outside the measurement.
                        repartitioner
                    },
                    BatchSize::LargeInput,
                );
            });
            group.bench_function(format!("place+gather ({schema}, {label})"), |b| {
                b.iter_batched(
                    || BenchRepartitioner::try_new(partitioning.clone(), BATCH_SIZE).unwrap(),
                    |mut repartitioner| {
                        repartitioner.place(batches).unwrap();
                        repartitioner.gather().unwrap();
                        repartitioner
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

/// Benchmarks the per-block IPC encoding cost (schema + record batch) in isolation, using the
/// `None` codec so that compression does not obscure the schema-encoding cost. Covers a wide flat
/// schema and a deeply nested schema, where the schema flatbuffer is largest.
fn schema_encoding_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_block_schema_encoding");

    for (name, batch) in [
        ("flat", flat_schema_batch(8192)),
        ("nested", nested_schema_batch(8192)),
    ] {
        let writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::None).unwrap();
        let ipc_time = Time::default();
        let mut codec_context = ShuffleCodecContext::default();
        group.bench_function(format!("write_batch ({name} schema)"), |b| {
            let mut buffer = vec![];
            b.iter(|| {
                buffer.clear();
                let mut cursor = Cursor::new(&mut buffer);
                writer
                    .write_batch(&batch, &mut cursor, &mut codec_context, &ipc_time)
                    .unwrap();
            });
        });
    }

    group.finish();
}

/// Compare context lifetimes through the production local block encoder. Both arms reuse
/// the writer and destination capacity; only the Arrow IPC context lifetime differs.
fn ipc_context_reuse_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_ipc_context");
    for rows in [128, 8192] {
        for (name, batch) in [
            ("mixed", create_batch(rows, true)),
            ("flat", flat_schema_batch(rows)),
            ("nested", nested_schema_batch(rows)),
        ] {
            for codec in [
                CompressionCodec::None,
                CompressionCodec::Lz4Frame,
                CompressionCodec::Snappy,
                CompressionCodec::Zstd(1),
            ] {
                let writer = ShuffleBlockWriter::try_new(&batch.schema(), codec.clone()).unwrap();
                for reuse in [false, true] {
                    let lifetime = if reuse { "reused" } else { "fresh" };
                    group.bench_function(format!("{name}/{rows}/{codec:?}/{lifetime}"), |b| {
                        let ipc_time = Time::default();
                        let mut context = ShuffleCodecContext::default();
                        let mut buffer = Vec::new();
                        // Warm the output buffer and the retained context before timing.
                        writer
                            .write_batch(
                                &batch,
                                &mut Cursor::new(&mut buffer),
                                &mut context,
                                &ipc_time,
                            )
                            .unwrap();
                        b.iter(|| {
                            if !reuse {
                                context = ShuffleCodecContext::default();
                            }
                            buffer.clear();
                            writer
                                .write_batch(
                                    &batch,
                                    &mut Cursor::new(&mut buffer),
                                    &mut context,
                                    &ipc_time,
                                )
                                .unwrap();
                            std::hint::black_box(&buffer);
                        });
                    });
                }
            }
        }
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

/// A schema of several deeply nested struct columns.
fn nested_schema_batch(num_rows: usize) -> RecordBatch {
    nested_batch(num_rows, 4, 6, Fill::Constant)
}

/// How a nested fixture fills its leaves.
#[derive(Clone, Copy)]
enum Fill {
    /// One value repeated down every leaf. Cheap to build, and enough for the encoding benches
    /// that only care about how much there is to encode. Useless for partitioning: identical
    /// rows hash alike, so a hash strategy would put the whole input on one output partition
    /// and never perform the scatter that a gather has to undo.
    Constant,
    /// A distinct value per row, so hash placement spreads rows across the output partitions the
    /// way real data does.
    PerRow,
}

/// `count` separately built batches, each filled from where the previous one left off.
fn nested_batches(
    num_rows: usize,
    count: usize,
    num_cols: usize,
    depth: usize,
    fill: Fill,
) -> Vec<RecordBatch> {
    (0..count)
        .map(|b| nested_batch_from(b * num_rows, num_rows, num_cols, depth, fill))
        .collect()
}

fn nested_batch(num_rows: usize, num_cols: usize, depth: usize, fill: Fill) -> RecordBatch {
    nested_batch_from(0, num_rows, num_cols, depth, fill)
}

fn nested_batch_from(
    first_row: usize,
    num_rows: usize,
    num_cols: usize,
    depth: usize,
    fill: Fill,
) -> RecordBatch {
    let mut fields: Vec<Field> = Vec::with_capacity(num_cols);
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(num_cols);
    for col in 0..num_cols {
        let array = nested_struct_array(first_row, num_rows, depth, fill);
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

/// Builds a struct array with a multi-field leaf, wrapped in `depth` single-field structs. Under
/// [`Fill::PerRow`] the leaf values run from `first_row`.
fn nested_struct_array(
    first_row: usize,
    num_rows: usize,
    depth: usize,
    fill: Fill,
) -> Arc<dyn Array> {
    use arrow::array::{Float64Array, Int64Array, StringArray, StructArray};

    let (ints, strings, floats): (Vec<i64>, Vec<String>, Vec<f64>) = match fill {
        Fill::Constant => (
            vec![1_i64; num_rows],
            vec!["x".to_string(); num_rows],
            vec![1.0_f64; num_rows],
        ),
        Fill::PerRow => {
            let rows = first_row..first_row + num_rows;
            (
                rows.clone().map(|row| row as i64).collect(),
                rows.clone().map(|row| format!("value {row}")).collect(),
                rows.map(|row| row as f64 * 1.5).collect(),
            )
        }
    };

    // Leaf: struct<a: int64, b: utf8, c: float64>
    let mut array: Arc<dyn Array> = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int64, false)),
            Arc::new(Int64Array::from(ints)) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("b", DataType::Utf8, false)),
            Arc::new(StringArray::from(strings)) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("c", DataType::Float64, false)),
            Arc::new(Float64Array::from(floats)) as Arc<dyn Array>,
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
    targets = criterion_benchmark, partitioning_benchmark, schema_encoding_benchmark, ipc_context_reuse_benchmark
}
criterion_main!(benches);
