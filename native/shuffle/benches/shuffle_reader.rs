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

//! Shuffle read benchmarks: the per-block schema parse measured against a full block decode,
//! across column counts, rows per block, the default codec and no codec, and a dictionary-encoded
//! string column.

use arrow::array::{ArrayRef, DictionaryArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_plan::metrics::Time;
use datafusion_comet_shuffle::{
    read_ipc_compressed, read_ipc_compressed_validated, reset_schema_cache, CompressionCodec,
    ShuffleBlockWriter, ShuffleCodecContext,
};
use std::hint::black_box;
use std::io::Cursor;
use std::sync::Arc;

/// 8-byte compressed length plus 8-byte field count; `read_ipc_compressed` expects what follows.
const BLOCK_HEADER_LEN: usize = 16;

/// How the odd columns hold their strings.
#[derive(Clone, Copy)]
enum Strings {
    Plain,
    /// `Dictionary(Int32, Utf8)`: the block carries a dictionary batch before its record batch,
    /// as the JVM columnar shuffle writes for strings.
    Dictionary,
}

/// Alternating `Int64` and string columns.
fn schema_of(num_columns: usize, strings: Strings) -> SchemaRef {
    Arc::new(Schema::new(
        (0..num_columns)
            .map(|i| {
                let data_type = if i % 2 == 0 {
                    DataType::Int64
                } else {
                    match strings {
                        Strings::Plain => DataType::Utf8,
                        Strings::Dictionary => DataType::Dictionary(
                            Box::new(DataType::Int32),
                            Box::new(DataType::Utf8),
                        ),
                    }
                };
                Field::new(format!("column_{i}"), data_type, false)
            })
            .collect::<Vec<_>>(),
    ))
}

fn batch_of(num_columns: usize, num_rows: usize, strings: Strings) -> RecordBatch {
    let schema = schema_of(num_columns, strings);
    let columns = (0..num_columns)
        .map(|i| {
            if i % 2 == 0 {
                Arc::new(
                    (0..num_rows)
                        .map(|r| Some(r as i64))
                        .collect::<Int64Array>(),
                ) as ArrayRef
            } else {
                match strings {
                    Strings::Plain => Arc::new(
                        (0..num_rows)
                            .map(|r| Some(format!("value_{r}")))
                            .collect::<StringArray>(),
                    ) as ArrayRef,
                    // a small dictionary that every row's key points into
                    Strings::Dictionary => {
                        let values: Vec<String> =
                            (0..num_rows).map(|r| format!("value_{}", r % 16)).collect();
                        Arc::new(
                            values
                                .iter()
                                .map(String::as_str)
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef
                    }
                }
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, columns).unwrap()
}

/// One encoded block, with the 16-byte Comet header stripped.
fn encode_block(batch: &RecordBatch, codec: CompressionCodec) -> Vec<u8> {
    let writer = ShuffleBlockWriter::try_new(batch.schema().as_ref(), codec).unwrap();
    let mut context = ShuffleCodecContext::default();
    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);
    writer
        .write_batch(batch, &mut cursor, &mut context, &Time::default())
        .unwrap();
    buffer[BLOCK_HEADER_LEN..].to_vec()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_reader");

    // Lz4Frame is the default codec; None isolates the decode from decompression.
    for (codec_name, codec) in [
        ("none", CompressionCodec::None),
        ("lz4", CompressionCodec::Lz4Frame),
    ] {
        // rows per block shrink as partition count rises, so the small cases stand in for wide
        // shuffles
        for num_columns in [5usize, 50] {
            for num_rows in [64usize, 512, 8192] {
                let batch = batch_of(num_columns, num_rows, Strings::Plain);
                let block = encode_block(&batch, codec.clone());
                let id = format!("{codec_name}/{num_columns}col_{num_rows}row");
                bench_block(&mut group, &id, &block);
            }
        }

        // This 8,000-column schema uses about 1.4 MiB of the serialized-plus-parsed cache budget.
        // Compare warm and cold decoding here to catch accidental cache-admission cutoffs.
        let batch = batch_of(8000, 64, Strings::Plain);
        let block = encode_block(&batch, codec.clone());
        bench_block(&mut group, &format!("{codec_name}/8000col_64row"), &block);

        // the dictionary batch before every record batch, at a narrow and a wide block
        for num_rows in [64usize, 8192] {
            let batch = batch_of(5, num_rows, Strings::Dictionary);
            let block = encode_block(&batch, codec.clone());
            let id = format!("{codec_name}/5col_{num_rows}row_dict");
            bench_block(&mut group, &id, &block);
        }
    }

    // schema parse alone: `try_new` stops before the record batch. Skips the codec tag, so it
    // only applies to uncompressed blocks. A control arm: this change does not touch it.
    for num_columns in [5usize, 50] {
        for num_rows in [64usize, 512, 8192] {
            let batch = batch_of(num_columns, num_rows, Strings::Plain);
            let block = encode_block(&batch, CompressionCodec::None);
            group.bench_with_input(
                BenchmarkId::new(
                    "parse_schema_only",
                    format!("none/{num_columns}col_{num_rows}row"),
                ),
                &block,
                |b, block| {
                    b.iter(|| {
                        let mut ipc = &black_box(block)[4..];
                        black_box(StreamReader::try_new(&mut ipc, None).unwrap().schema())
                    })
                },
            );
        }
    }

    group.finish();
}

fn bench_block(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    id: &str,
    block: &[u8],
) {
    // full decode with the schema served from the cache after the first iteration
    group.bench_with_input(BenchmarkId::new("decode_block", id), block, |b, block| {
        b.iter(|| black_box(read_ipc_compressed(black_box(block)).unwrap()))
    });

    // the remote entry point: the same decode with array validation on
    group.bench_with_input(
        BenchmarkId::new("decode_block_validated", id),
        block,
        |b, block| b.iter(|| black_box(read_ipc_compressed_validated(black_box(block)).unwrap())),
    );

    // same decode with the cache cleared each iteration, so drift moves both arms together
    group.bench_with_input(
        BenchmarkId::new("decode_block_uncached", id),
        block,
        |b, block| {
            b.iter(|| {
                reset_schema_cache();
                black_box(read_ipc_compressed(black_box(block)).unwrap())
            })
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
