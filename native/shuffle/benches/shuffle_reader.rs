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
//! across column counts and rows per block.

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::IpcWriteContext;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_plan::metrics::Time;
use datafusion_comet_shuffle::{read_ipc_compressed, CompressionCodec, ShuffleBlockWriter};
use std::hint::black_box;
use std::io::Cursor;
use std::sync::Arc;

/// 8-byte compressed length plus 8-byte field count; `read_ipc_compressed` expects what follows.
const BLOCK_HEADER_LEN: usize = 16;

/// Alternating `Int64` and `Utf8`.
fn schema_of(num_columns: usize) -> SchemaRef {
    Arc::new(Schema::new(
        (0..num_columns)
            .map(|i| {
                let data_type = if i % 2 == 0 {
                    DataType::Int64
                } else {
                    DataType::Utf8
                };
                Field::new(format!("column_{i}"), data_type, false)
            })
            .collect::<Vec<_>>(),
    ))
}

fn batch_of(num_columns: usize, num_rows: usize) -> RecordBatch {
    let schema = schema_of(num_columns);
    let columns = (0..num_columns)
        .map(|i| {
            if i % 2 == 0 {
                Arc::new(
                    (0..num_rows)
                        .map(|r| Some(r as i64))
                        .collect::<Int64Array>(),
                ) as arrow::array::ArrayRef
            } else {
                Arc::new(
                    (0..num_rows)
                        .map(|r| Some(format!("value_{r}")))
                        .collect::<StringArray>(),
                ) as arrow::array::ArrayRef
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, columns).unwrap()
}

/// One encoded block, with the 16-byte Comet header stripped.
fn encode_block(batch: &RecordBatch, codec: CompressionCodec) -> Vec<u8> {
    let writer = ShuffleBlockWriter::try_new(batch.schema().as_ref(), codec).unwrap();
    let mut context = IpcWriteContext::default();
    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);
    writer
        .write_batch(batch, &mut cursor, &mut context, &Time::default())
        .unwrap();
    buffer[BLOCK_HEADER_LEN..].to_vec()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("shuffle_reader");

    // rows per block shrink as partition count rises, so the small cases stand in for wide shuffles
    for num_columns in [5usize, 50] {
        for num_rows in [64usize, 512, 8192] {
            let batch = batch_of(num_columns, num_rows);
            let uncompressed = encode_block(&batch, CompressionCodec::None);

            let id = format!("{num_columns}col_{num_rows}row");

            // full decode: schema parse plus record batch
            group.bench_with_input(
                BenchmarkId::new("decode_block", &id),
                &uncompressed,
                |b, block| b.iter(|| black_box(read_ipc_compressed(black_box(block)).unwrap())),
            );

            // schema parse alone: `try_new` stops before the record batch. Skips the codec tag.
            group.bench_with_input(
                BenchmarkId::new("parse_schema_only", &id),
                &uncompressed,
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

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
