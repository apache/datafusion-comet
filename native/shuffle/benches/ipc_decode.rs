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

//! Measures shuffle frame decoding with a reused [`ShuffleDecodeContext`] against a fresh
//! context per frame (the cost profile of creating decode state for every block).

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::metrics::Time;
use datafusion_comet_shuffle::{
    read_ipc_compressed_with, CompressionCodec, ShuffleBlockWriter, ShuffleCodecContext,
    ShuffleDecodeContext,
};
use std::hint::black_box;
use std::io::{Cursor, Write};
use std::sync::Arc;

const SMALL_ROWS: usize = 8192;
const SMALL_FRAMES: usize = 64;
const LARGE_ROWS: usize = SMALL_ROWS * SMALL_FRAMES;
/// One wide-window frame is inserted after every this many small frames in the
/// over-cap recovery scenario.
const WIDE_FRAME_INTERVAL: usize = 16;

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn make_batch(start: usize, rows: usize) -> RecordBatch {
    let ids = Int64Array::from_iter_values((start..start + rows).map(|i| i as i64));
    let names = StringArray::from_iter(
        (start..start + rows).map(|i| (i % 7 != 0).then(|| format!("row-{i}-payload"))),
    );
    RecordBatch::try_new(test_schema(), vec![Arc::new(ids), Arc::new(names)]).unwrap()
}

/// Encodes `batch` with the crate's block writer and strips the 16-byte block header
/// (length + field count), leaving the codec tag + payload that the decoder consumes.
fn make_frame(batch: &RecordBatch, codec: CompressionCodec) -> Vec<u8> {
    let writer = ShuffleBlockWriter::try_new(batch.schema().as_ref(), codec).unwrap();
    let mut buffer = Vec::new();
    writer
        .write_batch(
            batch,
            &mut Cursor::new(&mut buffer),
            &mut ShuffleCodecContext::default(),
            &Time::default(),
        )
        .unwrap();
    buffer.split_off(16)
}

/// A zstd frame written by a streaming level-19 encode with no pledged source size, so its
/// header advertises the level's full default window. Decoding it grows the context's
/// workspace past the retention cap, forcing the context to be dropped and re-created.
fn make_wide_window_frame(batch: &RecordBatch) -> Vec<u8> {
    let uncompressed = make_frame(batch, CompressionCodec::None);
    let ipc_payload = &uncompressed[4..];
    let mut bytes = b"ZSTD".to_vec();
    let mut encoder = zstd::Encoder::new(&mut bytes, 19).unwrap();
    encoder.write_all(ipc_payload).unwrap();
    encoder.finish().unwrap();
    bytes
}

fn small_frames(codec: CompressionCodec) -> Vec<(Vec<u8>, RecordBatch)> {
    (0..SMALL_FRAMES)
        .map(|i| {
            let batch = make_batch(i * SMALL_ROWS, SMALL_ROWS);
            (make_frame(&batch, codec.clone()), batch)
        })
        .collect()
}

fn scenario_small_zstd() -> Vec<(Vec<u8>, RecordBatch)> {
    small_frames(CompressionCodec::Zstd(3))
}

fn scenario_large_frame() -> Vec<(Vec<u8>, RecordBatch)> {
    let batch = make_batch(0, LARGE_ROWS);
    vec![(make_frame(&batch, CompressionCodec::Zstd(3)), batch)]
}

fn scenario_over_cap_recovery() -> Vec<(Vec<u8>, RecordBatch)> {
    let wide_batch = make_batch(0, SMALL_ROWS);
    let wide_frame = make_wide_window_frame(&wide_batch);
    let mut frames = Vec::new();
    for (i, entry) in scenario_small_zstd().into_iter().enumerate() {
        frames.push(entry);
        if (i + 1) % WIDE_FRAME_INTERVAL == 0 {
            frames.push((wide_frame.clone(), wide_batch.clone()));
        }
    }
    frames
}

fn scenario_none() -> Vec<(Vec<u8>, RecordBatch)> {
    small_frames(CompressionCodec::None)
}

/// Both decode variants must produce the exact batches the frames were built from.
fn assert_variants_decode_identically(frames: &[(Vec<u8>, RecordBatch)]) {
    let mut reused = ShuffleDecodeContext::default();
    for (frame, expected) in frames {
        assert_eq!(
            &read_ipc_compressed_with(&mut reused, frame).unwrap(),
            expected
        );
        let mut fresh = ShuffleDecodeContext::default();
        assert_eq!(
            &read_ipc_compressed_with(&mut fresh, frame).unwrap(),
            expected
        );
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let scenarios = [
        ("small_frames_zstd3", scenario_small_zstd()),
        ("large_frame_zstd3", scenario_large_frame()),
        ("over_cap_recovery_zstd", scenario_over_cap_recovery()),
        ("small_frames_none", scenario_none()),
    ];

    let mut group = c.benchmark_group("ipc_decode");
    for (name, frames) in &scenarios {
        assert_variants_decode_identically(frames);

        group.bench_function(format!("{name}/reused"), |b| {
            let mut context = ShuffleDecodeContext::default();
            b.iter(|| {
                for (frame, _) in frames {
                    black_box(read_ipc_compressed_with(&mut context, frame).unwrap());
                }
            });
        });
        group.bench_function(format!("{name}/fresh"), |b| {
            b.iter(|| {
                for (frame, _) in frames {
                    let mut context = ShuffleDecodeContext::default();
                    black_box(read_ipc_compressed_with(&mut context, frame).unwrap());
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
