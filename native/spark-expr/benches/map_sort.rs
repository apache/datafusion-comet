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

//! Benchmarks for spark_map_sort.

use arrow::array::builder::{Int32Builder, MapBuilder, StringBuilder};
use arrow::array::{ArrayRef, MapArray};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_map_sort;
use std::hint::black_box;
use std::sync::Arc;

mod common;
#[path = "common/matched_maps.rs"]
mod matched_maps;

const BATCH_SIZE: usize = 8192;

/// Build a MapArray with `BATCH_SIZE` rows where each map has `entries_per_map` entries.
/// Keys are integers in reverse order so every map needs a real sort.
fn build_int_key_map(entries_per_map: usize) -> MapArray {
    let mut builder = MapBuilder::new(
        Some(common::map_field_names()),
        Int32Builder::new(),
        Int32Builder::new(),
    );
    for row in 0..BATCH_SIZE {
        for entry_idx in 0..entries_per_map {
            // Reverse order so input is unsorted; vary across rows so different maps differ.
            let key = (entries_per_map - entry_idx) as i32 + (row % 7) as i32;
            let value = entry_idx as i32;
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).unwrap();
    }
    builder.finish()
}

/// Same shape as `build_int_key_map` but with string keys.
fn build_string_key_map(entries_per_map: usize) -> MapArray {
    let mut builder = MapBuilder::new(
        Some(common::map_field_names()),
        StringBuilder::new(),
        Int32Builder::new(),
    );
    for row in 0..BATCH_SIZE {
        for entry_idx in 0..entries_per_map {
            let key = format!("key_{:04}", entries_per_map - entry_idx + (row % 7));
            let value = entry_idx as i32;
            builder.keys().append_value(&key);
            builder.values().append_value(value);
        }
        builder.append(true).unwrap();
    }
    builder.finish()
}

fn bench_map_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("spark_map_sort");

    for entries in [0usize, 1, 4, 16, 64] {
        let int_map: ArrayRef = Arc::new(build_int_key_map(entries));
        group.bench_with_input(
            BenchmarkId::new("int_keys", entries),
            &int_map,
            |b, array| {
                let args = vec![ColumnarValue::Array(Arc::clone(array))];
                b.iter(|| black_box(spark_map_sort(black_box(&args)).unwrap()));
            },
        );

        let string_map: ArrayRef = Arc::new(build_string_key_map(entries));
        group.bench_with_input(
            BenchmarkId::new("string_keys", entries),
            &string_map,
            |b, array| {
                let args = vec![ColumnarValue::Array(Arc::clone(array))];
                b.iter(|| black_box(spark_map_sort(black_box(&args)).unwrap()));
            },
        );
    }

    group.finish();
}

// Struct keys are rejected by Arrow even when each map has just one entry.
// Keep the fallible path in the baseline suite so type validation cannot disappear.
fn bench_unsupported_singleton(c: &mut Criterion) {
    use arrow::array::{Array, Int32Array, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};

    let keys = StructArray::new(
        vec![Arc::new(Field::new("k", DataType::Int32, false))].into(),
        vec![Arc::new(Int32Array::from_iter_values(0..BATCH_SIZE as i32))],
        None,
    );
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", keys.data_type().clone(), false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]
        .into(),
        vec![
            Arc::new(keys),
            Arc::new(Int32Array::from(vec![1; BATCH_SIZE])),
        ],
        None,
    );
    let map = MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new((0..=BATCH_SIZE as i32).collect::<Vec<_>>().into()),
        entries,
        None,
        false,
    );
    let args = [ColumnarValue::Array(Arc::new(map))];
    assert!(spark_map_sort(&args)
        .unwrap_err()
        .to_string()
        .contains("Sort not supported"));
    // Input creation is untimed; error creation and drop are timed. Only the first
    // nonempty row is visited, so this is not full-batch throughput.
    c.bench_function("spark_map_sort/unsupported_singleton_struct_key", |b| {
        b.iter(|| black_box(spark_map_sort(black_box(&args)).unwrap_err()));
    });
}

fn bench_matched_maps(c: &mut Criterion) {
    matched_maps::bench_maps(c, matched_maps::Stage::NormalizeOnly);
    matched_maps::bench_regression_maps(c, matched_maps::Stage::NormalizeOnly);
}

criterion_group!(
    benches,
    bench_map_sort,
    bench_matched_maps,
    bench_unsupported_singleton
);
criterion_main!(benches);
