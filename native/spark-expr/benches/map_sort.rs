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
use arrow::array::{ArrayRef, MapArray, MapFieldNames};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_map_sort;
use std::hint::black_box;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".into(),
        key: "key".into(),
        value: "value".into(),
    }
}

/// Build a MapArray with `BATCH_SIZE` rows where each map has `entries_per_map` entries.
/// Keys are integers in reverse order so every map needs a real sort.
fn build_int_key_map(entries_per_map: usize) -> MapArray {
    let mut builder = MapBuilder::new(
        Some(map_field_names()),
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
        Some(map_field_names()),
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

    for entries in [4usize, 16, 64] {
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

criterion_group!(benches, bench_map_sort);
criterion_main!(benches);
