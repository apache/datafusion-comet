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

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Date32Array, Int32Array, ListArray, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, TimeUnit};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_cast, EvalMode, SparkCastOptions};

const ROWS: usize = 8192;

fn validity(null_percent: usize) -> Option<NullBuffer> {
    (null_percent != 0).then(|| {
        NullBuffer::from(
            (0..ROWS)
                .map(|row| match null_percent {
                    // Exact ratios exercise the sparse-compaction boundary without rounding.
                    25 => row % 4 != 0,
                    50 => row % 2 != 0,
                    _ => row % 100 >= null_percent,
                })
                .collect::<Vec<_>>(),
        )
    })
}

fn list_input(width: usize, null_percent: usize, fallible: bool) -> (ArrayRef, DataType) {
    let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(width, ROWS));
    let values = (0..ROWS * width)
        .map(|index| (index % 31 != 0).then_some((index % 20000) as i32))
        .collect::<Vec<_>>();
    let (values, target): (ArrayRef, DataType) = if fallible {
        (
            Arc::new(Date32Array::from(values)),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        )
    } else {
        (Arc::new(Int32Array::from(values)), DataType::Int64)
    };
    let from_field = Arc::new(Field::new("item", values.data_type().clone(), true));
    (
        Arc::new(ListArray::new(
            from_field,
            offsets,
            values,
            validity(null_percent),
        )),
        DataType::List(Arc::new(Field::new("item", target, true))),
    )
}

fn struct_input(null_percent: usize) -> (ArrayRef, DataType) {
    let fields = vec![Arc::new(Field::new("value", DataType::Int32, true))].into();
    (
        Arc::new(StructArray::new(
            fields,
            vec![Arc::new(Int32Array::from_iter_values(0..ROWS as i32))],
            validity(null_percent),
        )),
        DataType::Struct(vec![Field::new("value", DataType::Int64, true)].into()),
    )
}

fn map_input(width: usize, null_percent: usize) -> (ArrayRef, DataType) {
    let fields = vec![
        Arc::new(Field::new("key", DataType::Int32, false)),
        Arc::new(Field::new("value", DataType::Int32, true)),
    ]
    .into();
    let entries = StructArray::new(
        fields,
        vec![
            Arc::new(Int32Array::from_iter_values(0..(ROWS * width) as i32)),
            Arc::new(Int32Array::from_iter(
                (0..ROWS * width).map(|index| (index % 31 != 0).then_some(index as i32)),
            )),
        ],
        None,
    );
    let target_entries = DataType::Struct(
        vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Int64, true),
        ]
        .into(),
    );
    (
        Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(width, ROWS)),
            entries,
            validity(null_percent),
            false,
        )),
        DataType::Map(
            Arc::new(Field::new("entries", target_entries, false)),
            false,
        ),
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    let options = SparkCastOptions::new(EvalMode::Ansi, "UTC", false);
    let mut group = c.benchmark_group("cast_nested");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    let mut bench = |name: &str, width: usize, null_percent: usize, input: (ArrayRef, DataType)| {
        let (array, target) = input;
        group.throughput(Throughput::Elements(array.len() as u64));
        let id = BenchmarkId::new(name, format!("width={width}/nulls={null_percent}%"));
        group.bench_function(id, |b| {
            b.iter(|| {
                black_box(
                    spark_cast(
                        ColumnarValue::Array(Arc::clone(black_box(&array))),
                        black_box(&target),
                        black_box(&options),
                    )
                    .unwrap(),
                )
            });
        });
    };

    for null_percent in [0, 10, 25, 50, 75, 99] {
        for width in [4, 64] {
            bench(
                "list_int_to_long",
                width,
                null_percent,
                list_input(width, null_percent, false),
            );
            bench(
                "map_int_to_long",
                width,
                null_percent,
                map_input(width, null_percent),
            );
            // Checked DATE conversion must retain normalization, even in legacy mode.
            bench(
                "list_date_to_timestamp",
                width,
                null_percent,
                list_input(width, null_percent, true),
            );
        }
        bench(
            "struct_int_to_long",
            1,
            null_percent,
            struct_input(null_percent),
        );
    }

    let (array, target) = list_input(4, 10, false);
    bench(
        "sliced_list_int_to_long",
        4,
        10,
        (array.slice(1, ROWS - 2), target),
    );
    let (array, target) = list_input(64, 0, false);
    bench(
        "small_slice_int_to_long",
        64,
        0,
        (array.slice(ROWS / 2, 32), target),
    );

    // One null parent can hide most values even when the parent null count is tiny.
    let lengths = (0..ROWS).map(|row| if row == ROWS - 1 { 1024 * 1024 } else { 4 });
    let offsets = OffsetBuffer::from_lengths(lengths);
    let child_len = offsets[ROWS] as usize;
    let array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        offsets,
        Arc::new(Int32Array::from_iter_values(0..child_len as i32)),
        Some(NullBuffer::from(
            (0..ROWS).map(|row| row != ROWS - 1).collect::<Vec<_>>(),
        )),
    ));
    let target = DataType::List(Arc::new(Field::new("item", DataType::Int64, false)));
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("list_int_to_long/one_large_null_parent", |b| {
        b.iter(|| {
            black_box(
                spark_cast(ColumnarValue::Array(Arc::clone(&array)), &target, &options).unwrap(),
            )
        });
    });

    // Invalid visible values exercise the error path instead of measuring a successful cast.
    let (array, target) = list_input(4, 10, true);
    let list = array.as_any().downcast_ref::<ListArray>().unwrap();
    let invalid: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Date32, true)),
        list.offsets().clone(),
        Arc::new(Date32Array::from(vec![i32::MAX; list.values().len()])),
        list.nulls().cloned(),
    ));
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("list_date_to_timestamp/visible_overflow", |b| {
        b.iter(|| {
            black_box(
                spark_cast(
                    ColumnarValue::Array(Arc::clone(&invalid)),
                    &target,
                    &options,
                )
                .unwrap_err(),
            )
        });
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
