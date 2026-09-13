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

//! Parquet timestamp conversion with nullable containers and unchanged array siblings.
//! Run the same benchmark on the reviewed head and the candidate with Criterion baselines.

use std::{hint::black_box, sync::Arc};

use arrow::array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, StructArray, TimestampMillisecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, TimeUnit};
use comet::parquet::parquet_support::{spark_parquet_convert, SparkParquetOptions};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::EvalMode;

const ROWS: usize = 1024;

// Null rows have empty ranges, as in a decoded Parquet list/map. Their presence still
// caused the original repeated_visibility implementation to scan all backing items.
fn offsets(width: usize) -> (OffsetBuffer<i32>, NullBuffer, usize) {
    let valid: Vec<bool> = (0..ROWS).map(|row| row % 8 != 0).collect();
    let mut offsets = vec![0_i32];
    for &present in &valid {
        offsets.push(offsets.last().unwrap() + if present { width as i32 } else { 0 });
    }
    let len = *offsets.last().unwrap() as usize;
    (
        OffsetBuffer::new(offsets.into()),
        NullBuffer::from(valid),
        len,
    )
}

fn timestamp_field(unit: TimeUnit) -> Arc<Field> {
    Arc::new(Field::new("ts", DataType::Timestamp(unit, None), true))
}

fn array_sibling(width: usize) -> (ArrayRef, DataType) {
    let (offsets, nulls, len) = offsets(width);
    let ints: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        offsets,
        Arc::new(Int32Array::from(vec![7; len])),
        Some(nulls),
    ));
    let field = Arc::new(Field::new("ints", ints.data_type().clone(), true));
    let target =
        DataType::Struct(vec![timestamp_field(TimeUnit::Microsecond), Arc::clone(&field)].into());
    let input = StructArray::new(
        vec![timestamp_field(TimeUnit::Millisecond), field].into(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![7; ROWS])),
            ints,
        ],
        None,
    );
    (Arc::new(input), target)
}

fn timestamp_containers() -> [(ArrayRef, DataType); 2] {
    let (offsets, nulls, len) = offsets(32);
    let timestamps: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![7; len]));
    let list = ListArray::new(
        timestamp_field(TimeUnit::Millisecond),
        offsets.clone(),
        Arc::clone(&timestamps),
        Some(nulls.clone()),
    );
    let key = Arc::new(Field::new("key", DataType::Int32, false));
    let entries = StructArray::new(
        vec![Arc::clone(&key), timestamp_field(TimeUnit::Millisecond)].into(),
        vec![
            Arc::new(Int32Array::from(
                (0..len).map(|i| (i % 32) as i32).collect::<Vec<_>>(),
            )),
            timestamps,
        ],
        None,
    );
    let map = MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        offsets,
        entries,
        Some(nulls),
        false,
    );
    let target_map = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![key, timestamp_field(TimeUnit::Microsecond)].into()),
            false,
        )),
        false,
    );
    [
        (
            Arc::new(list),
            DataType::List(timestamp_field(TimeUnit::Microsecond)),
        ),
        (Arc::new(map), target_map),
    ]
}

fn benchmark(c: &mut Criterion) {
    let options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
    let mut group = c.benchmark_group("parquet_timestamp_conversion");
    for width in [8, 1024] {
        let (array, target) = array_sibling(width);
        group.bench_function(format!("array_sibling_width_{width}"), |b| {
            b.iter(|| {
                spark_parquet_convert(
                    ColumnarValue::Array(Arc::clone(black_box(&array))),
                    black_box(&target),
                    black_box(&options),
                )
                .unwrap()
            })
        });
    }
    for (kind, (array, target)) in ["list", "map"].into_iter().zip(timestamp_containers()) {
        for sliced in [false, true] {
            let input = if sliced {
                array.slice(ROWS / 4, ROWS / 2)
            } else {
                Arc::clone(&array)
            };
            group.bench_function(format!("timestamp_{kind}_sliced_{sliced}"), |b| {
                b.iter(|| {
                    spark_parquet_convert(
                        ColumnarValue::Array(Arc::clone(black_box(&input))),
                        black_box(&target),
                        black_box(&options),
                    )
                    .unwrap()
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
