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

use arrow::array::{ArrayRef, Int32Array, LargeListArray, ListArray, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_size;
use std::hint::black_box;
use std::sync::Arc;

/// Build a `ListArray` of `rows` lists, each with `elems_per_row` Int32 elements.
/// When `with_nulls` is true every 10th row is null.
fn create_list_array(rows: usize, elems_per_row: usize, with_nulls: bool) -> ArrayRef {
    let total = rows * elems_per_row;
    let values = Int32Array::from((0..total as i32).collect::<Vec<i32>>());

    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0i32);
    for i in 1..=rows {
        offsets.push((i * elems_per_row) as i32);
    }

    let nulls =
        with_nulls.then(|| NullBuffer::from((0..rows).map(|i| i % 10 != 0).collect::<Vec<bool>>()));
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        nulls,
    ))
}

/// Build a `LargeListArray` (i64 offsets) of `rows` lists with `elems_per_row`
/// Int32 elements. Every 10th row is null. LargeList exercises the extra
/// Int64->Int32 cast on top of the length kernel.
fn create_large_list_array(rows: usize, elems_per_row: usize) -> ArrayRef {
    let total = rows * elems_per_row;
    let values = Int32Array::from((0..total as i32).collect::<Vec<i32>>());

    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0i64);
    for i in 1..=rows {
        offsets.push((i * elems_per_row) as i64);
    }

    let nulls = NullBuffer::from((0..rows).map(|i| i % 10 != 0).collect::<Vec<bool>>());
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    Arc::new(LargeListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        Some(nulls),
    ))
}

/// Build a `MapArray` of `rows` maps, each with `entries_per_row` Int32→Int32
/// entries. When `with_nulls` is true every 10th row is null. Map entry counts
/// come from the offset buffer, so per-row diff is O(1) already; this bench
/// isolates per-row dispatch + builder overhead vs. one-shot offset differencing.
fn create_map_array(rows: usize, entries_per_row: usize, with_nulls: bool) -> ArrayRef {
    let total = rows * entries_per_row;
    let keys = Int32Array::from((0..total as i32).collect::<Vec<i32>>());
    let values = Int32Array::from((0..total as i32).collect::<Vec<i32>>());

    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0i32);
    for i in 1..=rows {
        offsets.push((i * entries_per_row) as i32);
    }

    let key_field = Arc::new(Field::new("key", DataType::Int32, false));
    let value_field = Arc::new(Field::new("value", DataType::Int32, true));
    let entries = StructArray::new(
        Fields::from(vec![key_field, value_field]),
        vec![Arc::new(keys), Arc::new(values)],
        None,
    );

    let map_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ])),
        false,
    ));

    let nulls =
        with_nulls.then(|| NullBuffer::from((0..rows).map(|i| i % 10 != 0).collect::<Vec<bool>>()));
    Arc::new(
        MapArray::try_new(map_field, OffsetBuffer::new(offsets.into()), entries, nulls, false)
            .unwrap(),
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;

    let mut bench = |name: &str, arr: &ArrayRef| {
        let args = vec![ColumnarValue::Array(Arc::clone(arr))];
        c.bench_function(name, |b| {
            b.iter(|| black_box(spark_size(black_box(&args)).unwrap()))
        });
    };

    // 10%-null shapes: match the pre-existing coverage.
    bench(
        "spark_size: list of short arrays",
        &create_list_array(rows, 5, true),
    );
    bench(
        "spark_size: list of long arrays",
        &create_list_array(rows, 64, true),
    );

    // No-null shape: `CometSize.convert` wraps size() in a `CASE WHEN isnotnull(child)`
    // that filters null rows out before the THEN branch runs, so in a real Comet plan
    // spark_size_list_like only ever sees a null-free array. This shape measures that
    // path.
    bench(
        "spark_size: list, no nulls",
        &create_list_array(rows, 5, false),
    );

    // LargeList: exposes the Int64 length -> Int32 cast (extra allocation) on top of
    // the length kernel.
    bench(
        "spark_size: LargeList (10% null)",
        &create_large_list_array(rows, 5),
    );

    // Map shapes: no length-kernel support for MapArray, so this path differences
    // offsets directly. Cover the two shapes the issue calls for — with and without
    // nulls — since the null rewrite is the only branch that touches the values
    // buffer.
    bench(
        "spark_size: map (10% null)",
        &create_map_array(rows, 5, true),
    );
    bench(
        "spark_size: map, no nulls",
        &create_map_array(rows, 5, false),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
