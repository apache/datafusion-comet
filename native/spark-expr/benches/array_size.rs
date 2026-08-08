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

use arrow::array::{ArrayRef, Int32Array, LargeListArray, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
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
/// Int32 elements. When `with_nulls` is true every 10th row is null.
fn create_large_list_array(rows: usize, elems_per_row: usize, with_nulls: bool) -> ArrayRef {
    let total = rows * elems_per_row;
    let values = Int32Array::from((0..total as i32).collect::<Vec<i32>>());

    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0i64);
    for i in 1..=rows {
        offsets.push((i * elems_per_row) as i64);
    }

    let nulls =
        with_nulls.then(|| NullBuffer::from((0..rows).map(|i| i % 10 != 0).collect::<Vec<bool>>()));
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    Arc::new(LargeListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        nulls,
    ))
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

    // LargeList: builds Int32 lengths from i64 offsets (see spark_size_large_list_from_offsets).
    // Shapes mirror List coverage (short/long + 10% null, short + no nulls).
    bench(
        "spark_size: LargeList (10% null)",
        &create_large_list_array(rows, 5, true),
    );
    bench(
        "spark_size: LargeList of long arrays",
        &create_large_list_array(rows, 64, true),
    );
    // No-null LargeList: production path after CometSize's isnotnull filter.
    bench(
        "spark_size: LargeList, no nulls",
        &create_large_list_array(rows, 5, false),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
