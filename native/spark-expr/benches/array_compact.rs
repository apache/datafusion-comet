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

use arrow::array::{ArrayRef, Int32Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkArrayCompact;
use std::hint::black_box;
use std::sync::Arc;

/// Build a `ListArray` of `rows` lists, each with `elems_per_row` Int32 elements.
///
/// - `elem_null_every`: insert a null element every Nth element (0 = no element nulls).
/// - `row_null_every`: mark every Nth row null (0 = no null rows).
fn build(
    rows: usize,
    elems_per_row: usize,
    elem_null_every: usize,
    row_null_every: usize,
) -> ArrayRef {
    let total = rows * elems_per_row;

    let values: Int32Array = (0..total)
        .map(|i| {
            if elem_null_every != 0 && i % elem_null_every == 0 {
                None
            } else {
                Some(i as i32)
            }
        })
        .collect();

    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0i32);
    for i in 1..=rows {
        offsets.push((i * elems_per_row) as i32);
    }

    let row_nulls = if row_null_every == 0 {
        None
    } else {
        Some(NullBuffer::from(
            (0..rows)
                .map(|i| i % row_null_every != 0)
                .collect::<Vec<bool>>(),
        ))
    };

    let field = Arc::new(Field::new("item", DataType::Int32, true));
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        row_nulls,
    ))
}

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkArrayCompact::default();
    let rows = 8192;

    let call = |arr: &ArrayRef| {
        let return_field = Arc::new(Field::new("array_compact", arr.data_type().clone(), true));
        let sfa = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(arr))],
            number_rows: rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![],
        };
        udf.invoke_with_args(sfa).unwrap()
    };

    // Dense column of short lists with no null elements: the common shape where
    // nothing is removed.
    let no_nulls_short = build(rows, 8, 0, 0);
    // No null elements, longer lists.
    let no_nulls_long = build(rows, 64, 0, 0);
    // No null elements but ~10% of rows are null.
    let no_nulls_null_rows = build(rows, 8, 0, 10);
    // Sparse element nulls (~6%).
    let sparse_nulls = build(rows, 8, 17, 0);
    // Dense element nulls (every other element): the shape a run-batching
    // approach regressed on.
    let dense_nulls = build(rows, 8, 2, 0);

    let mut bench = |name: &str, arr: &ArrayRef| {
        c.bench_function(name, |b| b.iter(|| black_box(call(black_box(arr)))));
    };

    bench("array_compact: no nulls short", &no_nulls_short);
    bench("array_compact: no nulls long", &no_nulls_long);
    bench(
        "array_compact: no element nulls, null rows",
        &no_nulls_null_rows,
    );
    bench("array_compact: sparse nulls", &sparse_nulls);
    bench("array_compact: dense nulls", &dense_nulls);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
