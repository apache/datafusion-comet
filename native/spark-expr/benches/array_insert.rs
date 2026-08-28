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

use arrow::array::{Array, Int32Array, ListArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::ArrayInsert;
use std::hint::black_box;
use std::sync::Arc;

/// Build a `RecordBatch` with columns (src List<Int32>, pos Int32, item Int32) for
/// `rows` rows. Every `src_null_every`-th src row is null, every `pos_null_every`-th
/// pos is null (`_ == 0` disables that pattern). `item` is always non-null. The
/// exercised code path is the one refactored in the PR: `is_not_null` on src and pos,
/// then `and` for the item mask.
fn create_batch(rows: usize, src_null_every: usize, pos_null_every: usize) -> RecordBatch {
    let src_iter = (0..rows).map(|i| {
        if src_null_every != 0 && i % src_null_every == 0 {
            None
        } else {
            Some(vec![
                Some(i as i32),
                Some((i + 1) as i32),
                Some((i + 2) as i32),
            ])
        }
    });
    let src = ListArray::from_iter_primitive::<Int32Type, _, _>(src_iter);

    let positions = Int32Array::from(
        (0..rows)
            .map(|i| {
                if pos_null_every != 0 && i % pos_null_every == 0 {
                    None
                } else {
                    Some(((i as i32) % 3) + 1)
                }
            })
            .collect::<Vec<_>>(),
    );
    let items = Int32Array::from((0..rows).map(|i| Some(i as i32)).collect::<Vec<_>>());

    let list_field = match src.data_type() {
        DataType::List(f) => Arc::clone(f),
        _ => unreachable!(),
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("src", DataType::List(Arc::clone(&list_field)), true),
        Field::new("pos", DataType::Int32, true),
        Field::new("item", DataType::Int32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(src), Arc::new(positions), Arc::new(items)],
    )
    .unwrap()
}

fn make_expr(batch: &RecordBatch) -> ArrayInsert {
    let schema = batch.schema();
    ArrayInsert::new(
        col("src", &schema).unwrap(),
        col("pos", &schema).unwrap(),
        col("item", &schema).unwrap(),
        false,
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;

    let no_nulls = create_batch(rows, 0, 0);
    let sparse_src_nulls = create_batch(rows, 10, 0);
    let dense_src_nulls = create_batch(rows, 2, 0);
    let mixed_nulls = create_batch(rows, 10, 7);

    let no_nulls_expr = make_expr(&no_nulls);
    let sparse_src_expr = make_expr(&sparse_src_nulls);
    let dense_src_expr = make_expr(&dense_src_nulls);
    let mixed_expr = make_expr(&mixed_nulls);

    c.bench_function("array_insert: no nulls", |b| {
        b.iter(|| black_box(no_nulls_expr.evaluate(black_box(&no_nulls)).unwrap()))
    });
    c.bench_function("array_insert: sparse src nulls", |b| {
        b.iter(|| {
            black_box(
                sparse_src_expr
                    .evaluate(black_box(&sparse_src_nulls))
                    .unwrap(),
            )
        })
    });
    c.bench_function("array_insert: dense src nulls", |b| {
        b.iter(|| {
            black_box(
                dense_src_expr
                    .evaluate(black_box(&dense_src_nulls))
                    .unwrap(),
            )
        })
    });
    c.bench_function("array_insert: mixed src+pos nulls", |b| {
        b.iter(|| black_box(mixed_expr.evaluate(black_box(&mixed_nulls)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
