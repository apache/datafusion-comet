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

use arrow::array::{Decimal128Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::hint::black_box;
use std::sync::Arc;

const PRECISION: u8 = 20;
const SCALE: i8 = 2;

/// Build a Decimal128(20, 2) column of `rows` rows. Every `null_every`-th row is null
/// (`null_every == 0` means no nulls). Values alternate between 0 and non-zero so the
/// boolean result is a realistic mix.
fn create_batch(rows: usize, null_every: usize) -> RecordBatch {
    let arr: Decimal128Array = (0..rows)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else if i % 3 == 0 {
                Some(0i128)
            } else {
                Some(((i % 100) as i128) * 100)
            }
        })
        .collect::<Decimal128Array>()
        .with_precision_and_scale(PRECISION, SCALE)
        .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Decimal128(PRECISION, SCALE),
        true,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;
    let expr = Arc::new(Column::new("a", 0));
    let cast_to_bool = Cast::new(
        expr,
        DataType::Boolean,
        SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        None,
        None,
    );

    let no_nulls = create_batch(rows, 0);
    let sparse_nulls = create_batch(rows, 10);
    let dense_nulls = create_batch(rows, 2);

    let mut bench = |name: &str, batch: &RecordBatch| {
        c.bench_function(name, |b| {
            b.iter(|| black_box(cast_to_bool.evaluate(black_box(batch)).unwrap()))
        });
    };
    bench("cast_decimal_to_boolean: no nulls", &no_nulls);
    bench("cast_decimal_to_boolean: sparse nulls", &sparse_nulls);
    bench("cast_decimal_to_boolean: dense nulls", &dense_nulls);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
