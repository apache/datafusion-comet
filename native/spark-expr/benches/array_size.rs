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
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_size;
use std::hint::black_box;
use std::sync::Arc;

/// Build a `ListArray` of `rows` lists, each with `elems_per_row` Int32 elements,
/// with every 10th row null.
fn create_list_array(rows: usize, elems_per_row: usize) -> ArrayRef {
    let total = rows * elems_per_row;
    let values = Int32Array::from((0..total as i32).collect::<Vec<i32>>());

    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0i32);
    for i in 1..=rows {
        offsets.push((i * elems_per_row) as i32);
    }

    let nulls = NullBuffer::from((0..rows).map(|i| i % 10 != 0).collect::<Vec<bool>>());
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        Some(nulls),
    ))
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;

    let short_lists = create_list_array(rows, 5);
    c.bench_function("spark_size: list of short arrays", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&short_lists))];
        b.iter(|| black_box(spark_size(black_box(&args)).unwrap()))
    });

    let long_lists = create_list_array(rows, 64);
    c.bench_function("spark_size: list of long arrays", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&long_lists))];
        b.iter(|| black_box(spark_size(black_box(&args)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
