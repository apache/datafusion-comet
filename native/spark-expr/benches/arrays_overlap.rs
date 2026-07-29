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

use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkArraysOverlap;
use std::hint::black_box;
use std::sync::Arc;

fn list_of(values: ArrayRef, rows: usize, elems_per_row: usize) -> ArrayRef {
    let offsets: Vec<i32> = (0..=rows).map(|i| (i * elems_per_row) as i32).collect();
    let nulls = NullBuffer::from((0..rows).map(|i| i % 20 != 0).collect::<Vec<bool>>());
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        Some(nulls),
    ))
}

/// Int32 lists. `stride` spreads the two sides apart so most rows do not overlap, exercising
/// the full scan rather than exiting on the first probe element.
fn int_lists(rows: usize, elems_per_row: usize, offset: i32) -> (ArrayRef, ArrayRef) {
    let total = (rows * elems_per_row) as i32;
    let left: ArrayRef = Arc::new(Int32Array::from_iter((0..total).map(|i| {
        if i % 17 == 0 {
            None
        } else {
            Some(i % 1000)
        }
    })));
    let right: ArrayRef = Arc::new(Int32Array::from_iter(
        (0..total).map(|i| Some((i + offset) % 1000)),
    ));
    (
        list_of(left, rows, elems_per_row),
        list_of(right, rows, elems_per_row),
    )
}

fn string_lists(rows: usize, elems_per_row: usize, offset: usize) -> (ArrayRef, ArrayRef) {
    let total = rows * elems_per_row;
    let left: ArrayRef = Arc::new(StringArray::from_iter(
        (0..total).map(|i| Some(format!("key-{}", i % 500))),
    ));
    let right: ArrayRef = Arc::new(StringArray::from_iter(
        (0..total).map(|i| Some(format!("key-{}", (i + offset) % 500))),
    ));
    (
        list_of(left, rows, elems_per_row),
        list_of(right, rows, elems_per_row),
    )
}

fn invoke(udf: &SparkArraysOverlap, left: &ArrayRef, right: &ArrayRef) -> ColumnarValue {
    udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::clone(left)),
            ColumnarValue::Array(Arc::clone(right)),
        ],
        arg_fields: vec![],
        number_rows: left.len(),
        return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkArraysOverlap::default();
    let rows = 4096;

    let (left, right) = int_lists(rows, 8, 500);
    c.bench_function("spark_arrays_overlap: int32 short lists", |b| {
        b.iter(|| black_box(invoke(&udf, black_box(&left), black_box(&right))))
    });

    let (left, right) = int_lists(rows, 32, 500);
    c.bench_function("spark_arrays_overlap: int32 medium lists", |b| {
        b.iter(|| black_box(invoke(&udf, black_box(&left), black_box(&right))))
    });

    let (left, right) = int_lists(1024, 128, 500);
    c.bench_function("spark_arrays_overlap: int32 long lists", |b| {
        b.iter(|| black_box(invoke(&udf, black_box(&left), black_box(&right))))
    });

    let (left, right) = string_lists(rows, 8, 250);
    c.bench_function("spark_arrays_overlap: utf8 short lists", |b| {
        b.iter(|| black_box(invoke(&udf, black_box(&left), black_box(&right))))
    });

    let (left, right) = string_lists(1024, 64, 250);
    c.bench_function("spark_arrays_overlap: utf8 long lists", |b| {
        b.iter(|| black_box(invoke(&udf, black_box(&left), black_box(&right))))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
