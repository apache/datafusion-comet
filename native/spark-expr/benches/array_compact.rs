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
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkArrayCompact;
use std::hint::black_box;
use std::sync::Arc;

/// Build a `ListArray` of `num_rows` rows, each with `row_len` Int32 elements.
/// `null_every` controls how sparse element nulls are: element `k` is null when
/// `k % null_every == 0`. `null_every == 0` means no element nulls at all.
fn create_list_array(num_rows: usize, row_len: usize, null_every: usize) -> ArrayRef {
    let total = num_rows * row_len;
    let values: Vec<Option<i32>> = (0..total)
        .map(|k| {
            if null_every != 0 && k % null_every == 0 {
                None
            } else {
                Some(k as i32)
            }
        })
        .collect();
    let values = Arc::new(Int32Array::from(values)) as ArrayRef;

    let offsets: Vec<i32> = (0..=num_rows).map(|r| (r * row_len) as i32).collect();
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    ))
}

fn invoke(udf: &SparkArrayCompact, array: &ArrayRef) -> ColumnarValue {
    let return_field = Arc::new(Field::new(
        "c",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    ));
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::clone(array))],
        arg_fields: vec![Arc::clone(&return_field)],
        number_rows: array.len(),
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    };
    udf.invoke_with_args(args).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkArrayCompact::default();
    let num_rows = 4096;
    let row_len = 16;

    // No element nulls: hits the fast path (whole-row copy).
    let no_nulls = create_list_array(num_rows, row_len, 0);
    c.bench_function("array_compact: no nulls", |b| {
        b.iter(|| black_box(invoke(&udf, black_box(&no_nulls))))
    });

    // Sparse nulls: long non-null runs coalesced into few extends.
    let sparse = create_list_array(num_rows, row_len, 8);
    c.bench_function("array_compact: sparse nulls", |b| {
        b.iter(|| black_box(invoke(&udf, black_box(&sparse))))
    });

    // Dense nulls: every other element null (worst case for run-batching).
    let dense = create_list_array(num_rows, row_len, 2);
    c.bench_function("array_compact: dense nulls", |b| {
        b.iter(|| black_box(invoke(&udf, black_box(&dense))))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
