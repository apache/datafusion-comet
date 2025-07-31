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
// under the License.use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, RecordBatch, StringBuilder};

use arrow::array::builder::{BooleanBuilder, Int32Builder, StringBuilder};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use comet::execution::operators::comet_filter_record_batch;
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter");

    let num_rows = 8192;
    let num_int_cols = 4;
    let num_string_cols = 4;

    let batch = create_record_batch(num_rows, num_int_cols, num_string_cols);

    // create some different predicates
    let mut predicate_select_few = BooleanBuilder::with_capacity(num_rows);
    let mut predicate_select_many = BooleanBuilder::with_capacity(num_rows);
    let mut predicate_select_all = BooleanBuilder::with_capacity(num_rows);
    for i in 0..num_rows {
        predicate_select_few.append_value(i % 10 == 0);
        predicate_select_many.append_value(i % 10 > 0);
        predicate_select_all.append_value(true);
    }
    let predicate_select_few = predicate_select_few.finish();
    let predicate_select_many = predicate_select_many.finish();
    let predicate_select_all = predicate_select_all.finish();

    // baseline uses Arrow's filter_record_batch method
    group.bench_function("arrow_filter_record_batch - few rows selected", |b| {
        b.iter(|| filter_record_batch(black_box(&batch), black_box(&predicate_select_few)))
    });
    group.bench_function("arrow_filter_record_batch - many rows selected", |b| {
        b.iter(|| filter_record_batch(black_box(&batch), black_box(&predicate_select_many)))
    });
    group.bench_function("arrow_filter_record_batch - all rows selected", |b| {
        b.iter(|| filter_record_batch(black_box(&batch), black_box(&predicate_select_all)))
    });

    group.bench_function("comet_filter_record_batch - few rows selected", |b| {
        b.iter(|| comet_filter_record_batch(black_box(&batch), black_box(&predicate_select_few)))
    });
    group.bench_function("comet_filter_record_batch - many rows selected", |b| {
        b.iter(|| comet_filter_record_batch(black_box(&batch), black_box(&predicate_select_many)))
    });
    group.bench_function("comet_filter_record_batch - all rows selected", |b| {
        b.iter(|| comet_filter_record_batch(black_box(&batch), black_box(&predicate_select_all)))
    });

    group.finish();
}

fn create_record_batch(num_rows: usize, num_int_cols: i32, num_string_cols: i32) -> RecordBatch {
    let mut int32_builder = Int32Builder::with_capacity(num_rows);
    let mut string_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
    for i in 0..num_rows {
        int32_builder.append_value(i as i32);
        string_builder.append_value(format!("this is string #{i}"));
    }
    let int32_array = Arc::new(int32_builder.finish());
    let string_array = Arc::new(string_builder.finish());

    let mut fields = vec![];
    let mut columns: Vec<ArrayRef> = vec![];
    let mut i = 0;
    for _ in 0..num_int_cols {
        fields.push(Field::new(format!("c{i}"), DataType::Int32, false));
        columns.push(int32_array.clone()); // note this is just copying a reference to the array
        i += 1;
    }
    for _ in 0..num_string_cols {
        fields.push(Field::new(format!("c{i}"), DataType::Utf8, false));
        columns.push(string_array.clone()); // note this is just copying a reference to the array
        i += 1;
    }
    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn config() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::from_millis(500))
        .warm_up_time(Duration::from_millis(500))
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
