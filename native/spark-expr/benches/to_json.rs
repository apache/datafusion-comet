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

use arrow::array::{Array, ArrayRef, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion_comet_spark_expr::ToJson;
use std::hint::black_box;
use std::sync::Arc;

fn create_batch(array_size: usize) -> RecordBatch {
    // A struct of string fields exercises the per-row escaping path, which is the
    // dominant per-element cost of to_json for string-heavy structs.
    let samples = [
        "",
        "a",
        "abc",
        "hello world",
        "some longer string value here",
    ];
    let build = |offset: usize| -> ArrayRef {
        let vals: Vec<Option<String>> = (0..array_size)
            .map(|i| {
                if i % 10 == 0 {
                    None
                } else {
                    Some(samples[(i + offset) % samples.len()].to_string())
                }
            })
            .collect();
        Arc::new(StringArray::from(vals))
    };

    let f1 = build(0);
    let f2 = build(2);
    let f3 = build(4);
    let struct_array = StructArray::from(vec![
        (Arc::new(Field::new("f1", DataType::Utf8, true)), f1),
        (Arc::new(Field::new("f2", DataType::Utf8, true)), f2),
        (Arc::new(Field::new("f3", DataType::Utf8, true)), f3),
    ]);

    let schema = Schema::new(vec![Field::new(
        "s",
        struct_array.data_type().clone(),
        true,
    )]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let batch = create_batch(8192);
    let expr = ToJson::new(Arc::new(Column::new("s", 0)), "UTC", false);
    c.bench_function("to_json", |b| {
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
