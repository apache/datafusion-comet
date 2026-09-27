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

use arrow::array::{RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::GetStructField;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{i64_array, NULL_RATIOS, ROW_COUNTS};

fn criterion_benchmark(c: &mut Criterion) {
    let inner: Fields = vec![
        Field::new("x", DataType::Int64, true),
        Field::new("y", DataType::Int64, true),
    ]
    .into();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(inner.clone()),
        true,
    )]));
    let expr = GetStructField::new(Arc::new(Column::new("s", 0)), 0);

    let mut group = c.benchmark_group("get_struct_field");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let s = StructArray::new(
                inner.clone(),
                vec![
                    i64_array(rows, null_ratio, |i| i as i64),
                    i64_array(rows, 0.0, |i| i as i64 * 2),
                ],
                None,
            );
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(s)]).unwrap();
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &batch,
                |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
            );
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
