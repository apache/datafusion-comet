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

use arrow::array::builder::{Int32Builder, ListBuilder, StringBuilder};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::SparkArraysZipFunc;
use std::hint::black_box;
use std::sync::Arc;

/// A list column of `i32` where row `i` holds `len(i)` elements and every tenth
/// row is null.
fn int_list(num_rows: usize, len: impl Fn(usize) -> usize) -> ArrayRef {
    let mut builder = ListBuilder::new(Int32Builder::new());
    for row in 0..num_rows {
        if row % 10 == 0 {
            builder.append_null();
            continue;
        }
        for i in 0..len(row) {
            builder.values().append_value((row + i) as i32);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn string_list(num_rows: usize, len: impl Fn(usize) -> usize) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in 0..num_rows {
        if row % 7 == 0 {
            builder.append_null();
            continue;
        }
        for i in 0..len(row) {
            builder.values().append_value(format!("value_{row}_{i}"));
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let num_rows = 8192;

    let uniform: Vec<ArrayRef> = vec![
        int_list(num_rows, |_| 4),
        int_list(num_rows, |_| 4),
        string_list(num_rows, |_| 4),
    ];
    let ragged: Vec<ArrayRef> = vec![
        int_list(num_rows, |row| row % 5),
        int_list(num_rows, |row| row % 3),
        string_list(num_rows, |row| row % 7),
    ];

    for (name, columns) in [("uniform", uniform), ("ragged", ragged)] {
        let schema = Schema::new(
            columns
                .iter()
                .enumerate()
                .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
        let expr = SparkArraysZipFunc::new(
            (0..batch.num_columns())
                .map(|i| Arc::new(Column::new(&format!("c{i}"), i)) as Arc<dyn PhysicalExpr>)
                .collect(),
            (0..batch.num_columns()).map(|i| i.to_string()).collect(),
        );
        c.bench_function(&format!("arrays_zip/{name}"), |b| {
            b.iter(|| black_box(expr.evaluate(&batch).unwrap()))
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
