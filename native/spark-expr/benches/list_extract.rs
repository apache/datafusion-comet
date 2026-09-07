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

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{create_query_context_map, ListExtract};
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{list_arrays, NULL_RATIOS, ROW_COUNTS};

fn single_col_batch(col: ArrayRef) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("c0", col.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![col]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    // list_extract(list, ordinal = 2): one-based element access (Spark element_at), non-ANSI.
    let expr = ListExtract::new(
        Arc::new(Column::new("c0", 0)),
        Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        None,
        true,
        false,
        None,
        create_query_context_map(),
    );
    let mut group = c.benchmark_group("list_extract");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            for (ty, list, _) in list_arrays(rows, null_ratio, 8) {
                let batch = single_col_batch(list);
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!("{ty}/{rows}/{tag}")),
                    &batch,
                    |b, batch| b.iter(|| black_box(expr.evaluate(batch).unwrap())),
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
