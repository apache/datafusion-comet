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

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion_comet_spark_expr::create_negate_expr;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{i64_array, NULL_RATIOS, ROW_COUNTS};

fn criterion_benchmark(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let expr = create_negate_expr(Arc::new(Column::new("a", 0)), false).unwrap();

    let mut group = c.benchmark_group("negative");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![i64_array(rows, null_ratio, |i| i as i64 % 1000)],
            )
            .unwrap();
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
