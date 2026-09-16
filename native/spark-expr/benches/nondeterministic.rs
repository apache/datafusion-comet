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

//! Benchmarks for the Comet-owned nondeterministic expressions
//! (`native/spark-expr/src/nondetermenistic_funcs/`). Even though the outputs
//! are random, per-row timing of the RNG / id-generation path is meaningful and
//! has been a source of regressions. Covers `rand`, `randn`, `rand_str`,
//! `uuid`, `monotonically_increasing_id` (seedless `PhysicalExpr`s evaluated
//! over an empty row-counted batch), `shuffle` (a `PhysicalExpr` over a `List`
//! child), and the `BernoulliCellSampler` used by the Sample operator.

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Field, Int64Type, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{
    BernoulliCellSampler, monotonically_increasing_id::MonotonicallyIncreasingId, RandExpr, RandStrExpr, RandnExpr, ShuffleExpr,
    UuidExpr,
};
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{primitive_list_array, ROW_COUNTS};

const SEED: i64 = 42;

/// An empty-schema batch carrying only a row count, which is all the seedless
/// generators need (they produce one value per row).
fn empty_batch(rows: usize) -> RecordBatch {
    RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    // Seedless generators: rand / randn / rand_str / uuid / monotonically_increasing_id.
    let mut group = c.benchmark_group("nondeterministic");
    for rows in ROW_COUNTS {
        let batch = empty_batch(rows);

        let exprs: Vec<(&str, Arc<dyn PhysicalExpr>)> = vec![
            ("rand", Arc::new(RandExpr::new(SEED))),
            ("randn", Arc::new(RandnExpr::new(SEED))),
            ("rand_str", Arc::new(RandStrExpr::new(16, SEED))),
            ("uuid", Arc::new(UuidExpr::new(SEED))),
            (
                "monotonically_increasing_id",
                Arc::new(MonotonicallyIncreasingId::from_partition_id(0)),
            ),
        ];

        for (name, expr) in exprs {
            group.bench_with_input(BenchmarkId::new(name, rows), &batch, |b, batch| {
                b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
            });
        }
    }
    group.finish();

    // shuffle: a PhysicalExpr that permutes each row's List<Int64> child.
    let mut group = c.benchmark_group("nondeterministic_shuffle");
    for rows in ROW_COUNTS {
        let list = primitive_list_array::<Int64Type>(rows, 0.0, 16, |i| i as i64);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![list]).unwrap();
        let expr = ShuffleExpr::new(Arc::new(Column::new("a", 0)), SEED);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &batch, |b, batch| {
            b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
        });
    }
    group.finish();

    // BernoulliCellSampler: not an expr; produces one bool per row via `sample()`.
    let mut group = c.benchmark_group("bernoulli_cell_sampler");
    for rows in ROW_COUNTS {
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter(|| {
                let mut sampler = BernoulliCellSampler::new(0.0, 0.5, SEED);
                let mut kept = 0usize;
                for _ in 0..rows {
                    if black_box(sampler.sample()) {
                        kept += 1;
                    }
                }
                black_box(kept)
            })
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
