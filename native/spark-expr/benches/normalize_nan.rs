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

//! Benchmarks for NormalizeNaNAndZero expression

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::NormalizeNaNAndZero;
use std::hint::black_box;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

fn make_col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, index))
}

/// Create a batch with float64 column containing various values including NaN and -0.0
fn create_float_batch(nan_pct: usize, neg_zero_pct: usize, null_pct: usize) -> RecordBatch {
    let mut values: Vec<Option<f64>> = Vec::with_capacity(BATCH_SIZE);

    for i in 0..BATCH_SIZE {
        if null_pct > 0 && i % (100 / null_pct.max(1)) == 0 {
            values.push(None);
        } else if nan_pct > 0 && i % (100 / nan_pct.max(1)) == 1 {
            values.push(Some(f64::NAN));
        } else if neg_zero_pct > 0 && i % (100 / neg_zero_pct.max(1)) == 2 {
            values.push(Some(-0.0));
        } else {
            values.push(Some(i as f64 * 1.5));
        }
    }

    let array = Float64Array::from(values);
    let schema = Schema::new(vec![Field::new("c1", DataType::Float64, true)]);

    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
}

fn bench_normalize_nan_and_zero(c: &mut Criterion) {
    let mut group = c.benchmark_group("normalize_nan_and_zero");

    // Test with different percentages of special values
    let test_cases = [
        ("no_special", 0, 0, 0),
        ("10pct_nan", 10, 0, 0),
        ("10pct_neg_zero", 0, 10, 0),
        ("10pct_null", 0, 0, 10),
        ("mixed_10pct", 5, 5, 5),
        ("all_normal", 0, 0, 0),
    ];

    for (name, nan_pct, neg_zero_pct, null_pct) in test_cases {
        let batch = create_float_batch(nan_pct, neg_zero_pct, null_pct);

        let normalize_expr = Arc::new(NormalizeNaNAndZero::new(
            DataType::Float64,
            make_col("c1", 0),
        ));

        group.bench_with_input(BenchmarkId::new("float64", name), &batch, |b, batch| {
            b.iter(|| black_box(normalize_expr.evaluate(black_box(batch)).unwrap()));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_normalize_nan_and_zero);
criterion_main!(benches);
