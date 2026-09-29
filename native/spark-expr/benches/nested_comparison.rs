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

//! Compare the base DataFusion path, PR #6073's eager normalization, and Spark equality.
//! Ordinary finite inputs keep the answers identical across all three implementations.

use arrow::array::{ArrayRef, Float64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{in_list, BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{spark_comparison, spark_in_list, NormalizeNestedFloats};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const ROWS: usize = 8192;

fn input(width: usize, mismatch: Option<usize>, null_every: usize) -> ArrayRef {
    let values = (0..ROWS * width)
        .map(|i| {
            if mismatch == Some(i % width) {
                2.0
            } else {
                1.0
            }
        })
        .collect::<Vec<_>>();
    let nulls = (null_every != 0).then(|| {
        NullBuffer::from(
            (0..ROWS)
                .map(|i| (i + 1) % null_every != 0)
                .collect::<Vec<_>>(),
        )
    });
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Float64, true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(width, ROWS)),
        Arc::new(Float64Array::from(values)),
        nulls,
    ))
}

fn expression(version: &str, mode: &str, batch: &RecordBatch) -> Arc<dyn PhysicalExpr> {
    let schema = batch.schema();
    let a: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
    let b: Arc<dyn PhysicalExpr> = Arc::new(Column::new("b", 1));
    if mode == "eq" {
        return if version == "new" {
            spark_comparison(a, Operator::Eq, b, &schema).unwrap()
        } else {
            Arc::new(BinaryExpr::new(a, Operator::Eq, b))
        };
    }
    let literal: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(
        ScalarValue::try_from_array(batch.column(1), 0).unwrap(),
    ));
    let candidates = match mode {
        "constant" => vec![literal],
        "dynamic" => vec![b],
        "mixed" => vec![literal, b],
        _ => unreachable!(),
    };
    match version {
        "base" => in_list(a, candidates, &false, &schema).unwrap(),
        "head" => in_list(
            NormalizeNestedFloats::wrap_if_needed(a, &schema).unwrap(),
            candidates
                .into_iter()
                .map(|e| NormalizeNestedFloats::wrap_if_needed(e, &schema).unwrap())
                .collect(),
            &false,
            &schema,
        )
        .unwrap(),
        "new" => spark_in_list(a, candidates, false, &schema).unwrap(),
        _ => unreachable!(),
    }
}

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("nested_comparison");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(500));
    for width in [1, 16, 1024] {
        for (shape, mismatch) in [
            ("first", Some(0)),
            ("last", Some(width - 1)),
            ("equal", None),
        ] {
            for (nulls, null_every) in [("no_nulls", 0), ("sparse", 16), ("dense", 2)] {
                let a = input(width, None, null_every);
                let b = input(width, mismatch, null_every);
                let schema = Arc::new(Schema::new(vec![
                    Field::new("a", a.data_type().clone(), true),
                    Field::new("b", b.data_type().clone(), true),
                ]));
                let batch = RecordBatch::try_new(schema, vec![a, b]).unwrap();
                for mode in ["dynamic", "constant", "mixed", "eq"] {
                    let expected = expression("base", mode, &batch)
                        .evaluate(&batch)
                        .unwrap()
                        .into_array(ROWS)
                        .unwrap();
                    for version in ["base", "head", "new"] {
                        let expr = expression(version, mode, &batch);
                        assert_eq!(
                            expected.as_ref(),
                            expr.evaluate(&batch)
                                .unwrap()
                                .into_array(ROWS)
                                .unwrap()
                                .as_ref()
                        );
                        group.bench_with_input(
                            BenchmarkId::new(format!("{mode}/{width}/{shape}/{nulls}"), version),
                            &expr,
                            |b, e| {
                                b.iter(|| black_box(e.evaluate(black_box(&batch)).unwrap()));
                            },
                        );
                    }
                }
            }
        }
    }
    group.finish();
    let mut group = c.benchmark_group("nested_static_build");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(500));
    for width in [1, 16, 1024] {
        let a = input(width, None, 0);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", a.data_type().clone(), true),
            Field::new("b", a.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::clone(&a), a]).unwrap();
        for version in ["base", "head", "new"] {
            group.bench_function(BenchmarkId::new(width.to_string(), version), |b| {
                b.iter(|| black_box(expression(version, "constant", black_box(&batch))))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
