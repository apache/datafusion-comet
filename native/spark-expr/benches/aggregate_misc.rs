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

//! Benchmarks for Comet-owned `agg_funcs` accumulators not covered by `aggregate.rs`
//! (avg/sum) or `aggregate_stats.rs` (variance/covariance/correlation/percentile/HLL):
//! `mode`, `max_by`/`min_by`, `regr_*`, and `list_agg`. Each runs as a grouped
//! `AggregateMode::Partial` over 8192 rows x 10 batches, mirroring the other agg benches.

use arrow::array::{ArrayRef, Float64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{lit, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_spark_expr::{MaxMinBy, Mode, Regr, RegrType, SparkListAgg};
use futures::StreamExt;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

async fn agg_test(
    partitions: &[Vec<RecordBatch>],
    group_col: Arc<dyn PhysicalExpr>,
    input_exprs: Vec<Arc<dyn PhysicalExpr>>,
    aggregate_udf: Arc<AggregateUDF>,
    alias: &str,
) {
    let schema = &partitions[0][0].schema();
    let scan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(partitions, Arc::clone(schema), None).unwrap(),
    )));
    let aggregate = create_aggregate(scan, group_col, input_exprs, schema, aggregate_udf, alias);
    let mut stream = aggregate
        .execute(0, Arc::new(TaskContext::default()))
        .unwrap();
    while let Some(batch) = stream.next().await {
        let _batch = batch.unwrap();
    }
}

fn create_aggregate(
    scan: Arc<dyn ExecutionPlan>,
    group_col: Arc<dyn PhysicalExpr>,
    input_exprs: Vec<Arc<dyn PhysicalExpr>>,
    schema: &SchemaRef,
    aggregate_udf: Arc<AggregateUDF>,
    alias: &str,
) -> Arc<AggregateExec> {
    let aggr_expr = AggregateExprBuilder::new(aggregate_udf, input_exprs)
        .schema(schema.clone())
        .alias(alias)
        .with_ignore_nulls(false)
        .with_distinct(false)
        .build()
        .unwrap();

    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(group_col, "c0".to_string())]),
            vec![aggr_expr.into()],
            vec![None], // no filter expressions
            scan,
            Arc::clone(schema),
        )
        .unwrap(),
    )
}

/// `c0` is the Utf8 group key (1024 groups); `c1`/`c2` are spread-out Float64 inputs; `c3` is a
/// Utf8 value column for `list_agg`.
fn create_record_batch(num_rows: usize) -> RecordBatch {
    let mut c1_builder = Float64Builder::with_capacity(num_rows);
    let mut c2_builder = Float64Builder::with_capacity(num_rows);
    let mut group_builder = StringBuilder::with_capacity(num_rows, num_rows * 8);
    let mut c3_builder = StringBuilder::with_capacity(num_rows, num_rows * 8);
    for i in 0..num_rows {
        c1_builder.append_value(i as f64 * 1.5 + 0.25);
        c2_builder.append_value((num_rows - i) as f64 * 0.75 - 0.5);
        group_builder.append_value(format!("group_{}", i % 1024));
        c3_builder.append_value(format!("v{}", i % 100));
    }

    let fields = vec![
        Field::new("c0", DataType::Utf8, false),
        Field::new("c1", DataType::Float64, false),
        Field::new("c2", DataType::Float64, false),
        Field::new("c3", DataType::Utf8, false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(group_builder.finish()),
        Arc::new(c1_builder.finish()),
        Arc::new(c2_builder.finish()),
        Arc::new(c3_builder.finish()),
    ];

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let num_rows = 8192;
    let batch = create_record_batch(num_rows);
    let mut batches = Vec::new();
    for _ in 0..10 {
        batches.push(batch.clone());
    }
    let partitions = &[batches];

    let c0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
    let c1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c1", 1));
    let c2: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c2", 2));
    let c3: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c3", 3));

    let rt = Runtime::new().unwrap();

    // Single-input accumulator: mode.
    let mut group = c.benchmark_group("aggregate_misc_single");

    let single_cases: Vec<(&str, Arc<AggregateUDF>)> = vec![(
        "mode",
        Arc::new(AggregateUDF::new_from_impl(Mode::new(
            DataType::Float64,
            false,
        ))),
    )];

    for (name, udf) in single_cases {
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| {
                black_box(agg_test(
                    partitions,
                    c0.clone(),
                    vec![c1.clone()],
                    udf.clone(),
                    name,
                ))
            })
        });
    }
    group.finish();

    // Two-input accumulators: max_by/min_by (value, ordering) and regr_slope.
    let mut group = c.benchmark_group("aggregate_misc_pair");

    let pair_cases: Vec<(&str, Arc<AggregateUDF>)> = vec![
        (
            "max_by",
            Arc::new(AggregateUDF::new_from_impl(MaxMinBy::new_max_by())),
        ),
        (
            "min_by",
            Arc::new(AggregateUDF::new_from_impl(MaxMinBy::new_min_by())),
        ),
        (
            "regr_slope",
            Arc::new(AggregateUDF::new_from_impl(Regr::new(
                RegrType::Slope,
                "regr_slope",
                true,
                false,
            ))),
        ),
    ];

    for (name, udf) in pair_cases {
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| {
                black_box(agg_test(
                    partitions,
                    c0.clone(),
                    vec![c1.clone(), c2.clone()],
                    udf.clone(),
                    name,
                ))
            })
        });
    }
    group.finish();

    // list_agg takes a value column plus a literal delimiter.
    let mut group = c.benchmark_group("aggregate_misc_list_agg");
    let list_agg_udf: Arc<AggregateUDF> =
        Arc::new(AggregateUDF::new_from_impl(SparkListAgg::new()));
    group.bench_function("list_agg", |b| {
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                vec![c3.clone(), lit(",")],
                list_agg_udf.clone(),
                "list_agg",
            ))
        })
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
