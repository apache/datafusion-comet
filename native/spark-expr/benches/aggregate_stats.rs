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

//! Benchmarks for Comet-owned `agg_funcs` accumulators not covered by
//! `aggregate.rs` (which covers `avg_decimal` / `sum_decimal` / `sum_int`):
//! the Welford-path statistical aggregates (`variance`, `stddev`, `covariance`,
//! `correlation`), non-decimal `avg`, exact and approximate percentile, and
//! HyperLogLog++ `approx_count_distinct`. Each Comet accumulator is benched
//! alongside its DataFusion built-in (where one exists) so the Comet-owned
//! update loop can be attributed independently of the shared execution
//! framework.

use arrow::array::{ArrayRef, Float64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::correlation::corr_udaf;
use datafusion::functions_aggregate::covariance::{covar_pop_udaf, covar_samp_udaf};
use datafusion::functions_aggregate::stddev::{stddev_pop_udaf, stddev_udaf};
use datafusion::functions_aggregate::variance::{var_pop_udaf, var_samp_udaf};
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{lit, Column, StatsType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_spark_expr::{
    ApproxPercentile, Avg, Correlation, Covariance, HllPlusPlus, SparkPercentile, Stddev, Variance,
};
use futures::StreamExt;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

fn criterion_benchmark(c: &mut Criterion) {
    let num_rows = 8192;
    let batch = create_float_record_batch(num_rows);
    let mut batches = Vec::new();
    for _ in 0..10 {
        batches.push(batch.clone());
    }
    let partitions = &[batches];

    let c0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
    let c1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c1", 1));
    let c2: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c2", 2));

    let rt = Runtime::new().unwrap();

    // Single-input accumulators: variance / stddev (population + sample) and avg.
    let mut group = c.benchmark_group("stats_agg_single");

    let single_cases: Vec<(&str, Arc<AggregateUDF>)> = vec![
        ("variance_samp_datafusion", var_samp_udaf()),
        ("variance_samp_comet", comet_variance(StatsType::Sample)),
        ("variance_pop_datafusion", var_pop_udaf()),
        ("variance_pop_comet", comet_variance(StatsType::Population)),
        ("stddev_samp_datafusion", stddev_udaf()),
        ("stddev_samp_comet", comet_stddev(StatsType::Sample)),
        ("stddev_pop_datafusion", stddev_pop_udaf()),
        ("stddev_pop_comet", comet_stddev(StatsType::Population)),
        ("avg_datafusion", avg_udaf()),
        (
            "avg_comet",
            Arc::new(AggregateUDF::new_from_impl(Avg::new(
                "avg",
                DataType::Float64,
            ))),
        ),
    ];

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

    // Two-input accumulators: covariance (population + sample) / correlation.
    let mut group = c.benchmark_group("stats_agg_pair");

    let pair_cases: Vec<(&str, Arc<AggregateUDF>)> = vec![
        ("covariance_samp_datafusion", covar_samp_udaf()),
        ("covariance_samp_comet", comet_covariance(StatsType::Sample)),
        ("covariance_pop_datafusion", covar_pop_udaf()),
        (
            "covariance_pop_comet",
            comet_covariance(StatsType::Population),
        ),
        ("correlation_datafusion", corr_udaf()),
        ("correlation_comet", comet_correlation()),
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

    // Percentile and approximate distinct count (Comet-owned; no directly
    // comparable Spark-compatible DataFusion built-in, so Comet-only here).
    let mut group = c.benchmark_group("stats_agg_percentile");

    group.bench_function("approx_percentile_comet", |b| {
        let udf = Arc::new(AggregateUDF::new_from_impl(ApproxPercentile::new(
            vec![0.5],
            10000,
            DataType::Float64,
            false,
        )));
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                vec![c1.clone()],
                udf.clone(),
                "approx_percentile",
            ))
        })
    });

    group.bench_function("percentile_comet", |b| {
        let udf = Arc::new(AggregateUDF::new_from_impl(
            SparkPercentile::try_new(0.5).unwrap(),
        ));
        // Mirror the planner: exact percentile takes the value column plus the
        // percentage literal as inputs.
        let inputs: Vec<Arc<dyn PhysicalExpr>> = vec![c1.clone(), lit(0.5f64)];
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                inputs.clone(),
                udf.clone(),
                "percentile",
            ))
        })
    });

    for p in [10i32, 14] {
        group.bench_function(format!("approx_count_distinct_comet_p{p}"), |b| {
            let udf = Arc::new(AggregateUDF::new_from_impl(HllPlusPlus::new(p)));
            b.to_async(&rt).iter(|| {
                black_box(agg_test(
                    partitions,
                    c0.clone(),
                    vec![c1.clone()],
                    udf.clone(),
                    "approx_count_distinct",
                ))
            })
        });
    }
    group.finish();
}

fn comet_variance(stats_type: StatsType) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(Variance::new(
        "variance",
        DataType::Float64,
        stats_type,
        false,
    )))
}

fn comet_stddev(stats_type: StatsType) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(Stddev::new(
        "stddev",
        DataType::Float64,
        stats_type,
        false,
    )))
}

fn comet_covariance(stats_type: StatsType) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(Covariance::new(
        "covariance",
        DataType::Float64,
        stats_type,
        false,
    )))
}

fn comet_correlation() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(Correlation::new(
        "correlation",
        DataType::Float64,
        false,
    )))
}

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

fn create_float_record_batch(num_rows: usize) -> RecordBatch {
    let mut c1_builder = Float64Builder::with_capacity(num_rows);
    let mut c2_builder = Float64Builder::with_capacity(num_rows);
    let mut string_builder = StringBuilder::with_capacity(num_rows, num_rows * 8);
    for i in 0..num_rows {
        // Spread-out deterministic values: representative (non-constant) input
        // that also avoids the degenerate zero-variance / undefined-correlation
        // cases.
        c1_builder.append_value(i as f64 * 1.5 + 0.25);
        c2_builder.append_value((num_rows - i) as f64 * 0.75 - 0.5);
        string_builder.append_value(format!("group_{}", i % 1024));
    }

    let fields = vec![
        Field::new("c0", DataType::Utf8, false),
        Field::new("c1", DataType::Float64, false),
        Field::new("c2", DataType::Float64, false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(string_builder.finish()),
        Arc::new(c1_builder.finish()),
        Arc::new(c2_builder.finish()),
    ];

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
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
