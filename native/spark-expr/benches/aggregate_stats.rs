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
//! the Welford-path statistical aggregates (`variance`, `covariance`,
//! `correlation`), non-decimal `avg`, exact and approximate percentile, and
//! HyperLogLog++ `approx_count_distinct`. Only Comet's own accumulators are
//! benched here. `stddev` is intentionally omitted: `StddevAccumulator` wraps
//! `VarianceAccumulator`, so its per-row update loop is identical to `variance`
//! (it only adds a final `sqrt`).

use arrow::array::{ArrayRef, Float64Builder, RecordBatch, StringBuilder};
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{lit, Column, StatsType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_spark_expr::{
    ApproxPercentile, Avg, Correlation, Covariance, EvalMode, HllPlusPlus, Regr, RegrType,
    SparkPercentile, Variance,
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

    // Single-input accumulators: variance (population + sample) and avg.
    let mut group = c.benchmark_group("stats_agg_single");

    let single_cases: Vec<(&str, Arc<AggregateUDF>)> = vec![
        ("variance_samp", comet_variance(StatsType::Sample)),
        ("variance_pop", comet_variance(StatsType::Population)),
        (
            "avg",
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
        ("covariance_samp", comet_covariance(StatsType::Sample)),
        ("covariance_pop", comet_covariance(StatsType::Population)),
        ("correlation", comet_correlation()),
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

    // Percentile and approximate distinct count.
    let mut group = c.benchmark_group("stats_agg_percentile");

    group.bench_function("approx_percentile", |b| {
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

    group.bench_function("percentile", |b| {
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
        group.bench_function(format!("approx_count_distinct_p{p}"), |b| {
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

const REGRESSION_BATCHES: usize = 32;
const REGRESSION_BATCH_SIZE: usize = 8192;
const REGRESSION_ROWS: usize = REGRESSION_BATCHES * REGRESSION_BATCH_SIZE;

// Measure Partial aggregation (update + state emission), including the DataFusion
// grouping path. Full-query Spark benchmarks also cover finalization and shuffle.
fn regression_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("regr_partial");
    for (groups, null_every, clustered) in [
        (1024, 0, false),
        (1024, 101, false),
        (1024, 2, false),
        (65536, 0, false),
        (1, 0, false),
        (1024, 0, true),
    ] {
        let batches = regression_batches(groups, null_every, clustered);
        let partitions = &[batches];
        for (name, kind) in [
            ("regr_sxx", RegrType::SXX),
            ("regr_syy", RegrType::SYY),
            ("regr_sxy", RegrType::SXY),
            ("regr_slope", RegrType::Slope),
            ("regr_intercept", RegrType::Intercept),
            ("regr_r2", RegrType::R2),
        ] {
            // Cover every function with and without nulls, clustered keys, and
            // one group. Limit the high-cardinality control to compound states.
            if groups == 65536 && !matches!(kind, RegrType::Slope | RegrType::R2) {
                continue;
            }
            let udf = Arc::new(AggregateUDF::new_from_impl(Regr::new(
                kind,
                name,
                true,
                true,
                EvalMode::Legacy,
                None,
                datafusion_comet_spark_expr::create_query_context_map(),
            )));
            let group_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
            // Spark rewrites SXX/SYY to the pair-null-filtered selected column,
            // which Comet's serde passes twice to the native moment accumulator.
            let indices = match kind {
                RegrType::SXX => [4, 4],
                RegrType::SYY => [3, 3],
                _ => [1, 2],
            };
            let inputs: Vec<Arc<dyn PhysicalExpr>> = indices
                .map(|i| Arc::new(Column::new(&format!("c{i}"), i)) as Arc<dyn PhysicalExpr>)
                .to_vec();
            // Check actual cardinality outside timing. Batches use successive row
            // offsets, so the high-cardinality case really contains 64k groups.
            let (rows, aggregate) = rt.block_on(agg_test(
                partitions,
                group_col.clone(),
                inputs.clone(),
                udf.clone(),
                name,
            ));
            assert_eq!(rows, groups);
            let skipped = aggregate
                .metrics()
                .unwrap()
                .sum_by_name("skipped_aggregation_rows")
                .map(|metric| metric.as_usize())
                .unwrap_or(0);
            assert_eq!(skipped, 0);
            let nulls = match null_every {
                0 => "no_null",
                2 => "half_null",
                _ => "sparse_null",
            };
            let layout = if clustered { "/clustered" } else { "" };
            group.bench_function(format!("{name}/{groups}_groups/{nulls}{layout}"), |b| {
                b.to_async(&rt).iter(|| {
                    agg_test(
                        partitions,
                        group_col.clone(),
                        inputs.clone(),
                        udf.clone(),
                        name,
                    )
                })
            });
        }
    }
    group.finish();
}

fn regression_batches(groups: usize, null_every: usize, clustered: bool) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Utf8, false),
        Field::new("c1", DataType::Float64, true),
        Field::new("c2", DataType::Float64, false),
        Field::new("c3", DataType::Float64, true),
        Field::new("c4", DataType::Float64, true),
    ]));
    (0..REGRESSION_BATCHES)
        .map(|batch| {
            let mut keys = StringBuilder::new();
            let mut y = Float64Builder::new();
            let mut x = Float64Builder::new();
            let mut paired_x = Float64Builder::new();
            for row in 0..REGRESSION_BATCH_SIZE {
                let i = batch * REGRESSION_BATCH_SIZE + row;
                let xv = i as f64 * 0.25 + 1.0;
                let yv = xv * 1.5 + (i % 17) as f64;
                let valid = match null_every {
                    0 => true,
                    2 => !(i / groups).is_multiple_of(2),
                    // Coprime to the group count and batch size: every batch
                    // contains sparse nulls and every group still has valid rows.
                    _ => !i.is_multiple_of(null_every),
                };
                let group_index = if clustered {
                    i / (REGRESSION_ROWS / groups)
                } else {
                    i % groups
                };
                keys.append_value(format!("group_{group_index}"));
                y.append_option(valid.then_some(yv));
                x.append_value(xv);
                paired_x.append_option(valid.then_some(xv));
            }
            let y: ArrayRef = Arc::new(y.finish());
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(keys.finish()),
                    y.clone(),
                    Arc::new(x.finish()),
                    y,
                    Arc::new(paired_x.finish()),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn comet_variance(stats_type: StatsType) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(Variance::new(
        "variance",
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
        EvalMode::Legacy,
        None,
        datafusion_comet_spark_expr::create_query_context_map(),
    )))
}

async fn agg_test(
    partitions: &[Vec<RecordBatch>],
    group_col: Arc<dyn PhysicalExpr>,
    input_exprs: Vec<Arc<dyn PhysicalExpr>>,
    aggregate_udf: Arc<AggregateUDF>,
    alias: &str,
) -> (usize, Arc<AggregateExec>) {
    let schema = &partitions[0][0].schema();
    let scan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(partitions, Arc::clone(schema), None).unwrap(),
    )));
    let aggregate = create_aggregate(scan, group_col, input_exprs, schema, aggregate_udf, alias);
    // Match Comet's eligibility gate: regression aggregates cannot skip partial
    // aggregation (convert_to_state is unsupported by the grouped kernels).
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .skip_partial_aggregation_probe_ratio_threshold = 1.1;
    let context = TaskContext::default().with_session_config(config);
    let mut stream = aggregate.execute(0, Arc::new(context)).unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += black_box(batch.unwrap()).num_rows();
    }
    (rows, aggregate)
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
    targets = criterion_benchmark, regression_benchmark
}
criterion_main!(benches);
