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
// under the License.use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, RecordBatch, StringBuilder};

use arrow::array::builder::{Decimal128Builder, Int64Builder, StringBuilder};
use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{AggregateUDF, AggregateUDFImpl, EmitTo};
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{Column, StatsType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_spark_expr::{
    hllpp_precision, ApproxPercentile, Avg, AvgDecimal, Correlation, Covariance, EvalMode,
    HllPlusPlus, SparkPercentile, Stddev, SumDecimal, SumInteger, Variance,
};
use futures::StreamExt;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

#[path = "common/mod.rs"]
mod common;
use common::{f64_array, i64_array, NULL_RATIOS, ROW_COUNTS};

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate");
    let num_rows = 8192;
    let batch = create_record_batch(num_rows);
    let mut batches = Vec::new();
    for _ in 0..10 {
        batches.push(batch.clone());
    }
    let partitions = &[batches];
    let c0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
    let c1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c1", 1));

    let rt = Runtime::new().unwrap();

    group.bench_function("avg_decimal_datafusion", |b| {
        let datafusion_sum_decimal = avg_udaf();
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                c1.clone(),
                datafusion_sum_decimal.clone(),
                "avg",
            ))
        })
    });

    group.bench_function("avg_decimal_comet", |b| {
        let comet_avg_decimal = Arc::new(AggregateUDF::new_from_impl(AvgDecimal::new(
            DataType::Decimal128(38, 10),
            DataType::Decimal128(38, 10),
            datafusion_comet_spark_expr::EvalMode::Legacy,
            None,
            datafusion_comet_spark_expr::create_query_context_map(),
        )));
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                c1.clone(),
                comet_avg_decimal.clone(),
                "avg",
            ))
        })
    });

    group.bench_function("sum_decimal_datafusion", |b| {
        let datafusion_sum_decimal = sum_udaf();
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                c1.clone(),
                datafusion_sum_decimal.clone(),
                "sum",
            ))
        })
    });

    group.bench_function("sum_decimal_comet", |b| {
        let comet_sum_decimal = Arc::new(AggregateUDF::new_from_impl(
            SumDecimal::try_new(
                DataType::Decimal128(38, 10),
                EvalMode::Legacy,
                None,
                datafusion_comet_spark_expr::create_query_context_map(),
            )
            .unwrap(),
        ));
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                c1.clone(),
                comet_sum_decimal.clone(),
                "sum",
            ))
        })
    });

    group.finish();

    // SumInteger benchmarks
    let mut group = c.benchmark_group("sum_integer");
    let int_batch = create_int64_record_batch(num_rows);
    let mut int_batches = Vec::new();
    for _ in 0..10 {
        int_batches.push(int_batch.clone());
    }
    let int_partitions = &[int_batches];
    let int_c0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
    let int_c1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c1", 1));

    group.bench_function("sum_int64_datafusion", |b| {
        let datafusion_sum = sum_udaf();
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                int_partitions,
                int_c0.clone(),
                int_c1.clone(),
                datafusion_sum.clone(),
                "sum",
            ))
        })
    });

    group.bench_function("sum_int64_comet_legacy", |b| {
        let comet_sum = Arc::new(AggregateUDF::new_from_impl(
            SumInteger::try_new(DataType::Int64, EvalMode::Legacy).unwrap(),
        ));
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                int_partitions,
                int_c0.clone(),
                int_c1.clone(),
                comet_sum.clone(),
                "sum",
            ))
        })
    });

    group.bench_function("sum_int64_comet_ansi", |b| {
        let comet_sum = Arc::new(AggregateUDF::new_from_impl(
            SumInteger::try_new(DataType::Int64, EvalMode::Ansi).unwrap(),
        ));
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                int_partitions,
                int_c0.clone(),
                int_c1.clone(),
                comet_sum.clone(),
                "sum",
            ))
        })
    });

    group.bench_function("sum_int64_comet_try", |b| {
        let comet_sum = Arc::new(AggregateUDF::new_from_impl(
            SumInteger::try_new(DataType::Int64, EvalMode::Try).unwrap(),
        ));
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                int_partitions,
                int_c0.clone(),
                int_c1.clone(),
                comet_sum.clone(),
                "sum",
            ))
        })
    });

    group.finish();

    // Direct accumulator benchmarks (bypassing execution framework)
    let mut group = c.benchmark_group("sum_integer_accumulator");
    let int64_array: ArrayRef = Arc::new(Int64Array::from_iter_values(0..8192i64));
    let arrays: Vec<ArrayRef> = vec![int64_array];

    let return_field = Arc::new(Field::new("sum", DataType::Int64, true));
    let schema = Schema::new(vec![Field::new("c0", DataType::Int64, true)]);
    let expr_field = Arc::new(Field::new("c0", DataType::Int64, true));
    let expr_fields: Vec<Arc<Field>> = vec![expr_field];

    // Single-row Accumulator benchmarks
    for (name, eval_mode) in [
        ("row_legacy", EvalMode::Legacy),
        ("row_ansi", EvalMode::Ansi),
        ("row_try", EvalMode::Try),
    ] {
        let return_field = return_field.clone();
        let expr_fields = expr_fields.clone();
        group.bench_function(name, |b| {
            let udf = SumInteger::try_new(DataType::Int64, eval_mode).unwrap();
            b.iter(|| {
                let acc_args = AccumulatorArgs {
                    return_field: return_field.clone(),
                    schema: &schema,
                    ignore_nulls: false,
                    order_bys: &[],
                    name: "sum",
                    is_distinct: false,
                    is_reversed: false,
                    exprs: &[],
                    expr_fields: &expr_fields,
                };
                let mut acc = udf.accumulator(acc_args).unwrap();
                for _ in 0..10 {
                    acc.update_batch(&arrays).unwrap();
                }
                black_box(acc.evaluate().unwrap())
            })
        });
    }

    // GroupsAccumulator benchmarks
    let group_indices: Vec<usize> = (0..8192).map(|i| i % 1024).collect();
    for (name, eval_mode) in [
        ("groups_legacy", EvalMode::Legacy),
        ("groups_ansi", EvalMode::Ansi),
        ("groups_try", EvalMode::Try),
    ] {
        let return_field = return_field.clone();
        let expr_fields = expr_fields.clone();
        group.bench_function(name, |b| {
            let udf = SumInteger::try_new(DataType::Int64, eval_mode).unwrap();
            b.iter(|| {
                let acc_args = AccumulatorArgs {
                    return_field: return_field.clone(),
                    schema: &schema,
                    ignore_nulls: false,
                    order_bys: &[],
                    name: "sum",
                    is_distinct: false,
                    is_reversed: false,
                    exprs: &[],
                    expr_fields: &expr_fields,
                };
                let mut acc = udf.create_groups_accumulator(acc_args).unwrap();
                for _ in 0..10 {
                    acc.update_batch(&arrays, &group_indices, None, 1024)
                        .unwrap();
                }
                black_box(acc.evaluate(EmitTo::All).unwrap())
            })
        });
    }

    group.finish();

    bench_statistical_aggregates(c);
}

/// Benchmarks for the Comet-owned aggregate kernels in `agg_funcs/` not covered above:
/// `avg`, `variance` (pop/samp), `stddev` (pop/samp), `covariance` (pop/samp), `correlation`,
/// `percentile`, `approx_percentile`, and `hll_plus_plus`. Each drives the `Accumulator` directly
/// (`update_batch` + `evaluate`) so it measures the accumulator hot path, not scan/grouping.
fn bench_statistical_aggregates(c: &mut Criterion) {
    bench_f64_one_arg(c, "avg", DataType::Float64, || {
        Box::new(Avg::new("avg", DataType::Float64))
    });

    bench_f64_one_arg(c, "variance_pop", DataType::Float64, || {
        Box::new(Variance::new(
            "var",
            DataType::Float64,
            StatsType::Population,
            true,
        ))
    });
    bench_f64_one_arg(c, "variance_samp", DataType::Float64, || {
        Box::new(Variance::new(
            "var",
            DataType::Float64,
            StatsType::Sample,
            true,
        ))
    });

    bench_f64_one_arg(c, "stddev_pop", DataType::Float64, || {
        Box::new(Stddev::new(
            "stddev",
            DataType::Float64,
            StatsType::Population,
            true,
        ))
    });
    bench_f64_one_arg(c, "stddev_samp", DataType::Float64, || {
        Box::new(Stddev::new(
            "stddev",
            DataType::Float64,
            StatsType::Sample,
            true,
        ))
    });

    bench_f64_two_arg(c, "covariance_pop", || {
        Box::new(Covariance::new(
            "covar",
            DataType::Float64,
            StatsType::Population,
            true,
        ))
    });
    bench_f64_two_arg(c, "covariance_samp", || {
        Box::new(Covariance::new(
            "covar",
            DataType::Float64,
            StatsType::Sample,
            true,
        ))
    });
    bench_f64_two_arg(c, "correlation", || {
        Box::new(Correlation::new("corr", DataType::Float64, true))
    });

    bench_f64_one_arg(c, "percentile", DataType::Float64, || {
        Box::new(SparkPercentile::try_new(0.5).unwrap())
    });
    bench_f64_one_arg(c, "approx_percentile", DataType::Float64, || {
        Box::new(ApproxPercentile::new(
            vec![0.5],
            10_000,
            DataType::Float64,
            false,
        ))
    });

    // hll_plus_plus counts approximate distinct values; feed it an Int64 key column.
    let hll_fields = [Arc::new(Field::new("x", DataType::Int64, true))];
    let precision = hllpp_precision(0.05);
    let mut group = c.benchmark_group("hll_plus_plus");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let arrays = vec![i64_array(rows, null_ratio, |i| (i % 4096) as i64)];
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &arrays,
                |b, arrays| {
                    b.iter(|| {
                        black_box(drive_accumulator(
                            &HllPlusPlus::new(precision),
                            &hll_fields,
                            DataType::Int64,
                            arrays,
                        ))
                    })
                },
            );
        }
    }
    group.finish();
}

/// Build a fresh accumulator for `udf`, feed it `arrays` once, and return the aggregate result.
/// A new accumulator is built per call because accumulators are stateful and cannot be reused.
fn drive_accumulator(
    udf: &dyn AggregateUDFImpl,
    input_fields: &[Arc<Field>],
    return_type: DataType,
    arrays: &[ArrayRef],
) -> ScalarValue {
    let schema = Schema::new(
        input_fields
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    );
    let acc_args = AccumulatorArgs {
        return_field: Arc::new(Field::new("agg", return_type, true)),
        schema: &schema,
        ignore_nulls: false,
        order_bys: &[],
        name: "agg",
        is_distinct: false,
        is_reversed: false,
        exprs: &[],
        expr_fields: input_fields,
    };
    let mut acc = udf.accumulator(acc_args).unwrap();
    acc.update_batch(arrays).unwrap();
    acc.evaluate().unwrap()
}

/// Drive an aggregate that takes a single Float64 input column.
fn bench_f64_one_arg(
    c: &mut Criterion,
    name: &str,
    return_type: DataType,
    make: impl Fn() -> Box<dyn AggregateUDFImpl>,
) {
    let fields = [Arc::new(Field::new("x", DataType::Float64, true))];
    let mut group = c.benchmark_group(name);
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let arrays = vec![f64_array(rows, null_ratio, |i| (i as f64).sin() * 1000.0)];
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &arrays,
                |b, arrays| {
                    b.iter(|| {
                        black_box(drive_accumulator(
                            make().as_ref(),
                            &fields,
                            return_type.clone(),
                            arrays,
                        ))
                    })
                },
            );
        }
    }
    group.finish();
}

/// Drive an aggregate that takes two Float64 input columns (covariance / correlation).
fn bench_f64_two_arg(c: &mut Criterion, name: &str, make: impl Fn() -> Box<dyn AggregateUDFImpl>) {
    let fields = [
        Arc::new(Field::new("x", DataType::Float64, true)),
        Arc::new(Field::new("y", DataType::Float64, true)),
    ];
    let mut group = c.benchmark_group(name);
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let arrays = vec![
                f64_array(rows, null_ratio, |i| (i as f64).sin() * 1000.0),
                f64_array(rows, null_ratio, |i| (i as f64).cos() * 1000.0),
            ];
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &arrays,
                |b, arrays| {
                    b.iter(|| {
                        black_box(drive_accumulator(
                            make().as_ref(),
                            &fields,
                            DataType::Float64,
                            arrays,
                        ))
                    })
                },
            );
        }
    }
    group.finish();
}

async fn agg_test(
    partitions: &[Vec<RecordBatch>],
    c0: Arc<dyn PhysicalExpr>,
    c1: Arc<dyn PhysicalExpr>,
    aggregate_udf: Arc<AggregateUDF>,
    alias: &str,
) {
    let schema = &partitions[0][0].schema();
    let scan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(partitions, Arc::clone(schema), None).unwrap(),
    )));
    let aggregate = create_aggregate(scan, c0.clone(), c1.clone(), schema, aggregate_udf, alias);
    let mut stream = aggregate
        .execute(0, Arc::new(TaskContext::default()))
        .unwrap();
    while let Some(batch) = stream.next().await {
        let _batch = batch.unwrap();
    }
}

fn create_aggregate(
    scan: Arc<dyn ExecutionPlan>,
    c0: Arc<dyn PhysicalExpr>,
    c1: Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
    aggregate_udf: Arc<AggregateUDF>,
    alias: &str,
) -> Arc<AggregateExec> {
    let aggr_expr = AggregateExprBuilder::new(aggregate_udf, vec![c1])
        .schema(schema.clone())
        .alias(alias)
        .with_ignore_nulls(false)
        .with_distinct(false)
        .build()
        .unwrap();

    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(c0, "c0".to_string())]),
            vec![aggr_expr.into()],
            vec![None], // no filter expressions
            scan,
            Arc::clone(schema),
        )
        .unwrap(),
    )
}

fn create_record_batch(num_rows: usize) -> RecordBatch {
    let mut decimal_builder = Decimal128Builder::with_capacity(num_rows);
    let mut string_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
    for i in 0..num_rows {
        decimal_builder.append_value(i as i128);
        string_builder.append_value(format!("this is string #{}", i % 1024));
    }
    let decimal_array = Arc::new(decimal_builder.finish());
    let string_array = Arc::new(string_builder.finish());

    let mut fields = vec![];
    let mut columns: Vec<ArrayRef> = vec![];

    // string column
    fields.push(Field::new("c0", DataType::Utf8, false));
    columns.push(string_array);

    // decimal column
    fields.push(Field::new("c1", DataType::Decimal128(38, 10), false));
    columns.push(decimal_array);

    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn create_int64_record_batch(num_rows: usize) -> RecordBatch {
    let mut int64_builder = Int64Builder::with_capacity(num_rows);
    let mut string_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
    for i in 0..num_rows {
        int64_builder.append_value(i as i64);
        string_builder.append_value(format!("group_{}", i % 1024));
    }
    let int64_array = Arc::new(int64_builder.finish());
    let string_array = Arc::new(string_builder.finish());

    let mut fields = vec![];
    let mut columns: Vec<ArrayRef> = vec![];

    // string column for grouping
    fields.push(Field::new("c0", DataType::Utf8, false));
    columns.push(string_array);

    // int64 column for summing
    fields.push(Field::new("c1", DataType::Int64, false));
    columns.push(int64_array);

    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
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
