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

use arrow::array::builder::{Float64Builder, Int64Builder};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_spark_expr::ApproxPercentile;
use futures::StreamExt;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

const BATCH_SIZE: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("approx_percentile");
    let rt = Runtime::new().unwrap();

    // Scalar aggregation: one summary, exercises insert/flush/compress.
    let scalar_batches = create_batches(BATCH_SIZE * 32, 1);
    // High-cardinality grouped aggregation: one summary per group with ~8
    // values each, exercises per-group insert plus grouped merge of digests.
    let grouped_batches = create_batches(BATCH_SIZE * 32, 32768);

    for (name, batches, group_by) in [
        ("scalar", &scalar_batches, false),
        ("grouped_high_card", &grouped_batches, true),
    ] {
        let partitions = vec![batches.clone()];
        group.bench_function(format!("{name}_partial"), |b| {
            b.to_async(&rt)
                .iter(|| black_box(agg_test(&partitions, group_by, false)))
        });
        group.bench_function(format!("{name}_partial_final"), |b| {
            b.to_async(&rt)
                .iter(|| black_box(agg_test(&partitions, group_by, true)))
        });
    }

    group.finish();
}

/// Runs `approx_percentile(c0, 0.5)` over the partitions, either as a single
/// Partial stage (update + serialize state) or as a Partial -> Final pair
/// (additionally deserializing and merging every partial digest).
async fn agg_test(partitions: &[Vec<RecordBatch>], group_by: bool, add_final: bool) {
    let schema: SchemaRef = partitions[0][0].schema();
    let scan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None).unwrap(),
    )));

    let udaf = Arc::new(AggregateUDF::new_from_impl(ApproxPercentile::new(
        vec![0.5],
        10000,
        DataType::Float64,
        false,
    )));
    let c0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
    let aggr_expr = Arc::new(
        AggregateExprBuilder::new(udaf, vec![c0])
            .schema(Arc::clone(&schema))
            .alias("approx_percentile")
            .with_ignore_nulls(false)
            .with_distinct(false)
            .build()
            .unwrap(),
    );

    let grouping = if group_by {
        let g: Arc<dyn PhysicalExpr> = Arc::new(Column::new("g", 1));
        PhysicalGroupBy::new_single(vec![(g, "g".to_string())])
    } else {
        PhysicalGroupBy::new_single(vec![])
    };

    let partial = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            grouping.clone(),
            vec![Arc::clone(&aggr_expr)],
            vec![None],
            scan,
            Arc::clone(&schema),
        )
        .unwrap(),
    );

    let plan: Arc<dyn ExecutionPlan> = if add_final {
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                grouping.as_final(),
                vec![aggr_expr],
                vec![None],
                partial,
                schema,
            )
            .unwrap(),
        )
    } else {
        partial
    };

    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    while let Some(batch) = stream.next().await {
        let _batch = batch.unwrap();
    }
}

fn create_batches(num_rows: usize, num_groups: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Float64, false),
        Field::new("g", DataType::Int64, false),
    ]));
    let mut batches = Vec::new();
    let mut row = 0usize;
    while row < num_rows {
        let n = BATCH_SIZE.min(num_rows - row);
        let mut values = Float64Builder::with_capacity(n);
        let mut groups = Int64Builder::with_capacity(n);
        for i in row..row + n {
            // Deterministic pseudo-random values, spread across groups.
            values.append_value((i.wrapping_mul(2654435761) % 1000003) as f64);
            groups.append_value((i % num_groups) as i64);
        }
        let columns: Vec<ArrayRef> = vec![Arc::new(values.finish()), Arc::new(groups.finish())];
        batches.push(RecordBatch::try_new(Arc::clone(&schema), columns).unwrap());
        row += n;
    }
    batches
}

fn config() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(1))
        .sample_size(10)
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
