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

//! Benchmarks for the Comet HyperLogLog aggregates in `agg_funcs`: `hll_sketch_agg`
//! (builds a sketch from raw values) and `hll_union_agg` (merges serialized sketches).
//! Each runs as a grouped `AggregateMode::Partial` over 8192 rows x 10 batches, mirroring
//! the other aggregate benches. The union input is a column of real sketch bytes minted
//! up front with `SparkHllSketch`, since the kernel deserializes and merges each one.

use arrow::array::{ArrayRef, BinaryBuilder, Int64Builder, RecordBatch, StringBuilder};
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
use datafusion_comet_spark_expr::{HllSketchAgg, HllUnionAgg, SparkHllSketch};
use futures::StreamExt;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Spark's default `lgConfigK` for `approx_count_distinct`/HLL sketches.
const LG_CONFIG_K: u8 = 12;
/// Number of distinct sketches minted for the union input; rows cycle through them.
const SKETCH_POOL: usize = 256;

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

/// A pool of `SKETCH_POOL` distinct non-empty sketches, each covering a disjoint run of i64
/// values so a union of several estimates to a meaningful cardinality.
fn sketch_pool() -> Vec<Vec<u8>> {
    (0..SKETCH_POOL)
        .map(|p| {
            let mut sketch = SparkHllSketch::new(LG_CONFIG_K);
            let start = (p * 100) as i64;
            for v in start..start + 100 {
                sketch.update_i64(v);
            }
            sketch.to_sketch_bytes()
        })
        .collect()
}

/// `c0` is the Utf8 group key (1024 groups); `c1` is a spread-out Int64 input for
/// `hll_sketch_agg`; `c2` is a column of serialized sketches for `hll_union_agg`.
fn create_record_batch(num_rows: usize, pool: &[Vec<u8>]) -> RecordBatch {
    let mut group_builder = StringBuilder::with_capacity(num_rows, num_rows * 8);
    let mut c1_builder = Int64Builder::with_capacity(num_rows);
    let mut c2_builder = BinaryBuilder::new();
    for i in 0..num_rows {
        group_builder.append_value(format!("group_{}", i % 1024));
        c1_builder.append_value(i as i64);
        c2_builder.append_value(&pool[i % pool.len()]);
    }

    let fields = vec![
        Field::new("c0", DataType::Utf8, false),
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Binary, false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(group_builder.finish()),
        Arc::new(c1_builder.finish()),
        Arc::new(c2_builder.finish()),
    ];

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let num_rows = 8192;
    let pool = sketch_pool();
    let batch = create_record_batch(num_rows, &pool);
    let batches = vec![batch; 10];
    let partitions = &[batches];

    let c0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
    let c1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c1", 1));
    let c2: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c2", 2));

    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("hll_agg");

    let sketch_udf: Arc<AggregateUDF> = Arc::new(AggregateUDF::new_from_impl(HllSketchAgg::new(
        LG_CONFIG_K as i32,
    )));
    group.bench_function("hll_sketch_agg", |b| {
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                vec![c1.clone()],
                sketch_udf.clone(),
                "hll_sketch_agg",
            ))
        })
    });

    let union_udf: Arc<AggregateUDF> =
        Arc::new(AggregateUDF::new_from_impl(HllUnionAgg::new(false)));
    group.bench_function("hll_union_agg", |b| {
        b.to_async(&rt).iter(|| {
            black_box(agg_test(
                partitions,
                c0.clone(),
                vec![c2.clone()],
                union_udf.clone(),
                "hll_union_agg",
            ))
        })
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
