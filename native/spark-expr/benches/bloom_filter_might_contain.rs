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

//! Benchmark for the per-row `might_contain` probe (`BloomFilterMightContain`). The filter it
//! probes against is built up front by running the public `BloomFilterAgg` over a column of
//! longs, so the bench exercises only the probe path (`might_contain_longs`) and not filter
//! construction.

use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::{AggregateUDF, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{lit, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_comet_spark_expr::{
    BloomFilterAgg, BloomFilterMightContain, SparkBloomFilterVersion,
};
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

const DISTINCT_ITEMS: usize = 8192;
const NUM_BITS: i32 = (DISTINCT_ITEMS as i32) * 16;

/// Build a serialized Spark bloom filter containing `0..DISTINCT_ITEMS` by running the public
/// `BloomFilterAgg` once. Returns the filter's Spark-serialized bytes.
fn build_filter_bytes(rt: &Runtime) -> Vec<u8> {
    let values: ArrayRef = Arc::new(Int64Array::from(
        (0..DISTINCT_ITEMS as i64).collect::<Vec<_>>(),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values]).unwrap();

    let scan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&schema), None).unwrap(),
    )));
    let udf = AggregateUDF::new_from_impl(BloomFilterAgg::new(
        lit(ScalarValue::Int64(Some(DISTINCT_ITEMS as i64))),
        lit(ScalarValue::Int64(Some(NUM_BITS as i64))),
        DataType::Binary,
        SparkBloomFilterVersion::V1,
    ));
    let aggr_expr = AggregateExprBuilder::new(
        Arc::new(udf),
        vec![Arc::new(Column::new("v", 0)) as Arc<dyn PhysicalExpr>],
    )
    .schema(Arc::clone(&schema))
    .alias("bloom_filter")
    .build()
    .unwrap();

    let aggregate: Arc<dyn ExecutionPlan> = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new(vec![], vec![], vec![vec![]], false),
            vec![aggr_expr.into()],
            vec![None],
            scan,
            schema,
        )
        .unwrap(),
    );

    let ctx = SessionContext::new();
    let batches = rt.block_on(collect(aggregate, ctx.task_ctx())).unwrap();
    let col = batches[0].column(0);
    let bytes = col.as_any().downcast_ref::<BinaryArray>().unwrap();
    bytes.value(0).to_vec()
}

/// Build an Int64 probe column: the first half are hits (`0..rows/2`, present in the filter),
/// the second half are misses (large values not inserted). Every `null_every`-th row is null
/// (`null_every == 0` means no nulls).
fn create_probe_array(rows: usize, null_every: usize) -> ArrayRef {
    let arr: Int64Array = (0..rows)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else if i < rows / 2 {
                Some(i as i64)
            } else {
                Some(i as i64 + 1_000_000_000)
            }
        })
        .collect();
    Arc::new(arr)
}

fn run(udf: &BloomFilterMightContain, values: &ArrayRef, rows: usize) {
    let args = vec![ColumnarValue::Array(Arc::clone(values))];
    black_box(
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap(),
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;
    let rt = Runtime::new().unwrap();
    let filter_bytes = build_filter_bytes(&rt);
    let udf =
        BloomFilterMightContain::try_new(lit(ScalarValue::Binary(Some(filter_bytes)))).unwrap();

    let no_nulls = create_probe_array(rows, 0);
    let with_nulls = create_probe_array(rows, 10);

    let mut group = c.benchmark_group("bloom_filter_might_contain");
    group.bench_function("probe no nulls", |b| b.iter(|| run(&udf, &no_nulls, rows)));
    group.bench_function("probe with nulls", |b| {
        b.iter(|| run(&udf, &with_nulls, rows))
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
