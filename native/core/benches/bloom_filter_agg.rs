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

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::Int64Builder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use comet::execution::expressions::bloom_filter_agg::BloomFilterAgg;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::ScalarValue;
use datafusion_execution::TaskContext;
use datafusion_expr::AggregateUDF;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::{Column, Literal};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_filter_agg");
    let num_rows = 8192;
    let batch = create_record_batch(num_rows);
    let mut batches = Vec::new();
    for _ in 0..10 {
        batches.push(batch.clone());
    }
    let partitions = &[batches];
    let c0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
    // spark.sql.optimizer.runtime.bloomFilter.expectedNumItems
    let num_items_sv = ScalarValue::Int64(Some(1000000_i64));
    let num_items: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(num_items_sv));
    //spark.sql.optimizer.runtime.bloomFilter.numBits
    let num_bits_sv = ScalarValue::Int64(Some(8388608_i64));
    let num_bits: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(num_bits_sv));

    let rt = Runtime::new().unwrap();

    for agg_mode in [
        ("partial_agg", AggregateMode::Partial),
        ("single_agg", AggregateMode::Single),
    ] {
        group.bench_function(agg_mode.0, |b| {
            let comet_bloom_filter_agg =
                Arc::new(AggregateUDF::new_from_impl(BloomFilterAgg::new(
                    Arc::clone(&num_items),
                    Arc::clone(&num_bits),
                    DataType::Binary,
                )));
            b.to_async(&rt).iter(|| {
                black_box(agg_test(
                    partitions,
                    c0.clone(),
                    comet_bloom_filter_agg.clone(),
                    "bloom_filter_agg",
                    agg_mode.1,
                ))
            })
        });
    }

    group.finish();
}

async fn agg_test(
    partitions: &[Vec<RecordBatch>],
    c0: Arc<dyn PhysicalExpr>,
    aggregate_udf: Arc<AggregateUDF>,
    alias: &str,
    mode: AggregateMode,
) {
    let schema = &partitions[0][0].schema();
    let scan: Arc<dyn ExecutionPlan> =
        Arc::new(MemoryExec::try_new(partitions, Arc::clone(schema), None).unwrap());
    let aggregate = create_aggregate(scan, c0.clone(), schema, aggregate_udf, alias, mode);
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
    schema: &SchemaRef,
    aggregate_udf: Arc<AggregateUDF>,
    alias: &str,
    mode: AggregateMode,
) -> Arc<AggregateExec> {
    let aggr_expr = AggregateExprBuilder::new(aggregate_udf, vec![c0.clone()])
        .schema(schema.clone())
        .alias(alias)
        .with_ignore_nulls(false)
        .with_distinct(false)
        .build()
        .unwrap();

    Arc::new(
        AggregateExec::try_new(
            mode,
            PhysicalGroupBy::new_single(vec![]),
            vec![aggr_expr.into()],
            vec![None],
            scan,
            Arc::clone(schema),
        )
        .unwrap(),
    )
}

fn create_record_batch(num_rows: usize) -> RecordBatch {
    let mut int64_builder = Int64Builder::with_capacity(num_rows);
    for i in 0..num_rows {
        int64_builder.append_value(i as i64);
    }
    let int64_array = Arc::new(int64_builder.finish());

    let mut fields = vec![];
    let mut columns: Vec<ArrayRef> = vec![];

    // int64 column
    fields.push(Field::new("c0", DataType::Int64, false));
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
