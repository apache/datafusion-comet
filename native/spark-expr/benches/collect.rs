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

//! Grouped `collect_list` / `collect_set` aggregation over a high-cardinality string key,
//! the shape reported in <https://github.com/apache/datafusion-comet/issues/5797>.

use arrow::array::builder::{Int64Builder, StringBuilder};
use arrow::array::{ArrayRef, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{
    datasource::memory::MemorySourceConfig, datasource::source::DataSourceExec,
    physical_plan::collect,
};
use datafusion_comet_spark_expr::{CometCollectList, CometCollectSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

const BATCH_SIZE: usize = 8192;
const NUM_BATCHES: usize = 16;

/// Element column under aggregation.
#[derive(Clone, Copy)]
enum Element {
    Int64,
    Utf8,
    Struct,
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("collect");

    // (label, element type, distinct group keys, distinct element values, null ratio)
    let shapes = [
        ("int64/high_card", Element::Int64, 16384, 1 << 20, 0),
        ("utf8/high_card", Element::Utf8, 16384, 1 << 20, 0),
        ("utf8/low_card", Element::Utf8, 64, 1 << 20, 0),
        ("utf8/dup_elements", Element::Utf8, 16384, 64, 0),
        ("utf8/nulls", Element::Utf8, 16384, 1 << 20, 3),
        ("struct/high_card", Element::Struct, 16384, 1 << 20, 0),
    ];

    for (label, element, num_groups, num_values, null_every) in shapes {
        let partitions = vec![build_batches(element, num_groups, num_values, null_every)];
        for (fn_name, udf) in [
            (
                "collect_list",
                Arc::new(AggregateUDF::new_from_impl(CometCollectList::new())),
            ),
            (
                "collect_set",
                Arc::new(AggregateUDF::new_from_impl(CometCollectSet::new())),
            ),
        ] {
            for two_stage in [false, true] {
                let mode = if two_stage { "two_stage" } else { "partial" };
                group.bench_function(format!("{fn_name}/{label}/{mode}"), |b| {
                    b.iter(|| {
                        rt.block_on(async {
                            run(&partitions, Arc::clone(&udf), two_stage).await;
                        })
                    })
                });
            }
        }
    }

    group.finish();
}

async fn run(partitions: &[Vec<RecordBatch>], udf: Arc<AggregateUDF>, two_stage: bool) {
    let schema = partitions[0][0].schema();
    let scan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None).unwrap(),
    )));
    let plan = aggregate_plan(scan, &schema, udf, two_stage);
    let batches = collect(plan, Arc::new(TaskContext::default()))
        .await
        .unwrap();
    assert!(!batches.is_empty());
}

fn aggregate_plan(
    scan: Arc<dyn ExecutionPlan>,
    schema: &SchemaRef,
    udf: Arc<AggregateUDF>,
    two_stage: bool,
) -> Arc<dyn ExecutionPlan> {
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 1));
    let aggr_expr = Arc::new(
        AggregateExprBuilder::new(udf, vec![value])
            .schema(Arc::clone(schema))
            .alias("agg")
            .build()
            .unwrap(),
    );

    let partial: Arc<dyn ExecutionPlan> = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(Arc::clone(&key), "key".to_string())]),
            vec![Arc::clone(&aggr_expr)],
            vec![None],
            scan,
            Arc::clone(schema),
        )
        .unwrap(),
    );

    if !two_stage {
        return partial;
    }

    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(vec![(key, "key".to_string())]),
            vec![aggr_expr],
            vec![None],
            partial,
            Arc::clone(schema),
        )
        .unwrap(),
    )
}

fn build_batches(
    element: Element,
    num_groups: usize,
    num_values: usize,
    null_every: usize,
) -> Vec<RecordBatch> {
    (0..NUM_BATCHES)
        .map(|batch| build_batch(element, num_groups, num_values, null_every, batch))
        .collect()
}

fn build_batch(
    element: Element,
    num_groups: usize,
    num_values: usize,
    null_every: usize,
    batch: usize,
) -> RecordBatch {
    let start = batch * BATCH_SIZE;
    let mut keys = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 16);
    for i in start..start + BATCH_SIZE {
        keys.append_value(format!("key_{}", i % num_groups));
    }
    let keys: ArrayRef = Arc::new(keys.finish());

    let is_null = |i: usize| null_every != 0 && i.is_multiple_of(null_every);
    let (value_field, values): (Field, ArrayRef) = match element {
        Element::Int64 => {
            let mut b = Int64Builder::with_capacity(BATCH_SIZE);
            for i in start..start + BATCH_SIZE {
                if is_null(i) {
                    b.append_null();
                } else {
                    b.append_value((i % num_values) as i64);
                }
            }
            (
                Field::new("value", DataType::Int64, true),
                Arc::new(b.finish()),
            )
        }
        Element::Utf8 => {
            let mut b = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 24);
            for i in start..start + BATCH_SIZE {
                if is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(format!("this is value #{}", i % num_values));
                }
            }
            (
                Field::new("value", DataType::Utf8, true),
                Arc::new(b.finish()),
            )
        }
        Element::Struct => {
            let mut a = StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 24);
            let mut b = Int64Builder::with_capacity(BATCH_SIZE);
            for i in start..start + BATCH_SIZE {
                a.append_value(format!("this is value #{}", i % num_values));
                b.append_value((i % num_values) as i64);
            }
            let fields: Fields = vec![
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(Field::new("b", DataType::Int64, true)),
            ]
            .into();
            let struct_array = StructArray::new(
                fields.clone(),
                vec![Arc::new(a.finish()) as ArrayRef, Arc::new(b.finish())],
                None,
            );
            (
                Field::new("value", DataType::Struct(fields), true),
                Arc::new(struct_array),
            )
        }
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        value_field,
    ]));
    RecordBatch::try_new(schema, vec![keys, values]).unwrap()
}

fn config() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::from_millis(2000))
        .warm_up_time(Duration::from_millis(500))
        .sample_size(10)
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
