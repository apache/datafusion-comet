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
use arrow_array::builder::{Int32Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::Column;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use futures::StreamExt;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate");
    let num_rows = 8192;
    let batch = create_record_batch(num_rows);
    let mut batches = Vec::new();
    for _ in 0..10 {
        batches.push(batch.clone());
    }
    let partitions = &[batches];
    let scan : Arc<dyn ExecutionPlan> = Arc::new(MemoryExec::try_new(partitions, batch.schema(), None).unwrap());
    let c0 = Arc::new(Column::new("c0", 0));
    let c1 = Arc::new(Column::new("c1", 1));

    let schema = scan.schema();

    let aggr_expr = AggregateExprBuilder::new(sum_udaf(), vec![c1])
        .schema(schema.clone())
        .alias("sum")
        .with_ignore_nulls(false)
        .with_distinct(false)
        .build()
        .unwrap();

    let aggregate = Arc::new(
        datafusion::physical_plan::aggregates::AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(c0, "c0".to_string())]),
            vec![aggr_expr],
            vec![None], // no filter expressions
            scan,
            Arc::clone(&schema),
        ).unwrap()
    );

    let rt = Runtime::new().unwrap();

    group.bench_function("aggregate - sum int", |b| {
        b.to_async(&rt).iter(|| async {
            let mut x = aggregate.execute(0, Arc::new(TaskContext::default())).unwrap();
            while let Some(batch) = x.next().await {
                let _batch = batch.unwrap();
            }
        })
    });

    group.finish();
}



fn create_record_batch(num_rows: usize) -> RecordBatch {
    let mut int32_builder = Int32Builder::with_capacity(num_rows);
    let mut string_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
    for i in 0..num_rows {
        int32_builder.append_value(i as i32);
        string_builder.append_value(format!("this is string #{i}"));
    }
    let int32_array = Arc::new(int32_builder.finish());
    let string_array = Arc::new(string_builder.finish());

    let mut fields = vec![];
    let mut columns: Vec<ArrayRef> = vec![];
    let mut i = 0;
    // string column
    fields.push(Field::new(format!("c{i}"), DataType::Utf8, false));
    columns.push(string_array.clone()); // note this is just copying a reference to the array
    i += 1;
    // int column
    fields.push(Field::new(format!("c{i}"), DataType::Int32, false));
    columns.push(int32_array.clone()); // note this is just copying a reference to the array
    // i += 1;
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
