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

use super::super::parquet_reader::concrete_parquet_source;
use super::super::test_utils::{omit_null_counts, parquet_input, parquet_scan, session};
use super::*;
use arrow::array::Int32Array;
use arrow::compute::cast;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::{lit, CastExpr, IsNotNullExpr};
use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;

mod correctness;
mod eligibility;
mod lifecycle;
mod reader;

fn aggregate(input: Arc<dyn ExecutionPlan>, is_max: bool) -> AggregateExec {
    let expr = AggregateExprBuilder::new(
        if is_max { max_udaf() } else { min_udaf() },
        vec![Arc::new(Column::new("key", 0))],
    )
    .schema(input.schema())
    .alias("extremum")
    .build()
    .unwrap();
    AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::default(),
        vec![Arc::new(expr)],
        vec![None],
        Arc::clone(&input),
        input.schema(),
    )
    .unwrap()
}

fn wrapper(aggregate: &AggregateExec) -> MinMaxReaderFilterExec {
    MinMaxReaderFilterExec::try_new(aggregate, &ConfigOptions::default())
        .unwrap()
        .expect("eligible MIN/MAX")
}

fn result(batches: &[RecordBatch]) -> Option<i32> {
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = batches.iter().find(|batch| batch.num_rows() == 1).unwrap();
    let column = cast(batch.column(0), &DataType::Int32).unwrap();
    column
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .next()
        .unwrap()
}

fn metric(plan: &dyn ExecutionPlan, name: &str) -> usize {
    plan.metrics()
        .and_then(|metrics| match name {
            "output_rows" => metrics.output_rows(),
            _ => metrics.sum_by_name(name).map(|value| value.as_usize()),
        })
        .unwrap_or(0)
}

fn produced_filter(aggregate: &dyn ExecutionPlan) -> Arc<DynamicFilterPhysicalExpr> {
    let mut expressions = aggregate.dynamic_expressions_produced();
    assert_eq!(expressions.len(), 1);
    (expressions.pop().unwrap() as Arc<dyn std::any::Any + Send + Sync>)
        .downcast::<DynamicFilterPhysicalExpr>()
        .unwrap()
}
