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

use super::super::test_utils::{omit_null_counts, parquet_input, parquet_scan, session};
use super::*;

use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::ObjectStoreBackend;
use arrow::array::{ArrayRef, Int32Array};
use arrow::compute::{cast, SortOptions};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

mod correctness;
mod eligibility;
mod lifecycle;
mod reader;
mod schema;
mod statistics;
mod timestamp;

fn batch(values: Vec<Option<i32>>, key_type: &DataType) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("key", key_type.clone(), true)]));
    RecordBatch::try_new(
        schema,
        vec![cast(&Int32Array::from(values), key_type).unwrap()],
    )
    .unwrap()
}

fn memory_input(values: Vec<Option<i32>>, key_type: &DataType) -> Arc<dyn ExecutionPlan> {
    let batch = batch(values, key_type);
    let batches = if batch.num_rows() == 0 {
        vec![batch.clone()]
    } else {
        (0..batch.num_rows())
            .step_by(2)
            .map(|offset| batch.slice(offset, 2.min(batch.num_rows() - offset)))
            .collect()
    };
    MemorySourceConfig::try_new_exec(&[batches], batch.schema(), None).unwrap()
}

fn sort(input: Arc<dyn ExecutionPlan>, k: usize, options: SortOptions) -> SortExec {
    SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("key", 0)),
            options,
        }])
        .unwrap(),
        input,
    )
    .with_preserve_partitioning(true)
    .with_fetch(Some(k))
}

fn wrapper(sort: &SortExec, session: &SessionContext) -> TopKReaderFilterExec {
    TopKReaderFilterExec::try_new(sort, session.copied_config().options())
        .unwrap()
        .expect("eligible TopK")
}

fn produced_filter(sort: &SortExec) -> Arc<DynamicFilterPhysicalExpr> {
    let mut expressions = sort.dynamic_expressions_produced();
    assert_eq!(expressions.len(), 1);
    (expressions.pop().unwrap() as Arc<dyn std::any::Any + Send + Sync>)
        .downcast::<DynamicFilterPhysicalExpr>()
        .unwrap()
}

fn keys(batches: &[RecordBatch]) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = cast(batch.column(0), &DataType::Int32).unwrap();
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn count_metric(plan: &dyn ExecutionPlan, name: &str) -> usize {
    plan.metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map_or(0, |metric| metric.as_usize())
}
