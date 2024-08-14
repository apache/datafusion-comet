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

#[macro_use]
extern crate criterion;
extern crate arrow;
extern crate datafusion;

use arrow::record_batch::RecordBatch;
use criterion::Criterion;
use datafusion::common::JoinType;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::stream::StreamExt;
use log::debug;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn criterion_benchmark_join(c: &mut Criterion) {
    let config = SessionConfig::new().with_target_partitions(2);
    let ctx = SessionContext::new_with_config(config);

    let data_path =
        std::env::var("TPCDS_DATA_PATH").unwrap_or_else(panic!("TPCDS_DATA_PATH env var not set"));

    let mut group = c.benchmark_group("tpcds");
    group.bench_function("hash_join_date_dim_store_sales", |b| {
        let rt = Runtime::new().unwrap();

        /*
        select ...
        from date_dim join store_sales on d_date_sk = ss_sold_date_sk
         */
        let date_dim = rt
            .block_on(sample(&ctx, &format!("{data_path}/date_dim"), None))
            .unwrap();
        let store_sales = rt
            .block_on(sample(&ctx, &format!("{data_path}/store_sales"), Some(1)))
            .unwrap();
        let join_on = vec![(
            Arc::new(Column::new("d_date_sk", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("ss_sold_date_sk", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let hash_join = HashJoinExec::try_new(
            date_dim,
            store_sales,
            join_on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            false,
        )
        .unwrap();

        let task_ctx = Arc::new(TaskContext::default());
        b.iter(|| {
            let stream = hash_join.execute(0, task_ctx.clone()).unwrap();
            criterion::black_box(consume_all_batches(stream))
        })
    });
    group.finish();
}

/// create a MemoryExec with a sample of data from the specified Parquet file
pub async fn sample(
    ctx: &SessionContext,
    path: &str,
    max_batches: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let batches = read_batches(ctx, path, max_batches).await?;
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    debug!(
        "Loaded {} batches containing {} rows from {path}",
        batches.len(),
        row_count
    );
    create_mem_exec(batches)
}

/// Read a single batch from a Parquet file
pub async fn read_batches(
    ctx: &SessionContext,
    path: &str,
    max_batches: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    let df = ctx
        .read_parquet(path, ParquetReadOptions::default())
        .await?;
    let mut batches = df.collect().await?;
    if let Some(limit) = max_batches {
        batches.truncate(limit);
    }
    Ok(batches)
}

/// Create a MemoryExec from a single batch
pub fn create_mem_exec(batches: Vec<RecordBatch>) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = batches[0].schema().clone();
    Ok(Arc::new(MemoryExec::try_new(&[batches], schema, None)?))
}

pub async fn consume_all_batches(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    let mut stream = stream;
    while let Some(batch_result) = stream.next().await {
        match batch_result {
            Ok(batch) => batches.push(batch),
            Err(e) => return Err(e),
        }
    }
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    debug!(
        "Consumed {} batches containing {} rows",
        batches.len(),
        row_count
    );
    Ok(batches)
}

criterion_group!(benches, criterion_benchmark_join);
criterion_main!(benches);
