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

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::{NullEquality, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::JoinType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use super::CometSortMergeJoinExec;

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("l_key", DataType::Int32, true),
        Field::new("l_val", DataType::Utf8, true),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("r_key", DataType::Int32, true),
        Field::new("r_val", DataType::Utf8, true),
    ]))
}

fn make_sorted_batches(
    schema: SchemaRef,
    keys: Vec<Option<i32>>,
    vals: Vec<Option<&str>>,
) -> Vec<RecordBatch> {
    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(StringArray::from(vals)),
        ],
    )
    .unwrap()]
}

async fn execute_join(
    join_type: JoinType,
    left_batches: Vec<RecordBatch>,
    right_batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let l_schema = left_batches[0].schema();
    let r_schema = right_batches[0].schema();

    let left = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
        &[left_batches],
        l_schema,
        None,
    )?)));
    let right = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
        &[right_batches],
        r_schema,
        None,
    )?)));

    let on = vec![(
        Arc::new(datafusion::physical_expr::expressions::Column::new(
            "l_key", 0,
        )) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        Arc::new(datafusion::physical_expr::expressions::Column::new(
            "r_key", 0,
        )) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    )];

    let join = CometSortMergeJoinExec::try_new(
        left,
        right,
        on,
        None,
        join_type,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
    )?;

    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let stream = join.execute(0, task_ctx)?;

    let mut results = Vec::new();
    let mut stream = stream;
    while let Some(batch) = stream.next().await {
        results.push(batch?);
    }
    Ok(results)
}

fn total_row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn test_inner_join_basic() -> Result<()> {
    let left = make_sorted_batches(
        left_schema(),
        vec![Some(1), Some(2), Some(3)],
        vec![Some("a"), Some("b"), Some("c")],
    );
    let right = make_sorted_batches(
        right_schema(),
        vec![Some(2), Some(3), Some(4)],
        vec![Some("x"), Some("y"), Some("z")],
    );

    let result = execute_join(JoinType::Inner, left, right).await?;
    assert_eq!(total_row_count(&result), 2);
    Ok(())
}

#[tokio::test]
async fn test_inner_join_with_duplicates() -> Result<()> {
    let left = make_sorted_batches(
        left_schema(),
        vec![Some(1), Some(1), Some(2)],
        vec![Some("a"), Some("b"), Some("c")],
    );
    let right = make_sorted_batches(
        right_schema(),
        vec![Some(1), Some(1), Some(3)],
        vec![Some("x"), Some("y"), Some("z")],
    );

    let result = execute_join(JoinType::Inner, left, right).await?;
    assert_eq!(total_row_count(&result), 4);
    Ok(())
}

#[tokio::test]
async fn test_inner_join_null_keys_skipped() -> Result<()> {
    let left = make_sorted_batches(
        left_schema(),
        vec![None, Some(1), Some(2)],
        vec![Some("a"), Some("b"), Some("c")],
    );
    let right = make_sorted_batches(
        right_schema(),
        vec![None, Some(1), Some(2)],
        vec![Some("x"), Some("y"), Some("z")],
    );

    let result = execute_join(JoinType::Inner, left, right).await?;
    assert_eq!(total_row_count(&result), 2);
    Ok(())
}

#[tokio::test]
async fn test_inner_join_empty_result() -> Result<()> {
    let left = make_sorted_batches(
        left_schema(),
        vec![Some(1), Some(2)],
        vec![Some("a"), Some("b")],
    );
    let right = make_sorted_batches(
        right_schema(),
        vec![Some(3), Some(4)],
        vec![Some("x"), Some("y")],
    );

    let result = execute_join(JoinType::Inner, left, right).await?;
    assert_eq!(total_row_count(&result), 0);
    Ok(())
}

// --- Outer join tests ---

#[tokio::test]
async fn test_left_outer_join() -> Result<()> {
    let left = make_sorted_batches(
        left_schema(),
        vec![Some(1), Some(2), Some(3)],
        vec![Some("a"), Some("b"), Some("c")],
    );
    let right = make_sorted_batches(
        right_schema(),
        vec![Some(2), Some(4)],
        vec![Some("x"), Some("y")],
    );

    let result = execute_join(JoinType::Left, left, right).await?;
    assert_eq!(total_row_count(&result), 3);
    Ok(())
}

#[tokio::test]
async fn test_left_outer_null_keys() -> Result<()> {
    let left = make_sorted_batches(
        left_schema(),
        vec![None, Some(1)],
        vec![Some("a"), Some("b")],
    );
    let right = make_sorted_batches(
        right_schema(),
        vec![Some(1), Some(2)],
        vec![Some("x"), Some("y")],
    );

    let result = execute_join(JoinType::Left, left, right).await?;
    assert_eq!(total_row_count(&result), 2);
    Ok(())
}

#[tokio::test]
async fn test_right_outer_join() -> Result<()> {
    let left = make_sorted_batches(
        left_schema(),
        vec![Some(1), Some(3)],
        vec![Some("a"), Some("b")],
    );
    let right = make_sorted_batches(
        right_schema(),
        vec![Some(1), Some(2), Some(3)],
        vec![Some("x"), Some("y"), Some("z")],
    );

    let result = execute_join(JoinType::Right, left, right).await?;
    assert_eq!(total_row_count(&result), 3);
    Ok(())
}

#[tokio::test]
async fn test_full_outer_join() -> Result<()> {
    let left = make_sorted_batches(
        left_schema(),
        vec![Some(1), Some(2)],
        vec![Some("a"), Some("b")],
    );
    let right = make_sorted_batches(
        right_schema(),
        vec![Some(2), Some(3)],
        vec![Some("x"), Some("y")],
    );

    let result = execute_join(JoinType::Full, left, right).await?;
    assert_eq!(total_row_count(&result), 3);
    Ok(())
}

// --- Semi/Anti join tests ---

#[tokio::test]
async fn test_left_semi_join() -> Result<()> {
    let left = make_sorted_batches(
        left_schema(),
        vec![Some(1), Some(2), Some(3)],
        vec![Some("a"), Some("b"), Some("c")],
    );
    let right = make_sorted_batches(
        right_schema(),
        vec![Some(2), Some(3), Some(4)],
        vec![Some("x"), Some("y"), Some("z")],
    );

    let result = execute_join(JoinType::LeftSemi, left, right).await?;
    assert_eq!(total_row_count(&result), 2);
    // Semi join should only output left columns
    assert_eq!(result[0].num_columns(), 2);
    Ok(())
}

#[tokio::test]
async fn test_left_anti_join() -> Result<()> {
    let left = make_sorted_batches(
        left_schema(),
        vec![Some(1), Some(2), Some(3)],
        vec![Some("a"), Some("b"), Some("c")],
    );
    let right = make_sorted_batches(
        right_schema(),
        vec![Some(2), Some(4)],
        vec![Some("x"), Some("y")],
    );

    let result = execute_join(JoinType::LeftAnti, left, right).await?;
    assert_eq!(total_row_count(&result), 2);
    Ok(())
}

// --- Spill test ---

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_inner_join_with_spill() -> Result<()> {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    let l_schema = left_schema();
    let r_schema = right_schema();

    let left_batches = make_sorted_batches(
        Arc::clone(&l_schema),
        vec![Some(1), Some(1), Some(1), Some(2), Some(2)],
        vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e")],
    );
    let right_batches = make_sorted_batches(
        Arc::clone(&r_schema),
        vec![Some(1), Some(1), Some(1), Some(2)],
        vec![Some("w"), Some("x"), Some("y"), Some("z")],
    );

    let left_exec = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
        &[left_batches],
        l_schema,
        None,
    )?)));
    let right_exec = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
        &[right_batches],
        r_schema,
        None,
    )?)));

    let on = vec![(
        Arc::new(datafusion::physical_expr::expressions::Column::new(
            "l_key", 0,
        )) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        Arc::new(datafusion::physical_expr::expressions::Column::new(
            "r_key", 0,
        )) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    )];

    let join = CometSortMergeJoinExec::try_new(
        left_exec,
        right_exec,
        on,
        None,
        JoinType::Inner,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
    )?;

    let config = datafusion::prelude::SessionConfig::new().with_batch_size(2);
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_limit(1024, 1.0)
            .build()?,
    );
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    let task_ctx = ctx.task_ctx();
    let mut stream = join.execute(0, task_ctx)?;

    let mut results = Vec::new();
    while let Some(batch) = stream.next().await {
        results.push(batch?);
    }
    // 3*3 for key=1 + 2*1 for key=2 = 11
    assert_eq!(total_row_count(&results), 11);
    Ok(())
}
