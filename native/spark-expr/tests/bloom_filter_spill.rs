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

use arrow::array::{Array, BinaryArray, BinaryBuilder, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{Result, ScalarValue};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::{common::collect, ExecutionPlan};
use datafusion::prelude::SessionConfig;
use datafusion_comet_spark_expr::{BloomFilterAgg, SparkBloomFilterVersion};

fn bloom_state(aggregate: &AggregateFunctionExpr, values: &[i64]) -> Result<Vec<u8>> {
    let mut accumulator = aggregate.create_accumulator()?;
    accumulator.update_batch(&[Arc::new(Int64Array::from_iter_values(
        values.iter().copied(),
    ))])?;
    let ScalarValue::Binary(Some(state)) = accumulator.state()?.remove(0) else {
        panic!("a nonempty Bloom filter must have a binary state");
    };
    Ok(state)
}

#[tokio::test]
async fn grouped_bloom_filter_merges_spilled_states() -> Result<()> {
    const NUM_GROUPS: usize = 1024;
    const BATCH_SIZE: usize = 8;
    const NUM_BITS: i64 = 131_072;

    let value_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let aggregate_udf = Arc::new(AggregateUDF::new_from_impl(BloomFilterAgg::new(
        Arc::new(Literal::new(ScalarValue::Int64(Some(1024)))),
        Arc::new(Literal::new(ScalarValue::Int64(Some(NUM_BITS)))),
        DataType::Binary,
        SparkBloomFilterVersion::V1,
    )));
    let aggregate_expr = Arc::new(
        AggregateExprBuilder::new(aggregate_udf, vec![Arc::new(Column::new("value", 1))])
            .schema(Arc::clone(&value_schema))
            .alias("bloom")
            .build()?,
    );
    let state_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("bits", DataType::Binary, false),
    ]));

    // Each pass has one valid, nonempty partial state per group. The complete set
    // of 16 KiB filters exceeds the pool, so the two passes meet during sorted
    // spill replay. Distinct values detect lost states as well as merge panics.
    let mut batches = Vec::new();
    for pass in 0..2 {
        for start in (0..NUM_GROUPS).step_by(BATCH_SIZE) {
            let mut states = BinaryBuilder::new();
            for key in start..start + BATCH_SIZE {
                states.append_value(bloom_state(
                    &aggregate_expr,
                    &[(pass * NUM_GROUPS + key) as i64],
                )?);
            }
            batches.push(RecordBatch::try_new(
                Arc::clone(&state_schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (start..start + BATCH_SIZE).map(|key| key as i64),
                    )),
                    Arc::new(states.finish()),
                ],
            )?);
        }
    }
    let scan = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
        &[batches],
        state_schema,
        None,
    )?)));
    let aggregate = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(vec![(Arc::new(Column::new("key", 0)), "key".into())]),
        vec![Arc::clone(&aggregate_expr)],
        vec![None],
        scan,
        value_schema,
    )?;
    let pool = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as _)
        .build_arc()?;
    let context = Arc::new(
        TaskContext::default()
            .with_runtime(runtime)
            .with_session_config(
                // Exercise the sorted-spill replay path explicitly.
                SessionConfig::new()
                    .with_batch_size(BATCH_SIZE)
                    .set_bool("datafusion.execution.enable_migration_aggregate", false),
            ),
    );
    let output = collect(aggregate.execute(0, context)?).await?;

    let metrics = aggregate.metrics().unwrap();
    assert!(
        metrics.spill_count().is_some_and(|count| count > 0),
        "the regression must exercise spill replay: {metrics:?}"
    );
    let mut seen = vec![false; NUM_GROUPS];
    for batch in output {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let filters = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(filters.null_count(), 0);
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            assert!((0..NUM_GROUPS as i64).contains(&key));
            assert!(!seen[key as usize], "duplicate output group {key}");
            seen[key as usize] = true;
            assert_eq!(
                filters.value(row),
                bloom_state(&aggregate_expr, &[key, key + NUM_GROUPS as i64])?,
                "group {key} must contain both partial states"
            );
        }
    }
    assert!(seen.into_iter().all(|present| present));
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
