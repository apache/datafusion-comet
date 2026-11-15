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

use arrow::array::{AsArray, BinaryArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use datafusion::common::{Result, ScalarValue};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::{common::collect, ExecutionPlan};
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
async fn grouped_final_bloom_filter_merges_batched_states() -> Result<()> {
    let value_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let aggregate_udf = Arc::new(AggregateUDF::new_from_impl(BloomFilterAgg::new(
        Arc::new(Literal::new(ScalarValue::Int64(Some(100)))),
        Arc::new(Literal::new(ScalarValue::Int64(Some(1024)))),
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

    // Interleave the keys so one group merges two states and the other uses a
    // separate slice of the same binary array.
    let states = BinaryArray::from_iter_values([
        bloom_state(&aggregate_expr, &[11])?,
        bloom_state(&aggregate_expr, &[99])?,
        bloom_state(&aggregate_expr, &[22])?,
    ]);
    let batch = RecordBatch::try_new(
        Arc::clone(&state_schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 1])), Arc::new(states)],
    )?;
    let scan = MemorySourceConfig::try_new_exec(&[vec![batch]], state_schema, None)?;
    let aggregate = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::new_single(vec![(Arc::new(Column::new("key", 0)), "key".into())]),
        vec![Arc::clone(&aggregate_expr)],
        vec![None],
        scan,
        value_schema,
    )?;
    let output = collect(aggregate.execute(0, Arc::new(TaskContext::default()))?).await?;

    let mut actual = Vec::new();
    for batch in output {
        let keys = batch.column(0).as_primitive::<Int64Type>();
        let filters = batch.column(1).as_binary::<i32>();
        for (key, filter) in keys.iter().zip(filters.iter()) {
            actual.push((key.unwrap(), filter.unwrap().to_vec()));
        }
    }
    actual.sort_by_key(|(key, _)| *key);
    assert_eq!(
        actual,
        vec![
            (1, bloom_state(&aggregate_expr, &[11, 22])?),
            (2, bloom_state(&aggregate_expr, &[99])?),
        ]
    );
    Ok(())
}
