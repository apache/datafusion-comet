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

use super::*;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::prelude::SessionContext;

fn input(
    values: Vec<Option<i32>>,
    key_type: &DataType,
    key_index: usize,
) -> Arc<dyn ExecutionPlan> {
    let payload = Arc::new(Int32Array::from_iter_values(0..values.len() as i32)) as ArrayRef;
    let key = cast(&Int32Array::from(values), key_type).unwrap();
    let mut fields = vec![
        Field::new("key", key_type.clone(), true),
        Field::new("payload", DataType::Int32, false),
    ];
    let mut columns = vec![key, payload];
    fields.swap(0, key_index);
    columns.swap(0, key_index);
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    // Multiple build batches prove that an early subset of keys cannot prune
    // matches belonging to a later batch.
    let batches = if batch.num_rows() == 0 {
        vec![batch]
    } else {
        (0..batch.num_rows())
            .step_by(2)
            .map(|offset| batch.slice(offset, 2.min(batch.num_rows() - offset)))
            .collect()
    };
    memory_exec(batches)
}

fn memory_exec(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(std::slice::from_ref(&batches), batches[0].schema(), None)
        .unwrap()
}

fn metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return metric(projection.input(), name);
    }
    plan.metrics()
        .unwrap()
        .sum_by_name(name)
        .unwrap()
        .as_usize()
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test]
async fn placeholder_updates_and_errors_are_not_hidden() {
    let source = input((0..10).map(Some).collect(), &DataType::Int32, 1);
    let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(Column::new("key", 1))],
        lit(true),
    ));
    let wrapper: Arc<dyn ExecutionPlan> = Arc::new(DynamicFilterExec::new(
        source,
        Arc::clone(&predicate),
        ExecutionPlanMetricsSet::new(),
        "test_filter",
    ));
    let task = SessionContext::new().task_ctx();
    let mut stream = wrapper.execute(0, Arc::clone(&task)).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 2);
    predicate
        .update(Arc::new(BinaryExpr::new(
            Arc::new(Column::new("key", 1)),
            Operator::Lt,
            lit(2i32),
        )))
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 0);
    predicate.update(lit(false)).unwrap();
    while let Some(batch) = stream.next().await {
        assert_eq!(batch.unwrap().num_rows(), 0);
    }
    assert_eq!(metric(&wrapper, "test_filter_rows_bypassed"), 2);
    assert_eq!(metric(&wrapper, "test_filter_rows_pruned"), 8);
    assert_eq!(metric(&wrapper, "test_filter_rows_evaluated"), 8);

    // Reset must not preserve an old condition, even while another owner
    // still holds the previous predicate.
    let reset = Arc::clone(&wrapper).reset_state().unwrap();
    let reset_output = collect(Arc::clone(&reset), Arc::clone(&task))
        .await
        .unwrap();
    assert_eq!(row_count(&reset_output), 10);
    assert_eq!(metric(&reset, "test_filter_rows_pruned"), 0);
    assert_eq!(metric(&reset, "test_filter_rows_bypassed"), 10);

    predicate.update(lit(42i32)).unwrap();
    let error = collect(wrapper, task).await.unwrap_err();
    assert!(error.to_string().contains("must evaluate to a Boolean"));
}
