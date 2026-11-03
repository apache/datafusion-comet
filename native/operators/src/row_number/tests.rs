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

use arrow::array::{
    Array, DictionaryArray, Int64Array, Int8Array, ListArray, StringArray, StringViewArray,
    StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::windows::create_window_expr;
use datafusion::physical_plan::{collect, ExecutionPlanProperties, InputOrderMode};
use datafusion::prelude::SessionContext;

fn batch(keys: ArrayRef) -> RecordBatch {
    let rows = keys.len();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", keys.data_type().clone(), true),
            Field::new("i", DataType::Int64, false),
        ])),
        vec![keys, Arc::new(Int64Array::from_iter_values(0..rows as i64))],
    )
    .unwrap()
}

fn window(batches: &[RecordBatch], partitioned: bool, functions: &[&str]) -> BoundedWindowAggExec {
    let schema = batches[0].schema();
    let sort = |name| PhysicalSortExpr::new(col(name, &schema).unwrap(), SortOptions::default());
    let ordering = if partitioned {
        vec![sort("k"), sort("i")]
    } else {
        vec![sort("i")]
    };
    let source = MemorySourceConfig::try_new(&[batches.to_vec()], Arc::clone(&schema), None)
        .unwrap()
        .try_with_sort_information(vec![LexOrdering::new(ordering).unwrap()])
        .unwrap();
    let input = DataSourceExec::from_data_source(source);
    let ctx = SessionContext::new();
    let keys = if partitioned {
        vec![col("k", &schema).unwrap()]
    } else {
        vec![]
    };
    let frame = Arc::new(WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
        WindowFrameBound::CurrentRow,
    ));
    let expressions = functions
        .iter()
        .enumerate()
        .map(|(index, function)| {
            create_window_expr(
                &WindowFunctionDefinition::WindowUDF(ctx.udwf(function).unwrap()),
                format!("result_{index}"),
                &[],
                &keys,
                &[sort("i")],
                Arc::clone(&frame),
                Arc::clone(&schema),
                false,
                false,
                None,
            )
            .unwrap()
        })
        .collect();
    BoundedWindowAggExec::try_new(expressions, input, InputOrderMode::Sorted, partitioned).unwrap()
}

async fn compare(
    batches: &[RecordBatch],
    partitioned: bool,
    functions: &[&str],
) -> Vec<RecordBatch> {
    let generic = window(batches, partitioned, functions);
    let streaming = Arc::new(RowNumberExec::try_new(generic.clone()).unwrap());
    let ctx = SessionContext::new();
    let expected = collect(Arc::new(generic), ctx.task_ctx()).await.unwrap();
    let actual = collect(Arc::clone(&streaming) as _, ctx.task_ctx())
        .await
        .unwrap();
    assert_eq!(
        concat_batches(&streaming.schema(), &actual).unwrap(),
        concat_batches(&streaming.schema(), &expected).unwrap()
    );
    assert_eq!(
        actual.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>()
    );
    actual
}

#[tokio::test]
async fn partitions_continue_across_slices_and_empty_batches() {
    let input = batch(Arc::new(Int64Array::from(vec![
        None,
        None,
        Some(1),
        Some(1),
        Some(1),
        Some(2),
        Some(2),
        Some(3),
    ])));
    let batches = vec![
        input.slice(0, 0),
        input.slice(0, 3),
        input.slice(3, 0),
        input.slice(3, 1),
        input.slice(4, 4),
        input.slice(8, 0),
    ];
    let output = compare(&batches, true, &["row_number"]).await;
    let ranks = output
        .iter()
        .flat_map(|batch| {
            batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(ranks, vec![1, 2, 1, 2, 3, 1, 2, 1]);
}

#[tokio::test]
async fn global_windows_share_alias_output_and_input_columns() {
    let input = batch(Arc::new(Int64Array::from_iter_values(0..23)));
    let batches = vec![input.slice(0, 5), input.slice(5, 7), input.slice(12, 11)];
    let actual = compare(&batches, false, &["row_number", "row_number"]).await;
    for (output, input) in actual.iter().zip(&batches) {
        assert!(Arc::ptr_eq(output.column(0), input.column(0)));
        assert!(Arc::ptr_eq(output.column(2), output.column(3)));
        assert_eq!(output.column(2).null_count(), 0);
    }
    let last = actual[2]
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(last.values().as_ref(), &(13..=23).collect::<Vec<u64>>());
}

#[tokio::test]
async fn empty_input_preserves_schema() {
    compare(
        &[batch(Arc::new(Int64Array::from(Vec::<i64>::new())))],
        true,
        &["row_number"],
    )
    .await;
}

#[tokio::test]
async fn dictionary_keys_compare_values_across_different_codebooks() {
    use arrow::datatypes::Int8Type;
    let first = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![None, Some(0), Some(1), Some(1)]),
            Arc::new(StringArray::from(vec![None, Some("a")])),
        )
        .unwrap(),
    );
    let second = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(1), Some(0), Some(0)]),
            Arc::new(StringArray::from(vec![Some("b"), Some("a")])),
        )
        .unwrap(),
    );
    let output = compare(&[batch(first), batch(second)], true, &["row_number"]).await;
    let ranks = output[1]
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(ranks.values().as_ref(), &[3, 1, 2]);
}

#[tokio::test]
async fn nested_keys_and_nullable_children_match_generic_windows() {
    let keys = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        None,
        Some(vec![]),
        Some(vec![None]),
        Some(vec![None]),
        Some(vec![Some(2)]),
        Some(vec![Some(2)]),
    ]));
    let input = batch(keys);
    compare(
        &[input.slice(0, 3), input.slice(3, 3)],
        true,
        &["row_number"],
    )
    .await;
    let keys = Arc::new(StructArray::new(
        vec![Field::new("value", DataType::Int64, true)].into(),
        vec![Arc::new(Int64Array::from(vec![
            None,
            None,
            Some(1),
            Some(1),
        ]))],
        Some(NullBuffer::from(vec![false, false, true, true])),
    ));
    let input = batch(keys);
    compare(
        &[input.slice(0, 1), input.slice(1, 2), input.slice(3, 1)],
        true,
        &["row_number"],
    )
    .await;
}

#[test]
fn other_functions_cannot_enter_the_specialised_executor() {
    let input = batch(Arc::new(Int64Array::from(vec![1, 1, 2])));
    let mixed = window(std::slice::from_ref(&input), true, &["row_number", "rank"]);
    assert!(!RowNumberExec::supports(&mixed));
    assert!(RowNumberExec::try_new(mixed).is_err());
    assert!(!RowNumberExec::supports(&window(&[input], true, &["rank"])));
}

#[test]
fn plan_rewrites_keep_the_window_contracts() {
    let input = batch(Arc::new(Int64Array::from(vec![1, 1, 2])));
    let generic = window(&[input], true, &["row_number"]);
    let expected_order = generic.required_input_ordering();
    let expected_partitions = generic.properties().output_partitioning().partition_count();
    let plan = Arc::new(RowNumberExec::try_new(generic.clone()).unwrap());
    let child = Arc::clone(generic.input());
    for replacement in [
        Arc::clone(&plan)
            .replace_children(
                vec![Arc::clone(&child)],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap(),
        Arc::clone(&plan)
            .replace_children(
                vec![Arc::clone(&child)],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
            )
            .unwrap(),
    ] {
        assert_eq!(replacement.schema(), generic.schema());
        assert_eq!(replacement.required_input_ordering(), expected_order);
        assert!(matches!(
            replacement.input_distribution_requirements().into_per_child().as_slice(),
            [Distribution::KeyPartitioned(keys)] if keys == &generic.partition_keys()
        ));
        assert_eq!(
            replacement.output_partitioning().partition_count(),
            expected_partitions
        );
        assert!(matches!(
            replacement.cardinality_effect(),
            CardinalityEffect::Equal
        ));
        assert_eq!(replacement.maintains_input_order(), vec![true]);
    }
}

#[test]
fn retained_view_buffers_are_charged_and_a_continuing_key_is_reused() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let reservation = MemoryConsumer::new("test boundary").register(&pool);
    let mut state = RowNumberState {
        key: vec![],
        rank: 0,
        reservation,
    };
    let keys: ArrayRef = Arc::new(StringViewArray::from(vec!["x".repeat(16 * 1024); 3]));
    let input = batch(keys);
    let plan = window(std::slice::from_ref(&input), true, &["row_number"]);
    let expressions = plan.window_expr()[0].partition_by();
    state
        .evaluate(input.clone(), expressions, plan.schema(), 1)
        .unwrap();
    let retained = pool.reserved();
    assert!(retained >= 16 * 1024);
    let key = Arc::clone(&state.key[0]);
    state
        .evaluate(input, expressions, plan.schema(), 1)
        .unwrap();
    assert!(Arc::ptr_eq(&key, &state.key[0]));
    assert_eq!(pool.reserved(), retained);
    drop(key);
    drop(state);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn refused_boundary_reservations_are_released() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
    let reservation = MemoryConsumer::new("test refusal").register(&pool);
    let mut state = RowNumberState {
        key: vec![],
        rank: 0,
        reservation,
    };
    let input = batch(Arc::new(StringArray::from(vec!["some partition key"])));
    let plan = window(std::slice::from_ref(&input), true, &["row_number"]);
    let result = state.evaluate(
        input,
        plan.window_expr()[0].partition_by(),
        plan.schema(),
        1,
    );
    assert!(result.is_err());
    assert!(state.key.is_empty());
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn completion_and_cancellation_release_the_boundary_key() {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    let input = batch(Arc::new(Int64Array::from(vec![1, 1, 1, 2])));
    let plan = RowNumberExec::try_new(window(
        &[input.slice(0, 2), input.slice(2, 2)],
        true,
        &["row_number"],
    ))
    .unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .unwrap();
    let ctx = SessionContext::new_with_config_rt(Default::default(), runtime);
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 2);
    assert!(pool.reserved() > 0);
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    while let Some(batch) = stream.next().await {
        batch.unwrap();
    }
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn a_dictionary_boundary_does_not_retain_unused_values() {
    use arrow::datatypes::Int8Type;
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        "small",
        &"unused".repeat(64 * 1024),
    ]));
    let keys: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0, 0]), values).unwrap(),
    );
    let input = batch(keys);
    let plan = window(std::slice::from_ref(&input), true, &["row_number"]);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
    let mut state = RowNumberState {
        key: vec![],
        rank: 0,
        reservation: MemoryConsumer::new("dictionary boundary").register(&pool),
    };
    state
        .evaluate(
            input,
            plan.window_expr()[0].partition_by(),
            plan.schema(),
            1,
        )
        .unwrap();
    assert!(pool.reserved() < 4096);
    drop(state);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn a_view_boundary_does_not_retain_an_unrelated_large_buffer() {
    let keys: ArrayRef = Arc::new(StringViewArray::from(vec![
        "a".repeat(1024 * 1024),
        "z".repeat(13),
    ]));
    let input = batch(keys);
    let plan = window(std::slice::from_ref(&input), true, &["row_number"]);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
    let mut state = RowNumberState {
        key: vec![],
        rank: 0,
        reservation: MemoryConsumer::new("view boundary").register(&pool),
    };
    state
        .evaluate(
            input,
            plan.window_expr()[0].partition_by(),
            plan.schema(),
            1,
        )
        .unwrap();
    assert!(pool.reserved() < 64 * 1024);
    drop(state);
    assert_eq!(pool.reserved(), 0);
}
