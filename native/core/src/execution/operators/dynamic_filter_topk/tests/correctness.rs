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

#[tokio::test]
async fn signed_integer_ordering_nulls_and_ties_match_unfiltered_topk() {
    let session = session(4);
    for key_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                for values in [
                    vec![],
                    vec![None],
                    vec![Some(3), None],
                    vec![None, None, None, None, None, None],
                    vec![
                        Some(3),
                        None,
                        Some(-2),
                        Some(3),
                        Some(0),
                        None,
                        Some(-2),
                        Some(1),
                        Some(1),
                        Some(-1),
                        Some(0),
                        None,
                    ],
                ] {
                    let options = SortOptions {
                        descending,
                        nulls_first,
                    };
                    let (_file, scan) = parquet_input(values, &key_type, &session, 4);
                    let plain = sort(scan, 3, options);
                    let filtered = wrapper(&plain, &session);
                    assert!(
                        filtered
                            .build_runtime_sort()
                            .unwrap()
                            .reader_filter_attached
                    );
                    let expected = collect(Arc::new(plain), session.task_ctx()).await.unwrap();
                    let actual = collect(Arc::new(filtered), session.task_ctx())
                        .await
                        .unwrap();
                    assert_eq!(keys(&actual), keys(&expected), "{key_type:?}, {options:?}");
                }
            }
        }
    }
}

#[tokio::test]
async fn unsupported_input_still_executes_topk() {
    use datafusion::physical_plan::projection::ProjectionExec;
    let session = session(2);
    let input = memory_input(vec![Some(4), None, Some(1), Some(2)], &DataType::Int32);
    let projection = ProjectionExec::try_new(
        vec![(
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
            "key".into(),
        )],
        input,
    )
    .unwrap();
    let plan = wrapper(
        &sort(Arc::new(projection), 2, SortOptions::default()),
        &session,
    );
    assert!(!plan.build_runtime_sort().unwrap().reader_filter_attached);
    assert_eq!(
        keys(&collect(Arc::new(plan), session.task_ctx()).await.unwrap()),
        vec![None, Some(1)]
    );
}

/// Count residual work independently of reader pruning and heap output. This
/// memory input cannot attach a reader, but still consumes live TopK thresholds.
#[tokio::test]
async fn residual_metrics_follow_live_thresholds_and_reset_with_the_plan() {
    let session = session(2);
    // [4, 5] bypasses; [1, 6] rejects 6; [2, 3] rejects both after threshold=1.
    let input = memory_input(
        vec![Some(4), Some(5), Some(1), Some(6), Some(2), Some(3)],
        &DataType::Int32,
    );
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(wrapper(&sort(input, 1, SortOptions::default()), &session));
    for execution in 1..=2 {
        let batches = collect(Arc::clone(&plan), session.task_ctx())
            .await
            .unwrap();
        assert_eq!(keys(&batches), vec![Some(1)]);
        for (name, per_execution) in [
            ("dynamic_filter_topk_rows_bypassed", 2),
            ("dynamic_filter_topk_rows_evaluated", 4),
            ("dynamic_filter_topk_rows_pruned", 3),
            ("dynamic_filter_topk_filters_skipped", 1),
        ] {
            assert_eq!(count_metric(plan.as_ref(), name), execution * per_execution);
        }
        assert!(count_metric(plan.as_ref(), "dynamic_filter_topk_eval_time") > 0);
        assert_eq!(plan.metrics().unwrap().output_rows(), Some(execution));
    }
    let reset = Arc::clone(&plan).reset_state().unwrap();
    assert_eq!(
        count_metric(reset.as_ref(), "dynamic_filter_topk_rows_evaluated"),
        0
    );
    assert_eq!(
        keys(
            &collect(Arc::clone(&reset), session.task_ctx())
                .await
                .unwrap()
        ),
        vec![Some(1)]
    );
    assert_eq!(
        count_metric(reset.as_ref(), "dynamic_filter_topk_rows_evaluated"),
        4
    );
    assert_eq!(
        count_metric(reset.as_ref(), "dynamic_filter_topk_rows_pruned"),
        3
    );
    assert_eq!(
        count_metric(plan.as_ref(), "dynamic_filter_topk_rows_evaluated"),
        8
    );
    assert_eq!(reset.metrics().unwrap().output_rows(), Some(1));
}
