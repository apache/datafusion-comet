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
use futures::StreamExt;

#[tokio::test]
async fn each_execution_resets_both_the_predicate_and_accumulator_bounds() {
    let session = session(100);
    let (_file, scan) = parquet_input(
        vec![Some(4), Some(3), Some(2), Some(1)],
        &DataType::Int32,
        &session,
        2,
    );
    let plan = Arc::new(wrapper(&aggregate(scan, false)));
    let first = plan.build_runtime_aggregate().unwrap();
    let second = plan.build_runtime_aggregate().unwrap();
    let first_filter = produced_filter(first.aggregate.as_ref());
    let second_filter = produced_filter(second.aggregate.as_ref());
    assert_ne!(first_filter.expression_id(), second_filter.expression_id());
    for (runtime, predicate) in [(first, first_filter), (second, second_filter)] {
        assert_eq!(predicate.snapshot_generation(), 1);
        let rows = collect(runtime.aggregate, session.task_ctx())
            .await
            .unwrap();
        assert_eq!(result(&rows), Some(1));
        // A fresh predicate with stale shared bounds would never publish this bound.
        assert!(predicate.snapshot_generation() > 1);
    }
    for execution in [
        Arc::clone(&plan) as Arc<dyn ExecutionPlan>,
        Arc::clone(&plan) as _,
        Arc::clone(&plan).reset_state().unwrap(),
    ] {
        assert_eq!(
            result(&collect(execution, session.task_ctx()).await.unwrap()),
            Some(1)
        );
    }
    let (_replacement_file, replacement) = parquet_input(
        vec![Some(104), Some(103), Some(102), Some(101)],
        &DataType::Int32,
        &session,
        2,
    );
    let replaced = plan
        .replace_children(
            vec![replacement],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .unwrap();
    assert_eq!(
        result(&collect(replaced, session.task_ctx()).await.unwrap()),
        Some(101)
    );
}

#[tokio::test]
async fn completed_or_cancelled_streams_release_the_runtime_predicate() {
    let session = session(100);
    let (_file, scan) = parquet_input(
        vec![Some(4), Some(3), Some(2), Some(1)],
        &DataType::Int32,
        &session,
        2,
    );
    let plan = wrapper(&aggregate(scan, false));
    for complete in [false, true] {
        let runtime = plan.build_runtime_aggregate().unwrap();
        let weak = Arc::downgrade(&produced_filter(runtime.aggregate.as_ref()));
        let mut stream = plan
            .execute_runtime_aggregate(runtime.aggregate, 0, session.task_ctx())
            .unwrap();
        if complete {
            while let Some(batch) = stream.next().await {
                batch.unwrap();
            }
            assert!(
                weak.upgrade().is_none(),
                "retaining a spent stream must not retain its bound"
            );
        }
        drop(stream);
        assert!(weak.upgrade().is_none());
    }
}
