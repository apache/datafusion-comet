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
