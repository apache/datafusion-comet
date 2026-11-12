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
async fn reader_pruning_preserves_required_field_validation() {
    let session = session(2);
    for is_max in [false, true] {
        let sign = if is_max { -1 } else { 1 };
        let (file, _) = parquet_input(
            vec![Some(0), Some(sign), Some(10 * sign), None],
            &DataType::Int32,
            &session,
            2,
        );
        for nullable in [false, true] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("payload", DataType::Int32, false),
                Field::new("key", DataType::Int32, nullable),
            ]));
            for filtering in [false, true] {
                let scan = parquet_scan(
                    &[&file],
                    Arc::clone(&schema),
                    vec![1],
                    None,
                    false,
                    &session,
                );
                let plain = aggregate(Arc::clone(&scan) as _, is_max);
                let plan: Arc<dyn ExecutionPlan> = if filtering {
                    Arc::new(wrapper(&plain))
                } else {
                    Arc::new(plain)
                };
                let actual = collect(plan, session.task_ctx()).await;
                if nullable {
                    assert_eq!(result(&actual.unwrap()), Some(0));
                    assert_eq!(
                        metric(scan.as_ref(), "row_groups_pruned_dynamic_filter"),
                        usize::from(filtering)
                    );
                } else {
                    let error = actual.unwrap_err();
                    assert!(
                        error.to_string().contains(
                            "Column 'key' is declared as non-nullable but contains null values"
                        ),
                        "max={is_max}, filtering={filtering}: {error}"
                    );
                    assert_eq!(metric(scan.as_ref(), "row_groups_pruned_dynamic_filter"), 0);
                }
            }
        }
    }
}
