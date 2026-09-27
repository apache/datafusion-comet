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
async fn empty_and_all_null_inputs() {
    let session = session(4);
    for values in [vec![], vec![None; 6]] {
        let (_file, scan) = parquet_input(values.clone(), &DataType::Int32, &session, 4);
        let plan = wrapper(&sort(scan, 3, SortOptions::default()), &session);
        assert!(plan.build_runtime_sort().unwrap().reader_filter_attached);
        let actual = collect(Arc::new(plan), session.task_ctx()).await.unwrap();
        assert_eq!(
            keys(&actual),
            values.into_iter().take(3).collect::<Vec<_>>()
        );
    }
}
