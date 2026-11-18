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
async fn integer_extrema_preserve_empty_null_and_duplicate_results() {
    let session = session(100);
    for values in [
        vec![],
        vec![None; 4],
        vec![None, Some(-8), Some(4), Some(-8), None],
    ] {
        for is_max in [false, true] {
            let expected = if is_max {
                values.iter().flatten().max()
            } else {
                values.iter().flatten().min()
            }
            .copied();
            let (_file, scan) = parquet_input(values.clone(), &DataType::Int32, &session, 2);
            let filtered = Arc::new(wrapper(&aggregate(scan, is_max)));
            let actual = collect(Arc::clone(&filtered) as _, session.task_ctx())
                .await
                .unwrap();
            assert_eq!(result(&actual), expected, "max={is_max}");
            assert_eq!(
                metric(filtered.as_ref(), "dynamic_filter_minmax_filters_attached"),
                1
            );
        }
    }
}
