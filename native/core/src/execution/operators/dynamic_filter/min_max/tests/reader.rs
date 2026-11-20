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
async fn oversized_files_skip_runtime_pruning_when_every_row_group_improves_min() {
    use super::super::row_group_limit::MAX_ROW_GROUPS_PER_FILE;
    use std::time::Instant;

    for groups in [MAX_ROW_GROUPS_PER_FILE, MAX_ROW_GROUPS_PER_FILE + 1, 1024] {
        let session = session(1);
        let values: Vec<_> = (1..=groups as i32).rev().map(Some).collect();
        let (_file, scan) = parquet_input(values.clone(), &DataType::Int32, &session, 1);
        let filtered = Arc::new(wrapper(&aggregate(Arc::clone(&scan) as _, false)));
        let start = Instant::now();
        let actual = collect(Arc::clone(&filtered) as _, session.task_ctx())
            .await
            .unwrap();
        let filtered_time = start.elapsed();

        let (_baseline_file, baseline_scan) = parquet_input(values, &DataType::Int32, &session, 1);
        let start = Instant::now();
        let baseline = collect(
            Arc::new(aggregate(Arc::clone(&baseline_scan) as _, false)),
            session.task_ctx(),
        )
        .await
        .unwrap();
        eprintln!(
            "row groups={groups}: wrapper={filtered_time:?}, ordinary={:?}",
            start.elapsed()
        );

        assert_eq!(result(&actual), Some(1));
        assert_eq!(result(&baseline), Some(1));
        for scan in [scan, baseline_scan] {
            assert_eq!(metric(scan.as_ref(), "output_rows"), groups);
            assert_eq!(metric(scan.as_ref(), "row_groups_pruned_dynamic_filter"), 0);
        }
        let attached = usize::from(groups <= MAX_ROW_GROUPS_PER_FILE);
        assert_eq!(
            metric(filtered.as_ref(), "dynamic_filter_minmax_filters_attached"),
            attached
        );
        assert_eq!(
            metric(filtered.as_ref(), "dynamic_filter_minmax_filters_skipped"),
            1 - attached
        );
    }
}

#[tokio::test]
async fn live_bounds_prune_remapped_row_groups_and_keep_scan_metrics_on_the_scan() {
    let session = session(100);
    for is_max in [false, true] {
        for favorable in [false, true] {
            // MIN improves 200 -> 100 -> 0; MAX uses the negated layout.
            let mut starts = if favorable {
                vec![200, 100, 150, 0, 50, 300]
            } else {
                vec![500, 400, 300, 200, 100, 0]
            };
            let sign = if is_max { -1 } else { 1 };
            let values: Vec<_> = starts
                .drain(..)
                .flat_map(|start| start..start + 100)
                .map(|value| Some(sign * value))
                .collect();
            let (_file, scan) = parquet_input(values.clone(), &DataType::Int32, &session, 100);
            let filtered = Arc::new(wrapper(&aggregate(Arc::clone(&scan) as _, is_max)));
            let actual = collect(Arc::clone(&filtered) as _, session.task_ctx())
                .await
                .unwrap();
            let (_baseline_file, baseline_scan) =
                parquet_input(values, &DataType::Int32, &session, 100);
            let baseline = collect(
                Arc::new(aggregate(Arc::clone(&baseline_scan) as _, is_max)),
                session.task_ctx(),
            )
            .await
            .unwrap();
            assert_eq!(result(&actual), result(&baseline));
            assert_eq!(metric(baseline_scan.as_ref(), "output_rows"), 600);
            assert_eq!(
                metric(baseline_scan.as_ref(), "row_groups_pruned_dynamic_filter"),
                0
            );
            assert_eq!(metric(filtered.as_ref(), "output_rows"), 1);
            assert_eq!(
                metric(filtered.as_ref(), "dynamic_filter_minmax_filters_attached"),
                1
            );
            assert!(filtered
                .metrics()
                .unwrap()
                .sum_by_name("bytes_scanned")
                .is_none());
            if favorable {
                assert!(metric(scan.as_ref(), "row_groups_pruned_dynamic_filter") >= 3);
                assert!(metric(scan.as_ref(), "output_rows") <= 300);
                assert!(
                    metric(scan.as_ref(), "bytes_scanned")
                        < metric(baseline_scan.as_ref(), "bytes_scanned")
                );
            } else {
                assert_eq!(metric(scan.as_ref(), "row_groups_pruned_dynamic_filter"), 0);
                assert_eq!(metric(scan.as_ref(), "output_rows"), 600);
            }
        }
    }
}

#[tokio::test]
async fn unavailable_reader_attachment_keeps_the_ordinary_aggregate() {
    let session = session(100);
    let (_file, scan) = parquet_input(vec![Some(-3), Some(5)], &DataType::Int32, &session, 1);
    let (config, _, _) = concrete_parquet_source(&scan).unwrap();
    let mut config = config.clone();
    config.file_groups = config
        .file_groups
        .iter()
        .map(|group| {
            group
                .files()
                .iter()
                .cloned()
                .map(|file| file.with_statistics(Arc::new(Statistics::new_unknown(&scan.schema()))))
                .collect::<Vec<_>>()
                .into()
        })
        .collect();
    let scan = Arc::new(scan.as_ref().clone().with_data_source(Arc::new(config)));
    let plan = Arc::new(wrapper(&aggregate(scan, false)));
    let rows = collect(Arc::clone(&plan) as _, session.task_ctx())
        .await
        .unwrap();
    assert_eq!(result(&rows), Some(-3));
    assert_eq!(
        metric(plan.as_ref(), "dynamic_filter_minmax_filters_skipped"),
        1
    );
    assert_eq!(
        metric(plan.as_ref(), "dynamic_filter_minmax_filters_attached"),
        0
    );
}

#[tokio::test]
async fn missing_null_counts_do_not_hide_later_extrema() {
    let session = session(100);
    for is_max in [false, true] {
        let sign = if is_max { -1 } else { 1 };
        let (file, _) = parquet_input(
            vec![None, Some(10 * sign), None, Some(-10 * sign)],
            &DataType::Int32,
            &session,
            2,
        );
        omit_null_counts(&file, 1, &[0, 1]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Int32, false),
            Field::new("key", DataType::Int32, true),
        ]));
        let scan = parquet_scan(&[&file], schema, vec![1], None, false, &session);
        let plain = aggregate(scan, is_max);
        let filtered = Arc::new(wrapper(&plain));
        assert_eq!(
            result(&collect(filtered, session.task_ctx()).await.unwrap()),
            Some(-10 * sign)
        );
        assert_eq!(
            result(&collect(Arc::new(plain), session.task_ctx()).await.unwrap()),
            Some(-10 * sign)
        );
    }
}
