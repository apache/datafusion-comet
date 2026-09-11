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
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion_datasource::file::FileSource;

fn reader_filter(expression: &Arc<dyn PhysicalExpr>) -> Option<&DynamicFilterPhysicalExpr> {
    if let Some(filter) = expression.downcast_ref::<DynamicFilterPhysicalExpr>() {
        return Some(filter);
    }
    expression.children().into_iter().find_map(reader_filter)
}

fn groups() -> Vec<Option<i32>> {
    // Thresholds improve from 209 to 109 to 9. Three intervening/later groups
    // can be discarded only after the scan has started.
    [200..300, 100..200, 150..250, 0..100, 50..150, 300..400]
        .into_iter()
        .flatten()
        .map(Some)
        .collect()
}

#[tokio::test]
async fn live_threshold_reaches_remapped_parquet_column_and_prunes_later_groups() {
    let session = session(100);
    let (_file, scan) = parquet_input(groups(), &DataType::Int32, &session, 100);
    let plain = sort(Arc::clone(&scan) as _, 10, SortOptions::default());
    let plan = wrapper(&plain, &session);
    let runtime = plan.build_runtime_sort().unwrap();
    assert!(runtime.reader_filter_attached);
    let runtime_sort = Arc::new(runtime.sort);
    let producer = produced_filter(&runtime_sort);
    let reader = runtime_sort
        .input()
        .downcast_ref::<DataSourceExec>()
        .unwrap();
    let (_, source) = reader.downcast_to_file_source::<ParquetSource>().unwrap();
    let expression = source.filter().unwrap();
    let consumer = reader_filter(&expression).unwrap();
    assert_eq!(consumer.expression_id(), producer.expression_id());
    let children = consumer.children();
    let key = children[0].downcast_ref::<Column>().unwrap();
    assert_eq!((key.name(), key.index()), ("key", 1));
    assert_eq!(producer.snapshot_generation(), 1);
    let actual = collect(Arc::clone(&runtime_sort) as _, session.task_ctx())
        .await
        .unwrap();
    assert_eq!(keys(&actual), (0..10).map(Some).collect::<Vec<_>>());
    assert!(
        producer.snapshot_generation() >= 4,
        "at least three threshold improvements"
    );
    assert_eq!(
        consumer.snapshot_generation(),
        producer.snapshot_generation()
    );
    assert!(count_metric(scan.as_ref(), "row_groups_pruned_dynamic_filter") >= 3);
    let enabled_bytes = count_metric(scan.as_ref(), "bytes_scanned");
    assert!(enabled_bytes > 0);

    let (_file, baseline_scan) = parquet_input(groups(), &DataType::Int32, &session, 100);
    let baseline = sort(Arc::clone(&baseline_scan) as _, 10, SortOptions::default());
    let expected = collect(Arc::new(baseline), session.task_ctx())
        .await
        .unwrap();
    assert_eq!(keys(&actual), keys(&expected));
    assert_eq!(
        count_metric(baseline_scan.as_ref(), "row_groups_pruned_dynamic_filter"),
        0
    );
    assert!(enabled_bytes < count_metric(baseline_scan.as_ref(), "bytes_scanned"));
}

#[tokio::test]
async fn wrapper_reports_attachment_and_keeps_reader_metrics_on_scan() {
    let session = session(100);
    let (_file, scan) = parquet_input(groups(), &DataType::Int32, &session, 100);
    let plan = Arc::new(wrapper(
        &sort(Arc::clone(&scan) as _, 10, SortOptions::default()),
        &session,
    ));
    let actual = collect(Arc::clone(&plan) as _, session.task_ctx())
        .await
        .unwrap();
    assert_eq!(keys(&actual), (0..10).map(Some).collect::<Vec<_>>());
    assert_eq!(
        count_metric(plan.as_ref(), "dynamic_filter_reader_filters_attached"),
        1
    );
    assert_eq!(
        count_metric(plan.as_ref(), "dynamic_filter_reader_filters_skipped"),
        0
    );
    assert_eq!(plan.metrics().unwrap().output_rows(), Some(10));
    assert_eq!(count_metric(plan.as_ref(), "bytes_scanned"), 0);
    assert!(count_metric(scan.as_ref(), "bytes_scanned") > 0);
    assert!(count_metric(scan.as_ref(), "row_groups_pruned_dynamic_filter") >= 3);
}
