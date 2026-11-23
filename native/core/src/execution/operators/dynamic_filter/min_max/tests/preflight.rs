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

use super::super::row_group_limit::{
    prepare_scan, MAX_PREFLIGHT_METADATA_BYTES, MAX_ROW_GROUPS_PER_FILE,
};
use super::*;
use crate::parquet::eager_page_index_reader_factory::{EagerPageIndexReaderFactory, ScanIoSource};
use crate::parquet::parquet_exec::METADATA_SIZE_HINT;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

mod encryption;

fn instrumented_scan(
    files: &[&tempfile::NamedTempFile],
    session: &Arc<SessionContext>,
    prefetch: bool,
) -> (Arc<DataSourceExec>, ExecutionPlanMetricsSet) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Int32, false),
        Field::new("key", DataType::Int32, true),
    ]));
    let scan = parquet_scan(files, schema, vec![1], None, false, session);
    let (config, source, _) = concrete_parquet_source(&scan).unwrap();
    let runtime = session.runtime_env();
    let metrics = ExecutionPlanMetricsSet::new();
    // Instrument the local fixture as an object store to count range requests.
    let factory = EagerPageIndexReaderFactory::new(
        runtime
            .object_store(ObjectStoreUrl::local_filesystem())
            .unwrap(),
        runtime.cache_manager.get_file_metadata_cache(),
        ScanIoSource::ObjectStore,
        &metrics,
    )
    .with_next_file_metadata_prefetch(prefetch.then(|| config.file_groups[0].files()));
    let mut config = config.clone();
    config.file_source = Arc::new(
        source
            .clone()
            .with_parquet_file_reader_factory(Arc::new(factory)),
    );
    (
        Arc::new(scan.as_ref().clone().with_data_source(Arc::new(config))),
        metrics,
    )
}

fn io_metric(metrics: &ExecutionPlanMetricsSet, name: &str) -> usize {
    metrics.clone_inner().sum_by_name(name).unwrap().as_usize()
}

fn session_with_cache_limit(limit: usize) -> Arc<SessionContext> {
    let session = session(1);
    session
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache()
        .update_cache_limit(limit);
    session
}

#[tokio::test]
async fn row_group_preflight_reads_a_cold_footer_in_one_request() {
    let session = session(1);
    let (file, _) = parquet_input(vec![Some(1)], &DataType::Int32, &session, 1);
    let (scan, metrics) = instrumented_scan(&[&file], &session, false);
    let input: Arc<dyn ExecutionPlan> = scan;
    let (_prepared, enabled) = prepare_scan(&input, &input, 0).await;
    assert!(enabled);
    assert_eq!(io_metric(&metrics, "scan_io_object_store_get_calls"), 1);
}

#[tokio::test]
async fn preflight_reuses_footers_without_a_shared_cache() {
    let fixture_session = session(1);
    // Every file improves MIN, so ordinary and filtered scans both read all files.
    let files: Vec<_> = (1..=16)
        .rev()
        .map(|value| parquet_input(vec![Some(value)], &DataType::Int32, &fixture_session, 1).0)
        .collect();
    let files: Vec<_> = files.iter().collect();
    for cache_limit in [0, 1] {
        for prefetch in [false, true] {
            let baseline_session = session_with_cache_limit(cache_limit);
            let (baseline_scan, baseline_metrics) =
                instrumented_scan(&files, &baseline_session, prefetch);
            let baseline = collect(
                Arc::new(aggregate(baseline_scan, false)),
                baseline_session.task_ctx(),
            )
            .await
            .unwrap();
            assert_eq!(result(&baseline), Some(1));
            assert_eq!(io_metric(&baseline_metrics, "scan_io_footer_reads"), 16);
            let baseline_bytes = io_metric(&baseline_metrics, "scan_io_metadata_bytes");

            let session = session_with_cache_limit(cache_limit);
            let (scan, metrics) = instrumented_scan(&files, &session, prefetch);
            let filtered = Arc::new(wrapper(&aggregate(scan, false)));
            // Re-execution must prepare a fresh reader handoff after the previous one is consumed.
            for execution in 1..=2 {
                let actual = collect(Arc::clone(&filtered) as _, session.task_ctx())
                    .await
                    .unwrap();
                assert_eq!(result(&actual), Some(1));
                assert_eq!(io_metric(&metrics, "scan_io_footer_reads"), 16 * execution);
                assert_eq!(
                    io_metric(&metrics, "scan_io_metadata_bytes"),
                    baseline_bytes * execution,
                    "cache_limit={cache_limit}, prefetch={prefetch}, execution={execution}"
                );
            }
        }
    }
}

#[tokio::test]
async fn reader_replacement_preserves_later_duplicate_file_footer() {
    let session = session_with_cache_limit(0);
    let (file, _) = parquet_input(vec![Some(1)], &DataType::Int32, &session, 1);
    let (scan, metrics) = instrumented_scan(&[&file, &file], &session, false);
    let input: Arc<dyn ExecutionPlan> = scan;
    let (prepared, enabled) = prepare_scan(&input, &input, 0).await;
    assert!(enabled);
    assert_eq!(io_metric(&metrics, "scan_io_footer_reads"), 2);
    let metadata_bytes = io_metric(&metrics, "scan_io_metadata_bytes");
    let scan = prepared.downcast_ref::<DataSourceExec>().unwrap();
    let (config, source, _) = concrete_parquet_source(scan).unwrap();
    let files = config.file_groups[0].files();
    let factory = source.parquet_file_reader_factory().unwrap();
    let mut first = factory
        .create_reader(0, files[0].clone(), Some(METADATA_SIZE_HINT), &metrics)
        .unwrap();
    first.get_metadata(None).await.unwrap();
    // Bloom-filter pruning can replace a reader while retaining its already
    // loaded metadata. That replacement must not consume the later occurrence.
    let replacement = factory
        .create_reader(0, files[0].clone(), Some(METADATA_SIZE_HINT), &metrics)
        .unwrap();
    drop(replacement);
    let mut second = factory
        .create_reader(0, files[1].clone(), Some(METADATA_SIZE_HINT), &metrics)
        .unwrap();
    second.get_metadata(None).await.unwrap();
    assert_eq!(io_metric(&metrics, "scan_io_footer_reads"), 2);
    assert_eq!(
        io_metric(&metrics, "scan_io_metadata_bytes"),
        metadata_bytes
    );
}

fn file_with_large_footer() -> tempfile::NamedTempFile {
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Int32, false),
        Field::new("key", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![0])),
            Arc::new(Int32Array::from(vec![1])),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "padding".to_string(),
            "x".repeat(MAX_PREFLIGHT_METADATA_BYTES + 1),
        )]))
        .build();
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

#[tokio::test]
async fn preflight_fallback_reuses_loaded_footers_and_reads_the_remaining_files() {
    let fixture_session = session(1);
    for large_footer in [false, true] {
        let groups = if large_footer {
            1
        } else {
            MAX_ROW_GROUPS_PER_FILE + 1
        };
        let first = if large_footer {
            file_with_large_footer()
        } else {
            parquet_input(
                (1..=groups as i32).rev().map(Some).collect(),
                &DataType::Int32,
                &fixture_session,
                1,
            )
            .0
        };
        let (last, _) = parquet_input(vec![Some(0)], &DataType::Int32, &fixture_session, 1);
        for prefetch in [false, true] {
            let baseline_session = session_with_cache_limit(1);
            let (baseline_scan, baseline_metrics) =
                instrumented_scan(&[&first, &last], &baseline_session, prefetch);
            let baseline = collect(
                Arc::new(aggregate(baseline_scan, false)),
                baseline_session.task_ctx(),
            )
            .await
            .unwrap();
            assert_eq!(result(&baseline), Some(0));
            assert_eq!(io_metric(&baseline_metrics, "scan_io_footer_reads"), 2);

            let session = session_with_cache_limit(1);
            let (scan, metrics) = instrumented_scan(&[&first, &last], &session, prefetch);
            let filtered = Arc::new(wrapper(&aggregate(Arc::clone(&scan) as _, false)));
            let actual = collect(Arc::clone(&filtered) as _, session.task_ctx())
                .await
                .unwrap();
            assert_eq!(result(&actual), Some(0));
            assert_eq!(metric(scan.as_ref(), "output_rows"), groups + 1);
            assert_eq!(
                metric(filtered.as_ref(), "dynamic_filter_minmax_filters_skipped"),
                1
            );
            assert_eq!(
                metric(filtered.as_ref(), "dynamic_filter_minmax_filters_attached"),
                0
            );
            assert_eq!(io_metric(&metrics, "scan_io_footer_reads"), 2);
            assert_eq!(
                io_metric(&metrics, "scan_io_metadata_bytes"),
                io_metric(&baseline_metrics, "scan_io_metadata_bytes"),
                "large_footer={large_footer}, prefetch={prefetch}"
            );
        }
    }
}
