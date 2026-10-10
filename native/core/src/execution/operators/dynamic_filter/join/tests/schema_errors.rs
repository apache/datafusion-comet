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

mod partition_columns;

fn write_file(payload_type: &DataType, keys: std::ops::Range<i32>) -> tempfile::NamedTempFile {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", payload_type.clone(), false),
    ]));
    let values = Int32Array::from_iter_values(keys);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(values.clone()),
            cast(&values, payload_type).unwrap(),
        ],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer = ArrowWriter::try_new(
        file.reopen().unwrap(),
        schema,
        Some(
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn scan(
    files: &[&tempfile::NamedTempFile],
    project_payload: bool,
    filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
    allow_type_promotion: bool,
    session: &Arc<SessionContext>,
) -> Arc<DataSourceExec> {
    let data_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", DataType::Int64, false),
    ]));
    let projection = if project_payload { vec![0, 1] } else { vec![0] };
    let required_schema = Arc::new(data_schema.project(&projection).unwrap());
    init_datasource_exec(
        required_schema,
        Some(data_schema),
        None,
        ObjectStoreUrl::local_filesystem(),
        ObjectStoreBackend::Local,
        vec![files
            .iter()
            .map(|file| {
                PartitionedFile::from_path(file.path().to_string_lossy().into_owned()).unwrap()
            })
            .collect()],
        Some(projection),
        filters,
        None,
        "UTC",
        true,
        false,
        allow_type_promotion,
        false,
        session,
        false,
        false,
        false,
        false,
    )
    .unwrap()
}

fn filtered_join(
    scan: Arc<DataSourceExec>,
    enabled: bool,
    session: &Arc<SessionContext>,
) -> Arc<dyn ExecutionPlan> {
    let build = memory_exec(vec![RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![0]))],
    )
    .unwrap()]);
    let join = single_key_join_plans(build, scan, PartitionMode::Partitioned);
    PhysicalPlanner::apply_join_dynamic_filter(
        Arc::new(join),
        enabled,
        session.copied_config().options(),
    )
    .unwrap()
}

/// A nonmatching file still has to raise its projected payload conversion error.
/// Spark 3 rejects INT32 -> BIGINT unless type promotion is explicitly allowed.
#[tokio::test]
async fn join_reader_filter_preserves_schema_conversion_error() {
    let compatible = write_file(&DataType::Int64, 0..4);
    let incompatible = write_file(&DataType::Int32, 100..104);
    for row_filter in [false, true] {
        for enabled in [false, true] {
            let mut config = SessionConfig::new()
                .with_target_partitions(1)
                .with_parquet_page_index_pruning(false);
            config.options_mut().execution.parquet.pushdown_filters = row_filter;
            let session = Arc::new(SessionContext::new_with_config(config));
            let scan = scan(&[&compatible, &incompatible], true, None, false, &session);
            let plan = filtered_join(scan, enabled, &session);
            let result = collect(plan, session.task_ctx()).await;
            let error = result.expect_err(&format!(
                "projected INT32 -> BIGINT must fail: enabled={enabled}, row_filter={row_filter}"
            ));
            assert!(
                error
                    .to_string()
                    .contains("Parquet column cannot be converted"),
                "enabled={enabled}, row_filter={row_filter}: {error}"
            );
        }
    }
}

/// Supplied file statistics allow pruning before the file's schema adapter runs.
#[tokio::test]
async fn join_reader_filter_preserves_schema_error_with_file_statistics() {
    use datafusion::common::stats::Precision;
    use datafusion::common::{ScalarValue, Statistics};

    let compatible = write_file(&DataType::Int64, 0..4);
    let incompatible = write_file(&DataType::Int32, 100..104);
    for enabled in [false, true] {
        let mut config = SessionConfig::new()
            .with_target_partitions(1)
            .with_parquet_page_index_pruning(false);
        config.options_mut().execution.parquet.pushdown_filters = false;
        let session = Arc::new(SessionContext::new_with_config(config));
        let scan = scan(&[&compatible, &incompatible], true, None, false, &session);
        let (config, _) = scan.downcast_to_file_source::<ParquetSource>().unwrap();
        let mut config = config.clone();
        config.file_groups = vec![config.file_groups[0]
            .files()
            .iter()
            .cloned()
            .zip([0, 100])
            .map(|(file, first_key)| {
                let mut statistics = Statistics::new_unknown(&scan.schema());
                statistics.num_rows = Precision::Exact(4);
                statistics.column_statistics[0].min_value =
                    Precision::Exact(ScalarValue::Int32(Some(first_key)));
                statistics.column_statistics[0].max_value =
                    Precision::Exact(ScalarValue::Int32(Some(first_key + 3)));
                statistics.column_statistics[0].null_count = Precision::Exact(0);
                file.with_statistics(Arc::new(statistics))
            })
            .collect::<Vec<_>>()
            .into()];
        let scan = Arc::new(scan.as_ref().clone().with_data_source(Arc::new(config)));
        let plan = filtered_join(scan, enabled, &session);
        let error = collect(plan, session.task_ctx()).await.expect_err(&format!(
            "file statistics must preserve the schema error: enabled={enabled}"
        ));
        assert!(
            error
                .to_string()
                .contains("Parquet column cannot be converted"),
            "enabled={enabled}: {error}"
        );
    }
}

/// Valid conversions and unread mismatched columns must remain readable. Spark's
/// static predicates and empty files can also legitimately avoid conversion errors.
#[tokio::test]
async fn join_reader_schema_guard_preserves_readable_cases() {
    let compatible = write_file(&DataType::Int64, 0..4);
    for scenario in ["empty", "static", "unprojected", "allowed"] {
        let incompatible = write_file(
            &DataType::Int32,
            if scenario == "empty" {
                100..100
            } else {
                100..104
            },
        );
        for enabled in [false, true] {
            let mut config = SessionConfig::new()
                .with_target_partitions(1)
                .with_parquet_page_index_pruning(false);
            config.options_mut().execution.parquet.pushdown_filters = false;
            let session = Arc::new(SessionContext::new_with_config(config));
            let filters = (scenario == "static").then(|| {
                vec![Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("key", 0)),
                    Operator::Lt,
                    lit(100_i32),
                )) as Arc<dyn PhysicalExpr>]
            });
            let scan = scan(
                &[&compatible, &incompatible],
                scenario != "unprojected",
                filters,
                scenario == "allowed",
                &session,
            );
            let plan = filtered_join(scan, enabled, &session);
            let output = collect(plan, session.task_ctx())
                .await
                .unwrap_or_else(|error| panic!("scenario={scenario}, enabled={enabled}: {error}"));
            assert_eq!(
                row_count(&output),
                1,
                "scenario={scenario}, enabled={enabled}"
            );
        }
    }
}
