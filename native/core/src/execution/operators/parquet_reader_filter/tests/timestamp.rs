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
use crate::execution::operators::dynamic_filter::DynamicFilterJoinExec;
use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::ObjectStoreBackend;
use arrow::array::{new_null_array, Int32Array, RecordBatch};
use arrow::datatypes::{Field, Schema, TimeUnit};
use datafusion::common::{JoinType, NullEquality};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::prelude::{SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

fn timestamp_payloads() -> Vec<DataType> {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
    let timestamp_field = Arc::new(Field::new("ts", timestamp.clone(), true));
    let map = |key_type, value_type| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", key_type, false),
                        Field::new("value", value_type, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    };
    let map_value = map(DataType::Int32, timestamp.clone());
    vec![
        timestamp.clone(),
        DataType::Struct(vec![Arc::clone(&timestamp_field)].into()),
        DataType::List(timestamp_field),
        map(timestamp, DataType::Int32),
        map_value.clone(),
        DataType::Struct(
            vec![Field::new(
                "nested",
                DataType::List(Arc::new(Field::new("item", map_value, true))),
                true,
            )]
            .into(),
        ),
    ]
}

fn session() -> Arc<SessionContext> {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(16)
        .with_parquet_page_index_pruning(false);
    config.options_mut().execution.parquet.pushdown_filters = false;
    Arc::new(SessionContext::new_with_config(config))
}

fn parquet_input(
    payload: &DataType,
    project_payload: bool,
    session: &Arc<SessionContext>,
) -> (tempfile::NamedTempFile, Arc<DataSourceExec>) {
    // Put the key after the timestamp payload so attachment must also remap
    // key@0 from the projected schema to key@1 in the physical file.
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("payload", payload.clone(), true),
        Field::new("key", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&file_schema),
        vec![
            new_null_array(payload, 200),
            Arc::new(Int32Array::from_iter_values(0..200)),
        ],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(
        file.reopen().unwrap(),
        Arc::clone(&file_schema),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    assert_eq!(writer.close().unwrap().num_row_groups(), 2);
    let projection = if project_payload { vec![1, 0] } else { vec![1] };
    let required_schema = Arc::new(file_schema.project(&projection).unwrap());
    let scan = init_datasource_exec(
        required_schema,
        Some(file_schema),
        None,
        ObjectStoreUrl::local_filesystem(),
        ObjectStoreBackend::Local,
        vec![vec![PartitionedFile::from_path(
            file.path().to_string_lossy().into_owned(),
        )
        .unwrap()]],
        Some(projection),
        None,
        None,
        "UTC",
        true,
        false,
        false,
        false,
        session,
        false,
        false,
        false,
    )
    .unwrap();
    (file, scan)
}

fn reader_input(scan: &Arc<DataSourceExec>, null_check: bool) -> Arc<dyn ExecutionPlan> {
    if null_check {
        Arc::new(CometFilterExec::from_datafusion(
            FilterExec::try_new(
                Arc::new(IsNotNullExpr::new(Arc::new(Column::new("key", 0)))),
                Arc::clone(scan) as _,
            )
            .unwrap(),
        ))
    } else {
        Arc::clone(scan) as _
    }
}

fn dynamic_predicate() -> Arc<DynamicFilterPhysicalExpr> {
    Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(Column::new("key", 0))],
        lit(true),
    ))
}

#[test]
fn projected_timestamps_exclude_reader_filters() {
    let session = session();
    for payload in timestamp_payloads() {
        for null_check in [false, true] {
            let (_file, scan) = parquet_input(&payload, true, &session);
            let input = reader_input(&scan, null_check);
            let attached = try_attach_parquet_reader_filter(
                &input,
                dynamic_predicate(),
                session.copied_config().options(),
            )
            .unwrap();
            assert!(attached.is_none(), "{payload:?}, null_check={null_check}");
        }
    }
}

#[tokio::test]
async fn unprojected_timestamps_allow_reader_pruning() {
    let session = session();
    for payload in timestamp_payloads() {
        for null_check in [false, true] {
            let (_file, scan) = parquet_input(&payload, false, &session);
            let predicate = dynamic_predicate();
            let reader = try_attach_parquet_reader_filter(
                &reader_input(&scan, null_check),
                Arc::clone(&predicate),
                session.copied_config().options(),
            )
            .unwrap()
            .expect("unprojected timestamps must not block reader pruning");
            // Update after attachment to verify the remapped reader predicate shares
            // the producer's state. Because the reader has not opened yet, this update
            // takes effect through initial statistics pruning.
            predicate
                .update(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("key", 0)),
                    Operator::Lt,
                    lit(100_i32),
                )))
                .unwrap();
            let batches = collect(reader, session.task_ctx()).await.unwrap();
            let keys = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            assert_eq!(keys, (0..100).collect::<Vec<_>>(), "{payload:?}");
            assert_eq!(
                statistics_pruned(&scan),
                1,
                "{payload:?}, null_check={null_check}"
            );
        }
    }
}

#[tokio::test]
async fn join_reader_attachment_obeys_projected_timestamp_guard() {
    let session = session();
    let payload = timestamp_payloads().pop().unwrap();
    for project_payload in [false, true] {
        let (_file, scan) = parquet_input(&payload, project_payload, &session);
        let build_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let build_batch = RecordBatch::try_new(
            Arc::clone(&build_schema),
            vec![Arc::new(Int32Array::from(vec![0]))],
        )
        .unwrap();
        let build =
            MemorySourceConfig::try_new_exec(&[vec![build_batch]], build_schema, None).unwrap();
        let join = HashJoinExec::try_new(
            build,
            reader_input(&scan, true),
            vec![(
                Arc::new(Column::new("key", 0)),
                Arc::new(Column::new("key", 0)),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap();
        let wrapper: Arc<dyn ExecutionPlan> = Arc::new(
            DynamicFilterJoinExec::try_new(&join, session.copied_config().options())
                .unwrap()
                .unwrap(),
        );
        let result = collect(Arc::clone(&wrapper), session.task_ctx())
            .await
            .unwrap();
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let expected_metric = if project_payload {
            "dynamic_filter_reader_filters_skipped"
        } else {
            "dynamic_filter_reader_filters_attached"
        };
        assert_eq!(
            wrapper
                .metrics()
                .unwrap()
                .sum_by_name(expected_metric)
                .unwrap()
                .as_usize(),
            1
        );
        // The join completes its build before opening the probe reader, so its
        // predicate prunes through initial statistics rather than live updates.
        assert_eq!(statistics_pruned(&scan), usize::from(!project_payload));
    }
}

fn statistics_pruned(scan: &DataSourceExec) -> usize {
    let MetricValue::PruningMetrics {
        pruning_metrics, ..
    } = scan
        .metrics()
        .unwrap()
        .sum_by_name("row_groups_pruned_statistics")
        .unwrap()
    else {
        panic!("expected row-group statistics metric");
    };
    pruning_metrics.pruned()
}
