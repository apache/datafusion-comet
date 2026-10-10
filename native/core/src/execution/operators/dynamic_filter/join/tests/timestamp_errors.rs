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
use arrow::array::{StructArray, TimestampMicrosecondArray, TimestampMillisecondArray};

fn timestamp_payload(values: ArrayRef, nested: bool) -> ArrayRef {
    if nested {
        Arc::new(StructArray::new(
            vec![Field::new("ts", values.data_type().clone(), false)].into(),
            vec![values],
            None,
        ))
    } else {
        values
    }
}

async fn assert_timestamp_overflow_preserved(nested: bool) {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_parquet_page_index_pruning(false);
    // Isolate runtime row-group pruning from page and row filtering.
    config.options_mut().execution.parquet.pushdown_filters = false;
    let session = Arc::new(SessionContext::new_with_config(config));
    let payload = timestamp_payload(
        Arc::new(TimestampMillisecondArray::from_iter_values((0..200).map(
            |key| {
                if key < 100 {
                    0
                } else {
                    i64::MAX / 1_000 + 1
                }
            },
        ))),
        nested,
    );
    let key_field = Field::new("key", DataType::Int32, false);
    let physical_schema = Arc::new(Schema::new(vec![
        key_field.clone(),
        Field::new("payload", payload.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&physical_schema),
        vec![Arc::new(Int32Array::from_iter_values(0..200)), payload],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_dictionary_enabled(false)
        .build();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), physical_schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();
    assert_eq!(metadata.num_row_groups(), 2);

    // The build matches only the valid first row group. Spark's logical schema
    // requests microseconds for the physical millisecond timestamp payload.
    let logical_payload = timestamp_payload(
        Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new())),
        nested,
    );
    let logical_schema = Arc::new(Schema::new(vec![
        key_field,
        Field::new("payload", logical_payload.data_type().clone(), false),
    ]));
    for enabled in [false, true] {
        // The two plans read the same file with fresh scan execution state.
        let scan = init_datasource_exec(
            Arc::clone(&logical_schema),
            Some(Arc::clone(&logical_schema)),
            None,
            ObjectStoreUrl::local_filesystem(),
            ObjectStoreBackend::Local,
            vec![vec![PartitionedFile::from_path(
                file.path().to_string_lossy().into_owned(),
            )
            .unwrap()]],
            Some(vec![0, 1]),
            None,
            None,
            "UTC",
            true,
            false,
            false,
            false,
            &session,
            false,
            false,
            false,
            false,
        )
        .unwrap();
        let join = single_key_join_plans(
            input(vec![Some(0)], &DataType::Int32, 0),
            scan,
            PartitionMode::Partitioned,
        );
        let plan = PhysicalPlanner::apply_join_dynamic_filter(
            Arc::new(join),
            enabled,
            session.copied_config().options(),
        )
        .unwrap();
        let error = collect(plan, session.task_ctx())
            .await
            .expect_err("join runtime filtering must preserve the projected timestamp error");
        assert!(
            error.to_string().to_lowercase().contains("overflow"),
            "enabled={enabled}, nested={nested}: {error}"
        );
    }
}

#[tokio::test]
async fn reader_filter_preserves_timestamp_overflow() {
    assert_timestamp_overflow_preserved(false).await;
}

#[tokio::test]
async fn reader_filter_preserves_nested_timestamp_overflow() {
    assert_timestamp_overflow_preserved(true).await;
}
