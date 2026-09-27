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
use arrow::array::{Array, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray};

fn nested_payload(timestamps: ArrayRef) -> ArrayRef {
    let field = Arc::new(Field::new("ts", timestamps.data_type().clone(), false));
    Arc::new(StructArray::new(vec![field].into(), vec![timestamps], None))
}

fn timestamp_input(
    project_payload: bool,
    session: &Arc<SessionContext>,
) -> (tempfile::NamedTempFile, Arc<DataSourceExec>) {
    // The first group establishes key < 0. Only the later, prunable group
    // contains visible values that overflow a millis-to-micros conversion.
    let timestamps = TimestampMillisecondArray::from_iter_values((0..200).map(|key| {
        if key < 100 {
            0
        } else {
            92_233_720_368_547_758
        }
    }));
    let payload = nested_payload(Arc::new(timestamps));
    let key_field = Field::new("key", DataType::Int32, false);
    let file_schema = Arc::new(Schema::new(vec![
        key_field.clone(),
        Field::new("payload", payload.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&file_schema),
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
        ArrowWriter::try_new(file.reopen().unwrap(), file_schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();
    assert_eq!(metadata.num_row_groups(), 2);
    assert_eq!(metadata.row_group(0).num_rows(), 100);
    assert_eq!(metadata.row_group(1).num_rows(), 100);

    // Spark's logical schema requests microseconds even for physical MILLIS.
    let logical_payload =
        nested_payload(Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new())));
    let data_schema = Arc::new(Schema::new(vec![
        key_field.clone(),
        Field::new("payload", logical_payload.data_type().clone(), false),
    ]));
    let required_schema = if project_payload {
        Arc::clone(&data_schema)
    } else {
        Arc::new(Schema::new(vec![key_field]))
    };
    let scan = init_datasource_exec(
        required_schema,
        Some(data_schema),
        None,
        ObjectStoreUrl::local_filesystem(),
        ObjectStoreBackend::Local,
        vec![vec![PartitionedFile::from_path(
            file.path().to_string_lossy().into_owned(),
        )
        .unwrap()]],
        Some(if project_payload { vec![0, 1] } else { vec![0] }),
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

#[tokio::test]
async fn projected_nested_timestamps_preserve_overflow_errors() {
    let session = session(16);
    for filtering in [false, true] {
        // Independent scans ensure both attempts read the file, even after an error.
        let (_file, scan) = timestamp_input(true, &session);
        let plain = sort(scan, 1, SortOptions::default());
        let plan: Arc<dyn ExecutionPlan> = if filtering {
            Arc::new(wrapper(&plain, &session))
        } else {
            Arc::new(plain)
        };
        let error = collect(Arc::clone(&plan), session.task_ctx())
            .await
            .expect_err("nested conversion must overflow even with TopK pruning");
        assert!(
            error.to_string().to_lowercase().contains("overflow"),
            "filtering={filtering}: {error}"
        );
        // Attachment is allowed; the per-file adapter disables pruning when
        // the projected conversion can overflow.
        if filtering {
            assert_eq!(
                count_metric(plan.as_ref(), "dynamic_filter_topk_filters_attached"),
                1
            );
        }
    }
}

#[tokio::test]
async fn unprojected_nested_timestamps_allow_reader_pruning() {
    let session = session(16);
    let (_file, scan) = timestamp_input(false, &session);
    assert_eq!(scan.schema().fields().len(), 1);
    let filtered = Arc::new(wrapper(
        &sort(Arc::clone(&scan) as _, 1, SortOptions::default()),
        &session,
    ));
    let actual = collect(Arc::clone(&filtered) as _, session.task_ctx())
        .await
        .unwrap();
    assert_eq!(keys(&actual), vec![Some(0)]);
    assert_eq!(
        count_metric(filtered.as_ref(), "dynamic_filter_topk_filters_attached"),
        1
    );
    assert_eq!(
        count_metric(scan.as_ref(), "row_groups_pruned_dynamic_filter"),
        1
    );
}
