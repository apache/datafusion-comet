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
use arrow::array::TimestampMillisecondArray;
use arrow::datatypes::TimeUnit;

async fn assert_conversion_error(
    batch: RecordBatch,
    logical_schema: SchemaRef,
    build_key: i32,
    allow_type_promotion: bool,
    expected_error: &str,
) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_dictionary_enabled(false)
        .build();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    assert_eq!(writer.close().unwrap().num_row_groups(), 2);
    let session = ParquetJoinFixture::session();
    for enabled in [false, true] {
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
            Some((0..logical_schema.fields().len()).collect()),
            None,
            None,
            "UTC",
            false,
            false,
            allow_type_promotion,
            false,
            &session,
            false,
            false,
            false,
        )
        .unwrap();
        let plan = join_scan(scan, &session, enabled, build_key);
        let error = collect(plan, session.task_ctx()).await.expect_err(
            "a safe adaptation must not let runtime pruning hide another conversion error",
        );
        assert!(
            error.to_string().contains(expected_error),
            "enabled={enabled}: {error}"
        );
    }
}

#[tokio::test]
async fn struct_subset_preserves_retained_timestamp_overflow() {
    // The nonmatching second row group overflows. Dropping an unrelated struct
    // field does not make conversion of the retained timestamp infallible.
    let timestamp = Arc::new(TimestampMillisecondArray::from_iter_values(
        (0..200).map(|key| if key < 100 { 0 } else { i64::MAX / 1_000 + 1 }),
    )) as ArrayRef;
    let payload = Arc::new(StructArray::new(
        vec![
            Field::new("ts", timestamp.data_type().clone(), false),
            Field::new("omitted", DataType::Int32, false),
        ]
        .into(),
        vec![timestamp, Arc::new(Int32Array::from_iter_values(0..200))],
        None,
    ));
    let key_field = Field::new("key", DataType::Int32, false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            key_field.clone(),
            Field::new("payload", payload.data_type().clone(), false),
        ])),
        vec![Arc::new(Int32Array::from_iter_values(0..200)), payload],
    )
    .unwrap();
    let logical_schema = Arc::new(Schema::new(vec![
        key_field,
        Field::new(
            "payload",
            DataType::Struct(
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                )]
                .into(),
            ),
            false,
        ),
    ]));
    assert_conversion_error(batch, logical_schema, 0, false, "overflow").await;
}

#[tokio::test]
async fn allowed_promotion_does_not_hide_an_unrelated_conversion_error() {
    let key_field = Field::new("key", DataType::Int32, false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            key_field.clone(),
            Field::new("safe_payload", DataType::Int32, false),
            Field::new("unsafe_payload", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int32Array::from_iter_values(0..200)),
            Arc::new(Int32Array::from_iter_values(0..200)),
            Arc::new(Int64Array::from(vec![i64::MAX; 200])),
        ],
    )
    .unwrap();
    let logical_schema = Arc::new(Schema::new(vec![
        key_field,
        Field::new("safe_payload", DataType::Int64, false),
        Field::new("unsafe_payload", DataType::Int32, false),
    ]));
    // No keys match, so an eligibility check that stops at the permitted
    // promotion would prune both groups and incorrectly suppress the error.
    assert_conversion_error(
        batch,
        logical_schema,
        -1,
        true,
        "Parquet column cannot be converted",
    )
    .await;
}
