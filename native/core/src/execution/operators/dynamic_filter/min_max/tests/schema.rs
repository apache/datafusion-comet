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
use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::ObjectStoreBackend;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::scalar::ScalarValue;
use parquet::arrow::ArrowWriter;
use std::collections::HashMap;

#[tokio::test]
async fn reader_pruning_preserves_required_field_validation() {
    let session = session(2);
    for is_max in [false, true] {
        let sign = if is_max { -1 } else { 1 };
        let (file, _) = parquet_input(
            vec![Some(0), Some(sign), Some(10 * sign), None],
            &DataType::Int32,
            &session,
            2,
        );
        for nullable in [false, true] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("payload", DataType::Int32, false),
                Field::new("key", DataType::Int32, nullable),
            ]));
            for filtering in [false, true] {
                let scan = parquet_scan(
                    &[&file],
                    Arc::clone(&schema),
                    vec![1],
                    None,
                    false,
                    &session,
                );
                let plain = aggregate(Arc::clone(&scan) as _, is_max);
                let plan: Arc<dyn ExecutionPlan> = if filtering {
                    Arc::new(wrapper(&plain))
                } else {
                    Arc::new(plain)
                };
                let actual = collect(plan, session.task_ctx()).await;
                if nullable {
                    assert_eq!(result(&actual.unwrap()), Some(0));
                    assert_eq!(
                        metric(scan.as_ref(), "row_groups_pruned_dynamic_filter"),
                        usize::from(filtering)
                    );
                } else {
                    let error = actual.unwrap_err();
                    assert!(
                        error.to_string().contains(
                            "Column 'key' is declared as non-nullable but contains null values"
                        ),
                        "max={is_max}, filtering={filtering}: {error}"
                    );
                    assert_eq!(metric(scan.as_ref(), "row_groups_pruned_dynamic_filter"), 0);
                }
            }
        }
    }
}

#[tokio::test]
async fn reader_pruning_preserves_required_field_validation_for_null_defaults() {
    let session = session(2);
    for is_max in [false, true] {
        let sign = if is_max { -1 } else { 1 };
        let (first, _) = parquet_input(vec![Some(0), Some(sign)], &DataType::Int32, &session, 2);
        let missing_payload = tempfile::NamedTempFile::new().unwrap();
        let physical_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&physical_schema),
            vec![Arc::new(Int32Array::from(vec![10 * sign, 11 * sign]))],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(missing_payload.reopen().unwrap(), physical_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // NULL defaults must validate required fields; nullable fields and
        // non-NULL defaults remain eligible for runtime pruning.
        for (nullable, default) in [(false, None), (true, None), (false, Some(7))] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int32, true),
                Field::new("payload", DataType::Int32, nullable),
            ]));
            for filtering in [false, true] {
                let scan = init_datasource_exec(
                    Arc::clone(&schema),
                    Some(Arc::clone(&schema)),
                    None,
                    ObjectStoreUrl::local_filesystem(),
                    ObjectStoreBackend::Local,
                    vec![vec![&first, &missing_payload]
                        .into_iter()
                        .map(|file| {
                            PartitionedFile::from_path(file.path().to_string_lossy().into_owned())
                                .unwrap()
                        })
                        .collect()],
                    Some(vec![0, 1]),
                    None,
                    Some(HashMap::from([(
                        Column::new("payload", 1),
                        ScalarValue::Int32(default),
                    )])),
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
                    "CORRECTED",
                    "CORRECTED",
                )
                .unwrap();
                let plain = aggregate(Arc::clone(&scan) as _, is_max);
                let plan: Arc<dyn ExecutionPlan> = if filtering {
                    Arc::new(wrapper(&plain))
                } else {
                    Arc::new(plain)
                };
                let actual = collect(plan, session.task_ctx()).await;
                if nullable || default.is_some() {
                    assert_eq!(result(&actual.unwrap()), Some(0));
                    assert_eq!(
                        metric(scan.as_ref(), "output_rows"),
                        if filtering { 2 } else { 4 },
                        "max={is_max}, nullable={nullable}, default={default:?}, filtering={filtering}"
                    );
                } else {
                    let error = actual.unwrap_err();
                    assert!(
                        error.to_string().contains(
                            "Column 'payload' is declared as non-nullable but contains null values"
                        ),
                        "max={is_max}, filtering={filtering}: {error}"
                    );
                    assert_eq!(metric(scan.as_ref(), "row_groups_pruned_dynamic_filter"), 0);
                }
            }
        }
    }
}
