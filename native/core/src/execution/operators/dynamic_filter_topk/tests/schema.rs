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
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::BinaryExpr;

fn write_file(payload_type: &DataType, start: i32, rows: usize) -> tempfile::NamedTempFile {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", payload_type.clone(), false),
    ]));
    let values = Int32Array::from_iter_values(start..start + rows as i32);
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

/// A winning file is followed in the same partition by an older incompatible file.
/// Dynamic pruning must not hide the conversion error from that losing file.
#[tokio::test]
async fn mixed_file_schema_rejections_survive_topk_pruning() {
    for (physical_type, target_type) in [
        (DataType::Int32, DataType::Int64),
        (DataType::Int64, DataType::Int32),
        (DataType::Int32, DataType::Utf8),
    ] {
        let clean = write_file(&target_type, 0, 100);
        let incompatible = write_file(&physical_type, 100, 100);
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", target_type.clone(), false),
        ]));
        for (filtering, decoder_filters) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let session = if decoder_filters {
                let mut config = SessionConfig::new()
                    .with_target_partitions(1)
                    .with_batch_size(100)
                    .with_parquet_page_index_pruning(true);
                config.options_mut().execution.parquet.pushdown_filters = true;
                Arc::new(SessionContext::new_with_config(config))
            } else {
                session(100)
            };
            let scan = parquet_scan(
                &[&clean, &incompatible],
                Arc::clone(&schema),
                vec![0, 1],
                None,
                false,
                &session,
            );
            let plain = sort(scan, 10, SortOptions::default());
            let plan: Arc<dyn ExecutionPlan> = if filtering {
                Arc::new(wrapper(&plain, &session))
            } else {
                Arc::new(plain)
            };
            let error = collect(plan, session.task_ctx()).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("Parquet column cannot be converted"),
                "{physical_type:?} -> {target_type:?}, filtering={filtering}, decoder_filters={decoder_filters}: {error}"
            );
        }
    }
}

/// Keep Spark's empty-file/static-filter semantics and permit unprojected mismatches.
#[tokio::test]
async fn schema_guard_preserves_empty_static_and_unprojected_cases() {
    for filtering in [false, true] {
        for scenario in [
            "empty",
            "static",
            "static_missing_count",
            "unprojected",
            "allowed",
        ] {
            let session = session(100);
            let clean = write_file(&DataType::Int64, 0, 100);
            let incompatible = write_file(
                &DataType::Int32,
                100,
                if scenario == "empty" { 0 } else { 100 },
            );
            if scenario == "static_missing_count" {
                super::statistics::omit_last_null_count(&incompatible, 0);
            }
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("payload", DataType::Int64, false),
            ]));
            let filters = (scenario.starts_with("static")).then(|| {
                vec![Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("key", 0)),
                    Operator::Lt,
                    lit(100_i32),
                )) as Arc<dyn PhysicalExpr>]
            });
            let projection = if scenario == "unprojected" {
                vec![0]
            } else {
                vec![0, 1]
            };
            let scan = parquet_scan(
                &[&clean, &incompatible],
                schema,
                projection,
                filters,
                scenario == "allowed",
                &session,
            );
            let plain = sort(Arc::clone(&scan) as _, 10, SortOptions::default());
            let plan: Arc<dyn ExecutionPlan> = if filtering {
                Arc::new(wrapper(&plain, &session))
            } else {
                Arc::new(plain)
            };
            let batches = collect(Arc::clone(&plan), session.task_ctx())
                .await
                .unwrap();
            assert_eq!(
                keys(&batches),
                (0..10).map(Some).collect::<Vec<_>>(),
                "scenario={scenario}, filtering={filtering}"
            );
            if scenario.starts_with("static") && filtering {
                assert_eq!(
                    count_metric(plan.as_ref(), "dynamic_filter_topk_filters_skipped"),
                    1
                );
            }
            if scenario == "unprojected" && filtering {
                assert!(scan.metrics().unwrap().output_rows().unwrap() < 200);
            }
        }
    }
}
