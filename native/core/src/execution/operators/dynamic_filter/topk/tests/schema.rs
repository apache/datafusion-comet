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

fn write_file(payload_type: &DataType, start: i32) -> tempfile::NamedTempFile {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", payload_type.clone(), false),
    ]));
    let values = Int32Array::from_iter_values(start..start + 100);
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
async fn mixed_file_schema_rejection_survives_topk_pruning() {
    let clean = write_file(&DataType::Int64, 0);
    let incompatible = write_file(&DataType::Int32, 100);
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", DataType::Int64, false),
    ]));
    for filtering in [false, true] {
        let session = session(100);
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
            "filtering={filtering}: {error}"
        );
    }
}

/// Sanitizing incomplete statistics would expose a file excluded by a static
/// predicate. Decline attachment so the static predicate keeps hiding that file.
#[tokio::test]
async fn static_range_with_unknown_null_count_skips_reader_attachment() {
    let clean = write_file(&DataType::Int64, 0);
    let incompatible = write_file(&DataType::Int32, 100);
    super::statistics::omit_last_null_count(&incompatible, 0);
    for filtering in [false, true] {
        let session = session(100);
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", DataType::Int64, false),
        ]));
        let filters = vec![Arc::new(BinaryExpr::new(
            Arc::new(Column::new("key", 0)),
            Operator::Lt,
            lit(100_i32),
        )) as Arc<dyn PhysicalExpr>];
        let scan = parquet_scan(
            &[&clean, &incompatible],
            schema,
            vec![0, 1],
            Some(filters),
            false,
            &session,
        );
        let plain = sort(scan, 10, SortOptions::default());
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
            "filtering={filtering}"
        );
        if filtering {
            assert_eq!(
                count_metric(plan.as_ref(), "dynamic_filter_topk_filters_skipped"),
                1
            );
        }
    }
}
