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
use datafusion::common::ScalarValue;
use datafusion::physical_expr::utils::collect_columns;

fn partitioned_session() -> Arc<SessionContext> {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_parquet_page_index_pruning(false);
    // Isolate row-group pruning from the residual batch filter.
    config.options_mut().execution.parquet.pushdown_filters = false;
    Arc::new(SessionContext::new_with_config(config))
}

fn partitioned_scan(
    files: &[(&tempfile::NamedTempFile, i32)],
    projection: Vec<usize>,
    filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
    session: &Arc<SessionContext>,
) -> Arc<DataSourceExec> {
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", DataType::Int64, false),
    ]));
    let partition_field = Field::new("part", DataType::Int32, false);
    let table_schema = Schema::new(vec![
        file_schema.field(0).clone(),
        file_schema.field(1).clone(),
        partition_field.clone(),
    ]);
    init_datasource_exec(
        Arc::new(table_schema.project(&projection).unwrap()),
        Some(file_schema),
        Some(Arc::new(Schema::new(vec![partition_field]))),
        ObjectStoreUrl::local_filesystem(),
        ObjectStoreBackend::Local,
        vec![files
            .iter()
            .map(|(file, partition)| {
                let mut file =
                    PartitionedFile::from_path(file.path().to_string_lossy().into_owned()).unwrap();
                file.partition_values = vec![ScalarValue::Int32(Some(*partition))];
                file
            })
            .collect()],
        Some(projection),
        filters,
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
        false,
    )
    .unwrap()
}

#[tokio::test]
async fn projected_partition_preserves_payload_conversion_error() {
    let compatible = write_file(&DataType::Int64, 0..4);
    let incompatible = write_file(&DataType::Int32, 100..104);
    for enabled in [false, true] {
        let session = partitioned_session();
        let scan = partitioned_scan(
            &[(&compatible, 7), (&incompatible, 8)],
            vec![0, 2, 1],
            None,
            &session,
        );
        let result = collect(filtered_join(scan, enabled, &session), session.task_ctx()).await;
        let error = result.expect_err("the nonmatching partition still has an invalid payload");
        assert!(
            error
                .to_string()
                .contains("Parquet column cannot be converted"),
            "enabled={enabled}: {error}"
        );
    }
}

#[tokio::test]
async fn projected_partition_keeps_runtime_reader_pruning() {
    let matching = write_file(&DataType::Int64, 0..4);
    let nonmatching = write_file(&DataType::Int64, 100..104);
    let mut outputs = Vec::new();
    for enabled in [false, true] {
        let session = partitioned_session();
        let scan = partitioned_scan(
            &[(&matching, 7), (&nonmatching, 8)],
            vec![0, 2, 1],
            None,
            &session,
        );
        let plan = filtered_join(Arc::clone(&scan), enabled, &session);
        let output = collect(Arc::clone(&plan), session.task_ctx())
            .await
            .unwrap();
        assert_eq!(row_count(&output), 1, "enabled={enabled}");
        let batch = output.iter().find(|batch| batch.num_rows() > 0).unwrap();
        assert_eq!(batch.schema().field(2).name(), "part");
        assert_eq!(
            ScalarValue::try_from_array(batch.column(2), 0).unwrap(),
            ScalarValue::Int32(Some(7))
        );
        assert_eq!(
            pruning_metric(&scan, "row_groups_pruned_statistics"),
            usize::from(enabled),
            "partition literals must not disable safe runtime reader pruning"
        );
        if enabled {
            assert_eq!(metric(&plan, "dynamic_filter_join_filters_attached"), 1);
        }
        outputs.push(batches_to_sort_string(&output));
    }
    assert_eq!(outputs[0], outputs[1]);
}

#[tokio::test]
async fn unprojected_partition_predicate_keeps_runtime_reader_pruning() {
    let matching = write_file(&DataType::Int64, 0..4);
    let nonmatching = write_file(&DataType::Int64, 100..104);
    let mut outputs = Vec::new();
    let mut pruned_groups = Vec::new();
    for enabled in [false, true] {
        let session = partitioned_session();
        let scan = partitioned_scan(
            &[(&matching, 7), (&nonmatching, 7), (&matching, 8)],
            vec![0, 1],
            Some(vec![Arc::new(BinaryExpr::new(
                Arc::new(Column::new("part", 2)),
                Operator::Eq,
                lit(7_i32),
            ))]),
            &session,
        );
        assert!(scan.schema().index_of("part").is_err());
        let (_, source) = scan.downcast_to_file_source::<ParquetSource>().unwrap();
        assert!(collect_columns(&source.filter().unwrap()).contains(&Column::new("part", 2)));
        let plan = filtered_join(Arc::clone(&scan), enabled, &session);
        let output = collect(Arc::clone(&plan), session.task_ctx())
            .await
            .unwrap();
        // The static predicate excludes partition 8 even though its key matches.
        assert_eq!(row_count(&output), 1, "enabled={enabled}");
        if enabled {
            assert_eq!(metric(&plan, "dynamic_filter_join_filters_attached"), 1);
        }
        pruned_groups.push(pruning_metric(&scan, "row_groups_pruned_statistics"));
        outputs.push(batches_to_sort_string(&output));
    }
    assert_eq!(outputs[0], outputs[1]);
    assert_eq!(
        pruned_groups[1],
        pruned_groups[0] + 1,
        "runtime pruning must still exclude the nonmatching row group"
    );
}
