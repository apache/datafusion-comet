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

mod errors;

use super::*;
use arrow::array::{Array, StructArray};
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ParquetJoinCase {
    Direct,
    Promotion,
    StructSubset,
    WideSelective,
    WideNonPruning,
}

impl ParquetJoinCase {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Promotion => "promotion",
            Self::StructSubset => "struct_subset",
            Self::WideSelective => "wide_selective",
            Self::WideNonPruning => "wide_non_pruning",
        }
    }

    fn batch(self, first_key: usize, rows: usize) -> RecordBatch {
        let values = Arc::new(Int32Array::from_iter_values(
            (first_key..first_key + rows).map(|value| value as i32),
        )) as ArrayRef;
        let key = if self == Self::WideNonPruning {
            Arc::new(Int32Array::from_iter_values(
                (0..rows).map(|value| (value % 2) as i32),
            )) as ArrayRef
        } else {
            Arc::clone(&values)
        };
        let mut fields = vec![Field::new("key", DataType::Int32, false)];
        let mut columns = vec![key];
        match self {
            Self::Direct | Self::Promotion => {
                fields.push(Field::new("payload", DataType::Int32, false));
                columns.push(values);
            }
            Self::StructSubset => {
                let children = (0..8)
                    .map(|index| Field::new(format!("field_{index}"), DataType::Int32, false))
                    .collect::<Vec<_>>();
                let payload = Arc::new(StructArray::new(children.into(), vec![values; 8], None));
                fields.push(Field::new("payload", payload.data_type().clone(), false));
                columns.push(payload);
            }
            Self::WideSelective | Self::WideNonPruning => {
                for index in 1..64 {
                    fields.push(Field::new(
                        format!("payload_{index}"),
                        DataType::Int32,
                        false,
                    ));
                    columns.push(Arc::clone(&values));
                }
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn logical_schema(self, physical: &Schema) -> SchemaRef {
        let mut fields = physical.fields().iter().cloned().collect::<Vec<_>>();
        match self {
            Self::Promotion => fields[1] = Arc::new(Field::new("payload", DataType::Int64, false)),
            Self::StructSubset => {
                // Keep the first two children, matching the fixture used in PR #6067.
                let DataType::Struct(children) = physical.field(1).data_type() else {
                    unreachable!()
                };
                fields[1] = Arc::new(Field::new(
                    "payload",
                    DataType::Struct(children.iter().take(2).cloned().collect()),
                    false,
                ));
            }
            _ => {}
        }
        Arc::new(Schema::new(fields))
    }
}

/// File paths and scan schema for a join. The caller owns the directory lifetime.
/// Existing files are validated and reused so separate benchmark binaries read
/// identical fixtures; generation and validation happen before timed execution.
pub(super) struct ParquetJoinFixture {
    case: ParquetJoinCase,
    files: Vec<PathBuf>,
    logical_schema: SchemaRef,
    pub(super) row_groups: usize,
    pub(super) output_rows: usize,
}

impl ParquetJoinFixture {
    pub(super) fn new(
        directory: &Path,
        case: ParquetJoinCase,
        files: usize,
        rows_per_file: usize,
        rows_per_group: usize,
    ) -> Self {
        let directory = directory.join(case.name());
        std::fs::create_dir_all(&directory).unwrap();
        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_row_count(Some(rows_per_group))
            .build();
        let physical_schema = case.batch(0, 0).schema();
        let mut paths = Vec::with_capacity(files);
        let mut row_groups = 0;
        for index in 0..files {
            let path = directory.join(format!("part-{index:04}.parquet"));
            if !path.exists() {
                let batch = case.batch(index * rows_per_file, rows_per_file);
                let mut writer = ArrowWriter::try_new(
                    std::fs::File::create(&path).unwrap(),
                    Arc::clone(&physical_schema),
                    Some(properties.clone()),
                )
                .unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
            }
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap())
                    .unwrap();
            assert_eq!(reader.schema(), &physical_schema, "{}", path.display());
            assert_eq!(
                reader.metadata().file_metadata().num_rows(),
                rows_per_file as i64
            );
            assert_eq!(
                reader.metadata().num_row_groups(),
                rows_per_file.div_ceil(rows_per_group)
            );
            row_groups += reader.metadata().num_row_groups();
            paths.push(path);
        }
        Self {
            case,
            files: paths,
            logical_schema: case.logical_schema(&physical_schema),
            row_groups,
            output_rows: if case == ParquetJoinCase::WideNonPruning {
                files * rows_per_file.div_ceil(2)
            } else {
                1
            },
        }
    }

    pub(super) fn session() -> Arc<SessionContext> {
        let mut config = SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(8192)
            .with_parquet_page_index_pruning(false);
        // Isolate row-group pruning, retaining the decoded-batch join filter.
        config.options_mut().execution.parquet.pushdown_filters = false;
        Arc::new(SessionContext::new_with_config(config))
    }

    fn scan(
        &self,
        session: &Arc<SessionContext>,
        projection: Vec<usize>,
        filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
        allow_type_promotion: bool,
    ) -> Arc<DataSourceExec> {
        init_datasource_exec(
            Arc::new(self.logical_schema.project(&projection).unwrap()),
            Some(Arc::clone(&self.logical_schema)),
            None,
            ObjectStoreUrl::local_filesystem(),
            ObjectStoreBackend::Local,
            vec![self
                .files
                .iter()
                .map(|path| {
                    PartitionedFile::from_path(path.to_string_lossy().into_owned()).unwrap()
                })
                .collect()],
            Some(projection),
            filters,
            None,
            "UTC",
            false,
            false,
            allow_type_promotion,
            false,
            session,
            false,
            false,
            false,
        )
        .unwrap()
    }

    pub(super) fn plan(
        &self,
        session: &Arc<SessionContext>,
        enabled: bool,
    ) -> (Arc<DataSourceExec>, Arc<dyn ExecutionPlan>) {
        let scan = self.scan(
            session,
            (0..self.logical_schema.fields().len()).collect(),
            None,
            self.case == ParquetJoinCase::Promotion,
        );
        let plan = join_scan(Arc::clone(&scan), session, enabled, 0);
        (scan, plan)
    }

    pub(super) fn data_bytes(scan: &Arc<DataSourceExec>) -> usize {
        scan.metrics()
            .unwrap()
            .sum_by_name("scan_io_data_bytes")
            .unwrap()
            .as_usize()
    }
}

fn join_scan(
    scan: Arc<DataSourceExec>,
    session: &Arc<SessionContext>,
    enabled: bool,
    build_key: i32,
) -> Arc<dyn ExecutionPlan> {
    let build = memory_exec(vec![RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![build_key]))],
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

async fn assert_adaptation_keeps_pruning(case: ParquetJoinCase) {
    let directory = tempfile::tempdir().unwrap();
    let fixture = ParquetJoinFixture::new(directory.path(), case, 2, 400, 100);
    let session = ParquetJoinFixture::session();
    let mut outputs = Vec::new();
    let mut bytes = Vec::new();
    for enabled in [false, true] {
        let (scan, plan) = fixture.plan(&session, enabled);
        let output = collect(Arc::clone(&plan), session.task_ctx())
            .await
            .unwrap();
        assert_eq!(row_count(&output), fixture.output_rows);
        assert_eq!(
            pruning_metric(&scan, "row_groups_pruned_statistics"),
            if enabled { fixture.row_groups - 1 } else { 0 },
            "case={case:?}, enabled={enabled}"
        );
        if enabled {
            assert_eq!(metric(&plan, "dynamic_filter_join_filters_attached"), 1);
        }
        bytes.push(ParquetJoinFixture::data_bytes(&scan));
        outputs.push(batches_to_sort_string(&output));
    }
    assert_eq!(outputs[0], outputs[1]);
    assert!(
        bytes[1] < bytes[0],
        "case={case:?}, data bytes OFF/ON={bytes:?}"
    );
}

#[tokio::test]
async fn allowed_int32_to_bigint_keeps_reader_pruning() {
    assert_adaptation_keeps_pruning(ParquetJoinCase::Promotion).await;
}

#[tokio::test]
async fn struct_subset_keeps_reader_pruning() {
    assert_adaptation_keeps_pruning(ParquetJoinCase::StructSubset).await;
}

#[tokio::test]
async fn static_predicate_only_column_preserves_conversion_error() {
    let directory = tempfile::tempdir().unwrap();
    let fixture =
        ParquetJoinFixture::new(directory.path(), ParquetJoinCase::Promotion, 1, 400, 100);
    for enabled in [false, true] {
        let mut config = SessionConfig::new()
            .with_target_partitions(1)
            .with_parquet_page_index_pruning(false);
        // This dependency is absent from the projection. Enable row filtering
        // so the static predicate actually evaluates the payload conversion.
        config.options_mut().execution.parquet.pushdown_filters = true;
        let session = Arc::new(SessionContext::new_with_config(config));
        let scan = fixture.scan(
            &session,
            vec![0],
            Some(vec![Arc::new(BinaryExpr::new(
                Arc::new(Column::new("payload", 1)),
                Operator::Gt,
                lit(-1_i64),
            ))]),
            false,
        );
        // No probe keys match. The payload is needed only by the static reader
        // predicate, whose unsupported conversion still has to be evaluated.
        let plan = join_scan(scan, &session, enabled, -1);
        let error = collect(plan, session.task_ctx()).await.expect_err(
            "runtime filtering must retain conversion errors in static-predicate-only columns",
        );
        // The Parquet row filter wraps the structured conversion error using Debug.
        let message = error.to_string();
        assert!(
            message.contains("ParquetSchemaConvert") && message.contains("[payload]"),
            "enabled={enabled}: {error}"
        );
    }
}
