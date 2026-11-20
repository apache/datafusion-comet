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

use std::io::Write;
use std::sync::Arc;

use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::ObjectStoreBackend;
use arrow::array::{ArrayRef, Int32Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::{SessionConfig, SessionContext};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::ParquetMetaDataWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::statistics::Statistics as ParquetStatistics;
use parquet::file::writer::TrackedWrite;

pub(super) fn session(batch_size: usize) -> Arc<SessionContext> {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(batch_size)
        .with_parquet_page_index_pruning(false);
    // Isolate dynamic row-group pruning from row filters and page indexes.
    config.options_mut().execution.parquet.pushdown_filters = false;
    Arc::new(SessionContext::new_with_config(config))
}

/// Select key@1 in the physical file as key@0 in the scan output. Reader
/// attachment must retain live updates when it remaps the predicate.
pub(super) fn parquet_input(
    values: Vec<Option<i32>>,
    key_type: &DataType,
    session: &Arc<SessionContext>,
    group_size: usize,
) -> (tempfile::NamedTempFile, Arc<DataSourceExec>) {
    let key = cast(&Int32Array::from(values), key_type).unwrap();
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Int32, false),
        Field::new("key", key_type.clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&file_schema),
        vec![
            Arc::new(Int32Array::from_iter_values(0..key.len() as i32)) as ArrayRef,
            key,
        ],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(group_size))
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
    let metadata = writer.close().unwrap();
    assert_eq!(
        metadata.num_row_groups(),
        batch.num_rows().div_ceil(group_size)
    );
    let scan = parquet_scan(&[&file], file_schema, vec![1], None, false, session);
    (file, scan)
}

pub(super) fn parquet_scan(
    files: &[&tempfile::NamedTempFile],
    file_schema: Arc<Schema>,
    projection: Vec<usize>,
    data_filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
    allow_type_promotion: bool,
    session: &Arc<SessionContext>,
) -> Arc<DataSourceExec> {
    init_datasource_exec(
        Arc::new(file_schema.project(&projection).unwrap()),
        Some(file_schema),
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
        data_filters,
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
        "CORRECTED",
        "CORRECTED",
    )
    .unwrap()
}

/// Remove null counts from selected INT32 column chunks without changing their bounds.
pub(super) fn omit_null_counts(
    file: &tempfile::NamedTempFile,
    column: usize,
    row_groups: &[usize],
) {
    let original = std::fs::read(file.path()).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
    let mut builder = reader.metadata().as_ref().clone().into_builder();
    let mut groups = builder.take_row_groups();
    for &index in row_groups {
        let group = groups[index].clone();
        let mut columns = group.columns().to_vec();
        let Some(ParquetStatistics::Int32(stats)) = columns[column].statistics() else {
            panic!("expected INT32 statistics");
        };
        let (min, max) = (stats.min_opt().copied(), stats.max_opt().copied());
        columns[column] = columns[column]
            .clone()
            .into_builder()
            .set_statistics(ParquetStatistics::int32(min, max, None, None, false))
            .build()
            .unwrap();
        groups[index] = group
            .into_builder()
            .set_column_metadata(columns)
            .build()
            .unwrap();
    }
    let metadata = builder.set_row_groups(groups).build();
    let mut output = TrackedWrite::new(file.reopen().unwrap());
    output.write_all(&original).unwrap();
    ParquetMetaDataWriter::new_with_tracked(output, &metadata)
        .finish()
        .unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
    for &index in row_groups {
        assert!(reader
            .metadata()
            .row_group(index)
            .column(column)
            .statistics()
            .unwrap()
            .null_count_opt()
            .is_none());
    }
}
