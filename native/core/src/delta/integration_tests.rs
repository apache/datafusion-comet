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

//! End-to-end Phase 1 test: write a real Parquet data file + a Delta
//! transaction log pointing at it, list via `list_delta_files` (kernel
//! path), then drive the file through Comet's own `init_datasource_exec`
//! (arrow-58 ParquetSource path) and assert the batches.
//!
//! This proves the vertical slice — kernel log replay side-by-side with
//! Comet's reader — without pulling in the full `PhysicalPlanner`
//! (that's covered by the Scala integration tests in later phases, which
//! go through the actual proto → planner match arm).

#![cfg(test)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::delta::{list_delta_files, DeltaStorageConfig};
use crate::parquet::parquet_exec::init_datasource_exec;

/// Build a minimal two-column schema: `id: Int64`, `name: Utf8`.
fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Produce a 3-row batch.
fn sample_batch(schema: &SchemaRef) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    RecordBatch::try_new(Arc::clone(schema), vec![ids, names]).unwrap()
}

/// Write `batch` to `path` as a Parquet file and return the byte size.
fn write_parquet_file(path: &std::path::Path, batch: &RecordBatch) -> u64 {
    let file = std::fs::File::create(path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    std::fs::metadata(path).unwrap().len()
}

/// Commit a single-file Delta table with a hand-rolled JSON log entry.
///
/// We use the same minimal protocol/metadata pattern as the Phase 0 test,
/// plus an accurate `size` and `numRecords` from the real Parquet file.
fn commit_delta_table(table_dir: &std::path::Path, data_file: &str, size: u64, rows: usize) {
    let delta_log = table_dir.join("_delta_log");
    std::fs::create_dir_all(&delta_log).unwrap();

    let schema_json = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}"#;

    let commit0 = format!(
        "{protocol}\n{metadata}\n{add}",
        protocol = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        metadata = format!(
            r#"{{"metaData":{{"id":"test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{schema_json}","partitionColumns":[],"configuration":{{}},"createdTime":1700000000000}}}}"#,
        ),
        add = format!(
            r#"{{"add":{{"path":"{data_file}","partitionValues":{{}},"size":{size},"modificationTime":1700000000000,"dataChange":true,"stats":"{{\"numRecords\":{rows}}}"}}}}"#,
        ),
    );
    std::fs::write(delta_log.join("00000000000000000000.json"), commit0).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn delta_end_to_end_read_unpartitioned() {
    // --- 1. Materialize a real Delta table on disk ---
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("delta_e2e");
    std::fs::create_dir_all(&table_dir).unwrap();

    let schema = sample_schema();
    let batch = sample_batch(&schema);

    let data_file_name = "part-00000.parquet";
    let data_path = table_dir.join(data_file_name);
    let size = write_parquet_file(&data_path, &batch);

    commit_delta_table(&table_dir, data_file_name, size, batch.num_rows());

    // --- 2. Kernel-side log replay: list the active files ---
    let config = DeltaStorageConfig::default();
    let (entries, version) = list_delta_files(table_dir.to_str().unwrap(), &config, None).unwrap();

    assert_eq!(version, 0);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, data_file_name);
    assert_eq!(entries[0].size as u64, size);
    assert_eq!(entries[0].num_records, Some(batch.num_rows() as u64));
    assert!(!entries[0].has_deletion_vector());

    // --- 3. Comet-side read: build PartitionedFile and go through
    // init_datasource_exec (the same path the planner match arm uses) ---
    let session_ctx = Arc::new(SessionContext::new());

    // The PartitionedFile location must be the absolute filesystem path
    // (without the file:// prefix) — object_store's LocalFileSystem wants
    // raw posix paths.
    let absolute = data_path.to_str().unwrap().to_string();
    let partitioned_file = PartitionedFile::new(absolute, size);

    // For a local filesystem read the base URL is just "file://"; the
    // object-store cache key is unused because Phase 1 only tests local.
    let object_store_url = ObjectStoreUrl::parse("file://").unwrap();

    let empty_partition_schema: SchemaRef = Arc::new(Schema::empty());

    let exec = init_datasource_exec(
        Arc::clone(&schema),
        Some(Arc::clone(&schema)),
        Some(empty_partition_schema),
        object_store_url,
        vec![vec![partitioned_file]],
        None, // projection_vector: all columns
        None, // data_filters: none
        None, // default_values: none
        "UTC",
        true, // case_sensitive
        &session_ctx,
        false, // encryption_enabled
    )
    .expect("init_datasource_exec should succeed for a simple Delta-like read");

    // --- 4. Execute and assert ---
    let task_ctx = session_ctx.task_ctx();
    let batches = collect(exec, task_ctx).await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "expected 3 rows from the Delta table");

    // Spot-check column values from the first batch.
    let first = &batches[0];
    let id_col = first
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let name_col = first
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(id_col.values(), &[1, 2, 3]);
    assert_eq!(name_col.value(0), "a");
    assert_eq!(name_col.value(1), "b");
    assert_eq!(name_col.value(2), "c");
}
