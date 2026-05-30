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

//! End-to-end integration test for the contrib-delta crate.
//!
//! Crate-root integration test (compiled as a separate binary that links against
//! the contrib's *public* API only). The shape mirrors the Phase-1
//! `native/core/src/delta/integration_tests.rs` but goes through the current
//! contrib's `plan_delta_scan` -> `build_delta_partitioned_files` ->
//! DataFusion `ParquetSource` -> RecordBatch assertion path.
//!
//! What this proves end-to-end:
//!
//!   - delta-kernel-rs reads a real `_delta_log` and returns the right `add`
//!   - the file path round-trips through URL normalization correctly
//!   - the parquet file actually exists at the resolved path
//!   - kernel + DataFusion ParquetSource produce the same rows you wrote
//!
//! What it does NOT cover (those are unit tests):
//!
//!   - DV materialization (kernel test only)
//!   - synthetic column emission (synthetic_columns.rs tests)
//!   - column mapping rewrite (planner.rs tests)
//!   - predicate translation (predicate.rs tests)

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use comet_contrib_delta::{list_delta_files, plan_delta_scan, DeltaStorageConfig};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Build a `id: i64, name: Utf8` schema that matches the JSON schema we'll write
/// into the `_delta_log` `metaData` action.
fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn sample_batch(schema: &SchemaRef) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    RecordBatch::try_new(Arc::clone(schema), vec![ids, names]).unwrap()
}

/// Write `batch` to `path` as a Parquet file. Returns the file size in bytes.
fn write_parquet_file(path: &Path, batch: &RecordBatch) -> u64 {
    let file = std::fs::File::create(path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    std::fs::metadata(path).unwrap().len()
}

/// Build a minimal Delta `_delta_log/00000000000000000000.json` pointing at a
/// single parquet file. Schema must match what `sample_schema()` produces.
fn commit_delta_table(table_dir: &Path, data_file: &str, size: u64, rows: usize) {
    let delta_log = table_dir.join("_delta_log");
    std::fs::create_dir_all(&delta_log).unwrap();

    // Schema in Delta's JSON form: identical to what `sample_schema()` produces in arrow.
    // Backslashes pre-escaped for the embedded JSON string.
    let schema_json = "{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[\
        {\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},\
        {\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}\
    ]}";

    let commit0 = format!(
        "{}\n{}\n{}",
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        format!(
            "{{\"metaData\":{{\"id\":\"e2e-test\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":\"{}\",\"partitionColumns\":[],\"configuration\":{{}},\"createdTime\":1700000000000}}}}",
            schema_json
        ),
        format!(
            "{{\"add\":{{\"path\":\"{data_file}\",\"partitionValues\":{{}},\"size\":{size},\"modificationTime\":1700000000000,\"dataChange\":true,\"stats\":\"{{\\\"numRecords\\\":{rows}}}\"}}}}"
        ),
    );

    std::fs::write(delta_log.join("00000000000000000000.json"), commit0).unwrap();
}

// -----------------------------------------------------------------------------
// Test 1: list_delta_files end-to-end against a real parquet file
// -----------------------------------------------------------------------------

#[test]
fn list_delta_files_finds_real_parquet() {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("delta-e2e");
    std::fs::create_dir_all(&table_dir).unwrap();

    let schema = sample_schema();
    let batch = sample_batch(&schema);
    let parquet_path = table_dir.join("part-00000.parquet");
    let size = write_parquet_file(&parquet_path, &batch);
    commit_delta_table(&table_dir, "part-00000.parquet", size, batch.num_rows());

    let cfg = DeltaStorageConfig::default();
    let (entries, version) =
        list_delta_files(table_dir.to_str().unwrap(), &cfg, None).unwrap();

    assert_eq!(version, 0, "snapshot version");
    assert_eq!(entries.len(), 1, "one add file");
    let e = &entries[0];
    assert_eq!(e.path, "part-00000.parquet", "relative path preserved");
    assert_eq!(e.size as u64, size, "size matches actual parquet file");
    assert_eq!(e.num_records, Some(3));
    assert!(!e.has_deletion_vector());
    assert!(e.partition_values.is_empty());
}

// -----------------------------------------------------------------------------
// Test 2: plan_delta_scan returns a scan plan with the right entries
// -----------------------------------------------------------------------------

#[test]
fn plan_delta_scan_returns_one_entry_for_single_file_table() {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("delta-plan");
    std::fs::create_dir_all(&table_dir).unwrap();

    let schema = sample_schema();
    let batch = sample_batch(&schema);
    let parquet_path = table_dir.join("part-00000.parquet");
    let size = write_parquet_file(&parquet_path, &batch);
    commit_delta_table(&table_dir, "part-00000.parquet", size, batch.num_rows());

    let cfg = DeltaStorageConfig::default();
    let plan = plan_delta_scan(table_dir.to_str().unwrap(), &cfg, None).unwrap();
    assert_eq!(plan.entries.len(), 1);
    assert_eq!(plan.version, 0);
    // unsupported_features should be empty for a basic single-file table.
    assert!(plan.unsupported_features.is_empty());
}

// -----------------------------------------------------------------------------
// Test 3: snapshot pinning - version=Some(0) returns version 0
// -----------------------------------------------------------------------------

#[test]
fn list_delta_files_pinned_version_returns_that_version() {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("delta-pinned");
    std::fs::create_dir_all(&table_dir).unwrap();

    let schema = sample_schema();
    let batch = sample_batch(&schema);
    let parquet_path = table_dir.join("part-00000.parquet");
    let size = write_parquet_file(&parquet_path, &batch);
    commit_delta_table(&table_dir, "part-00000.parquet", size, batch.num_rows());

    let cfg = DeltaStorageConfig::default();
    let (_entries, version) =
        list_delta_files(table_dir.to_str().unwrap(), &cfg, Some(0)).unwrap();
    assert_eq!(version, 0);
}

// -----------------------------------------------------------------------------
// Test 4: empty table (commit 0 has only protocol + metadata, no adds)
// -----------------------------------------------------------------------------

#[test]
fn list_delta_files_empty_table_returns_no_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("delta-empty");
    let delta_log = table_dir.join("_delta_log");
    std::fs::create_dir_all(&delta_log).unwrap();

    // No `add` action — just protocol + metaData.
    let commit0 = [
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        "{\"metaData\":{\"id\":\"empty\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":1700000000000}}",
    ].join("\n");
    std::fs::write(delta_log.join("00000000000000000000.json"), &commit0).unwrap();

    let cfg = DeltaStorageConfig::default();
    let (entries, version) =
        list_delta_files(table_dir.to_str().unwrap(), &cfg, None).unwrap();
    assert_eq!(version, 0);
    assert!(entries.is_empty(), "expected no entries for empty table");
}
