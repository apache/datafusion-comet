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

//! JNI bridge for Iceberg compaction operations.
//!
//! This module provides JNI functions for native Iceberg compaction (scan + write).
//! Commit is handled by Iceberg's Java API in Scala for reliability.

use std::collections::HashMap;
use std::sync::Arc;

use jni::objects::{JClass, JString};
use jni::sys::jstring;
use jni::JNIEnv;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use crate::execution::operators::{IcebergCompactionResult, IcebergDataFileMetadata};

/// Configuration for Iceberg table metadata passed from JVM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergTableConfig {
    /// Table identifier (e.g., "db.table_name")
    pub table_identifier: String,
    /// Metadata file location
    pub metadata_location: String,
    /// Warehouse location
    pub warehouse_location: String,
    /// Current snapshot ID (for validation)
    pub current_snapshot_id: Option<i64>,
    /// File IO properties (for object store access)
    pub file_io_properties: HashMap<String, String>,
}

impl IcebergTableConfig {
    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|e| format!("Failed to parse table config: {}", e))
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string(self).map_err(|e| format!("Failed to serialize table config: {}", e))
    }
}

/// File scan task configuration passed from JVM for compaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileScanTaskConfig {
    /// Data file path
    pub file_path: String,
    /// File size in bytes
    pub file_size_bytes: u64,
    /// Record count
    pub record_count: u64,
    /// Partition path (e.g., "year=2024/month=01" or "" for unpartitioned)
    pub partition_path: String,
    /// Partition spec ID
    pub partition_spec_id: i32,
    /// Start position in file (for split reads)
    pub start: i64,
    /// Length to read (for split reads)
    pub length: i64,
}

/// Compaction task configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionTaskConfig {
    /// Table configuration
    pub table_config: IcebergTableConfig,
    /// Files to compact (scan tasks)
    pub file_scan_tasks: Vec<FileScanTaskConfig>,
    /// Target file size for output
    pub target_file_size_bytes: u64,
    /// Compression codec (snappy, zstd, etc.)
    pub compression: String,
    /// Output data directory
    pub data_dir: String,
}

impl CompactionTaskConfig {
    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|e| format!("Failed to parse compaction config: {}", e))
    }
}

/// Result of native compaction execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeCompactionResult {
    /// Whether compaction succeeded
    pub success: bool,
    /// Error message if failed
    pub error_message: Option<String>,
    /// Compaction result with files to add/delete
    pub result: Option<IcebergCompactionResult>,
}

impl NativeCompactionResult {
    /// Create a success result
    pub fn success(result: IcebergCompactionResult) -> Self {
        Self {
            success: true,
            error_message: None,
            result: Some(result),
        }
    }

    /// Create a failure result
    pub fn failure(error: String) -> Self {
        Self {
            success: false,
            error_message: Some(error),
            result: None,
        }
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string(self)
            .map_err(|e| format!("Failed to serialize compaction result: {}", e))
    }
}

/// Execute native Iceberg compaction.
///
/// This function:
/// 1. Parses the compaction configuration from JSON
/// 2. Creates a native scan plan for the input files
/// 3. Writes compacted output using IcebergParquetWriterExec
/// 4. Returns metadata for new files (does NOT commit)
///
/// # Arguments
/// * `compaction_config_json` - JSON string with CompactionTaskConfig
///
/// # Returns
/// * JSON string with NativeCompactionResult
///
/// # Safety
/// This function is called from JNI and expects valid JNI environment and string parameters.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_executeIcebergCompaction(
    mut env: JNIEnv,
    _class: JClass,
    compaction_config_json: JString,
) -> jstring {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Get config JSON from JNI
        let config_json: String = env
            .get_string(&compaction_config_json)
            .map_err(|e| format!("Failed to get config string: {}", e))?
            .into();

        // Parse config
        let config = CompactionTaskConfig::from_json(&config_json)?;

        // Execute compaction
        execute_compaction_internal(&config)
    }));

    let native_result = match result {
        Ok(Ok(compaction_result)) => NativeCompactionResult::success(compaction_result),
        Ok(Err(e)) => NativeCompactionResult::failure(e),
        Err(_) => NativeCompactionResult::failure("Panic during compaction execution".to_string()),
    };

    let result_json = native_result
        .to_json()
        .unwrap_or_else(|e| format!(r#"{{"success":false,"error_message":"{}"}}"#, e));

    env.new_string(&result_json)
        .map(|s| s.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

/// Internal compaction execution using DataFusion to read and write Parquet files
fn execute_compaction_internal(
    config: &CompactionTaskConfig,
) -> Result<IcebergCompactionResult, String> {
    use datafusion::prelude::*;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use uuid::Uuid;

    // Create tokio runtime for async operations
    let rt = Runtime::new().map_err(|e| format!("Failed to create runtime: {}", e))?;

    rt.block_on(async {
        // Build the list of files to delete (input files)
        let files_to_delete: Vec<String> = config
            .file_scan_tasks
            .iter()
            .map(|t| t.file_path.clone())
            .collect();

        if files_to_delete.is_empty() {
            return Ok(IcebergCompactionResult::new());
        }

        // Create DataFusion context
        let ctx = SessionContext::new();

        // Convert file:// URIs to paths for reading
        let file_paths: Vec<String> = files_to_delete
            .iter()
            .map(|p| p.strip_prefix("file://").unwrap_or(p).to_string())
            .collect();

        // Read all input Parquet files into a single DataFrame
        let df = ctx
            .read_parquet(file_paths, ParquetReadOptions::default())
            .await
            .map_err(|e| format!("Failed to read input files: {}", e))?;

        // Collect all data into batches
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Failed to collect batches: {}", e))?;

        if batches.is_empty() {
            return Ok(IcebergCompactionResult::new());
        }

        // Get schema from first batch
        let schema = batches[0].schema();

        // Calculate total rows
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Generate output file path
        let output_filename = format!("{}-compacted.parquet", Uuid::new_v4());
        let table_location = config
            .table_config
            .warehouse_location
            .strip_prefix("file://")
            .unwrap_or(&config.table_config.warehouse_location);
        let output_path = format!(
            "{}/{}/data/{}",
            table_location,
            config.table_config.table_identifier.replace('.', "/"),
            output_filename
        );

        // Ensure data directory exists
        if let Some(parent) = std::path::Path::new(&output_path).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create output directory: {}", e))?;
        }

        // Configure Parquet writer with compression
        let compression = match config.compression.to_lowercase().as_str() {
            "zstd" => Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            "snappy" => Compression::SNAPPY,
            "lz4" => Compression::LZ4,
            "gzip" => Compression::GZIP(Default::default()),
            _ => Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        };

        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        // Write compacted file
        let file = File::create(&output_path)
            .map_err(|e| format!("Failed to create output file: {}", e))?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))
            .map_err(|e| format!("Failed to create Parquet writer: {}", e))?;

        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| format!("Failed to write batch: {}", e))?;
        }

        writer
            .close()
            .map_err(|e| format!("Failed to close writer: {}", e))?;

        // Get file size
        let file_size = std::fs::metadata(&output_path)
            .map(|m| m.len())
            .unwrap_or(0);

        // Build result
        let mut result = IcebergCompactionResult::new();
        result.files_to_delete = files_to_delete;
        result.total_rows = total_rows as u64;
        result.total_bytes_written = file_size;

        // Add the new compacted file
        let data_file = IcebergDataFileMetadata {
            file_path: format!("file://{}", output_path),
            file_format: "parquet".to_string(),
            record_count: total_rows as u64,
            file_size_in_bytes: file_size,
            partition_json: config
                .file_scan_tasks
                .first()
                .map(|t| t.partition_path.clone())
                .unwrap_or_default(),
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            split_offsets: vec![4], // Parquet magic bytes offset
            partition_spec_id: config
                .file_scan_tasks
                .first()
                .map(|t| t.partition_spec_id)
                .unwrap_or(0),
        };
        result.files_to_add.push(data_file);

        Ok(result)
    })
}

/// Get the version of the native Iceberg compaction library
///
/// # Safety
/// This function is called from JNI and expects valid JNI environment.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_getIcebergCompactionVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version = env!("CARGO_PKG_VERSION");
    env.new_string(version)
        .map(|s| s.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_config_serialization() {
        let config = IcebergTableConfig {
            table_identifier: "db.test_table".to_string(),
            metadata_location: "s3://bucket/warehouse/db/test_table/metadata/v1.json".to_string(),
            warehouse_location: "s3://bucket/warehouse".to_string(),
            current_snapshot_id: Some(12345),
            file_io_properties: HashMap::from([(
                "fs.s3a.access.key".to_string(),
                "key".to_string(),
            )]),
        };

        let json = config.to_json().unwrap();
        let parsed = IcebergTableConfig::from_json(&json).unwrap();

        assert_eq!(parsed.table_identifier, config.table_identifier);
        assert_eq!(parsed.current_snapshot_id, config.current_snapshot_id);
    }

    #[test]
    fn test_compaction_task_config() {
        let config = CompactionTaskConfig {
            table_config: IcebergTableConfig {
                table_identifier: "db.table".to_string(),
                metadata_location: "file:///tmp/metadata.json".to_string(),
                warehouse_location: "file:///tmp/warehouse".to_string(),
                current_snapshot_id: None,
                file_io_properties: HashMap::new(),
            },
            file_scan_tasks: vec![FileScanTaskConfig {
                file_path: "file:///tmp/data/part-00000.parquet".to_string(),
                file_size_bytes: 1024,
                record_count: 100,
                partition_path: "".to_string(), // unpartitioned
                partition_spec_id: 0,
                start: 0,
                length: 1024,
            }],
            target_file_size_bytes: 128 * 1024 * 1024,
            compression: "zstd".to_string(),
            data_dir: "data".to_string(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed = CompactionTaskConfig::from_json(&json).unwrap();

        assert_eq!(parsed.file_scan_tasks.len(), 1);
        assert_eq!(parsed.target_file_size_bytes, 128 * 1024 * 1024);
    }

    #[test]
    fn test_native_compaction_result() {
        let mut result = IcebergCompactionResult::new();
        result.files_to_delete = vec!["old1.parquet".to_string(), "old2.parquet".to_string()];
        result.files_to_add.push(IcebergDataFileMetadata::new(
            "new.parquet".to_string(),
            2000,
            2048,
            0,
        ));

        let native_result = NativeCompactionResult::success(result);
        let json = native_result.to_json().unwrap();

        assert!(json.contains("success"));
        assert!(json.contains("old1.parquet"));
    }
}
