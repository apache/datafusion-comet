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

//! Iceberg Parquet writer operator for writing RecordBatches to Parquet files
//! with Iceberg-compatible metadata (DataFile structures).

use std::{
    any::Any,
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    fs::File,
    sync::Arc,
};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream, Statistics,
    },
};
use futures::TryStreamExt;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::execution::shuffle::CompressionCodec;

/// Metadata for an Iceberg DataFile, returned after writing.
/// This structure mirrors iceberg-rust's DataFile but is serializable for JNI transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergDataFileMetadata {
    /// Full URI for the file with FS scheme
    pub file_path: String,
    /// File format (always "parquet" for this writer)
    pub file_format: String,
    /// Number of records in this file
    pub record_count: u64,
    /// Total file size in bytes
    pub file_size_in_bytes: u64,
    /// Partition values as JSON (empty for unpartitioned tables)
    pub partition_json: String,
    /// Map from column id to the total size on disk
    pub column_sizes: HashMap<i32, u64>,
    /// Map from column id to number of values
    pub value_counts: HashMap<i32, u64>,
    /// Map from column id to number of null values
    pub null_value_counts: HashMap<i32, u64>,
    /// Split offsets (row group offsets in Parquet)
    pub split_offsets: Vec<i64>,
    /// Partition spec ID
    pub partition_spec_id: i32,
}

impl IcebergDataFileMetadata {
    /// Create a new IcebergDataFileMetadata
    pub fn new(
        file_path: String,
        record_count: u64,
        file_size_in_bytes: u64,
        partition_spec_id: i32,
    ) -> Self {
        Self {
            file_path,
            file_format: "parquet".to_string(),
            record_count,
            file_size_in_bytes,
            partition_json: "{}".to_string(),
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            split_offsets: Vec::new(),
            partition_spec_id,
        }
    }

    /// Set partition values from JSON
    pub fn with_partition_json(mut self, partition_json: String) -> Self {
        self.partition_json = partition_json;
        self
    }

    /// Set column sizes
    pub fn with_column_sizes(mut self, column_sizes: HashMap<i32, u64>) -> Self {
        self.column_sizes = column_sizes;
        self
    }

    /// Set value counts
    pub fn with_value_counts(mut self, value_counts: HashMap<i32, u64>) -> Self {
        self.value_counts = value_counts;
        self
    }

    /// Set null value counts
    pub fn with_null_value_counts(mut self, null_value_counts: HashMap<i32, u64>) -> Self {
        self.null_value_counts = null_value_counts;
        self
    }

    /// Set split offsets
    pub fn with_split_offsets(mut self, split_offsets: Vec<i64>) -> Self {
        self.split_offsets = split_offsets;
        self
    }

    /// Serialize to JSON for JNI transport
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| {
            DataFusionError::Execution(format!("Failed to serialize DataFileMetadata: {}", e))
        })
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| {
            DataFusionError::Execution(format!("Failed to deserialize DataFileMetadata: {}", e))
        })
    }
}

/// Iceberg Parquet writer operator that writes input batches to Parquet files
/// and produces DataFile metadata for Iceberg table commits.
#[derive(Debug)]
pub struct IcebergParquetWriterExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Table location (warehouse path)
    table_location: String,
    /// Data directory within table location
    data_dir: String,
    /// Compression codec
    compression: CompressionCodec,
    /// Target file size in bytes (for splitting large writes)
    target_file_size_bytes: u64,
    /// Partition spec ID
    partition_spec_id: i32,
    /// Column names for the output schema
    column_names: Vec<String>,
    /// Column IDs for Iceberg schema (maps to column_names)
    column_ids: Vec<i32>,
    /// Object store configuration options
    object_store_options: HashMap<String, String>,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache for plan properties
    cache: PlanProperties,
}

impl IcebergParquetWriterExec {
    /// Create a new IcebergParquetWriterExec
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        table_location: String,
        data_dir: String,
        compression: CompressionCodec,
        target_file_size_bytes: u64,
        partition_spec_id: i32,
        column_names: Vec<String>,
        column_ids: Vec<i32>,
        object_store_options: HashMap<String, String>,
    ) -> Result<Self> {
        let input_partitioning = input.output_partitioning().clone();

        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&input.schema())),
            input_partitioning,
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(IcebergParquetWriterExec {
            input,
            table_location,
            data_dir,
            compression,
            target_file_size_bytes,
            partition_spec_id,
            column_names,
            column_ids,
            object_store_options,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn compression_to_parquet(&self) -> Result<Compression> {
        match self.compression {
            CompressionCodec::None => Ok(Compression::UNCOMPRESSED),
            CompressionCodec::Zstd(level) => Ok(Compression::ZSTD(ZstdLevel::try_new(level)?)),
            CompressionCodec::Lz4Frame => Ok(Compression::LZ4),
            CompressionCodec::Snappy => Ok(Compression::SNAPPY),
        }
    }
}

impl DisplayAs for IcebergParquetWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergParquetWriterExec: table_location={}, compression={:?}, target_file_size={}",
                    self.table_location, self.compression, self.target_file_size_bytes
                )
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for IcebergParquetWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "IcebergParquetWriterExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.partition_statistics(None)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(IcebergParquetWriterExec::try_new(
                Arc::clone(&children[0]),
                self.table_location.clone(),
                self.data_dir.clone(),
                self.compression.clone(),
                self.target_file_size_bytes,
                self.partition_spec_id,
                self.column_names.clone(),
                self.column_ids.clone(),
                self.object_store_options.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "IcebergParquetWriterExec requires exactly one child".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let files_written = MetricBuilder::new(&self.metrics).counter("files_written", partition);
        let bytes_written = MetricBuilder::new(&self.metrics).counter("bytes_written", partition);
        let rows_written = MetricBuilder::new(&self.metrics).counter("rows_written", partition);

        let input = self.input.execute(partition, context)?;
        let input_schema = self.input.schema();
        let compression = self.compression_to_parquet()?;
        let column_names = self.column_names.clone();
        let column_ids = self.column_ids.clone();
        let table_location = self.table_location.clone();
        let data_dir = self.data_dir.clone();
        let target_file_size_bytes = self.target_file_size_bytes;
        let partition_spec_id = self.partition_spec_id;

        assert_eq!(input_schema.fields().len(), column_names.len());

        // Build output schema with correct column names
        let fields: Vec<_> = input_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| Arc::new(field.as_ref().clone().with_name(&column_names[i])))
            .collect();
        let output_schema = Arc::new(Schema::new(fields));

        let write_task = async move {
            let mut stream = input;
            let mut total_rows = 0u64;
            let mut total_bytes = 0u64;
            let mut file_index = 0usize;
            let mut data_files: Vec<IcebergDataFileMetadata> = Vec::new();

            // Current file state
            let mut current_file_path: Option<String> = None;
            let mut current_writer: Option<ArrowWriter<File>> = None;
            let mut current_file_rows = 0u64;
            let mut current_value_counts: HashMap<i32, u64> = HashMap::new();
            let mut current_null_counts: HashMap<i32, u64> = HashMap::new();

            // Helper to finalize current file
            let finalize_file = |writer: ArrowWriter<File>,
                                 file_path: &str,
                                 file_rows: u64,
                                 value_counts: HashMap<i32, u64>,
                                 null_counts: HashMap<i32, u64>,
                                 partition_spec_id: i32|
             -> Result<IcebergDataFileMetadata> {
                writer.close().map_err(|e| {
                    DataFusionError::Execution(format!("Failed to close writer: {}", e))
                })?;

                let local_path = file_path
                    .strip_prefix("file://")
                    .or_else(|| file_path.strip_prefix("file:"))
                    .unwrap_or(file_path);
                let file_size = std::fs::metadata(local_path).map(|m| m.len()).unwrap_or(0);

                let metadata = IcebergDataFileMetadata::new(
                    file_path.to_string(),
                    file_rows,
                    file_size,
                    partition_spec_id,
                )
                .with_value_counts(value_counts)
                .with_null_value_counts(null_counts);

                Ok(metadata)
            };

            while let Some(batch_result) = stream.try_next().await.transpose() {
                let batch = batch_result?;
                let batch_rows = batch.num_rows() as u64;

                // Check if we need to start a new file
                let need_new_file = current_writer.is_none()
                    || (target_file_size_bytes > 0 && current_file_rows >= target_file_size_bytes);

                if need_new_file {
                    // Finalize current file if exists
                    if let (Some(writer), Some(ref path)) =
                        (current_writer.take(), current_file_path.take())
                    {
                        let metadata = finalize_file(
                            writer,
                            path,
                            current_file_rows,
                            std::mem::take(&mut current_value_counts),
                            std::mem::take(&mut current_null_counts),
                            partition_spec_id,
                        )?;
                        total_bytes += metadata.file_size_in_bytes;
                        data_files.push(metadata);
                    }

                    // Start new file
                    file_index += 1;
                    let uuid = Uuid::now_v7();
                    let new_file_path = format!(
                        "{}/{}/{:05}-{:05}-{}.parquet",
                        table_location, data_dir, partition, file_index, uuid
                    );

                    let local_path = new_file_path
                        .strip_prefix("file://")
                        .or_else(|| new_file_path.strip_prefix("file:"))
                        .unwrap_or(&new_file_path);

                    if let Some(parent) = std::path::Path::new(local_path).parent() {
                        std::fs::create_dir_all(parent).map_err(|e| {
                            DataFusionError::Execution(format!("Failed to create directory: {}", e))
                        })?;
                    }

                    let file = File::create(local_path).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to create file: {}", e))
                    })?;

                    let props = WriterProperties::builder()
                        .set_compression(compression)
                        .build();

                    let writer =
                        ArrowWriter::try_new(file, Arc::clone(&output_schema), Some(props))
                            .map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to create writer: {}",
                                    e
                                ))
                            })?;

                    current_writer = Some(writer);
                    current_file_path = Some(new_file_path);
                    current_file_rows = 0;
                }

                // Rename columns in batch
                let renamed_batch =
                    RecordBatch::try_new(Arc::clone(&output_schema), batch.columns().to_vec())
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to rename batch columns: {}",
                                e
                            ))
                        })?;

                // Update column statistics
                for (i, col_id) in column_ids.iter().enumerate() {
                    let col = renamed_batch.column(i);
                    let null_count = col.null_count() as u64;
                    let value_count = col.len() as u64;

                    *current_value_counts.entry(*col_id).or_insert(0) += value_count;
                    *current_null_counts.entry(*col_id).or_insert(0) += null_count;
                }

                // Write batch
                if let Some(ref mut writer) = current_writer {
                    writer.write(&renamed_batch).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to write batch: {}", e))
                    })?;
                }

                current_file_rows += batch_rows;
                total_rows += batch_rows;
            }

            // Finalize last file
            if let (Some(writer), Some(ref path)) =
                (current_writer.take(), current_file_path.take())
            {
                let metadata = finalize_file(
                    writer,
                    path,
                    current_file_rows,
                    current_value_counts,
                    current_null_counts,
                    partition_spec_id,
                )?;
                total_bytes += metadata.file_size_in_bytes;
                data_files.push(metadata);
            }

            // Update metrics
            files_written.add(data_files.len());
            bytes_written.add(total_bytes as usize);
            rows_written.add(total_rows as usize);

            eprintln!(
                "IcebergParquetWriter: wrote {} files, {} bytes, {} rows",
                data_files.len(),
                total_bytes,
                total_rows
            );

            Ok::<_, DataFusionError>(futures::stream::empty())
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(write_task).try_flatten(),
        )))
    }
}

/// Result of Iceberg compaction operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergCompactionResult {
    /// Files that were deleted (old fragmented files)
    pub files_to_delete: Vec<String>,
    /// New compacted files with metadata
    pub files_to_add: Vec<IcebergDataFileMetadata>,
    /// Total rows processed
    pub total_rows: u64,
    /// Total bytes written
    pub total_bytes_written: u64,
}

impl IcebergCompactionResult {
    /// Create a new compaction result
    pub fn new() -> Self {
        Self {
            files_to_delete: Vec::new(),
            files_to_add: Vec::new(),
            total_rows: 0,
            total_bytes_written: 0,
        }
    }

    /// Serialize to JSON for JNI transport
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| {
            DataFusionError::Execution(format!("Failed to serialize CompactionResult: {}", e))
        })
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| {
            DataFusionError::Execution(format!("Failed to deserialize CompactionResult: {}", e))
        })
    }
}

impl Default for IcebergCompactionResult {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_file_metadata_serialization() {
        let metadata = IcebergDataFileMetadata::new(
            "s3://bucket/table/data/part-00000.parquet".to_string(),
            1000,
            1024 * 1024,
            0,
        )
        .with_partition_json(r#"{"date": "2024-01-01"}"#.to_string());

        let json = metadata.to_json().unwrap();
        let deserialized = IcebergDataFileMetadata::from_json(&json).unwrap();

        assert_eq!(deserialized.file_path, metadata.file_path);
        assert_eq!(deserialized.record_count, metadata.record_count);
        assert_eq!(deserialized.file_size_in_bytes, metadata.file_size_in_bytes);
        assert_eq!(deserialized.partition_json, metadata.partition_json);
    }

    #[test]
    fn test_compaction_result_serialization() {
        let mut result = IcebergCompactionResult::new();
        result.files_to_delete = vec![
            "s3://bucket/table/data/old-1.parquet".to_string(),
            "s3://bucket/table/data/old-2.parquet".to_string(),
        ];
        result.files_to_add.push(IcebergDataFileMetadata::new(
            "s3://bucket/table/data/compacted.parquet".to_string(),
            2000,
            2 * 1024 * 1024,
            0,
        ));
        result.total_rows = 2000;
        result.total_bytes_written = 2 * 1024 * 1024;

        let json = result.to_json().unwrap();
        let deserialized = IcebergCompactionResult::from_json(&json).unwrap();

        assert_eq!(deserialized.files_to_delete.len(), 2);
        assert_eq!(deserialized.files_to_add.len(), 1);
        assert_eq!(deserialized.total_rows, 2000);
    }
}
