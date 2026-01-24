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

//! Parquet writer operator for writing RecordBatches to Parquet files

use std::{
    any::Any,
    collections::HashMap,
    fmt,
    fmt::{Debug, Formatter},
    fs::File,
    io::Cursor,
    sync::Arc,
};

use opendal::Operator;

use crate::execution::shuffle::CompressionCodec;
use crate::parquet::parquet_support::{
    create_hdfs_operator, is_hdfs_scheme, prepare_object_store_with_configs,
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
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
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
use url::Url;

/// Enum representing different types of Arrow writers based on storage backend
enum ParquetWriter {
    /// Writer for local file system
    LocalFile(ArrowWriter<File>),
    /// Writer for HDFS or other remote storage (writes to in-memory buffer)
    /// Contains the arrow writer, HDFS operator, and destination path
    /// an Arrow writer writes to in-memory buffer the data converted to Parquet format
    /// The opendal::Writer is created lazily on first write
    Remote(
        ArrowWriter<Cursor<Vec<u8>>>,
        Option<opendal::Writer>,
        Operator,
        String,
    ),
}

impl ParquetWriter {
    /// Write a RecordBatch to the underlying writer
    async fn write(
        &mut self,
        batch: &RecordBatch,
    ) -> std::result::Result<(), parquet::errors::ParquetError> {
        match self {
            ParquetWriter::LocalFile(writer) => writer.write(batch),
            ParquetWriter::Remote(
                arrow_parquet_buffer_writer,
                hdfs_writer_opt,
                op,
                output_path,
            ) => {
                // Write batch to in-memory buffer
                arrow_parquet_buffer_writer.write(batch)?;

                // Flush and get the current buffer content
                arrow_parquet_buffer_writer.flush()?;
                let cursor = arrow_parquet_buffer_writer.inner_mut();
                let current_data = cursor.get_ref().clone();

                // Create HDFS writer lazily on first write
                if hdfs_writer_opt.is_none() {
                    let writer = op.writer(output_path.as_str()).await.map_err(|e| {
                        parquet::errors::ParquetError::External(
                            format!("Failed to create HDFS writer for '{}': {}", output_path, e)
                                .into(),
                        )
                    })?;
                    *hdfs_writer_opt = Some(writer);
                }

                // Write the accumulated data to HDFS
                if let Some(hdfs_writer) = hdfs_writer_opt {
                    hdfs_writer.write(current_data).await.map_err(|e| {
                        parquet::errors::ParquetError::External(
                            format!(
                                "Failed to write batch to HDFS file '{}': {}",
                                output_path, e
                            )
                            .into(),
                        )
                    })?;
                }

                // Clear the buffer after upload
                cursor.get_mut().clear();
                cursor.set_position(0);

                Ok(())
            }
        }
    }

    /// Close the writer and finalize the file
    async fn close(self) -> std::result::Result<(), parquet::errors::ParquetError> {
        match self {
            ParquetWriter::LocalFile(writer) => {
                writer.close()?;
                Ok(())
            }
            ParquetWriter::Remote(
                arrow_parquet_buffer_writer,
                mut hdfs_writer_opt,
                op,
                output_path,
            ) => {
                // Close the arrow writer to finalize parquet format
                let cursor = arrow_parquet_buffer_writer.into_inner()?;
                let final_data = cursor.into_inner();

                // Create HDFS writer if not already created
                if hdfs_writer_opt.is_none() && !final_data.is_empty() {
                    let writer = op.writer(output_path.as_str()).await.map_err(|e| {
                        parquet::errors::ParquetError::External(
                            format!("Failed to create HDFS writer for '{}': {}", output_path, e)
                                .into(),
                        )
                    })?;
                    hdfs_writer_opt = Some(writer);
                }

                // Write any remaining data
                if !final_data.is_empty() {
                    if let Some(mut hdfs_writer) = hdfs_writer_opt {
                        hdfs_writer.write(final_data).await.map_err(|e| {
                            parquet::errors::ParquetError::External(
                                format!(
                                    "Failed to write final data to HDFS file '{}': {}",
                                    output_path, e
                                )
                                .into(),
                            )
                        })?;

                        // Close the HDFS writer
                        hdfs_writer.close().await.map_err(|e| {
                            parquet::errors::ParquetError::External(
                                format!("Failed to close HDFS writer for '{}': {}", output_path, e)
                                    .into(),
                            )
                        })?;
                    }
                }

                Ok(())
            }
        }
    }
}

/// Parquet writer operator that writes input batches to a Parquet file
#[derive(Debug)]
pub struct ParquetWriterExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Output file path (final destination)
    output_path: String,
    /// Working directory for temporary files (used by FileCommitProtocol)
    work_dir: String,
    /// Job ID for tracking this write operation
    job_id: Option<String>,
    /// Task attempt ID for this specific task
    task_attempt_id: Option<i32>,
    /// Compression codec
    compression: CompressionCodec,
    /// Partition ID (from Spark TaskContext)
    partition_id: i32,
    /// Column names to use in the output Parquet file
    column_names: Vec<String>,
    /// Object store configuration options
    object_store_options: HashMap<String, String>,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache for plan properties
    cache: PlanProperties,
}

impl ParquetWriterExec {
    /// Create a new ParquetWriterExec
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        output_path: String,
        work_dir: String,
        job_id: Option<String>,
        task_attempt_id: Option<i32>,
        compression: CompressionCodec,
        partition_id: i32,
        column_names: Vec<String>,
        object_store_options: HashMap<String, String>,
    ) -> Result<Self> {
        // Preserve the input's partitioning so each partition writes its own file
        let input_partitioning = input.output_partitioning().clone();

        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&input.schema())),
            input_partitioning,
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(ParquetWriterExec {
            input,
            output_path,
            work_dir,
            job_id,
            task_attempt_id,
            compression,
            partition_id,
            column_names,
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

    /// Create an Arrow writer based on the storage scheme
    ///
    /// # Arguments
    /// * `output_file_path` - The full path to the output file
    /// * `schema` - The Arrow schema for the Parquet file
    /// * `props` - Writer properties including compression
    /// * `runtime_env` - Runtime environment for object store registration
    /// * `object_store_options` - Configuration options for object store
    ///
    /// # Returns
    /// * `Ok(ParquetWriter)` - A writer appropriate for the storage scheme
    /// * `Err(DataFusionError)` - If writer creation fails
    fn create_arrow_writer(
        output_file_path: &str,
        schema: SchemaRef,
        props: WriterProperties,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        object_store_options: &HashMap<String, String>,
    ) -> Result<ParquetWriter> {
        // Parse URL and match on storage scheme directly
        let url = Url::parse(output_file_path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to parse URL '{}': {}", output_file_path, e))
        })?;

        if is_hdfs_scheme(&url, object_store_options) {
            // HDFS storage
            {
                // Use prepare_object_store_with_configs to create and register the object store
                let (_object_store_url, object_store_path) = prepare_object_store_with_configs(
                    runtime_env,
                    output_file_path.to_string(),
                    object_store_options,
                )
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to prepare object store for '{}': {}",
                        output_file_path, e
                    ))
                })?;

                // For remote storage (HDFS, S3), write to an in-memory buffer
                let buffer = Vec::new();
                let cursor = Cursor::new(buffer);
                let arrow_parquet_buffer_writer = ArrowWriter::try_new(cursor, schema, Some(props))
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Failed to create HDFS writer: {}", e))
                    })?;

                // Create HDFS operator with configuration options using the helper function
                let op = create_hdfs_operator(&url).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to create HDFS operator for '{}': {}",
                        output_file_path, e
                    ))
                })?;

                // HDFS writer will be created lazily on first write
                // Use the path from prepare_object_store_with_configs
                Ok(ParquetWriter::Remote(
                    arrow_parquet_buffer_writer,
                    None,
                    op,
                    object_store_path.to_string(),
                ))
            }
        } else if output_file_path.starts_with("file://")
            || output_file_path.starts_with("file:")
            || !output_file_path.contains("://")
        {
            // Local file system
            {
                // For a local file system, write directly to file
                // Strip file:// or file: prefix if present
                let local_path = output_file_path
                    .strip_prefix("file://")
                    .or_else(|| output_file_path.strip_prefix("file:"))
                    .unwrap_or(output_file_path);

                // Extract the parent directory from the file path
                let output_dir = std::path::Path::new(local_path).parent().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Failed to extract parent directory from path '{}'",
                        local_path
                    ))
                })?;

                // Create the parent directory if it doesn't exist
                std::fs::create_dir_all(output_dir).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to create output directory '{}': {}",
                        output_dir.display(),
                        e
                    ))
                })?;

                let file = File::create(local_path).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to create output file '{}': {}",
                        local_path, e
                    ))
                })?;

                let writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to create local file writer: {}", e))
                })?;
                Ok(ParquetWriter::LocalFile(writer))
            }
        } else {
            // Unsupported storage scheme
            Err(DataFusionError::Execution(format!(
                "Unsupported storage scheme in path: {}",
                output_file_path
            )))
        }
    }
}

impl DisplayAs for ParquetWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ParquetWriterExec: path={}, compression={:?}",
                    self.output_path, self.compression
                )
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for ParquetWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ParquetWriterExec"
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
            1 => Ok(Arc::new(ParquetWriterExec::try_new(
                Arc::clone(&children[0]),
                self.output_path.clone(),
                self.work_dir.clone(),
                self.job_id.clone(),
                self.task_attempt_id,
                self.compression.clone(),
                self.partition_id,
                self.column_names.clone(),
                self.object_store_options.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "ParquetWriterExec requires exactly one child".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        use datafusion::physical_plan::metrics::MetricBuilder;

        // Create metrics for tracking write statistics
        let files_written = MetricBuilder::new(&self.metrics).counter("files_written", partition);
        let bytes_written = MetricBuilder::new(&self.metrics).counter("bytes_written", partition);
        let rows_written = MetricBuilder::new(&self.metrics).counter("rows_written", partition);

        let runtime_env = context.runtime_env();
        let input = self.input.execute(partition, context)?;
        let input_schema = self.input.schema();
        let work_dir = self.work_dir.clone();
        let task_attempt_id = self.task_attempt_id;
        let compression = self.compression_to_parquet()?;
        let column_names = self.column_names.clone();

        assert_eq!(input_schema.fields().len(), column_names.len());

        // Replace the generic column names (col_0, col_1, etc.) with the actual names
        let fields: Vec<_> = input_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| Arc::new(field.as_ref().clone().with_name(&column_names[i])))
            .collect();
        let output_schema = Arc::new(arrow::datatypes::Schema::new(fields));

        // Generate part file name for this partition
        // If using FileCommitProtocol (work_dir is set), include task_attempt_id in the filename
        let part_file = if let Some(attempt_id) = task_attempt_id {
            format!(
                "{}/part-{:05}-{:05}.parquet",
                work_dir, self.partition_id, attempt_id
            )
        } else {
            format!("{}/part-{:05}.parquet", work_dir, self.partition_id)
        };

        // Configure writer properties
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        let object_store_options = self.object_store_options.clone();
        let mut writer = Self::create_arrow_writer(
            &part_file,
            Arc::clone(&output_schema),
            props,
            runtime_env,
            &object_store_options,
        )?;

        // Clone schema for use in async closure
        let schema_for_write = Arc::clone(&output_schema);

        // Write batches
        let write_task = async move {
            let mut stream = input;
            let mut total_rows = 0i64;

            while let Some(batch_result) = stream.try_next().await.transpose() {
                let batch = batch_result?;

                // Track row count
                total_rows += batch.num_rows() as i64;

                // Rename columns in the batch to match output schema
                let renamed_batch = if !column_names.is_empty() {
                    RecordBatch::try_new(Arc::clone(&schema_for_write), batch.columns().to_vec())
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to rename batch columns: {}",
                                e
                            ))
                        })?
                } else {
                    batch
                };

                writer.write(&renamed_batch).await.map_err(|e| {
                    DataFusionError::Execution(format!("Failed to write batch: {}", e))
                })?;
            }

            writer.close().await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to close writer: {}", e))
            })?;

            // Get file size - strip file:// prefix if present for local filesystem access
            let local_path = part_file
                .strip_prefix("file://")
                .or_else(|| part_file.strip_prefix("file:"))
                .unwrap_or(&part_file);
            let file_size = std::fs::metadata(local_path)
                .map(|m| m.len() as i64)
                .unwrap_or(0);

            // Update metrics with write statistics
            files_written.add(1);
            bytes_written.add(file_size as usize);
            rows_written.add(total_rows as usize);

            // Log metadata for debugging
            eprintln!(
                "Wrote Parquet file: path={}, size={}, rows={}",
                part_file, file_size, total_rows
            );

            // Return empty stream to indicate completion
            Ok::<_, DataFusionError>(futures::stream::empty())
        };

        // Execute the write task and create a stream that does not return any batches
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(write_task).try_flatten(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Helper function to create a test RecordBatch with 1000 rows of (int, string) data
    /// Example batch_id 1 -> 0..1000, 2 -> 1001..2000
    fn create_test_record_batch(batch_id: i32) -> Result<RecordBatch> {
        assert!(batch_id > 0, "batch_id must be greater than 0");
        let num_rows = batch_id * 1000;

        let int_array = Int32Array::from_iter_values(((batch_id - 1) * 1000)..num_rows);

        let string_values: Vec<String> = (((batch_id - 1) * 1000)..num_rows)
            .map(|i| format!("value_{}", i))
            .collect();
        let string_array = StringArray::from(string_values);

        // Define schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create RecordBatch
        RecordBatch::try_new(schema, vec![Arc::new(int_array), Arc::new(string_array)])
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    #[tokio::test]
    #[cfg(feature = "hdfs-opendal")]
    #[ignore = "This test requires a running HDFS cluster"]
    async fn test_write_to_hdfs_sync() -> Result<()> {
        use opendal::services::Hdfs;
        use opendal::Operator;

        // Configure HDFS connection
        let namenode = "hdfs://namenode:9000";
        let output_path = "/user/test_write/data.parquet";

        // Create OpenDAL HDFS operator
        let builder = Hdfs::default().name_node(namenode);
        let op = Operator::new(builder)
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to create HDFS operator: {}", e))
            })?
            .finish();

        let mut hdfs_writer = op.writer(output_path).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to create HDFS writer: {}", e))
        })?;

        let mut buffer = Cursor::new(Vec::new());
        let mut writer =
            ArrowWriter::try_new(&mut buffer, create_test_record_batch(1)?.schema(), None)?;

        for i in 1..=5 {
            let record_batch = create_test_record_batch(i)?;

            writer.write(&record_batch)?;

            println!(
                "Successfully wrote 1000 rows to HDFS at {}{}",
                namenode, output_path
            );
        }

        writer.close()?;

        hdfs_writer.write(buffer.into_inner()).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to write with HDFS writer: {}", e))
        })?;

        hdfs_writer.close().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to close HDFS writer: {}", e))
        })?;

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "hdfs-opendal")]
    #[ignore = "This test requires a running HDFS cluster"]
    async fn test_write_to_hdfs_streaming() -> Result<()> {
        use opendal::services::Hdfs;
        use opendal::Operator;

        // Configure HDFS connection
        let namenode = "hdfs://namenode:9000";
        let output_path = "/user/test_write_streaming/data.parquet";

        // Create OpenDAL HDFS operator
        let builder = Hdfs::default().name_node(namenode);
        let op = Operator::new(builder)
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to create HDFS operator: {}", e))
            })?
            .finish();

        // Create a single HDFS writer for the entire file
        let mut hdfs_writer = op.writer(output_path).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to create HDFS writer: {}", e))
        })?;

        // Create a single ArrowWriter that will be used for all batches
        let buffer = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(buffer, create_test_record_batch(1)?.schema(), None)?;

        // Write each batch and upload to HDFS immediately (streaming approach)
        for i in 1..=5 {
            let record_batch = create_test_record_batch(i)?;

            // Write the batch to the parquet writer
            writer.write(&record_batch)?;

            // Flush the writer to ensure data is written to the buffer
            writer.flush()?;

            // Get the current buffer content through the writer
            let cursor = writer.inner_mut();
            let current_data = cursor.get_ref().clone();

            // Write the accumulated data to HDFS
            hdfs_writer.write(current_data).await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to write batch {} to HDFS: {}", i, e))
            })?;

            // Clear the buffer for the next iteration
            cursor.get_mut().clear();
            cursor.set_position(0);

            println!(
                "Successfully streamed batch {} (1000 rows) to HDFS at {}{}",
                i, namenode, output_path
            );
        }

        // Close the ArrowWriter to finalize the parquet file
        let cursor = writer.into_inner()?;

        // Write any remaining data from closing the writer
        let final_data = cursor.into_inner();
        if !final_data.is_empty() {
            hdfs_writer.write(final_data).await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to write final data to HDFS: {}", e))
            })?;
        }

        // Close the HDFS writer
        hdfs_writer.close().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to close HDFS writer: {}", e))
        })?;

        println!(
            "Successfully completed streaming write of 5 batches (5000 total rows) to HDFS at {}{}",
            namenode, output_path
        );

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "hdfs-opendal")]
    #[ignore = "This test requires a running HDFS cluster"]
    async fn test_parquet_writer_streaming() -> Result<()> {
        // Configure output path
        let output_path = "/user/test_parquet_writer_streaming/data.parquet";

        // Configure writer properties
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();

        // Create ParquetWriter using the create_arrow_writer method
        // Use full HDFS URL format
        let full_output_path = format!("hdfs://namenode:9000{}", output_path);
        let session_ctx = datafusion::prelude::SessionContext::new();
        let runtime_env = session_ctx.runtime_env();
        let mut writer = ParquetWriterExec::create_arrow_writer(
            &full_output_path,
            create_test_record_batch(1)?.schema(),
            props,
            runtime_env,
            &HashMap::new(),
        )?;

        // Write 5 batches in a loop
        for i in 1..=5 {
            let record_batch = create_test_record_batch(i)?;

            writer.write(&record_batch).await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to write batch {}: {}", i, e))
            })?;

            println!(
                "Successfully wrote batch {} (1000 rows) using ParquetWriter",
                i
            );
        }

        // Close the writer
        writer
            .close()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to close writer: {}", e)))?;

        println!(
            "Successfully completed ParquetWriter streaming write of 5 batches (5000 total rows) to HDFS at {}",
            output_path
        );

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "hdfs-opendal")]
    #[ignore = "This test requires a running HDFS cluster"]
    async fn test_parquet_writer_exec_with_memory_input() -> Result<()> {
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::prelude::SessionContext;

        // Create 5 batches for the DataSourceExec input
        let mut batches = Vec::new();
        for i in 1..=5 {
            batches.push(create_test_record_batch(i)?);
        }

        // Get schema from the first batch
        let schema = batches[0].schema();

        // Create DataSourceExec with MemorySourceConfig containing the 5 batches as a single partition
        let partitions = vec![batches];
        let memory_source_config = MemorySourceConfig::try_new(&partitions, schema, None)?;
        let memory_exec = Arc::new(DataSourceExec::new(Arc::new(memory_source_config)));

        // Create ParquetWriterExec with DataSourceExec as input
        let output_path = "unused".to_string();
        let work_dir = "hdfs://namenode:9000/user/test_parquet_writer_exec".to_string();
        let column_names = vec!["id".to_string(), "name".to_string()];

        let parquet_writer = ParquetWriterExec::try_new(
            memory_exec,
            output_path,
            work_dir,
            None,      // job_id
            Some(123), // task_attempt_id
            CompressionCodec::None,
            0, // partition_id
            column_names,
            HashMap::new(), // object_store_options
        )?;

        // Create a session context and execute the plan
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // Execute partition 0
        let mut stream = parquet_writer.execute(0, task_ctx)?;

        // Consume the stream (this triggers the write)
        while let Some(batch_result) = stream.try_next().await? {
            // The stream should be empty as ParquetWriterExec returns empty batches
            assert_eq!(batch_result.num_rows(), 0);
        }

        println!(
            "Successfully completed ParquetWriterExec test with DataSourceExec input (5 batches, 5000 total rows)"
        );

        Ok(())
    }
}
