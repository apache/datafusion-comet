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
    fmt,
    fmt::{Debug, Formatter},
    fs::File,
    io::Cursor,
    sync::Arc,
};

use bytes::Bytes;
use url::Url;

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

use crate::execution::shuffle::CompressionCodec;
use crate::parquet::parquet_support::write_to_hdfs_with_opendal_async;

#[cfg(all(test, feature = "hdfs-opendal"))]
use crate::parquet::parquet_support::write_record_batch_to_hdfs;

/// Enum representing different types of Arrow writers based on storage backend
enum ParquetWriter {
    /// Writer for local file system
    LocalFile(ArrowWriter<File>),
    /// Writer for HDFS or other remote storage (writes to in-memory buffer)
    /// Contains the writer and the destination HDFS path
    Remote(ArrowWriter<Cursor<Vec<u8>>>, String),
}

impl ParquetWriter {
    /// Write a RecordBatch to the underlying writer
    async fn write(
        &mut self,
        batch: &RecordBatch,
    ) -> std::result::Result<(), parquet::errors::ParquetError> {
        match self {
            ParquetWriter::LocalFile(writer) => writer.write(batch),
            ParquetWriter::Remote(writer, output_path) => {
                // Write batch to in-memory buffer
                writer.write(batch)?;

                // Flush and get the current buffer content
                writer.flush()?;
                let cursor = writer.inner_mut();
                let buffer = cursor.get_ref().clone();

                // Upload
                let url = Url::parse(output_path).map_err(|e| {
                    parquet::errors::ParquetError::General(format!(
                        "Failed to parse URL '{}': {}",
                        output_path, e
                    ))
                })?;

                write_to_hdfs_with_opendal_async(&url, Bytes::from(buffer))
                    .await
                    .map_err(|e| {
                        parquet::errors::ParquetError::General(format!(
                            "Failed to upload to '{}': {}",
                            output_path, e
                        ))
                    })?;

                // Clear the buffer after upload
                cursor.get_mut().clear();
                cursor.set_position(0);

                Ok(())
            }
        }
    }

    /// Close the writer and return the buffer for remote writers
    fn close(
        self,
    ) -> std::result::Result<Option<(Vec<u8>, String)>, parquet::errors::ParquetError> {
        match self {
            ParquetWriter::LocalFile(writer) => {
                writer.close()?;
                Ok(None)
            }
            ParquetWriter::Remote(writer, path) => {
                let cursor = writer.into_inner()?;
                Ok(Some((cursor.into_inner(), path)))
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
    /// * `storage_scheme` - The storage backend ("hdfs", "s3", or "local")
    /// * `output_file_path` - The full path to the output file
    /// * `schema` - The Arrow schema for the Parquet file
    /// * `props` - Writer properties including compression
    ///
    /// # Returns
    /// * `Ok(ParquetWriter)` - A writer appropriate for the storage scheme
    /// * `Err(DataFusionError)` - If writer creation fails
    fn create_arrow_writer(
        storage_scheme: &str,
        output_file_path: &str,
        schema: SchemaRef,
        props: WriterProperties,
    ) -> Result<ParquetWriter> {
        match storage_scheme {
            "hdfs" | "s3" => {
                // For remote storage (HDFS, S3), write to an in-memory buffer
                let buffer = Vec::new();
                let cursor = Cursor::new(buffer);
                let writer = ArrowWriter::try_new(cursor, schema, Some(props)).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to create {} writer: {}",
                        storage_scheme, e
                    ))
                })?;
                Ok(ParquetWriter::Remote(writer, output_file_path.to_string()))
            }
            "local" => {
                // For local file system, write directly to file
                // Strip file:// or file: prefix if present
                let local_path = output_file_path
                    .strip_prefix("file://")
                    .or_else(|| output_file_path.strip_prefix("file:"))
                    .unwrap_or(output_file_path);

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
            _ => Err(DataFusionError::Execution(format!(
                "Unsupported storage scheme: {}",
                storage_scheme
            ))),
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

        // Determine storage scheme from work_dir
        let storage_scheme = if work_dir.starts_with("hdfs://") {
            "hdfs"
        } else if work_dir.starts_with("s3://") || work_dir.starts_with("s3a://") {
            "s3"
        } else {
            "local"
        };

        // Strip file:// or file: prefix if present
        let local_path = work_dir
            .strip_prefix("file://")
            .or_else(|| work_dir.strip_prefix("file:"))
            .unwrap_or(&work_dir)
            .to_string();

        // Create output directory
        std::fs::create_dir_all(&local_path).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create output directory '{}': {}",
                local_path, e
            ))
        })?;

        // Generate part file name for this partition
        // If using FileCommitProtocol (work_dir is set), include task_attempt_id in the filename
        let part_file = if let Some(attempt_id) = task_attempt_id {
            format!(
                "{}/part-{:05}-{:05}.parquet",
                local_path, self.partition_id, attempt_id
            )
        } else {
            format!("{}/part-{:05}.parquet", local_path, self.partition_id)
        };

        // Configure writer properties
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        let mut writer = Self::create_arrow_writer(
            storage_scheme,
            &part_file,
            Arc::clone(&output_schema),
            props,
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

            writer.close().map_err(|e| {
                DataFusionError::Execution(format!("Failed to close writer: {}", e))
            })?;

            // Get file size
            let file_size = std::fs::metadata(&part_file)
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
    fn create_test_record_batch() -> Result<RecordBatch> {
        let num_rows = 1000;
        
        // Create int column with values 0..1000
        let int_array = Int32Array::from_iter_values(0..num_rows);
        
        // Create string column with values "value_0", "value_1", ..., "value_999"
        let string_values: Vec<String> = (0..num_rows)
            .map(|i| format!("value_{}", i))
            .collect();
        let string_array = StringArray::from(string_values);
        
        // Define schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        
        // Create RecordBatch
        RecordBatch::try_new(
            schema,
            vec![Arc::new(int_array), Arc::new(string_array)],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    #[tokio::test]
    //#[cfg(feature = "hdfs-opendal")]
    async fn test_write_to_hdfs() -> Result<()> {
        use opendal::services::Hdfs;
        use opendal::Operator;
        
        // Create test data
        let batch = create_test_record_batch()?;
        
        // Configure HDFS connection
        let namenode = "hdfs://namenode:9000";
        let output_path = "/user/test_write/data.parquet";
        
        // Create OpenDAL HDFS operator
        let builder = Hdfs::default().name_node(namenode);
        let op = Operator::new(builder)
            .map_err(|e| DataFusionError::Execution(format!("Failed to create HDFS operator: {}", e)))?
            .finish();
        
        // Write the batch using write_record_batch_to_hdfs
        write_record_batch_to_hdfs(&op, output_path, batch)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to write to HDFS: {}", e)))?;
        
        println!("Successfully wrote 1000 rows to HDFS at {}{}", namenode, output_path);
        
        Ok(())
    }  
}
