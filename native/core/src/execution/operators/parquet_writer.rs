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
    sync::Arc,
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::{
    array::{Int64Array, StringArray},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
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

/// Parquet writer operator that writes input batches to a Parquet file
#[derive(Debug)]
pub struct ParquetWriterExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Output file path (final destination)
    output_path: String,
    /// Working directory for temporary files (used by FileCommitProtocol)
    /// If None, files are written directly to output_path
    work_dir: Option<String>,
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
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        output_path: String,
        work_dir: Option<String>,
        job_id: Option<String>,
        task_attempt_id: Option<i32>,
        compression: CompressionCodec,
        partition_id: i32,
        column_names: Vec<String>,
    ) -> Result<Self> {
        // Preserve the input's partitioning so each partition writes its own file
        let input_partitioning = input.output_partitioning().clone();

        let cache = PlanProperties::new(
            EquivalenceProperties::new(Self::metadata_schema()),
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

    /// Schema for the metadata returned by this operator
    /// Returns: (file_path: Utf8, size_bytes: Int64, num_rows: Int64)
    fn metadata_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("size_bytes", DataType::Int64, false),
            Field::new("num_rows", DataType::Int64, false),
        ]))
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
        Self::metadata_schema()
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
        let input = self.input.execute(partition, context)?;
        let input_schema = self.schema();
        let output_path = self.output_path.clone();
        let work_dir = self.work_dir.clone();
        let task_attempt_id = self.task_attempt_id;
        let compression = self.compression_to_parquet()?;
        let column_names = self.column_names.clone();

        assert_eq!(input_schema.fields().len(), column_names.len());

        // Create output schema with correct column names
        let output_schema = if !column_names.is_empty() {
            // Replace the generic column names (col_0, col_1, etc.) with the actual names
            let fields: Vec<_> = input_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| Arc::new(field.as_ref().clone().with_name(&column_names[i])))
                .collect();
            Arc::new(arrow::datatypes::Schema::new(fields))
        } else {
            // No column names provided, use input schema as-is
            Arc::clone(&input_schema)
        };

        // Determine the write path (work_dir for temp files, or output_path for direct write)
        let write_base_path = if let Some(ref work_dir) = work_dir {
            work_dir.clone()
        } else {
            output_path.clone()
        };

        // Strip file:// or file: prefix if present
        let local_path = write_base_path
            .strip_prefix("file://")
            .or_else(|| write_base_path.strip_prefix("file:"))
            .unwrap_or(&write_base_path)
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

        // Create the Parquet file
        let file = File::create(&part_file).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create output file '{}': {}",
                part_file, e
            ))
        })?;

        // Configure writer properties
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        let mut writer = ArrowWriter::try_new(file, Arc::clone(&output_schema), Some(props))
            .map_err(|e| DataFusionError::Execution(format!("Failed to create writer: {}", e)))?;

        // Clone schema for use in async closure
        let schema_for_write = Arc::clone(&output_schema);
        let metadata_schema = Self::metadata_schema();

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

                writer.write(&renamed_batch).map_err(|e| {
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

            // Create metadata RecordBatch to return to JVM
            let file_path_array = StringArray::from(vec![part_file.clone()]);
            let size_bytes_array = Int64Array::from(vec![file_size]);
            let num_rows_array = Int64Array::from(vec![total_rows]);

            let metadata_batch = RecordBatch::try_new(
                metadata_schema,
                vec![
                    Arc::new(file_path_array),
                    Arc::new(size_bytes_array),
                    Arc::new(num_rows_array),
                ],
            )
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to create metadata batch: {}", e))
            })?;

            // Return a stream containing the metadata batch
            Ok::<_, DataFusionError>(futures::stream::once(async { Ok(metadata_batch) }))
        };

        // Execute the write task and convert to a stream
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Self::metadata_schema(),
            futures::stream::once(write_task).try_flatten(),
        )))
    }
}
