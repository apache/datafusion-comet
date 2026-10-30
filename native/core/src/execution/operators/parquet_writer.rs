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
    collections::HashMap,
    fmt,
    fmt::{Debug, Formatter},
    fs::File,
    io::Write,
    sync::Arc,
};

#[cfg(feature = "hdfs-opendal")]
use opendal::Operator;
#[cfg(feature = "hdfs-opendal")]
use std::io::Cursor;

use crate::jvm_bridge::OutputStreamSink;
use crate::parquet::parquet_support::is_hdfs_scheme;
#[cfg(feature = "hdfs-opendal")]
use crate::parquet::parquet_support::{create_hdfs_operator, prepare_object_store_with_configs};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::{
    common::tree_node::TreeNodeRecursion,
    error::{DataFusionError, Result},
    execution::{context::TaskContext, memory_pool::MemoryConsumer},
    physical_expr::{EquivalenceProperties, PhysicalExpr},
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
    },
};
use futures::TryStreamExt;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use url::Url;

/// Compression codecs supported by the native Parquet writer.
///
/// This is deliberately separate from the shuffle `CompressionCodec`: gzip is a valid Parquet
/// codec but is not a codec we compress shuffle blocks with.
#[derive(Debug, Clone, PartialEq)]
pub enum ParquetCompression {
    None,
    Snappy,
    Lz4,
    Zstd(i32),
    Gzip,
}

impl ParquetCompression {
    pub fn to_parquet(&self) -> Result<Compression> {
        match self {
            ParquetCompression::None => Ok(Compression::UNCOMPRESSED),
            ParquetCompression::Snappy => Ok(Compression::SNAPPY),
            ParquetCompression::Lz4 => Ok(Compression::LZ4),
            ParquetCompression::Zstd(level) => Ok(Compression::ZSTD(ZstdLevel::try_new(*level)?)),
            // Level 6, matching the default of parquet-mr's zlib codec
            ParquetCompression::Gzip => Ok(Compression::GZIP(GzipLevel::default())),
        }
    }
}

/// Enum representing different types of Arrow writers based on storage backend
#[allow(clippy::large_enum_variant)]
enum ParquetWriter {
    /// Writer for local file system
    LocalFile(ArrowWriter<File>),
    /// Writer for HDFS or other remote storage (writes to in-memory buffer)
    /// Contains the arrow writer, HDFS operator, and destination path
    /// an Arrow writer writes to in-memory buffer the data converted to Parquet format
    /// The opendal::Writer is created lazily on first write
    #[cfg(feature = "hdfs-opendal")]
    Remote(
        Box<ArrowWriter<Cursor<Vec<u8>>>>,
        Option<opendal::Writer>,
        Box<Operator>,
        String,
    ),
    /// Writer for a file the JVM created through Hadoop's FileSystem API (`HadoopFileOutput`),
    /// which only supplies the file's bytes.
    OutputStream(ArrowWriter<OutputStreamWriter>),
}

impl ParquetWriter {
    /// Write a RecordBatch to the underlying writer
    async fn write(
        &mut self,
        batch: &RecordBatch,
    ) -> std::result::Result<(), parquet::errors::ParquetError> {
        match self {
            ParquetWriter::LocalFile(writer) => writer.write(batch),
            #[cfg(feature = "hdfs-opendal")]
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
            ParquetWriter::OutputStream(writer) => writer
                .write(batch)
                .map_err(|e| writer.inner_mut().stream_failure_or(e)),
        }
    }

    /// Memory the writer holds between batches: parquet-rs's estimate for the in-progress row
    /// group, which counts encoded pages, encoder buffers, dictionaries and any Bloom filters.
    /// The remote writer flushes a row group after every batch and stages it in a buffer that is
    /// cleared after each upload but keeps its capacity, so that buffer counts too, as does the
    /// buffer an output stream writer collects its next write in.
    fn memory_size(&self) -> usize {
        match self {
            ParquetWriter::LocalFile(writer) => writer.memory_size(),
            #[cfg(feature = "hdfs-opendal")]
            ParquetWriter::Remote(writer, ..) => {
                writer.memory_size() + writer.inner().get_ref().capacity()
            }
            ParquetWriter::OutputStream(writer) => {
                writer.memory_size() + writer.inner().buffer.capacity()
            }
        }
    }

    /// Close the writer and finalize the file. Returns the file's size in bytes, which the local and
    /// output stream writers count as they write. The HDFS writer uploads its file in pieces and
    /// reports none.
    async fn close(self) -> std::result::Result<Option<u64>, parquet::errors::ParquetError> {
        match self {
            ParquetWriter::LocalFile(mut writer) => {
                // `finish` rather than `close`, which would drop the count of bytes written.
                writer.finish()?;
                Ok(Some(writer.bytes_written() as u64))
            }
            #[cfg(feature = "hdfs-opendal")]
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

                Ok(None)
            }
            ParquetWriter::OutputStream(mut writer) => {
                // `finish` sends the footer and flushes the writer's last write, so the JVM can
                // close the file once this returns.
                writer
                    .finish()
                    .map_err(|e| writer.inner_mut().stream_failure_or(e))?;
                Ok(Some(writer.bytes_written() as u64))
            }
        }
    }
}

/// The task's error for `e`, a Parquet write that failed. A failed write to the JVM output stream
/// comes back as the stream's own error, which keeps the Java exception it carries, so the JVM
/// rethrows that exception as it was. Any other failure is reported with `what` and its text.
fn write_error(e: parquet::errors::ParquetError, what: &str) -> DataFusionError {
    match e {
        parquet::errors::ParquetError::External(source) => {
            match source.downcast::<DataFusionError>() {
                Ok(stream_failure) => *stream_failure,
                Err(source) => DataFusionError::Execution(format!(
                    "{what}: {}",
                    parquet::errors::ParquetError::External(source)
                )),
            }
        }
        e => DataFusionError::Execution(format!("{what}: {e}")),
    }
}

/// Writes a Parquet file's bytes to the output stream of the file the JVM created. Bytes are
/// collected into writes of the stream's `max_write_size`, so a file costs one upcall per write
/// rather than one per page.
struct OutputStreamWriter {
    sink: Arc<dyn OutputStreamSink>,
    /// Bytes not yet handed to the stream, at most `capacity` of them.
    buffer: Vec<u8>,
    capacity: usize,
    /// Set once a write to the stream has failed. Nothing is sent after that, not even what
    /// dropping the Parquet writer flushes, since the stream is in an unknown state and the JVM
    /// aborts the file once the failure reaches it.
    failed: bool,
    /// What the failed write returned, until [`Self::stream_failure_or`] reports it. Parquet only
    /// sees the `io::Error` that `Write` allows, so this keeps the error itself: for the JNI sink
    /// it holds the Java exception the stream threw, which the JVM then rethrows as it was.
    failure: Option<DataFusionError>,
}

impl OutputStreamWriter {
    fn new(sink: Arc<dyn OutputStreamSink>) -> Self {
        let capacity = sink.max_write_size().max(1);
        Self {
            sink,
            buffer: Vec::with_capacity(capacity),
            capacity,
            failed: false,
            failure: None,
        }
    }

    /// The error to report for `e`, a write of this file that Parquet failed: the stream's own
    /// error when a write to the stream caused it.
    fn stream_failure_or(
        &mut self,
        e: parquet::errors::ParquetError,
    ) -> parquet::errors::ParquetError {
        match self.failure.take() {
            Some(failure) => parquet::errors::ParquetError::External(Box::new(failure)),
            None => e,
        }
    }

    /// Hands the buffered bytes to the stream.
    fn send(&mut self) -> std::io::Result<()> {
        if self.failed {
            return Err(std::io::Error::other(
                "a previous write to the output stream failed",
            ));
        }
        if !self.buffer.is_empty() {
            if let Err(e) = self.sink.write(&self.buffer) {
                self.failed = true;
                let error = std::io::Error::other(e.to_string());
                self.failure = Some(e);
                return Err(error);
            }
            self.buffer.clear();
        }
        Ok(())
    }
}

impl Write for OutputStreamWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        // A full buffer is sent before taking more rather than as soon as it fills, so that a
        // failed send consumes none of `data`, as `Write` requires of an error.
        if self.buffer.len() == self.capacity || self.failed {
            self.send()?;
        }
        let taken = data.len().min(self.capacity - self.buffer.len());
        self.buffer.extend_from_slice(&data[..taken]);
        Ok(taken)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.send()
    }
}

/// Parquet writer operator that writes input batches to a Parquet file
#[derive(Debug)]
pub struct ParquetWriterExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// The exact path of the file this task writes, chosen by Spark's commit protocol on the JVM
    /// side and used verbatim - this operator never derives file names of its own.
    output_path: String,
    /// Compression codec
    compression: ParquetCompression,
    /// Partition ID (from Spark TaskContext)
    partition_id: i32,
    /// Column names to use in the output Parquet file
    column_names: Vec<String>,
    /// Catalyst's target schema, including nullability and Parquet field metadata.
    output_schema: Option<SchemaRef>,
    /// Object store configuration options
    object_store_options: HashMap<String, String>,
    /// The output stream of the file at `output_path`, when the JVM created that file. The writer
    /// then writes the file's bytes there and never opens `output_path` itself.
    output_stream: Option<Arc<dyn OutputStreamSink>>,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache for plan properties
    cache: Arc<PlanProperties>,
}

impl ParquetWriterExec {
    /// Create a new ParquetWriterExec
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        output_path: String,
        compression: ParquetCompression,
        partition_id: i32,
        column_names: Vec<String>,
        output_schema: Option<SchemaRef>,
        object_store_options: HashMap<String, String>,
    ) -> Result<Self> {
        // Preserve the input's partitioning so each partition writes its own file
        let input_partitioning = input.output_partitioning().clone();

        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&input.schema())),
            input_partitioning,
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Ok(ParquetWriterExec {
            input,
            output_path,
            compression,
            partition_id,
            column_names,
            output_schema,
            object_store_options,
            output_stream: None,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// Write the file's bytes to `output_stream`, when there is one: the output stream of the file
    /// the JVM created at `output_path`, which the writer then never creates itself.
    pub fn with_output_stream(mut self, output_stream: Option<Arc<dyn OutputStreamSink>>) -> Self {
        self.output_stream = output_stream;
        self
    }

    /// Create an Arrow writer based on the storage scheme
    ///
    /// # Arguments
    /// * `output_file_path` - The full path to the output file
    /// * `schema` - The Arrow schema for the Parquet file
    /// * `props` - Writer properties including compression
    /// * `runtime_env` - Runtime environment for object store registration
    /// * `object_store_options` - Configuration options for object store
    /// * `output_stream` - The output stream of the file, when the JVM created it
    ///
    /// # Returns
    /// * `Ok(ParquetWriter)` - A writer appropriate for the storage scheme
    /// * `Err(DataFusionError)` - If writer creation fails
    fn create_arrow_writer(
        output_file_path: &str,
        schema: SchemaRef,
        props: WriterProperties,
        _runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
        object_store_options: &HashMap<String, String>,
        output_stream: Option<Arc<dyn OutputStreamSink>>,
    ) -> Result<ParquetWriter> {
        // The JVM created the file through its own FileSystem, so the path is not this writer's
        // to interpret, whatever its scheme.
        if let Some(output_stream) = output_stream {
            let writer =
                ArrowWriter::try_new(OutputStreamWriter::new(output_stream), schema, Some(props))
                    .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to create output stream writer: {}",
                        e
                    ))
                })?;
            return Ok(ParquetWriter::OutputStream(writer));
        }

        // Parse URL and match on storage scheme directly
        let url = Url::parse(output_file_path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to parse URL '{}': {}", output_file_path, e))
        })?;

        if is_hdfs_scheme(&url, object_store_options) {
            #[cfg(feature = "hdfs-opendal")]
            {
                // Use prepare_object_store_with_configs to create and register the object store
                let (_object_store_url, object_store_path, _) = prepare_object_store_with_configs(
                    _runtime_env,
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
                    Box::new(arrow_parquet_buffer_writer),
                    None,
                    Box::new(op),
                    object_store_path.to_string(),
                ))
            }
            #[cfg(not(feature = "hdfs-opendal"))]
            {
                Err(DataFusionError::Execution(
                    "HDFS support is not enabled. Rebuild with the 'hdfs-opendal' feature.".into(),
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
    fn name(&self) -> &str {
        "ParquetWriterExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(
                ParquetWriterExec::try_new(
                    Arc::clone(&children[0]),
                    self.output_path.clone(),
                    self.compression.clone(),
                    self.partition_id,
                    self.column_names.clone(),
                    self.output_schema.clone(),
                    self.object_store_options.clone(),
                )?
                .with_output_stream(self.output_stream.clone()),
            )),
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
        let reservation = MemoryConsumer::new(format!("ParquetWriterExec[{partition}]"))
            .register(context.memory_pool());
        let input = self.input.execute(partition, context)?;
        let input_schema = self.input.schema();
        let compression = self.compression.to_parquet()?;
        let column_names = self.column_names.clone();

        assert_eq!(input_schema.fields().len(), column_names.len());

        // The input schema comes from the placeholder Scan and marks every top-level field
        // nullable. Use Catalyst's target schema so Parquet repetition and field IDs match Spark.
        // Keep the column-name-only path for plans serialized before output_schema was added.
        let output_schema = self.output_schema.clone().unwrap_or_else(|| {
            let fields: Vec<_> = input_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| Arc::new(field.as_ref().clone().with_name(&column_names[i])))
                .collect();
            Arc::new(Schema::new(fields))
        });

        // The JVM commit protocol has already chosen the exact file to write.
        let part_file = self.output_path.clone();

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
            self.output_stream.clone(),
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
                    // Collection field IDs exist on the target schema, not on arrays produced by
                    // the placeholder Scan. Both schemas use the same Catalyst data types, and
                    // disabling field-name matching still recursively validates nested nullability;
                    // only nested field names and metadata are ignored.
                    let (_, columns, _) = batch.into_parts();
                    RecordBatch::try_new_with_options(
                        Arc::clone(&schema_for_write),
                        columns,
                        &RecordBatchOptions::new().with_match_field_names(false),
                    )
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Failed to rename batch columns: {}", e))
                    })?
                } else {
                    batch
                };

                writer
                    .write(&renamed_batch)
                    .await
                    .map_err(|e| write_error(e, "Failed to write batch"))?;
                // The writer cannot spill, so a row group the pool will not grant fails the task.
                reservation.try_resize(writer.memory_size())?;
            }

            let file_size = writer
                .close()
                .await
                .map_err(|e| write_error(e, "Failed to close writer"))?
                .unwrap_or(0) as i64;

            // Update metrics with write statistics
            files_written.add(1);
            bytes_written.add(file_size as usize);
            rows_written.add(total_rows as usize);

            log::debug!(
                "Wrote Parquet file: path={}, size={}, rows={}",
                part_file,
                file_size,
                total_rows
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
    use crate::execution::memory_pools::testing::PeakMemoryPool;
    use arrow::array::{Array, Int32Array, ListArray, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::memory_pool::MemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::basic::Repetition;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::sync::Arc;

    #[test]
    fn test_parquet_compression_to_parquet() {
        assert_eq!(
            ParquetCompression::None.to_parquet().unwrap(),
            Compression::UNCOMPRESSED
        );
        assert_eq!(
            ParquetCompression::Snappy.to_parquet().unwrap(),
            Compression::SNAPPY
        );
        assert_eq!(
            ParquetCompression::Lz4.to_parquet().unwrap(),
            Compression::LZ4
        );
        assert_eq!(
            ParquetCompression::Zstd(3).to_parquet().unwrap(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap())
        );
        // gzip level 6 matches the default of parquet-mr's zlib codec
        assert_eq!(
            ParquetCompression::Gzip.to_parquet().unwrap(),
            Compression::GZIP(GzipLevel::default())
        );
    }

    /// The JVM hands over the exact file to write. The writer must use that path verbatim - Spark's commit protocol owns naming and staging, and
    /// committers that track individual files depend on the name it chose.
    #[tokio::test]
    async fn test_parquet_writer_uses_output_path_verbatim() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        let memory_source = MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&schema), None)?;
        let input = Arc::new(DataSourceExec::new(Arc::new(memory_source)));
        let temp_dir = tempfile::tempdir()?;
        // A name the writer could never have derived itself, matching Spark's convention.
        let file_name = "part-00007-11111111-2222-3333-4444-555555555555-c000.parquet";
        let output_path = format!("file://{}/{}", temp_dir.path().display(), file_name);

        let writer = ParquetWriterExec::try_new(
            input,
            output_path,
            ParquetCompression::None,
            // A non-zero partition id must not leak into the file name.
            3,
            vec!["id".to_string()],
            None,
            HashMap::new(),
        )?;

        let mut stream = writer.execute(0, SessionContext::new().task_ctx())?;
        while stream.try_next().await?.is_some() {}

        let written = temp_dir.path().join(file_name);
        assert!(
            written.exists(),
            "expected the writer to use the given path verbatim, found: {:?}",
            std::fs::read_dir(temp_dir.path())?
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect::<Vec<_>>()
        );

        let reader = SerializedFileReader::new(File::open(written)?)?;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer_preserves_catalyst_schema_in_footer() -> Result<()> {
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None]),
            Some(vec![Some(2)]),
        ]);
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("col_0", DataType::Int32, true),
            Field::new("col_1", values.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(values)],
        )?;

        let DataType::List(input_element) = input_schema.field(1).data_type() else {
            panic!("expected list input");
        };
        let list_element = input_element
            .as_ref()
            .clone()
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "23".to_string(),
            )]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("required_id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "11".to_string(),
            )])),
            Field::new("values", DataType::List(Arc::new(list_element)), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "22".to_string())]),
            ),
        ]));

        let memory_source = MemorySourceConfig::try_new(&[vec![batch]], input_schema, None)?;
        let input = Arc::new(DataSourceExec::new(Arc::new(memory_source)));
        let temp_dir = tempfile::tempdir()?;
        let output_path = format!("file://{}/part-00000.parquet", temp_dir.path().display());
        let writer = ParquetWriterExec::try_new(
            input,
            output_path,
            ParquetCompression::None,
            0,
            vec!["required_id".to_string(), "values".to_string()],
            Some(output_schema),
            HashMap::new(),
        )?;

        let mut stream = writer.execute(0, SessionContext::new().task_ctx())?;
        while stream.try_next().await?.is_some() {}

        let file = File::open(temp_dir.path().join("part-00000.parquet"))?;
        let reader = SerializedFileReader::new(file)?;
        let fields = reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .root_schema()
            .get_fields();
        let required_id = fields[0].get_basic_info();
        assert_eq!(required_id.repetition(), Repetition::REQUIRED);
        assert_eq!(required_id.id(), 11);

        let list = &fields[1];
        assert_eq!(list.get_basic_info().repetition(), Repetition::OPTIONAL);
        assert_eq!(list.get_basic_info().id(), 22);
        let element = list.get_fields()[0].get_fields()[0].get_basic_info();
        assert_eq!(element.repetition(), Repetition::OPTIONAL);
        assert_eq!(element.id(), 23);

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer_rejects_mismatched_nested_nullability() -> Result<()> {
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1)]),
            Some(vec![Some(2)]),
        ]);
        let DataType::List(input_element) = values.data_type() else {
            panic!("expected list input");
        };
        assert!(input_element.is_nullable());

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "col_0",
            values.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&input_schema), vec![Arc::new(values)])?;

        let target_element = Field::new("element", DataType::Int32, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "23".to_string())]),
        );
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "values",
            DataType::List(Arc::new(target_element)),
            true,
        )]));

        let memory_source = MemorySourceConfig::try_new(&[vec![batch]], input_schema, None)?;
        let input = Arc::new(DataSourceExec::new(Arc::new(memory_source)));
        let temp_dir = tempfile::tempdir()?;
        let output_path = format!("file://{}/part-00000.parquet", temp_dir.path().display());
        let writer = ParquetWriterExec::try_new(
            input,
            output_path,
            ParquetCompression::None,
            0,
            vec!["values".to_string()],
            Some(output_schema),
            HashMap::new(),
        )?;

        let mut stream = writer.execute(0, SessionContext::new().task_ctx())?;
        let error = stream
            .try_next()
            .await
            .expect_err("mismatched nested nullability must be rejected");
        let message = error.to_string();
        assert!(
            message.contains("Failed to rename batch columns"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("column types must match schema types"),
            "unexpected error: {message}"
        );

        Ok(())
    }

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

    /// Writes `batches` to a local file, reserving from `pool` as a task reserves from its own,
    /// and returns the size of the file written.
    async fn write_reserving_from(
        pool: &Arc<PeakMemoryPool>,
        batches: Vec<RecordBatch>,
    ) -> Result<u64> {
        let temp_dir = tempfile::tempdir()?;
        let file = temp_dir.path().join("part-00000.parquet");
        let writer = writer_of(
            batches,
            format!("file://{}", file.display()),
            ParquetCompression::None,
        )?;
        let pool: Arc<dyn MemoryPool> = Arc::<PeakMemoryPool>::clone(pool);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()?;
        let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let mut stream = writer.execute(0, context.task_ctx())?;
        while stream.try_next().await?.is_some() {}
        Ok(std::fs::metadata(file)?.len())
    }

    /// The writer reserves its in-progress row group, here the whole file, until the file is
    /// closed.
    #[tokio::test]
    async fn test_parquet_writer_reserves_its_row_group_until_the_file_closes() -> Result<()> {
        let pool = Arc::new(PeakMemoryPool::new(usize::MAX));
        let batches = (1..=5)
            .map(create_test_record_batch)
            .collect::<Result<_>>()?;
        let file_size = write_reserving_from(&pool, batches).await?;
        assert!(
            pool.peak() as u64 >= file_size / 2,
            "reserved {} bytes for a row group written as a {file_size}-byte file",
            pool.peak()
        );
        assert_eq!(pool.reserved(), 0, "the writer kept its reservation");
        Ok(())
    }

    /// A row group the pool will not grant fails the write with the pool's error, rather than
    /// holding memory nothing accounts for.
    #[tokio::test]
    async fn test_parquet_writer_fails_when_the_pool_refuses_its_row_group() -> Result<()> {
        let pool = Arc::new(PeakMemoryPool::new(1024));
        let error = write_reserving_from(&pool, vec![create_test_record_batch(1)?])
            .await
            .expect_err("a row group larger than the pool must fail the write");
        assert!(
            matches!(error, DataFusionError::ResourcesExhausted(_)),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("ParquetWriterExec[0]"),
            "{error}"
        );
        assert_eq!(pool.reserved(), 0, "the failed write kept its reservation");
        Ok(())
    }

    /// Records what a writer hands the output stream of a file the JVM created, and fails the
    /// write with index `failing_write` when there is one.
    #[derive(Default)]
    struct RecordingSink {
        max_write_size: usize,
        failing_write: Option<usize>,
        writes: std::sync::Mutex<Vec<Vec<u8>>>,
    }

    impl RecordingSink {
        fn new(max_write_size: usize) -> Self {
            Self {
                max_write_size,
                ..Default::default()
            }
        }

        fn writes(&self) -> Vec<Vec<u8>> {
            self.writes.lock().unwrap().clone()
        }
    }

    impl OutputStreamSink for RecordingSink {
        fn max_write_size(&self) -> usize {
            self.max_write_size
        }

        fn write(&self, data: &[u8]) -> Result<()> {
            let mut writes = self.writes.lock().unwrap();
            assert!(
                data.len() <= self.max_write_size,
                "a write of {} bytes exceeds the stream's limit of {}",
                data.len(),
                self.max_write_size
            );
            writes.push(data.to_vec());
            if self.failing_write == Some(writes.len() - 1) {
                return Err(DataFusionError::Execution(
                    "injected output stream failure".to_string(),
                ));
            }
            Ok(())
        }
    }

    /// A writer of `batches`, read from memory, to `output_path`.
    fn writer_of(
        batches: Vec<RecordBatch>,
        output_path: String,
        compression: ParquetCompression,
    ) -> Result<ParquetWriterExec> {
        let schema = batches[0].schema();
        let column_names = schema.fields().iter().map(|f| f.name().clone()).collect();
        let memory_source = MemorySourceConfig::try_new(&[batches], Arc::clone(&schema), None)?;
        let input = Arc::new(DataSourceExec::new(Arc::new(memory_source)));
        ParquetWriterExec::try_new(
            input,
            output_path,
            compression,
            0,
            column_names,
            None,
            HashMap::new(),
        )
    }

    /// Writes `batches` with a writer for `output_path`, through `output_stream` when there is
    /// one, and returns the writer for its metrics.
    async fn write_batches(
        batches: Vec<RecordBatch>,
        output_path: String,
        output_stream: Option<Arc<dyn OutputStreamSink>>,
    ) -> Result<Arc<ParquetWriterExec>> {
        let writer = Arc::new(
            writer_of(batches, output_path, ParquetCompression::Snappy)?
                .with_output_stream(output_stream),
        );
        let mut stream = writer.execute(0, SessionContext::new().task_ctx())?;
        while stream.try_next().await?.is_some() {}
        Ok(writer)
    }

    /// A file the JVM created gets exactly the bytes the writer would have written to a file of
    /// its own, in writes no larger than the stream accepts, and the writer never opens the path
    /// itself.
    #[tokio::test]
    async fn test_parquet_writer_writes_a_jvm_created_file_through_its_output_stream() -> Result<()>
    {
        let batches: Vec<RecordBatch> = (1..=5)
            .map(create_test_record_batch)
            .collect::<Result<_>>()?;
        let temp_dir = tempfile::tempdir()?;
        let local_file = temp_dir.path().join("part-00000.parquet");
        write_batches(
            batches.clone(),
            format!("file://{}", local_file.display()),
            None,
        )
        .await?;
        let expected = std::fs::read(&local_file)?;

        let max_write_size = 4096;
        let sink = Arc::new(RecordingSink::new(max_write_size));
        let magic_path = "s3a://bucket/table/__magic_job-0001/job-0001/tasks/\
                          attempt_0001_m_000000_0/__base/part-00000.parquet";
        let writer = write_batches(
            batches,
            magic_path.to_string(),
            Some(Arc::clone(&sink) as Arc<dyn OutputStreamSink>),
        )
        .await?;

        let writes = sink.writes();
        assert!(
            writes.len() > 1,
            "expected several writes, got {}",
            writes.len()
        );
        let (last, full) = writes.split_last().unwrap();
        assert!(
            full.iter().all(|write| write.len() == max_write_size),
            "every write but the last fills the stream's limit: {:?}",
            writes.iter().map(Vec::len).collect::<Vec<_>>()
        );
        assert!(!last.is_empty() && last.len() <= max_write_size);
        assert_eq!(writes.concat(), expected);

        let metrics = writer.metrics().unwrap();
        assert_eq!(
            metrics.sum_by_name("bytes_written").unwrap().as_usize(),
            expected.len()
        );
        assert_eq!(
            metrics.sum_by_name("rows_written").unwrap().as_usize(),
            5000
        );
        Ok(())
    }

    /// A failed write to the output stream fails the task with the stream's own error, unchanged,
    /// which for the JNI sink carries the Java exception the JVM then rethrows as it was.
    #[tokio::test]
    async fn test_parquet_writer_fails_when_its_output_stream_fails() -> Result<()> {
        let batches: Vec<RecordBatch> = (1..=5)
            .map(create_test_record_batch)
            .collect::<Result<_>>()?;
        let sink = Arc::new(RecordingSink {
            failing_write: Some(1),
            ..RecordingSink::new(1024)
        });
        let error = write_batches(
            batches,
            "s3a://bucket/table/part-00000.parquet".to_string(),
            Some(Arc::clone(&sink) as Arc<dyn OutputStreamSink>),
        )
        .await
        .expect_err("a failed write to the output stream must fail the task");

        assert!(
            matches!(&error, DataFusionError::Execution(message)
                if message == "injected output stream failure"),
            "{error:?}"
        );
        assert_eq!(
            sink.writes().len(),
            2,
            "the writer kept writing to a stream that had failed"
        );
        Ok(())
    }

    /// Once a write to the stream has failed, the writer sends nothing more: neither what a
    /// caller writes next, nor what a flush, such as the one dropping the Parquet writer makes,
    /// would send. A failed send also consumes none of the bytes it was handed, as `Write`
    /// requires of an error.
    #[test]
    fn test_output_stream_writer_sends_nothing_after_a_failed_write() {
        let sink = Arc::new(RecordingSink {
            failing_write: Some(0),
            ..RecordingSink::new(4)
        });
        let mut writer = OutputStreamWriter::new(Arc::clone(&sink) as Arc<dyn OutputStreamSink>);

        // A full buffer is only sent when the next bytes arrive.
        writer.write_all(b"abcd").unwrap();
        assert!(sink.writes().is_empty());
        writer
            .write(b"e")
            .expect_err("the stream fails its first write");
        assert_eq!(sink.writes(), vec![b"abcd".to_vec()]);
        assert_eq!(writer.buffer, b"abcd", "the failed write consumed bytes");

        writer
            .write(b"e")
            .expect_err("the writer must not write to a failed stream");
        writer
            .flush()
            .expect_err("the writer must not flush to a failed stream");
        assert_eq!(
            sink.writes().len(),
            1,
            "the writer wrote to a failed stream"
        );
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
        let op = Operator::new(builder).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create HDFS operator: {}", e))
        })?;

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
        let op = Operator::new(builder).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create HDFS operator: {}", e))
        })?;

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
            None,
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
        let output_path =
            "hdfs://namenode:9000/user/test_parquet_writer_exec/part-00000-00123.parquet"
                .to_string();
        let column_names = vec!["id".to_string(), "name".to_string()];

        let parquet_writer = ParquetWriterExec::try_new(
            memory_exec,
            output_path,
            ParquetCompression::None,
            0, // partition_id
            column_names,
            None,           // output_schema
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
