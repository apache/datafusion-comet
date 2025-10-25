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

//! Native Iceberg table scan operator using iceberg-rust

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream, StreamExt, TryStreamExt};
use iceberg::io::FileIO;

use crate::execution::operators::ExecutionError;
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkSchemaAdapterFactory;
use datafusion::datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_comet_spark_expr::EvalMode;

/// Native Iceberg scan operator that uses iceberg-rust to read Iceberg tables.
///
/// Bypasses Spark's DataSource V2 API by reading pre-planned FileScanTasks directly.
#[derive(Debug)]
pub struct IcebergScanExec {
    /// Iceberg table metadata location for FileIO initialization
    metadata_location: String,
    /// Output schema after projection
    output_schema: SchemaRef,
    /// Cached execution plan properties
    plan_properties: PlanProperties,
    /// Catalog-specific configuration for FileIO
    catalog_properties: HashMap<String, String>,
    /// Pre-planned file scan tasks from Scala, grouped by Spark partition
    file_task_groups: Option<Vec<Vec<iceberg::scan::FileScanTask>>>,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergScanExec {
    pub fn new(
        metadata_location: String,
        schema: SchemaRef,
        catalog_properties: HashMap<String, String>,
        file_task_groups: Option<Vec<Vec<iceberg::scan::FileScanTask>>>,
        num_partitions: usize,
    ) -> Result<Self, ExecutionError> {
        // Don't normalize - just use the schema as provided by Spark
        let output_schema = schema;

        let plan_properties = Self::compute_properties(Arc::clone(&output_schema), num_partitions);

        let metrics = ExecutionPlanMetricsSet::new();

        Ok(Self {
            metadata_location,
            output_schema,
            plan_properties,
            catalog_properties,
            file_task_groups,
            metrics,
        })
    }

    fn compute_properties(schema: SchemaRef, num_partitions: usize) -> PlanProperties {
        // Matches Spark partition count to ensure proper parallelism
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl ExecutionPlan for IcebergScanExec {
    fn name(&self) -> &str {
        "IcebergScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Execute pre-planned tasks from Scala (planning happens via Iceberg's Java API)
        if let Some(ref task_groups) = self.file_task_groups {
            if partition < task_groups.len() {
                let tasks = &task_groups[partition];

                return self.execute_with_tasks(tasks.clone(), partition, context);
            } else {
                return Err(DataFusionError::Execution(format!(
                    "IcebergScanExec: Partition index {} out of range (only {} task groups available)",
                    partition,
                    task_groups.len()
                )));
            }
        }

        Err(DataFusionError::Execution(format!(
            "IcebergScanExec: No FileScanTasks provided for partition {}. \
             All scan planning must happen on the Scala side.",
            partition
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl IcebergScanExec {
    /// Handles MOR (Merge-On-Read) tables by automatically applying positional and equality
    /// deletes via iceberg-rust's ArrowReader.
    fn execute_with_tasks(
        &self,
        tasks: Vec<iceberg::scan::FileScanTask>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let output_schema = Arc::clone(&self.output_schema);

        // Create FileIO synchronously
        let file_io = Self::load_file_io(&self.catalog_properties, &self.metadata_location)?;

        // Get batch size from context
        let batch_size = context.session_config().batch_size();

        // Create baseline metrics for this partition
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        // Create parallel file stream that overlaps opening next file with reading current file
        let file_stream = IcebergFileStream::new(
            tasks,
            file_io,
            batch_size,
            Arc::clone(&output_schema),
            baseline_metrics,
        )?;

        // Note: BatchSplitStream adds overhead. Since we're already setting batch_size in
        // iceberg-rust's ArrowReaderBuilder, it should produce correctly sized batches.
        // Only use BatchSplitStream as a safety net if needed.
        // For now, return the file_stream directly to reduce stream nesting overhead.

        Ok(Box::pin(file_stream))
    }

    fn load_file_io(
        catalog_properties: &HashMap<String, String>,
        metadata_location: &str,
    ) -> Result<FileIO, DataFusionError> {
        let mut file_io_builder = FileIO::from_path(metadata_location)
            .map_err(|e| DataFusionError::Execution(format!("Failed to create FileIO: {}", e)))?;

        for (key, value) in catalog_properties {
            file_io_builder = file_io_builder.with_prop(key, value);
        }

        file_io_builder
            .build()
            .map_err(|e| DataFusionError::Execution(format!("Failed to build FileIO: {}", e)))
    }
}

/// State machine for IcebergFileStream
enum FileStreamState {
    /// Idle state - need to start opening next file
    Idle,
    /// Opening a file
    Opening {
        future: BoxFuture<'static, DFResult<SendableRecordBatchStream>>,
    },
    /// Reading from current file while potentially opening next file
    Reading {
        current: SendableRecordBatchStream,
        next: Option<BoxFuture<'static, DFResult<SendableRecordBatchStream>>>,
    },
    /// Error state
    Error,
}

/// Stream that reads Iceberg files with parallel opening optimization.
/// Opens the next file while reading the current file to overlap IO with compute.
///
/// Inspired by DataFusion's [`FileStream`] pattern for overlapping file opening with reading.
///
/// [`FileStream`]: https://github.com/apache/datafusion/blob/main/datafusion/datasource/src/file_stream.rs
struct IcebergFileStream {
    schema: SchemaRef,
    file_io: FileIO,
    batch_size: usize,
    tasks: VecDeque<iceberg::scan::FileScanTask>,
    state: FileStreamState,
    baseline_metrics: BaselineMetrics,
}

impl IcebergFileStream {
    fn new(
        tasks: Vec<iceberg::scan::FileScanTask>,
        file_io: FileIO,
        batch_size: usize,
        schema: SchemaRef,
        baseline_metrics: BaselineMetrics,
    ) -> DFResult<Self> {
        Ok(Self {
            schema,
            file_io,
            batch_size,
            tasks: tasks.into_iter().collect(),
            state: FileStreamState::Idle,
            baseline_metrics,
        })
    }

    /// Start opening the next file
    fn start_next_file(
        &mut self,
    ) -> Option<BoxFuture<'static, DFResult<SendableRecordBatchStream>>> {
        let task = self.tasks.pop_front()?;
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;
        let schema = Arc::clone(&self.schema);

        Some(Box::pin(async move {
            // Create a single-task stream
            let task_stream = futures::stream::iter(vec![Ok(task)]).boxed();

            // Create reader with optimizations
            let reader = iceberg::arrow::ArrowReaderBuilder::new(file_io)
                .with_batch_size(batch_size)
                .with_row_selection_enabled(true)
                .build();

            // Read the task
            let stream = reader.read(task_stream).map_err(|e| {
                DataFusionError::Execution(format!("Failed to read Iceberg task: {}", e))
            })?;

            // Clone schema for transformation
            let target_schema = Arc::clone(&schema);

            // Apply schema adaptation to each batch (same approach as regular Parquet scans)
            // This handles differences in field names ("element" vs "item", "key_value" vs "entries")
            // and metadata (PARQUET:field_id) just like regular Parquet scans
            let mapped_stream = stream
                .map_err(|e| DataFusionError::Execution(format!("Iceberg scan error: {}", e)))
                .and_then(move |batch| {
                    // Use SparkSchemaAdapter to transform the batch
                    let spark_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
                    let adapter_factory = SparkSchemaAdapterFactory::new(spark_options, None);
                    let file_schema = batch.schema();
                    let adapter = adapter_factory
                        .create(Arc::clone(&target_schema), Arc::clone(&file_schema));

                    // Apply the schema mapping
                    let result = match adapter.map_schema(file_schema.as_ref()) {
                        Ok((schema_mapper, _projection)) => {
                            schema_mapper.map_batch(batch).map_err(|e| {
                                DataFusionError::Execution(format!("Batch mapping failed: {}", e))
                            })
                        }
                        Err(e) => Err(DataFusionError::Execution(format!(
                            "Schema mapping failed: {}",
                            e
                        ))),
                    };
                    futures::future::ready(result)
                });

            Ok(Box::pin(IcebergStreamWrapper {
                inner: mapped_stream,
                schema,
            }) as SendableRecordBatchStream)
        }))
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<DFResult<RecordBatch>>> {
        loop {
            match &mut self.state {
                FileStreamState::Idle => {
                    // Start opening the first file
                    match self.start_next_file() {
                        Some(future) => {
                            self.state = FileStreamState::Opening { future };
                        }
                        None => return Poll::Ready(None),
                    }
                }
                FileStreamState::Opening { future } => {
                    // Wait for file to open
                    match ready!(future.poll_unpin(cx)) {
                        Ok(stream) => {
                            // File opened, start reading and open next file in parallel
                            let next = self.start_next_file();
                            self.state = FileStreamState::Reading {
                                current: stream,
                                next,
                            };
                        }
                        Err(e) => {
                            self.state = FileStreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                FileStreamState::Reading { current, next } => {
                    // Poll next file opening future to drive it forward (background IO)
                    if let Some(next_future) = next {
                        if let Poll::Ready(result) = next_future.poll_unpin(cx) {
                            // Next file is ready, store it
                            match result {
                                Ok(stream) => {
                                    *next = Some(Box::pin(futures::future::ready(Ok(stream))));
                                }
                                Err(e) => {
                                    self.state = FileStreamState::Error;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                        }
                    }

                    // Poll current stream for next batch
                    match ready!(current.poll_next_unpin(cx)) {
                        Some(result) => {
                            // Record output metrics if batch is successful
                            if let Ok(ref batch) = result {
                                self.baseline_metrics.record_output(batch.num_rows());
                            }
                            return Poll::Ready(Some(result));
                        }
                        None => {
                            // Current file is done, move to next file if available
                            match next.take() {
                                Some(mut next_future) => {
                                    // Check if next file is already opened
                                    match next_future.poll_unpin(cx) {
                                        Poll::Ready(Ok(stream)) => {
                                            let next_next = self.start_next_file();
                                            self.state = FileStreamState::Reading {
                                                current: stream,
                                                next: next_next,
                                            };
                                        }
                                        Poll::Ready(Err(e)) => {
                                            self.state = FileStreamState::Error;
                                            return Poll::Ready(Some(Err(e)));
                                        }
                                        Poll::Pending => {
                                            // Still opening, wait for it
                                            self.state = FileStreamState::Opening {
                                                future: next_future,
                                            };
                                        }
                                    }
                                }
                                None => {
                                    // No more files
                                    return Poll::Ready(None);
                                }
                            }
                        }
                    }
                }
                FileStreamState::Error => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl Stream for IcebergFileStream {
    type Item = DFResult<arrow::array::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

impl RecordBatchStream for IcebergFileStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Wrapper around iceberg-rust's stream that reports Comet's schema without validation.
/// This avoids strict schema checks that would reject batches with PARQUET:field_id metadata.
struct IcebergStreamWrapper<S> {
    inner: S,
    schema: SchemaRef,
}

impl<S> Stream for IcebergStreamWrapper<S>
where
    S: Stream<Item = DFResult<RecordBatch>> + Unpin,
{
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl<S> RecordBatchStream for IcebergStreamWrapper<S>
where
    S: Stream<Item = DFResult<RecordBatch>> + Unpin,
{
    fn schema(&self) -> SchemaRef {
        // Return Comet's schema, not the batch schema with metadata
        Arc::clone(&self.schema)
    }
}

impl DisplayAs for IcebergScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IcebergScanExec: metadata_location={}, num_tasks={:?}",
            self.metadata_location,
            self.file_task_groups
                .as_ref()
                .map(|groups| groups.iter().map(|g| g.len()).sum::<usize>())
        )
    }
}
