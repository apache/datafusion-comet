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
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
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
use datafusion_datasource::file_stream::FileStreamMetrics;

/// Iceberg table scan operator that uses iceberg-rust to read Iceberg tables.
///
/// Executes pre-planned FileScanTasks for efficient parallel scanning.
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
    /// Pre-planned file scan tasks, grouped by partition
    file_task_groups: Vec<Vec<iceberg::scan::FileScanTask>>,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergScanExec {
    pub fn new(
        metadata_location: String,
        schema: SchemaRef,
        catalog_properties: HashMap<String, String>,
        file_task_groups: Vec<Vec<iceberg::scan::FileScanTask>>,
    ) -> Result<Self, ExecutionError> {
        let output_schema = schema;
        let num_partitions = file_task_groups.len();
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
        if partition < self.file_task_groups.len() {
            let tasks = &self.file_task_groups[partition];
            self.execute_with_tasks(tasks.clone(), partition, context)
        } else {
            Err(DataFusionError::Execution(format!(
                "IcebergScanExec: Partition index {} out of range (only {} task groups available)",
                partition,
                self.file_task_groups.len()
            )))
        }
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
        let file_io = Self::load_file_io(&self.catalog_properties, &self.metadata_location)?;
        let batch_size = context.session_config().batch_size();

        let metrics = IcebergScanMetrics::new(&self.metrics, partition);

        // Create parallel file stream that overlaps opening next file with reading current file
        let file_stream = IcebergFileStream::new(
            tasks,
            file_io,
            batch_size,
            Arc::clone(&output_schema),
            metrics,
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

/// Metrics for IcebergScanExec
struct IcebergScanMetrics {
    /// Baseline metrics (output rows, elapsed compute time)
    baseline: BaselineMetrics,
    /// File stream metrics (time opening, time scanning, etc.)
    file_stream: FileStreamMetrics,
    /// Count of file splits (FileScanTasks) processed
    num_splits: Count,
}

impl IcebergScanMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            file_stream: FileStreamMetrics::new(metrics, partition),
            num_splits: MetricBuilder::new(metrics).counter("num_splits", partition),
        }
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
    metrics: IcebergScanMetrics,
}

impl IcebergFileStream {
    fn new(
        tasks: Vec<iceberg::scan::FileScanTask>,
        file_io: FileIO,
        batch_size: usize,
        schema: SchemaRef,
        metrics: IcebergScanMetrics,
    ) -> DFResult<Self> {
        Ok(Self {
            schema,
            file_io,
            batch_size,
            tasks: tasks.into_iter().collect(),
            state: FileStreamState::Idle,
            metrics,
        })
    }

    fn start_next_file(
        &mut self,
    ) -> Option<BoxFuture<'static, DFResult<SendableRecordBatchStream>>> {
        let task = self.tasks.pop_front()?;

        self.metrics.num_splits.add(1);

        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;
        let schema = Arc::clone(&self.schema);

        Some(Box::pin(async move {
            let task_stream = futures::stream::iter(vec![Ok(task)]).boxed();

            let reader = iceberg::arrow::ArrowReaderBuilder::new(file_io)
                .with_batch_size(batch_size)
                .with_row_selection_enabled(true)
                .build();

            let stream = reader.read(task_stream).map_err(|e| {
                DataFusionError::Execution(format!("Failed to read Iceberg task: {}", e))
            })?;

            let target_schema = Arc::clone(&schema);

            // Schema adaptation handles differences in Arrow field names and metadata
            // between the file schema and expected output schema
            let mapped_stream = stream
                .map_err(|e| DataFusionError::Execution(format!("Iceberg scan error: {}", e)))
                .and_then(move |batch| {
                    let spark_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
                    let adapter_factory = SparkSchemaAdapterFactory::new(spark_options, None);
                    let file_schema = batch.schema();
                    let adapter = adapter_factory
                        .create(Arc::clone(&target_schema), Arc::clone(&file_schema));

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
                    self.metrics.file_stream.time_opening.start();
                    match self.start_next_file() {
                        Some(future) => {
                            self.state = FileStreamState::Opening { future };
                        }
                        None => return Poll::Ready(None),
                    }
                }
                FileStreamState::Opening { future } => match ready!(future.poll_unpin(cx)) {
                    Ok(stream) => {
                        self.metrics.file_stream.time_opening.stop();
                        self.metrics.file_stream.time_scanning_until_data.start();
                        self.metrics.file_stream.time_scanning_total.start();
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
                },
                FileStreamState::Reading { current, next } => {
                    // Poll next file opening future to drive it forward (background IO)
                    if let Some(next_future) = next {
                        if let Poll::Ready(result) = next_future.poll_unpin(cx) {
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

                    match ready!(self
                        .metrics
                        .baseline
                        .record_poll(current.poll_next_unpin(cx)))
                    {
                        Some(result) => {
                            // Stop time_scanning_until_data on first batch (idempotent)
                            self.metrics.file_stream.time_scanning_until_data.stop();
                            self.metrics.file_stream.time_scanning_total.stop();
                            // Restart time_scanning_total for next batch
                            self.metrics.file_stream.time_scanning_total.start();
                            return Poll::Ready(Some(result));
                        }
                        None => {
                            self.metrics.file_stream.time_scanning_until_data.stop();
                            self.metrics.file_stream.time_scanning_total.stop();
                            match next.take() {
                                Some(mut next_future) => match next_future.poll_unpin(cx) {
                                    Poll::Ready(Ok(stream)) => {
                                        self.metrics.file_stream.time_scanning_until_data.start();
                                        self.metrics.file_stream.time_scanning_total.start();
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
                                        self.state = FileStreamState::Opening {
                                            future: next_future,
                                        };
                                    }
                                },
                                None => {
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
        self.metrics.file_stream.time_processing.start();
        let result = self.poll_inner(cx);
        self.metrics.file_stream.time_processing.stop();
        result
    }
}

impl RecordBatchStream for IcebergFileStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Wrapper around iceberg-rust's stream that avoids strict schema checks.
/// Returns the expected output schema to prevent rejection of batches with metadata differences.
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
        Arc::clone(&self.schema)
    }
}

impl DisplayAs for IcebergScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let num_tasks: usize = self.file_task_groups.iter().map(|g| g.len()).sum();
        write!(
            f,
            "IcebergScanExec: metadata_location={}, num_tasks={}",
            self.metadata_location, num_tasks
        )
    }
}
