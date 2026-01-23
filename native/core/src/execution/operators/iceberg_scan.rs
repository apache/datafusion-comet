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
use std::collections::HashMap;
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
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::io::FileIO;

use crate::execution::operators::ExecutionError;
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkSchemaAdapterFactory;
use datafusion::datasource::schema_adapter::{SchemaAdapterFactory, SchemaMapper};
use datafusion_comet_spark_expr::EvalMode;

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
        let num_tasks = tasks.len();
        metrics.num_splits.add(num_tasks);

        let task_stream = futures::stream::iter(tasks.into_iter().map(Ok)).boxed();

        let reader = iceberg::arrow::ArrowReaderBuilder::new(file_io)
            .with_batch_size(batch_size)
            .with_data_file_concurrency_limit(context.session_config().target_partitions())
            .with_row_selection_enabled(true)
            .build();

        // Pass all tasks to iceberg-rust at once to utilize its flatten_unordered
        // parallelization, avoiding overhead of single-task streams
        let stream = reader.read(task_stream).map_err(|e| {
            DataFusionError::Execution(format!("Failed to read Iceberg tasks: {}", e))
        })?;

        let spark_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        let adapter_factory = SparkSchemaAdapterFactory::new(spark_options, None);

        let adapted_stream =
            stream.map_err(|e| DataFusionError::Execution(format!("Iceberg scan error: {}", e)));

        let wrapped_stream = IcebergStreamWrapper {
            inner: adapted_stream,
            schema: output_schema,
            cached_adapter: None,
            adapter_factory,
            baseline_metrics: metrics.baseline,
        };

        Ok(Box::pin(wrapped_stream))
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
    /// Count of file splits (FileScanTasks) processed
    num_splits: Count,
}

impl IcebergScanMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            num_splits: MetricBuilder::new(metrics).counter("num_splits", partition),
        }
    }
}

/// Wrapper around iceberg-rust's stream that performs schema adaptation.
/// Handles batches from multiple files that may have different Arrow schemas
/// (metadata, field IDs, etc.). Caches schema adapters by source schema to avoid
/// recreating them for every batch from the same file.
struct IcebergStreamWrapper<S> {
    inner: S,
    schema: SchemaRef,
    /// Cached schema adapter with its source schema. Created when schema changes.
    cached_adapter: Option<(SchemaRef, Arc<dyn SchemaMapper>)>,
    /// Factory for creating schema adapters
    adapter_factory: SparkSchemaAdapterFactory,
    /// Metrics for output tracking
    baseline_metrics: BaselineMetrics,
}

impl<S> Stream for IcebergStreamWrapper<S>
where
    S: Stream<Item = DFResult<RecordBatch>> + Unpin,
{
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll_result = self.inner.poll_next_unpin(cx);

        let result = match poll_result {
            Poll::Ready(Some(Ok(batch))) => {
                let file_schema = batch.schema();

                // Check if we need to create a new adapter for this file's schema
                let needs_new_adapter = match &self.cached_adapter {
                    Some((cached_schema, _)) => !Arc::ptr_eq(cached_schema, &file_schema),
                    None => true,
                };

                if needs_new_adapter {
                    let adapter = self
                        .adapter_factory
                        .create(Arc::clone(&self.schema), Arc::clone(&file_schema));

                    match adapter.map_schema(file_schema.as_ref()) {
                        Ok((schema_mapper, _projection)) => {
                            self.cached_adapter = Some((file_schema, schema_mapper));
                        }
                        Err(e) => {
                            return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                                "Schema mapping failed: {}",
                                e
                            )))));
                        }
                    }
                }

                let result = self
                    .cached_adapter
                    .as_ref()
                    .expect("cached_adapter should be initialized")
                    .1
                    .map_batch(batch)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Batch mapping failed: {}", e))
                    });

                Poll::Ready(Some(result))
            }
            other => other,
        };

        self.baseline_metrics.record_poll(result)
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
