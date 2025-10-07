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

use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};
use iceberg::io::FileIO;

use crate::execution::operators::ExecutionError;

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
}

impl IcebergScanExec {
    pub fn new(
        metadata_location: String,
        schema: SchemaRef,
        catalog_properties: HashMap<String, String>,
        file_task_groups: Option<Vec<Vec<iceberg::scan::FileScanTask>>>,
        num_partitions: usize,
    ) -> Result<Self, ExecutionError> {
        let output_schema = schema;

        let plan_properties = Self::compute_properties(Arc::clone(&output_schema), num_partitions);

        Ok(Self {
            metadata_location,
            output_schema,
            plan_properties,
            catalog_properties,
            file_task_groups,
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
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Execute pre-planned tasks from Scala (planning happens via Iceberg's Java API)
        if let Some(ref task_groups) = self.file_task_groups {
            if partition < task_groups.len() {
                let tasks = &task_groups[partition];

                return self.execute_with_tasks(tasks.clone());
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
}

impl IcebergScanExec {
    /// Handles MOR (Merge-On-Read) tables by automatically applying positional and equality
    /// deletes via iceberg-rust's ArrowReader.
    fn execute_with_tasks(
        &self,
        tasks: Vec<iceberg::scan::FileScanTask>,
    ) -> DFResult<SendableRecordBatchStream> {
        let output_schema = Arc::clone(&self.output_schema);
        let catalog_properties = self.catalog_properties.clone();
        let metadata_location = self.metadata_location.clone();

        let fut = async move {
            let file_io = Self::load_file_io(&catalog_properties, &metadata_location)?;

            let task_stream = futures::stream::iter(tasks.into_iter().map(Ok)).boxed();

            let reader = iceberg::arrow::ArrowReaderBuilder::new(file_io).build();

            // read() is synchronous and returns Result<ArrowRecordBatchStream>
            let stream = reader.read(task_stream).map_err(|e| {
                DataFusionError::Execution(format!("Failed to read Iceberg tasks: {}", e))
            })?;

            let mapped_stream = stream
                .map_err(|e| DataFusionError::Execution(format!("Iceberg scan error: {}", e)));

            Ok::<_, DataFusionError>(Box::pin(mapped_stream)
                as Pin<
                    Box<dyn futures::Stream<Item = DFResult<arrow::array::RecordBatch>> + Send>,
                >)
        };

        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
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
