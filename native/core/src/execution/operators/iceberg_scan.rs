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

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::ScanMetrics;
use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg::Runtime as IcebergRuntime;
use iceberg::{Error, ErrorKind};
use iceberg_storage_opendal::CustomAwsCredentialLoader;
use iceberg_storage_opendal::OpenDalStorageFactory;

use crate::cloud::s3::credential_bridge::{AccessMode, CometS3CredentialBridge};
use crate::execution::jni_api::get_runtime;
use crate::execution::operators::ExecutionError;
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
use datafusion_comet_spark_expr::EvalMode;
use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use iceberg::scan::FileScanTask;

/// Activation key for the `CometS3CredentialProvider` SPI on the Iceberg path, read from a Spark
/// catalog's `s3.*` property bag.
const ICEBERG_PROVIDER_CLASS_PROPERTY: &str = "s3.comet.credential.provider.class";

/// A valid Parquet file ends with at least an 8-byte footer (4-byte metadata length + "PAR1").
/// A delete file that stats below this cannot be read, so we reject it in the fill step. opendal
/// returns size 0 from a successful HEAD whose response carries no Content-Length header (some
/// S3-compatible endpoints/proxies, or a path-style mismatch), and for a genuinely empty object;
/// neither errors at the stat layer, so without this floor the 0 would flow into the Parquet
/// reader and surface as an opaque "file size of 0 is less than footer".
const MIN_PARQUET_FILE_SIZE: u64 = 8;

/// Iceberg table scan operator that uses iceberg-rust to read Iceberg tables.
///
/// Executes pre-planned FileScanTasks for efficient parallel scanning.
pub struct IcebergScanExec {
    /// Iceberg table metadata location for FileIO initialization
    metadata_location: String,
    /// Output schema after projection
    output_schema: SchemaRef,
    /// Cached execution plan properties
    plan_properties: Arc<PlanProperties>,
    /// Catalog-specific configuration for FileIO. Holds the unfiltered FileIO property bag, which
    /// may contain OAuth tokens, REST `credentials.uri`, and other secrets the credential bridge
    /// needs. Redacted in `Debug` so plan dumps and tracing do not leak credentials.
    catalog_properties: HashMap<String, String>,
    /// Spark V2 catalog name; forwarded as dispatchKey to the credential bridge. Empty when the
    /// table has no catalog identity.
    catalog_name: String,
    /// Pre-planned file scan tasks
    tasks: Vec<FileScanTask>,
    /// Number of data files to read concurrently
    data_file_concurrency_limit: usize,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergScanExec {
    pub fn new(
        metadata_location: String,
        schema: SchemaRef,
        catalog_properties: HashMap<String, String>,
        catalog_name: String,
        tasks: Vec<FileScanTask>,
        data_file_concurrency_limit: usize,
    ) -> Result<Self, ExecutionError> {
        let output_schema = schema;
        let plan_properties = Self::compute_properties(Arc::clone(&output_schema), 1);

        let metrics = ExecutionPlanMetricsSet::new();

        Ok(Self {
            metadata_location,
            output_schema,
            plan_properties,
            catalog_properties,
            catalog_name,
            tasks,
            data_file_concurrency_limit,
            metrics,
        })
    }

    fn compute_properties(schema: SchemaRef, num_partitions: usize) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl ExecutionPlan for IcebergScanExec {
    fn name(&self) -> &str {
        "IcebergScanExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.execute_with_tasks(self.tasks.clone(), context)
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
        tasks: Vec<FileScanTask>,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let output_schema = Arc::clone(&self.output_schema);
        let file_io = Self::load_file_io(
            &self.catalog_properties,
            &self.metadata_location,
            &self.catalog_name,
        )?;
        let batch_size = context.session_config().batch_size();

        let metrics = IcebergScanMetrics::new(&self.metrics);
        metrics.num_splits.add(tasks.len());

        // Fill delete-file sizes as the first step of the task stream so the stats run on the
        // iceberg runtime alongside the reads, not on the calling executor thread (see
        // fill_delete_file_sizes).
        let fill_io = file_io.clone();
        let concurrency_limit = self.data_file_concurrency_limit;
        let task_stream = futures::stream::once(async move {
            let mut tasks = tasks;
            Self::fill_delete_file_sizes(&mut tasks, &fill_io, concurrency_limit).await?;
            Ok::<_, Error>(futures::stream::iter(tasks.into_iter().map(Ok::<_, Error>)))
        })
        .try_flatten()
        .boxed();

        // iceberg-rust's ArrowReader spawns IO/CPU work onto an iceberg::Runtime. execute() runs
        // on the JVM-called thread outside any tokio context, so Runtime::current() would panic;
        // build it from Comet's global runtime, which is where the stream is later polled.
        let reader =
            iceberg::arrow::ArrowReaderBuilder::new(file_io, IcebergRuntime::new(get_runtime()))
                .with_batch_size(batch_size)
                .with_data_file_concurrency_limit(self.data_file_concurrency_limit)
                .with_row_selection_enabled(true)
                .with_metadata_size_hint(512 * 1024) // Same as DataFusion's default
                .build();

        // Pass all tasks to iceberg-rust at once to utilize its flatten_unordered
        // parallelization, avoiding overhead of single-task streams
        let scan_result = reader.read(task_stream).map_err(|e| {
            DataFusionError::Execution(format!("Failed to read Iceberg tasks: {}", e))
        })?;

        let scan_metrics = scan_result.metrics().clone();
        let stream = scan_result.stream();

        let spark_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        let adapter_factory = SparkPhysicalExprAdapterFactory::new(spark_options, None);

        let adapted_stream =
            stream.map_err(|e| DataFusionError::Execution(format!("Iceberg scan error: {}", e)));

        let wrapped_stream = IcebergStreamWrapper {
            inner: adapted_stream,
            schema: output_schema,
            adapter_factory,
            cached: None,
            baseline_metrics: metrics.baseline,
            scan_metrics,
            bytes_scanned: metrics.bytes_scanned,
            last_reported_bytes: 0,
        };

        Ok(Box::pin(wrapped_stream))
    }

    fn storage_factory_for(
        path: &str,
        catalog_properties: &HashMap<String, String>,
        catalog_name: &str,
    ) -> Result<Arc<dyn StorageFactory>, DataFusionError> {
        let scheme = if path.contains("://") {
            path.split("://").next().unwrap_or("file")
        } else {
            "file"
        };
        match scheme {
            "file" => Ok(Arc::new(OpenDalStorageFactory::Fs)),
            "s3" | "s3a" => {
                let customized_credential_load =
                    build_s3_credential_loader(path, catalog_properties, catalog_name);
                Ok(Arc::new(OpenDalStorageFactory::S3 {
                    customized_credential_load,
                }))
            }
            "gs" => Ok(Arc::new(OpenDalStorageFactory::Gcs)),
            "oss" => Ok(Arc::new(OpenDalStorageFactory::Oss)),
            _ => Err(DataFusionError::Execution(format!(
                "Unsupported storage scheme: {scheme}"
            ))),
        }
    }

    /// Stats each unique delete file to fill its `file_size_in_bytes` (0 on arrival, since it is
    /// not serialized).
    ///
    /// iceberg-rust seeks the Parquet footer from this size, so it must be correct. A stat failure
    /// is fatal: reading with missing deletes would silently leak deleted rows.
    async fn fill_delete_file_sizes(
        tasks: &mut [FileScanTask],
        file_io: &FileIO,
        concurrency_limit: usize,
    ) -> Result<(), Error> {
        use datafusion::common::{HashMap, HashSet};
        use futures::TryStreamExt;

        // Dedup: the JVM pools delete-file lists, not individual files, so the same delete file
        // recurs across the tasks that share it. Owning the paths also avoids holding a borrow of
        // `tasks` into the mutable write-back below.
        let mut needed: HashSet<String> = HashSet::new();
        for task in tasks.iter() {
            for delete in &task.deletes {
                // Delete-file sizes are never serialized, so they always arrive as 0. If we ever
                // trust manifest sizes (pending the unreleased apache/iceberg#12554 fix), skip
                // already-sized files here instead of asserting.
                debug_assert_eq!(delete.file_size_in_bytes, 0);
                needed.insert(delete.file_path.clone());
            }
        }
        if needed.is_empty() {
            return Ok(());
        }

        // Bound the in-flight stats to match the downstream read concurrency (iceberg-rust uses
        // try_buffer_unordered at the same limit for delete-file loads). An unbounded fan-out
        // would burst N HEAD requests at once for no gain, since the reads are throttled anyway.
        // Guaranteed > 0 by COMET_ICEBERG_DATA_FILE_CONCURRENCY_LIMIT; buffer_unordered(0) would
        // never poll.
        debug_assert!(concurrency_limit > 0);
        let sizes: Vec<(String, u64)> = futures::stream::iter(needed.into_iter().map(|path| {
            let file_io = file_io.clone();
            async move {
                let size = file_io
                    .new_input(&path)?
                    .metadata()
                    .await
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("Failed to stat delete file '{path}'"),
                        )
                        .with_source(e)
                    })?
                    .size;
                if size < MIN_PARQUET_FILE_SIZE {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Delete file '{path}' statted to {size} bytes, below the \
                             {MIN_PARQUET_FILE_SIZE}-byte Parquet minimum. The object is empty or \
                             truncated, or the stat returned no Content-Length (check the object \
                             store endpoint, TLS, and path-style config)."
                        ),
                    ));
                }
                Ok::<(String, u64), Error>((path, size))
            }
        }))
        .buffer_unordered(concurrency_limit)
        .try_collect()
        .await?;

        let size_map: HashMap<String, u64> = sizes.into_iter().collect();
        for task in tasks.iter_mut() {
            for delete in task.deletes.iter_mut() {
                if let Some(&size) = size_map.get(&delete.file_path) {
                    delete.file_size_in_bytes = size;
                }
            }
        }
        Ok(())
    }

    fn load_file_io(
        catalog_properties: &HashMap<String, String>,
        metadata_location: &str,
        catalog_name: &str,
    ) -> Result<FileIO, DataFusionError> {
        let factory =
            Self::storage_factory_for(metadata_location, catalog_properties, catalog_name)?;
        let mut file_io_builder = FileIOBuilder::new(factory);

        // Narrow to storage-prefix keys before forwarding to iceberg-rust's FileIO. The full
        // unfiltered bag (catalog URI, OAuth tokens, credentials.uri, tenant-id, etc.) is kept
        // upstream so CometS3CredentialBridge can read whatever the vendor needs.
        for (key, value) in catalog_properties {
            if STORAGE_PROPERTY_PREFIXES.iter().any(|p| key.starts_with(p)) {
                file_io_builder = file_io_builder.with_prop(key, value);
            }
        }

        Ok(file_io_builder.build())
    }
}

const STORAGE_PROPERTY_PREFIXES: &[&str] = &["s3.", "gcs.", "adls.", "client."];

/// Wires the configured Comet credential provider into opendal's S3 service, or returns `None`
/// so opendal falls back to its default credential chain.
fn build_s3_credential_loader(
    metadata_location: &str,
    catalog_properties: &HashMap<String, String>,
    catalog_name: &str,
) -> Option<CustomAwsCredentialLoader> {
    let url = url::Url::parse(metadata_location).ok()?;
    let bucket = url.host_str()?;
    let provider_class = catalog_properties
        .get(ICEBERG_PROVIDER_CLASS_PROPERTY)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())?;
    // Fall back to the bucket when the table has no catalog identity (e.g. HadoopTables loaded by
    // raw path).
    let dispatch_key: &str = if catalog_name.is_empty() {
        bucket
    } else {
        catalog_name
    };
    let bridge = CometS3CredentialBridge::new(
        provider_class,
        dispatch_key,
        bucket,
        url.path(),
        AccessMode::Read,
        catalog_properties,
    );
    match bridge {
        Ok(b) => Some(CustomAwsCredentialLoader::new(b)),
        Err(e) => {
            log::warn!(
                "Failed to initialize CometS3CredentialBridge for {provider_class}: {e}; \
                 falling back to default opendal credential chain"
            );
            None
        }
    }
}

/// Metrics for IcebergScanExec
struct IcebergScanMetrics {
    /// Baseline metrics (output rows, elapsed compute time)
    baseline: BaselineMetrics,
    /// Count of file splits (FileScanTasks) processed
    num_splits: Count,
    /// Total bytes read from storage
    bytes_scanned: Count,
}

impl IcebergScanMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, 0),
            num_splits: MetricBuilder::new(metrics).counter("num_splits", 0),
            bytes_scanned: MetricBuilder::new(metrics).counter("bytes_scanned", 0),
        }
    }
}

/// Wrapper around iceberg-rust's stream that performs schema adaptation.
/// Handles batches from multiple files that may have different Arrow schemas
/// (metadata, field IDs, etc.).
struct IcebergStreamWrapper<S> {
    inner: S,
    schema: SchemaRef,
    /// Factory for creating adapters when file schema changes
    adapter_factory: SparkPhysicalExprAdapterFactory,
    /// Cached adapter and projection expressions for the current file schema,
    /// reused across batches with the same schema
    cached: Option<CachedProjection>,
    /// Metrics for output tracking
    baseline_metrics: BaselineMetrics,
    /// Iceberg scan metrics for bytes read tracking
    scan_metrics: ScanMetrics,
    /// DF metric counter bridging iceberg-rust's bytes_read to the metric tree
    bytes_scanned: Count,
    /// Last reported bytes_read value for delta computation
    last_reported_bytes: u64,
}

/// Cached projection state: file schema, adapter, and pre-built projection expressions.
struct CachedProjection {
    file_schema: SchemaRef,
    projection_exprs: Vec<Arc<dyn PhysicalExpr>>,
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

                // Reuse cached projection expressions if file schema hasn't changed.
                // Batches from the same file share the same Arc<Schema> pointer,
                // so pointer equality is sufficient here.
                let projection_exprs = match &self.cached {
                    Some(cached) if Arc::ptr_eq(&cached.file_schema, &file_schema) => {
                        &cached.projection_exprs
                    }
                    _ => {
                        let adapter = self
                            .adapter_factory
                            .create(Arc::clone(&self.schema), Arc::clone(&file_schema))?;
                        let exprs =
                            build_projection_expressions(&self.schema, &adapter).map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to build projection expressions: {}",
                                    e
                                ))
                            })?;
                        self.cached = Some(CachedProjection {
                            file_schema,
                            projection_exprs: exprs,
                        });
                        &self.cached.as_ref().unwrap().projection_exprs
                    }
                };

                let result = adapt_batch_with_expressions(batch, &self.schema, projection_exprs)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Batch adaptation failed: {}", e))
                    });

                Poll::Ready(Some(result))
            }
            other => other,
        };

        // Bridge iceberg-rust's live AtomicU64 counter into the DF metric tree
        let current = self.scan_metrics.bytes_read();
        let delta = current - self.last_reported_bytes;
        if delta > 0 {
            self.bytes_scanned.add(delta as usize);
            self.last_reported_bytes = current;
        }

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
        write!(
            f,
            "IcebergScanExec: metadata_location={}, num_tasks={}",
            self.metadata_location,
            self.tasks.len()
        )
    }
}

impl fmt::Debug for IcebergScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergScanExec")
            .field("metadata_location", &self.metadata_location)
            .field("catalog_name", &self.catalog_name)
            .field(
                "catalog_properties",
                &RedactedProperties(&self.catalog_properties),
            )
            .field("num_tasks", &self.tasks.len())
            .field(
                "data_file_concurrency_limit",
                &self.data_file_concurrency_limit,
            )
            .finish_non_exhaustive()
    }
}

/// Wraps a property map so `Debug` shows keys but elides values, since the unfiltered FileIO bag
/// may contain bearer tokens and OAuth secrets.
struct RedactedProperties<'a>(&'a HashMap<String, String>);

impl fmt::Debug for RedactedProperties<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut m = f.debug_map();
        for k in self.0.keys() {
            m.key(k).value(&"<redacted>");
        }
        m.finish()
    }
}

/// Build projection expressions that adapt batches from a file schema to the target schema.
///
/// The returned expressions can be cached and reused across multiple batches
/// that share the same file schema, avoiding repeated expression construction.
fn build_projection_expressions(
    target_schema: &SchemaRef,
    adapter: &Arc<dyn PhysicalExprAdapter>,
) -> DFResult<Vec<Arc<dyn PhysicalExpr>>> {
    target_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, _field)| {
            let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new_with_schema(
                target_schema.field(i).name(),
                target_schema.as_ref(),
            )?);
            adapter.rewrite(col_expr)
        })
        .collect::<DFResult<Vec<_>>>()
}

/// Adapt a batch to match the target schema using pre-built projection expressions.
///
/// The caller provides pre-built `projection_exprs` (from [`build_projection_expressions`])
/// which can be cached and reused across multiple batches with the same file schema.
fn adapt_batch_with_expressions(
    batch: RecordBatch,
    target_schema: &SchemaRef,
    projection_exprs: &[Arc<dyn PhysicalExpr>],
) -> DFResult<RecordBatch> {
    // If schemas match, no adaptation needed
    if Arc::ptr_eq(&batch.schema(), target_schema) {
        return Ok(batch);
    }

    // Zero-column projection (e.g. SELECT count(*)), preserve row count
    if projection_exprs.is_empty() {
        return Ok(RecordBatch::try_new_with_options(
            Arc::clone(target_schema),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?);
    }

    // Evaluate expressions against batch
    let columns: Vec<ArrayRef> = projection_exprs
        .iter()
        .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
        .collect::<DFResult<Vec<_>>>()?;

    RecordBatch::try_new(Arc::clone(target_schema), columns).map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;

    use iceberg::io::{FileIO, FileIOBuilder};
    use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
    use iceberg::spec::{DataContentType, DataFileFormat, Schema};
    use iceberg_storage_opendal::OpenDalStorageFactory;

    use super::IcebergScanExec;

    fn fs_file_io() -> FileIO {
        FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Fs)).build()
    }

    fn task_with_deletes(deletes: Vec<FileScanTaskDeleteFile>) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: 0,
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: "data.parquet".to_string(),
            data_file_format: DataFileFormat::Parquet,
            schema: Arc::new(Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes,
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        }
    }

    fn delete_file(path: &str) -> FileScanTaskDeleteFile {
        FileScanTaskDeleteFile {
            file_path: path.to_string(),
            file_type: DataContentType::PositionDeletes,
            file_size_in_bytes: 0,
            partition_spec_id: 0,
            equality_ids: None,
        }
    }

    // A delete file we cannot stat must fail the scan, not be silently read with a missing/0 size.
    #[tokio::test]
    async fn fill_delete_file_sizes_errors_on_unreadable_delete_file() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing-delete.parquet");
        let mut tasks = vec![task_with_deletes(vec![delete_file(
            missing.to_str().unwrap(),
        )])];

        let result = IcebergScanExec::fill_delete_file_sizes(&mut tasks, &fs_file_io(), 4).await;

        assert!(
            result.is_err(),
            "expected an error when a delete file cannot be statted"
        );
        assert_eq!(tasks[0].deletes[0].file_size_in_bytes, 0);
    }

    // The real on-disk size is filled in from the FileIO, replacing the 0 placeholder.
    #[tokio::test]
    async fn fill_delete_file_sizes_populates_real_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delete.parquet");
        let bytes = b"these bytes stand in for a delete file";
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(bytes).unwrap();
        f.flush().unwrap();

        let mut tasks = vec![task_with_deletes(vec![delete_file(path.to_str().unwrap())])];
        IcebergScanExec::fill_delete_file_sizes(&mut tasks, &fs_file_io(), 4)
            .await
            .unwrap();

        assert_eq!(tasks[0].deletes[0].file_size_in_bytes, bytes.len() as u64);
    }

    // A present-but-undersized delete file (0-byte object, truncated write, or a HEAD with no
    // Content-Length that opendal reports as size 0) must fail loudly rather than passing a
    // sub-footer size into the Parquet reader.
    #[tokio::test]
    async fn fill_delete_file_sizes_errors_on_subfooter_delete_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty-delete.parquet");
        std::fs::File::create(&path).unwrap(); // 0 bytes on disk

        let mut tasks = vec![task_with_deletes(vec![delete_file(path.to_str().unwrap())])];
        let result = IcebergScanExec::fill_delete_file_sizes(&mut tasks, &fs_file_io(), 4).await;

        assert!(
            result.is_err(),
            "expected an error when a delete file is below the Parquet footer minimum"
        );
        assert_eq!(tasks[0].deletes[0].file_size_in_bytes, 0);
    }

    // No deletes means no stats and no error.
    #[tokio::test]
    async fn fill_delete_file_sizes_noop_without_deletes() {
        let mut tasks = vec![task_with_deletes(vec![])];
        IcebergScanExec::fill_delete_file_sizes(&mut tasks, &fs_file_io(), 4)
            .await
            .unwrap();
    }
}
