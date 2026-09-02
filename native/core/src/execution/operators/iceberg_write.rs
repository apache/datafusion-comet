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

//! Native Iceberg write operator using iceberg-rust.
//!
//! Drains the upstream Arrow stream through iceberg-rust's writer stack
//! (`ParquetWriterBuilder` -> `RollingFileWriterBuilder` -> `DataFileWriterBuilder`
//! -> `Unpartitioned`/`Fanout`/`Clustered`Writer) and emits a single-row, single-column
//! Arrow batch carrying the `Vec<DataFile>` produced for the task, packed as an Iceberg V2
//! data manifest via iceberg-rust's `ManifestWriter` against an in-memory `FileIO`. The JVM
//! decodes the bytes with `ManifestFiles.read(...)` to recover the `DataFile`s for commit.

use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, BinaryArray, RecordBatch, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures::TryStreamExt;
use iceberg::arrow::{
    arrow_struct_to_literal, PartitionValueCalculator, RecordBatchPartitionSplitter,
};
use iceberg::io::FileIO;
use iceberg::spec::{
    DataFile, DataFileFormat, Literal, ManifestWriterBuilder, PartitionKey, PartitionSpec,
    PartitionSpecRef, Schema as IcebergSchema, SchemaRef as IcebergSchemaRef,
    Struct as IcebergStruct, StructType,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::clustered_writer::ClusteredWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::unpartitioned_writer::UnpartitionedWriter;
#[cfg(test)]
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use datafusion_comet_proto::spark_operator::{
    CompressionCodec as ProtoCompressionCodec, IcebergParquetWriteSettings, IcebergWrite,
    IcebergWriteCommon, IcebergWriterMode as ProtoIcebergWriterMode,
};

use crate::cloud::s3::credential_bridge::AccessMode;
use crate::execution::operators::iceberg_common::load_file_io;

/// Builder chain instantiated once per task and handed to the partitioning wrapper.
type IcebergDataFileWriterBuilder = DataFileWriterBuilder<
    ParquetWriterBuilder,
    TrackingLocationGenerator,
    DefaultFileNameGenerator,
>;

/// `DefaultLocationGenerator` that records every location it hands to a file writer.
///
/// iceberg-rust's writers keep the `DataFile`s they have finalized private until `close`, and
/// have no abort hook, so when a task fails partway through there is no other way to learn which
/// files it created. The recorded locations let `delete_task_files` clean up after a failure the
/// way iceberg-java's `DataWriter.abort()` does.
#[derive(Clone, Debug)]
struct TrackingLocationGenerator {
    inner: DefaultLocationGenerator,
    locations: Arc<Mutex<Vec<String>>>,
}

impl TrackingLocationGenerator {
    fn new(data_location: String) -> Self {
        Self {
            inner: DefaultLocationGenerator::with_data_location(data_location),
            locations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn locations(&self) -> Vec<String> {
        self.locations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl LocationGenerator for TrackingLocationGenerator {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        let location = self.inner.generate_location(partition_key, file_name);
        self.locations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(location.clone());
        location
    }
}

/// Deletes the tracked files if the write task is dropped before it finished.
///
/// A task can end without its future ever observing an error: when the JVM-side input iterator
/// throws, `executePlan` returns that error straight from the JNI batch pull and the JVM then
/// releases the plan, dropping this future mid-flight. The guard turns that drop into the same
/// cleanup the explicit error path performs. It stays armed until the task's output batch has
/// been handed to the JVM, which is the point where the JVM takes over cleanup ownership.
struct AbortOnDrop {
    file_io: FileIO,
    generator: TrackingLocationGenerator,
    armed: bool,
}

impl AbortOnDrop {
    /// Every location the task's writers were handed, in the order they were generated.
    fn locations(&self) -> Vec<String> {
        self.generator.locations()
    }

    /// Give up ownership without deleting: the files are now someone else's responsibility.
    fn disarm(&mut self) {
        self.armed = false;
    }

    /// Delete the tracked files, awaiting completion, and give up ownership. Preferred over the
    /// `Drop` path wherever the failure is observed inside the task's own future, so the deletes
    /// finish before the task reports its error rather than racing the runtime's shutdown.
    async fn abort(&mut self) {
        self.armed = false;
        delete_task_files(&self.file_io, self.generator.locations()).await;
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let locations = self.generator.locations();
        if locations.is_empty() {
            return;
        }
        let file_io = self.file_io.clone();
        match tokio::runtime::Handle::try_current() {
            // Dropped from inside a runtime (the stream was dropped while being polled): the
            // delete cannot block here, so hand it to the runtime.
            Ok(handle) => {
                handle.spawn(async move { delete_task_files(&file_io, locations).await });
            }
            // Dropped from a plain JVM thread (`releasePlan`): run the delete to completion so the
            // files are gone before the task reports its failure.
            Err(_) => match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(delete_task_files(&file_io, locations)),
                Err(e) => log::warn!(
                    "Could not build a runtime to delete {} data file(s) left by a failed \
                     Iceberg write task: {e}",
                    locations.len()
                ),
            },
        }
    }
}

/// Best-effort deletion of every file a failed task attempt created, the native counterpart of
/// iceberg-java's `SparkCleanupUtil.deleteTaskFiles`. Failures are logged rather than returned:
/// the original task failure must stay the one Spark reports, and anything left behind is still
/// invisible to readers and reclaimed by `remove_orphan_files`.
async fn delete_task_files(file_io: &FileIO, locations: Vec<String>) {
    if locations.is_empty() {
        return;
    }
    let mut deleted = 0usize;
    for location in &locations {
        match file_io.delete(location).await {
            Ok(()) => deleted += 1,
            Err(e) => log::warn!(
                "Failed to delete data file {location} left by a failed Iceberg write task: {e}"
            ),
        }
    }
    log::info!(
        "Deleted {deleted} of {} data file(s) left by a failed Iceberg write task",
        locations.len()
    );
}

/// Native Iceberg write operator. Owns the parsed Iceberg schema/spec and the parquet writer
/// properties; at task execution it builds the iceberg-rust writer stack, drains the upstream
/// Arrow stream into it, and emits a single Avro-encoded `Vec<DataFile>` row.
pub struct IcebergWriteExec {
    input: Arc<dyn ExecutionPlan>,
    common: Arc<IcebergWriteCommon>,
    iceberg_schema: IcebergSchemaRef,
    partition_spec: PartitionSpecRef,
    writer_mode: ProtoIcebergWriterMode,
    writer_properties: Arc<WriterProperties>,
    partition_id: Option<i32>,
    task_attempt_id: Option<i64>,
    output_schema: SchemaRef,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergWriteExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, proto: IcebergWrite) -> DFResult<Self> {
        let IcebergWrite {
            common,
            partition_id,
            task_attempt_id,
        } = proto;
        let common = common.ok_or_else(|| {
            DataFusionError::Internal("IcebergWrite missing common payload".into())
        })?;
        let settings = common.parquet_settings.as_ref().ok_or_else(|| {
            DataFusionError::Internal("IcebergWriteCommon missing parquet_settings".into())
        })?;
        let writer_properties = build_writer_properties(settings)?;
        let iceberg_schema = parse_iceberg_schema(&common.iceberg_schema_json)?;
        let partition_spec = parse_partition_spec(&common.partition_spec_json)?;
        let writer_mode = ProtoIcebergWriterMode::try_from(common.writer_mode).map_err(|_| {
            DataFusionError::Internal(format!(
                "Unknown IcebergWriterMode proto value: {}",
                common.writer_mode
            ))
        })?;
        let output_schema = build_output_schema();
        let plan_properties = Self::compute_properties(&input, Arc::clone(&output_schema));
        Ok(Self {
            input,
            common: Arc::new(common),
            iceberg_schema,
            partition_spec,
            writer_mode,
            writer_properties: Arc::new(writer_properties),
            partition_id,
            task_attempt_id,
            output_schema,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ))
    }
}

impl ExecutionPlan for IcebergWriteExec {
    fn name(&self) -> &str {
        "IcebergWriteExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        // IcebergWriteExec holds no physical expressions; the write is driven by the input
        // stream and the table's partition spec, so there is nothing to visit here.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IcebergWriteExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children.pop().unwrap(),
            common: Arc::clone(&self.common),
            iceberg_schema: Arc::clone(&self.iceberg_schema),
            partition_spec: Arc::clone(&self.partition_spec),
            writer_mode: self.writer_mode,
            writer_properties: Arc::clone(&self.writer_properties),
            partition_id: self.partition_id,
            task_attempt_id: self.task_attempt_id,
            output_schema: Arc::clone(&self.output_schema),
            plan_properties: Arc::clone(&self.plan_properties),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Time spent inside the iceberg-rust writer stack (write + close), excluding time spent
        // waiting on the upstream input stream. Surfaced on the JVM exec's SQL metrics by name.
        let write_time = MetricBuilder::new(&self.metrics).subset_time("write_time", partition);
        let input_stream = self.input.execute(partition, context)?;
        let common = Arc::clone(&self.common);
        let iceberg_schema = Arc::clone(&self.iceberg_schema);
        let partition_spec = Arc::clone(&self.partition_spec);
        let writer_mode = self.writer_mode;
        let writer_properties = Arc::clone(&self.writer_properties);
        let partition_id = self.partition_id;
        let task_attempt_id = self.task_attempt_id;
        let output_schema = Arc::clone(&self.output_schema);

        let task = async move {
            let (data_files, mut abort_guard) = run_write_task(
                input_stream,
                Arc::clone(&common),
                Arc::clone(&iceberg_schema),
                Arc::clone(&partition_spec),
                writer_mode,
                writer_properties.as_ref().clone(),
                partition_id,
                task_attempt_id,
                write_time,
            )
            .await?;
            // The guard is still armed: until the output batch reaches the JVM nothing else knows
            // which files this attempt created, so a failure while packaging it has to delete
            // them here.
            let locations = abort_guard.locations();
            let packaged = async {
                let manifest_bytes = encode_data_files_as_manifest(
                    data_files,
                    iceberg_schema,
                    partition_spec,
                    partition_id,
                    task_attempt_id,
                    &common.operation_id,
                )
                .await?;
                build_output_batch(manifest_bytes, &locations, &output_schema)
            }
            .await;
            match packaged {
                // The batch carries the locations, and the JVM takes cleanup ownership of them
                // before it decodes the manifest, so the guard's job is done.
                Ok(batch) => {
                    abort_guard.disarm();
                    Ok::<_, DataFusionError>(futures::stream::iter(vec![Ok(batch)]))
                }
                Err(e) => {
                    abort_guard.abort().await;
                    Err(e)
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            futures::stream::once(task).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl fmt::Debug for IcebergWriteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergWriteExec")
            .field("metadata_location", &self.common.metadata_location)
            .field("data_location", &self.common.data_location)
            .field("operation_id", &self.common.operation_id)
            .field("writer_mode", &self.writer_mode)
            .field("partition_id", &self.partition_id)
            .field("task_attempt_id", &self.task_attempt_id)
            .finish()
    }
}

impl DisplayAs for IcebergWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IcebergWriteExec: metadata_location={}, data_location={}, operation_id={}",
            self.common.metadata_location, self.common.data_location, self.common.operation_id
        )
    }
}

/// One-shot per-task write coroutine. Builds the iceberg-rust writer stack, decorates each input
/// batch with `PARQUET_FIELD_ID_META_KEY` metadata so iceberg-rust can match Arrow columns to
/// Iceberg field IDs, and routes through `UnpartitionedWriter`/`FanoutWriter`/`ClusteredWriter`
/// depending on `writer_mode`.
///
/// On success the still-armed [`AbortOnDrop`] is returned along with the data files: the caller
/// owns cleanup until the output batch has been handed to the JVM.
#[allow(clippy::too_many_arguments)]
async fn run_write_task(
    mut input: SendableRecordBatchStream,
    common: Arc<IcebergWriteCommon>,
    iceberg_schema: IcebergSchemaRef,
    partition_spec: PartitionSpecRef,
    writer_mode: ProtoIcebergWriterMode,
    writer_properties: WriterProperties,
    partition_id: Option<i32>,
    task_attempt_id: Option<i64>,
    write_time: Time,
) -> DFResult<(Vec<DataFile>, AbortOnDrop)> {
    // The JVM exec wrapper stamps both ids per task; a missing id means the plan template was
    // executed directly, and defaulting would make every task collide on the same file names.
    let partition_id = partition_id.ok_or_else(|| {
        DataFusionError::Internal("IcebergWrite executed without a partition_id".into())
    })?;
    let task_attempt_id = task_attempt_id.ok_or_else(|| {
        DataFusionError::Internal("IcebergWrite executed without a task_attempt_id".into())
    })?;
    let catalog_properties = common
        .catalog_properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let file_io = load_file_io(
        &catalog_properties,
        &common.data_location,
        &common.catalog_name,
        AccessMode::Write,
    )?;

    let location_generator = TrackingLocationGenerator::new(common.data_location.clone());
    let file_name_generator = DefaultFileNameGenerator::new(
        file_name_prefix(partition_id, task_attempt_id, &common.operation_id),
        None,
        DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(writer_properties, Arc::clone(&iceberg_schema));
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        common.target_file_size_bytes as usize,
        file_io.clone(),
        location_generator.clone(),
        file_name_generator,
    );
    let data_file_builder = DataFileWriterBuilder::new(rolling_builder);
    let mut abort_guard = AbortOnDrop {
        file_io,
        generator: location_generator,
        armed: true,
    };

    let unpartitioned = partition_spec.is_unpartitioned();
    let mut writer = match (unpartitioned, writer_mode) {
        (true, ProtoIcebergWriterMode::IcebergWriterUnpartitioned) => {
            InnerWriter::Unpartitioned(UnpartitionedWriter::new(data_file_builder))
        }
        (false, ProtoIcebergWriterMode::IcebergWriterFanout) => {
            InnerWriter::Fanout(FanoutWriter::new(data_file_builder))
        }
        (false, ProtoIcebergWriterMode::IcebergWriterClustered) => {
            InnerWriter::Clustered(ClusteredWriter::new(data_file_builder))
        }
        (actual, mode) => {
            return Err(DataFusionError::Internal(format!(
                "IcebergWrite writer_mode {mode:?} is inconsistent with the partition spec \
                 (unpartitioned={actual})"
            )))
        }
    };

    let clustered_splitter = match &writer {
        InnerWriter::Clustered(_) => Some(ClusteredBatchSplitter::try_new(
            Arc::clone(&partition_spec),
            Arc::clone(&iceberg_schema),
        )?),
        _ => None,
    };
    let fanout_splitter = match &writer {
        InnerWriter::Fanout(_) => Some(
            RecordBatchPartitionSplitter::try_new_with_computed_values(
                Arc::clone(&iceberg_schema),
                Arc::clone(&partition_spec),
            )
            .map_err(iceberg_err)?,
        ),
        _ => None,
    };

    // Build the field-id-decorated target schema once per task; every batch is cast against it.
    let target_schema =
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).map_err(iceberg_err)?);
    let outcome = async move {
        while let Some(batch) = input.try_next().await? {
            let decorated = decorate_batch_with_field_ids(batch, &target_schema)?;
            let _timer = write_time.timer();
            writer
                .write(
                    decorated,
                    fanout_splitter.as_ref(),
                    clustered_splitter.as_ref(),
                )
                .await?;
        }
        let _timer = write_time.timer();
        writer.close().await
    }
    .await;
    match outcome {
        // Hand the still-armed guard to the caller: manifest encoding and output-batch
        // construction can still fail, and until the batch reaches the JVM nothing else knows
        // which files this attempt created.
        Ok(data_files) => Ok((data_files, abort_guard)),
        // Whether the input stream, a write, or the close failed, every file this attempt created
        // is orphaned from here on: nothing will commit it, and the retry uses attempt-unique
        // names.
        Err(e) => {
            abort_guard.abort().await;
            Err(e)
        }
    }
}

/// Enum-based dispatch over the three iceberg-rust partitioning writers. Each variant takes the
/// same builder chain so we can keep the type fixed.
enum InnerWriter {
    Unpartitioned(UnpartitionedWriter<IcebergDataFileWriterBuilder>),
    Fanout(FanoutWriter<IcebergDataFileWriterBuilder>),
    Clustered(ClusteredWriter<IcebergDataFileWriterBuilder>),
}

impl InnerWriter {
    async fn write(
        &mut self,
        batch: RecordBatch,
        fanout_splitter: Option<&RecordBatchPartitionSplitter>,
        clustered_splitter: Option<&ClusteredBatchSplitter>,
    ) -> DFResult<()> {
        use iceberg::writer::partitioning::PartitioningWriter;
        match self {
            InnerWriter::Unpartitioned(w) => w.write(batch).await.map_err(iceberg_err),
            InnerWriter::Fanout(w) => {
                let parts = fanout_splitter
                    .expect("fanout splitter must be Some for fanout writes")
                    .split(&batch)
                    .map_err(iceberg_err)?;
                for (key, part) in parts {
                    w.write(key, part).await.map_err(iceberg_err)?;
                }
                Ok(())
            }
            InnerWriter::Clustered(w) => {
                let parts = clustered_splitter
                    .expect("clustered splitter must be Some for clustered writes")
                    .split(&batch)?;
                for (key, part) in parts {
                    w.write(key, part).await.map_err(iceberg_err)?;
                }
                Ok(())
            }
        }
    }

    async fn close(self) -> DFResult<Vec<DataFile>> {
        use iceberg::writer::partitioning::PartitioningWriter;
        match self {
            InnerWriter::Unpartitioned(w) => w.close().await.map_err(iceberg_err),
            InnerWriter::Fanout(w) => w.close().await.map_err(iceberg_err),
            InnerWriter::Clustered(w) => w.close().await.map_err(iceberg_err),
        }
    }
}

// --- helpers -------------------------------------------------------------

fn parse_iceberg_schema(json: &str) -> DFResult<IcebergSchemaRef> {
    let schema: IcebergSchema = serde_json::from_str(json).map_err(|e| {
        DataFusionError::Internal(format!("Failed to parse iceberg schema JSON: {e}"))
    })?;
    Ok(Arc::new(schema))
}

fn parse_partition_spec(json: &str) -> DFResult<PartitionSpecRef> {
    let spec: PartitionSpec = serde_json::from_str(json).map_err(|e| {
        DataFusionError::Internal(format!("Failed to parse partition spec JSON: {e}"))
    })?;
    Ok(Arc::new(spec))
}

fn iceberg_err(e: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// The per-task output: the V2 data manifest the JVM decodes into `DataFile`s, plus the plain
/// list of locations the task's writers were handed.
///
/// The locations are redundant with the manifest, and deliberately so: they let the JVM take
/// cleanup ownership of the written files with a `ByteBuffer` walk, before it runs the far more
/// allocation-hungry Avro decode that recovers the `DataFile`s. A failure in that decode would
/// otherwise leave files that only the failed decode could have named. See `decodeLocations` and
/// `WrittenFileCleanup` on the JVM side (`CometIcebergWriteExec`).
fn build_output_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("iceberg_manifest", DataType::Binary, false),
        Field::new("written_file_locations", DataType::Binary, false),
    ]))
}

/// Align an input batch with the field-id-decorated target schema by casting each column. The
/// caller is responsible for building `target_schema` once per task via
/// `iceberg::arrow::schema_to_arrow_schema` — it carries `PARQUET_FIELD_ID_META_KEY` on every
/// nested field, and `arrow::compute::cast` rebuilds the column structure to match while
/// reusing data buffers. This is the same conformance step the iceberg-rust DataFusion
/// integration gets for free from DataFusion's `INSERT INTO` planner.
fn decorate_batch_with_field_ids(
    batch: RecordBatch,
    target_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    if batch.num_columns() != target_schema.fields().len() {
        return Err(DataFusionError::Plan(format!(
            "Iceberg write column count mismatch: arrow batch has {} columns but schema has {}",
            batch.num_columns(),
            target_schema.fields().len()
        )));
    }
    // safe:false so a lossy type divergence fails the task instead of writing silent NULLs.
    let cast_options = arrow::compute::CastOptions {
        safe: false,
        ..Default::default()
    };
    let casted: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .zip(target_schema.fields().iter())
        .map(|(col, target)| {
            arrow::compute::cast_with_options(col, target.data_type(), &cast_options)
        })
        .collect::<Result<_, _>>()
        .map_err(DataFusionError::from)?;
    RecordBatch::try_new(Arc::clone(target_schema), casted).map_err(DataFusionError::from)
}

fn file_name_prefix(partition_id: i32, task_attempt_id: i64, operation_id: &str) -> String {
    format!("{partition_id:05}-{task_attempt_id:05}-{operation_id}")
}

/// Splits each batch into contiguous runs of equal partition value, in batch order.
///
/// `RecordBatchPartitionSplitter::split` computes the partition transforms internally and groups
/// rows through a HashMap, which emits parts in unspecified order -- and `ClusteredWriter`
/// hard-errors when a closed partition is revisited, so the clustered path needs the batch's own
/// (partition-clustered) order back. Splitting on run boundaries preserves that order by
/// construction and computes the transforms exactly once. Input that is not actually clustered
/// yields multiple runs with the same key and surfaces the same `ClusteredWriter` error the
/// splitter path would have produced.
struct ClusteredBatchSplitter {
    calculator: PartitionValueCalculator,
    partition_type: StructType,
    partition_spec: PartitionSpecRef,
    schema: IcebergSchemaRef,
}

impl ClusteredBatchSplitter {
    fn try_new(partition_spec: PartitionSpecRef, schema: IcebergSchemaRef) -> DFResult<Self> {
        Ok(Self {
            calculator: PartitionValueCalculator::try_new(&partition_spec, &schema)
                .map_err(iceberg_err)?,
            partition_type: partition_spec
                .partition_type(&schema)
                .map_err(iceberg_err)?,
            partition_spec,
            schema,
        })
    }

    fn split(&self, batch: &RecordBatch) -> DFResult<Vec<(PartitionKey, RecordBatch)>> {
        let partition_array = self.calculator.calculate(batch).map_err(iceberg_err)?;
        let literals =
            arrow_struct_to_literal(&partition_array, &self.partition_type).map_err(iceberg_err)?;
        let mut runs: Vec<(IcebergStruct, usize, usize)> = Vec::new();
        for (row, literal) in literals.into_iter().enumerate() {
            let value = match literal {
                Some(Literal::Struct(value)) => value,
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "partition value is not a struct literal: {other:?}"
                    )))
                }
            };
            match runs.last_mut() {
                Some((current, _, len)) if *current == value => *len += 1,
                _ => runs.push((value, row, 1)),
            }
        }
        // Single-run batches (the common case: one partition per task batch) pass through as
        // zero-copy clones -- the arrays keep their original zero offsets, so the slice hazard
        // in `materialize_run`'s comment does not apply.
        if runs.len() == 1 {
            let (value, _, _) = runs.pop().expect("runs has exactly one element");
            return Ok(vec![(self.partition_key(value), batch.clone())]);
        }
        // One sequential index array per batch; each run gathers through a zero-copy slice of
        // it, so the per-run cost is O(run length) and the per-batch total is O(batch rows).
        let indices = UInt32Array::from_iter_values(0..batch.num_rows() as u32);
        runs.into_iter()
            .map(|(value, start, len)| {
                let part = materialize_run(batch, &indices.slice(start, len))?;
                Ok((self.partition_key(value), part))
            })
            .collect()
    }

    // `PartitionKey::new` takes its `PartitionSpec` by value (`copy_with_data` clones too), so
    // one spec clone per run is the floor with the current iceberg-rust API.
    fn partition_key(&self, value: IcebergStruct) -> PartitionKey {
        PartitionKey::new(
            self.partition_spec.as_ref().clone(),
            Arc::clone(&self.schema),
            value,
        )
    }
}

/// Gather the rows selected by `indices` out of `batch`. A zero-copy `RecordBatch::slice` would
/// be cheaper, but the parquet writer's NaN-count visitor reads list/map children via
/// `list_array.values()`, which ignores a slice's offset window -- sliced list-of-float columns
/// would over-count NaNs. `take` gathers the referenced children into fresh compacted arrays,
/// keeping those counts correct.
fn materialize_run(batch: &RecordBatch, indices: &UInt32Array) -> DFResult<RecordBatch> {
    arrow::compute::take_record_batch(batch, indices).map_err(DataFusionError::from)
}

/// Serialise the produced data files as an in-memory Iceberg V2 data manifest, then read the
/// manifest bytes back out. The JVM side decodes these bytes with `ManifestFiles.read(...)` to
/// recover the `DataFile`s.
///
/// The manifest entries carry a placeholder `snapshot_id` (`None` -> `UNASSIGNED_SNAPSHOT_ID =
/// -1`) and a placeholder `sequence_number` of `0`. Neither is meaningful here: the JVM ignores
/// the entry-level fields and only consumes the embedded `DataFile`s, which the driver later
/// re-stamps with the real snapshot id during `BatchWrite.commit`.
async fn encode_data_files_as_manifest(
    data_files: Vec<DataFile>,
    iceberg_schema: IcebergSchemaRef,
    partition_spec: PartitionSpecRef,
    partition_id: Option<i32>,
    task_attempt_id: Option<i64>,
    operation_id: &str,
) -> DFResult<Vec<u8>> {
    // The manifest is assembled entirely in-process via the `memory` scheme, so the credential
    // dispatch key / access mode are inert here. Each opendal memory backend owns a fresh
    // in-process store (no process-global state), so the manifest bytes are freed when this
    // `FileIO` drops at function return.
    let memory_io = load_file_io(
        &std::collections::HashMap::new(),
        "memory:///",
        "",
        AccessMode::Write,
    )?;
    let path = format!(
        "memory:///comet-manifest-{:05}-{:05}-{}.avro",
        partition_id.unwrap_or(0),
        task_attempt_id.unwrap_or(0),
        operation_id,
    );
    let output_file = memory_io.new_output(&path).map_err(iceberg_err)?;
    let mut manifest_writer =
        ManifestWriterBuilder::new(output_file, None, iceberg_schema, (*partition_spec).clone())
            .build_v2_data();
    for data_file in data_files {
        manifest_writer
            .add_file(data_file, 0)
            .map_err(iceberg_err)?;
    }
    manifest_writer
        .write_manifest_file()
        .await
        .map_err(iceberg_err)?;
    let bytes = memory_io
        .new_input(&path)
        .map_err(iceberg_err)?
        .read()
        .await
        .map_err(iceberg_err)?;
    Ok(bytes.to_vec())
}

/// Frame the written-file locations for the `written_file_locations` column: a big-endian `i32`
/// count, then a big-endian `i32` byte length and the UTF-8 bytes for each location. Explicit
/// lengths rather than a separator so a path is never re-interpreted, whatever it contains.
fn encode_locations(locations: &[String]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(
        4 + locations.len() * 4 + locations.iter().map(|l| l.len()).sum::<usize>(),
    );
    encoded.extend_from_slice(&(locations.len() as i32).to_be_bytes());
    for location in locations {
        encoded.extend_from_slice(&(location.len() as i32).to_be_bytes());
        encoded.extend_from_slice(location.as_bytes());
    }
    encoded
}

fn build_output_batch(
    manifest_bytes: Vec<u8>,
    locations: &[String],
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let manifest: ArrayRef = Arc::new(BinaryArray::from(vec![manifest_bytes.as_slice()]));
    let encoded_locations = encode_locations(locations);
    let locations: ArrayRef = Arc::new(BinaryArray::from(vec![encoded_locations.as_slice()]));
    RecordBatch::try_new(Arc::clone(output_schema), vec![manifest, locations])
        .map_err(DataFusionError::from)
}

/// Translate `IcebergParquetWriteSettings` into parquet-rs `WriterProperties`.
///
/// Iceberg defaults are applied by the JVM-side translator; this function trusts the wire and
/// only re-applies parquet-rs-shaped settings.
///
/// Footer statistics are always written in full and never truncated, matching parquet-mr
/// (which has no footer-stat truncation). Iceberg's metrics modes
/// (`write.metadata.metrics.*`) do not apply here: they shape the *manifest* metrics, which
/// the JVM re-derives from the footer with Iceberg's own `MetricsConfig` logic before commit.
fn build_writer_properties(settings: &IcebergParquetWriteSettings) -> DFResult<WriterProperties> {
    let compression = compression_from_proto(settings.compression, settings.compression_level)?;
    Ok(WriterProperties::builder()
        .set_compression(compression)
        .set_created_by(settings.created_by.clone())
        .set_max_row_group_bytes(Some(settings.row_group_size_bytes as usize))
        // parquet-rs also caps row groups at 1Mi rows by default; parquet-mr flushes purely by
        // estimated byte size (`parquet.block.size`), so drop the row cap for the same cadence.
        .set_max_row_group_row_count(None)
        .set_data_page_size_limit(settings.page_size_bytes as usize)
        .set_dictionary_page_size_limit(settings.dict_size_bytes as usize)
        .set_data_page_row_count_limit(settings.page_row_limit as usize)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_statistics_truncate_length(None)
        .build())
}

fn compression_from_proto(codec: i32, level: Option<i32>) -> DFResult<Compression> {
    let codec = ProtoCompressionCodec::try_from(codec).map_err(|_| {
        DataFusionError::Internal(format!("Unknown CompressionCodec proto value: {codec}"))
    })?;
    match codec {
        ProtoCompressionCodec::None => Ok(Compression::UNCOMPRESSED),
        ProtoCompressionCodec::Snappy => Ok(Compression::SNAPPY),
        ProtoCompressionCodec::Lz4 => Ok(Compression::LZ4),
        ProtoCompressionCodec::Zstd => {
            let lvl = level.unwrap_or(ZstdLevel::default().compression_level());
            let zstd = ZstdLevel::try_new(lvl).map_err(|e| {
                DataFusionError::Internal(format!("Invalid zstd compression level {lvl}: {e}"))
            })?;
            Ok(Compression::ZSTD(zstd))
        }
        ProtoCompressionCodec::Gzip => {
            let lvl = match level {
                Some(v) => u32::try_from(v).map_err(|_| {
                    DataFusionError::Internal(format!("Negative gzip compression level: {v}"))
                })?,
                None => GzipLevel::default().compression_level(),
            };
            let gzip = GzipLevel::try_new(lvl).map_err(|e| {
                DataFusionError::Internal(format!("Invalid gzip compression level {lvl}: {e}"))
            })?;
            Ok(Compression::GZIP(gzip))
        }
        ProtoCompressionCodec::Brotli => {
            let lvl = match level {
                Some(v) => u32::try_from(v).map_err(|_| {
                    DataFusionError::Internal(format!("Negative brotli compression level: {v}"))
                })?,
                None => BrotliLevel::default().compression_level(),
            };
            let brotli = BrotliLevel::try_new(lvl).map_err(|e| {
                DataFusionError::Internal(format!("Invalid brotli compression level {lvl}: {e}"))
            })?;
            Ok(Compression::BROTLI(brotli))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::io::FileIOBuilder;
    use iceberg_storage_opendal::OpenDalStorageFactory;

    #[tokio::test]
    async fn failed_task_deletes_every_tracked_location() {
        let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Memory)).build();
        let generator = TrackingLocationGenerator::new("memory:/warehouse/data".to_string());
        // Two files the writer "created", plus one location that was handed out but never
        // written (the file open at the time of the failure): deleting it must not fail.
        let first = generator.generate_location(None, "00000-00001-op-00001.parquet");
        let second = generator.generate_location(None, "00000-00001-op-00002.parquet");
        let never_written = generator.generate_location(None, "00000-00001-op-00003.parquet");
        for location in [&first, &second] {
            file_io
                .new_output(location)
                .unwrap()
                .write(bytes::Bytes::from_static(b"parquet"))
                .await
                .unwrap();
        }
        assert_eq!(
            generator.locations(),
            vec![first.clone(), second.clone(), never_written.clone()]
        );
        assert!(file_io.exists(&first).await.unwrap());

        delete_task_files(&file_io, generator.locations()).await;

        assert!(!file_io.exists(&first).await.unwrap());
        assert!(!file_io.exists(&second).await.unwrap());
        assert!(!file_io.exists(&never_written).await.unwrap());
    }

    #[test]
    fn dropping_an_armed_guard_outside_a_runtime_deletes_the_files() {
        let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Memory)).build();
        let generator = TrackingLocationGenerator::new("memory:/warehouse/data".to_string());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let armed = generator.generate_location(None, "armed.parquet");
        let disarmed = generator.generate_location(None, "disarmed.parquet");
        runtime.block_on(async {
            for location in [&armed, &disarmed] {
                file_io
                    .new_output(location)
                    .unwrap()
                    .write(bytes::Bytes::from_static(b"parquet"))
                    .await
                    .unwrap();
            }
        });

        // Disarmed: the task completed, nothing may be deleted.
        let mut guard = AbortOnDrop {
            file_io: file_io.clone(),
            generator: generator.clone(),
            armed: true,
        };
        guard.disarm();
        drop(guard);
        assert!(runtime.block_on(file_io.exists(&armed)).unwrap());

        // Armed and dropped from a thread without a runtime, as `releasePlan` does: the delete
        // runs to completion before `drop` returns.
        drop(AbortOnDrop {
            file_io: file_io.clone(),
            generator,
            armed: true,
        });
        assert!(!runtime.block_on(file_io.exists(&armed)).unwrap());
        assert!(!runtime.block_on(file_io.exists(&disarmed)).unwrap());
    }

    #[test]
    fn tracked_locations_follow_the_default_layout() {
        let generator = TrackingLocationGenerator::new("s3://bucket/table/data".to_string());
        let location = generator.generate_location(None, "f.parquet");
        assert_eq!(location, "s3://bucket/table/data/f.parquet");
        assert_eq!(generator.locations(), vec![location]);
    }
    use datafusion_comet_proto::spark_operator::CompressionCodec as ProtoCodec;

    fn base_settings() -> IcebergParquetWriteSettings {
        IcebergParquetWriteSettings {
            compression: ProtoCodec::Zstd as i32,
            compression_level: Some(3),
            row_group_size_bytes: 128 * 1024 * 1024,
            page_size_bytes: 1024 * 1024,
            dict_size_bytes: 2 * 1024 * 1024,
            page_row_limit: 20_000,
            created_by: "Apache Iceberg (Comet test)".to_string(),
        }
    }

    #[test]
    fn translates_default_settings_to_zstd_3() {
        let props = build_writer_properties(&base_settings()).unwrap();
        assert!(matches!(
            props.compression(&"any".into()),
            Compression::ZSTD(_)
        ));
        if let Compression::ZSTD(level) = props.compression(&"any".into()) {
            assert_eq!(level.compression_level(), 3);
        } else {
            panic!("expected zstd compression");
        }
    }

    #[test]
    fn translates_each_codec_to_matching_parquet_compression() {
        let codecs = [
            (ProtoCodec::None, Compression::UNCOMPRESSED),
            (ProtoCodec::Snappy, Compression::SNAPPY),
            (ProtoCodec::Lz4, Compression::LZ4),
        ];
        for (proto, expected) in codecs {
            let mut settings = base_settings();
            settings.compression = proto as i32;
            settings.compression_level = None;
            let props = build_writer_properties(&settings).unwrap();
            assert_eq!(props.compression(&"c".into()), expected, "codec={proto:?}");
        }
    }

    #[test]
    fn translates_gzip_with_explicit_level() {
        let mut settings = base_settings();
        settings.compression = ProtoCodec::Gzip as i32;
        settings.compression_level = Some(9);
        let props = build_writer_properties(&settings).unwrap();
        match props.compression(&"c".into()) {
            Compression::GZIP(level) => assert_eq!(level.compression_level(), 9),
            other => panic!("expected gzip, got {other:?}"),
        }
    }

    #[test]
    fn translates_brotli_default_level_to_one() {
        let mut settings = base_settings();
        settings.compression = ProtoCodec::Brotli as i32;
        settings.compression_level = None;
        let props = build_writer_properties(&settings).unwrap();
        match props.compression(&"c".into()) {
            Compression::BROTLI(level) => assert_eq!(level.compression_level(), 1),
            other => panic!("expected brotli, got {other:?}"),
        }
    }

    #[test]
    fn translates_size_settings_to_parquet_setters() {
        let mut settings = base_settings();
        settings.row_group_size_bytes = 64 * 1024 * 1024;
        settings.page_size_bytes = 65_536;
        settings.dict_size_bytes = 1_048_576;
        settings.page_row_limit = 1_000;
        let props = build_writer_properties(&settings).unwrap();
        assert_eq!(props.max_row_group_bytes(), Some(64 * 1024 * 1024));
        assert_eq!(props.data_page_size_limit(), 65_536);
        assert_eq!(props.dictionary_page_size_limit(), 1_048_576);
        assert_eq!(props.data_page_row_count_limit(), 1_000);
    }

    #[test]
    fn passes_created_by_through() {
        let mut settings = base_settings();
        settings.created_by = "Apache Iceberg 1.7.1 (Comet 0.16.0)".to_string();
        let props = build_writer_properties(&settings).unwrap();
        assert_eq!(props.created_by(), "Apache Iceberg 1.7.1 (Comet 0.16.0)");
    }

    #[test]
    fn rejects_unknown_codec() {
        let mut settings = base_settings();
        settings.compression = 999;
        let err = build_writer_properties(&settings).unwrap_err();
        assert!(format!("{err}").contains("Unknown CompressionCodec"));
    }

    #[test]
    fn rejects_out_of_range_zstd_level() {
        let mut settings = base_settings();
        settings.compression = ProtoCodec::Zstd as i32;
        settings.compression_level = Some(100);
        let err = build_writer_properties(&settings).unwrap_err();
        assert!(format!("{err}").contains("zstd"));
    }

    #[test]
    fn build_output_schema_carries_the_manifest_and_the_written_locations() {
        let schema = build_output_schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "iceberg_manifest");
        assert_eq!(schema.field(0).data_type(), &DataType::Binary);
        assert_eq!(schema.field(1).name(), "written_file_locations");
        assert_eq!(schema.field(1).data_type(), &DataType::Binary);
    }

    #[test]
    fn encoded_locations_round_trip_through_the_jvm_framing() {
        // Mirrors `CometIcebergWriteExec.decodeLocations`; a path containing a newline is decoded
        // as one location rather than two.
        fn decode(encoded: &[u8]) -> Vec<String> {
            let mut out = Vec::new();
            let count = i32::from_be_bytes(encoded[0..4].try_into().unwrap());
            let mut at = 4usize;
            for _ in 0..count {
                let len = i32::from_be_bytes(encoded[at..at + 4].try_into().unwrap()) as usize;
                at += 4;
                out.push(String::from_utf8(encoded[at..at + len].to_vec()).unwrap());
                at += len;
            }
            assert_eq!(at, encoded.len());
            out
        }
        let locations = vec![
            "s3://bucket/table/data/00000-00001-op-00001.parquet".to_string(),
            "file:/tmp/t/data/region=a%2Fb/00000-00001-op-00002.parquet".to_string(),
            "file:/tmp/t/data/odd\nname.parquet".to_string(),
            "file:/tmp/t/data/日本.parquet".to_string(),
        ];
        assert_eq!(decode(&encode_locations(&locations)), locations);
        assert_eq!(decode(&encode_locations(&[])), Vec::<String>::new());
    }

    #[test]
    fn file_name_prefix_pads_ids() {
        let prefix = file_name_prefix(7, 42, "op-abc");
        assert_eq!(prefix, "00007-00042-op-abc");
    }

    // -- Integration tests against the real iceberg-rust writer stack ---------

    mod integration {
        use super::super::*;
        use arrow::array::{Int32Array, StringArray};
        use datafusion::common::Result as DFResult;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion_comet_proto::spark_operator::{
            CompressionCodec as ProtoCodec, IcebergParquetWriteSettings, IcebergWriteCommon,
            IcebergWriterMode as ProtoIcebergWriterMode,
        };
        use iceberg::spec::{
            Manifest, NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type,
        };
        use parquet::file::properties::WriterProperties;
        use std::collections::HashMap;
        use std::path::PathBuf;
        use std::sync::Arc;
        use tempfile::TempDir;

        fn user_schema() -> SchemaRef {
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("region", DataType::Utf8, false),
            ]))
        }

        fn iceberg_user_schema() -> Schema {
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap()
        }

        fn batch(ids: &[i32], regions: &[&str]) -> RecordBatch {
            RecordBatch::try_new(
                user_schema(),
                vec![
                    Arc::new(Int32Array::from(ids.to_vec())),
                    Arc::new(StringArray::from(regions.to_vec())),
                ],
            )
            .unwrap()
        }

        fn input_stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
            let schema = user_schema();
            Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)),
            ))
        }

        fn common(
            data_location: String,
            spec_json: String,
            schema_json: String,
            writer_mode: ProtoIcebergWriterMode,
        ) -> Arc<IcebergWriteCommon> {
            let settings = IcebergParquetWriteSettings {
                compression: ProtoCodec::Zstd as i32,
                compression_level: Some(3),
                row_group_size_bytes: 128 * 1024 * 1024,
                page_size_bytes: 1024 * 1024,
                dict_size_bytes: 2 * 1024 * 1024,
                page_row_limit: 20_000,
                created_by: "Apache Iceberg (Comet integration test)".to_string(),
            };
            Arc::new(IcebergWriteCommon {
                catalog_properties: HashMap::new(),
                metadata_location: "file:/tmp/metadata.json".to_string(),
                iceberg_schema_json: schema_json,
                partition_spec_json: spec_json,
                sort_order_id: 0,
                data_location,
                operation_id: "test-op".to_string(),
                target_file_size_bytes: 512 * 1024 * 1024,
                writer_mode: writer_mode as i32,
                parquet_settings: Some(settings),
                catalog_name: String::new(),
            })
        }

        async fn run(
            common: Arc<IcebergWriteCommon>,
            schema: Schema,
            spec: PartitionSpec,
            writer_mode: ProtoIcebergWriterMode,
            batches: Vec<RecordBatch>,
        ) -> DFResult<Vec<DataFile>> {
            let (data_files, mut abort_guard) = run_write_task(
                input_stream(batches),
                common,
                Arc::new(schema),
                Arc::new(spec),
                writer_mode,
                WriterProperties::builder().build(),
                Some(0),
                Some(0),
                Time::default(),
            )
            .await?;
            // These tests assert on the written files, so they stand in for the JVM taking
            // ownership of them.
            abort_guard.disarm();
            Ok(data_files)
        }

        #[tokio::test]
        async fn unpartitioned_write_emits_single_file_with_all_rows() {
            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = iceberg_user_schema();
            let spec = PartitionSpec::builder(Arc::new(schema.clone()))
                .build()
                .unwrap();
            let common = common(
                data_location.clone(),
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
            );

            let data_files = run(
                common,
                schema,
                spec,
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
                vec![batch(&[1, 2, 3], &["us", "eu", "us"])],
            )
            .await
            .unwrap();

            assert_eq!(data_files.len(), 1);
            assert_eq!(data_files[0].record_count(), 3);
            assert!(data_files[0]
                .file_path()
                .contains(temp_dir.path().to_str().unwrap()));
            assert!(data_files[0].file_path().ends_with(".parquet"));
        }

        // Manifest encoding and output-batch construction run after the writer has closed and can
        // still fail. `run_write_task` therefore hands its caller a guard that is still armed, so
        // the files a successful writer produced are deleted if packaging them fails.
        #[tokio::test]
        async fn a_successful_write_returns_an_armed_guard_that_can_still_delete_its_files() {
            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = iceberg_user_schema();
            let spec = PartitionSpec::builder(Arc::new(schema.clone()))
                .build()
                .unwrap();
            let common = common(
                data_location,
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
            );

            let (data_files, mut abort_guard) = run_write_task(
                input_stream(vec![batch(&[1, 2, 3], &["us", "eu", "us"])]),
                common,
                Arc::new(schema),
                Arc::new(spec),
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
                WriterProperties::builder().build(),
                Some(0),
                Some(0),
                Time::default(),
            )
            .await
            .unwrap();

            assert!(
                abort_guard.armed,
                "the caller owns cleanup until the JVM does"
            );
            let written: Vec<PathBuf> = data_files
                .iter()
                .map(|f| PathBuf::from(f.file_path().trim_start_matches("file:")))
                .collect();
            assert!(written.iter().all(|p| p.exists()), "{written:?}");
            assert_eq!(abort_guard.locations().len(), written.len());

            // What the outer task does when `encode_data_files_as_manifest` or
            // `build_output_batch` fails.
            abort_guard.abort().await;
            assert!(written.iter().all(|p| !p.exists()), "{written:?}");
            assert!(!abort_guard.armed, "aborting also gives up ownership");
        }

        #[tokio::test]
        async fn fanout_partitioned_write_produces_one_file_per_partition() {
            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = iceberg_user_schema();
            let spec = PartitionSpec::builder(Arc::new(schema.clone()))
                .with_spec_id(1)
                .add_partition_field("region", "region", Transform::Identity)
                .unwrap()
                .build()
                .unwrap();
            let common = common(
                data_location,
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterFanout,
            );

            let data_files = run(
                common,
                schema,
                spec,
                ProtoIcebergWriterMode::IcebergWriterFanout,
                vec![batch(&[1, 2, 3, 4], &["us", "eu", "us", "eu"])],
            )
            .await
            .unwrap();

            assert_eq!(data_files.len(), 2);
            let total: u64 = data_files.iter().map(|f| f.record_count()).sum();
            assert_eq!(total, 4);
        }

        #[tokio::test]
        async fn clustered_partitioned_write_handles_sorted_input() {
            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = iceberg_user_schema();
            let spec = PartitionSpec::builder(Arc::new(schema.clone()))
                .with_spec_id(1)
                .add_partition_field("region", "region", Transform::Identity)
                .unwrap()
                .build()
                .unwrap();
            let common = common(
                data_location,
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterClustered,
            );

            // ClusteredWriter requires partition-sorted input.
            let data_files = run(
                common,
                schema,
                spec,
                ProtoIcebergWriterMode::IcebergWriterClustered,
                vec![batch(&[1, 2, 3, 4], &["eu", "eu", "us", "us"])],
            )
            .await
            .unwrap();

            assert_eq!(data_files.len(), 2);
            let total: u64 = data_files.iter().map(|f| f.record_count()).sum();
            assert_eq!(total, 4);
        }

        // Regression: the partition splitter groups through a HashMap whose iteration order is
        // unspecified, but ClusteredWriter errors when a closed partition is revisited. With a
        // partition spanning a batch boundary and multiple partitions per batch, only the
        // first-occurrence write order is correct.
        #[tokio::test]
        async fn clustered_write_survives_partition_spanning_batch_boundary() {
            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = iceberg_user_schema();
            let spec = PartitionSpec::builder(Arc::new(schema.clone()))
                .with_spec_id(1)
                .add_partition_field("region", "region", Transform::Identity)
                .unwrap()
                .build()
                .unwrap();
            let common = common(
                data_location,
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterClustered,
            );

            // Partition "eu" continues from batch 1 into batch 2; each batch spans partitions.
            let data_files = run(
                common,
                schema,
                spec,
                ProtoIcebergWriterMode::IcebergWriterClustered,
                vec![
                    batch(&[1, 2, 3], &["de", "de", "eu"]),
                    batch(&[4, 5], &["eu", "us"]),
                ],
            )
            .await
            .unwrap();

            assert_eq!(data_files.len(), 3);
            let total: u64 = data_files.iter().map(|f| f.record_count()).sum();
            assert_eq!(total, 5);
        }

        #[tokio::test]
        async fn encoded_manifest_round_trips_through_iceberg_parser() {
            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = iceberg_user_schema();
            let spec = PartitionSpec::builder(Arc::new(schema.clone()))
                .build()
                .unwrap();
            let common = common(
                data_location,
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
            );

            let schema_arc = Arc::new(schema);
            let spec_arc = Arc::new(spec);
            let (data_files, mut abort_guard) = run_write_task(
                input_stream(vec![batch(&[10, 20], &["x", "y"])]),
                Arc::clone(&common),
                Arc::clone(&schema_arc),
                Arc::clone(&spec_arc),
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
                WriterProperties::builder().build(),
                Some(0),
                Some(0),
                Time::default(),
            )
            .await
            .unwrap();
            let locations = abort_guard.locations();
            abort_guard.disarm();

            let manifest_bytes = encode_data_files_as_manifest(
                data_files.clone(),
                Arc::clone(&schema_arc),
                Arc::clone(&spec_arc),
                Some(0),
                Some(0),
                &common.operation_id,
            )
            .await
            .unwrap();
            let output_schema = build_output_schema();
            let batch =
                build_output_batch(manifest_bytes.clone(), &locations, &output_schema).unwrap();
            assert_eq!(batch.num_rows(), 1);
            // The locations column names every file the manifest does, so the JVM can clean up
            // without decoding the manifest.
            assert_eq!(
                locations,
                data_files
                    .iter()
                    .map(|f| f.file_path().to_string())
                    .collect::<Vec<_>>()
            );

            let manifest = Manifest::parse_avro(&manifest_bytes).unwrap();
            let entries = manifest.entries();
            assert_eq!(entries.len(), data_files.len());
            assert_eq!(
                entries[0].data_file().record_count(),
                data_files[0].record_count()
            );
            assert_eq!(
                entries[0].data_file().file_path(),
                data_files[0].file_path()
            );
        }

        #[tokio::test]
        async fn decorate_batch_adds_field_ids() {
            let schema = iceberg_user_schema();
            let target = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema).unwrap());
            let original = batch(&[1, 2], &["a", "b"]);
            let decorated = decorate_batch_with_field_ids(original, &target).unwrap();
            let arrow_schema = decorated.schema();
            assert_eq!(
                arrow_schema
                    .field(0)
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY),
                Some(&"1".to_string())
            );
            assert_eq!(
                arrow_schema
                    .field(1)
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY),
                Some(&"2".to_string())
            );
        }

        #[tokio::test]
        async fn decorate_batch_rejects_column_count_mismatch() {
            let schema = iceberg_user_schema();
            let target = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema).unwrap());
            let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "unknown",
                DataType::Int32,
                false,
            )]));
            let batch =
                RecordBatch::try_new(arrow_schema, vec![Arc::new(Int32Array::from(vec![1]))])
                    .unwrap();
            let err = decorate_batch_with_field_ids(batch, &target).unwrap_err();
            assert!(format!("{err}").contains("column count mismatch"));
        }
    }
}

/// Pins Comet's Iceberg system-function kernels to iceberg-rust's partition transforms.
///
/// A partitioned write runs both: the sort in front of [`IcebergWriteExec`] is keyed on the
/// `datafusion-comet-spark-expr` kernels (Iceberg plans the sort as `bucket(...)`, `days(...)`,
/// ... system-function calls), while [`ClusteredWriter`] groups the sorted rows by the partition
/// values that [`PartitionValueCalculator`] computes with iceberg-rust's transforms. The writer
/// requires the two to agree: when they do not it fails at runtime with "The input is not sorted!
/// Cannot write to partition that was previously closed". These tests make an iceberg-rust bump
/// that changes a transform break here first.
#[cfg(test)]
mod iceberg_rust_transform_parity {
    use arrow::array::{
        ArrayRef, BinaryArray, Date32Array, Decimal128Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use datafusion_comet_spark_expr::{
        SparkIcebergBucket, SparkIcebergTemporalTransform, SparkIcebergTruncate,
    };
    use iceberg::spec::Transform;
    use iceberg::transform::create_transform_function;
    use std::sync::Arc;

    const MICROS_PER_DAY: i64 = 86_400_000_000;

    /// Runs a Comet kernel over `value`, prepending `parameter` for the two-argument transforms.
    fn comet(udf: &dyn ScalarUDFImpl, parameter: Option<i32>, value: &ArrayRef) -> ArrayRef {
        let mut args: Vec<ColumnarValue> = parameter
            .map(|p| ColumnarValue::Scalar(ScalarValue::Int32(Some(p))))
            .into_iter()
            .collect();
        args.push(ColumnarValue::Array(Arc::clone(value)));
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(i, a)| Arc::new(Field::new(format!("arg{i}"), a.data_type(), true)))
            .collect();
        let arg_types: Vec<DataType> = arg_fields.iter().map(|f| f.data_type().clone()).collect();
        let return_type = udf.return_type(&arg_types).unwrap();
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: value.len(),
            return_field: Arc::new(Field::new(udf.name(), return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
        .to_array(value.len())
        .unwrap()
    }

    fn iceberg_rust(transform: Transform, value: &ArrayRef) -> ArrayRef {
        create_transform_function(&transform)
            .unwrap()
            .transform(Arc::clone(value))
            .unwrap()
    }

    fn assert_agree(label: &str, transform: Transform, udf: &dyn ScalarUDFImpl, value: &ArrayRef) {
        let parameter = match transform {
            Transform::Bucket(n) => Some(n as i32),
            Transform::Truncate(w) => Some(w as i32),
            _ => None,
        };
        assert_eq!(
            comet(udf, parameter, value).as_ref(),
            iceberg_rust(transform, value).as_ref(),
            "{label} disagrees with iceberg-rust's {transform}"
        );
    }

    fn timestamps(micros: Vec<Option<i64>>) -> Vec<(&'static str, ArrayRef)> {
        // The two tags Comet can produce: `TimestampNTZType` is untagged and `TimestampType` is
        // always tagged UTC.
        vec![
            (
                "timestamp_ntz",
                Arc::new(TimestampMicrosecondArray::from(micros.clone())) as ArrayRef,
            ),
            (
                "timestamp_utc",
                Arc::new(TimestampMicrosecondArray::from(micros).with_timezone("UTC")) as ArrayRef,
            ),
        ]
    }

    /// Every type both sides accept. `Int8` and `Int16` are missing on purpose: Iceberg binds
    /// tinyint and smallint to `BucketInt`, iceberg-rust has no arm for them, and Comet's kernel
    /// widens them to the same 8 little-endian bytes that the `Int32` case pins here.
    #[test]
    fn bucket_agrees_with_iceberg_rust() {
        let mut inputs: Vec<(&str, ArrayRef)> = vec![
            (
                "int",
                Arc::new(Int32Array::from(vec![
                    Some(i32::MIN),
                    Some(-1),
                    Some(0),
                    Some(34),
                    Some(i32::MAX),
                    None,
                ])),
            ),
            (
                "long",
                Arc::new(Int64Array::from(vec![
                    Some(i64::MIN),
                    Some(-1),
                    Some(0),
                    Some(34),
                    Some(i64::MAX),
                    None,
                ])),
            ),
            (
                "date",
                Arc::new(Date32Array::from(vec![
                    Some(i32::MIN),
                    Some(-1),
                    Some(0),
                    Some(17_486),
                    Some(i32::MAX),
                    None,
                ])),
            ),
            (
                "decimal",
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(-(10i128.pow(38) - 1)),
                        Some(-129),
                        Some(0),
                        Some(1420),
                        Some(10i128.pow(38) - 1),
                        None,
                    ])
                    .with_precision_and_scale(38, 10)
                    .unwrap(),
                ),
            ),
            (
                "string",
                Arc::new(StringArray::from(vec![
                    Some(""),
                    Some("a"),
                    Some("iceberg"),
                    Some("日本語😀"),
                    None,
                ])),
            ),
            (
                "binary",
                Arc::new(BinaryArray::from(vec![
                    Some([].as_slice()),
                    Some([0u8, 1, 2, 3].as_slice()),
                    Some([0xffu8; 9].as_slice()),
                    None,
                ])),
            ),
        ];
        inputs.extend(timestamps(vec![
            Some(i64::MIN),
            Some(-1),
            Some(0),
            Some(1_510_871_468_000_000),
            Some(i64::MAX),
            None,
        ]));

        let udf = SparkIcebergBucket::new();
        for num_buckets in [1u32, 7, 16, i32::MAX as u32] {
            for (label, input) in &inputs {
                assert_agree(
                    &format!("bucket({num_buckets}, {label})"),
                    Transform::Bucket(num_buckets),
                    &udf,
                    input,
                );
            }
        }
    }

    /// `i32::MIN`, `i64::MIN`, and widths above 2^30 are left out: Java's `TruncateUtil` wraps
    /// there and iceberg-rust does not (`truncate_i32` uses `rem_euclid`, `truncate_i64` and the
    /// decimal kernel subtract without wrapping and overflow in a debug build). That is an
    /// iceberg-rust bug affecting the writer's own partition values, independent of these
    /// kernels -- apache/iceberg-rust#3141. The wrapping cases are pinned against the JVM in the
    /// kernel's own unit tests; add them here once that issue is fixed.
    #[test]
    fn truncate_agrees_with_iceberg_rust() {
        let inputs: Vec<(&str, ArrayRef)> = vec![
            (
                "int",
                Arc::new(Int32Array::from(vec![
                    Some(i32::MIN + 1_000_000),
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(i32::MAX - 1_000_000),
                    None,
                ])),
            ),
            (
                "long",
                Arc::new(Int64Array::from(vec![
                    Some(i64::MIN + 1_000_000),
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(i64::MAX - 1_000_000),
                    None,
                ])),
            ),
            (
                "decimal",
                Arc::new(
                    Decimal128Array::from(vec![Some(-1065), Some(0), Some(1065), None])
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                ),
            ),
            (
                "string",
                Arc::new(StringArray::from(vec![
                    Some(""),
                    Some("ic"),
                    Some("iceberg"),
                    Some("日本語テキスト"),
                    Some("a😀b😀c"),
                    None,
                ])),
            ),
            (
                "binary",
                Arc::new(BinaryArray::from(vec![
                    Some([].as_slice()),
                    Some([1u8].as_slice()),
                    Some([1u8, 2, 3, 4, 5].as_slice()),
                    None,
                ])),
            ),
        ];

        let udf = SparkIcebergTruncate::new();
        for width in [1u32, 3, 10, 1000, 1 << 30] {
            for (label, input) in &inputs {
                assert_agree(
                    &format!("truncate({width}, {label})"),
                    Transform::Truncate(width),
                    &udf,
                    input,
                );
            }
        }
    }

    /// `days` and `hours` are plain floor division on both sides, so the whole domain agrees.
    #[test]
    fn days_and_hours_agree_with_iceberg_rust() {
        let micros = vec![
            Some(0),
            Some(-1),
            Some(-MICROS_PER_DAY),
            Some(-MICROS_PER_DAY - 1),
            Some(1_510_871_468_000_000),
            Some(365 * MICROS_PER_DAY - 1),
            None,
        ];
        let days_udf = SparkIcebergTemporalTransform::days();
        let hours_udf = SparkIcebergTemporalTransform::hours();
        for (label, input) in timestamps(micros) {
            assert_agree(&format!("days({label})"), Transform::Day, &days_udf, &input);
            assert_agree(
                &format!("hours({label})"),
                Transform::Hour,
                &hours_udf,
                &input,
            );
        }
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(i32::MIN),
            Some(-366),
            Some(0),
            Some(17_486),
            Some(i32::MAX),
            None,
        ]));
        assert_agree("days(date)", Transform::Day, &days_udf, &dates);
    }

    /// `years` and `months` agree over the dates iceberg-rust can represent -- it splits the
    /// calendar with `chrono`, so anything past year 262143 errors there while Comet and the JVM
    /// keep going (apache/iceberg-rust#3142; see the kernel's own unit tests for those).
    #[test]
    fn years_and_months_agree_with_iceberg_rust_within_its_range() {
        let years_udf = SparkIcebergTemporalTransform::years();
        let months_udf = SparkIcebergTemporalTransform::months();
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(-100_000),
            Some(-366),
            Some(-365),
            Some(-1),
            Some(0),
            Some(30),
            Some(17_486),
            Some(100_000),
            None,
        ]));
        assert_agree("years(date)", Transform::Year, &years_udf, &dates);
        assert_agree("months(date)", Transform::Month, &months_udf, &dates);
        for (label, input) in timestamps(vec![
            Some(-100_000 * MICROS_PER_DAY),
            Some(-1),
            Some(0),
            Some(1_510_871_468_000_000),
            None,
        ]) {
            assert_agree(
                &format!("years({label})"),
                Transform::Year,
                &years_udf,
                &input,
            );
            assert_agree(
                &format!("months({label})"),
                Transform::Month,
                &months_udf,
                &input,
            );
        }
    }

    /// Why `years` and `months` are not delegated to iceberg-rust even though `bucket`, `days`,
    /// and `hours` could be: its kernels go through Arrow's `date_part`, which honours the
    /// array's timezone tag, while Iceberg's Java `DateTimeUtil` is always UTC. Comet only ever
    /// produces `UTC` and untagged timestamps today, so the parity above holds; this pins the
    /// reason the local kernel exists. Reported as apache/iceberg-rust#3142; if this ever fails,
    /// iceberg-rust dropped the tag dependency and delegating becomes safe.
    #[test]
    fn iceberg_rust_years_follow_the_timezone_tag() {
        // 1969-12-31T23:59:59.999999Z, which is 1970-01-01T05:44:59.999999 in Kathmandu.
        let tagged: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![-1i64]).with_timezone("Asia/Kathmandu"));
        let comet_years = comet(&SparkIcebergTemporalTransform::years(), None, &tagged);
        let iceberg_years = iceberg_rust(Transform::Year, &tagged);
        assert_eq!(
            comet_years.as_ref(),
            &Int32Array::from(vec![-1]) as &dyn arrow::array::Array
        );
        assert_eq!(
            iceberg_years.as_ref(),
            &Int32Array::from(vec![0]) as &dyn arrow::array::Array
        );
    }
}
