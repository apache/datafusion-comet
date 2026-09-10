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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

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
use iceberg::spec::{
    DataFile, DataFileFormat, Literal, ManifestWriterBuilder, PartitionKey, PartitionSpec,
    PartitionSpecRef, Schema as IcebergSchema, SchemaRef as IcebergSchemaRef,
    Struct as IcebergStruct, StructType,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::DefaultFileNameGenerator;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::clustered_writer::ClusteredWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::unpartitioned_writer::UnpartitionedWriter;
use iceberg::ErrorKind;
#[cfg(test)]
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use datafusion_comet_proto::spark_operator::{
    CompressionCodec as ProtoCompressionCodec, IcebergParquetWriteSettings, IcebergWrite,
    IcebergWriteCommon, IcebergWriterMode as ProtoIcebergWriterMode,
};

use crate::cloud::s3::credential_bridge::AccessMode;
use crate::errors::CometError;
use crate::execution::operators::iceberg_common::load_file_io;
use crate::execution::operators::iceberg_partition_path::{
    partition_to_path, CometLocationGenerator,
};

/// Builder chain instantiated once per task and handed to the partitioning wrapper.
type IcebergDataFileWriterBuilder =
    DataFileWriterBuilder<ParquetWriterBuilder, CometLocationGenerator, DefaultFileNameGenerator>;

/// How many rows the rolling writer may take before it re-checks the target file size.
///
/// iceberg-java's `RollingFileWriter` re-checks once every 1000 rows
/// (`RollingFileWriter.ROWS_DIVISOR`), counted per open file, so a JVM-written file overshoots
/// `write.target-file-size-bytes` by less than 1000 rows and every roll lands on a 1000-row
/// boundary. iceberg-rust's `RollingFileWriter` re-checks once per `write` call instead, which here
/// means once per input batch: a task whose rows all arrive in one batch never rolls at all, a file
/// overshoots by up to a whole batch, and the roll point depends on how Spark happened to batch the
/// rows. Handing the writer rows in `ROWS_DIVISOR`-row units (see [`RowPacer`]) restores
/// iceberg-java's grid without changing iceberg-rust.
const ROWS_DIVISOR: usize = 1000;

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
            let data_files = run_write_task(
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
            let manifest_bytes = encode_data_files_as_manifest(
                data_files,
                iceberg_schema,
                partition_spec,
                partition_id,
                task_attempt_id,
                &common.operation_id,
            )
            .await?;
            let batch = build_output_batch(manifest_bytes, &output_schema)?;
            Ok::<_, DataFusionError>(futures::stream::iter(vec![Ok(batch)]))
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
) -> DFResult<Vec<DataFile>> {
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

    // Resolves the write's partition type once per task, before any data is written, so a spec the
    // location generator could not render fails the task cleanly rather than panicking inside the
    // infallible `LocationGenerator::generate_location`.
    let location_generator = CometLocationGenerator::try_new(
        common.data_location.clone(),
        &partition_spec,
        &iceberg_schema,
    )
    .map_err(iceberg_err)?;
    let file_name_generator = DefaultFileNameGenerator::new(
        file_name_prefix(partition_id, task_attempt_id, &common.operation_id),
        None,
        DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(writer_properties, Arc::clone(&iceberg_schema));
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        common.target_file_size_bytes as usize,
        file_io,
        location_generator,
        file_name_generator,
    );
    let data_file_builder = DataFileWriterBuilder::new(rolling_builder);

    // Build the field-id-decorated target schema once per task; every batch is cast against it.
    let target_schema =
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).map_err(iceberg_err)?);
    let slicer = RowSlicer::for_schema(&target_schema);

    let unpartitioned = partition_spec.is_unpartitioned();
    let mut writer = match (unpartitioned, writer_mode) {
        (true, ProtoIcebergWriterMode::IcebergWriterUnpartitioned) => InnerWriter::Unpartitioned(
            UnpartitionedWriter::new(data_file_builder),
            RowPacer::new(slicer),
        ),
        (false, ProtoIcebergWriterMode::IcebergWriterFanout) => {
            InnerWriter::Fanout(FanoutWriter::new(data_file_builder), HashMap::new())
        }
        (false, ProtoIcebergWriterMode::IcebergWriterClustered) => {
            InnerWriter::Clustered(ClusteredWriter::new(data_file_builder), None)
        }
        (actual, mode) => {
            return Err(DataFusionError::Internal(format!(
                "IcebergWrite writer_mode {mode:?} is inconsistent with the partition spec \
                 (unpartitioned={actual})"
            )))
        }
    };

    let clustered_splitter = match &writer {
        InnerWriter::Clustered(..) => Some(ClusteredBatchSplitter::try_new(
            Arc::clone(&partition_spec),
            Arc::clone(&iceberg_schema),
            slicer,
        )?),
        _ => None,
    };
    let fanout_splitter = match &writer {
        InnerWriter::Fanout(..) => Some(
            RecordBatchPartitionSplitter::try_new_with_computed_values(
                Arc::clone(&iceberg_schema),
                Arc::clone(&partition_spec),
            )
            .map_err(iceberg_err)?,
        ),
        _ => None,
    };

    while let Some(batch) = input.try_next().await? {
        let decorated = decorate_batch_with_field_ids(batch, &target_schema)?;
        let _timer = write_time.timer();
        writer
            .write(
                decorated,
                slicer,
                fanout_splitter.as_ref(),
                clustered_splitter.as_ref(),
            )
            .await?;
    }
    let _timer = write_time.timer();
    writer.close(clustered_splitter.as_ref()).await
}

/// Enum-based dispatch over the three iceberg-rust partitioning writers, each paired with the
/// [`RowPacer`] state that keeps its rolling writer on iceberg-java's row grid. Each variant takes
/// the same builder chain so we can keep the type fixed.
enum InnerWriter {
    Unpartitioned(UnpartitionedWriter<IcebergDataFileWriterBuilder>, RowPacer),
    /// The fanout writer keeps one file open per partition, so every partition paces separately.
    /// The `PartitionKey` is kept alongside so leftovers can still be written out at close.
    Fanout(
        FanoutWriter<IcebergDataFileWriterBuilder>,
        HashMap<IcebergStruct, (PartitionKey, RowPacer)>,
    ),
    /// The clustered writer closes a partition's file as soon as the next key arrives, so only the
    /// current key's leftovers are live; they are written out before the switch.
    Clustered(
        ClusteredWriter<IcebergDataFileWriterBuilder>,
        Option<(PartitionKey, RowPacer)>,
    ),
}

impl InnerWriter {
    /// Writes `batch` through the writer this task built, in the [`ROWS_DIVISOR`]-row units the
    /// rolling writer needs to re-check the target file size on iceberg-java's cadence. Rows are
    /// paced after partition splitting, so each partition's file is measured against its own row
    /// count the way the JVM writer measures it.
    async fn write(
        &mut self,
        batch: RecordBatch,
        slicer: RowSlicer,
        fanout_splitter: Option<&RecordBatchPartitionSplitter>,
        clustered_splitter: Option<&ClusteredBatchSplitter>,
    ) -> DFResult<()> {
        use iceberg::writer::partitioning::PartitioningWriter;
        match self {
            InnerWriter::Unpartitioned(w, pacer) => {
                for unit in pacer.push(batch)? {
                    w.write(unit).await.map_err(iceberg_err)?;
                }
                Ok(())
            }
            InnerWriter::Fanout(w, pacers) => {
                let parts = fanout_splitter
                    .expect("fanout splitter must be Some for fanout writes")
                    .split(&batch)
                    .map_err(iceberg_err)?;
                for (key, part) in parts {
                    let (_, pacer) = pacers
                        .entry(key.data().clone())
                        .or_insert_with(|| (key.clone(), RowPacer::new(slicer)));
                    for unit in pacer.push(part)? {
                        w.write(key.clone(), unit).await.map_err(iceberg_err)?;
                    }
                }
                Ok(())
            }
            InnerWriter::Clustered(w, live) => {
                let splitter = clustered_splitter
                    .expect("clustered splitter must be Some for clustered writes");
                for (key, part) in splitter.split(&batch)? {
                    // A new key closes the previous partition's file, so its leftovers have to go
                    // out first -- and its row grid does not carry over to the new partition.
                    if let Some((open, mut pacer)) =
                        live.take_if(|(open, _)| open.data() != key.data())
                    {
                        if let Some(rest) = pacer.flush()? {
                            // `write` consumes the key, but the unclustered-input error needs it
                            // for the partition path.
                            let key_for_error = open.clone();
                            w.write(open, rest)
                                .await
                                .map_err(|e| clustered_write_err(e, &key_for_error, splitter))?;
                        }
                    }
                    let (_, pacer) =
                        live.get_or_insert_with(|| (key.clone(), RowPacer::new(slicer)));
                    for unit in pacer.push(part)? {
                        w.write(key.clone(), unit)
                            .await
                            .map_err(|e| clustered_write_err(e, &key, splitter))?;
                    }
                }
                Ok(())
            }
        }
    }

    /// Hands over whatever rows are still waiting, then closes. iceberg-java's writer does the
    /// same at close: the leftovers land in the file that is open at that point, unless the
    /// pending target-size check rolls first -- exactly as they would have on the JVM path.
    async fn close(
        self,
        clustered_splitter: Option<&ClusteredBatchSplitter>,
    ) -> DFResult<Vec<DataFile>> {
        use iceberg::writer::partitioning::PartitioningWriter;
        match self {
            InnerWriter::Unpartitioned(mut w, mut pacer) => {
                if let Some(rest) = pacer.flush()? {
                    w.write(rest).await.map_err(iceberg_err)?;
                }
                w.close().await.map_err(iceberg_err)
            }
            InnerWriter::Fanout(mut w, pacers) => {
                for (_, (key, mut pacer)) in pacers {
                    if let Some(rest) = pacer.flush()? {
                        w.write(key, rest).await.map_err(iceberg_err)?;
                    }
                }
                w.close().await.map_err(iceberg_err)
            }
            InnerWriter::Clustered(mut w, live) => {
                if let Some((key, mut pacer)) = live {
                    if let Some(rest) = pacer.flush()? {
                        // Pacing defers a partition's rows to here, so the unclustered-input
                        // rejection can surface at close rather than during `write`. It still has
                        // to read as iceberg-java's error.
                        let splitter = clustered_splitter
                            .expect("clustered splitter must be Some for clustered writes");
                        let key_for_error = key.clone();
                        w.write(key, rest)
                            .await
                            .map_err(|e| clustered_write_err(e, &key_for_error, splitter))?;
                    }
                }
                w.close().await.map_err(iceberg_err)
            }
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

/// iceberg-java's `ClusteredWriter` preamble, copied verbatim from
/// `core/src/main/java/org/apache/iceberg/io/ClusteredWriter.java`. Applications catch that
/// writer's `IllegalStateException` and match on this text -- Iceberg's own
/// `TestRequiredDistributionAndOrdering` does both -- so the native writer reproduces it instead
/// of surfacing iceberg-rust's differently worded error.
const NOT_CLUSTERED_ROWS_ERROR_MSG_TEMPLATE: &str = "Incoming records violate the writer \
     assumption that records are clustered by spec and by partition within each spec. Either \
     cluster the incoming records or switch to fanout writers.\n\
     Encountered records that belong to already closed files:\n";

/// The message iceberg-rust's `ClusteredWriter` raises for the same condition.
const UNSORTED_INPUT_MESSAGE_PREFIX: &str = "The input is not sorted!";

/// Restates iceberg-rust's unclustered-input failure as iceberg-java's, passing every other
/// failure through unchanged.
///
/// Leaving `ClusteredWriter` as the sole judge of whether the input is clustered means coupling to
/// its message text, which is the cheaper of the two couplings available: re-deriving the
/// condition here would need a copy of the writer's closed-partition bookkeeping, and because that
/// copy would fire first, no test could catch it drifting from the original. A wording change
/// upstream instead makes `clustered_write_rejects_unclustered_input_like_iceberg_java` fail, since
/// that test drives the real writer.
fn clustered_write_err(
    e: iceberg::Error,
    key: &PartitionKey,
    splitter: &ClusteredBatchSplitter,
) -> DataFusionError {
    if e.kind() == ErrorKind::Unexpected && e.message().starts_with(UNSORTED_INPUT_MESSAGE_PREFIX) {
        not_clustered_error(key, &splitter.partition_type)
    } else {
        iceberg_err(e)
    }
}

/// The error iceberg-java's `ClusteredWriter.write` raises when a closed partition is revisited,
/// down to the `partition '<path>' in spec <spec>` context. Only the partition branch of that
/// check is reachable here: a task writes through a single output spec, so the spec can never
/// change mid-stream.
///
/// The path comes from the same Java-compatible renderer that names the data directories, not from
/// `PartitionKey::to_path`: iceberg-rust hex-encodes binary where iceberg-java base64-encodes it,
/// and panics outright on a pre-epoch `timestamptz` with a sub-second part.
fn not_clustered_error(key: &PartitionKey, partition_type: &StructType) -> DataFusionError {
    DataFusionError::from(CometError::IllegalState(format!(
        "{NOT_CLUSTERED_ROWS_ERROR_MSG_TEMPLATE}partition '{}' in spec {}",
        partition_to_path(partition_type, key),
        format_partition_spec(key.spec())
    )))
}

/// Renders a partition spec the way iceberg-java's `PartitionSpec.toString` and
/// `PartitionField.toString` do, so the error context matches character for character.
fn format_partition_spec(spec: &PartitionSpec) -> String {
    let fields: String = spec
        .fields()
        .iter()
        .map(|field| {
            format!(
                "\n  {}: {}: {}({})",
                field.field_id, field.name, field.transform, field.source_id
            )
        })
        .collect();
    if fields.is_empty() {
        "[]".to_string()
    } else {
        format!("[{fields}\n]")
    }
}

fn build_output_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![Field::new(
        "iceberg_manifest",
        DataType::Binary,
        false,
    )]))
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
    slicer: RowSlicer,
}

impl ClusteredBatchSplitter {
    fn try_new(
        partition_spec: PartitionSpecRef,
        schema: IcebergSchemaRef,
        slicer: RowSlicer,
    ) -> DFResult<Self> {
        Ok(Self {
            calculator: PartitionValueCalculator::try_new(&partition_spec, &schema)
                .map_err(iceberg_err)?,
            partition_type: partition_spec
                .partition_type(&schema)
                .map_err(iceberg_err)?,
            partition_spec,
            schema,
            slicer,
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
        // A single-run batch (the common case: one partition per task batch) is the whole batch,
        // which `RowSlicer::slice` hands back as a clone.
        runs.into_iter()
            .map(|(value, start, len)| {
                let part = self.slicer.slice(batch, start, len)?;
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

/// Cuts contiguous row ranges out of a batch: the partition runs the clustered writer needs, and
/// the [`ROWS_DIVISOR`]-row pieces the rolling writer needs.
///
/// A zero-copy `RecordBatch::slice` is the cheap way to cut a range, but it is only exact when
/// the parquet writer's NaN-count visitor sees every float through the slice. The visitor reaches
/// list elements and map entries through `list_array.values()` / `map_array.entries()`, which
/// ignore the parent's offset window, so a sliced `list<float>` column would have every NaN in
/// the batch counted once per range cut from it -- and since the JVM carries the native writer's
/// NaN counts into the manifest, a `nan_value_count` that reaches `record_count` makes Iceberg's
/// metrics evaluator prune the file from ordinary comparison predicates. Struct children are
/// safe, because `StructArray::slice` slices them, so the only schemas that need the fix are the
/// ones with a float or double under a list or map; those ranges go through `take`, which gathers
/// the referenced children into fresh compacted arrays.
#[derive(Clone, Copy)]
struct RowSlicer {
    gather: bool,
}

impl RowSlicer {
    /// `schema` is the field-id-decorated target schema every batch is cast to, so this decision
    /// is made once per task rather than per batch.
    fn for_schema(schema: &ArrowSchema) -> Self {
        Self {
            gather: schema
                .fields()
                .iter()
                .any(|field| float_under_list_or_map(field.data_type())),
        }
    }

    fn slice(&self, batch: &RecordBatch, offset: usize, len: usize) -> DFResult<RecordBatch> {
        if offset == 0 && len == batch.num_rows() {
            return Ok(batch.clone());
        }
        if self.gather {
            gather_rows(batch, offset, len)
        } else {
            Ok(batch.slice(offset, len))
        }
    }

    /// Cuts a range that outlives the batch it came from, because it is waiting for the rows that
    /// complete its unit. Always gathers: a zero-copy slice would pin every buffer of its parent
    /// batch for the wait, and a partition that receives a handful of rows per batch would pin one
    /// parent per batch until its unit fills.
    fn detach(&self, batch: &RecordBatch, offset: usize, len: usize) -> DFResult<RecordBatch> {
        if offset == 0 && len == batch.num_rows() {
            // The range is the whole batch, so it pins nothing beyond the rows it holds.
            return Ok(batch.clone());
        }
        gather_rows(batch, offset, len)
    }
}

fn gather_rows(batch: &RecordBatch, offset: usize, len: usize) -> DFResult<RecordBatch> {
    let indices = UInt32Array::from_iter_values(offset as u32..(offset + len) as u32);
    arrow::compute::take_record_batch(batch, &indices).map_err(DataFusionError::from)
}

/// Hands one iceberg-rust writer exactly [`ROWS_DIVISOR`] rows at a time, so the rolling writer
/// underneath re-checks the target file size on the same row boundaries iceberg-java's
/// `RollingFileWriter` checks on -- multiples of 1000 rows since the current file opened -- however
/// Spark happened to batch the rows.
///
/// Pacing, rather than just cutting each batch up, is what makes the roll point independent of the
/// batch shape: a task fed 800-row batches would otherwise be offered a boundary every 800 rows
/// and roll into 800-row files where the JVM writer produces 1000-row ones. Rows left over from a
/// batch wait here for the rows that complete their unit, so at most `ROWS_DIVISOR - 1` rows per
/// open file are held back.
struct RowPacer {
    slicer: RowSlicer,
    /// Rows handed in but not yet handed over; fewer than [`ROWS_DIVISOR`] in total.
    pending: Vec<RecordBatch>,
    pending_rows: usize,
}

impl RowPacer {
    fn new(slicer: RowSlicer) -> Self {
        Self {
            slicer,
            pending: Vec::new(),
            pending_rows: 0,
        }
    }

    /// The complete units `batch` makes available, in row order. Whatever does not fill a unit is
    /// held for the next call.
    fn push(&mut self, batch: RecordBatch) -> DFResult<Vec<RecordBatch>> {
        debug_assert!(
            self.pending_rows < ROWS_DIVISOR,
            "a complete unit was left pending"
        );
        let rows = batch.num_rows();
        let mut units = Vec::with_capacity((self.pending_rows + rows) / ROWS_DIVISOR);
        let mut offset = 0;
        while self.pending_rows + (rows - offset) >= ROWS_DIVISOR {
            let len = ROWS_DIVISOR - self.pending_rows;
            let piece = self.slicer.slice(&batch, offset, len)?;
            offset += len;
            units.push(self.drain_pending_into(piece)?);
        }
        if offset < rows {
            let rest = self.slicer.detach(&batch, offset, rows - offset)?;
            self.pending_rows += rest.num_rows();
            self.pending.push(rest);
        }
        Ok(units)
    }

    /// The rows still waiting, if any, so a close does not lose them.
    fn flush(&mut self) -> DFResult<Option<RecordBatch>> {
        if self.pending_rows == 0 {
            return Ok(None);
        }
        self.concat_pending().map(Some)
    }

    /// Completes a unit from the held rows plus `piece`, which together are exactly
    /// [`ROWS_DIVISOR`] rows.
    fn drain_pending_into(&mut self, piece: RecordBatch) -> DFResult<RecordBatch> {
        if self.pending.is_empty() {
            debug_assert_eq!(self.pending_rows, 0, "no held rows but a non-zero count");
            return Ok(piece);
        }
        self.pending.push(piece);
        self.concat_pending()
    }

    fn concat_pending(&mut self) -> DFResult<RecordBatch> {
        self.pending_rows = 0;
        if self.pending.len() == 1 {
            return Ok(self.pending.pop().expect("pending has one batch"));
        }
        let schema = self.pending[0].schema();
        let unit = arrow::compute::concat_batches(&schema, &self.pending)
            .map_err(DataFusionError::from)?;
        self.pending.clear();
        Ok(unit)
    }
}

/// `true` when `data_type` puts a float or double under a list or map, where a slice's offset
/// window is invisible to iceberg-rust's NaN-count visitor.
fn float_under_list_or_map(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => contains_float(field.data_type()),
        DataType::Map(entries, _) => contains_float(entries.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| float_under_list_or_map(field.data_type())),
        _ => false,
    }
}

/// `true` when `data_type` is, or nests, a float or double. `Float16` is unreachable from an
/// Iceberg schema and is listed only so a future half-float stays on the safe side.
fn contains_float(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => contains_float(field.data_type()),
        DataType::Map(entries, _) => contains_float(entries.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_float(field.data_type())),
        _ => false,
    }
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
    let memory_io = load_file_io(&HashMap::new(), "memory:///", "", AccessMode::Write)?;
    let path = format!(
        "memory:///comet-manifest-{:05}-{:05}-{}.avro",
        partition_id.unwrap_or(0),
        task_attempt_id.unwrap_or(0),
        operation_id,
    );
    let output_file = memory_io.new_output(&path).map_err(iceberg_err)?;
    let mut manifest_writer = ManifestWriterBuilder::new(
        output_file,
        None,
        iceberg_schema,
        manifest_partition_spec(&partition_spec),
    )
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

/// The partition spec the per-task transport manifest is encoded against.
///
/// Normally the write's own spec, but a spec that `PartitionSpec::is_unpartitioned` accepts can
/// still carry fields: iceberg-java's `UpdatePartitionSpec` keeps a dropped partition field in a
/// format-version-1 spec as a `void` transform to preserve its field id, and "unpartitioned" means
/// *every* field is `void` on both the Java and Rust sides, not that there are none. `run_write_task`
/// routes such a write through `UnpartitionedWriter`, which stamps every data file with an empty
/// partition struct, while `ManifestWriter` derives its partition summaries from the spec's fields
/// and `zip_eq`s the two -- panicking across the JNI boundary on the length mismatch
/// (apache/datafusion-comet#5691). Encoding against a field-less spec of the same id makes the two
/// agree.
///
/// Nothing downstream loses information. The JVM re-reads this manifest with the spec embedded in
/// its own Avro metadata, then rebuilds each `DataFile` against the table's real output spec, whose
/// `DataFiles.Builder` drops partition data outright for an unpartitioned spec -- so the manifest
/// that reaches storage carries exactly what iceberg-java's own writer would have committed for a
/// `void`-only spec (a null per `void` field, filled in by `PartitionData.get` returning null past
/// the end of its backing array).
///
/// Dropping the fields also skips the `partition_type` resolution `ManifestWriter` would otherwise
/// do, which fails once the `void` field's source column has itself been dropped from the schema
/// (apache/datafusion-comet#5693).
fn manifest_partition_spec(partition_spec: &PartitionSpecRef) -> PartitionSpec {
    if partition_spec.is_unpartitioned() {
        // A no-op when the spec already has no fields, which is the common case.
        PartitionSpec::unpartition_spec().with_spec_id(partition_spec.spec_id())
    } else {
        (**partition_spec).clone()
    }
}

fn build_output_batch(manifest_bytes: Vec<u8>, output_schema: &SchemaRef) -> DFResult<RecordBatch> {
    let array: ArrayRef = Arc::new(BinaryArray::from(vec![manifest_bytes.as_slice()]));
    RecordBatch::try_new(Arc::clone(output_schema), vec![array]).map_err(DataFusionError::from)
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
    fn build_output_schema_has_single_binary_column() {
        let schema = build_output_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "iceberg_manifest");
        assert_eq!(schema.field(0).data_type(), &DataType::Binary);
    }

    #[test]
    fn file_name_prefix_pads_ids() {
        let prefix = file_name_prefix(7, 42, "op-abc");
        assert_eq!(prefix, "00007-00042-op-abc");
    }

    // -- Row slicing / rolling cadence ---------------------------------------

    fn list_of(data_type: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("element", data_type, true)))
    }

    fn struct_of(name: &str, data_type: DataType) -> DataType {
        DataType::Struct(vec![Field::new(name, data_type, true)].into())
    }

    fn map_to(value: DataType) -> DataType {
        let entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", value, true),
                ]
                .into(),
            ),
            false,
        );
        DataType::Map(Arc::new(entries), false)
    }

    fn slicer_for(columns: Vec<(&str, DataType)>) -> RowSlicer {
        let fields: Vec<Field> = columns
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type, true))
            .collect();
        RowSlicer::for_schema(&ArrowSchema::new(fields))
    }

    #[test]
    fn row_slicer_slices_when_no_float_sits_under_a_list_or_map() {
        // A float is only a problem when the NaN-count visitor reaches it through a container
        // that ignores the slice window; top-level and struct floats are sliced exactly.
        let slicer = slicer_for(vec![
            ("i", DataType::Int32),
            ("f", DataType::Float64),
            ("s", struct_of("inner", DataType::Float32)),
            ("l", list_of(DataType::Utf8)),
            ("m", map_to(DataType::Utf8)),
            ("ls", list_of(struct_of("inner", DataType::Utf8))),
        ]);
        assert!(!slicer.gather);
    }

    #[test]
    fn row_slicer_gathers_when_a_float_sits_under_a_list_or_map() {
        let cases = vec![
            ("list of double", list_of(DataType::Float64)),
            ("list of float", list_of(DataType::Float32)),
            (
                "list of struct of float",
                list_of(struct_of("inner", DataType::Float32)),
            ),
            (
                "list of list of double",
                list_of(list_of(DataType::Float64)),
            ),
            ("map to double", map_to(DataType::Float64)),
            (
                "struct of list of double",
                struct_of("l", list_of(DataType::Float64)),
            ),
        ];
        for (label, data_type) in cases {
            assert!(
                slicer_for(vec![("c", data_type)]).gather,
                "{label} must be gathered, not sliced"
            );
        }
    }

    /// Rows `first..first + rows`, so a sequence of batches carries distinguishable values.
    fn int_batch_from(first: i32, rows: usize) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "i",
            DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int32Array::from_iter_values(
                first..first + rows as i32,
            ))],
        )
        .unwrap()
    }

    fn int_batch(rows: usize) -> RecordBatch {
        int_batch_from(0, rows)
    }

    /// Paces `batches` and returns the row count of every unit handed over, the trailing flush
    /// included.
    fn paced_rows(gather: bool, batches: &[usize]) -> Vec<usize> {
        let mut pacer = RowPacer::new(RowSlicer { gather });
        let mut rows = Vec::new();
        let mut first = 0;
        for &batch_rows in batches {
            for unit in pacer.push(int_batch_from(first, batch_rows)).unwrap() {
                rows.push(unit.num_rows());
            }
            first += batch_rows as i32;
        }
        rows.extend(pacer.flush().unwrap().map(|rest| rest.num_rows()));
        rows
    }

    #[test]
    fn pacer_hands_over_whole_units_whatever_the_batch_shape() {
        for gather in [false, true] {
            // Nothing to write, and nothing held back.
            assert_eq!(paced_rows(gather, &[]), Vec::<usize>::new(), "{gather}");
            assert_eq!(paced_rows(gather, &[0, 0]), Vec::<usize>::new(), "{gather}");
            // Short of a unit: everything waits for the flush.
            assert_eq!(paced_rows(gather, &[1]), vec![1], "{gather}");
            assert_eq!(
                paced_rows(gather, &[ROWS_DIVISOR - 1]),
                vec![ROWS_DIVISOR - 1],
                "{gather}"
            );
            // Exact units are handed over as they complete, with nothing left to flush.
            assert_eq!(
                paced_rows(gather, &[ROWS_DIVISOR]),
                vec![ROWS_DIVISOR],
                "{gather}"
            );
            assert_eq!(
                paced_rows(gather, &[2 * ROWS_DIVISOR + 500]),
                vec![ROWS_DIVISOR, ROWS_DIVISOR, 500],
                "{gather}"
            );
            // The case the batch-at-a-time cut got wrong: batches that are not a multiple of the
            // cadence still produce whole units, not one file boundary per batch.
            assert_eq!(paced_rows(gather, &[800; 5]), vec![1000; 4], "{gather}");
            assert_eq!(
                paced_rows(gather, &[8192, 1808]),
                vec![ROWS_DIVISOR; 10],
                "{gather}"
            );
            assert_eq!(
                paced_rows(gather, &[1; ROWS_DIVISOR + 1]),
                vec![ROWS_DIVISOR, 1],
                "{gather}"
            );
        }
    }

    #[test]
    fn pacer_preserves_every_row_in_order() {
        for gather in [false, true] {
            let batches: Vec<usize> = vec![800, 0, 1, 2500, 999];
            let mut pacer = RowPacer::new(RowSlicer { gather });
            let mut units = Vec::new();
            let mut first = 0;
            for batch_rows in &batches {
                units.extend(pacer.push(int_batch_from(first, *batch_rows)).unwrap());
                first += *batch_rows as i32;
            }
            units.extend(pacer.flush().unwrap());
            let schema = int_batch(1).schema();
            let rebuilt = arrow::compute::concat_batches(&schema, &units).unwrap();
            assert_eq!(rebuilt, int_batch(batches.iter().sum()), "gather={gather}");
        }
    }

    // -- Integration tests against the real iceberg-rust writer stack ---------

    mod integration {
        use super::super::*;
        use arrow::array::{BinaryArray, Int32Array, StringArray, TimestampMicrosecondArray};
        use arrow::datatypes::TimeUnit;
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

        /// Streams `batches` under their own schema, so a test can use a column layout other than
        /// [`user_schema`].
        fn input_stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
            let schema = batches
                .first()
                .map(RecordBatch::schema)
                .unwrap_or_else(user_schema);
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
            run_write_task(
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
            .await
        }

        fn with_target_file_size(
            common: Arc<IcebergWriteCommon>,
            target_file_size_bytes: u64,
        ) -> Arc<IcebergWriteCommon> {
            Arc::new(IcebergWriteCommon {
                target_file_size_bytes,
                ..(*common).clone()
            })
        }

        fn identity_region_spec(schema: &Schema) -> PartitionSpec {
            PartitionSpec::builder(Arc::new(schema.clone()))
                .with_spec_id(1)
                .add_partition_field("region", "region", Transform::Identity)
                .unwrap()
                .build()
                .unwrap()
        }

        /// One batch of `rows` rows, with `region` deciding each row's partition.
        fn region_batch(rows: usize, region: impl Fn(usize) -> &'static str) -> RecordBatch {
            let ids: Vec<i32> = (0..rows as i32).collect();
            let regions: Vec<&str> = (0..rows).map(region).collect();
            batch(&ids, &regions)
        }

        fn record_counts(data_files: &[DataFile]) -> Vec<u64> {
            data_files.iter().map(|file| file.record_count()).collect()
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

        #[tokio::test]
        async fn fanout_partitioned_write_produces_one_file_per_partition() {
            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = iceberg_user_schema();
            let spec = identity_region_spec(&schema);
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
            let spec = identity_region_spec(&schema);
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
            let spec = identity_region_spec(&schema);
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

        /// Runs a clustered write over input that revisits a closed partition, and returns the
        /// `CometError::IllegalState` message it has to fail with.
        ///
        /// Driving the real iceberg-rust writer is what makes these tests the tripwire for
        /// [`UNSORTED_INPUT_MESSAGE_PREFIX`] going stale on an iceberg-rust bump: the translation
        /// stops matching and the error arrives as a plain `iceberg::Error` instead.
        async fn unclustered_write_message(
            schema: Schema,
            spec: PartitionSpec,
            batches: Vec<RecordBatch>,
        ) -> String {
            let temp_dir = TempDir::new().unwrap();
            let common = common(
                format!("file://{}", temp_dir.path().display()),
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterClustered,
            );
            let err = run(
                common,
                schema,
                spec,
                ProtoIcebergWriterMode::IcebergWriterClustered,
                batches,
            )
            .await
            .unwrap_err();
            let comet_error = match &err {
                DataFusionError::External(external) => external.downcast_ref::<CometError>(),
                _ => None,
            };
            match comet_error {
                Some(CometError::IllegalState(message)) => message.clone(),
                _ => panic!("expected CometError::IllegalState, got {err:?}"),
            }
        }

        /// Builds an identity-partitioned single-column spec over `schema`.
        fn identity_spec(schema: &Schema, column: &str) -> PartitionSpec {
            PartitionSpec::builder(Arc::new(schema.clone()))
                .with_spec_id(1)
                .add_partition_field(column, column, Transform::Identity)
                .unwrap()
                .build()
                .unwrap()
        }

        /// Unclustered input must be rejected with iceberg-java's `ClusteredWriter` error rather
        /// than iceberg-rust's "The input is not sorted!", and it has to reach the JVM as an
        /// `IllegalStateException`. Iceberg's own `TestRequiredDistributionAndOrdering` asserts
        /// both. See https://github.com/apache/datafusion-comet/issues/5698.
        #[tokio::test]
        async fn clustered_write_rejects_unclustered_input_like_iceberg_java() {
            let schema = iceberg_user_schema();
            let spec = identity_spec(&schema, "region");
            // "eu" is revisited after the writer has moved on to "us" and closed it.
            let message = unclustered_write_message(
                schema,
                spec,
                vec![batch(&[1, 2], &["eu", "us"]), batch(&[3], &["eu"])],
            )
            .await;

            // The wording itself is pinned against the real JVM writer by
            // CometIcebergWriteActionSuite; here it only has to carry the right context.
            assert_eq!(
                message,
                format!(
                    "{NOT_CLUSTERED_ROWS_ERROR_MSG_TEMPLATE}\
                     partition 'region=eu' in spec [\n  1000: region: identity(2)\n]"
                )
            );
        }

        /// iceberg-java base64-encodes a binary partition value where iceberg-rust's
        /// `PartitionKey::to_path` writes uppercase hex, so the error has to go through the same
        /// Java-compatible renderer that names the data directories.
        #[tokio::test]
        async fn unclustered_binary_partition_renders_like_iceberg_java() {
            let schema = Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "part", Type::Primitive(PrimitiveType::Binary)).into(),
                ])
                .build()
                .unwrap();
            let spec = identity_spec(&schema, "part");
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("part", DataType::Binary, false),
            ]));
            let batch = |ids: &[i32], parts: Vec<&[u8]>| {
                RecordBatch::try_new(
                    Arc::clone(&arrow_schema),
                    vec![
                        Arc::new(Int32Array::from(ids.to_vec())),
                        Arc::new(BinaryArray::from(parts)),
                    ],
                )
                .unwrap()
            };

            let message = unclustered_write_message(
                schema,
                spec,
                vec![
                    batch(&[1, 2], vec![&[0x00, 0x01, 0xFF], &[0x00]]),
                    batch(&[3], vec![&[0x00, 0x01, 0xFF]]),
                ],
            )
            .await;

            // base64(00 01 FF) = "AAH/", form-urlencoded to "AAH%2F". iceberg-rust would have
            // rendered the same bytes as "0001FF".
            assert!(
                message
                    .ends_with("partition 'part=AAH%2F' in spec [\n  1000: part: identity(2)\n]"),
                "{message}"
            );
        }

        /// iceberg-rust's `Transform::to_human_string` panics on a pre-epoch `timestamptz` with a
        /// sub-second part, so routing the error through `PartitionKey::to_path` would crash the
        /// task instead of raising `IllegalStateException`.
        #[tokio::test]
        async fn unclustered_negative_timestamptz_partition_renders_like_iceberg_java() {
            let schema = Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                ])
                .build()
                .unwrap();
            let spec = identity_spec(&schema, "ts");
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                    false,
                ),
            ]));
            let batch = |ids: &[i32], micros: &[i64]| {
                RecordBatch::try_new(
                    Arc::clone(&arrow_schema),
                    vec![
                        Arc::new(Int32Array::from(ids.to_vec())),
                        Arc::new(
                            TimestampMicrosecondArray::from(micros.to_vec())
                                .with_timezone("+00:00"),
                        ),
                    ],
                )
                .unwrap()
            };

            // -1_500_000 micros is 1969-12-31T23:59:58.5Z -- negative with a sub-second part.
            let message = unclustered_write_message(
                schema,
                spec,
                vec![batch(&[1, 2], &[-1_500_000, 0]), batch(&[3], &[-1_500_000])],
            )
            .await;

            assert!(
                message.ends_with(
                    "partition 'ts=1969-12-31T23%3A59%3A58.5%2B00%3A00' in spec \
                     [\n  1000: ts: identity(2)\n]"
                ),
                "{message}"
            );
        }

        /// The transform rendering in [`format_partition_spec`] has to match iceberg-java's
        /// `PartitionField.toString` for the parameterised transforms too.
        #[test]
        fn partition_spec_renders_like_iceberg_java() {
            let schema = iceberg_user_schema();
            let spec = PartitionSpec::builder(Arc::new(schema))
                .with_spec_id(7)
                .add_partition_field("id", "id_bucket", Transform::Bucket(8))
                .unwrap()
                .add_partition_field("region", "region_trunc", Transform::Truncate(4))
                .unwrap()
                .build()
                .unwrap();
            assert_eq!(
                format_partition_spec(&spec),
                "[\n  1000: id_bucket: bucket[8](1)\n  1001: region_trunc: truncate[4](2)\n]"
            );
        }

        // -- Rolling cadence ------------------------------------------------

        /// Runs one task against a fresh table directory and returns the record count of every
        /// data file it produced, in write order.
        async fn write_and_count(
            writer_mode: ProtoIcebergWriterMode,
            target_file_size_bytes: u64,
            batches: Vec<RecordBatch>,
        ) -> Vec<u64> {
            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = iceberg_user_schema();
            let spec = match writer_mode {
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned => {
                    PartitionSpec::builder(Arc::new(schema.clone()))
                        .build()
                        .unwrap()
                }
                _ => identity_region_spec(&schema),
            };
            let common = with_target_file_size(
                common(
                    data_location,
                    serde_json::to_string(&spec).unwrap(),
                    serde_json::to_string(&schema).unwrap(),
                    writer_mode,
                ),
                target_file_size_bytes,
            );
            let data_files = run(common, schema, spec, writer_mode, batches)
                .await
                .unwrap();
            record_counts(&data_files)
        }

        /// A target of one byte trips every check, so the file count is exactly the number of
        /// checks: iceberg-java checks every 1000 rows of the open file, which turns 4000 rows
        /// into four 1000-row files even when they all arrive in one batch. This is
        /// `TestSparkDataWrite.testUnpartitionedCreateWithTargetFileSizeViaTableProperties`
        /// reduced to a single task.
        #[tokio::test]
        async fn unpartitioned_write_rolls_every_thousand_rows_inside_one_batch() {
            let rows = write_and_count(
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
                1,
                vec![region_batch(4000, |_| "us")],
            )
            .await;
            assert_eq!(rows, vec![1000; 4]);
        }

        /// The row-granular check must not roll a file that has not reached the target: the
        /// default 512 MiB target keeps 4000 rows in one file even though they are handed to the
        /// writer in four units.
        #[tokio::test]
        async fn unpartitioned_write_keeps_one_file_below_the_target() {
            let rows = write_and_count(
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
                512 * 1024 * 1024,
                vec![region_batch(4000, |_| "us")],
            )
            .await;
            assert_eq!(rows, vec![4000]);
        }

        /// The roll point must not depend on how Spark batched the rows: 4000 rows arriving as
        /// five 800-row batches -- the shape a `coalesce(1)` over a local relation produces --
        /// roll into the same four 1000-row files as one batch of 4000 would. Cutting each batch
        /// on its own would give five 800-row files instead.
        #[tokio::test]
        async fn unpartitioned_write_rolls_on_the_same_grid_across_batches() {
            let rows = write_and_count(
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
                1,
                (0..5).map(|_| region_batch(800, |_| "us")).collect(),
            )
            .await;
            assert_eq!(rows, vec![1000; 4]);
        }

        /// Every row still reaches a file when the batches do not add up to whole units: 4500 rows
        /// leave a 500-row remainder, which iceberg-java also writes as a trailing short file.
        #[tokio::test]
        async fn unpartitioned_write_flushes_the_trailing_partial_unit() {
            let rows = write_and_count(
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
                1,
                (0..6)
                    .map(|batch| region_batch(if batch == 5 { 500 } else { 800 }, |_| "us"))
                    .collect(),
            )
            .await;
            assert_eq!(rows, vec![1000, 1000, 1000, 1000, 500]);
        }

        /// Rows are counted per partition file, not per input batch, so each of two interleaved
        /// partitions rolls on its own 1000-row boundaries. This is
        /// `TestSparkDataWrite.testPartitionedCreateWithTargetFileSizeViaOption` with the fanout
        /// writer.
        #[tokio::test]
        async fn fanout_write_rolls_each_partition_every_thousand_rows() {
            let rows = write_and_count(
                ProtoIcebergWriterMode::IcebergWriterFanout,
                1,
                vec![region_batch(
                    4000,
                    |row| {
                        if row % 2 == 0 {
                            "us"
                        } else {
                            "eu"
                        }
                    },
                )],
            )
            .await;
            assert_eq!(rows, vec![1000; 4]);
        }

        /// A fanout partition's grid follows its own rows across batches too: two partitions
        /// receiving 250 rows each per batch still roll every 1000 rows of that partition.
        #[tokio::test]
        async fn fanout_write_paces_each_partition_across_batches() {
            let rows = write_and_count(
                ProtoIcebergWriterMode::IcebergWriterFanout,
                1,
                (0..8)
                    .map(|_| region_batch(500, |row| if row % 2 == 0 { "us" } else { "eu" }))
                    .collect(),
            )
            .await;
            assert_eq!(rows, vec![1000; 4]);
        }

        /// The clustered writer sees the same cadence: 2000 sorted rows per partition roll into
        /// two 1000-row files each.
        #[tokio::test]
        async fn clustered_write_rolls_each_partition_every_thousand_rows() {
            let rows = write_and_count(
                ProtoIcebergWriterMode::IcebergWriterClustered,
                1,
                vec![region_batch(
                    4000,
                    |row| {
                        if row < 2000 {
                            "eu"
                        } else {
                            "us"
                        }
                    },
                )],
            )
            .await;
            assert_eq!(rows, vec![1000; 4]);
        }

        /// The clustered writer closes a partition's file when the next key arrives, so a
        /// partition's leftover rows must go out before the switch -- and the new partition starts
        /// a fresh grid, exactly as a new `RollingFileWriter` does on the JVM. Here 1500 sorted
        /// rows per partition arrive 500 at a time.
        #[tokio::test]
        async fn clustered_write_flushes_leftovers_before_the_next_partition() {
            let rows = write_and_count(
                ProtoIcebergWriterMode::IcebergWriterClustered,
                1,
                (0..6)
                    .map(|batch| region_batch(500, move |_| if batch < 3 { "eu" } else { "us" }))
                    .collect(),
            )
            .await;
            assert_eq!(rows, vec![1000, 500, 1000, 500]);
        }

        /// Cutting a batch into pieces must not disturb the NaN counts the JVM carries into the
        /// manifest. iceberg-rust's visitor reads list children through `values()`, which ignores
        /// a slice's offset window, so a zero-copy slice would report every NaN in the batch once
        /// per piece -- three pieces here, and a `nan_value_count` that reaches `record_count`
        /// makes Iceberg's metrics evaluator prune the file from ordinary comparisons.
        #[tokio::test]
        async fn nan_counts_stay_exact_when_a_list_of_double_is_cut_into_pieces() {
            use arrow::array::{Array, ListArray};
            use arrow::datatypes::Float64Type;
            use iceberg::spec::ListType;

            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        2,
                        "vals",
                        Type::List(ListType {
                            element_field: NestedField::list_element(
                                3,
                                Type::Primitive(PrimitiveType::Double),
                                true,
                            )
                            .into(),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap();
            let spec = PartitionSpec::builder(Arc::new(schema.clone()))
                .build()
                .unwrap();
            let common = common(
                data_location,
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
            );

            // 2500 rows -> three pieces; one NaN in each piece.
            let rows = 2 * ROWS_DIVISOR + 500;
            let nan_rows = [7usize, ROWS_DIVISOR + 7, 2 * ROWS_DIVISOR + 7];
            let vals = ListArray::from_iter_primitive::<Float64Type, _, _>((0..rows).map(|row| {
                Some(vec![
                    Some(row as f64),
                    Some(if nan_rows.contains(&row) {
                        f64::NAN
                    } else {
                        0.5
                    }),
                ])
            }));
            let input_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("vals", vals.data_type().clone(), true),
            ]));
            let batch = RecordBatch::try_new(
                input_schema,
                vec![
                    Arc::new(Int32Array::from_iter_values(0..rows as i32)),
                    Arc::new(vals),
                ],
            )
            .unwrap();

            let data_files = run(
                common,
                schema,
                spec,
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
                vec![batch],
            )
            .await
            .unwrap();

            assert_eq!(record_counts(&data_files), vec![rows as u64]);
            assert_eq!(
                data_files[0].nan_value_counts().get(&3),
                Some(&(nan_rows.len() as u64)),
                "nan counts: {:?}",
                data_files[0].nan_value_counts()
            );
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
            let data_files = run_write_task(
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
            let batch = build_output_batch(manifest_bytes.clone(), &output_schema).unwrap();
            assert_eq!(batch.num_rows(), 1);

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

        /// A spec whose only partition field is a `void` transform, as iceberg-java's
        /// `UpdatePartitionSpec` leaves a format-version-1 spec after `DROP PARTITION FIELD`.
        /// Built by deserialising the spec JSON rather than through `PartitionSpec::builder`,
        /// which is how the real path builds it (`parse_partition_spec`) and which is also the
        /// only way to reach a `void` field whose source column is gone.
        fn void_spec(source_id: i32) -> PartitionSpec {
            serde_json::from_str(&format!(
                r#"{{"spec-id":2,"fields":[
                     {{"source-id":{source_id},"field-id":1000,
                       "name":"region_part","transform":"void"}}]}}"#
            ))
            .unwrap()
        }

        /// Regression for apache/datafusion-comet#5691. `is_unpartitioned()` accepts a spec whose
        /// fields are all `void`, so the write routes through `UnpartitionedWriter` and every data
        /// file gets an empty partition struct -- while `ManifestWriter` derives one partition
        /// summary per spec field and `zip_eq`s the two, which used to panic (`itertools:
        /// .zip_eq() reached end of one iterator before the other`) instead of returning an error.
        #[tokio::test]
        async fn void_only_spec_write_round_trips_through_the_manifest() {
            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = iceberg_user_schema();
            // source-id 2 is `region`, still present in the schema.
            let spec = void_spec(2);
            assert!(spec.is_unpartitioned() && !spec.fields().is_empty());
            let common = common(
                data_location.clone(),
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
            );

            let schema_arc = Arc::new(schema);
            let spec_arc = Arc::new(spec);
            let data_files = run_write_task(
                input_stream(vec![batch(&[1, 2], &["us", "eu"])]),
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
            assert_eq!(data_files.len(), 1);
            // No partition directory, matching iceberg-java's `UnpartitionedDataWriter`: the file
            // sits directly under the data location.
            let file_path = data_files[0].file_path().to_string();
            let relative = file_path
                .strip_prefix(&format!("{data_location}/"))
                .unwrap_or_else(|| panic!("{file_path} is not under {data_location}"));
            assert!(
                !relative.contains('/'),
                "unexpected directory in {relative}"
            );

            let manifest_bytes = encode_data_files_as_manifest(
                data_files,
                schema_arc,
                Arc::clone(&spec_arc),
                Some(0),
                Some(0),
                &common.operation_id,
            )
            .await
            .unwrap();
            let manifest = Manifest::parse_avro(&manifest_bytes).unwrap();
            assert_eq!(manifest.entries().len(), 1);
            assert_eq!(manifest.entries()[0].data_file().record_count(), 2);
            // The transport manifest is encoded against a field-less spec of the same id, so the
            // JVM reads back an empty partition struct -- which is what `DataFiles.Builder` would
            // have kept for this spec anyway.
            assert_eq!(manifest.metadata().partition_spec().spec_id(), 2);
            assert!(manifest.metadata().partition_spec().fields().is_empty());
        }

        /// Regression for apache/datafusion-comet#5693, the same shape one step further along:
        /// the `void` field's source column has since been dropped from the schema, which used to
        /// fail the write with "No column with source column id 9 in schema" from the manifest
        /// encode. Nothing needs that column -- a `void` field contributes no partition value and
        /// no partition directory.
        #[tokio::test]
        async fn void_field_with_a_dropped_source_column_still_writes() {
            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = iceberg_user_schema();
            let spec = void_spec(9);
            let common = common(
                data_location,
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterUnpartitioned,
            );

            let schema_arc = Arc::new(schema);
            let spec_arc = Arc::new(spec);
            let data_files = run_write_task(
                input_stream(vec![batch(&[1], &["us"])]),
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
            let manifest_bytes = encode_data_files_as_manifest(
                data_files,
                schema_arc,
                spec_arc,
                Some(0),
                Some(0),
                &common.operation_id,
            )
            .await
            .unwrap();
            assert_eq!(
                Manifest::parse_avro(&manifest_bytes)
                    .unwrap()
                    .entries()
                    .len(),
                1
            );
        }

        /// Regression for apache/datafusion-comet#5694. A pre-epoch `timestamptz` partition value
        /// used to panic while iceberg-rust rendered the partition directory name
        /// (`microseconds_to_datetimetz` unwraps a `None` for a negative sub-second remainder);
        /// Comet now renders the path itself, in iceberg-java's format.
        #[tokio::test]
        async fn pre_epoch_timestamptz_partition_gets_a_java_shaped_directory() {
            use arrow::array::TimestampMicrosecondArray;

            let temp_dir = TempDir::new().unwrap();
            let data_location = format!("file://{}", temp_dir.path().display());
            let schema = Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                ])
                .build()
                .unwrap();
            let spec = PartitionSpec::builder(Arc::new(schema.clone()))
                .with_spec_id(1)
                .add_partition_field("ts", "ts_part", Transform::Identity)
                .unwrap()
                .build()
                .unwrap();
            let common = common(
                data_location,
                serde_json::to_string(&spec).unwrap(),
                serde_json::to_string(&schema).unwrap(),
                ProtoIcebergWriterMode::IcebergWriterFanout,
            );

            // 1969-12-31T23:59:58.5Z: negative micros with a sub-second remainder.
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        Some("UTC".into()),
                    ),
                    false,
                ),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&arrow_schema),
                vec![
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![-1_500_000i64]).with_timezone("UTC"),
                    ),
                ],
            )
            .unwrap();

            let data_files = run_write_task(
                Box::pin(RecordBatchStreamAdapter::new(
                    arrow_schema,
                    futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
                )),
                common,
                Arc::new(schema),
                Arc::new(spec),
                ProtoIcebergWriterMode::IcebergWriterFanout,
                WriterProperties::builder().build(),
                Some(0),
                Some(0),
                Time::default(),
            )
            .await
            .unwrap();

            assert_eq!(data_files.len(), 1);
            assert!(
                data_files[0]
                    .file_path()
                    .contains("/ts_part=1969-12-31T23%3A59%3A58.5%2B00%3A00/"),
                "unexpected partition directory in {}",
                data_files[0].file_path()
            );
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
