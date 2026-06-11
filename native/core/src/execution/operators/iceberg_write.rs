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

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures::TryStreamExt;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::{
    DataFile, DataFileFormat, ManifestWriterBuilder, PartitionSpec, PartitionSpecRef,
    Schema as IcebergSchema, SchemaRef as IcebergSchemaRef,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
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
use parquet::schema::types::ColumnPath;

use datafusion_comet_proto::spark_operator::{
    CompressionCodec as ProtoCompressionCodec, IcebergParquetColumnStatistics,
    IcebergParquetWriteSettings, IcebergWrite, IcebergWriteCommon,
    IcebergWriterMode as ProtoIcebergWriterMode,
};

use crate::cloud::s3::credential_bridge::AccessMode;
use crate::execution::operators::iceberg_common::load_file_io;

/// Builder chain instantiated once per task and handed to the partitioning wrapper.
type IcebergDataFileWriterBuilder =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

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

    fn as_any(&self) -> &dyn Any {
        self
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
) -> DFResult<Vec<DataFile>> {
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

    let location_generator =
        DefaultLocationGenerator::with_data_location(common.data_location.clone());
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

    let unpartitioned = partition_spec.is_unpartitioned();
    let mut writer = match (unpartitioned, writer_mode) {
        (true, _) => InnerWriter::Unpartitioned(UnpartitionedWriter::new(data_file_builder)),
        (false, ProtoIcebergWriterMode::IcebergWriterFanout) => {
            InnerWriter::Fanout(FanoutWriter::new(data_file_builder))
        }
        (false, _) => InnerWriter::Clustered(ClusteredWriter::new(data_file_builder)),
    };

    let splitter = if unpartitioned {
        None
    } else {
        Some(
            RecordBatchPartitionSplitter::try_new_with_computed_values(
                Arc::clone(&iceberg_schema),
                Arc::clone(&partition_spec),
            )
            .map_err(iceberg_err)?,
        )
    };

    // Build the field-id-decorated target schema once per task; every batch is cast against it.
    let target_schema =
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).map_err(iceberg_err)?);
    while let Some(batch) = input.try_next().await? {
        let decorated = decorate_batch_with_field_ids(batch, &target_schema)?;
        writer.write(decorated, splitter.as_ref()).await?;
    }
    writer.close().await
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
        splitter: Option<&RecordBatchPartitionSplitter>,
    ) -> DFResult<()> {
        use iceberg::writer::partitioning::PartitioningWriter;
        match self {
            InnerWriter::Unpartitioned(w) => w.write(batch).await.map_err(iceberg_err),
            InnerWriter::Fanout(w) => {
                let parts = splitter
                    .expect("partition splitter must be Some for partitioned writes")
                    .split(&batch)
                    .map_err(iceberg_err)?;
                for (key, part) in parts {
                    w.write(key, part).await.map_err(iceberg_err)?;
                }
                Ok(())
            }
            InnerWriter::Clustered(w) => {
                let parts = splitter
                    .expect("partition splitter must be Some for partitioned writes")
                    .split(&batch)
                    .map_err(iceberg_err)?;
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
    let casted: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .zip(target_schema.fields().iter())
        .map(|(col, target)| arrow::compute::cast(col, target.data_type()))
        .collect::<Result<_, _>>()
        .map_err(DataFusionError::from)?;
    RecordBatch::try_new(Arc::clone(target_schema), casted).map_err(DataFusionError::from)
}

fn file_name_prefix(
    partition_id: Option<i32>,
    task_attempt_id: Option<i64>,
    operation_id: &str,
) -> String {
    format!(
        "{:05}-{:05}-{}",
        partition_id.unwrap_or(0),
        task_attempt_id.unwrap_or(0),
        operation_id
    )
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
    // dispatch key / access mode are inert here.
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
    let mut manifest_writer = ManifestWriterBuilder::new(
        output_file,
        None,
        None,
        iceberg_schema,
        (*partition_spec).clone(),
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

fn build_output_batch(manifest_bytes: Vec<u8>, output_schema: &SchemaRef) -> DFResult<RecordBatch> {
    let array: ArrayRef = Arc::new(BinaryArray::from(vec![manifest_bytes.as_slice()]));
    RecordBatch::try_new(Arc::clone(output_schema), vec![array]).map_err(DataFusionError::from)
}

/// Translate `IcebergParquetWriteSettings` into parquet-rs `WriterProperties`.
///
/// Iceberg defaults are applied by the JVM-side translator; this function trusts the wire and
/// only re-applies parquet-rs-shaped settings.
fn build_writer_properties(settings: &IcebergParquetWriteSettings) -> DFResult<WriterProperties> {
    let compression = compression_from_proto(settings.compression, settings.compression_level)?;
    let mut builder = WriterProperties::builder()
        .set_compression(compression)
        .set_created_by(settings.created_by.clone())
        .set_max_row_group_bytes(Some(settings.row_group_size_bytes as usize))
        .set_data_page_size_limit(settings.page_size_bytes as usize)
        .set_dictionary_page_size_limit(settings.dict_size_bytes as usize)
        .set_data_page_row_count_limit(settings.page_row_limit as usize);

    builder = if settings.default_statistics_enabled {
        builder
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_statistics_truncate_length(settings.statistics_truncate_length.map(|n| n as usize))
    } else {
        builder.set_statistics_enabled(EnabledStatistics::None)
    };

    for column in &settings.column_statistics {
        builder = apply_column_statistics(builder, column);
    }

    Ok(builder.build())
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

fn apply_column_statistics(
    builder: parquet::file::properties::WriterPropertiesBuilder,
    column: &IcebergParquetColumnStatistics,
) -> parquet::file::properties::WriterPropertiesBuilder {
    let path = ColumnPath::new(column.column.split('.').map(str::to_owned).collect());
    if column.enabled {
        // Per-column truncate length is honoured at commit time by the JVM via
        // `ParquetUtil.footerMetrics`; parquet-rs has no `set_column_statistics_truncate_length`.
        builder.set_column_statistics_enabled(path, EnabledStatistics::Page)
    } else {
        builder.set_column_statistics_enabled(path, EnabledStatistics::None)
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
            default_statistics_enabled: true,
            statistics_truncate_length: Some(16),
            column_statistics: vec![],
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
    fn disables_statistics_when_default_off() {
        let mut settings = base_settings();
        settings.default_statistics_enabled = false;
        settings.statistics_truncate_length = None;
        let props = build_writer_properties(&settings).unwrap();
        assert_eq!(
            props.statistics_enabled(&"c".into()),
            EnabledStatistics::None
        );
    }

    #[test]
    fn applies_default_truncate_length() {
        let mut settings = base_settings();
        settings.statistics_truncate_length = Some(32);
        let props = build_writer_properties(&settings).unwrap();
        assert_eq!(props.statistics_truncate_length(), Some(32));
    }

    #[test]
    fn full_mode_removes_truncate_length() {
        let mut settings = base_settings();
        settings.statistics_truncate_length = None;
        let props = build_writer_properties(&settings).unwrap();
        assert_eq!(props.statistics_truncate_length(), None);
    }

    #[test]
    fn applies_per_column_statistics_overrides() {
        let mut settings = base_settings();
        settings.column_statistics = vec![
            IcebergParquetColumnStatistics {
                column: "id".to_string(),
                enabled: true,
                truncate_length: None,
            },
            IcebergParquetColumnStatistics {
                column: "payload".to_string(),
                enabled: false,
                truncate_length: None,
            },
        ];
        let props = build_writer_properties(&settings).unwrap();
        assert_eq!(
            props.statistics_enabled(&ColumnPath::new(vec!["id".to_string()])),
            EnabledStatistics::Page
        );
        assert_eq!(
            props.statistics_enabled(&ColumnPath::new(vec!["payload".to_string()])),
            EnabledStatistics::None
        );
    }

    #[test]
    fn nested_column_name_splits_on_dot() {
        let mut settings = base_settings();
        settings.column_statistics = vec![IcebergParquetColumnStatistics {
            column: "user.address.zip".to_string(),
            enabled: false,
            truncate_length: None,
        }];
        let props = build_writer_properties(&settings).unwrap();
        let path = ColumnPath::new(vec![
            "user".to_string(),
            "address".to_string(),
            "zip".to_string(),
        ]);
        assert_eq!(props.statistics_enabled(&path), EnabledStatistics::None);
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
        let prefix = file_name_prefix(Some(7), Some(42), "op-abc");
        assert_eq!(prefix, "00007-00042-op-abc");
    }

    #[test]
    fn file_name_prefix_defaults_unset_ids_to_zero() {
        let prefix = file_name_prefix(None, None, "x");
        assert_eq!(prefix, "00000-00000-x");
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
                default_statistics_enabled: true,
                statistics_truncate_length: Some(16),
                column_statistics: vec![],
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
            )
            .await
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
    }
}
