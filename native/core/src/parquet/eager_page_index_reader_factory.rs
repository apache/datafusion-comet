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

//! A `ParquetFileReaderFactory` that always loads the Parquet page index into the shared
//! `FileMetadataCache` on the first metadata fetch for a file, instead of deferring to
//! DataFusion's opener.
//!
//! DataFusion's opener requests `PageIndexPolicy::Skip` on the initial metadata load and defers
//! loading the page index until row-group pruning shows it is still needed
//! (apache/datafusion#22857). That deferred load reads the page index directly off the
//! `AsyncFileReader` (`load_page_index` in `datafusion-datasource-parquet`'s opener), bypassing
//! `FileMetadataCache` entirely. For a predicate that never resolves to fully-matched by
//! row-group statistics alone, such as `IS NOT NULL` on a column whose row groups don't carry
//! `null_count` statistics, the skip never fires, and the page index is re-fetched, uncached, on
//! every open. At the scale of a wide fact table scanned across many partitions, that is
//! repeated, unbounded I/O for the same bytes.
//!
//! This factory forces `PageIndexPolicy::Optional` on every metadata fetch for files with no
//! decryption properties, ignoring whatever policy the caller requests, so the page index is
//! always present in the cached `ParquetMetaData` after the first load. DataFusion's opener
//! checks whether the metadata it already has includes the page index before issuing its own
//! fetch, so with this factory that check is always true and the uncached fetch never happens.
//! The tradeoff: files where the opener's skip heuristic would have avoided the page index load
//! entirely now load it anyway.
//!
//! Encrypted files are exempt from the override: `DFParquetMetadata::fetch_metadata` disables
//! `FileMetadataCache` entirely whenever decryption properties are set, so nothing gets cached
//! for them either way, and forcing eager loading would only add an unconditional page-index
//! fetch to encrypted scans that have no pruning predicate at all. Encrypted opens get exactly
//! the caller's requested policy, unchanged from stock behavior.
//!
//! Filed upstream as apache/datafusion#23978. Revert this once the opener merges its deferred
//! page-index load back into `FileMetadataCache` instead of bypassing it. Preserve the
//! duplicate-field validation when replacing this factory.

use arrow::datatypes::{DataType, FieldRef, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::Result as DFResult;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::{
    ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory, MetricType,
};
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    coalesce_ranges, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result as ObjectStoreResult,
    OBJECT_STORE_COALESCE_DEFAULT,
};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{encode_arrow_schema, parquet_to_arrow_schema, ARROW_SCHEMA_META_KEY};
use parquet::basic::{ConvertedType, LogicalType};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{FileMetaData, KeyValue, ParquetMetaDataBuilder};
use parquet::file::metadata::{
    FooterTail, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use parquet::schema::types::{ColumnDescPtr, SchemaDescriptor, Type};
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanIoSource {
    ObjectStore,
    Local,
    OtherObjectStore,
}

#[derive(Debug)]
struct ScanIoMetrics {
    data_bytes: Count,
    metadata_bytes: Count,
    footer_reads: Count,
    footer_bytes: Count,
    object_store_get_calls: Count,
    object_store_get_requested_bytes: Count,
    object_store_response_bytes_read: Count,
    metadata_cache_hits: Count,
    metadata_cache_misses: Count,
}

impl ScanIoMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            data_bytes: byte_counter(metrics, "scan_io_data_bytes"),
            metadata_bytes: byte_counter(metrics, "scan_io_metadata_bytes"),
            footer_reads: count_counter(metrics, "scan_io_footer_reads"),
            footer_bytes: byte_counter(metrics, "scan_io_footer_bytes"),
            object_store_get_calls: count_counter(metrics, "scan_io_object_store_get_calls"),
            object_store_get_requested_bytes: byte_counter(
                metrics,
                "scan_io_object_store_get_requested_bytes",
            ),
            object_store_response_bytes_read: byte_counter(
                metrics,
                "scan_io_object_store_response_bytes_read",
            ),
            metadata_cache_hits: count_counter(metrics, "scan_io_metadata_cache_hits"),
            metadata_cache_misses: count_counter(metrics, "scan_io_metadata_cache_misses"),
        }
    }

    fn record_metadata_cache_result(&self, storage_reads: usize) {
        if storage_reads == 0 {
            self.metadata_cache_hits.add(1);
        } else {
            self.metadata_cache_misses.add(1);
        }
    }
}

fn byte_counter(metrics: &ExecutionPlanMetricsSet, name: &'static str) -> Count {
    MetricBuilder::new(metrics)
        .with_type(MetricType::Summary)
        .with_category(MetricCategory::Bytes)
        .global_counter(name)
}

fn count_counter(metrics: &ExecutionPlanMetricsSet, name: &'static str) -> Count {
    MetricBuilder::new(metrics)
        .with_type(MetricType::Summary)
        .global_counter(name)
}

fn range_bytes(range: &Range<u64>) -> usize {
    (range.end - range.start) as usize
}

fn ranges_bytes(ranges: &[Range<u64>]) -> usize {
    ranges.iter().map(range_bytes).sum()
}

#[derive(Debug)]
pub struct EagerPageIndexReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<FileMetadataCache>,
    scan_io_metrics: Arc<ScanIoMetrics>,
    // Arrow schema hints and ENUM inference can change Spark's Variant interpretation.
    // Enable the footer workaround only for scans that project Variant.
    // https://github.com/apache/datafusion-comet/issues/5477
    spark_variant_schema: bool,
}

impl EagerPageIndexReaderFactory {
    /// Shares the scan's counters and metadata cache across all readers made by this factory.
    /// Native cloud stores are wrapped so coalesced requests and consumed response bytes are
    /// visible to the counters. Local and custom HDFS stores retain their range-read behavior.
    /// Construction performs no I/O; the returned factory owns the store and cache references.
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<FileMetadataCache>,
        source: ScanIoSource,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let scan_io_metrics = Arc::new(ScanIoMetrics::new(metrics));
        let store: Arc<dyn ObjectStore> = if source == ScanIoSource::ObjectStore {
            Arc::new(ScanIoObjectStore {
                inner: store,
                scan_io_metrics: Arc::clone(&scan_io_metrics),
                role: ScanIoStoreRole::ObjectStore,
            })
        } else {
            store
        };
        Self {
            store,
            metadata_cache,
            scan_io_metrics,
            spark_variant_schema: false,
        }
    }

    pub fn with_spark_variant_schema(mut self, enabled: bool) -> Self {
        self.spark_variant_schema = enabled;
        self
    }
}

impl ParquetFileReaderFactory for EagerPageIndexReaderFactory {
    /// Creates a reader without issuing I/O. It owns the file description and shares the
    /// factory's store, metadata cache, and scan counters. Per-file DataFusion metrics remain
    /// associated with `partition_index`; dropping the reader updates its scan-efficiency metric.
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DFResult<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            partitioned_file.object_meta.location.as_ref(),
            metrics,
        );

        Ok(Box::new(EagerPageIndexReader {
            file_metrics,
            scan_io_metrics: Arc::clone(&self.scan_io_metrics),
            store: Arc::clone(&self.store),
            partitioned_file,
            metadata_cache: Arc::clone(&self.metadata_cache),
            metadata_size_hint,
            spark_variant_schema: self.spark_variant_schema,
        }))
    }
}

/// Reads bytes straight off the `ObjectStore`, the same way DataFusion's own `ParquetFileReader`
/// does, while instrumenting ranges and overriding metadata fetching. `ParquetFileReader::new`
/// is crate-private, so the byte-range plumbing is duplicated here rather than delegated.
struct EagerPageIndexReader {
    file_metrics: ParquetFileMetrics,
    scan_io_metrics: Arc<ScanIoMetrics>,
    store: Arc<dyn ObjectStore>,
    partitioned_file: PartitionedFile,
    metadata_cache: Arc<FileMetadataCache>,
    metadata_size_hint: Option<usize>,
    spark_variant_schema: bool,
}

// Arrow infers ENUM as Binary, losing the distinction from raw binary that Spark needs.
// Inspect the Parquet annotation so Variant reconstruction can retain ENUM as a string.
// https://github.com/apache/datafusion-comet/issues/5477
fn is_enum_column(column: &ColumnDescPtr) -> bool {
    matches!(column.logical_type_ref(), Some(LogicalType::Enum))
        || column.converted_type() == ConvertedType::ENUM
}

fn spark_enum_field(
    field: &FieldRef,
    columns: &[ColumnDescPtr],
    column_index: &mut usize,
) -> ParquetResult<FieldRef> {
    let rewrite =
        |field: &FieldRef, data_type| Arc::new(field.as_ref().clone().with_data_type(data_type));
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| spark_enum_field(field, columns, column_index))
                .collect::<ParquetResult<Vec<_>>>()?
                .into(),
        ),
        DataType::List(child) => DataType::List(spark_enum_field(child, columns, column_index)?),
        DataType::LargeList(child) => {
            DataType::LargeList(spark_enum_field(child, columns, column_index)?)
        }
        DataType::FixedSizeList(child, size) => {
            DataType::FixedSizeList(spark_enum_field(child, columns, column_index)?, *size)
        }
        DataType::ListView(child) => {
            DataType::ListView(spark_enum_field(child, columns, column_index)?)
        }
        DataType::LargeListView(child) => {
            DataType::LargeListView(spark_enum_field(child, columns, column_index)?)
        }
        DataType::Map(child, sorted) => {
            DataType::Map(spark_enum_field(child, columns, column_index)?, *sorted)
        }
        _ => {
            let column = columns.get(*column_index).ok_or_else(|| {
                ParquetError::General(
                    "Arrow schema contains more leaves than the Parquet schema".to_string(),
                )
            })?;
            *column_index += 1;
            if is_enum_column(column) {
                DataType::Utf8
            } else {
                return Ok(Arc::clone(field));
            }
        }
    };
    Ok(rewrite(field, data_type))
}

/// Arrow maps Parquet ENUM to Binary, while Spark reads it as String. Supply a schema hint
/// that changes only ENUM leaves so Variant reconstruction preserves Spark's interpretation.
/// https://github.com/apache/datafusion-comet/issues/5477
fn spark_enum_schema(schema: &SchemaDescriptor) -> ParquetResult<Option<Schema>> {
    let columns = schema.columns();
    if !columns.iter().any(is_enum_column) {
        return Ok(None);
    }

    let arrow_schema = parquet_to_arrow_schema(schema, None)?;
    let mut column_index = 0;
    let fields = arrow_schema
        .fields()
        .iter()
        .map(|field| spark_enum_field(field, columns, &mut column_index))
        .collect::<ParquetResult<Vec<_>>>()?;
    if column_index != columns.len() {
        return Err(ParquetError::General(
            "Parquet schema contains more leaves than the Arrow schema".to_string(),
        ));
    }
    Ok(Some(Schema::new_with_metadata(
        fields,
        arrow_schema.metadata().clone(),
    )))
}

/// Arrow restores advisory `ARROW:schema` types that can differ from Spark's physical Parquet
/// interpretation. Replace that hint with physical inference and the ENUM string mapping.
/// Rebuild only the returned metadata; the shared cache retains the original footer.
/// https://github.com/apache/datafusion-comet/issues/5477
fn with_spark_arrow_schema(metadata: Arc<ParquetMetaData>) -> ParquetResult<Arc<ParquetMetaData>> {
    let file = metadata.file_metadata();
    let has_arrow_schema = file.key_value_metadata().is_some_and(|key_values| {
        key_values
            .iter()
            .any(|key_value| key_value.key == ARROW_SCHEMA_META_KEY)
    });
    let enum_schema = spark_enum_schema(file.schema_descr())?;
    if !has_arrow_schema && enum_schema.is_none() {
        return Ok(metadata);
    }

    let mut key_values = file
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter(|key_value| key_value.key != ARROW_SCHEMA_META_KEY)
        .cloned()
        .collect::<Vec<_>>();
    if let Some(schema) = enum_schema {
        key_values.push(KeyValue {
            key: ARROW_SCHEMA_META_KEY.to_string(),
            value: Some(encode_arrow_schema(&schema)),
        });
    }

    let file = FileMetaData::new(
        file.version(),
        file.num_rows(),
        file.created_by().map(str::to_owned),
        Some(key_values),
        file.schema_descr_ptr(),
        file.column_orders().cloned(),
    );
    Ok(Arc::new(
        ParquetMetaDataBuilder::new(file)
            .set_row_groups(metadata.row_groups().to_vec())
            .set_column_index(metadata.column_index().cloned())
            .set_offset_index(metadata.offset_index().cloned())
            .build(),
    ))
}

// Duplicate sibling names can make the decoder combine distinct leaves into one column,
// multiplying rows before schema adaptation can reject or resolve the duplicate (#5783).
// Reject the entire file, including unprojected fields, until the decoder can safely
// resolve duplicate siblings. Names in separate groups do not collide.
fn validate_field_names(schema: &Type) -> parquet::errors::Result<()> {
    if let Type::GroupType { fields, .. } = schema {
        let mut names = HashSet::with_capacity(fields.len());
        for field in fields {
            if !names.insert(field.name()) {
                return Err(ParquetError::General(format!(
                    "Comet native scan does not support duplicate Parquet field name '{}' in group '{}'",
                    field.name(),
                    schema.name()
                )));
            }
            validate_field_names(field)?;
        }
    }
    Ok(())
}

impl AsyncFileReader for EagerPageIndexReader {
    /// Reads a metadata range, counting its requested size before I/O and its returned
    /// bytes only on success. The returned future borrows this reader; store errors retain
    /// their cause as an external Parquet error.
    ///
    /// Parquet 59.2 uses this entry point for Bloom filters and `get_byte_ranges` for data
    /// pages. Footer/page-index loading is instrumented separately in `get_metadata`.
    /// Revisit these call sites on upgrades: offsets alone do not identify metadata.
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let requested = range_bytes(&range);
        // Preserve the existing requested-range metric. Metadata fetched through get_metadata
        // bypasses it, and object-store coalescing can fetch more bytes than these ranges.
        self.file_metrics.bytes_scanned.add(requested);
        let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
        async move {
            let bytes = self
                .store
                .get_range(&self.partitioned_file.object_meta.location, range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            scan_io_metrics.metadata_bytes.add(bytes.len());
            Ok(bytes)
        }
        .boxed()
    }

    /// Reads data-page ranges through the selected store's multi-range API. Local/custom stores
    /// retain delegation; native cloud wrappers observe coalescing. Requested bytes update
    /// `bytes_scanned` before I/O; successful
    /// logical ranges update `data_bytes`, excluding gaps fetched by cloud-store coalescing.
    /// The future borrows this reader and propagates store errors as external Parquet errors.
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        let requested = ranges_bytes(&ranges);
        self.file_metrics.bytes_scanned.add(requested);
        let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
        async move {
            let bytes = self
                .store
                .get_ranges(&self.partitioned_file.object_meta.location, &ranges)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            scan_io_metrics
                .data_bytes
                .add(bytes.iter().map(Bytes::len).sum());
            Ok(bytes)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        // Forward decryption properties like `CachedParquetFileReader` does. Only override the
        // policy for non-encrypted opens; see module docs for why.
        let object_meta = self.partitioned_file.object_meta.clone();
        let metadata_cache = Arc::clone(&self.metadata_cache);
        let store = Arc::clone(&self.store);
        let metadata_size_hint = self.metadata_size_hint;
        let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
        let spark_variant_schema = self.spark_variant_schema;
        async move {
            let file_decryption_properties = options
                .and_then(|o| o.file_decryption_properties())
                .map(Arc::clone);
            let cache_enabled = file_decryption_properties.is_none();
            if spark_variant_schema && file_decryption_properties.is_some() {
                return Err(ParquetError::General(
                    "Projected Variant with Parquet encryption requires Spark fallback".to_string(),
                ));
            }
            let page_index_policy = if file_decryption_properties.is_none() {
                Some(PageIndexPolicy::Optional)
            } else {
                options.map(|o| o.column_index_policy())
            };
            let metadata_storage_reads = Arc::new(AtomicUsize::new(0));
            let footer_payload_bytes = Arc::new(AtomicUsize::new(0));
            let metadata_store = ScanIoObjectStore {
                inner: store,
                scan_io_metrics: Arc::clone(&scan_io_metrics),
                role: ScanIoStoreRole::Metadata {
                    storage_reads: Arc::clone(&metadata_storage_reads),
                    footer_payload_bytes: Arc::clone(&footer_payload_bytes),
                    footer_payload: OnceLock::new(),
                    footer_recorded: AtomicBool::new(false),
                    file_size: object_meta.size,
                    record_footer_immediately: !cache_enabled,
                },
            };

            let metadata = DFParquetMetadata::new(&metadata_store, &object_meta)
                .with_decryption_properties(file_decryption_properties)
                .with_file_metadata_cache(Some(metadata_cache))
                .with_metadata_size_hint(metadata_size_hint)
                .with_page_index_policy(page_index_policy)
                .fetch_metadata()
                .await
                .map_err(|e| {
                    parquet::errors::ParquetError::General(format!(
                        "Failed to fetch metadata for file {}: {e}",
                        object_meta.location,
                    ))
                });

            if metadata.is_ok() {
                let footer_bytes = footer_payload_bytes.load(Ordering::Relaxed);
                if footer_bytes > 0 && cache_enabled {
                    metadata_store.record_footer(footer_bytes);
                }
                if cache_enabled {
                    scan_io_metrics.record_metadata_cache_result(
                        metadata_storage_reads.load(Ordering::Relaxed),
                    );
                }
            } else if cache_enabled {
                metadata_store.record_valid_footer();
            }

            let metadata = metadata?;
            // Validate cache hits too, before Arrow constructs a decoder for any projection.
            validate_field_names(metadata.file_metadata().schema_descr().root_schema())?;
            if spark_variant_schema {
                with_spark_arrow_schema(metadata)
            } else {
                Ok(metadata)
            }
        }
        .boxed()
    }
}

#[derive(Debug)]
enum ScanIoStoreRole {
    ObjectStore,
    Metadata {
        storage_reads: Arc<AtomicUsize>,
        footer_payload_bytes: Arc<AtomicUsize>,
        footer_payload: OnceLock<Bytes>,
        footer_recorded: AtomicBool,
        file_size: u64,
        record_footer_immediately: bool,
    },
}

#[derive(Debug)]
struct ScanIoObjectStore {
    inner: Arc<dyn ObjectStore>,
    scan_io_metrics: Arc<ScanIoMetrics>,
    role: ScanIoStoreRole,
}

impl ScanIoObjectStore {
    fn record_valid_footer(&self) {
        if let ScanIoStoreRole::Metadata {
            footer_payload_bytes,
            footer_payload,
            footer_recorded,
            ..
        } = &self.role
        {
            if !footer_recorded.load(Ordering::Relaxed)
                && footer_payload
                    .get()
                    .is_some_and(|payload| ParquetMetaDataReader::decode_metadata(payload).is_ok())
            {
                self.record_footer(footer_payload_bytes.load(Ordering::Relaxed));
            }
        }
    }

    fn record_footer(&self, bytes: usize) {
        if let ScanIoStoreRole::Metadata {
            footer_recorded, ..
        } = &self.role
        {
            if bytes > 0 && !footer_recorded.swap(true, Ordering::Relaxed) {
                self.scan_io_metrics.footer_reads.add(1);
                self.scan_io_metrics.footer_bytes.add(bytes);
            }
        }
    }

    fn record_request(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        match &self.role {
            ScanIoStoreRole::ObjectStore => {
                self.scan_io_metrics.object_store_get_calls.add(1);
                self.scan_io_metrics
                    .object_store_get_requested_bytes
                    .add(bytes);
            }
            ScanIoStoreRole::Metadata { storage_reads, .. } => {
                storage_reads.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // Footer accounting follows the DataFusion 55.0/parquet 59.2 metadata push decoder: a tail
    // read supplies the payload length, then one or more reads supply the complete payload.
    // Plaintext payloads are retained here and counted only after metadata succeeds, or when
    // get_ranges observes the subsequent page-index request wholly below the footer. Thus a
    // later index failure does not erase a decoded footer. Encrypted payloads are counted as
    // soon as complete, before key retrieval/decryption; their counter measures payload I/O,
    // not successful authentication. Recheck this protocol when upgrading the decoder.
    fn record_returned(&self, range: Option<&Range<u64>>, bytes: &Bytes) {
        match &self.role {
            ScanIoStoreRole::ObjectStore => self
                .scan_io_metrics
                .object_store_response_bytes_read
                .add(bytes.len()),
            ScanIoStoreRole::Metadata {
                footer_payload_bytes,
                footer_payload,
                file_size,
                record_footer_immediately,
                ..
            } => {
                self.scan_io_metrics.metadata_bytes.add(bytes.len());
                if range.is_some_and(|range| range.end == *file_size) && bytes.len() >= 8 {
                    if let Ok(footer) = FooterTail::try_from(&bytes[bytes.len() - 8..]) {
                        let _ = footer_payload_bytes.compare_exchange(
                            0,
                            footer.metadata_length(),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        );
                    }
                }

                let footer_bytes = footer_payload_bytes.load(Ordering::Relaxed);
                let footer_end = file_size.saturating_sub(8);
                if footer_bytes > 0
                    && footer_end
                        .checked_sub(footer_bytes as u64)
                        .is_some_and(|footer_start| {
                            range.is_some_and(|range| {
                                range.start <= footer_start
                                    && range.start.saturating_add(bytes.len() as u64) >= footer_end
                            })
                        })
                {
                    if *record_footer_immediately {
                        // Encrypted opens bypass the shared cache and retrieve the key only after
                        // the complete encrypted payload is read. Count completed payload I/O,
                        // even if key retrieval, authentication, or metadata decoding later fails.
                        self.record_footer(footer_bytes);
                    } else if let Some(range) = range {
                        let payload_start =
                            (footer_end - footer_bytes as u64 - range.start) as usize;
                        let _ = footer_payload
                            .set(bytes.slice(payload_start..payload_start + footer_bytes));
                    }
                }
            }
        }
    }
}

impl Display for ScanIoObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "scan-io({})", self.inner)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for ScanIoObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        if options.head {
            return self.inner.get_opts(location, options).await;
        }

        let requested = match options.range.as_ref() {
            Some(GetRange::Bounded(range)) => Some(range_bytes(range)),
            Some(GetRange::Suffix(bytes)) => Some(*bytes as usize),
            Some(GetRange::Offset(_)) | None => None,
        };
        if let Some(requested) = requested {
            self.record_request(requested);
        }

        let result = self.inner.get_opts(location, options).await?;
        if requested.is_none() {
            self.record_request(range_bytes(&result.range));
        }

        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let payload = if matches!(&result.payload, GetResultPayload::File(..)) {
            let bytes = result.bytes().await?;
            self.record_returned(Some(&range), &bytes);
            GetResultPayload::Stream(futures::stream::once(async move { Ok(bytes) }).boxed())
        } else {
            let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
            let metadata_read = matches!(self.role, ScanIoStoreRole::Metadata { .. });
            GetResultPayload::Stream(
                result
                    .into_stream()
                    .inspect_ok(move |bytes| {
                        if metadata_read {
                            scan_io_metrics.metadata_bytes.add(bytes.len());
                        } else {
                            scan_io_metrics
                                .object_store_response_bytes_read
                                .add(bytes.len());
                        }
                    })
                    .boxed(),
            )
        };

        Ok(GetResult {
            payload,
            meta,
            range,
            attributes,
        })
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        match &self.role {
            ScanIoStoreRole::ObjectStore => {
                // Supported native cloud stores use object_store's default coalescing. Apply it
                // here so get_opts observes the coalesced requests, not just logical ranges.
                // This deliberately bypasses an inner get_ranges override: a cache or custom
                // backend must define that observation boundary before being composed here.
                // Local/custom HDFS backends are not wrapped in this role and keep delegation.
                coalesce_ranges(
                    ranges,
                    |range| self.get_range(location, range),
                    OBJECT_STORE_COALESCE_DEFAULT,
                )
                .await
            }
            ScanIoStoreRole::Metadata {
                footer_payload_bytes,
                file_size,
                record_footer_immediately,
                ..
            } => {
                let footer_bytes = footer_payload_bytes.load(Ordering::Relaxed);
                if !record_footer_immediately
                    && footer_bytes > 0
                    && !ranges.is_empty()
                    && file_size
                        .saturating_sub(8)
                        .checked_sub(footer_bytes as u64)
                        .is_some_and(|footer_start| {
                            ranges.iter().all(|range| range.end <= footer_start)
                        })
                {
                    // In parquet 59.2, plaintext page-index requests below the footer begin only
                    // after its metadata payload has decoded successfully. Record that footer
                    // before awaiting the indexes, so a later index-read failure does not erase
                    // a successful footer read. Encrypted opens use the complete-payload path
                    // above; cached plaintext opens are recorded only after metadata succeeds.
                    self.record_footer(footer_bytes);
                }
                self.record_request(ranges_bytes(ranges));
                let bytes = self.inner.get_ranges(location, ranges).await?;
                for (range, bytes) in ranges.iter().zip(bytes.iter()) {
                    self.record_returned(Some(range), bytes);
                }
                Ok(bytes)
            }
        }
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> futures::stream::BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures::stream::BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

impl Drop for EagerPageIndexReader {
    fn drop(&mut self) {
        self.file_metrics
            .scan_efficiency_ratio
            .add_part(self.file_metrics.bytes_scanned.value());
        // Multiple readers may run against the same file, so we set_total on every drop rather
        // than accumulating it, to avoid adding the file's total size multiple times.
        self.file_metrics
            .scan_efficiency_ratio
            .set_total(self.partitioned_file.object_meta.size as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::Int32Array, record_batch::RecordBatch};
    use object_store::memory::InMemory;
    use parquet::{
        arrow::ArrowWriter,
        file::{
            properties::{EnabledStatistics, WriterProperties},
            reader::FileReader,
            serialized_reader::{ReadOptionsBuilder, SerializedFileReader},
        },
    };

    #[derive(Debug)]
    struct RecordingRangeStore {
        inner: InMemory,
        range_calls: AtomicUsize,
        get_calls: AtomicUsize,
    }

    impl Display for RecordingRangeStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "recording-range-store")
        }
    }

    #[async_trait]
    impl ObjectStore for RecordingRangeStore {
        async fn put_opts(
            &self,
            p: &Path,
            v: PutPayload,
            o: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(p, v, o).await
        }

        async fn put_multipart_opts(
            &self,
            p: &Path,
            o: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(p, o).await
        }

        async fn get_opts(&self, p: &Path, o: GetOptions) -> ObjectStoreResult<GetResult> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.get_opts(p, o).await
        }

        async fn get_ranges(
            &self,
            p: &Path,
            ranges: &[Range<u64>],
        ) -> ObjectStoreResult<Vec<Bytes>> {
            self.range_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.get_ranges(p, ranges).await
        }

        fn delete_stream(
            &self,
            paths: futures::stream::BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> futures::stream::BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(paths)
        }

        fn list(
            &self,
            p: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(p)
        }

        async fn list_with_delimiter(&self, p: Option<&Path>) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(p).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn preserves_custom_range_delegation_for_local_and_other_backends() {
        assert_range_read_contract(ScanIoSource::Local).await;
        // This is also the classification returned for custom libhdfs schemes, including s3.
        assert_range_read_contract(ScanIoSource::OtherObjectStore).await;
    }

    #[tokio::test]
    async fn remote_metrics_observe_default_coalescing_instead_of_inner_override() {
        assert_range_read_contract(ScanIoSource::ObjectStore).await;
    }

    async fn assert_range_read_contract(source: ScanIoSource) {
        let store = Arc::new(RecordingRangeStore {
            inner: InMemory::new(),
            range_calls: AtomicUsize::new(0),
            get_calls: AtomicUsize::new(0),
        });
        let location = Path::from("ranges.parquet");
        store
            .put(&location, Bytes::from_static(b"0123456789").into())
            .await
            .unwrap();
        let runtime = datafusion::execution::runtime_env::RuntimeEnv::default();
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = EagerPageIndexReaderFactory::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            runtime.cache_manager.get_file_metadata_cache(),
            source,
            &metrics,
        );
        let mut reader = factory
            .create_reader(
                0,
                PartitionedFile::new(location.to_string(), 10),
                None,
                &metrics,
            )
            .unwrap();
        let result = reader.get_byte_ranges(vec![0..2, 4..6]).await.unwrap();
        assert_eq!(
            result,
            vec![Bytes::from_static(b"01"), Bytes::from_static(b"45")]
        );
        let remote = source == ScanIoSource::ObjectStore;
        assert_eq!(
            store.range_calls.load(Ordering::Relaxed),
            usize::from(!remote)
        );
        assert_eq!(store.get_calls.load(Ordering::Relaxed), usize::from(remote));
        assert_eq!(
            metrics
                .clone_inner()
                .sum_by_name("scan_io_data_bytes")
                .unwrap()
                .as_usize(),
            4
        );
        assert_eq!(
            metrics
                .clone_inner()
                .sum_by_name("scan_io_object_store_response_bytes_read")
                .unwrap()
                .as_usize(),
            if remote { 6 } else { 0 }
        );
    }

    #[test]
    fn variant_policy_preserves_footer_metadata_and_indexes() {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![arrow::datatypes::Field::new("id", DataType::Int32, false)],
            [("application".to_string(), "keep".to_string())].into(),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "application".to_string(),
                "keep".to_string(),
            )]))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(1)
            .build();
        let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let reader = SerializedFileReader::new_with_options(
            file.reopen().unwrap(),
            ReadOptionsBuilder::new().with_page_index().build(),
        )
        .unwrap();
        let original = Arc::new(reader.metadata().clone());
        let rewritten = with_spark_arrow_schema(Arc::clone(&original)).unwrap();
        assert!(original.column_index().is_some());
        assert!(original.offset_index().is_some());
        assert_eq!(rewritten.column_index(), original.column_index());
        assert_eq!(rewritten.offset_index(), original.offset_index());
        assert_eq!(rewritten.row_groups(), original.row_groups());
        assert_eq!(
            rewritten.file_metadata().column_orders(),
            original.file_metadata().column_orders()
        );
        assert!(original
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .any(|entry| entry.key == ARROW_SCHEMA_META_KEY));
        assert_eq!(
            rewritten.file_metadata().key_value_metadata().unwrap(),
            &vec![KeyValue::new("application".to_string(), "keep".to_string())]
        );
        assert!(Arc::ptr_eq(
            &rewritten,
            &with_spark_arrow_schema(Arc::clone(&rewritten)).unwrap()
        ));
    }
}
