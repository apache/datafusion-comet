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
//! Filed upstream as apache/datafusion#23978. Once the opener merges its deferred page-index
//! load back into `FileMetadataCache` instead of bypassing it, the eager policy can go, but the
//! factory cannot: `get_metadata` is the per-file hook that sees the raw footer.
//!
//! The first is Spark's missing field id check. `ParquetReadSupport` refuses to open a file
//! whose Parquet schema carries no field id when the requested schema carries one, unless
//! `ignoreMissing` is set, and it walks the raw `MessageType` to decide. That walk has to run
//! over the Parquet schema rather than the Arrow schema the schema adapter is handed later,
//! because an id on a repeated `list` or `key_value` group, or on the message root, never
//! reaches an Arrow field. DataFusion 55.2.0 preserves container metadata through INT96 coercion
//! (apache/datafusion#24790), so struct ids no longer vanish on the way to Arrow.
//!
//! The second is the Variant footer rewrite, `with_spark_arrow_schema`, which replaces the
//! Arrow schema hint in the footer for scans that project Variant.
//!
//! The third is the INT96 leaf stamp, `with_int96_leaf_stamp`, enabled by rebase-aware scans: the
//! factory stamps each unencrypted file's INT96 leaf ordinals into the in-memory copy of its footer
//! key-value metadata -- `datetime_rebase::stamp_int96_leaves`, derived from the footer's own
//! `SchemaDescriptor` -- and caches the stamped copy in place of the plain one. parquet-rs copies
//! every key-value pair into the arrow schema it derives from the metadata, which is the only
//! per-file channel DataFusion's opener gives the expression adapter; the stamp is how the adapter
//! tells INT96 timestamp columns from INT64 ones after both were coerced to the same arrow type.
//! The rebuild happens once per file per cache lifetime (later opens find the stamp already
//! present); encrypted opens are left untouched because the parquet API cannot carry a file
//! decryptor across the rebuild. `FileMetadataCache` is keyed by object path and shared by every
//! scan of one `RuntimeEnv`, so a plain (non-stamping) scan of the same file in the same plan sees
//! the stamped copy too; nothing outside the rebase path reads the key, and the copy is otherwise
//! identical.
//!
//! Conversion checks also use the raw footer to preserve legacy LIST clipping failures and to see
//! which leaves the file declares `TIMESTAMP(NANOS)`. With row filters enabled, or for a file with
//! a `TIMESTAMP(NANOS)` leaf even without them, a deferred conversion failure is raised on the
//! first data-page request. Footer, Bloom-filter and page-index reads still proceed, so format
//! pruning can discard a file without decoding it, but row selection cannot hide the failure.
//!
//! With `with_next_file_metadata_prefetch`, readers also load the next file's metadata before
//! DataFusion opens it. DataFusion's `FileStream` keeps one I/O outstanding and plans a file
//! only after the previous file's reader has returned its last batch (apache/datafusion#20916
//! removed its next-file prefetch), so without this every file boundary waits for a footer and
//! page-index round trip. When a file opens, its reader spawns a task that loads the metadata of
//! the next file in the scan's file group through the cache, and that file's open waits for the
//! task instead of reading the footer itself. This assumes DataFusion opens the files in the
//! group's order, which holds unless the source reorders them (`ParquetSource::reorder_files`);
//! a reordered scan would only waste its prefetches.
//!
//! This is a stopgap for the overlap DataFusion dropped. It can go once `FileStream` opens the next
//! file while the current reader runs again; apache/datafusion#5209 (buffer more than one file
//! open) and apache/datafusion#18470 (prefetch the next row group) are the open issues that would
//! bring it back.

use crate::parquet::datetime_rebase::stamp_int96_leaves;
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::{
    check_file_conversions, RejectOnNonEmpty, NANOS_TIMESTAMP_KEY, REPEATED_PRIMITIVE_KEY,
};
use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::physical_plan::parquet::metadata::{
    CachedParquetMetaData, DFParquetMetadata,
};
use datafusion::datasource::physical_plan::parquet::{
    ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion::execution::cache::cache_manager::{CachedFileMetadataEntry, FileMetadataCache};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory, MetricType, MetricValue, Time,
};
use datafusion_comet_common::SparkError;
use datafusion_datasource::{FileRange, PartitionedFile};
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
use parquet::basic::{ConvertedType, LogicalType, TimeUnit as ParquetTimeUnit, TimestampType};
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{FileMetaData, KeyValue, ParquetMetaDataBuilder};
use parquet::file::metadata::{
    FooterTail, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use parquet::schema::types::{ColumnDescPtr, SchemaDescriptor, Type as ParquetType};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Instant;

use crate::parquet::objectstore::http_metrics::HttpRequestMetrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanIoSource {
    ObjectStore,
    Local,
    OtherObjectStore,
}

/// How a scan requests file bytes from storage. The default turns every option off.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ScanIoOptions {
    /// Keep the bytes the footer read fetched from the end of a file, and serve the data ranges
    /// inside them from memory instead of requesting them again.
    pub reuse_footer_read: bool,
    /// Read a file of at most this many bytes whole with its footer read, so with
    /// `reuse_footer_read` the file costs one request. 0 turns it off.
    pub whole_file_read_threshold: u64,
    /// On a remote object store, fetch a merged range longer than this many bytes as parallel
    /// requests of about this size. 0 turns it off.
    pub range_split_size: u64,
}

impl ScanIoOptions {
    /// The size of the footer read for a file of `file_size` bytes: the whole file when it is
    /// within `whole_file_read_threshold` and its bytes will be reused, `hint` otherwise.
    fn footer_read_size(&self, file_size: u64, hint: Option<usize>) -> Option<usize> {
        if self.reuse_footer_read
            && self.whole_file_read_threshold > 0
            && file_size <= self.whole_file_read_threshold
        {
            usize::try_from(file_size).ok().or(hint)
        } else {
            hint
        }
    }
}

/// The bytes a metadata load read from the end of a file. A data range inside them needs no
/// second request.
#[derive(Debug)]
struct FileTail {
    start: u64,
    bytes: Bytes,
}

impl FileTail {
    /// The bytes of `range` when it lies inside the tail.
    fn slice(&self, range: &Range<u64>) -> Option<Bytes> {
        let offset = usize::try_from(range.start.checked_sub(self.start)?).ok()?;
        let end = offset.checked_add(range_bytes(range))?;
        (end <= self.bytes.len()).then(|| self.bytes.slice(offset..end))
    }
}

/// A file as a scan opens it: its path and the byte range of its split.
type FileKey = (Path, Option<FileRange>);

/// The tails of the files that have an open reader. DataFusion reads a file's Bloom filters
/// through the reader that loaded its metadata, then decodes through a new reader that never
/// loads metadata, so the new reader finds the tail here. The first reader holding a tail to
/// drop removes it from the map; any other reader that took it keeps its own reference.
type FileTails = Arc<Mutex<HashMap<FileKey, Weak<FileTail>>>>;

#[derive(Debug)]
struct ScanIoMetrics {
    http_observed_gets: Count,
    http_retries: Count,
    data_bytes: Count,
    metadata_bytes: Count,
    footer_reads: Count,
    footer_bytes: Count,
    object_store_get_calls: Count,
    object_store_get_requested_bytes: Count,
    object_store_response_bytes_read: Count,
    metadata_cache_hits: Count,
    metadata_cache_misses: Count,
    metadata_prefetches: Count,
    metadata_storage_reads: Count,
    footer_read_reused_bytes: Count,
    data_wait_time: Time,
}

impl ScanIoMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            http_observed_gets: count_counter(metrics, "scan_io_http_observed_gets"),
            http_retries: count_counter(metrics, "scan_io_http_retries"),
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
            metadata_prefetches: count_counter(metrics, "scan_io_metadata_prefetches"),
            metadata_storage_reads: count_counter(metrics, "scan_io_metadata_storage_reads"),
            footer_read_reused_bytes: byte_counter(metrics, "scan_io_footer_read_reused_bytes"),
            data_wait_time: time_counter(metrics, "scan_io_data_wait_time"),
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

fn time_counter(metrics: &ExecutionPlanMetricsSet, name: &'static str) -> Time {
    let time = Time::new();
    MetricBuilder::new(metrics)
        .with_type(MetricType::Summary)
        .with_category(MetricCategory::Timing)
        .build(MetricValue::Time {
            name: name.into(),
            time: time.clone(),
        });
    time
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
    // Refuse a file whose Parquet schema carries no field id, as Spark's `ParquetReadSupport`
    // does when the requested schema carries one and `ignoreMissing` is not set.
    require_field_ids: bool,
    stamp_int96_leaves: bool,
    conversion_check: Option<(SchemaRef, SparkParquetOptions, bool)>,
    deferred_rejections: Arc<Mutex<HashMap<Path, (ObjectMeta, RejectOnNonEmpty)>>>,
    next_file_prefetch: Option<Arc<NextFileMetadataPrefetch>>,
    options: ScanIoOptions,
    tails: FileTails,
}

impl EagerPageIndexReaderFactory {
    /// [`Self::with_options`] with every [`ScanIoOptions`] option off.
    #[cfg(test)]
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<FileMetadataCache>,
        source: ScanIoSource,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        Self::with_options(
            store,
            metadata_cache,
            source,
            metrics,
            ScanIoOptions::default(),
        )
    }

    /// Shares the scan's counters and metadata cache across all readers made by this factory.
    /// Native cloud stores are wrapped so coalesced requests and consumed response bytes are
    /// visible to the counters. Local and custom HDFS stores retain their range-read behavior.
    /// `options` decide how readers request file bytes. Construction performs no I/O; the
    /// returned factory owns the store and cache references.
    pub(crate) fn with_options(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<FileMetadataCache>,
        source: ScanIoSource,
        metrics: &ExecutionPlanMetricsSet,
        options: ScanIoOptions,
    ) -> Self {
        let scan_io_metrics = Arc::new(ScanIoMetrics::new(metrics));
        let store: Arc<dyn ObjectStore> = if source == ScanIoSource::ObjectStore {
            Arc::new(ScanIoObjectStore {
                inner: store,
                scan_io_metrics: Arc::clone(&scan_io_metrics),
                role: ScanIoStoreRole::ObjectStore {
                    range_split_size: options.range_split_size,
                },
            })
        } else {
            store
        };
        Self {
            store,
            metadata_cache,
            scan_io_metrics,
            spark_variant_schema: false,
            require_field_ids: false,
            stamp_int96_leaves: false,
            conversion_check: None,
            deferred_rejections: Arc::new(Mutex::new(HashMap::new())),
            next_file_prefetch: None,
            options,
            tails: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Retain raw LIST encoding and `TIMESTAMP(NANOS)` leaves for open-time validation. When row
    /// filters are enabled, or the file has a `TIMESTAMP(NANOS)` leaf, enforce deferred failures
    /// at the first data-page read, after format pruning.
    pub(crate) fn with_conversion_check(
        mut self,
        required: SchemaRef,
        options: SparkParquetOptions,
        row_filters: bool,
    ) -> Self {
        self.conversion_check = Some((required, options, row_filters));
        self
    }

    pub fn with_spark_variant_schema(mut self, enabled: bool) -> Self {
        self.spark_variant_schema = enabled;
        self
    }

    /// Refuse a file whose Parquet schema carries no field id. Off by default, so a factory
    /// that never calls this reads every file.
    pub fn with_require_field_ids(mut self, enabled: bool) -> Self {
        self.require_field_ids = enabled;
        self
    }

    /// Whether readers stamp each unencrypted file's INT96 leaf ordinals into its metadata
    /// (see the module docs). Off by default.
    pub fn with_int96_leaf_stamp(mut self, enabled: bool) -> Self {
        self.stamp_int96_leaves = enabled;
        self
    }

    /// With `Some(files)`, load the footer and page index of the file DataFusion opens next while
    /// the current file is read (see the module docs). `files` is the scan's one file group, in
    /// the order DataFusion opens it. `None`, the default, leaves the prefetch off. A prefetch
    /// carries no decryption properties, so an open that has them neither waits for nor starts
    /// one.
    pub(crate) fn with_next_file_metadata_prefetch(
        mut self,
        files: Option<&[PartitionedFile]>,
    ) -> Self {
        self.next_file_prefetch = files.map(|files| {
            Arc::new(NextFileMetadataPrefetch::new(
                files,
                Arc::clone(&self.store),
                Arc::clone(&self.scan_io_metrics),
                Arc::clone(&self.metadata_cache),
                self.options,
            ))
        });
        self
    }
}

/// True when `node` or any node under it carries a field id, the way Spark's
/// `containsFieldIds` answers it over the raw Parquet schema, message root included.
fn contains_field_ids(node: &ParquetType) -> bool {
    node.get_basic_info().has_id()
        || (node.is_group()
            && node
                .get_fields()
                .iter()
                .any(|field| contains_field_ids(field)))
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

        // DataFusion replaces the metadata reader after Bloom-filter pruning. The replacement
        // receives already-loaded metadata, so retain deferred failures in this scan's factory.
        let deferred_rejection = self
            .deferred_rejections
            .lock()
            .unwrap()
            .get(&partitioned_file.object_meta.location)
            .filter(|(meta, _)| meta == &partitioned_file.object_meta)
            .map(|(_, rejection)| rejection.clone());
        // A reader DataFusion makes after another one loaded this file's metadata serves its
        // data from that load's tail.
        let tail = self
            .tails
            .lock()
            .unwrap()
            .get(&file_key(&partitioned_file))
            .and_then(Weak::upgrade);
        Ok(Box::new(EagerPageIndexReader {
            file_metrics,
            scan_io_metrics: Arc::clone(&self.scan_io_metrics),
            store: Arc::clone(&self.store),
            partitioned_file,
            metadata_cache: Arc::clone(&self.metadata_cache),
            metadata_size_hint,
            spark_variant_schema: self.spark_variant_schema,
            require_field_ids: self.require_field_ids,
            stamp_int96_leaves: self.stamp_int96_leaves,
            conversion_check: self.conversion_check.clone(),
            deferred_rejections: Arc::clone(&self.deferred_rejections),
            deferred_rejection,
            next_file_prefetch: self.next_file_prefetch.clone(),
            options: self.options,
            tails: Arc::clone(&self.tails),
            tail,
        }))
    }
}

fn file_key(file: &PartitionedFile) -> FileKey {
    (file.object_meta.location.clone(), file.range.clone())
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
    require_field_ids: bool,
    stamp_int96_leaves: bool,
    conversion_check: Option<(SchemaRef, SparkParquetOptions, bool)>,
    deferred_rejections: Arc<Mutex<HashMap<Path, (ObjectMeta, RejectOnNonEmpty)>>>,
    deferred_rejection: Option<RejectOnNonEmpty>,
    next_file_prefetch: Option<Arc<NextFileMetadataPrefetch>>,
    options: ScanIoOptions,
    tails: FileTails,
    /// The bytes this file's footer read fetched, which serve the ranges inside them.
    tail: Option<Arc<FileTail>>,
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

/// Whether the file declares `column` as `TIMESTAMP(NANOS)`.
fn is_nanos_timestamp(column: &ColumnDescPtr) -> bool {
    matches!(
        column.logical_type_ref(),
        Some(LogicalType::Timestamp(TimestampType {
            unit: ParquetTimeUnit::NANOS,
            ..
        }))
    )
}

/// Arrow normalizes both standard and legacy LISTs to the same type, and gives INT96 and
/// `TIMESTAMP(NANOS)` leaves the same nanosecond timestamp type. Carry the repeated primitive
/// and `TIMESTAMP(NANOS)` distinctions in an ephemeral schema used by the conversion check,
/// never in the shared footer cache or in the schema given to the decoder.
fn mark_parquet_leaves(schema: &Schema, parquet: &SchemaDescriptor) -> Schema {
    fn mark(field: &FieldRef, columns: &[ColumnDescPtr], index: &mut usize) -> FieldRef {
        let data_type = match field.data_type() {
            DataType::Struct(fields) => DataType::Struct(
                fields
                    .iter()
                    .map(|f| mark(f, columns, index))
                    .collect::<Vec<_>>()
                    .into(),
            ),
            DataType::List(item) => DataType::List(mark(item, columns, index)),
            DataType::LargeList(item) => DataType::LargeList(mark(item, columns, index)),
            DataType::FixedSizeList(item, size) => {
                DataType::FixedSizeList(mark(item, columns, index), *size)
            }
            DataType::ListView(item) => DataType::ListView(mark(item, columns, index)),
            DataType::LargeListView(item) => DataType::LargeListView(mark(item, columns, index)),
            DataType::Map(entries, sorted) => DataType::Map(mark(entries, columns, index), *sorted),
            _ => {
                let column = columns.get(*index);
                *index += 1;
                let repeated = column.is_some_and(|column| {
                    column.self_type().get_basic_info().repetition()
                        == parquet::basic::Repetition::REPEATED
                });
                let nanos = column.is_some_and(is_nanos_timestamp);
                let mut field = field.as_ref().clone();
                if repeated || nanos {
                    let mut metadata = field.metadata().clone();
                    if repeated {
                        metadata.insert(REPEATED_PRIMITIVE_KEY.to_string(), "true".to_string());
                    }
                    if nanos {
                        metadata.insert(NANOS_TIMESTAMP_KEY.to_string(), "true".to_string());
                    }
                    field = field.with_metadata(metadata);
                }
                return Arc::new(field);
            }
        };
        Arc::new(field.as_ref().clone().with_data_type(data_type))
    }
    let mut index = 0;
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| mark(f, parquet.columns(), &mut index))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
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
        if let Some(bytes) = self.tail.as_ref().and_then(|tail| tail.slice(&range)) {
            scan_io_metrics.footer_read_reused_bytes.add(bytes.len());
            scan_io_metrics.metadata_bytes.add(bytes.len());
            return futures::future::ready(Ok(bytes)).boxed();
        }
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
        if ranges.iter().any(|range| !range.is_empty()) {
            if let Some(rejection) = &self.deferred_rejection {
                let error = ParquetError::External(Box::new(rejection.error()));
                return async move { Err(error) }.boxed();
            }
        }
        let requested = ranges_bytes(&ranges);
        self.file_metrics.bytes_scanned.add(requested);
        let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
        // Ranges inside the footer read come from it; only the others go to storage.
        let mut served: Vec<Option<Bytes>> = ranges
            .iter()
            .map(|range| self.tail.as_ref().and_then(|tail| tail.slice(range)))
            .collect();
        let missing: Vec<Range<u64>> = ranges
            .iter()
            .zip(&served)
            .filter(|(_, served)| served.is_none())
            .map(|(range, _)| range.clone())
            .collect();
        scan_io_metrics
            .footer_read_reused_bytes
            .add(served.iter().flatten().map(Bytes::len).sum());
        async move {
            if !missing.is_empty() {
                let started = Instant::now();
                let fetched = self
                    .store
                    .get_ranges(&self.partitioned_file.object_meta.location, &missing)
                    .await;
                scan_io_metrics.data_wait_time.add_elapsed(started);
                let mut fetched = fetched
                    .map_err(|e| ParquetError::External(Box::new(e)))?
                    .into_iter();
                for slot in served.iter_mut().filter(|slot| slot.is_none()) {
                    *slot = Some(fetched.next().ok_or_else(|| {
                        ParquetError::General(
                            "The object store returned fewer ranges than requested".to_string(),
                        )
                    })?);
                }
            }
            let bytes: Vec<Bytes> = served.into_iter().flatten().collect();
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
        let require_field_ids = self.require_field_ids;
        let stamp_enabled = self.stamp_int96_leaves;
        let file_decryption_properties = options
            .and_then(|o| o.file_decryption_properties())
            .map(Arc::clone);
        let cache_enabled = file_decryption_properties.is_none();
        let capture_tail = self.options.reuse_footer_read;
        let footer_read_size = self
            .options
            .footer_read_size(object_meta.size, metadata_size_hint);
        // Take the load of this file's metadata that the previous file's open started, if any,
        // and start the next file's.
        let pending_load = self
            .next_file_prefetch
            .as_ref()
            .filter(|_| cache_enabled)
            .and_then(|prefetch| prefetch.open(&self.partitioned_file, metadata_size_hint));
        async move {
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
            // Wait for the prefetch rather than reading the footer a second time. A prefetch
            // that failed or was cancelled yields nothing, and this open then loads the metadata
            // itself, so any error it reports is the open's own.
            let prefetched = match pending_load {
                Some(load) => load.await.ok().flatten(),
                None => None,
            };
            let loaded = match prefetched {
                Some(loaded) => {
                    // The load read from storage ahead of this open, so the open records a cache
                    // miss, as it would have without the prefetch, and the load counts as overlapped.
                    if loaded.storage_reads > 0 {
                        scan_io_metrics.metadata_prefetches.add(1);
                    }
                    loaded
                }
                None => {
                    load_metadata(
                        store,
                        Arc::clone(&scan_io_metrics),
                        Arc::clone(&metadata_cache),
                        &object_meta,
                        footer_read_size,
                        file_decryption_properties,
                        page_index_policy,
                        capture_tail,
                    )
                    .await?
                }
            };
            let LoadedMetadata {
                metadata,
                storage_reads,
                tail,
            } = loaded;
            if cache_enabled {
                scan_io_metrics.record_metadata_cache_result(storage_reads);
            }
            if let Some(tail) = tail {
                let tail = Arc::new(tail);
                self.tails
                    .lock()
                    .unwrap()
                    .insert(file_key(&self.partitioned_file), Arc::downgrade(&tail));
                self.tail = Some(tail);
            }
            // Spark's missing field id check, over the raw Parquet schema as in
            // `ParquetReadSupport`. The JNI layer unwraps the `External` error, so the JVM sees
            // the exception Spark raises.
            if require_field_ids
                && !contains_field_ids(metadata.file_metadata().schema_descr().root_schema())
            {
                return Err(ParquetError::External(Box::new(
                    SparkError::ParquetMissingFieldIds {
                        file_path: object_meta.location.to_string(),
                    },
                )));
            }
            // Stamp before the Variant rewrite so the shared cache keeps the footer as read
            // plus the stamp, which the rewrite leaves in place; the rewrite is per open and
            // never written back. Encrypted opens (`!cache_enabled`) are never stamped:
            // nothing is cached for them and the rebuild cannot carry a file decryptor.
            let metadata = if stamp_enabled && cache_enabled {
                // First open of this file since the cache last held it: rebuild once with the
                // stamp and replace the cached plain copy so later opens skip the rebuild.
                // Same entry shape `DFParquetMetadata::cache_metadata` stores, so cache
                // validation and page-index reuse behave identically.
                match stamp_int96_leaves(&metadata) {
                    None => metadata,
                    Some(stamped) => {
                        let stamped = Arc::new(stamped);
                        metadata_cache.put(
                            &object_meta.location,
                            CachedFileMetadataEntry::new(
                                object_meta.clone(),
                                Arc::new(CachedParquetMetaData::new(Arc::clone(&stamped))),
                            ),
                        );
                        stamped
                    }
                }
            } else {
                metadata
            };
            let metadata = if spark_variant_schema {
                with_spark_arrow_schema(metadata)?
            } else {
                metadata
            };
            if let Some((required, conversion_options, row_filters)) =
                self.conversion_check.as_ref()
            {
                let file = metadata.file_metadata();
                let columns = file.schema_descr().columns();
                // Only this check sees the Parquet type of a nanosecond timestamp leaf, so a
                // `TIMESTAMP(NANOS)` rejection is deferred from here even without row filters.
                let has_nanos = columns.iter().any(is_nanos_timestamp);
                let needs_check = *row_filters
                    || has_nanos
                    || columns.iter().any(|column| {
                        column.self_type().get_basic_info().repetition()
                            == parquet::basic::Repetition::REPEATED
                    });
                if !needs_check {
                    return Ok(metadata);
                }
                let schema =
                    parquet_to_arrow_schema(file.schema_descr(), file.key_value_metadata())?;
                let schema = mark_parquet_leaves(&schema, file.schema_descr());
                let rejection = check_file_conversions(
                    Arc::clone(required),
                    Arc::new(schema),
                    conversion_options.clone(),
                )
                .map_err(|e| match e {
                    // Keep structured Spark errors directly inside the Parquet wrapper so
                    // the JNI error converter preserves Spark's exception type and class.
                    DataFusionError::External(source) => ParquetError::External(source),
                    other => ParquetError::External(Box::new(other)),
                })?;
                if *row_filters || has_nanos {
                    if let Some(rejection) = &rejection {
                        self.deferred_rejections.lock().unwrap().insert(
                            object_meta.location.clone(),
                            (object_meta.clone(), rejection.clone()),
                        );
                    }
                    self.deferred_rejection = rejection;
                }
            }
            Ok(metadata)
        }
        .boxed()
    }
}

/// A file's metadata, the number of storage reads its load took (zero when the cache held the
/// metadata), and the bytes the load read from the end of the file, when it was asked to keep
/// them and read any.
#[derive(Debug)]
struct LoadedMetadata {
    metadata: Arc<ParquetMetaData>,
    storage_reads: usize,
    tail: Option<FileTail>,
}

/// Loads a file's metadata through `metadata_cache`, with the page index as `page_index_policy`
/// asks, counting the metadata and footer bytes and the storage reads it takes. With
/// `capture_tail`, it keeps the bytes of its read that ends at the end of the file.
#[allow(clippy::too_many_arguments)]
async fn load_metadata(
    store: Arc<dyn ObjectStore>,
    scan_io_metrics: Arc<ScanIoMetrics>,
    metadata_cache: Arc<FileMetadataCache>,
    object_meta: &ObjectMeta,
    metadata_size_hint: Option<usize>,
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    page_index_policy: Option<PageIndexPolicy>,
    capture_tail: bool,
) -> ParquetResult<LoadedMetadata> {
    let cache_enabled = file_decryption_properties.is_none();
    let storage_reads = Arc::new(AtomicUsize::new(0));
    let footer_payload_bytes = Arc::new(AtomicUsize::new(0));
    let metadata_store = ScanIoObjectStore {
        inner: store,
        scan_io_metrics: Arc::clone(&scan_io_metrics),
        role: ScanIoStoreRole::Metadata {
            storage_reads: Arc::clone(&storage_reads),
            footer_payload_bytes: Arc::clone(&footer_payload_bytes),
            footer_payload: OnceLock::new(),
            footer_recorded: AtomicBool::new(false),
            file_size: object_meta.size,
            record_footer_immediately: !cache_enabled,
            capture_tail,
            tail: OnceLock::new(),
        },
    };

    let metadata = DFParquetMetadata::new(&metadata_store, object_meta)
        .with_decryption_properties(file_decryption_properties)
        .with_file_metadata_cache(Some(metadata_cache))
        .with_metadata_size_hint(metadata_size_hint)
        .with_page_index_policy(page_index_policy)
        .fetch_metadata()
        .await
        .map_err(|e| {
            ParquetError::General(format!(
                "Failed to fetch metadata for file {}: {e}",
                object_meta.location,
            ))
        });

    if metadata.is_ok() {
        let footer_bytes = footer_payload_bytes.load(Ordering::Relaxed);
        if footer_bytes > 0 && cache_enabled {
            metadata_store.record_footer(footer_bytes);
        }
    } else if cache_enabled {
        metadata_store.record_valid_footer();
    }
    let storage_reads = storage_reads.load(Ordering::Relaxed);
    scan_io_metrics.metadata_storage_reads.add(storage_reads);
    let metadata = metadata?;
    let tail = match metadata_store.role {
        ScanIoStoreRole::Metadata { tail, .. } => tail.into_inner(),
        ScanIoStoreRole::ObjectStore { .. } => None,
    };
    Ok(LoadedMetadata {
        metadata,
        storage_reads,
        tail,
    })
}

/// A load of a file's metadata started before DataFusion opens the file. Dropping it aborts the
/// load.
/// Its result carries the number of storage reads it took, for the open that uses it to record.
type MetadataLoad = SpawnedTask<Option<LoadedMetadata>>;

/// The scan-wide state of `with_next_file_metadata_prefetch`.
#[derive(Debug)]
struct NextFileMetadataPrefetch {
    store: Arc<dyn ObjectStore>,
    scan_io_metrics: Arc<ScanIoMetrics>,
    metadata_cache: Arc<FileMetadataCache>,
    /// For each file of the group, keyed by path and byte range, the next file with another path
    /// and that file's metadata size hint. Further ranges of a file's own path are skipped,
    /// because opening the file already loads their metadata.
    next_files: HashMap<(Path, Option<FileRange>), (ObjectMeta, Option<usize>)>,
    /// The load in flight, with the path it loads. Each open replaces it unless it is for the
    /// next file, so a load for a file DataFusion never opens, such as one pruned before its
    /// open, is dropped at the next open.
    in_flight: Mutex<Option<(Path, MetadataLoad)>>,
    /// The scan's options, so a load reads and keeps the footer the way the file's own open
    /// would.
    options: ScanIoOptions,
}

impl NextFileMetadataPrefetch {
    fn new(
        files: &[PartitionedFile],
        store: Arc<dyn ObjectStore>,
        scan_io_metrics: Arc<ScanIoMetrics>,
        metadata_cache: Arc<FileMetadataCache>,
        options: ScanIoOptions,
    ) -> Self {
        // One pass from the back, so a long run of ranges of one path costs linear time. The next
        // file of `files[i]` is `files[i + 1]` when its path differs and `files[i + 1]`'s own next
        // file otherwise. Inserting while walking backwards lets the earliest of any duplicate
        // key win.
        let mut next_files = HashMap::new();
        let mut next: Option<&PartitionedFile> = None;
        for (position, file) in files.iter().enumerate().rev() {
            let location = &file.object_meta.location;
            if let Some(following) = files.get(position + 1) {
                if &following.object_meta.location != location {
                    next = Some(following);
                }
            }
            if let Some(next) = next {
                next_files.insert(
                    (location.clone(), file.range.clone()),
                    (next.object_meta.clone(), next.metadata_size_hint),
                );
            }
        }
        Self {
            store,
            scan_io_metrics,
            metadata_cache,
            next_files,
            in_flight: Mutex::new(None),
            options,
        }
    }

    /// Called as `file` opens: takes the load of its metadata that the previous open started, if
    /// any, and starts loading the next file's.
    fn open(
        &self,
        file: &PartitionedFile,
        metadata_size_hint: Option<usize>,
    ) -> Option<MetadataLoad> {
        let mut in_flight = self.in_flight.lock().unwrap();
        let pending = in_flight
            .take_if(|(location, _)| *location == file.object_meta.location)
            .map(|(_, load)| load);
        let key = (file.object_meta.location.clone(), file.range.clone());
        match self.next_files.get(&key) {
            // Another range of an earlier file already started this load.
            Some((next, _))
                if in_flight
                    .as_ref()
                    .is_some_and(|(location, _)| *location == next.location) => {}
            next => {
                *in_flight =
                    next.and_then(|(next, hint)| self.start(next, hint.or(metadata_size_hint)));
            }
        }
        pending
    }

    /// Starts loading `object_meta`'s metadata on the current Tokio runtime, if there is one.
    fn start(
        &self,
        object_meta: &ObjectMeta,
        metadata_size_hint: Option<usize>,
    ) -> Option<(Path, MetadataLoad)> {
        tokio::runtime::Handle::try_current().ok()?;
        let store = Arc::clone(&self.store);
        let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
        let metadata_cache = Arc::clone(&self.metadata_cache);
        let metadata_size_hint = self
            .options
            .footer_read_size(object_meta.size, metadata_size_hint);
        let capture_tail = self.options.reuse_footer_read;
        let object_meta = object_meta.clone();
        let location = object_meta.location.clone();
        let load = SpawnedTask::spawn(async move {
            load_metadata(
                store,
                scan_io_metrics,
                metadata_cache,
                &object_meta,
                metadata_size_hint,
                None,
                Some(PageIndexPolicy::Optional),
                capture_tail,
            )
            .await
            .ok()
        });
        Some((location, load))
    }
}

#[derive(Debug)]
enum ScanIoStoreRole {
    ObjectStore {
        /// See [`ScanIoOptions::range_split_size`].
        range_split_size: u64,
    },
    Metadata {
        storage_reads: Arc<AtomicUsize>,
        footer_payload_bytes: Arc<AtomicUsize>,
        footer_payload: OnceLock<Bytes>,
        footer_recorded: AtomicBool,
        file_size: u64,
        record_footer_immediately: bool,
        /// Keep the bytes of the first read that ends at the end of the file in `tail`.
        capture_tail: bool,
        tail: OnceLock<FileTail>,
    },
}

/// At most this many parts of a split range are requested at once.
const RANGE_PART_CONCURRENCY: usize = 16;

/// Fetches `ranges` with `fetch` the way `coalesce_ranges` does, merging ranges up to
/// `OBJECT_STORE_COALESCE_DEFAULT` apart, except that a merged range longer than `part_size`
/// is fetched as parts of at most `part_size` bytes, many at once. A single GET is limited by
/// one connection's throughput, so the parts of a large range arrive sooner together.
async fn fetch_in_parts<F, Fut>(
    ranges: &[Range<u64>],
    part_size: u64,
    fetch: F,
) -> ObjectStoreResult<Vec<Bytes>>
where
    F: Fn(Range<u64>) -> Fut,
    Fut: std::future::Future<Output = ObjectStoreResult<Bytes>>,
{
    let parts: Vec<Range<u64>> = merge_ranges(ranges, OBJECT_STORE_COALESCE_DEFAULT)
        .iter()
        .flat_map(|merged| split_range(merged, part_size))
        .collect();
    let fetched: Vec<Bytes> = futures::stream::iter(parts.iter().cloned())
        .map(fetch)
        .buffered(RANGE_PART_CONCURRENCY)
        .try_collect()
        .await?;
    for (part, bytes) in parts.iter().zip(&fetched) {
        if bytes.len() != range_bytes(part) {
            return Err(object_store::Error::Generic {
                store: "scan-io",
                source: format!(
                    "requested {} bytes at {} and received {}",
                    range_bytes(part),
                    part.start,
                    bytes.len()
                )
                .into(),
            });
        }
    }
    Ok(ranges
        .iter()
        .map(|range| assemble_range(range, &parts, &fetched))
        .collect())
}

/// Sorts `ranges` and merges those that overlap or lie at most `coalesce` bytes apart, the
/// merge `object_store::coalesce_ranges` does. Empty ranges are dropped.
fn merge_ranges(ranges: &[Range<u64>], coalesce: u64) -> Vec<Range<u64>> {
    let mut sorted: Vec<Range<u64>> = ranges
        .iter()
        .filter(|range| !range.is_empty())
        .cloned()
        .collect();
    sorted.sort_unstable_by_key(|range| range.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(sorted.len());
    for range in sorted {
        match merged.last_mut() {
            Some(last) if range.start <= last.end.saturating_add(coalesce) => {
                last.end = last.end.max(range.end);
            }
            _ => merged.push(range),
        }
    }
    merged
}

/// Splits `range` into the fewest parts of at most `part_size` bytes, all of one length except
/// a shorter last part.
fn split_range(range: &Range<u64>, part_size: u64) -> impl Iterator<Item = Range<u64>> + '_ {
    let length = range.end - range.start;
    let parts = length.div_ceil(part_size).max(1);
    let part_length = length.div_ceil(parts);
    (0..parts).map(move |part| {
        let start = range.start + part * part_length;
        start..(start + part_length).min(range.end)
    })
}

/// The bytes of `range`, cut from the sorted, contiguous `parts` that cover it.
fn assemble_range(range: &Range<u64>, parts: &[Range<u64>], fetched: &[Bytes]) -> Bytes {
    if range.is_empty() {
        return Bytes::new();
    }
    let first = parts.partition_point(|part| part.end <= range.start);
    let part = &parts[first];
    let offset = (range.start - part.start) as usize;
    if range.end <= part.end {
        return fetched[first].slice(offset..offset + range_bytes(range));
    }
    let mut bytes = bytes::BytesMut::with_capacity(range_bytes(range));
    for (part, part_bytes) in parts[first..].iter().zip(&fetched[first..]) {
        let from = range.start.max(part.start) - part.start;
        let to = range.end.min(part.end) - part.start;
        bytes.extend_from_slice(&part_bytes[from as usize..to as usize]);
        if part.end >= range.end {
            break;
        }
    }
    bytes.freeze()
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
            ScanIoStoreRole::ObjectStore { .. } => {
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
            ScanIoStoreRole::ObjectStore { .. } => self
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

    async fn get_opts(
        &self,
        location: &Path,
        mut options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        if options.head {
            return self.inner.get_opts(location, options).await;
        }
        if matches!(self.role, ScanIoStoreRole::ObjectStore { .. }) {
            HttpRequestMetrics::new(
                self.scan_io_metrics.http_observed_gets.clone(),
                self.scan_io_metrics.http_retries.clone(),
            )
            .track(&mut options);
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
            ScanIoStoreRole::ObjectStore { range_split_size } if *range_split_size > 0 => {
                fetch_in_parts(ranges, *range_split_size, |range| {
                    self.get_range(location, range)
                })
                .await
            }
            ScanIoStoreRole::ObjectStore { .. } => {
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
                capture_tail,
                tail,
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
                    // The first read that reaches the end of the file is the footer read.
                    if *capture_tail && range.end == *file_size && bytes.len() == range_bytes(range)
                    {
                        let _ = tail.set(FileTail {
                            start: range.start,
                            bytes: bytes.clone(),
                        });
                    }
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
        // Unregister this file's tail when a reader holding it drops. Any other reader that took
        // it keeps its own reference.
        if let Some(tail) = &self.tail {
            let key = file_key(&self.partitioned_file);
            let mut tails = self.tails.lock().unwrap();
            if tails
                .get(&key)
                .is_some_and(|registered| std::ptr::eq(registered.as_ptr(), Arc::as_ptr(tail)))
            {
                tails.remove(&key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{array::Int32Array, record_batch::RecordBatch};
    use object_store::memory::InMemory;
    use parquet::{
        arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY},
        file::{
            properties::{EnabledStatistics, WriterProperties},
            reader::FileReader,
            serialized_reader::{ReadOptionsBuilder, SerializedFileReader},
        },
    };

    /// An `InMemory` store that counts the gets and range calls of each path. Reads of the path
    /// set by `with_gate` wait until `release` is called, and each held read keeps a receiver of
    /// `gate`.
    #[derive(Debug)]
    struct RecordingStore {
        inner: InMemory,
        gets: Mutex<HashMap<Path, usize>>,
        range_calls: Mutex<HashMap<Path, usize>>,
        gated: Option<Path>,
        gate: tokio::sync::watch::Sender<bool>,
    }

    impl RecordingStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                gets: Mutex::new(HashMap::new()),
                range_calls: Mutex::new(HashMap::new()),
                gated: None,
                gate: tokio::sync::watch::channel(false).0,
            }
        }

        fn with_gate(mut self, gated: &str) -> Self {
            self.gated = Some(Path::from(gated));
            self
        }

        fn release(&self) {
            self.gate.send_replace(true);
        }

        /// The gets and range calls of `location`.
        fn reads(&self, location: &str) -> usize {
            let location = Path::from(location);
            [&self.gets, &self.range_calls]
                .iter()
                .map(|calls| calls.lock().unwrap().get(&location).copied().unwrap_or(0))
                .sum()
        }

        fn get_calls(&self) -> usize {
            self.gets.lock().unwrap().values().sum()
        }

        fn range_calls(&self) -> usize {
            self.range_calls.lock().unwrap().values().sum()
        }

        /// Waits until a read of the gated path is held at the gate.
        async fn wait_for_held_read(&self) {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while self.gate.receiver_count() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the gated path was never read");
        }

        async fn hold_if_gated(&self, location: &Path) {
            if self.gated.as_ref() == Some(location) {
                let mut gate = self.gate.subscribe();
                gate.wait_for(|released| *released).await.unwrap();
            }
        }
    }

    impl Display for RecordingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "recording-store")
        }
    }

    #[async_trait]
    impl ObjectStore for RecordingStore {
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
            *self.gets.lock().unwrap().entry(p.clone()).or_default() += 1;
            self.hold_if_gated(p).await;
            self.inner.get_opts(p, o).await
        }

        async fn get_ranges(
            &self,
            p: &Path,
            ranges: &[Range<u64>],
        ) -> ObjectStoreResult<Vec<Bytes>> {
            *self
                .range_calls
                .lock()
                .unwrap()
                .entry(p.clone())
                .or_default() += 1;
            self.hold_if_gated(p).await;
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
        let store = Arc::new(RecordingStore::new());
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
        assert_eq!(store.range_calls(), usize::from(!remote));
        assert_eq!(store.get_calls(), usize::from(remote));
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

    /// The raw schema walk answers like Spark's `containsFieldIds`: an id on the message root,
    /// on a leaf, or on a repeated `list` or `key_value` group counts, and a schema without
    /// any does not.
    #[test]
    fn contains_field_ids_sees_ids_on_any_node() {
        use parquet::basic::Type as PhysicalType;
        use parquet::schema::types::TypePtr;

        let leaf = |name: &str, id: Option<i32>| -> TypePtr {
            Arc::new(
                ParquetType::primitive_type_builder(name, PhysicalType::INT32)
                    .with_id(id)
                    .build()
                    .unwrap(),
            )
        };
        let group = |name: &str, id: Option<i32>, fields: Vec<TypePtr>| -> TypePtr {
            Arc::new(
                ParquetType::group_type_builder(name)
                    .with_id(id)
                    .with_fields(fields)
                    .build()
                    .unwrap(),
            )
        };

        assert!(!contains_field_ids(&group("schema", None, vec![])));
        assert!(!contains_field_ids(&group(
            "schema",
            None,
            vec![leaf("a", None)]
        )));
        assert!(contains_field_ids(&group(
            "schema",
            Some(1),
            vec![leaf("a", None)]
        )));
        assert!(contains_field_ids(&group(
            "schema",
            None,
            vec![leaf("a", Some(1))]
        )));
        let list_group_only = group(
            "schema",
            None,
            vec![group(
                "l",
                None,
                vec![group("list", Some(5), vec![leaf("element", None)])],
            )],
        );
        assert!(contains_field_ids(&list_group_only));
        let key_value_group_only = group(
            "schema",
            None,
            vec![group(
                "m",
                None,
                vec![group(
                    "key_value",
                    Some(6),
                    vec![leaf("key", None), leaf("value", None)],
                )],
            )],
        );
        assert!(contains_field_ids(&key_value_group_only));
        let nested_without_ids = group(
            "schema",
            None,
            vec![group("s", None, vec![leaf("a", None)])],
        );
        assert!(!contains_field_ids(&nested_without_ids));
    }

    /// Write a one-column `a: int32` file into `store`, with a field id on `a` when `id` is
    /// set, and return the file as `PartitionedFile`.
    async fn put_int_file(store: &InMemory, location: &str, id: Option<&str>) -> PartitionedFile {
        let mut field = arrow::datatypes::Field::new("a", DataType::Int32, false);
        if let Some(id) = id {
            field = field
                .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())].into());
        }
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let size = bytes.len() as u64;
        store
            .put(&Path::from(location), Bytes::from(bytes).into())
            .await
            .unwrap();
        PartitionedFile::new(location.to_string(), size)
    }

    /// A reader made by a factory with `require_field_ids` set refuses a file without ids on
    /// its first metadata fetch, naming the file, and reads one that carries an id. A factory
    /// without it reads the file without ids.
    #[tokio::test]
    async fn get_metadata_refuses_a_file_without_ids_only_when_required() {
        let store = Arc::new(InMemory::new());
        let without_ids = put_int_file(&store, "no_ids.parquet", None).await;
        let with_ids = put_int_file(&store, "ids.parquet", Some("1")).await;
        let runtime = datafusion::execution::runtime_env::RuntimeEnv::default();
        let metrics = ExecutionPlanMetricsSet::new();
        let metadata_for = |require: bool, file: PartitionedFile| {
            let factory = EagerPageIndexReaderFactory::new(
                Arc::clone(&store) as Arc<dyn ObjectStore>,
                runtime.cache_manager.get_file_metadata_cache(),
                ScanIoSource::Local,
                &metrics,
            )
            .with_require_field_ids(require);
            let mut reader = factory.create_reader(0, file, None, &metrics).unwrap();
            async move { reader.get_metadata(None).await }
        };

        let err = metadata_for(true, without_ids.clone())
            .await
            .expect_err("a file without ids must be refused");
        match err {
            ParquetError::External(inner) => assert!(
                matches!(
                    inner.downcast_ref::<SparkError>(),
                    Some(SparkError::ParquetMissingFieldIds { file_path }) if file_path == "no_ids.parquet"
                ),
                "unexpected error: {inner}"
            ),
            other => panic!("unexpected error: {other}"),
        }
        assert!(metadata_for(true, with_ids).await.is_ok());
        assert!(metadata_for(false, without_ids).await.is_ok());
    }

    #[tokio::test]
    async fn conversion_check_preserves_structured_spark_errors() {
        let store = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("B", DataType::Int32, false),
            arrow::datatypes::Field::new("b", DataType::Int32, false),
        ]));
        let values: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&values), values]).unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let size = bytes.len() as u64;
        let location = "duplicates.parquet";
        store
            .put(&Path::from(location), Bytes::from(bytes).into())
            .await
            .unwrap();
        let runtime = datafusion::execution::runtime_env::RuntimeEnv::default();
        let metrics = ExecutionPlanMetricsSet::new();
        let required = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "B",
            DataType::Int32,
            false,
        )]));
        let mut options =
            SparkParquetOptions::new(datafusion_comet_spark_expr::EvalMode::Legacy, "UTC");
        options.case_sensitive = false;
        let factory = EagerPageIndexReaderFactory::new(
            store,
            runtime.cache_manager.get_file_metadata_cache(),
            ScanIoSource::Local,
            &metrics,
        )
        .with_conversion_check(required, options, true);
        let mut reader = factory
            .create_reader(
                0,
                PartitionedFile::new(location.to_string(), size),
                None,
                &metrics,
            )
            .unwrap();
        let error = reader.get_metadata(None).await.unwrap_err();
        let ParquetError::External(source) = error else {
            panic!("expected structured Spark error, got {error}");
        };
        assert!(
            matches!(
                source.downcast_ref::<SparkError>(),
                Some(SparkError::DuplicateFieldCaseInsensitive { .. })
            ),
            "unexpected error: {source}"
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

    async fn put_int_files(store: &InMemory, locations: &[&str]) -> Vec<PartitionedFile> {
        let mut files = vec![];
        for location in locations {
            files.push(put_int_file(store, location, None).await);
        }
        files
    }

    fn prefetching_factory(
        store: Arc<dyn ObjectStore>,
        files: &[PartitionedFile],
        metrics: &ExecutionPlanMetricsSet,
    ) -> EagerPageIndexReaderFactory {
        let runtime = datafusion::execution::runtime_env::RuntimeEnv::default();
        EagerPageIndexReaderFactory::new(
            store,
            runtime.cache_manager.get_file_metadata_cache(),
            ScanIoSource::Local,
            metrics,
        )
        .with_next_file_metadata_prefetch(Some(files))
    }

    /// Opens `file` the way DataFusion's opener does: a new reader, then its metadata.
    async fn open(
        factory: &EagerPageIndexReaderFactory,
        file: &PartitionedFile,
        metrics: &ExecutionPlanMetricsSet,
    ) -> ParquetResult<Arc<ParquetMetaData>> {
        let mut reader = factory
            .create_reader(0, file.clone(), None, metrics)
            .unwrap();
        reader.get_metadata(None).await
    }

    fn metric(metrics: &ExecutionPlanMetricsSet, name: &str) -> usize {
        metrics
            .clone_inner()
            .sum_by_name(name)
            .unwrap_or_else(|| panic!("missing metric {name}"))
            .as_usize()
    }

    fn in_flight_is_empty(factory: &EagerPageIndexReaderFactory) -> bool {
        let prefetch = factory.next_file_prefetch.as_ref().unwrap();
        let in_flight = prefetch.in_flight.lock().unwrap();
        in_flight.is_none()
    }

    /// Opening a file loads the next file's metadata in the background. Every file is read from
    /// storage, so each open is a cache miss, and the two loads that ran ahead of their open are
    /// counted as prefetches. Opening the last file starts nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn opening_a_file_loads_the_next_files_metadata_ahead() {
        let store = Arc::new(InMemory::new());
        let files = put_int_files(&store, &["a.parquet", "b.parquet", "c.parquet"]).await;
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = prefetching_factory(store, &files, &metrics);

        for file in &files {
            let metadata = open(&factory, file, &metrics).await.unwrap();
            assert_eq!(metadata.file_metadata().num_rows(), 2);
            assert!(metadata.column_index().is_some() && metadata.offset_index().is_some());
        }

        assert_eq!(metric(&metrics, "scan_io_metadata_cache_misses"), 3);
        assert_eq!(metric(&metrics, "scan_io_metadata_cache_hits"), 0);
        assert_eq!(metric(&metrics, "scan_io_metadata_prefetches"), 2);
        assert_eq!(metric(&metrics, "scan_io_footer_reads"), 3);
        assert!(in_flight_is_empty(&factory));
    }

    /// A file DataFusion never opens, such as one pruned before its open, doesn't keep the load
    /// started for it: the next open reads its own metadata, and the load it then starts replaces
    /// the stale one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_load_for_a_file_never_opened_is_replaced_by_the_next_one() {
        let store = Arc::new(InMemory::new());
        let files = put_int_files(
            &store,
            &["a.parquet", "b.parquet", "c.parquet", "d.parquet"],
        )
        .await;
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = prefetching_factory(store, &files, &metrics);

        // b is never opened.
        for file in [&files[0], &files[2], &files[3]] {
            open(&factory, file, &metrics).await.unwrap();
        }

        assert_eq!(metric(&metrics, "scan_io_metadata_cache_misses"), 3);
        assert_eq!(metric(&metrics, "scan_io_metadata_cache_hits"), 0);
        assert_eq!(metric(&metrics, "scan_io_metadata_prefetches"), 1);
        assert!(in_flight_is_empty(&factory));
    }

    /// A file whose metadata is still being read ahead when DataFusion opens it waits for that
    /// read instead of starting its own.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_open_waits_for_the_load_of_its_metadata_in_flight() {
        let store = Arc::new(RecordingStore::new().with_gate("b.parquet"));
        let files = put_int_files(&store.inner, &["a.parquet", "b.parquet"]).await;
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = prefetching_factory(Arc::clone(&store) as _, &files, &metrics);
        open(&factory, &files[0], &metrics).await.unwrap();
        store.wait_for_held_read().await;

        let mut metadata = Box::pin(open(&factory, &files[1], &metrics));
        assert!((&mut metadata).now_or_never().is_none());
        assert_eq!(store.reads("b.parquet"), 1, "the open read b itself");
        store.release();
        assert_eq!(metadata.await.unwrap().file_metadata().num_rows(), 2);

        assert_eq!(metric(&metrics, "scan_io_metadata_cache_misses"), 2);
        assert_eq!(metric(&metrics, "scan_io_metadata_cache_hits"), 0);
        assert_eq!(metric(&metrics, "scan_io_metadata_prefetches"), 1);
        assert_eq!(metric(&metrics, "scan_io_footer_reads"), 2);
    }

    /// The next file is the next one with a different path. Further ranges of a file share its
    /// metadata, so they share its next file too, and opening them doesn't start a second load.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_next_file_skips_further_ranges_of_the_same_path() {
        let store = Arc::new(RecordingStore::new());
        let a = put_int_file(&store.inner, "a.parquet", None).await;
        let b = put_int_file(&store.inner, "b.parquet", None).await;
        let files = [
            a.clone().with_range(0, 50),
            a.with_range(50, 100),
            b.clone(),
        ];
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = prefetching_factory(Arc::clone(&store) as _, &files, &metrics);

        for file in &files {
            open(&factory, file, &metrics).await.unwrap();
        }

        // b is read once, by the load that the first range of a started. A second range of a
        // with no next file would drop that load, and b's open would read it again.
        let single_load = Arc::new(RecordingStore::new());
        let alone = put_int_file(&single_load.inner, "b.parquet", None).await;
        let alone_metrics = ExecutionPlanMetricsSet::new();
        let alone_factory = prefetching_factory(
            Arc::clone(&single_load) as _,
            std::slice::from_ref(&alone),
            &alone_metrics,
        );
        open(&alone_factory, &alone, &alone_metrics).await.unwrap();
        assert_eq!(store.reads("b.parquet"), single_load.reads("b.parquet"));
        assert_eq!(metric(&metrics, "scan_io_metadata_prefetches"), 1);
        assert!(in_flight_is_empty(&factory));
    }

    /// The map of next files comes from one pass over the group, so a path that comes back later
    /// keeps the next file of its earliest range, and a long run of ranges of one path builds
    /// without a search per range.
    #[tokio::test]
    async fn the_next_file_map_handles_repeated_paths_and_long_runs() {
        let file = |name: &str| PartitionedFile::new(name.to_string(), 100);
        let mut files = vec![
            file("a.parquet"),
            file("b.parquet"),
            file("a.parquet"),
            file("c.parquet"),
        ];
        // 40,000 ranges of one path: quadratic construction takes seconds, a linear pass doesn't.
        files.extend((0..40_000).map(|i| file("d.parquet").with_range(i, i + 1)));
        let metrics = ExecutionPlanMetricsSet::new();
        let started = std::time::Instant::now();
        let factory = prefetching_factory(Arc::new(InMemory::new()), &files, &metrics);
        assert!(started.elapsed() < std::time::Duration::from_secs(1));

        let prefetch = factory.next_file_prefetch.as_ref().unwrap();
        let next = |file: &PartitionedFile| {
            prefetch
                .next_files
                .get(&(file.object_meta.location.clone(), file.range.clone()))
                .map(|(next, _)| next.location.to_string())
        };
        // The first a wins over the second, which would give c.
        assert_eq!(next(&files[0]).as_deref(), Some("b.parquet"));
        assert_eq!(next(&files[1]).as_deref(), Some("a.parquet"));
        assert_eq!(next(&files[3]).as_deref(), Some("d.parquet"));
        // d has no file after its ranges.
        assert_eq!(next(&files[4]), None);
        assert_eq!(next(&files[files.len() - 1]), None);
        assert_eq!(prefetch.next_files.len(), 3);
    }

    /// A prefetch that fails yields nothing, so the file's open loads the metadata itself and
    /// reports the error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_prefetch_leaves_the_open_to_report_its_own_error() {
        let store = Arc::new(RecordingStore::new());
        let a = put_int_file(&store.inner, "a.parquet", None).await;
        let content = Bytes::from_static(b"not a parquet file");
        let bad = PartitionedFile::new("bad.parquet".to_string(), content.len() as u64);
        store
            .put(&Path::from("bad.parquet"), content.into())
            .await
            .unwrap();
        let metrics = ExecutionPlanMetricsSet::new();
        let files = [a, bad];
        let factory = prefetching_factory(Arc::clone(&store) as _, &files, &metrics);

        open(&factory, &files[0], &metrics).await.unwrap();
        // The prefetch's failure doesn't surface until the open of `bad`, which reads it again.
        let error = open(&factory, &files[1], &metrics).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Failed to fetch metadata for file bad.parquet"),
            "unexpected error: {error}"
        );
        assert!(
            store.reads("bad.parquet") >= 2,
            "both the prefetch and the open should read bad.parquet",
        );
        assert_eq!(metric(&metrics, "scan_io_metadata_prefetches"), 0);
    }

    /// Dropping the scan cancels a metadata load that is still in flight.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_the_scan_aborts_a_metadata_load_in_flight() {
        let store = Arc::new(RecordingStore::new().with_gate("b.parquet"));
        let files = put_int_files(&store.inner, &["a.parquet", "b.parquet"]).await;
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = prefetching_factory(Arc::clone(&store) as _, &files, &metrics);
        open(&factory, &files[0], &metrics).await.unwrap();
        store.wait_for_held_read().await;

        drop(factory);
        // The held read drops its gate receiver only when its task is aborted.
        tokio::time::timeout(std::time::Duration::from_secs(10), store.gate.closed())
            .await
            .expect("the metadata load in flight was not aborted");
        assert_eq!(metric(&metrics, "scan_io_metadata_prefetches"), 0);
    }

    fn scan_io_options(reuse: bool, whole_file: u64, split: u64) -> ScanIoOptions {
        ScanIoOptions {
            reuse_footer_read: reuse,
            whole_file_read_threshold: whole_file,
            range_split_size: split,
        }
    }

    /// Writes an `a: int32` column holding `0..row_groups * rows` in row groups of `rows` rows,
    /// without dictionary encoding, and returns the file.
    async fn put_row_group_file(
        store: &InMemory,
        location: &str,
        row_groups: usize,
        rows: usize,
    ) -> PartitionedFile {
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows))
            .set_dictionary_enabled(false)
            .build();
        let values: Vec<i32> = (0..(row_groups * rows) as i32).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let size = bytes.len() as u64;
        store
            .put(&Path::from(location), Bytes::from(bytes).into())
            .await
            .unwrap();
        PartitionedFile::new(location.to_string(), size)
    }

    /// The byte range of row group `index`'s column chunk, read with the synchronous reader.
    async fn row_group_range(store: &InMemory, file: &PartitionedFile, index: usize) -> Range<u64> {
        let bytes = store
            .get(&file.object_meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let reader = SerializedFileReader::new(bytes).unwrap();
        let (start, length) = reader.metadata().row_group(index).column(0).byte_range();
        start..start + length
    }

    /// Arrow's stream builder takes a sized `AsyncFileReader`, so this forwards to a factory's
    /// boxed reader.
    struct ForwardingReader(Box<dyn AsyncFileReader + Send>);

    impl AsyncFileReader for ForwardingReader {
        fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
            self.0.get_bytes(range)
        }

        fn get_byte_ranges(
            &mut self,
            ranges: Vec<Range<u64>>,
        ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
            self.0.get_byte_ranges(ranges)
        }

        fn get_metadata<'a>(
            &'a mut self,
            options: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
            self.0.get_metadata(options)
        }
    }

    /// Reads every row the way DataFusion's opener does: the metadata, then one request per row
    /// group.
    async fn read_all(reader: Box<dyn AsyncFileReader + Send>) -> Vec<i32> {
        let stream = parquet::arrow::ParquetRecordBatchStreamBuilder::new(ForwardingReader(reader))
            .await
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// Reads every row of `file` from a fresh remote store holding a copy of it, through a
    /// factory with `options`, and returns the values, the GETs the store served and the metrics.
    async fn read_remote(
        source: &InMemory,
        file: &PartitionedFile,
        hint: Option<usize>,
        options: ScanIoOptions,
    ) -> (Vec<i32>, usize, ExecutionPlanMetricsSet) {
        let store = Arc::new(RecordingStore::new());
        let bytes = source
            .get(&file.object_meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        store
            .put(&file.object_meta.location, bytes.into())
            .await
            .unwrap();
        let runtime = datafusion::execution::runtime_env::RuntimeEnv::default();
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = EagerPageIndexReaderFactory::with_options(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            runtime.cache_manager.get_file_metadata_cache(),
            ScanIoSource::ObjectStore,
            &metrics,
            options,
        );
        let reader = factory
            .create_reader(0, file.clone(), hint, &metrics)
            .unwrap();
        let values = read_all(reader).await;
        (values, store.get_calls(), metrics)
    }

    /// A file smaller than the footer read arrives whole with it. Reusing those bytes reads the
    /// file with one GET; requesting its data again takes a second.
    #[tokio::test]
    async fn a_file_inside_the_footer_read_is_read_with_one_get() {
        let source = InMemory::new();
        let file = put_int_file(&source, "small.parquet", None).await;
        let hint = Some(crate::parquet::parquet_exec::METADATA_SIZE_HINT);
        for (reuse, gets) in [(true, 1), (false, 2)] {
            let (values, get_calls, _) =
                read_remote(&source, &file, hint, scan_io_options(reuse, 0, 0)).await;
            assert_eq!(values, vec![1, 2]);
            assert_eq!(get_calls, gets, "reuse_footer_read={reuse}");
        }
    }

    /// DataFusion's opener reads Bloom filters through the reader that loaded the metadata and
    /// decodes through a new one from the factory, which gets the loaded metadata and never
    /// loads its own (`load_bloom_filters` in `datafusion-datasource-parquet`'s opener). The new
    /// reader still serves the data from the first one's footer read, so the file takes one GET.
    #[tokio::test]
    async fn a_reader_made_after_the_metadata_load_reuses_its_footer_read() {
        let store = Arc::new(RecordingStore::new());
        let file = put_int_file(&store.inner, "replaced.parquet", None).await;
        let runtime = datafusion::execution::runtime_env::RuntimeEnv::default();
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = EagerPageIndexReaderFactory::with_options(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            runtime.cache_manager.get_file_metadata_cache(),
            ScanIoSource::ObjectStore,
            &metrics,
            scan_io_options(true, 0, 0),
        );
        let hint = Some(crate::parquet::parquet_exec::METADATA_SIZE_HINT);

        let mut metadata_reader = factory
            .create_reader(0, file.clone(), hint, &metrics)
            .unwrap();
        let metadata = metadata_reader.get_metadata(None).await.unwrap();
        let decoding_reader = factory.create_reader(0, file, hint, &metrics).unwrap();
        let metadata = parquet::arrow::arrow_reader::ArrowReaderMetadata::try_new(
            metadata,
            ArrowReaderOptions::new(),
        )
        .unwrap();
        let stream = parquet::arrow::ParquetRecordBatchStreamBuilder::new_with_metadata(
            ForwardingReader(decoding_reader),
            metadata,
        )
        .build()
        .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        drop(metadata_reader);

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(store.get_calls(), 1);
    }

    /// A native scan with a filter, built from the plan's session options like any Comet scan,
    /// reads a small file from a remote store with one GET when footer reads are reused, and
    /// with two when they are not. The filter makes DataFusion's opener decode through a second
    /// reader (see the test above).
    #[tokio::test]
    async fn a_filtered_scan_reads_a_small_remote_file_with_one_get() {
        use crate::parquet::parquet_exec::{init_datasource_exec, CometParquetScanOptions};
        use crate::parquet::parquet_support::ObjectStoreBackend;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_plan::ExecutionPlan;
        use datafusion::prelude::{SessionConfig, SessionContext};
        use datafusion::scalar::ScalarValue;

        for (reuse, gets) in [(true, 1), (false, 2)] {
            let store = Arc::new(RecordingStore::new());
            let file = put_int_file(&store.inner, "small.parquet", None).await;
            let config = SessionConfig::new().with_extension(Arc::new(CometParquetScanOptions {
                prefetch_next_file_metadata: false,
                scan_io: scan_io_options(reuse, 0, 0),
            }));
            let session_ctx = Arc::new(SessionContext::new_with_config(config));
            let url = ObjectStoreUrl::parse("s3://bucket").unwrap();
            session_ctx
                .runtime_env()
                .register_object_store(url.as_ref(), Arc::clone(&store) as Arc<dyn ObjectStore>);
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "a",
                DataType::Int32,
                false,
            )]));
            let filter: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::GtEq,
                Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
            ));
            let scan = init_datasource_exec(
                Arc::clone(&schema),
                Some(schema),
                None,
                url,
                ObjectStoreBackend::Remote,
                vec![vec![file]],
                None,
                Some(vec![filter]),
                None,
                "UTC",
                true,
                false,
                false,
                false,
                &session_ctx,
                false,
                false,
                false,
                false,
                "",
                "",
            )
            .unwrap();

            let mut stream = scan.execute(0, session_ctx.task_ctx()).unwrap();
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch.unwrap().num_rows();
            }

            assert_eq!(rows, 2);
            assert_eq!(store.get_calls(), gets, "reuse_footer_read={reuse}");
        }
    }

    /// The row groups inside the footer read are served from it and the others are requested.
    /// With a footer read that reaches back to row group 2 of 4, the file takes the footer read
    /// and one GET each for row groups 0 and 1, instead of a GET for every row group.
    #[tokio::test]
    async fn the_footer_read_serves_the_row_groups_it_covers() {
        let source = InMemory::new();
        let file = put_row_group_file(&source, "groups.parquet", 4, 1000).await;
        let third = row_group_range(&source, &file, 2).await;
        let fourth = row_group_range(&source, &file, 3).await;
        let hint = Some((file.object_meta.size - third.start) as usize);
        let expected: Vec<i32> = (0..4000).collect();
        let covered = (third.end - third.start + fourth.end - fourth.start) as usize;
        for (reuse, gets, reused) in [(true, 3, covered), (false, 5, 0)] {
            let (values, get_calls, metrics) =
                read_remote(&source, &file, hint, scan_io_options(reuse, 0, 0)).await;
            assert_eq!(values, expected);
            assert_eq!(get_calls, gets, "reuse_footer_read={reuse}");
            assert_eq!(
                metric(&metrics, "scan_io_footer_read_reused_bytes"),
                reused,
                "reuse_footer_read={reuse}"
            );
        }
    }

    /// A file no bigger than the whole-file threshold is read with one GET, even when the footer
    /// read alone would hold none of its data. A file one byte over the threshold is not.
    #[tokio::test]
    async fn a_file_within_the_whole_file_threshold_is_read_with_one_get() {
        let source = InMemory::new();
        let file = put_row_group_file(&source, "whole.parquet", 4, 1000).await;
        let size = file.object_meta.size;
        let expected: Vec<i32> = (0..4000).collect();

        let (values, gets, _) =
            read_remote(&source, &file, Some(64), scan_io_options(true, size, 0)).await;
        assert_eq!(values, expected);
        assert_eq!(gets, 1);

        let (values, gets, _) =
            read_remote(&source, &file, Some(64), scan_io_options(true, size - 1, 0)).await;
        assert_eq!(values, expected);
        assert!(gets > 1, "a file over the threshold took {gets} GET");
    }

    /// The next file's metadata load hands its footer read to that file's reader, so a small
    /// file opened after a prefetch is read with the prefetch's one GET.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_prefetched_small_file_is_read_with_one_get() {
        let store = Arc::new(RecordingStore::new());
        let files = put_int_files(&store.inner, &["a.parquet", "b.parquet"]).await;
        let runtime = datafusion::execution::runtime_env::RuntimeEnv::default();
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = EagerPageIndexReaderFactory::with_options(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            runtime.cache_manager.get_file_metadata_cache(),
            ScanIoSource::ObjectStore,
            &metrics,
            scan_io_options(true, 0, 0),
        )
        .with_next_file_metadata_prefetch(Some(&files));
        let hint = Some(crate::parquet::parquet_exec::METADATA_SIZE_HINT);

        for file in &files {
            let reader = factory
                .create_reader(0, file.clone(), hint, &metrics)
                .unwrap();
            assert_eq!(read_all(reader).await, vec![1, 2]);
        }

        assert_eq!(metric(&metrics, "scan_io_metadata_prefetches"), 1);
        assert_eq!(store.reads("a.parquet"), 1);
        assert_eq!(store.reads("b.parquet"), 1);
    }

    /// Waits until `count` reads are held at the store's gate at once.
    async fn wait_for_held_reads(store: &RecordingStore, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while store.gate.receiver_count() < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{count} reads were never in flight at once"));
    }

    /// On a remote store, a merged range longer than the split size is fetched as parts that
    /// are all in flight at once, and every requested range comes back whole. 1,000 bytes in
    /// parts of at most 300 take 4 GETs. Ranges 100..200 and 650..990 merge into 890 bytes,
    /// which take 3.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::single_range_in_vec_init)]
    async fn ranges_longer_than_the_split_size_are_fetched_in_parallel_parts() {
        let content = Bytes::from((0..1000u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
        let store = Arc::new(RecordingStore::new().with_gate("split.parquet"));
        store
            .put(&Path::from("split.parquet"), content.clone().into())
            .await
            .unwrap();
        let runtime = datafusion::execution::runtime_env::RuntimeEnv::default();
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = EagerPageIndexReaderFactory::with_options(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            runtime.cache_manager.get_file_metadata_cache(),
            ScanIoSource::ObjectStore,
            &metrics,
            scan_io_options(false, 0, 300),
        );
        let mut reader = factory
            .create_reader(
                0,
                PartitionedFile::new("split.parquet".to_string(), 1000),
                None,
                &metrics,
            )
            .unwrap();

        let read = tokio::spawn(async move {
            let result = reader.get_byte_ranges(vec![0..1000]).await;
            (reader, result)
        });
        wait_for_held_reads(&store, 4).await;
        store.release();
        let (mut reader, result) = read.await.unwrap();
        assert_eq!(result.unwrap(), vec![content.clone()]);
        assert_eq!(store.get_calls(), 4);

        let result = reader
            .get_byte_ranges(vec![100..200, 650..990])
            .await
            .unwrap();
        assert_eq!(
            result,
            vec![content.slice(100..200), content.slice(650..990)]
        );
        assert_eq!(store.get_calls(), 7);
    }

    /// The time a data read spends waiting on storage is recorded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::single_range_in_vec_init)]
    async fn time_waiting_for_a_data_read_is_recorded() {
        let store = Arc::new(RecordingStore::new().with_gate("wait.parquet"));
        store
            .put(
                &Path::from("wait.parquet"),
                Bytes::from_static(b"0123456789").into(),
            )
            .await
            .unwrap();
        let runtime = datafusion::execution::runtime_env::RuntimeEnv::default();
        let metrics = ExecutionPlanMetricsSet::new();
        let factory = EagerPageIndexReaderFactory::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            runtime.cache_manager.get_file_metadata_cache(),
            ScanIoSource::ObjectStore,
            &metrics,
        );
        let mut reader = factory
            .create_reader(
                0,
                PartitionedFile::new("wait.parquet".to_string(), 10),
                None,
                &metrics,
            )
            .unwrap();

        let read = tokio::spawn(async move { reader.get_byte_ranges(vec![0..10]).await.is_ok() });
        store.wait_for_held_read().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        store.release();
        assert!(read.await.unwrap());
        let waited = metric(&metrics, "scan_io_data_wait_time");
        assert!(waited >= 50_000_000, "recorded a wait of {waited} ns");
    }

    /// Every storage read of a metadata load is counted: one when the footer read holds the
    /// footer and page index, and three when it holds only the footer's length, leaving the
    /// footer and then the page index to read.
    #[tokio::test]
    async fn metadata_storage_reads_are_counted() {
        let hint = crate::parquet::parquet_exec::METADATA_SIZE_HINT;
        for (hint, reads) in [(hint, 1), (8, 3)] {
            let store = Arc::new(InMemory::new());
            let file = put_int_file(&store, "meta.parquet", None).await;
            let runtime = datafusion::execution::runtime_env::RuntimeEnv::default();
            let metrics = ExecutionPlanMetricsSet::new();
            let factory = EagerPageIndexReaderFactory::new(
                store,
                runtime.cache_manager.get_file_metadata_cache(),
                ScanIoSource::Local,
                &metrics,
            );
            let mut reader = factory
                .create_reader(0, file, Some(hint), &metrics)
                .unwrap();
            reader.get_metadata(None).await.unwrap();
            assert_eq!(
                metric(&metrics, "scan_io_metadata_storage_reads"),
                reads,
                "hint {hint}"
            );
        }
    }
}
