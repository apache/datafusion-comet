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
//! page-index load back into `FileMetadataCache` instead of bypassing it.

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
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

/// Whether the reader's storage API represents a local file or a non-local object store.
///
/// The source metrics intentionally describe the API boundary, not network wire bytes. A remote
/// backend can coalesce ranges, retry requests, or serve bytes from a cache below ObjectStore,
/// none of which this reader can observe without backend-specific hooks.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ScanIoSource {
    ObjectStore,
    Local,
}

type ColumnDataRangeCache = HashMap<Path, (Weak<ParquetMetaData>, Arc<[Range<u64>]>)>;

/// Scan I/O metrics at the boundaries this reader can observe without guessing.
///
/// requested is the sum of byte ranges requested by the Parquet reader or metadata loader.
/// returned is the length of successfully returned buffers at the same boundary. Metadata
/// includes footer prefetches, footer decode follow-up reads, page-index ranges, and Bloom
/// filters; DataFusion does not identify those subranges separately, so we report them together
/// instead of claiming unsupported per-subtype precision.
///
/// scan_io_object_store metrics count non-file storage API bytes and scan_io_local metrics count
/// file bytes. Parsed metadata cache entries do not have a meaningful raw-byte size, so cache
/// metrics report successful cache-eligible loads that did or did not require storage I/O rather
/// than inventing cache-byte counts.
#[derive(Debug)]
struct ScanIoMetrics {
    bytes_requested: Count,
    bytes_returned: Count,
    data_bytes_requested: Count,
    data_bytes_returned: Count,
    metadata_bytes_requested: Count,
    metadata_bytes_returned: Count,
    object_store_bytes_requested: Count,
    object_store_bytes_returned: Count,
    local_bytes_requested: Count,
    local_bytes_returned: Count,
    metadata_cache_hits: Count,
    metadata_cache_misses: Count,
    column_data_ranges: Mutex<ColumnDataRangeCache>,
    source: ScanIoSource,
}

impl ScanIoMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, source: ScanIoSource) -> Self {
        Self {
            bytes_requested: byte_counter(metrics, "scan_io_bytes_requested"),
            bytes_returned: byte_counter(metrics, "scan_io_bytes_returned"),
            data_bytes_requested: byte_counter(metrics, "scan_io_data_bytes_requested"),
            data_bytes_returned: byte_counter(metrics, "scan_io_data_bytes_returned"),
            metadata_bytes_requested: byte_counter(metrics, "scan_io_metadata_bytes_requested"),
            metadata_bytes_returned: byte_counter(metrics, "scan_io_metadata_bytes_returned"),
            object_store_bytes_requested: byte_counter(
                metrics,
                "scan_io_object_store_bytes_requested",
            ),
            object_store_bytes_returned: byte_counter(
                metrics,
                "scan_io_object_store_bytes_returned",
            ),
            local_bytes_requested: byte_counter(metrics, "scan_io_local_bytes_requested"),
            local_bytes_returned: byte_counter(metrics, "scan_io_local_bytes_returned"),
            metadata_cache_hits: count_counter(metrics, "scan_io_metadata_cache_hits"),
            metadata_cache_misses: count_counter(metrics, "scan_io_metadata_cache_misses"),
            column_data_ranges: Mutex::new(HashMap::new()),
            source,
        }
    }

    fn column_data_ranges(
        &self,
        location: &Path,
        metadata: &Arc<ParquetMetaData>,
    ) -> Arc<[Range<u64>]> {
        let metadata_identity = Arc::downgrade(metadata);
        let mut cached_ranges = self.column_data_ranges.lock();
        if let Some((cached_metadata, ranges)) = cached_ranges.get(location) {
            if cached_metadata.ptr_eq(&metadata_identity) {
                return Arc::clone(ranges);
            }
        }

        if cached_ranges.len() == cached_ranges.capacity() {
            cached_ranges.retain(|_, (cached_metadata, _)| cached_metadata.strong_count() > 0);
        }

        let ranges = column_data_ranges(metadata);
        cached_ranges.insert(location.clone(), (metadata_identity, Arc::clone(&ranges)));
        ranges
    }

    fn add_reader_requested(&self, bytes: ScanIoBytes) {
        if bytes.data > 0 {
            self.add_data_requested(bytes.data);
        }
        if bytes.metadata > 0 {
            self.add_metadata_requested(bytes.metadata);
        }
    }

    fn add_reader_returned(&self, bytes: ScanIoBytes) {
        if bytes.data > 0 {
            self.add_data_returned(bytes.data);
        }
        if bytes.metadata > 0 {
            self.add_metadata_returned(bytes.metadata);
        }
    }

    fn add_data_requested(&self, bytes: usize) {
        self.data_bytes_requested.add(bytes);
        self.add_requested(bytes);
    }

    fn add_data_returned(&self, bytes: usize) {
        self.data_bytes_returned.add(bytes);
        self.add_returned(bytes);
    }

    fn add_metadata_requested(&self, bytes: usize) {
        self.metadata_bytes_requested.add(bytes);
        self.add_requested(bytes);
    }

    fn add_metadata_returned(&self, bytes: usize) {
        self.metadata_bytes_returned.add(bytes);
        self.add_returned(bytes);
    }

    fn record_metadata_cache_result(&self, storage_reads: usize) {
        if storage_reads == 0 {
            self.metadata_cache_hits.add(1);
        } else {
            self.metadata_cache_misses.add(1);
        }
    }

    fn add_requested(&self, bytes: usize) {
        self.bytes_requested.add(bytes);
        match self.source {
            ScanIoSource::ObjectStore => self.object_store_bytes_requested.add(bytes),
            ScanIoSource::Local => self.local_bytes_requested.add(bytes),
        }
    }

    fn add_returned(&self, bytes: usize) {
        self.bytes_returned.add(bytes);
        match self.source {
            ScanIoSource::ObjectStore => self.object_store_bytes_returned.add(bytes),
            ScanIoSource::Local => self.local_bytes_returned.add(bytes),
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ScanIoBytes {
    data: usize,
    metadata: usize,
}

impl ScanIoBytes {
    fn add(&mut self, other: Self) {
        self.data += other.data;
        self.metadata += other.metadata;
    }
}

fn range_bytes(range: &Range<u64>) -> usize {
    (range.end - range.start) as usize
}

fn ranges_bytes(ranges: &[Range<u64>]) -> usize {
    ranges.iter().map(range_bytes).sum()
}

fn column_data_ranges(metadata: &ParquetMetaData) -> Arc<[Range<u64>]> {
    let mut ranges = metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .filter_map(|column| {
            let start = u64::try_from(
                column
                    .dictionary_page_offset()
                    .unwrap_or_else(|| column.data_page_offset()),
            )
            .ok()?;
            let length = u64::try_from(column.compressed_size()).ok()?;
            let end = start.checked_add(length)?;
            (start < end).then_some(start..end)
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable_by_key(|range| range.start);

    let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = merged.last_mut() {
            if range.start <= previous.end {
                previous.end = previous.end.max(range.end);
                continue;
            }
        }
        merged.push(range);
    }
    Arc::from(merged)
}

fn classify_reader_range(range: &Range<u64>, data_ranges: Option<&[Range<u64>]>) -> ScanIoBytes {
    let requested = range_bytes(range);
    let Some(data_ranges) = data_ranges else {
        return ScanIoBytes {
            data: requested,
            metadata: 0,
        };
    };

    let first = data_ranges.partition_point(|data_range| data_range.end <= range.start);
    let mut data = 0;
    for data_range in &data_ranges[first..] {
        if data_range.start >= range.end {
            break;
        }
        let start = range.start.max(data_range.start);
        let end = range.end.min(data_range.end);
        if start < end {
            data += range_bytes(&(start..end));
        }
    }

    ScanIoBytes {
        data,
        metadata: requested - data,
    }
}

fn classify_returned_range(
    requested: &Range<u64>,
    returned: usize,
    data_ranges: Option<&[Range<u64>]>,
) -> ScanIoBytes {
    let end = requested.start.saturating_add(returned as u64);
    classify_reader_range(&(requested.start..end), data_ranges)
}

#[derive(Debug)]
pub struct EagerPageIndexReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
    scan_io_metrics: Arc<ScanIoMetrics>,
}

impl EagerPageIndexReaderFactory {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<dyn FileMetadataCache>,
        source: ScanIoSource,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            store,
            metadata_cache,
            scan_io_metrics: Arc::new(ScanIoMetrics::new(metrics, source)),
        }
    }
}

impl ParquetFileReaderFactory for EagerPageIndexReaderFactory {
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
        let mut inner = ParquetObjectReader::new(
            Arc::clone(&self.store),
            partitioned_file.object_meta.location.clone(),
        )
        .with_file_size(partitioned_file.object_meta.size);
        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint);
        }

        Ok(Box::new(EagerPageIndexReader {
            file_metrics,
            scan_io_metrics: Arc::clone(&self.scan_io_metrics),
            store: Arc::clone(&self.store),
            inner,
            partitioned_file,
            metadata_cache: Arc::clone(&self.metadata_cache),
            metadata_size_hint,
            data_ranges: None,
        }))
    }
}

struct EagerPageIndexReader {
    file_metrics: ParquetFileMetrics,
    scan_io_metrics: Arc<ScanIoMetrics>,
    store: Arc<dyn ObjectStore>,
    inner: ParquetObjectReader,
    partitioned_file: PartitionedFile,
    metadata_cache: Arc<dyn FileMetadataCache>,
    metadata_size_hint: Option<usize>,
    data_ranges: Option<Arc<[Range<u64>]>>,
}

impl AsyncFileReader for EagerPageIndexReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let requested = range_bytes(&range);
        self.file_metrics.bytes_scanned.add(requested);
        self.scan_io_metrics
            .add_reader_requested(classify_reader_range(&range, self.data_ranges.as_deref()));
        let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
        let data_ranges = self.data_ranges.clone();
        let requested_range = range.clone();
        let future = self.inner.get_bytes(range);
        async move {
            let bytes = future.await?;
            scan_io_metrics.add_reader_returned(classify_returned_range(
                &requested_range,
                bytes.len(),
                data_ranges.as_deref(),
            ));
            Ok(bytes)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        let requested = ranges_bytes(&ranges);
        self.file_metrics.bytes_scanned.add(requested);
        let requested_bytes = ranges
            .iter()
            .fold(ScanIoBytes::default(), |mut total, range| {
                total.add(classify_reader_range(range, self.data_ranges.as_deref()));
                total
            });
        self.scan_io_metrics.add_reader_requested(requested_bytes);
        let returned_ranges = (requested_bytes.metadata > 0).then(|| ranges.clone());
        let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
        let data_ranges = self.data_ranges.clone();
        let future = self.inner.get_byte_ranges(ranges);
        async move {
            let bytes = future.await?;
            let returned = if let Some(ranges) = returned_ranges {
                ranges.iter().zip(&bytes).fold(
                    ScanIoBytes::default(),
                    |mut total, (range, bytes)| {
                        total.add(classify_returned_range(
                            range,
                            bytes.len(),
                            data_ranges.as_deref(),
                        ));
                        total
                    },
                )
            } else {
                ScanIoBytes {
                    data: bytes.iter().map(Bytes::len).sum(),
                    metadata: 0,
                }
            };
            scan_io_metrics.add_reader_returned(returned);
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
        let data_ranges = &mut self.data_ranges;
        async move {
            let file_decryption_properties = options
                .and_then(|o| o.file_decryption_properties())
                .map(Arc::clone);
            let cache_enabled = file_decryption_properties.is_none();
            let page_index_policy = if file_decryption_properties.is_none() {
                Some(PageIndexPolicy::Optional)
            } else {
                options.map(|o| o.column_index_policy())
            };
            let metadata_storage_reads = Arc::new(AtomicUsize::new(0));
            let metadata_store = MetadataIoObjectStore {
                inner: store,
                scan_io_metrics: Arc::clone(&scan_io_metrics),
                storage_reads: Arc::clone(&metadata_storage_reads),
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

            if let Ok(metadata) = &metadata {
                *data_ranges =
                    Some(scan_io_metrics.column_data_ranges(&object_meta.location, metadata));
                if cache_enabled {
                    scan_io_metrics.record_metadata_cache_result(
                        metadata_storage_reads.load(Ordering::Relaxed),
                    );
                }
            }

            metadata
        }
        .boxed()
    }
}

/// Counts metadata storage calls while forwarding every operation to the configured store.
///
/// Data reads are already visible at the AsyncFileReader data methods. Metadata reads bypass
/// those methods inside DFParquetMetadata, so only metadata uses this wrapper.
#[derive(Debug)]
struct MetadataIoObjectStore {
    inner: Arc<dyn ObjectStore>,
    scan_io_metrics: Arc<ScanIoMetrics>,
    storage_reads: Arc<AtomicUsize>,
}

impl MetadataIoObjectStore {
    fn record_request(&self, bytes: usize) {
        if bytes > 0 {
            self.storage_reads.fetch_add(1, Ordering::Relaxed);
            self.scan_io_metrics.add_metadata_requested(bytes);
        }
    }
}

impl Display for MetadataIoObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "metadata-io({})", self.inner)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for MetadataIoObjectStore {
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
            self.scan_io_metrics.add_metadata_returned(bytes.len());
            GetResultPayload::Stream(futures::stream::once(async move { Ok(bytes) }).boxed())
        } else {
            let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
            GetResultPayload::Stream(
                result
                    .into_stream()
                    .inspect_ok(move |bytes| scan_io_metrics.add_metadata_returned(bytes.len()))
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
        self.record_request(ranges_bytes(ranges));
        let bytes = self.inner.get_ranges(location, ranges).await?;
        self.scan_io_metrics
            .add_metadata_returned(bytes.iter().map(Bytes::len).sum());
        Ok(bytes)
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
    use parquet::file::metadata::FileMetaData;
    use parquet::schema::types::{SchemaDescriptor, Type};

    #[test]
    fn shares_column_data_ranges_by_metadata_identity() {
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("schema").build().unwrap(),
        )));
        let metadata = Arc::new(ParquetMetaData::new(
            FileMetaData::new(1, 0, None, None, schema, None),
            vec![],
        ));
        let metrics = ScanIoMetrics::new(&ExecutionPlanMetricsSet::new(), ScanIoSource::Local);
        let location = Path::from("test.parquet");

        let first = metrics.column_data_ranges(&location, &metadata);
        let second = metrics.column_data_ranges(&location, &metadata);
        assert!(Arc::ptr_eq(&first, &second));

        let replacement = Arc::new(metadata.as_ref().clone());
        let refreshed = metrics.column_data_ranges(&location, &replacement);
        assert!(!Arc::ptr_eq(&first, &refreshed));
        assert!(Arc::ptr_eq(
            &refreshed,
            &metrics.column_data_ranges(&location, &replacement)
        ));
    }
}
