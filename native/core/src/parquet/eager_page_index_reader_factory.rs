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
    coalesce_ranges, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result as ObjectStoreResult,
    OBJECT_STORE_COALESCE_DEFAULT,
};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::{FooterTail, PageIndexPolicy, ParquetMetaData};
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

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
}

impl AsyncFileReader for EagerPageIndexReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let requested = range_bytes(&range);
        self.file_metrics.bytes_scanned.add(requested);
        let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
        let future = self.inner.get_bytes(range);
        async move {
            let bytes = future.await?;
            scan_io_metrics.metadata_bytes.add(bytes.len());
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
        let scan_io_metrics = Arc::clone(&self.scan_io_metrics);
        let future = self.inner.get_byte_ranges(ranges);
        async move {
            let bytes = future.await?;
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
            let footer_payload_bytes = Arc::new(AtomicUsize::new(0));
            let metadata_store = ScanIoObjectStore {
                inner: store,
                scan_io_metrics: Arc::clone(&scan_io_metrics),
                role: ScanIoStoreRole::Metadata {
                    storage_reads: Arc::clone(&metadata_storage_reads),
                    footer_payload_bytes: Arc::clone(&footer_payload_bytes),
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
                    scan_io_metrics.footer_reads.add(1);
                    scan_io_metrics.footer_bytes.add(footer_bytes);
                }
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

#[derive(Debug)]
enum ScanIoStoreRole {
    ObjectStore,
    Metadata {
        storage_reads: Arc<AtomicUsize>,
        footer_payload_bytes: Arc<AtomicUsize>,
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

    fn record_returned(&self, range: Option<&Range<u64>>, bytes: &Bytes) {
        match &self.role {
            ScanIoStoreRole::ObjectStore => self
                .scan_io_metrics
                .object_store_response_bytes_read
                .add(bytes.len()),
            ScanIoStoreRole::Metadata {
                footer_payload_bytes,
                footer_recorded,
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

                if *record_footer_immediately {
                    let footer_bytes = footer_payload_bytes.load(Ordering::Relaxed);
                    let footer_end = file_size.saturating_sub(8);
                    if footer_bytes > 0
                        && footer_end
                            .checked_sub(footer_bytes as u64)
                            .is_some_and(|footer_start| {
                                range.is_some_and(|range| {
                                    range.start <= footer_start
                                        && range.start.saturating_add(bytes.len() as u64)
                                            >= footer_end
                                })
                            })
                        && !footer_recorded.swap(true, Ordering::Relaxed)
                    {
                        self.scan_io_metrics.footer_reads.add(1);
                        self.scan_io_metrics.footer_bytes.add(footer_bytes);
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
                coalesce_ranges(
                    ranges,
                    |range| self.get_range(location, range),
                    OBJECT_STORE_COALESCE_DEFAULT,
                )
                .await
            }
            ScanIoStoreRole::Metadata { .. } => {
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
