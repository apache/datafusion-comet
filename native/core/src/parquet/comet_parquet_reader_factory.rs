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

//! Comet's `ParquetFileReaderFactory`. Every native Parquet scan installs it, so it is the one
//! place guaranteed to see each file's footer exactly once per open. It has two jobs, described
//! below: eager page-index loading, and legacy-calendar rejection.
//!
//! # Eager page-index loading
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
//! Filed upstream as apache/datafusion#23978. Revert this behavior once the opener merges its
//! deferred page-index load back into `FileMetadataCache` instead of bypassing it.
//!
//! # Legacy-calendar rejection
//!
//! When `spark.comet.exceptionOnDatetimeRebase` is enabled and the scan reads a date or timestamp
//! column, a file whose footer says its values were written in the legacy hybrid calendar fails
//! the scan rather than returning silently-unrebased values. See [`crate::parquet::
//! legacy_datetime`]. The check lives here, rather than in the schema/expression adapter, because
//! the adapter is only created when the logical and physical schemas differ or a predicate is
//! pushed down, whereas `get_metadata` runs for every file.

use crate::parquet::legacy_datetime::LegacyCalendarGuard;
use bytes::Bytes;
use datafusion::common::Result as DFResult;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::parquet::{
    ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
pub struct CometParquetFileReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
    /// `Some` only when `spark.comet.exceptionOnDatetimeRebase` is on *and* this scan reads a
    /// calendar-sensitive column, so a disarmed guard costs one `Option` check per file.
    legacy_calendar_guard: Option<LegacyCalendarGuard>,
}

impl CometParquetFileReaderFactory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<dyn FileMetadataCache>,
        legacy_calendar_guard: Option<LegacyCalendarGuard>,
    ) -> Self {
        Self {
            store,
            metadata_cache,
            legacy_calendar_guard,
        }
    }
}

impl ParquetFileReaderFactory for CometParquetFileReaderFactory {
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

        Ok(Box::new(CometParquetFileReader {
            file_metrics,
            store: Arc::clone(&self.store),
            inner,
            partitioned_file,
            metadata_cache: Arc::clone(&self.metadata_cache),
            metadata_size_hint,
            legacy_calendar_guard: self.legacy_calendar_guard.clone(),
        }))
    }
}

struct CometParquetFileReader {
    file_metrics: ParquetFileMetrics,
    store: Arc<dyn ObjectStore>,
    inner: ParquetObjectReader,
    partitioned_file: PartitionedFile,
    metadata_cache: Arc<dyn FileMetadataCache>,
    metadata_size_hint: Option<usize>,
    legacy_calendar_guard: Option<LegacyCalendarGuard>,
}

impl AsyncFileReader for CometParquetFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>>
    where
        Self: Send,
    {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.file_metrics.bytes_scanned.add(total as usize);
        self.inner.get_byte_ranges(ranges)
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
        let legacy_calendar_guard = self.legacy_calendar_guard.clone();
        async move {
            let file_decryption_properties = options
                .and_then(|o| o.file_decryption_properties())
                .map(Arc::clone);
            let page_index_policy = if file_decryption_properties.is_none() {
                Some(PageIndexPolicy::Optional)
            } else {
                options.map(|o| o.column_index_policy())
            };

            let metadata = DFParquetMetadata::new(store.as_ref(), &object_meta)
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
                })?;

            if let Some(guard) = &legacy_calendar_guard {
                guard.check(&metadata)?;
            }

            Ok(metadata)
        }
        .boxed()
    }
}

impl Drop for CometParquetFileReader {
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
