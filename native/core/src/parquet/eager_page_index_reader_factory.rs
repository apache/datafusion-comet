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
//! This factory forces `PageIndexPolicy::Optional` on every metadata fetch, ignoring whatever
//! policy the caller requests, so the page index is always present in the cached
//! `ParquetMetaData` after the first load. DataFusion's opener checks whether the metadata it
//! already has includes the page index before issuing its own fetch, so with this factory that
//! check is always true and the uncached fetch never happens. The tradeoff: files where the
//! opener's skip heuristic would have avoided the page index load entirely now load it anyway.
//!
//! Filed upstream as apache/datafusion#23978. Revert this once the opener merges its deferred
//! page-index load back into `FileMetadataCache` instead of bypassing it.

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
pub struct EagerPageIndexReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<FileMetadataCache>,
}

impl EagerPageIndexReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>, metadata_cache: Arc<FileMetadataCache>) -> Self {
        Self {
            store,
            metadata_cache,
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
    store: Arc<dyn ObjectStore>,
    inner: ParquetObjectReader,
    partitioned_file: PartitionedFile,
    metadata_cache: Arc<FileMetadataCache>,
    metadata_size_hint: Option<usize>,
}

impl AsyncFileReader for EagerPageIndexReader {
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
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        // Ignore the caller-requested policy: always fetch and cache the page index eagerly so
        // the opener never has a reason to load it again, uncached, later in the same open.
        let object_meta = self.partitioned_file.object_meta.clone();
        let metadata_cache = Arc::clone(&self.metadata_cache);
        let store = Arc::clone(&self.store);
        let metadata_size_hint = self.metadata_size_hint;
        async move {
            DFParquetMetadata::new(store.as_ref(), &object_meta)
                .with_file_metadata_cache(Some(metadata_cache))
                .with_metadata_size_hint(metadata_size_hint)
                .with_page_index_policy(Some(PageIndexPolicy::Optional))
                .fetch_metadata()
                .await
                .map_err(|e| {
                    parquet::errors::ParquetError::General(format!(
                        "Failed to fetch metadata for file {}: {e}",
                        object_meta.location,
                    ))
                })
        }
        .boxed()
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
