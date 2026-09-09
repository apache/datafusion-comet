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
//!
//! The metadata fetch is also the one per-file hook DataFusion runs unconditionally, so the
//! factory validates requested Parquet field ids there; see [`FieldIdCheck`].

use crate::parquet::parquet_support::{
    schema_holds_field_ids, validate_field_mapping, SparkParquetOptions,
};
use arrow::datatypes::SchemaRef;
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
use futures::{FutureExt, TryFutureExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

#[derive(Debug)]
pub struct EagerPageIndexReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<FileMetadataCache>,
    field_id_check: Option<Arc<FieldIdCheck>>,
}

impl EagerPageIndexReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>, metadata_cache: Arc<FileMetadataCache>) -> Self {
        Self {
            store,
            metadata_cache,
            field_id_check: None,
        }
    }

    /// Validate the ids `requested_schema` carries against each file's schema as its footer
    /// loads. Installs nothing when field id matching is off or the schema carries no id, so
    /// ordinary reads pay nothing.
    pub fn with_field_id_check(
        mut self,
        requested_schema: SchemaRef,
        parquet_options: &SparkParquetOptions,
    ) -> Self {
        if parquet_options.use_field_id && schema_holds_field_ids(&requested_schema) {
            self.field_id_check = Some(Arc::new(FieldIdCheck {
                requested_schema,
                parquet_options: parquet_options.clone(),
                validated: Mutex::new(HashMap::new()),
            }));
        }
        self
    }
}

/// Validates requested field ids for files the expression adapter never sees: DataFusion's
/// opener creates the adapter only when a predicate is pushed or the file schema differs from
/// the requested one, so a metadata-free file whose schema equals it is read positionally
/// (comet#5801). Resolves the mapping the adapter resolves, so both raise the same error.
#[derive(Debug)]
struct FieldIdCheck {
    requested_schema: SchemaRef,
    parquet_options: SparkParquetOptions,
    /// Files already validated, keyed by path to the metadata they were checked against, so a
    /// footer served from `FileMetadataCache` is not rechecked on every open.
    validated: Mutex<HashMap<Path, Weak<ParquetMetaData>>>,
}

impl FieldIdCheck {
    fn validate(
        &self,
        location: &Path,
        metadata: &Arc<ParquetMetaData>,
    ) -> parquet::errors::Result<()> {
        if self.is_validated(location, metadata) {
            return Ok(());
        }
        // The same conversion the opener applies, so field ids land in field metadata under
        // `PARQUET:field_id` and the mapping resolves against the schema the adapter would see.
        let file_metadata = metadata.file_metadata();
        let file_schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )?;
        validate_field_mapping(&file_schema, &self.requested_schema, &self.parquet_options)
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        self.lock()
            .insert(location.clone(), Arc::downgrade(metadata));
        Ok(())
    }

    fn is_validated(&self, location: &Path, metadata: &Arc<ParquetMetaData>) -> bool {
        self.lock()
            .get(location)
            .is_some_and(|seen| std::ptr::eq(Weak::as_ptr(seen), Arc::as_ptr(metadata)))
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<Path, Weak<ParquetMetaData>>> {
        self.validated
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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

        Ok(Box::new(EagerPageIndexReader {
            file_metrics,
            store: Arc::clone(&self.store),
            partitioned_file,
            metadata_cache: Arc::clone(&self.metadata_cache),
            metadata_size_hint,
            field_id_check: self.field_id_check.clone(),
        }))
    }
}

/// Reads bytes straight off the `ObjectStore`, the same way DataFusion's own `ParquetFileReader`
/// does, and overrides only the metadata fetch. `ParquetFileReader::new` is crate-private, so the
/// byte-range plumbing is duplicated here rather than delegated.
struct EagerPageIndexReader {
    file_metrics: ParquetFileMetrics,
    store: Arc<dyn ObjectStore>,
    partitioned_file: PartitionedFile,
    metadata_cache: Arc<FileMetadataCache>,
    metadata_size_hint: Option<usize>,
    field_id_check: Option<Arc<FieldIdCheck>>,
}

impl AsyncFileReader for EagerPageIndexReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let bytes_scanned = range.end - range.start;
        self.file_metrics.bytes_scanned.add(bytes_scanned as usize);
        self.store
            .get_range(&self.partitioned_file.object_meta.location, range)
            .map_err(|e| ParquetError::External(Box::new(e)))
            .boxed()
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
        async move {
            self.store
                .get_ranges(&self.partitioned_file.object_meta.location, &ranges)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
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
        let field_id_check = self.field_id_check.clone();
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
            if let Some(check) = &field_id_check {
                check.validate(&object_meta.location, &metadata)?;
            }
            Ok(metadata)
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
