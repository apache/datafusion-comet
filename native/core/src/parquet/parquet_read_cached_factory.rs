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

//! A `ParquetFileReaderFactory` that caches parquet footer metadata across
//! partitions within a single scan. When multiple Spark partitions read from
//! the same parquet file (different row group ranges), this avoids redundant
//! footer reads and parsing.
//!
//! The cache is scoped to the factory instance (one per scan), not global,
//! so it does not persist across queries.
//!
//! Uses `tokio::sync::OnceCell` per file path so that concurrent partitions
//! wait for the first reader to load the footer rather than all racing.

use bytes::Bytes;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::file::metadata::ParquetMetaData;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

type MetadataCell = Arc<OnceCell<Arc<ParquetMetaData>>>;

/// A `ParquetFileReaderFactory` that caches footer metadata by file path.
/// The cache is scoped to this factory instance (shared across partitions
/// within a single scan via Arc), not global.
#[derive(Debug)]
pub struct CachingParquetReaderFactory {
    store: Arc<dyn ObjectStore>,
    cache: Arc<Mutex<HashMap<Path, MetadataCell>>>,
}

impl CachingParquetReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl ParquetFileReaderFactory for CachingParquetReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DataFusionResult<Box<dyn AsyncFileReader + Send>> {
        let bytes_scanned = MetricBuilder::new(metrics).counter("bytes_scanned", partition_index);

        let location = partitioned_file.object_meta.location.clone();

        // Get or create the OnceCell for this file path
        let cell = Arc::<OnceCell<Arc<ParquetMetaData>>>::clone(
            self.cache
                .lock()
                .unwrap()
                .entry(location.clone())
                .or_insert_with(|| Arc::new(OnceCell::new())),
        );

        let mut inner = ParquetObjectReader::new(Arc::clone(&self.store), location.clone())
            .with_file_size(partitioned_file.object_meta.size);

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint);
        }

        Ok(Box::new(CachingParquetFileReader {
            inner,
            location,
            cell,
            bytes_scanned,
        }))
    }
}

struct CachingParquetFileReader {
    inner: ParquetObjectReader,
    location: Path,
    cell: MetadataCell,
    bytes_scanned: datafusion::physical_plan::metrics::Count,
}

impl AsyncFileReader for CachingParquetFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.bytes_scanned.add((range.end - range.start) as usize);
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
        self.bytes_scanned.add(total as usize);
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let cell = Arc::clone(&self.cell);
        let location = self.location.clone();

        async move {
            let metadata = cell
                .get_or_try_init(|| async {
                    log::trace!("CachingParquetFileReader: loading footer for {}", location);
                    self.inner.get_metadata(options).await
                })
                .await?;

            log::trace!("CachingParquetFileReader: cache HIT for {}", self.location);
            Ok(Arc::clone(metadata))
        }
        .boxed()
    }
}
