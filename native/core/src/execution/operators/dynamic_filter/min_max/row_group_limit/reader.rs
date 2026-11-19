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

//! Hand preflight readers to one execution's scan without rereading their footers.

use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::ParquetMetaData;

/// Identify an occurrence in this execution's file group, including repeated
/// paths/ranges. DataFusion's Bloom-filter reader replacement keeps the same ID.
pub(super) struct PreflightFileIndex(pub(super) usize);

/// Own readers only until this execution opens their files. The permanent scan
/// keeps the original factory, and dropping the runtime stream releases any
/// readers that were not consumed.
pub(super) struct PreloadedReaderFactory {
    inner: Arc<dyn ParquetFileReaderFactory>,
    readers: Mutex<Vec<Option<PreloadedReader>>>,
}

impl PreloadedReaderFactory {
    pub(super) fn new(inner: Arc<dyn ParquetFileReaderFactory>) -> Self {
        Self {
            inner,
            readers: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn push(
        &mut self,
        inner: Box<dyn AsyncFileReader + Send>,
        metadata: Arc<ParquetMetaData>,
    ) {
        // Preflight visits a contiguous prefix of the file group.
        self.readers
            .get_mut()
            .unwrap()
            .push(Some(PreloadedReader { inner, metadata }));
    }
}

impl Debug for PreloadedReaderFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreloadedReaderFactory")
            .finish_non_exhaustive()
    }
}

impl ParquetFileReaderFactory for PreloadedReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        if let Some(index) = file.extensions.get::<PreflightFileIndex>() {
            if let Some(reader) = self
                .readers
                .lock()
                .unwrap()
                .get_mut(index.0)
                .and_then(Option::take)
            {
                return Ok(Box::new(reader));
            }
        }
        // The scan may replace a reader after Bloom-filter pruning, or reach a
        // file that preflight did not inspect after deciding to fall back. A
        // replacement must not consume another occurrence of the same path/range.
        self.inner
            .create_reader(partition_index, file, metadata_size_hint, metrics)
    }
}

struct PreloadedReader {
    inner: Box<dyn AsyncFileReader + Send>,
    metadata: Arc<ParquetMetaData>,
}

impl AsyncFileReader for PreloadedReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        // DataFusion initially requests PageIndexPolicy::Skip. Its later page
        // index loading starts from this metadata rather than rereading it.
        // Preflight has no decryption properties. Let the original reader apply
        // them if the actual scan requests an encrypted open.
        if options.is_some_and(|options| options.file_decryption_properties().is_some()) {
            return self.inner.get_metadata(options);
        }
        let metadata = Arc::clone(&self.metadata);
        Box::pin(async move { Ok(metadata) })
    }
}
