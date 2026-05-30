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

//! Tolerant FileSource / FileOpener decorators for honouring Spark's
//! `spark.sql.files.ignoreMissingFiles`.
//!
//! DataFusion's `DataSourceExec` constructs its `FileStream` with the default
//! `OnError::Fail` and provides no public knob to flip it to `OnError::Skip`,
//! so Spark's "silently skip files that disappeared between planning and
//! execution" semantics cannot be opted into via a config. Instead we wrap the
//! inner `FileOpener`: when the opener's future resolves to a NotFound error
//! we return an empty `BoxStream` so the file contributes zero batches and the
//! `FileStream` simply moves to the next file.
//!
//! All other `FileSource` trait methods delegate to the inner source verbatim
//! so optimizer hooks (`try_pushdown_filters`, `repartitioned`, ...) keep
//! their normal behaviour.

use arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion::physical_plan::DisplayFormatType;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::table_schema::TableSchema;
use futures::{stream, FutureExt, StreamExt};
use object_store::ObjectStore;
use std::any::Any;
use std::fmt::{self, Formatter};
use std::sync::Arc;

/// FileOpener decorator that converts NotFound errors from the wrapped
/// opener's future into an empty `BoxStream`. Any other error is propagated
/// unchanged so we don't paper over real corruption / IO problems.
pub(crate) struct IgnoreMissingFileOpener {
    inner: Arc<dyn FileOpener>,
}

impl IgnoreMissingFileOpener {
    pub(crate) fn new(inner: Arc<dyn FileOpener>) -> Self {
        Self { inner }
    }
}

fn is_not_found(err: &datafusion::error::DataFusionError) -> bool {
    use datafusion::error::DataFusionError;
    // object_store wraps NotFound into ObjectStore; parquet's reader may surface it via External.
    // We inspect both the variant and its display chain for the literal "not found" /
    // "NotFound" token because the precise DataFusion wrapping differs by error path.
    let mut current: &(dyn std::error::Error + 'static) = err;
    loop {
        if let Some(os_err) = current.downcast_ref::<object_store::Error>() {
            if matches!(os_err, object_store::Error::NotFound { .. }) {
                return true;
            }
        }
        if let Some(io_err) = current.downcast_ref::<std::io::Error>() {
            if io_err.kind() == std::io::ErrorKind::NotFound {
                return true;
            }
        }
        match current.source() {
            Some(src) => current = src,
            None => break,
        }
    }
    // Display-based fallback for adapters that erase the underlying type
    // (e.g. parquet's ParquetError -> DataFusionError::External). Anchored to
    // recognised NotFound phrasings only -- a loose substring match on "not found"
    // would silently swallow unrelated parquet messages like "row group statistics
    // not found" or "page index not found" and produce wrong empty results.
    let msg = err.to_string();
    matches!(
        err,
        DataFusionError::External(_) | DataFusionError::ObjectStore(_)
    ) && (msg.contains("Object at location")
        || msg.contains("Generic NotFound")
        || msg.contains("NoSuchKey")
        || msg.contains("NoSuchFile")
        || msg.contains("No such file or directory"))
}

impl FileOpener for IgnoreMissingFileOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let inner_future = self.inner.open(partitioned_file)?;
        Ok(Box::pin(inner_future.map(|opened| match opened {
            Ok(stream) => Ok(stream),
            Err(e) if is_not_found(&e) => {
                let empty = stream::empty::<Result<RecordBatch>>();
                Ok(empty.boxed())
            }
            Err(e) => Err(e),
        })))
    }
}

/// FileSource decorator that wraps the inner source's `FileOpener` in
/// `IgnoreMissingFileOpener`. All other methods delegate verbatim.
#[derive(Clone)]
pub struct IgnoreMissingFileSource {
    inner: Arc<dyn FileSource>,
}

impl IgnoreMissingFileSource {
    pub fn new(inner: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        Arc::new(Self { inner })
    }
}

impl FileSource for IgnoreMissingFileSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let inner_opener = self
            .inner
            .create_file_opener(object_store, base_config, partition)?;
        Ok(Arc::new(IgnoreMissingFileOpener::new(inner_opener)))
    }

    fn as_any(&self) -> &dyn Any {
        // Delegate to the inner source so DataFusion optimizations that downcast a
        // `FileSource` to its concrete type (e.g. `ParquetSource`, to read/apply
        // parquet-specific config) see through this decorator instead of failing the
        // downcast and silently skipping the optimization. Nothing downcasts to
        // `IgnoreMissingFileSource` itself, and all source operations still flow through
        // this wrapper's trait methods (which delegate + re-wrap), so the
        // ignore-missing behavior is preserved.
        self.inner.as_any()
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        // Re-wrap so the batch-sized clone still skips missing files.
        IgnoreMissingFileSource::new(self.inner.with_batch_size(batch_size))
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.inner.projection()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        self.inner.fmt_extra(t, f)
    }

    fn supports_repartitioning(&self) -> bool {
        self.inner.supports_repartitioning()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        self.inner.repartitioned(
            target_partitions,
            repartition_file_min_size,
            output_ordering,
            config,
        )
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &datafusion::config::ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let prop = self.inner.try_pushdown_filters(filters, config)?;
        // Re-wrap the updated_node so the post-pushdown FileSource keeps the
        // missing-file tolerance.
        Ok(FilterPushdownPropagation {
            filters: prop.filters,
            updated_node: prop.updated_node.map(IgnoreMissingFileSource::new),
        })
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        match self.inner.try_pushdown_sort(order, eq_properties)? {
            SortOrderPushdownResult::Exact { inner } => Ok(SortOrderPushdownResult::Exact {
                inner: IgnoreMissingFileSource::new(inner),
            }),
            SortOrderPushdownResult::Inexact { inner } => Ok(SortOrderPushdownResult::Inexact {
                inner: IgnoreMissingFileSource::new(inner),
            }),
            SortOrderPushdownResult::Unsupported => Ok(SortOrderPushdownResult::Unsupported),
        }
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        Ok(self
            .inner
            .try_pushdown_projection(projection)?
            .map(IgnoreMissingFileSource::new))
    }
}
