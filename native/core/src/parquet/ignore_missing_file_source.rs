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

//! `FileSource` wrapper that mirrors Spark's `spark.sql.files.ignoreMissingFiles`
//! semantics: when the underlying `FileOpener` fails to open a file because it
//! does not exist, swallow the error and produce an empty stream for that file.
//!
//! The wrapping is purely lazy: no extra IO is performed up-front. The
//! `head` / `get_range` request that DataFusion's `ParquetSource` was going
//! to issue anyway is the one that returns NotFound, and we just translate
//! that single error into "no rows from this file" instead of letting it
//! abort the whole stage.

use std::any::Any;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::error::Result;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr_common::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion_datasource::TableSchema;
use futures::stream;
use object_store::ObjectStore;

/// Wraps any `Arc<dyn FileSource>` so that per-file FileNotFound errors
/// produce an empty record-batch stream instead of aborting the scan.
pub struct IgnoreMissingFileSource {
    inner: Arc<dyn FileSource>,
}

impl fmt::Debug for IgnoreMissingFileSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "IgnoreMissingFileSource({})", self.inner.file_type())
    }
}

impl IgnoreMissingFileSource {
    pub fn new(inner: Arc<dyn FileSource>) -> Self {
        Self { inner }
    }
}

impl FileSource for IgnoreMissingFileSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let inner = self.inner.create_file_opener(object_store, base_config, partition)?;
        Ok(Arc::new(IgnoreMissingFileOpener { inner }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner: self.inner.with_batch_size(batch_size),
        })
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

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        // Delegate so ParquetSource (or whatever inner is) does the actual
        // projection rewrite, then re-wrap the rewritten inner so we keep
        // ignore-missing-files semantics on the new node.
        match self.inner.try_pushdown_projection(projection)? {
            Some(new_inner) => Ok(Some(Arc::new(IgnoreMissingFileSource::new(new_inner)))),
            None => Ok(None),
        }
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        // Delegate to the inner so its filter pushdown works normally. The
        // returned `updated_node`, if any, is the rewritten inner FileSource;
        // re-wrap it to preserve our error-handling layer.
        let prop = self.inner.try_pushdown_filters(filters, config)?;
        if let Some(new_inner) = prop.updated_node.as_ref() {
            let rewrapped: Arc<dyn FileSource> =
                Arc::new(IgnoreMissingFileSource::new(Arc::clone(new_inner)));
            Ok(FilterPushdownPropagation {
                filters: prop.filters,
                updated_node: Some(rewrapped),
            })
        } else {
            Ok(prop)
        }
    }
}

struct IgnoreMissingFileOpener {
    inner: Arc<dyn FileOpener>,
}

impl FileOpener for IgnoreMissingFileOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let inner_future = self.inner.open(partitioned_file)?;
        Ok(Box::pin(async move {
            match inner_future.await {
                Ok(stream) => Ok(stream),
                Err(e) if is_not_found(&e) => {
                    let empty: futures::stream::BoxStream<
                        'static,
                        std::result::Result<arrow::array::RecordBatch, DataFusionError>,
                    > = Box::pin(stream::empty());
                    Ok(empty)
                }
                Err(e) => Err(e),
            }
        }))
    }
}

/// Best-effort detection of "the file does not exist" across the layers
/// DataFusion threads errors through. Object-store reports it as
/// `Error::NotFound`; the `io::Error::NotFound` kind appears for local-file
/// paths; either may be wrapped in `DataFusionError::ObjectStore` /
/// `DataFusionError::IoError` / `DataFusionError::External`.
fn is_not_found(e: &DataFusionError) -> bool {
    match e {
        DataFusionError::ObjectStore(os_err) => {
            matches!(&**os_err, object_store::Error::NotFound { .. })
        }
        DataFusionError::IoError(io_err) => {
            io_err.kind() == std::io::ErrorKind::NotFound
        }
        DataFusionError::External(boxed) => {
            // Object store errors get boxed in some code paths.
            if let Some(os_err) = boxed.downcast_ref::<object_store::Error>() {
                matches!(os_err, object_store::Error::NotFound { .. })
            } else if let Some(io_err) = boxed.downcast_ref::<std::io::Error>() {
                io_err.kind() == std::io::ErrorKind::NotFound
            } else {
                // Last-resort string match for opaque wrappers (parquet's
                // ParquetError, for instance, can carry a NotFound body
                // without preserving the typed source).
                let msg = boxed.to_string();
                msg.contains("NotFound") || msg.contains("not found")
            }
        }
        _ => {
            let msg = e.to_string();
            // Match common error message shapes; this is the fallback when
            // the typed error chain has been flattened to a string.
            msg.contains("Object at location") && msg.contains("not found")
        }
    }
}
