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

//! Preserve the file identity across Parquet's asynchronous planning and decoding.
//!
//! The schema adapter receives no file identity. Capture it when planning a file and retain it
//! through that file's pending planners and streams. Delegate source operations to preserve
//! Parquet's projection, filter, and sort pushdown.

use arrow::array::RecordBatch;
use datafusion::common::{
    config::ConfigOptions, tree_node::TreeNodeRecursion, DataFusionError, Result,
};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::physical_expr::{
    projection::ProjectionExprs, EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::{
    filter_pushdown::FilterPushdownPropagation, metrics::ExecutionPlanMetricsSet,
    DisplayFormatType, SortOrderPushdownResult,
};
use datafusion_comet_common::SparkError;
use datafusion_datasource::{
    file_stream::FileOpener,
    morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer},
    PartitionedFile, TableSchema,
};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use std::{fmt, sync::Arc};

/// The original URL-encoded Spark path, before object-store aliases and paths are normalized.
#[derive(Debug, Clone)]
pub(crate) struct SparkFilePath(pub Arc<str>);

/// Delegate scan planning and pushdown to ParquetSource, wrapping only its per-file work.
pub(crate) struct ParquetErrorContext(Arc<dyn FileSource>);

impl ParquetErrorContext {
    pub(crate) fn wrap(source: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        Arc::new(Self(source))
    }
}

impl FileSource for ParquetErrorContext {
    fn create_file_opener(
        &self,
        store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        self.0.create_file_opener(store, config, partition)
    }

    fn create_morselizer(
        &self,
        store: Arc<dyn ObjectStore>,
        config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        Ok(Box::new(FileContextMorselizer(
            self.0.create_morselizer(store, config, partition)?,
        )))
    }

    fn table_schema(&self) -> &TableSchema {
        self.0.table_schema()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Self::wrap(self.0.with_batch_size(batch_size))
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.0.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.0.projection()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.0.metrics()
    }

    fn file_type(&self) -> &str {
        self.0.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt_extra(t, f)
    }

    fn supports_repartitioning(&self) -> bool {
        self.0.supports_repartitioning()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        min_size: usize,
        ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        self.0
            .repartitioned(target_partitions, min_size, ordering, config)
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let mut result = self.0.try_pushdown_filters(filters, config)?;
        result.updated_node = result.updated_node.map(Self::wrap);
        Ok(result)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        Ok(self.0.try_pushdown_sort(order, properties)?.map(Self::wrap))
    }

    fn reorder_files(&self, files: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
        self.0.reorder_files(files)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        Ok(self.0.try_pushdown_projection(projection)?.map(Self::wrap))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.0.apply_expressions(f)
    }
}

#[derive(Debug)]
struct FileContextMorselizer(Box<dyn Morselizer>);

impl Morselizer for FileContextMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        let path = file
            .extensions
            .get::<SparkFilePath>()
            .map(|path| Arc::clone(&path.0))
            .unwrap_or_else(|| Arc::from(file.object_meta.location.as_ref()));
        Ok(Box::new(FileContext {
            inner: self.0.plan_file(file)?,
            path,
        }))
    }
}

#[derive(Debug)]
struct FileContext<T> {
    inner: T,
    path: Arc<str>,
}

impl MorselPlanner for FileContext<Box<dyn MorselPlanner>> {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let Self { inner, path } = *self;
        let Some(mut plan) = inner.plan()? else {
            return Ok(None);
        };
        let morsels = plan
            .take_morsels()
            .into_iter()
            .map(|inner| {
                Box::new(FileContext {
                    inner,
                    path: Arc::clone(&path),
                }) as Box<dyn Morsel>
            })
            .collect();
        let planners = plan
            .take_ready_planners()
            .into_iter()
            .map(|inner| {
                Box::new(FileContext {
                    inner,
                    path: Arc::clone(&path),
                }) as Box<dyn MorselPlanner>
            })
            .collect();
        if let Some(pending) = plan.take_pending_planner() {
            plan.set_pending_planner(async move {
                Ok(Box::new(FileContext {
                    inner: pending.await?,
                    path,
                }) as Box<dyn MorselPlanner>)
            });
        }
        Ok(Some(plan.with_morsels(morsels).with_planners(planners)))
    }
}

impl Morsel for FileContext<Box<dyn Morsel>> {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        let Self { inner, path } = *self;
        inner
            .into_stream()
            .map_err(move |error| {
                // Conversion runs inside this file's stream, before batches from other files mix.
                if let DataFusionError::External(source) = &error {
                    if let Some(SparkError::ParquetTimestampOverflow { .. }) =
                        source.downcast_ref::<SparkError>()
                    {
                        return SparkError::ParquetTimestampOverflow {
                            file_path: path.to_string(),
                        }
                        .into();
                    }
                }
                error
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct FailedMorsel(SparkError);

    impl Morsel for FailedMorsel {
        fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
            futures::stream::once(async move { Err(self.0.into()) }).boxed()
        }
    }

    #[tokio::test]
    async fn file_paths_stay_with_their_streams() {
        let stream = |path: &'static str, error| {
            Box::new(FileContext {
                inner: Box::new(FailedMorsel(error)) as Box<dyn Morsel>,
                path: Arc::from(path),
            })
            .into_stream()
        };
        let mut first = stream(
            "file:///first%20file.parquet",
            SparkError::ParquetTimestampOverflow {
                file_path: String::new(),
            },
        );
        let mut second = stream(
            "s3a://bucket/second.parquet",
            SparkError::ParquetTimestampOverflow {
                file_path: String::new(),
            },
        );
        // Consume in the opposite order from creation; no shared current-file state is involved.
        for (result, path) in [
            (second.next().await, "s3a://bucket/second.parquet"),
            (first.next().await, "file:///first%20file.parquet"),
        ] {
            let DataFusionError::External(error) = result.unwrap().unwrap_err() else {
                panic!("expected a structured Spark error");
            };
            assert!(matches!(error.downcast_ref::<SparkError>(),
                Some(SparkError::ParquetTimestampOverflow { file_path }) if file_path == path));
        }
        let error = stream("file:///first%20file.parquet", SparkError::LongOverflow)
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, DataFusionError::External(source)
            if matches!(source.downcast_ref::<SparkError>(), Some(SparkError::LongOverflow))));
    }
}
