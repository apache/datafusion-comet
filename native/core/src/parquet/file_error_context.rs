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

//! Preserve per-file reader settings and error context through Parquet planning and decoding.
//!
//! The schema adapter receives no file identity. Capture it when planning a file and retain it
//! through that file's pending planners and streams. Delegate source operations to preserve
//! Parquet's projection, filter, and sort pushdown.

use super::eager_page_index_reader_factory::ReplanFooter;
use arrow::array::RecordBatch;
use datafusion::common::{
    config::ConfigOptions, tree_node::TreeNodeRecursion, DataFusionError, Result,
};
use datafusion::datasource::physical_plan::parquet::{
    ParquetAccessPlan, RowGroupAccess, RowGroupAccessPlanFilter,
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
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use std::{fmt, sync::Arc};

/// The original URL-encoded Spark path, before object-store aliases and paths are normalized.
#[derive(Debug, Clone)]
pub(crate) struct SparkFilePath(pub Arc<str>);

/// Spark's read boundary, attached to every file of a scan that might need it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SparkReaderBatchSize {
    pub spark: usize,
    pub comet: usize,
}

impl SparkReaderBatchSize {
    fn decoder_size(self) -> usize {
        // Sub-batches must stay inside both Spark's error boundary and Comet's
        // original window, whose distinct values fit the Arrow dictionary key.
        num::integer::gcd(self.spark, self.comet)
    }
}

/// Delegate scan planning and pushdown to ParquetSource, wrapping only its per-file work.
pub(crate) struct ParquetErrorContext(Arc<dyn FileSource>);

impl ParquetErrorContext {
    pub(crate) fn wrap(source: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        Arc::new(Self(source))
    }

    /// The wrapped source, for a caller that rebuilds it and then wraps the result again.
    pub(crate) fn inner(&self) -> &Arc<dyn FileSource> {
        &self.0
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
        // The reader batch size is a scan-wide setting. DataFusion fixes it
        // here, before the footer reveals any dictionary timestamp columns.
        let spark_morselizer = config
            .file_groups
            .iter()
            .find_map(|group| group.files().first())
            .and_then(|file| file.extensions.get::<SparkReaderBatchSize>())
            .map(|size| {
                self.0
                    .with_batch_size(size.decoder_size())
                    .create_morselizer(Arc::clone(&store), config, partition)
                    .map(|reader| (Arc::from(reader), *size))
            })
            .transpose()?;
        Ok(Box::new(FileContextMorselizer {
            inner: self.0.create_morselizer(store, config, partition)?,
            spark_morselizer,
        }))
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
struct FileContextMorselizer {
    inner: Box<dyn Morselizer>,
    spark_morselizer: Option<(Arc<dyn Morselizer>, SparkReaderBatchSize)>,
}

#[derive(Debug)]
struct SparkBatchReplan {
    morselizer: Arc<dyn Morselizer>,
    file: PartitionedFile,
    footer: Arc<ReplanFooter>,
    size: SparkReaderBatchSize,
}

#[derive(Debug)]
struct SparkWindows {
    size: usize,
    row_groups: Vec<usize>,
}

impl SparkBatchReplan {
    fn windows(&self) -> Option<Arc<SparkWindows>> {
        if self.size.decoder_size() == self.size.spark {
            return None;
        }
        let groups = self.footer.0.get()?.row_groups();
        let mut filter = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(groups.len()));
        if let Some(range) = &self.file.range {
            filter.prune_by_range(groups, range);
        }
        let access = self.file.extensions.get::<ParquetAccessPlan>();
        let row_groups = filter
            .row_group_indexes()
            .map(|i| match access.and_then(|plan| plan.inner().get(i)) {
                Some(RowGroupAccess::Skip) => 0,
                Some(RowGroupAccess::Selection(selection)) => selection.row_count(),
                _ => groups[i].num_rows() as usize,
            })
            .collect();
        Some(Arc::new(SparkWindows {
            size: self.size.spark,
            row_groups,
        }))
    }
}

impl SparkWindows {
    fn buffer(
        &self,
        input: BoxStream<'static, Result<RecordBatch>>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        // The unfiltered reader starts a fresh batch in each row group. Validate
        // the whole Spark window before exposing its first sub-batch to LIMIT.
        let size = self.size;
        let windows = self.row_groups.clone().into_iter().flat_map(move |rows| {
            (0..rows)
                .step_by(size)
                .map(move |start| size.min(rows - start))
        });
        stream::try_unfold(
            (input.fuse(), windows),
            |(mut input, mut windows)| async move {
                let mut batches = Vec::new();
                let mut remaining = windows.next().unwrap_or(1);
                while remaining > 0 {
                    let Some(batch) = input.try_next().await? else {
                        break;
                    };
                    remaining = remaining.saturating_sub(batch.num_rows());
                    batches.push(Ok::<_, DataFusionError>(batch));
                }
                Ok::<_, DataFusionError>(
                    (!batches.is_empty()).then_some((stream::iter(batches), (input, windows))),
                )
            },
        )
        .try_flatten()
        .boxed()
    }
}

impl Morselizer for FileContextMorselizer {
    fn plan_file(&self, mut file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        let path = file
            .extensions
            .get::<SparkFilePath>()
            .map(|path| Arc::clone(&path.0))
            .unwrap_or_else(|| Arc::from(file.object_meta.location.as_ref()));
        let replan = self.spark_morselizer.as_ref().map(|(morselizer, size)| {
            let footer = Arc::new(ReplanFooter::default());
            file.extensions.insert_arc(Arc::clone(&footer));
            Arc::new(SparkBatchReplan {
                morselizer: Arc::clone(morselizer),
                file: file.clone(),
                footer,
                size: *size,
            })
        });
        Ok(Box::new(FilePlanner {
            inner: self.inner.plan_file(file)?,
            path,
            replan,
            windows: None,
        }))
    }
}

#[derive(Debug)]
struct FilePlanner {
    inner: Box<dyn MorselPlanner>,
    path: Arc<str>,
    replan: Option<Arc<SparkBatchReplan>>,
    windows: Option<Arc<SparkWindows>>,
}

impl MorselPlanner for FilePlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let Self {
            mut inner,
            path,
            mut replan,
            mut windows,
        } = *self;
        // The first planner after the metadata load can switch readers
        // before decoding rows. The replacement uses the marked footer.
        if let Some(next) = replan.as_ref().filter(|next| next.footer.0.get().is_some()) {
            inner = next.morselizer.plan_file(next.file.clone())?;
            windows = next.windows();
            replan = None;
        }
        let Some(mut plan) = inner.plan()? else {
            return Ok(None);
        };
        let morsels = plan
            .take_morsels()
            .into_iter()
            .map(|inner| {
                Box::new(FileMorsel {
                    inner,
                    path: Arc::clone(&path),
                    windows: windows.clone(),
                }) as Box<dyn Morsel>
            })
            .collect();
        let planners = plan
            .take_ready_planners()
            .into_iter()
            .map(|inner| {
                Box::new(Self {
                    inner,
                    path: Arc::clone(&path),
                    windows: windows.clone(),
                    replan: replan.clone(),
                }) as Box<dyn MorselPlanner>
            })
            .collect();
        if let Some(pending) = plan.take_pending_planner() {
            plan.set_pending_planner(async move {
                Ok(Box::new(Self {
                    inner: pending.await?,
                    path,
                    replan,
                    windows,
                }) as Box<dyn MorselPlanner>)
            });
        }
        Ok(Some(plan.with_morsels(morsels).with_planners(planners)))
    }
}

#[derive(Debug)]
struct FileMorsel {
    windows: Option<Arc<SparkWindows>>,
    inner: Box<dyn Morsel>,
    path: Arc<str>,
}

impl Morsel for FileMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        let Self {
            inner,
            path,
            windows,
        } = *self;
        let mut input = inner.into_stream();
        if let Some(windows) = windows {
            input = windows.buffer(input);
        }
        input
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
            Box::new(FileMorsel {
                inner: Box::new(FailedMorsel(error)) as Box<dyn Morsel>,
                path: Arc::from(path),
                windows: None,
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
        let error = stream("file:///first%20file.parquet", SparkError::DivideByZero)
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, DataFusionError::External(source)
            if matches!(source.downcast_ref::<SparkError>(), Some(SparkError::DivideByZero))));
    }
}
