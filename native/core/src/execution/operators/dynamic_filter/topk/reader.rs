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

//! Keep omitted null counts unknown when a TopK threshold is attached to Parquet.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::common::config::ConfigOptions;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::{FileSource, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;

use super::super::parquet_reader::{
    is_direct_column_null_checks, try_attach_parquet_reader_filter,
};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::BoxFuture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::ParquetMetaData;

pub(super) fn try_attach_topk_reader_filter(
    input: &Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(scan) = input.downcast_ref::<DataSourceExec>() else {
        return Ok(None);
    };
    let Some((_, source)) = scan.downcast_to_file_source::<ParquetSource>() else {
        return Ok(None);
    };
    // Clearing incomplete statistics must not weaken an existing static range
    // predicate: it could expose a conversion error in a formerly pruned group.
    // Direct null checks are safe because unknown and assumed-zero null counts
    // both retain a nonempty group. Known all-null statistics remain intact.
    if source
        .filter()
        .is_some_and(|filter| !is_direct_column_null_checks(&filter))
    {
        return Ok(None);
    }
    let Some(filtered) = try_attach_parquet_reader_filter(input, predicate, config)? else {
        return Ok(None);
    };
    let Some(filtered) = filtered.downcast_ref::<DataSourceExec>() else {
        return Ok(None);
    };
    let Some((file_config, source)) = filtered.downcast_to_file_source::<ParquetSource>() else {
        return Ok(None);
    };
    let Some(factory) = source.parquet_file_reader_factory() else {
        return Ok(None);
    };
    let mut file_config = file_config.clone();
    file_config.file_source = Arc::new(source.clone().with_parquet_file_reader_factory(Arc::new(
        UnknownNullCountReaderFactory::new(Arc::clone(factory)),
    )));
    Ok(Some(Arc::new(
        filtered.clone().with_data_source(Arc::new(file_config)),
    )))
}

#[derive(Debug)]
struct UnknownNullCountReaderFactory {
    inner: Arc<dyn ParquetFileReaderFactory>,
}

impl UnknownNullCountReaderFactory {
    fn new(inner: Arc<dyn ParquetFileReaderFactory>) -> Self {
        Self { inner }
    }
}

impl ParquetFileReaderFactory for UnknownNullCountReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        Ok(Box::new(UnknownNullCountReader {
            inner: self
                .inner
                .create_reader(partition_index, file, metadata_size_hint, metrics)?,
        }))
    }
}

struct UnknownNullCountReader {
    inner: Box<dyn AsyncFileReader + Send>,
}

impl AsyncFileReader for UnknownNullCountReader {
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
        Box::pin(
            async move { preserve_unknown_null_counts(self.inner.get_metadata(options).await?) },
        )
    }
}

fn preserve_unknown_null_counts(
    metadata: Arc<ParquetMetaData>,
) -> ParquetResult<Arc<ParquetMetaData>> {
    let missing_count = |column: &parquet::file::metadata::ColumnChunkMetaData| {
        column
            .statistics()
            .is_some_and(|stats| stats.null_count_opt().is_none())
    };
    if !metadata
        .row_groups()
        .iter()
        .any(|group| group.columns().iter().any(missing_count))
    {
        return Ok(metadata);
    }
    // DataFusion 55.1 interprets an omitted null_count as zero when other column
    // statistics exist (apache/datafusion#25239). Drop those incomplete statistics
    // so both initial and live pruning receive unknowns. Known counts retain pruning.
    // Clone before editing: the underlying reader may share its original cached footer.
    let mut builder = metadata.as_ref().clone().into_builder();
    let groups = builder
        .take_row_groups()
        .into_iter()
        .map(|group| {
            let columns = group
                .columns()
                .iter()
                .map(|column| {
                    if missing_count(column) {
                        column.clone().into_builder().clear_statistics().build()
                    } else {
                        Ok(column.clone())
                    }
                })
                .collect::<ParquetResult<Vec<_>>>()?;
            group.into_builder().set_column_metadata(columns).build()
        })
        .collect::<ParquetResult<Vec<_>>>()?;
    Ok(Arc::new(builder.set_row_groups(groups).build()))
}

#[cfg(test)]
mod tests;
