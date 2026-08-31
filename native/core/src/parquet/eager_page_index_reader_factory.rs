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
//! For unencrypted scans that project Variant, this factory also replaces the advisory
//! `ARROW:schema` footer entry with physical Parquet schema inference. The only added hint maps
//! Parquet ENUM leaves to Arrow Utf8, matching Spark's physical-schema interpretation:
//! https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L585-L599

use arrow::datatypes::{DataType, FieldRef, Schema};
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
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::arrow::{
    arrow_reader::ArrowReaderOptions, encode_arrow_schema, parquet_to_arrow_schema,
    ARROW_SCHEMA_META_KEY,
};
use parquet::basic::{ConvertedType, LogicalType};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{
    FileMetaData, KeyValue, PageIndexPolicy, ParquetMetaData, ParquetMetaDataBuilder,
};
use parquet::schema::types::{ColumnDescPtr, SchemaDescriptor};
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
pub struct EagerPageIndexReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
    skip_arrow_schema: bool,
}

impl EagerPageIndexReaderFactory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<dyn FileMetadataCache>,
        skip_arrow_schema: bool,
    ) -> Self {
        Self {
            store,
            metadata_cache,
            skip_arrow_schema,
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
            skip_arrow_schema: self.skip_arrow_schema,
        }))
    }
}

struct EagerPageIndexReader {
    file_metrics: ParquetFileMetrics,
    store: Arc<dyn ObjectStore>,
    inner: ParquetObjectReader,
    partitioned_file: PartitionedFile,
    metadata_cache: Arc<dyn FileMetadataCache>,
    metadata_size_hint: Option<usize>,
    skip_arrow_schema: bool,
}

fn is_enum_column(column: &ColumnDescPtr) -> bool {
    matches!(column.logical_type_ref(), Some(LogicalType::Enum))
        || column.converted_type() == ConvertedType::ENUM
}

fn spark_enum_field(
    field: &FieldRef,
    columns: &[ColumnDescPtr],
    column_index: &mut usize,
) -> ParquetResult<FieldRef> {
    let rewrite =
        |field: &FieldRef, data_type| Arc::new(field.as_ref().clone().with_data_type(data_type));
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| spark_enum_field(field, columns, column_index))
                .collect::<ParquetResult<Vec<_>>>()?
                .into(),
        ),
        DataType::List(child) => DataType::List(spark_enum_field(child, columns, column_index)?),
        DataType::LargeList(child) => {
            DataType::LargeList(spark_enum_field(child, columns, column_index)?)
        }
        DataType::FixedSizeList(child, size) => {
            DataType::FixedSizeList(spark_enum_field(child, columns, column_index)?, *size)
        }
        DataType::ListView(child) => {
            DataType::ListView(spark_enum_field(child, columns, column_index)?)
        }
        DataType::LargeListView(child) => {
            DataType::LargeListView(spark_enum_field(child, columns, column_index)?)
        }
        DataType::Map(child, sorted) => {
            DataType::Map(spark_enum_field(child, columns, column_index)?, *sorted)
        }
        _ => {
            let column = columns.get(*column_index).ok_or_else(|| {
                ParquetError::General(
                    "Arrow schema contains more leaves than the Parquet schema".to_string(),
                )
            })?;
            *column_index += 1;
            if is_enum_column(column) {
                DataType::Utf8
            } else {
                return Ok(Arc::clone(field));
            }
        }
    };
    Ok(rewrite(field, data_type))
}

/// Arrow maps Parquet ENUM to Binary, while Spark maps it to String. Build the smallest physical
/// schema hint needed to preserve Spark's interpretation without restoring the file's advisory
/// `ARROW:schema` types such as Date64 or Decimal256.
/// https://github.com/apache/arrow-rs/blob/58.4.0/parquet/src/arrow/schema/primitive.rs#L285-L293
/// https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetSchemaConverter.scala#L357-L363
fn spark_enum_schema(schema: &SchemaDescriptor) -> ParquetResult<Option<Schema>> {
    let columns = schema.columns();
    if !columns.iter().any(is_enum_column) {
        return Ok(None);
    }

    let arrow_schema = parquet_to_arrow_schema(schema, None)?;
    let mut column_index = 0;
    let fields = arrow_schema
        .fields()
        .iter()
        .map(|field| spark_enum_field(field, columns, &mut column_index))
        .collect::<ParquetResult<Vec<_>>>()?;
    if column_index != columns.len() {
        return Err(ParquetError::General(
            "Parquet schema contains more leaves than the Arrow schema".to_string(),
        ));
    }
    Ok(Some(Schema::new_with_metadata(
        fields,
        arrow_schema.metadata().clone(),
    )))
}

/// Ignore the file's Arrow schema hint so ambiguous leaves such as Date64 retain Spark semantics.
/// Add back only a physical-schema-derived hint for Parquet ENUM, which Spark reads as String.
fn with_spark_arrow_schema(metadata: Arc<ParquetMetaData>) -> ParquetResult<Arc<ParquetMetaData>> {
    let file = metadata.file_metadata();
    let has_arrow_schema = file.key_value_metadata().is_some_and(|key_values| {
        key_values
            .iter()
            .any(|key_value| key_value.key == ARROW_SCHEMA_META_KEY)
    });
    let enum_schema = spark_enum_schema(file.schema_descr())?;
    if !has_arrow_schema && enum_schema.is_none() {
        return Ok(metadata);
    }

    let mut key_values = file
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter(|key_value| key_value.key != ARROW_SCHEMA_META_KEY)
        .cloned()
        .collect::<Vec<_>>();
    if let Some(schema) = enum_schema {
        key_values.push(KeyValue {
            key: ARROW_SCHEMA_META_KEY.to_string(),
            value: Some(encode_arrow_schema(&schema)),
        });
    }

    let file = FileMetaData::new(
        file.version(),
        file.num_rows(),
        file.created_by().map(str::to_owned),
        Some(key_values),
        file.schema_descr_ptr(),
        file.column_orders().cloned(),
    );
    Ok(Arc::new(
        ParquetMetaDataBuilder::new(file)
            .set_row_groups(metadata.row_groups().to_vec())
            .set_column_index(metadata.column_index().cloned())
            .set_offset_index(metadata.offset_index().cloned())
            .build(),
    ))
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
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        // Forward decryption properties like `CachedParquetFileReader` does. Only override the
        // policy for non-encrypted opens; see module docs for why.
        let object_meta = self.partitioned_file.object_meta.clone();
        let metadata_cache = Arc::clone(&self.metadata_cache);
        let store = Arc::clone(&self.store);
        let metadata_size_hint = self.metadata_size_hint;
        let skip_arrow_schema = self.skip_arrow_schema;
        async move {
            let file_decryption_properties = options
                .and_then(|o| o.file_decryption_properties())
                .map(Arc::clone);
            let encrypted = file_decryption_properties.is_some();
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
            Ok(if skip_arrow_schema && !encrypted {
                with_spark_arrow_schema(metadata)?
            } else {
                metadata
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
