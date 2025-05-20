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

//! Custom schema adapter that uses Spark-compatible conversions

use crate::parquet::parquet_support::{spark_parquet_convert, SparkParquetOptions};
use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

/// An implementation of DataFusion's `SchemaAdapterFactory` that uses a Spark-compatible
/// `cast` implementation.
#[derive(Clone, Debug)]
pub struct SparkSchemaAdapterFactory {
    /// Spark cast options
    parquet_options: SparkParquetOptions,
    default_values: Option<HashMap<usize, ScalarValue>>,
}

impl SparkSchemaAdapterFactory {
    pub fn new(
        options: SparkParquetOptions,
        default_values: Option<HashMap<usize, ScalarValue>>,
    ) -> Self {
        Self {
            parquet_options: options,
            default_values,
        }
    }
}

impl SchemaAdapterFactory for SparkSchemaAdapterFactory {
    /// Create a new factory for mapping batches from a file schema to a table
    /// schema.
    ///
    /// This is a convenience for [`DefaultSchemaAdapterFactory::create`] with
    /// the same schema for both the projected table schema and the table
    /// schema.
    fn create(
        &self,
        required_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(SparkSchemaAdapter {
            required_schema,
            parquet_options: self.parquet_options.clone(),
            default_values: self.default_values.clone(),
        })
    }
}

/// This SchemaAdapter requires both the table schema and the projected table
/// schema. See  [`SchemaMapping`] for more details
#[derive(Clone, Debug)]
pub struct SparkSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by the
    /// associated ParquetExec
    required_schema: SchemaRef,
    /// Spark cast options
    parquet_options: SparkParquetOptions,
    default_values: Option<HashMap<usize, ScalarValue>>,
}

impl SchemaAdapter for SparkSchemaAdapter {
    /// Map a column index in the table schema to a column index in a particular
    /// file schema
    ///
    /// Panics if index is not in range for the table schema
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.required_schema.field(index);
        Some(
            file_schema
                .fields
                .iter()
                .enumerate()
                .find(|(_, b)| {
                    if self.parquet_options.case_sensitive {
                        b.name() == field.name()
                    } else {
                        b.name().to_lowercase() == field.name().to_lowercase()
                    }
                })?
                .0,
        )
    }

    /// Creates a `SchemaMapping` for casting or mapping the columns from the
    /// file schema to the table schema.
    ///
    /// If the provided `file_schema` contains columns of a different type to
    /// the expected `table_schema`, the method will attempt to cast the array
    /// data from the file schema to the table schema where possible.
    ///
    /// Returns a [`SchemaMapping`] that can be applied to the output batch
    /// along with an ordered list of columns to project from the file
    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion::common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let mut field_mappings = vec![None; self.required_schema.fields().len()];

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some((table_idx, _table_field)) = self
                .required_schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, b)| {
                    if self.parquet_options.case_sensitive {
                        b.name() == file_field.name()
                    } else {
                        b.name().to_lowercase() == file_field.name().to_lowercase()
                    }
                })
            {
                field_mappings[table_idx] = Some(projection.len());
                projection.push(file_idx);
            }
        }

        Ok((
            Arc::new(SchemaMapping {
                required_schema: Arc::<Schema>::clone(&self.required_schema),
                field_mappings,
                parquet_options: self.parquet_options.clone(),
                default_values: self.default_values.clone(),
            }),
            projection,
        ))
    }
}

// TODO SchemaMapping is mostly copied from DataFusion but calls spark_cast
// instead of arrow cast - can we reduce the amount of code copied here and make
// the DataFusion version more extensible?

/// The SchemaMapping struct holds a mapping from the file schema to the table
/// schema and any necessary type conversions.
///
/// Note, because `map_batch` and `map_partial_batch` functions have different
/// needs, this struct holds two schemas:
///
/// 1. The projected **table** schema
/// 2. The full table schema
///
/// [`map_batch`] is used by the ParquetOpener to produce a RecordBatch which
/// has the projected schema, since that's the schema which is supposed to come
/// out of the execution of this query. Thus `map_batch` uses
/// `projected_table_schema` as it can only operate on the projected fields.
///
/// [`map_batch`]: Self::map_batch
#[derive(Debug)]
pub struct SchemaMapping {
    /// The schema of the table. This is the expected schema after conversion
    /// and it should match the schema of the query result.
    required_schema: SchemaRef,
    /// Mapping from field index in `projected_table_schema` to index in
    /// projected file_schema.
    ///
    /// They are Options instead of just plain `usize`s because the table could
    /// have fields that don't exist in the file.
    field_mappings: Vec<Option<usize>>,
    /// Spark cast options
    parquet_options: SparkParquetOptions,
    default_values: Option<HashMap<usize, ScalarValue>>,
}

impl SchemaMapper for SchemaMapping {
    /// Adapts a `RecordBatch` to match the `projected_table_schema` using the stored mapping and
    /// conversions. The produced RecordBatch has a schema that contains only the projected
    /// columns, so if one needs a RecordBatch with a schema that references columns which are not
    /// in the projected, it would be better to use `map_partial_batch`
    fn map_batch(&self, batch: RecordBatch) -> datafusion::common::Result<RecordBatch> {
        let batch_rows = batch.num_rows();
        let batch_cols = batch.columns().to_vec();

        let cols = self
            .required_schema
            // go through each field in the projected schema
            .fields()
            .iter()
            .enumerate()
            // and zip it with the index that maps fields from the projected table schema to the
            // projected file schema in `batch`
            .zip(&self.field_mappings)
            // and for each one...
            .map(|((field_idx, field), file_idx)| {
                file_idx.map_or_else(
                    // If this field only exists in the table, and not in the file, then we need to
                    // populate a default value for it.
                    || {
                        if self.default_values.is_some() {
                            // We have a map of default values, see if this field is in there.
                            if let Some(value) =
                                self.default_values.as_ref().unwrap().get(&field_idx)
                            // Default value exists, construct a column from it.
                            {
                                let cv = if field.data_type() == &value.data_type() {
                                    ColumnarValue::Scalar(value.clone())
                                } else {
                                    // Data types don't match. This can happen when default values
                                    // are stored by Spark in a format different than the column's
                                    // type (e.g., INT32 when the column is DATE32)
                                    spark_parquet_convert(
                                        ColumnarValue::Scalar(value.clone()),
                                        field.data_type(),
                                        &self.parquet_options,
                                    )?
                                };
                                return cv.into_array(batch_rows);
                            }
                        }
                        // Construct an entire column of nulls. We use the Scalar representation
                        // for better performance.
                        let cv =
                            ColumnarValue::Scalar(ScalarValue::try_new_null(field.data_type())?);
                        cv.into_array(batch_rows)
                    },
                    // However, if it does exist in both, then try to cast it to the correct output
                    // type
                    |batch_idx| {
                        spark_parquet_convert(
                            ColumnarValue::Array(Arc::clone(&batch_cols[batch_idx])),
                            field.data_type(),
                            &self.parquet_options,
                        )?
                        .into_array(batch_rows)
                    },
                )
            })
            .collect::<datafusion::common::Result<Vec<_>, _>>()?;

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        let schema = Arc::<Schema>::clone(&self.required_schema);
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }
}

#[cfg(test)]
mod test {
    use crate::parquet::parquet_support::SparkParquetOptions;
    use crate::parquet::schema_adapter::SparkSchemaAdapterFactory;
    use arrow::array::UInt32Array;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::config::TableParquetOptions;
    use datafusion::common::DataFusionError;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_comet_spark_expr::test_common::file_util::get_temp_filename;
    use datafusion_comet_spark_expr::EvalMode;
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;

    #[tokio::test]
    async fn parquet_roundtrip_int_as_string() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let names = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]))
            as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![ids, names])?;

        let required_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let _ = roundtrip(&batch, required_schema).await?;

        Ok(())
    }

    #[tokio::test]
    async fn parquet_roundtrip_unsigned_int() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));

        let ids = Arc::new(UInt32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![ids])?;

        let required_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let _ = roundtrip(&batch, required_schema).await?;

        Ok(())
    }

    /// Create a Parquet file containing a single batch and then read the batch back using
    /// the specified required_schema. This will cause the SchemaAdapter code to be used.
    async fn roundtrip(
        batch: &RecordBatch,
        required_schema: SchemaRef,
    ) -> Result<RecordBatch, DataFusionError> {
        let filename = get_temp_filename();
        let filename = filename.as_path().as_os_str().to_str().unwrap().to_string();
        let file = File::create(&filename)?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&batch.schema()), None)?;
        writer.write(batch)?;
        writer.close()?;

        let object_store_url = ObjectStoreUrl::local_filesystem();

        let mut spark_parquet_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        spark_parquet_options.allow_cast_unsigned_ints = true;

        let parquet_source = Arc::new(
            ParquetSource::new(TableParquetOptions::new()).with_schema_adapter_factory(Arc::new(
                SparkSchemaAdapterFactory::new(spark_parquet_options, None),
            )),
        );

        let files = FileGroup::new(vec![PartitionedFile::from_path(filename.to_string())?]);
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, required_schema, parquet_source)
                .with_file_groups(vec![files])
                .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));

        let mut stream = parquet_exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        stream.next().await.unwrap()
    }
}
