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
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::ColumnStatistics;
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

        if self.parquet_options.schema_validation_enabled {
            // Case-insensitive duplicate field detection
            if !self.parquet_options.case_sensitive {
                for required_field in self.required_schema.fields().iter() {
                    let required_name_lower = required_field.name().to_lowercase();
                    let matching_names: Vec<&str> = file_schema
                        .fields
                        .iter()
                        .filter(|f| f.name().to_lowercase() == required_name_lower)
                        .map(|f| f.name().as_str())
                        .collect();
                    if matching_names.len() > 1 {
                        return Err(datafusion::error::DataFusionError::External(
                            format!(
                                "Found duplicate field(s) \"{}\": [{}] in case-insensitive mode",
                                required_field.name(),
                                matching_names.join(", ")
                            )
                            .into(),
                        ));
                    }
                }
            }

            // Type coercion validation
            for (table_idx, file_idx_opt) in field_mappings.iter().enumerate() {
                if let Some(proj_idx) = file_idx_opt {
                    let file_field_idx = projection[*proj_idx];
                    let file_type = file_schema.field(file_field_idx).data_type();
                    let required_type = self.required_schema.field(table_idx).data_type();
                    if file_type != required_type
                        && !is_spark_compatible_parquet_coercion(
                            file_type,
                            required_type,
                            self.parquet_options.schema_evolution_enabled,
                        )
                    {
                        let col_name = self.required_schema.field(table_idx).name();
                        let required_spark_name = arrow_type_to_spark_name(required_type);
                        let file_spark_name = arrow_type_to_parquet_physical_name(file_type);

                        // Special error for reading TimestampLTZ as TimestampNTZ
                        // to match Spark's error message format
                        if matches!(
                            (file_type, required_type),
                            (
                                DataType::Timestamp(_, Some(_)),
                                DataType::Timestamp(_, None)
                            )
                        ) {
                            return Err(datafusion::error::DataFusionError::External(
                                format!(
                                    "Unable to create Parquet converter for data type \"{}\"",
                                    required_spark_name
                                )
                                .into(),
                            ));
                        }

                        return Err(datafusion::error::DataFusionError::External(
                            format!(
                                "Parquet column cannot be converted in file. \
                                 Column: [{}], Expected: {}, Found: {}",
                                col_name, required_spark_name, file_spark_name
                            )
                            .into(),
                        ));
                    }
                }
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
                        if let Some(default_values) = &self.default_values {
                            // We have a map of default values, see if this field is in there.
                            if let Some(value) = default_values.get(&field_idx)
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

    fn map_column_statistics(
        &self,
        _file_col_statistics: &[ColumnStatistics],
    ) -> datafusion::common::Result<Vec<ColumnStatistics>> {
        Ok(vec![])
    }
}

/// Check if a type coercion from a Parquet file type to a required Spark type is allowed.
/// Returns true if the coercion is compatible, false if Spark's vectorized reader would reject it.
///
/// This function rejects specific type conversions that Spark's vectorized reader would reject.
/// Conversions not explicitly rejected are allowed (Comet's parquet_convert_array or arrow-rs
/// cast handles them).
///
/// When `schema_evolution_enabled` is true, integer and float widening conversions are allowed
/// (e.g., Int32 → Int64, Float32 → Float64).
fn is_spark_compatible_parquet_coercion(
    file_type: &DataType,
    required_type: &DataType,
    schema_evolution_enabled: bool,
) -> bool {
    use DataType::*;
    match (file_type, required_type) {
        // Same type is always OK
        (a, b) if a == b => true,

        // Spark rejects reading TimestampLTZ as TimestampNTZ (and vice versa)
        (Timestamp(_, Some(_)), Timestamp(_, None))
        | (Timestamp(_, None), Timestamp(_, Some(_))) => false,

        // Spark rejects integer type widening in the vectorized reader
        // (INT32 → LongType, INT32 → DoubleType, etc.)
        // When schema evolution is enabled, these widenings are allowed.
        (Int8 | Int16 | Int32, Int64) => schema_evolution_enabled,
        (Int8 | Int16 | Int32 | Int64, Float32 | Float64) => schema_evolution_enabled,
        (Float32, Float64) => schema_evolution_enabled,

        // Spark rejects reading string/binary columns as timestamp or other numeric types
        (Utf8 | LargeUtf8 | Binary | LargeBinary, Timestamp(_, _)) => false,
        (Utf8 | LargeUtf8 | Binary | LargeBinary, Int8 | Int16 | Int32 | Int64) => false,

        // Reject cross-category conversions between non-matching structural types
        // e.g., scalar types to list/struct/map types
        (_, List(_) | LargeList(_) | Struct(_) | Map(_, _))
            if !matches!(file_type, List(_) | LargeList(_) | Struct(_) | Map(_, _)) =>
        {
            false
        }

        // For struct types, recursively check field conversions
        (Struct(from_fields), Struct(to_fields)) => {
            for to_field in to_fields.iter() {
                if let Some(from_field) = from_fields
                    .iter()
                    .find(|f| f.name().to_lowercase() == to_field.name().to_lowercase())
                {
                    if from_field.data_type() != to_field.data_type()
                        && !is_spark_compatible_parquet_coercion(
                            from_field.data_type(),
                            to_field.data_type(),
                            schema_evolution_enabled,
                        )
                    {
                        return false;
                    }
                }
            }
            true
        }

        // For list types, check element type conversions
        (List(from_inner), List(to_inner)) => {
            from_inner.data_type() == to_inner.data_type()
                || is_spark_compatible_parquet_coercion(
                    from_inner.data_type(),
                    to_inner.data_type(),
                    schema_evolution_enabled,
                )
        }

        // Everything else is allowed (handled by parquet_convert_array or arrow-rs cast)
        _ => true,
    }
}

/// Convert an Arrow DataType to a Spark-style display name for error messages
fn arrow_type_to_spark_name(dt: &DataType) -> String {
    use DataType::*;
    match dt {
        Boolean => "boolean".to_string(),
        Int8 => "tinyint".to_string(),
        Int16 => "smallint".to_string(),
        Int32 => "int".to_string(),
        Int64 => "bigint".to_string(),
        Float32 => "float".to_string(),
        Float64 => "double".to_string(),
        Utf8 | LargeUtf8 => "string".to_string(),
        Binary | LargeBinary => "binary".to_string(),
        Date32 => "date".to_string(),
        Timestamp(_, Some(_)) => "timestamp".to_string(),
        Timestamp(_, None) => "timestamp_ntz".to_string(),
        Decimal128(p, s) => format!("decimal({},{})", p, s),
        _ => format!("{}", dt),
    }
}

/// Convert an Arrow DataType to a Parquet physical type name for error messages
fn arrow_type_to_parquet_physical_name(dt: &DataType) -> String {
    use DataType::*;
    match dt {
        Boolean => "BOOLEAN".to_string(),
        Int8 | Int16 | Int32 | UInt8 | UInt16 => "INT32".to_string(),
        Int64 | UInt32 | UInt64 => "INT64".to_string(),
        Float32 => "FLOAT".to_string(),
        Float64 => "DOUBLE".to_string(),
        Utf8 | LargeUtf8 | Binary | LargeBinary => "BINARY".to_string(),
        Date32 => "INT32".to_string(),
        Timestamp(_, _) => "INT64".to_string(),
        Decimal128(_, _) => "FIXED_LEN_BYTE_ARRAY".to_string(),
        _ => format!("{}", dt),
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
    use datafusion::datasource::physical_plan::FileSource;
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

        let parquet_source =
            ParquetSource::new(TableParquetOptions::new()).with_schema_adapter_factory(
                Arc::new(SparkSchemaAdapterFactory::new(spark_parquet_options, None)),
            )?;

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
