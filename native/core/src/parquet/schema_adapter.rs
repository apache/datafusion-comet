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

//! Spark-compatible schema mapping for runtime batch transformation

use crate::parquet::parquet_support::{spark_parquet_convert, SparkParquetOptions};
use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Factory for creating [`SparkSchemaMapper`] instances.
///
/// This replaces the deprecated DataFusion `SchemaAdapterFactory` with a standalone
/// implementation that performs runtime batch transformation using Spark-compatible
/// type conversions.
#[derive(Clone, Debug)]
pub struct SparkSchemaMapperFactory {
    /// Spark cast options
    parquet_options: SparkParquetOptions,
    /// Default values for missing columns
    default_values: Option<HashMap<usize, ScalarValue>>,
}

impl SparkSchemaMapperFactory {
    pub fn new(
        options: SparkParquetOptions,
        default_values: Option<HashMap<usize, ScalarValue>>,
    ) -> Self {
        Self {
            parquet_options: options,
            default_values,
        }
    }

    /// Create a schema mapper for transforming batches from file schema to required schema
    pub fn create_mapper(
        &self,
        required_schema: SchemaRef,
        file_schema: &Schema,
    ) -> datafusion::common::Result<SparkSchemaMapper> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let mut field_mappings = vec![None; required_schema.fields().len()];

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some((table_idx, _table_field)) =
                required_schema.fields().iter().enumerate().find(|(_, b)| {
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

        Ok(SparkSchemaMapper {
            required_schema,
            field_mappings,
            parquet_options: self.parquet_options.clone(),
            default_values: self.default_values.clone(),
        })
    }
}

/// Maps record batches from file schema to required schema using Spark-compatible conversions.
///
/// This performs runtime batch transformation, applying type conversions and handling
/// missing columns with default values or nulls.
#[derive(Debug)]
pub struct SparkSchemaMapper {
    /// The required output schema after conversion
    required_schema: SchemaRef,
    /// Mapping from field index in required schema to index in file batch
    field_mappings: Vec<Option<usize>>,
    /// Spark cast options
    parquet_options: SparkParquetOptions,
    /// Default values for missing columns
    default_values: Option<HashMap<usize, ScalarValue>>,
}

impl SparkSchemaMapper {
    /// Transform a record batch from file schema to required schema
    pub fn map_batch(&self, batch: RecordBatch) -> datafusion::common::Result<RecordBatch> {
        let batch_rows = batch.num_rows();
        let batch_cols = batch.columns().to_vec();

        let cols = self
            .required_schema
            .fields()
            .iter()
            .enumerate()
            .zip(&self.field_mappings)
            .map(|((field_idx, field), file_idx)| {
                file_idx.map_or_else(
                    // Field only exists in required schema, not in file
                    || {
                        if let Some(default_values) = &self.default_values {
                            // We have a map of default values, see if this field is in there.
                            if let Some(value) = default_values.get(&field_idx)
                            // Default value exists, construct a column from it.
                            {
                                let cv = if field.data_type() == &value.data_type() {
                                    ColumnarValue::Scalar(value.clone())
                                } else {
                                    // Data types don't match - convert using Spark semantics
                                    spark_parquet_convert(
                                        ColumnarValue::Scalar(value.clone()),
                                        field.data_type(),
                                        &self.parquet_options,
                                    )?
                                };
                                return cv.into_array(batch_rows);
                            }
                        }
                        // No default value - create null column
                        let cv =
                            ColumnarValue::Scalar(ScalarValue::try_new_null(field.data_type())?);
                        cv.into_array(batch_rows)
                    },
                    // Field exists in both schemas - convert if needed
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

        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let schema = Arc::<Schema>::clone(&self.required_schema);
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }
}

#[cfg(test)]
mod test {
    use crate::parquet::parquet_support::SparkParquetOptions;
    use crate::parquet::schema_adapter::SparkSchemaMapperFactory;
    use arrow::array::UInt32Array;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::DataFusionError;
    use datafusion_comet_spark_expr::EvalMode;
    use std::sync::Arc;

    #[test]
    fn test_schema_mapper_int_to_string() -> Result<(), DataFusionError> {
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

        let spark_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        let factory = SparkSchemaMapperFactory::new(spark_options, None);
        let mapper = factory.create_mapper(Arc::clone(&required_schema), file_schema.as_ref())?;

        let result = mapper.map_batch(batch)?;

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema(), required_schema);

        Ok(())
    }

    #[test]
    fn test_schema_mapper_unsigned_int() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));

        let ids = Arc::new(UInt32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![ids])?;

        let required_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let mut spark_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        spark_options.allow_cast_unsigned_ints = true;

        let factory = SparkSchemaMapperFactory::new(spark_options, None);
        let mapper = factory.create_mapper(Arc::clone(&required_schema), file_schema.as_ref())?;

        let result = mapper.map_batch(batch)?;

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema(), required_schema);

        Ok(())
    }
}
