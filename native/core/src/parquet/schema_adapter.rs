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
use datafusion::common::ColumnStatistics;
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};
use datafusion::physical_plan::{
    ColumnarValue, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;


/// A stream wrapper that applies schema adaptation to batches from an underlying stream.
///
/// This stream wraps another `RecordBatchStream` and applies Spark-compatible schema
/// transformation to each batch, including type casting and handling missing columns.
pub struct ParquetSchemaAdapterStream {
    /// The underlying stream producing batches
    inner: datafusion::execution::SendableRecordBatchStream,
    /// The target schema after adaptation
    required_schema: SchemaRef,
    /// Mapping from field index in `required_schema` to index in source schema
    field_mappings: Vec<Option<usize>>,
    /// Spark cast options
    parquet_options: SparkParquetOptions,
    /// Default values for missing columns (keyed by required schema index)
    default_values: Option<HashMap<usize, ScalarValue>>,
}

impl ParquetSchemaAdapterStream {
    /// Create a new schema adapter stream by mapping from source schema to required schema
    ///
    /// # Arguments
    /// * `inner` - The underlying stream to wrap
    /// * `required_schema` - The target schema after adaptation
    /// * `source_schema` - The schema of batches from the inner stream
    /// * `parquet_options` - Spark-specific parquet options for casting
    /// * `default_values` - Optional default values for missing columns
    pub fn new(
        inner: datafusion::execution::SendableRecordBatchStream,
        required_schema: SchemaRef,
        source_schema: &Schema,
        parquet_options: SparkParquetOptions,
        default_values: Option<HashMap<usize, ScalarValue>>,
    ) -> Self {
        let field_mappings = Self::map_schema(&required_schema, source_schema, &parquet_options);

        Self {
            inner,
            required_schema,
            field_mappings,
            parquet_options,
            default_values,
        }
    }

    /// Map schema to create field mappings from source to required schema
    ///
    /// Returns a vector where each index corresponds to a field in the required schema,
    /// and the value is the index of that field in the source schema (or None if missing)
    fn map_schema(
        required_schema: &Schema,
        source_schema: &Schema,
        parquet_options: &SparkParquetOptions,
    ) -> Vec<Option<usize>> {
        let mut field_mappings = vec![None; required_schema.fields().len()];

        for (required_idx, required_field) in required_schema.fields().iter().enumerate() {
            // Find matching field in source schema
            let source_idx = source_schema
                .fields
                .iter()
                .enumerate()
                .find(|(_, source_field)| {
                    if parquet_options.case_sensitive {
                        source_field.name() == required_field.name()
                    } else {
                        source_field.name().to_lowercase() == required_field.name().to_lowercase()
                    }
                })
                .map(|(idx, _)| idx);

            field_mappings[required_idx] = source_idx;
        }

        field_mappings
    }

    /// Map a batch from the source schema to the required schema
    fn map_batch(&self, batch: RecordBatch) -> datafusion::common::Result<RecordBatch> {
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
                    // Field only exists in required schema, not in source
                    || {
                        if let Some(default_values) = &self.default_values {
                            // We have a map of default values, see if this field is in there.
                            if let Some(value) = default_values.get(&field_idx)
                            // Default value exists, construct a column from it.
                            {
                                let cv = if field.data_type() == &value.data_type() {
                                    ColumnarValue::Scalar(value.clone())
                                } else {
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
                    // Field exists in both schemas - cast if needed
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
        RecordBatch::try_new_with_options(schema, cols, &options).map_err(|e| e.into())
    }
}

impl futures::Stream for ParquetSchemaAdapterStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures::StreamExt;

        match futures::ready!(self.inner.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                // Apply schema mapping to the batch
                let adapted_batch = self.map_batch(batch);
                std::task::Poll::Ready(Some(adapted_batch))
            }
            Some(Err(e)) => std::task::Poll::Ready(Some(Err(e))),
            None => std::task::Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl datafusion::execution::RecordBatchStream for ParquetSchemaAdapterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.required_schema)
    }
}

/// An execution plan that wraps another execution plan and applies schema adaptation
/// to transform batches from the source schema to a required schema.
///
/// This is useful when you need to apply Spark-compatible type conversions and handle
/// missing columns at the execution plan level.
#[derive(Debug)]
pub struct ParquetSchemaAdapterExec {
    /// The underlying execution plan
    input: Arc<dyn ExecutionPlan>,
    /// The target schema after adaptation
    required_schema: SchemaRef,
    /// Spark cast options
    parquet_options: SparkParquetOptions,
    /// Default values for missing columns (keyed by required schema index)
    default_values: Option<HashMap<usize, ScalarValue>>,
    /// Cached plan properties
    properties: PlanProperties,
}

impl ParquetSchemaAdapterExec {
    /// Create a new schema adapter execution plan
    ///
    /// # Arguments
    /// * `input` - The underlying execution plan to wrap
    /// * `required_schema` - The target schema after adaptation
    /// * `parquet_options` - Spark-specific parquet options for casting
    /// * `default_values` - Optional default values for missing columns
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        required_schema: SchemaRef,
        parquet_options: SparkParquetOptions,
        default_values: Option<HashMap<usize, ScalarValue>>,
    ) -> Self {
        use datafusion::physical_plan::execution_plan::{EmissionType, Boundedness};
        use datafusion::physical_expr::EquivalenceProperties;

        // Create properties with the required schema
        let eq_properties = EquivalenceProperties::new(Arc::clone(&required_schema));
        let properties = PlanProperties::new(
            eq_properties,
            input.properties().output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            input,
            required_schema,
            parquet_options,
            default_values,
            properties,
        }
    }

    /// Get the underlying input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for ParquetSchemaAdapterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ParquetSchemaAdapterExec: required_schema={:?}",
                    self.required_schema
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "ParquetSchemaAdapterExec")
            }
        }
    }
}

impl ExecutionPlan for ParquetSchemaAdapterExec {
    fn name(&self) -> &str {
        "ParquetSchemaAdapterExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::common::DataFusionError::Internal(
                "ParquetSchemaAdapterExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(ParquetSchemaAdapterExec::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.required_schema),
            self.parquet_options.clone(),
            self.default_values.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
        // Execute the input plan to get its stream
        let input_stream = self.input.execute(partition, context)?;
        let source_schema = input_stream.schema();

        // Wrap the input stream with schema adaptation
        let adapter_stream = ParquetSchemaAdapterStream::new(
            input_stream,
            Arc::clone(&self.required_schema),
            &source_schema,
            self.parquet_options.clone(),
            self.default_values.clone(),
        );

        Ok(Box::pin(adapter_stream))
    }
}

#[cfg(test)]
mod test {
    use crate::parquet::parquet_support::SparkParquetOptions;
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
    use crate::parquet::schema_adapter::ParquetSchemaAdapterExec;

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
            Arc::new(ParquetSource::new(TableParquetOptions::new()));

        let files = FileGroup::new(vec![PartitionedFile::from_path(filename.to_string())?]);
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, Arc::clone(&required_schema), parquet_source)
                .with_file_groups(vec![files])
                .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));

        let adapter_exec: Arc<dyn ExecutionPlan> = Arc::new(ParquetSchemaAdapterExec::new(
            Arc::new(parquet_exec),
            required_schema,
            spark_parquet_options,
            None,
        ));

        let mut stream = adapter_exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        stream.next().await.unwrap()
    }

    #[tokio::test]
    async fn test_schema_adapter_stream() -> Result<(), DataFusionError> {
        use datafusion::execution::RecordBatchStream;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        // Create source data with Int32
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let ids = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let values = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let batch = RecordBatch::try_new(Arc::clone(&source_schema), vec![ids, values])?;

        // Create target schema with String for id
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Create Spark options
        let spark_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);

        // Create a simple stream that yields one batch
        let source_stream = RecordBatchStreamAdapter::new(
            Arc::clone(&source_schema),
            futures::stream::once(async move { Ok(batch) }),
        );

        // Wrap with schema adapter stream - it will compute field mappings internally
        let mut adapter_stream = super::ParquetSchemaAdapterStream::new(
            Box::pin(source_stream),
            Arc::clone(&target_schema),
            &source_schema,
            spark_options,
            None,
        );

        // Verify the schema
        assert_eq!(adapter_stream.schema(), target_schema);

        // Read and verify the adapted batch
        let adapted_batch = adapter_stream.next().await.unwrap()?;
        assert_eq!(adapted_batch.num_rows(), 3);
        assert_eq!(adapted_batch.schema(), target_schema);

        // Verify id column was cast to String
        let id_col = adapted_batch.column(0);
        assert_eq!(id_col.data_type(), &DataType::Utf8);
        let id_array = id_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(id_array.value(0), "1");
        assert_eq!(id_array.value(1), "2");
        assert_eq!(id_array.value(2), "3");

        // Verify value column remained Int32
        let value_col = adapted_batch.column(1);
        assert_eq!(value_col.data_type(), &DataType::Int32);
        let value_array = value_col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(value_array.value(0), 10);
        assert_eq!(value_array.value(1), 20);
        assert_eq!(value_array.value(2), 30);

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_adapter_exec() -> Result<(), DataFusionError> {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::physical_plan::{DisplayAs, DisplayFormatType};

        // Create source data with Int32
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let ids = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let values = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let batch = RecordBatch::try_new(Arc::clone(&source_schema), vec![ids, values])?;

        // Create target schema with String for id
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Create a simple mock execution plan that returns our batch
        #[derive(Debug)]
        struct MockExec {
            schema: SchemaRef,
            batch: RecordBatch,
            properties: datafusion::physical_plan::PlanProperties,
        }

        impl MockExec {
            fn new(schema: SchemaRef, batch: RecordBatch) -> Self {
                use datafusion::physical_plan::Partitioning;
                use datafusion::physical_plan::execution_plan::{EmissionType, Boundedness};
                use datafusion::physical_expr::EquivalenceProperties;

                let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
                let properties = datafusion::physical_plan::PlanProperties::new(
                    eq_properties,
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Final,
                    Boundedness::Bounded,
                );

                Self {
                    schema,
                    batch,
                    properties,
                }
            }
        }

        impl DisplayAs for MockExec {
            fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "MockExec")
            }
        }

        impl ExecutionPlan for MockExec {
            fn name(&self) -> &str {
                "MockExec"
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
                &self.properties
            }

            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                vec![]
            }

            fn with_new_children(
                self: Arc<Self>,
                _children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
                Ok(self)
            }

            fn execute(
                &self,
                _partition: usize,
                _context: Arc<datafusion::execution::TaskContext>,
            ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
                let batch = self.batch.clone();
                let schema = Arc::clone(&self.schema);
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::once(async move { Ok(batch) }),
                )))
            }
        }

        let mock_exec = Arc::new(MockExec::new(Arc::clone(&source_schema), batch));

        // Wrap with schema adapter exec
        let spark_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        let adapter_exec = super::ParquetSchemaAdapterExec::new(
            mock_exec,
            Arc::clone(&target_schema),
            spark_options,
            None,
        );

        // Verify the schema
        assert_eq!(adapter_exec.schema(), target_schema);

        // Execute and verify the adapted batch
        let mut stream = adapter_exec.execute(0, Arc::new(TaskContext::default()))?;
        let adapted_batch = stream.next().await.unwrap()?;

        assert_eq!(adapted_batch.num_rows(), 3);
        assert_eq!(adapted_batch.schema(), target_schema);

        // Verify id column was cast to String
        let id_col = adapted_batch.column(0);
        assert_eq!(id_col.data_type(), &DataType::Utf8);
        let id_array = id_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(id_array.value(0), "1");
        assert_eq!(id_array.value(1), "2");
        assert_eq!(id_array.value(2), "3");

        // Verify value column remained Int32
        let value_col = adapted_batch.column(1);
        assert_eq!(value_col.data_type(), &DataType::Int32);
        let value_array = value_col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(value_array.value(0), 10);
        assert_eq!(value_array.value(1), 20);
        assert_eq!(value_array.value(2), 30);

        Ok(())
    }
}
