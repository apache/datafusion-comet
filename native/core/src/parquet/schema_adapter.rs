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

use crate::parquet::parquet_support::SparkParquetOptions;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::tree_node::{Transformed, TransformedResult};
use datafusion::common::Result as DataFusionResult;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::{replace_columns_with_literals, DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion::scalar::ScalarValue;
use datafusion_comet_spark_expr::{Cast, SparkCastOptions};
use std::collections::HashMap;
use std::sync::Arc;

/// Factory for creating [`SparkSchemaMapper`] instances.
///
/// This replaces the deprecated DataFusion `SchemaAdapterFactory` with a standalone
/// implementation that performs runtime batch transformation using Spark-compatible
/// type conversions.
#[derive(Clone, Debug)]
pub struct SparkPhysicalExprAdapterFactory {
    /// Spark-specific parquet options for type conversions
    parquet_options: SparkParquetOptions,
    /// Default values for columns that may be missing from the physical schema.
    /// The key is the column index in the logical schema.
    default_values: Option<HashMap<usize, ScalarValue>>,
}

impl SparkPhysicalExprAdapterFactory {
    /// Create a new factory with the given options.
    pub fn new(
        parquet_options: SparkParquetOptions,
        default_values: Option<HashMap<usize, ScalarValue>>,
    ) -> Self {
        Self {
            parquet_options,
            default_values,
        }
    }
}

impl PhysicalExprAdapterFactory for SparkPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        let default_factory = DefaultPhysicalExprAdapterFactory;
        let default_adapter = default_factory.create(
            Arc::clone(&logical_file_schema),
            Arc::clone(&physical_file_schema),
        );

        Arc::new(SparkPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
            parquet_options: self.parquet_options.clone(),
            default_values: self.default_values.clone(),
            default_adapter,
        })
    }
}

#[derive(Debug)]
struct SparkPhysicalExprAdapter {
    /// The logical schema expected by the query
    logical_file_schema: SchemaRef,
    /// The physical schema of the actual file being read
    physical_file_schema: SchemaRef,
    /// Spark-specific options for type conversions
    parquet_options: SparkParquetOptions,
    /// Default values for missing columns (keyed by logical schema index)
    default_values: Option<HashMap<usize, ScalarValue>>,
    /// The default DataFusion adapter to delegate standard handling to
    default_adapter: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for SparkPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        // Step 1: Handle default values for missing columns
        let expr = self.replace_missing_with_defaults(expr)?;

        // Step 2: Delegate to default adapter for standard handling
        // This handles: missing columns → nulls, type mismatches → CastColumnExpr
        let expr = self.default_adapter.rewrite(expr)?;

        // Step 3: Replace CastColumnExpr with Spark-compatible Cast expressions
        expr.transform(|e| self.replace_with_spark_cast(e)).data()
    }
}

impl SparkPhysicalExprAdapter {
    /// Replace CastColumnExpr (DataFusion's cast) with Spark's Cast expression.
    fn replace_with_spark_cast(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Transformed<Arc<dyn PhysicalExpr>>> {
        // Check for CastColumnExpr and replace with spark_expr::Cast
        // CastColumnExpr is in datafusion_physical_expr::expressions
        if let Some(cast) = expr
            .as_any()
            .downcast_ref::<datafusion::physical_expr::expressions::CastColumnExpr>()
        {
            let child = cast.expr().clone();
            let target_type = cast.target_field().data_type().clone();

            // Create Spark-compatible cast options
            let mut cast_options = SparkCastOptions::new(
                self.parquet_options.eval_mode,
                &self.parquet_options.timezone,
                self.parquet_options.allow_incompat,
            );
            cast_options.allow_cast_unsigned_ints = self.parquet_options.allow_cast_unsigned_ints;
            cast_options.is_adapting_schema = true;

            let spark_cast = Arc::new(Cast::new(child, target_type, cast_options));

            return Ok(Transformed::yes(spark_cast as Arc<dyn PhysicalExpr>));
        }

        Ok(Transformed::no(expr))
    }

    /// Replace references to missing columns with default values.
    fn replace_missing_with_defaults(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let Some(defaults) = &self.default_values else {
            return Ok(expr);
        };

        if defaults.is_empty() {
            return Ok(expr);
        }

        // Convert index-based defaults to name-based for replace_columns_with_literals
        let name_based: HashMap<&str, &ScalarValue> = defaults
            .iter()
            .filter_map(|(idx, val)| {
                self.logical_file_schema
                    .fields()
                    .get(*idx)
                    .map(|f| (f.name().as_str(), val))
            })
            .collect();

        if name_based.is_empty() {
            return Ok(expr);
        }

        replace_columns_with_literals(expr, &name_based)
    }
}

pub fn adapt_batch_with_expressions(
    batch: RecordBatch,
    target_schema: &SchemaRef,
    parquet_options: &SparkParquetOptions,
) -> DataFusionResult<RecordBatch> {
    let file_schema = batch.schema();

    // If schemas match, no adaptation needed
    if file_schema.as_ref() == target_schema.as_ref() {
        return Ok(batch);
    }

    // Create adapter
    let factory = SparkPhysicalExprAdapterFactory::new(parquet_options.clone(), None);
    let adapter = factory.create(Arc::clone(target_schema), Arc::clone(&file_schema));

    // Create column projection expressions for target schema
    let projection_exprs: Vec<Arc<dyn PhysicalExpr>> = target_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, _field)| {
            let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new_with_schema(
                target_schema.field(i).name(),
                target_schema.as_ref(),
            )?);
            adapter.rewrite(col_expr)
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    // Evaluate expressions against batch
    let columns: Vec<ArrayRef> = projection_exprs
        .iter()
        .map(|expr| {
            expr.evaluate(&batch)?
                .into_array(batch.num_rows())
                .map_err(|e| e.into())
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    RecordBatch::try_new(Arc::clone(target_schema), columns).map_err(|e| e.into())
}

#[cfg(test)]
mod test {
    use crate::parquet::parquet_support::SparkParquetOptions;
    use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
    use arrow::array::UInt32Array;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::DataFusionError;
    use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
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
        let factory = SparkPhysicalExprAdapterFactory::new(spark_options, None);
        let mapper = factory.create(Arc::clone(&required_schema), file_schema?)?;

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

        let factory = SparkPhysicalExprAdapterFactory::new(spark_options, None);
        let mapper = factory.create(Arc::clone(&required_schema), Arc::new(*file_schema.as_ref()));

        let result = mapper.map_batch(batch)?;

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema(), required_schema);

        Ok(())
    }
}
