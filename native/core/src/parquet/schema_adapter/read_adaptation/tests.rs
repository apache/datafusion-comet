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

mod scalar;
mod structural;

use super::*;
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
use arrow::datatypes::SchemaRef;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_comet_spark_expr::EvalMode;

fn options() -> SparkParquetOptions {
    let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
    options.case_sensitive = true;
    options
}

fn adapt(
    source: Field,
    target: Field,
    options: SparkParquetOptions,
) -> (SchemaRef, Arc<dyn PhysicalExpr>) {
    let physical = Arc::new(Schema::new(vec![source]));
    let logical = Arc::new(Schema::new(vec![target]));
    let adapter = SparkPhysicalExprAdapterFactory::new(options, None)
        .create(logical, Arc::clone(&physical))
        .unwrap();
    let expr = adapter
        .rewrite(Arc::new(Column::new("s", usize::MAX)))
        .unwrap();
    (physical, expr)
}

fn structural_cast(source: DataType, target: DataType) -> (Schema, Arc<dyn PhysicalExpr>) {
    let schema = Schema::new(vec![Field::new("s", source, true)]);
    let expr = Arc::new(CastExpr::new_with_target_field(
        Arc::new(Column::new("s", 0)),
        Arc::new(Field::new("s", target, true)),
        None,
    ));
    (schema, expr)
}

fn struct_type(fields: Vec<Field>) -> DataType {
    DataType::Struct(fields.into())
}
