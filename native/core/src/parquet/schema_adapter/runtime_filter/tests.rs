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

mod factory;
mod resolution;

use super::*;
use crate::parquet::parquet_support::SparkParquetOptions;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::EvalMode;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct CountRewrites {
    inner: Arc<dyn PhysicalExprAdapter>,
    count: Arc<AtomicUsize>,
}

impl PhysicalExprAdapter for CountRewrites {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.inner.rewrite(expr)
    }
}

fn factory() -> SparkPhysicalExprAdapterFactory {
    let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
    options.case_sensitive = true;
    SparkPhysicalExprAdapterFactory::new(options, None)
}

fn counted_adapter(
    factory: &SparkPhysicalExprAdapterFactory,
    logical: SchemaRef,
    physical: SchemaRef,
) -> (SparkPhysicalExprAdapter, Arc<AtomicUsize>) {
    let mut adapter = factory.create_adapter(logical, physical).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    adapter.default_adapter = Arc::new(CountRewrites {
        inner: Arc::clone(&adapter.default_adapter),
        count: Arc::clone(&count),
    });
    (adapter, count)
}

#[test]
fn matching_wide_schema_needs_no_eligibility_rewrites() {
    let schema = Arc::new(Schema::new(
        (0..256)
            .map(|index| Field::new(format!("col_{index}"), DataType::Int64, true))
            .collect::<Vec<_>>(),
    ));
    let columns = schema
        .fields()
        .iter()
        // Include stale predicate indices: name resolution must establish safety.
        .map(|field| Column::new(field.name(), usize::MAX))
        .collect::<Vec<_>>();
    let (adapter, count) = counted_adapter(&factory(), Arc::clone(&schema), Arc::clone(&schema));
    assert!(adapter.read_columns_are_infallible(&columns, &schema));
    assert_eq!(count.load(Ordering::Relaxed), 0);
}

#[test]
fn matching_schemas_do_not_prove_an_unresolved_column_safe() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
    let (adapter, count) = counted_adapter(&factory(), Arc::clone(&schema), Arc::clone(&schema));
    let columns = [Column::new("missing", 0)];
    assert!(!adapter.read_columns_are_infallible(&columns, &schema));
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[test]
fn matching_variant_fields_still_require_normalization() {
    let storage = DataType::Struct(
        vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]
        .into(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", storage, true).with_extension_type(VariantType)
    ]));
    let (adapter, count) = counted_adapter(&factory(), Arc::clone(&schema), Arc::clone(&schema));
    assert!(!adapter.read_columns_are_infallible(&[Column::new("payload", 0)], &schema));
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[test]
fn conversion_in_a_required_payload_uses_the_rewrite_verdict() {
    let physical = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, true),
        Field::new("payload", DataType::Int32, true),
    ]));
    let logical = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, true),
        Field::new("payload", DataType::Int64, true),
    ]));
    for allowed in [false, true] {
        let mut factory = factory();
        factory.parquet_options.allow_type_promotion = allowed;
        let (adapter, count) =
            counted_adapter(&factory, Arc::clone(&logical), Arc::clone(&physical));
        assert_eq!(
            adapter.read_columns_are_infallible(
                &[Column::new("key", 0), Column::new("payload", usize::MAX)],
                &physical,
            ),
            allowed,
        );
        assert_eq!(count.load(Ordering::Relaxed), 2);
    }
}
