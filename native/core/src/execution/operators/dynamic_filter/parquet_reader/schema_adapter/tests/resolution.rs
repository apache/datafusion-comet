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

use super::*;
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
use datafusion::common::ScalarValue;
use datafusion_comet_spark_expr::EvalMode;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use std::collections::HashMap;

fn spark_factory(
    use_field_id: bool,
    defaults: Option<HashMap<Column, ScalarValue>>,
) -> Arc<dyn PhysicalExprAdapterFactory> {
    let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
    options.use_field_id = use_field_id;
    Arc::new(SparkPhysicalExprAdapterFactory::new(options, defaults))
}

fn assert_filter_result(
    adapter: &Arc<dyn PhysicalExprAdapter>,
    key: &str,
    physical_schema: SchemaRef,
    expected: bool,
) {
    let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::new(Column::new(key, 0))],
        lit(false),
    ));
    let batch =
        RecordBatch::try_new(physical_schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let actual = adapter
        .rewrite(predicate)
        .unwrap()
        .evaluate(&batch)
        .unwrap()
        .into_array(1)
        .unwrap();
    assert_eq!(
        actual.as_any().downcast_ref::<BooleanArray>().unwrap(),
        &BooleanArray::from(vec![expected])
    );
}

#[test]
fn unresolved_spark_column_disables_reader_filter() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
    let factory = spark_factory(false, None);
    let column = Column::new("partition", 1);
    let inner = factory
        .create(Arc::clone(&schema), Arc::clone(&schema))
        .unwrap();
    // Spark's fallback can return an unknown name unchanged after the default
    // adapter fails. A bare Column alone does not establish safe resolution.
    let unresolved = inner.rewrite(Arc::new(column.clone())).unwrap();
    assert_eq!(unresolved.downcast_ref::<Column>(), Some(&column));
    let guarded = RuntimeFilterSchemaAdapterFactory::new(factory, vec![column])
        .create(Arc::clone(&schema), Arc::clone(&schema))
        .unwrap();
    assert_filter_result(&guarded, "key", schema, true);
}

#[test]
fn remapped_physical_names_keep_reader_filter() {
    for (physical_name, use_field_id) in [("KEY", false), ("stored_key", true)] {
        let mut logical_field = Field::new("Key", DataType::Int32, false);
        let mut physical_field = Field::new(physical_name, DataType::Int32, false);
        if use_field_id {
            let metadata =
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())]);
            logical_field = logical_field.with_metadata(metadata.clone());
            physical_field = physical_field.with_metadata(metadata);
        }
        let logical = Arc::new(Schema::new(vec![logical_field]));
        let physical = Arc::new(Schema::new(vec![physical_field]));
        let column = Column::new("Key", usize::MAX);
        let factory = spark_factory(use_field_id, None);
        let inner = factory
            .create(Arc::clone(&logical), Arc::clone(&physical))
            .unwrap();
        let remapped = inner.rewrite(Arc::new(column.clone())).unwrap();
        assert_eq!(
            remapped.downcast_ref::<Column>().unwrap().name(),
            physical_name
        );
        let guarded = RuntimeFilterSchemaAdapterFactory::new(factory, vec![column])
            .create(logical, Arc::clone(&physical))
            .unwrap();
        assert_filter_result(&guarded, "Key", physical, false);
    }
}

#[test]
fn missing_column_literals_keep_reader_filter() {
    for default in [None, Some(ScalarValue::Int32(Some(7)))] {
        let logical = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("missing", DataType::Int32, true),
        ]));
        let physical = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let column = Column::new("missing", 1);
        let factory = spark_factory(
            false,
            default.map(|value| HashMap::from([(column.clone(), value)])),
        );
        let inner = factory
            .create(Arc::clone(&logical), Arc::clone(&physical))
            .unwrap();
        assert!(inner
            .rewrite(Arc::new(column.clone()))
            .unwrap()
            .is::<Literal>());
        let guarded = RuntimeFilterSchemaAdapterFactory::new(factory, vec![column])
            .create(logical, Arc::clone(&physical))
            .unwrap();
        assert_filter_result(&guarded, "key", physical, false);
    }
}
