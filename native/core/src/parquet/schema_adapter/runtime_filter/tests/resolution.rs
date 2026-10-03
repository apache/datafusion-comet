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
use arrow::array::{Int32Array, RecordBatch};
use datafusion::common::ScalarValue;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

#[test]
fn matching_case_and_field_id_remaps_need_no_eligibility_rewrites() {
    for use_field_id in [false, true] {
        let mut factory = factory();
        factory.parquet_options.case_sensitive = false;
        factory.parquet_options.use_field_id = use_field_id;
        let physical_name = if use_field_id { "stored_key" } else { "KEY" };
        let mut logical_field = Field::new("key", DataType::Int32, true);
        let mut physical_field = Field::new(physical_name, DataType::Int32, true);
        if use_field_id {
            let metadata =
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())]);
            logical_field = logical_field.with_metadata(metadata.clone());
            physical_field = physical_field.with_metadata(metadata);
        }
        let logical = Arc::new(Schema::new(vec![logical_field]));
        let physical = Arc::new(Schema::new(vec![physical_field]));
        let (adapter, count) = counted_adapter(&factory, logical, Arc::clone(&physical));
        let column = Column::new("key", usize::MAX);
        assert!(adapter.read_columns_are_infallible(std::slice::from_ref(&column), &physical));
        assert_eq!(count.load(Ordering::Relaxed), 0);

        // The eligibility shortcut must still leave normal expression adaptation responsible
        // for restoring the physical name and repairing the stale projection index.
        let rewritten = adapter.rewrite(Arc::new(column)).unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 1);
        assert_eq!(
            rewritten.downcast_ref::<Column>(),
            Some(&Column::new(physical_name, 0))
        );
        let values = Int32Array::from(vec![Some(7), None, Some(-2)]);
        let batch = RecordBatch::try_new(physical, vec![Arc::new(values.clone())]).unwrap();
        let output = rewritten
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<Int32Array>().unwrap(),
            &values
        );
    }
}

#[test]
fn case_and_field_id_remapping_keep_the_rewrite_fallback() {
    for use_field_id in [false, true] {
        let mut factory = factory();
        factory.parquet_options.case_sensitive = false;
        factory.parquet_options.use_field_id = use_field_id;
        let mut logical_field = Field::new("key", DataType::Int32, true);
        let mut physical_field = Field::new(
            if use_field_id { "stored_key" } else { "KEY" },
            DataType::Int32,
            true,
        );
        if use_field_id {
            let metadata =
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())]);
            logical_field = logical_field.with_metadata(metadata.clone());
            physical_field = physical_field.with_metadata(metadata);
        }
        // Different field metadata deliberately falls back to the normal adapter even
        // though its identity cast ultimately leaves only a resolved physical column.
        physical_field
            .metadata_mut()
            .insert("extra".to_string(), "file".to_string());
        let logical = Arc::new(Schema::new(vec![logical_field]));
        let physical = Arc::new(Schema::new(vec![physical_field]));
        let (adapter, count) = counted_adapter(&factory, logical, Arc::clone(&physical));
        assert!(adapter.read_columns_are_infallible(&[Column::new("key", usize::MAX)], &physical));
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn missing_defaults_keep_the_rewrite_fallback() {
    let mut factory = factory();
    factory.default_values = Some(HashMap::from([(
        Column::new("payload", 1),
        ScalarValue::Int32(Some(9)),
    )]));
    let logical = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, true),
        Field::new("payload", DataType::Int32, true),
    ]));
    let physical = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
    let (adapter, count) = counted_adapter(&factory, logical, Arc::clone(&physical));
    assert!(adapter.read_columns_are_infallible(&[Column::new("payload", 1)], &physical));
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[test]
fn root_name_ambiguity_cannot_use_matching_fields() {
    let mut factory = factory();
    factory.parquet_options.case_sensitive = false;
    let logical = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
    let physical = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, true),
        Field::new("KEY", DataType::Int32, true),
    ]));
    let (adapter, _) = counted_adapter(&factory, logical, Arc::clone(&physical));
    assert!(!adapter.read_columns_are_infallible(&[Column::new("key", 0)], &physical));
}
