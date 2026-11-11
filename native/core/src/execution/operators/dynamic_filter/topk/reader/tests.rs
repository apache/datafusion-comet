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
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::statistics::Statistics;

#[test]
fn sanitizing_statistics_preserves_cached_metadata_and_indexes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("unknown", DataType::Int32, true),
        Field::new("known", DataType::Int32, true),
    ]));
    let values: arrow::array::ArrayRef =
        Arc::new(Int32Array::from(vec![None, Some(100), Some(101)]));
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&values), values]).unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    let bytes = Bytes::from(writer.into_inner().unwrap());
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
        bytes,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )
    .unwrap();
    let complete = Arc::clone(reader.metadata());
    assert!(Arc::ptr_eq(
        &complete,
        &preserve_unknown_null_counts(Arc::clone(&complete)).unwrap()
    ));
    let mut builder = complete.as_ref().clone().into_builder();
    let group = builder.take_row_groups().pop().unwrap();
    let mut columns = group.columns().to_vec();
    columns[0] = columns[0]
        .clone()
        .into_builder()
        .set_statistics(Statistics::int32(Some(100), Some(101), None, None, false))
        .build()
        .unwrap();
    let group = group
        .into_builder()
        .set_column_metadata(columns)
        .build()
        .unwrap();
    let cached = Arc::new(builder.set_row_groups(vec![group]).build());
    let filtered = preserve_unknown_null_counts(Arc::clone(&cached)).unwrap();
    assert!(!Arc::ptr_eq(&cached, &filtered));
    assert!(filtered.row_group(0).column(0).statistics().is_none());
    let original_stats = cached.row_group(0).column(0).statistics().unwrap();
    assert!(original_stats.null_count_opt().is_none());
    assert_eq!(
        original_stats.min_bytes_opt(),
        Some(100_i32.to_le_bytes().as_slice())
    );
    assert_eq!(
        filtered.row_group(0).column(1),
        cached.row_group(0).column(1)
    );
    assert!(filtered.column_index().is_some());
    assert_eq!(filtered.column_index(), cached.column_index());
    assert!(filtered.offset_index().is_some());
    assert_eq!(filtered.offset_index(), cached.offset_index());
}
