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

#[tokio::test]
async fn signed_integer_boundaries_survive_dictionary_reader_pruning() {
    use arrow::array::Int64Array;

    for (data_type, min, max) in [
        (DataType::Int8, i8::MIN as i64, i8::MAX as i64),
        (DataType::Int16, i16::MIN as i64, i16::MAX as i64),
        (DataType::Int32, i32::MIN as i64, i32::MAX as i64),
        (DataType::Int64, i64::MIN, i64::MAX),
    ] {
        let values = vec![
            Some(max),
            Some(min),
            None,
            Some(0),
            Some(min),
            Some(max),
            None,
        ];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            data_type.clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![cast(&Int64Array::from(values.clone()), &data_type).unwrap()],
        )
        .unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ArrowWriter::try_new(
            file.reopen().unwrap(),
            Arc::clone(&schema),
            Some(
                WriterProperties::builder()
                    .set_dictionary_enabled(true)
                    .set_max_row_group_row_count(Some(2))
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        for descending in [false, true] {
            for nulls_first in [false, true] {
                for k in [1, 3, 20] {
                    let session = session(2);
                    let scan = parquet_scan(
                        &[&file],
                        Arc::clone(&schema),
                        vec![0],
                        None,
                        false,
                        &session,
                    );
                    let options = SortOptions {
                        descending,
                        nulls_first,
                    };
                    let plan = wrapper(&sort(scan, k, options), &session);
                    let actual = collect(Arc::new(plan), session.task_ctx()).await.unwrap();
                    let actual: Vec<_> = actual
                        .iter()
                        .flat_map(|batch| {
                            let array = cast(batch.column(0), &DataType::Int64).unwrap();
                            array
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .iter()
                                .collect::<Vec<_>>()
                        })
                        .collect();
                    let mut expected = values.clone();
                    expected.sort_by(|left, right| match (left, right) {
                        (None, None) => std::cmp::Ordering::Equal,
                        (None, Some(_)) => {
                            if nulls_first {
                                std::cmp::Ordering::Less
                            } else {
                                std::cmp::Ordering::Greater
                            }
                        }
                        (Some(_), None) => {
                            if nulls_first {
                                std::cmp::Ordering::Greater
                            } else {
                                std::cmp::Ordering::Less
                            }
                        }
                        (Some(left), Some(right)) => {
                            if descending {
                                right.cmp(left)
                            } else {
                                left.cmp(right)
                            }
                        }
                    });
                    expected.truncate(k);
                    assert_eq!(actual, expected, "{data_type:?}, {options:?}, K={k}");
                }
            }
        }
    }
}

#[tokio::test]
async fn empty_and_all_null_inputs() {
    let session = session(4);
    for values in [vec![], vec![None; 6]] {
        let (_file, scan) = parquet_input(values.clone(), &DataType::Int32, &session, 4);
        let plan = wrapper(&sort(scan, 3, SortOptions::default()), &session);
        assert!(plan.build_runtime_sort().unwrap().reader_filter_attached);
        let actual = collect(Arc::new(plan), session.task_ctx()).await.unwrap();
        assert_eq!(
            keys(&actual),
            values.into_iter().take(3).collect::<Vec<_>>()
        );
    }
}
