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
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaDataWriter;
use parquet::file::statistics::Statistics as ParquetStatistics;
use parquet::file::writer::TrackedWrite;
use std::io::Write;

pub(super) fn omit_last_null_count(file: &tempfile::NamedTempFile, column: usize) {
    let original = std::fs::read(file.path()).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
    let mut builder = reader.metadata().as_ref().clone().into_builder();
    let mut groups = builder.take_row_groups();
    let group = groups.pop().unwrap();
    let mut columns = group.columns().to_vec();
    let Some(ParquetStatistics::Int32(stats)) = columns[column].statistics() else {
        panic!("expected INT32 statistics");
    };
    let (min, max) = (stats.min_opt().copied(), stats.max_opt().copied());
    columns[column] = columns[column]
        .clone()
        .into_builder()
        .set_statistics(ParquetStatistics::int32(min, max, None, None, false))
        .build()
        .unwrap();
    groups.push(
        group
            .into_builder()
            .set_column_metadata(columns)
            .build()
            .unwrap(),
    );
    let metadata = builder.set_row_groups(groups).build();
    let mut output = TrackedWrite::new(file.reopen().unwrap());
    output.write_all(&original).unwrap();
    ParquetMetaDataWriter::new_with_tracked(output, &metadata)
        .finish()
        .unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
    assert!(reader
        .metadata()
        .row_groups()
        .last()
        .unwrap()
        .column(column)
        .statistics()
        .unwrap()
        .null_count_opt()
        .is_none());
}

/// Exercise both initial pruning of a later file and live pruning within one file.
#[tokio::test]
async fn topk_preserves_unknown_null_counts_and_prunes_known_zero_counts() {
    for descending in [false, true] {
        for separate_files in [false, true] {
            for missing_count in [false, true] {
                for filtering in [false, true] {
                    let session = session(3);
                    let sign = if descending { -1 } else { 1 };
                    let first = vec![Some(0), Some(sign), Some(2 * sign)];
                    let later = vec![
                        if missing_count {
                            None
                        } else {
                            Some(102 * sign)
                        },
                        Some(100 * sign),
                        Some(101 * sign),
                    ];
                    let values = if separate_files {
                        vec![first, later]
                    } else {
                        vec![first.into_iter().chain(later).collect()]
                    };
                    let files = values
                        .into_iter()
                        .map(|values| parquet_input(values, &DataType::Int32, &session, 3).0)
                        .collect::<Vec<_>>();
                    if missing_count {
                        omit_last_null_count(files.last().unwrap(), 1);
                    }
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("payload", DataType::Int32, false),
                        Field::new("key", DataType::Int32, true),
                    ]));
                    let scan = parquet_scan(
                        &files.iter().collect::<Vec<_>>(),
                        schema,
                        vec![1],
                        None,
                        false,
                        &session,
                    );
                    let plain = sort(
                        Arc::clone(&scan) as _,
                        1,
                        SortOptions {
                            descending,
                            nulls_first: true,
                        },
                    );
                    let plan: Arc<dyn ExecutionPlan> = if filtering {
                        Arc::new(wrapper(&plain, &session))
                    } else {
                        Arc::new(plain)
                    };
                    let batches = collect(plan, session.task_ctx()).await.unwrap();
                    assert_eq!(keys(&batches), vec![if missing_count { None } else { Some(0) }],
                        "descending={descending}, separate_files={separate_files}, missing_count={missing_count}, filtering={filtering}");
                    if filtering && !missing_count {
                        assert!(scan.metrics().unwrap().output_rows().unwrap() < 6);
                    }
                }
            }
        }
    }
}
