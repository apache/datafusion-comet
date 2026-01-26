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

use arrow::datatypes::DataType as ArrowDataType;
use comet::execution::shuffle::row::{
    process_sorted_row_partition, SparkUnsafeObject, SparkUnsafeRow,
};
use comet::execution::shuffle::CompressionCodec;
use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::Builder;

const NUM_ROWS: usize = 10000;
const BATCH_SIZE: usize = 5000;
const NUM_COLS: usize = 100;
const ROW_SIZE: usize = SparkUnsafeRow::get_row_bitset_width(NUM_COLS) + NUM_COLS * 8;

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_array_conversion");

    group.bench_function("row_to_array", |b| {
        let spark_rows = (0..NUM_ROWS)
            .map(|_| {
                let mut spark_row = SparkUnsafeRow::new_with_num_fields(NUM_COLS);
                let mut row = Row::new();

                for i in SparkUnsafeRow::get_row_bitset_width(NUM_COLS)..ROW_SIZE {
                    row.data[i] = i as u8;
                }

                row.to_spark_row(&mut spark_row);

                for i in 0..NUM_COLS {
                    spark_row.set_not_null_at(i);
                }

                spark_row
            })
            .collect::<Vec<_>>();

        let mut row_addresses = spark_rows
            .iter()
            .map(|row| row.get_row_addr())
            .collect::<Vec<_>>();
        let mut row_sizes = spark_rows
            .iter()
            .map(|row| row.get_row_size())
            .collect::<Vec<_>>();

        let row_address_ptr = row_addresses.as_mut_ptr();
        let row_size_ptr = row_sizes.as_mut_ptr();
        let schema = vec![ArrowDataType::Int64; NUM_COLS];

        b.iter(|| {
            let tempfile = Builder::new().tempfile().unwrap();

            process_sorted_row_partition(
                NUM_ROWS,
                BATCH_SIZE,
                row_address_ptr,
                row_size_ptr,
                &schema,
                tempfile.path().to_str().unwrap().to_string(),
                1.0,
                false,
                0,
                None,
                &CompressionCodec::Zstd(1),
            )
            .unwrap();
        });
    });
}

struct Row {
    data: Box<[u8; ROW_SIZE]>,
}

impl Row {
    pub fn new() -> Self {
        Row {
            data: Box::new([0u8; ROW_SIZE]),
        }
    }

    pub fn to_spark_row(&self, spark_row: &mut SparkUnsafeRow) {
        spark_row.point_to_slice(self.data.as_ref());
    }
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = benchmark
}
criterion_main!(benches);
