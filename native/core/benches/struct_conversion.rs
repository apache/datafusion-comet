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

//! Benchmark for struct column processing in native shuffle.
//!
//! This benchmark measures the performance of converting Spark UnsafeRow
//! with struct columns to Arrow arrays.

use arrow::datatypes::{DataType, Field, Fields};
use comet::execution::shuffle::row::{
    process_sorted_row_partition, SparkUnsafeObject, SparkUnsafeRow,
};
use comet::execution::shuffle::CompressionCodec;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use tempfile::Builder;

const BATCH_SIZE: usize = 5000;

/// Create a struct schema with the given number of int64 fields.
fn make_struct_schema(num_fields: usize) -> DataType {
    let fields: Vec<Field> = (0..num_fields)
        .map(|i| Field::new(format!("f{}", i), DataType::Int64, true))
        .collect();
    DataType::Struct(Fields::from(fields))
}

/// Calculate the row size for a struct with the given number of fields.
/// UnsafeRow layout: [null bits] [fixed-length values]
/// For struct: the struct value is stored as offset+size (8 bytes) pointing to nested row
fn get_row_size(num_struct_fields: usize) -> usize {
    // Top-level row has 1 column (the struct)
    let top_level_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
    // Struct pointer (offset + size) is 8 bytes
    let struct_pointer_size = 8;
    // Nested struct row
    let nested_bitset_width = SparkUnsafeRow::get_row_bitset_width(num_struct_fields);
    let nested_data_size = num_struct_fields * 8; // int64 values

    top_level_bitset_width + struct_pointer_size + nested_bitset_width + nested_data_size
}

struct RowData {
    data: Vec<u8>,
}

impl RowData {
    fn new(num_struct_fields: usize) -> Self {
        let row_size = get_row_size(num_struct_fields);
        let mut data = vec![0u8; row_size];

        // Top-level row layout:
        // [null bits for 1 field] [struct pointer (offset, size)]
        let top_level_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);

        // Nested struct starts after top-level row header + pointer
        let nested_offset = top_level_bitset_width + 8;
        let nested_bitset_width = SparkUnsafeRow::get_row_bitset_width(num_struct_fields);
        let nested_size = nested_bitset_width + num_struct_fields * 8;

        // Write struct pointer (offset in upper 32 bits, size in lower 32 bits)
        let offset_and_size = ((nested_offset as i64) << 32) | (nested_size as i64);
        data[top_level_bitset_width..top_level_bitset_width + 8]
            .copy_from_slice(&offset_and_size.to_le_bytes());

        // Fill nested struct with some data
        for i in 0..num_struct_fields {
            let value_offset = nested_offset + nested_bitset_width + i * 8;
            let value = (i as i64) * 100;
            data[value_offset..value_offset + 8].copy_from_slice(&value.to_le_bytes());
        }

        RowData { data }
    }

    fn to_spark_row(&self, spark_row: &mut SparkUnsafeRow) {
        spark_row.point_to_slice(&self.data);
    }
}

fn benchmark_struct_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("struct_conversion");

    // Test with different struct sizes and row counts
    for num_fields in [5, 10, 20] {
        for num_rows in [1000, 10000] {
            let schema = vec![make_struct_schema(num_fields)];

            // Create row data
            let rows: Vec<RowData> = (0..num_rows).map(|_| RowData::new(num_fields)).collect();

            let mut spark_rows: Vec<SparkUnsafeRow> = rows
                .iter()
                .map(|row_data| {
                    let mut spark_row = SparkUnsafeRow::new_with_num_fields(1);
                    row_data.to_spark_row(&mut spark_row);
                    // Mark the struct column as not null
                    spark_row.set_not_null_at(0);
                    spark_row
                })
                .collect();

            let mut row_addresses: Vec<i64> = spark_rows
                .iter()
                .map(|row| row.get_row_addr())
                .collect();
            let mut row_sizes: Vec<i32> = spark_rows
                .iter()
                .map(|row| row.get_row_size())
                .collect();

            let row_address_ptr = row_addresses.as_mut_ptr();
            let row_size_ptr = row_sizes.as_mut_ptr();

            group.bench_with_input(
                BenchmarkId::new(
                    format!("fields_{}", num_fields),
                    format!("rows_{}", num_rows),
                ),
                &(num_rows, &schema),
                |b, (num_rows, schema)| {
                    b.iter(|| {
                        let tempfile = Builder::new().tempfile().unwrap();

                        process_sorted_row_partition(
                            *num_rows,
                            BATCH_SIZE,
                            row_address_ptr,
                            row_size_ptr,
                            schema,
                            tempfile.path().to_str().unwrap().to_string(),
                            1.0,
                            false,
                            0,
                            None,
                            &CompressionCodec::Zstd(1),
                        )
                        .unwrap();
                    });
                },
            );

            // Keep spark_rows alive for the benchmark
            std::mem::drop(spark_rows);
        }
    }

    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = benchmark_struct_conversion
}
criterion_main!(benches);
