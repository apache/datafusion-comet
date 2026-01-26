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

            let spark_rows: Vec<SparkUnsafeRow> = rows
                .iter()
                .map(|row_data| {
                    let mut spark_row = SparkUnsafeRow::new_with_num_fields(1);
                    row_data.to_spark_row(&mut spark_row);
                    // Mark the struct column as not null
                    spark_row.set_not_null_at(0);
                    spark_row
                })
                .collect();

            let mut row_addresses: Vec<i64> =
                spark_rows.iter().map(|row| row.get_row_addr()).collect();
            let mut row_sizes: Vec<i32> = spark_rows.iter().map(|row| row.get_row_size()).collect();

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

/// Create a schema with nested structs: Struct<Struct<int64 fields>>
fn make_nested_struct_schema(num_fields: usize) -> DataType {
    let inner_fields: Vec<Field> = (0..num_fields)
        .map(|i| Field::new(format!("inner_f{}", i), DataType::Int64, true))
        .collect();
    let inner_struct = DataType::Struct(Fields::from(inner_fields));
    let outer_fields = vec![Field::new("nested", inner_struct, true)];
    DataType::Struct(Fields::from(outer_fields))
}

/// Create a schema with deeply nested structs (3 levels): Struct<Struct<Struct<int64 fields>>>
fn make_deeply_nested_struct_schema(num_fields: usize) -> DataType {
    let inner_fields: Vec<Field> = (0..num_fields)
        .map(|i| Field::new(format!("inner_f{}", i), DataType::Int64, true))
        .collect();
    let inner_struct = DataType::Struct(Fields::from(inner_fields));
    let middle_fields = vec![Field::new("level2", inner_struct, true)];
    let middle_struct = DataType::Struct(Fields::from(middle_fields));
    let outer_fields = vec![Field::new("level1", middle_struct, true)];
    DataType::Struct(Fields::from(outer_fields))
}

/// Calculate row size for nested struct: Struct<Struct<int64 fields>>
fn get_nested_row_size(num_inner_fields: usize) -> usize {
    // Top-level row has 1 column (the outer struct)
    let top_level_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
    let struct_pointer_size = 8;

    // Outer struct has 1 field (the inner struct)
    let outer_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
    let outer_struct_size = outer_bitset_width + 8; // pointer to inner struct

    // Inner struct has num_inner_fields int64 fields
    let inner_bitset_width = SparkUnsafeRow::get_row_bitset_width(num_inner_fields);
    let inner_data_size = num_inner_fields * 8;
    let inner_struct_size = inner_bitset_width + inner_data_size;

    top_level_bitset_width + struct_pointer_size + outer_struct_size + inner_struct_size
}

/// Calculate row size for deeply nested struct: Struct<Struct<Struct<int64 fields>>>
fn get_deeply_nested_row_size(num_inner_fields: usize) -> usize {
    // Top-level row has 1 column (the level1 struct)
    let top_level_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
    let struct_pointer_size = 8;

    // Level 1 struct has 1 field (the level2 struct)
    let level1_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
    let level1_struct_size = level1_bitset_width + 8;

    // Level 2 struct has 1 field (the inner struct)
    let level2_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
    let level2_struct_size = level2_bitset_width + 8;

    // Inner struct has num_inner_fields int64 fields
    let inner_bitset_width = SparkUnsafeRow::get_row_bitset_width(num_inner_fields);
    let inner_data_size = num_inner_fields * 8;
    let inner_struct_size = inner_bitset_width + inner_data_size;

    top_level_bitset_width
        + struct_pointer_size
        + level1_struct_size
        + level2_struct_size
        + inner_struct_size
}

struct NestedRowData {
    data: Vec<u8>,
}

impl NestedRowData {
    fn new(num_inner_fields: usize) -> Self {
        let row_size = get_nested_row_size(num_inner_fields);
        let mut data = vec![0u8; row_size];

        let top_level_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
        let outer_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
        let inner_bitset_width = SparkUnsafeRow::get_row_bitset_width(num_inner_fields);

        // Calculate offsets
        let outer_struct_start = top_level_bitset_width + 8;
        let outer_struct_size = outer_bitset_width + 8;
        let inner_struct_start = outer_struct_start + outer_struct_size;
        let inner_struct_size = inner_bitset_width + num_inner_fields * 8;

        // Write top-level struct pointer (points to outer struct)
        let outer_offset_and_size =
            ((outer_struct_start as i64) << 32) | (outer_struct_size as i64);
        data[top_level_bitset_width..top_level_bitset_width + 8]
            .copy_from_slice(&outer_offset_and_size.to_le_bytes());

        // Write outer struct pointer (points to inner struct)
        // Offset is relative to outer struct start
        let inner_relative_offset = inner_struct_start - outer_struct_start;
        let inner_offset_and_size =
            ((inner_relative_offset as i64) << 32) | (inner_struct_size as i64);
        data[outer_struct_start + outer_bitset_width..outer_struct_start + outer_bitset_width + 8]
            .copy_from_slice(&inner_offset_and_size.to_le_bytes());

        // Fill inner struct with some data
        for i in 0..num_inner_fields {
            let value_offset = inner_struct_start + inner_bitset_width + i * 8;
            let value = (i as i64) * 100;
            data[value_offset..value_offset + 8].copy_from_slice(&value.to_le_bytes());
        }

        NestedRowData { data }
    }

    fn to_spark_row(&self, spark_row: &mut SparkUnsafeRow) {
        spark_row.point_to_slice(&self.data);
    }
}

struct DeeplyNestedRowData {
    data: Vec<u8>,
}

impl DeeplyNestedRowData {
    fn new(num_inner_fields: usize) -> Self {
        let row_size = get_deeply_nested_row_size(num_inner_fields);
        let mut data = vec![0u8; row_size];

        let top_level_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
        let level1_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
        let level2_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
        let inner_bitset_width = SparkUnsafeRow::get_row_bitset_width(num_inner_fields);

        // Calculate offsets
        let level1_struct_start = top_level_bitset_width + 8;
        let level1_struct_size = level1_bitset_width + 8;
        let level2_struct_start = level1_struct_start + level1_struct_size;
        let level2_struct_size = level2_bitset_width + 8;
        let inner_struct_start = level2_struct_start + level2_struct_size;
        let inner_struct_size = inner_bitset_width + num_inner_fields * 8;

        // Write top-level struct pointer (points to level1 struct)
        let level1_offset_and_size =
            ((level1_struct_start as i64) << 32) | (level1_struct_size as i64);
        data[top_level_bitset_width..top_level_bitset_width + 8]
            .copy_from_slice(&level1_offset_and_size.to_le_bytes());

        // Write level1 struct pointer (points to level2 struct)
        let level2_relative_offset = level2_struct_start - level1_struct_start;
        let level2_offset_and_size =
            ((level2_relative_offset as i64) << 32) | (level2_struct_size as i64);
        data[level1_struct_start + level1_bitset_width
            ..level1_struct_start + level1_bitset_width + 8]
            .copy_from_slice(&level2_offset_and_size.to_le_bytes());

        // Write level2 struct pointer (points to inner struct)
        let inner_relative_offset = inner_struct_start - level2_struct_start;
        let inner_offset_and_size =
            ((inner_relative_offset as i64) << 32) | (inner_struct_size as i64);
        data[level2_struct_start + level2_bitset_width
            ..level2_struct_start + level2_bitset_width + 8]
            .copy_from_slice(&inner_offset_and_size.to_le_bytes());

        // Fill inner struct with some data
        for i in 0..num_inner_fields {
            let value_offset = inner_struct_start + inner_bitset_width + i * 8;
            let value = (i as i64) * 100;
            data[value_offset..value_offset + 8].copy_from_slice(&value.to_le_bytes());
        }

        DeeplyNestedRowData { data }
    }

    fn to_spark_row(&self, spark_row: &mut SparkUnsafeRow) {
        spark_row.point_to_slice(&self.data);
    }
}

fn benchmark_nested_struct_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("nested_struct_conversion");

    // Test nested structs with different inner field counts
    for num_fields in [5, 10, 20] {
        for num_rows in [1000, 10000] {
            let schema = vec![make_nested_struct_schema(num_fields)];

            // Create row data
            let rows: Vec<NestedRowData> = (0..num_rows)
                .map(|_| NestedRowData::new(num_fields))
                .collect();

            let spark_rows: Vec<SparkUnsafeRow> = rows
                .iter()
                .map(|row_data| {
                    let mut spark_row = SparkUnsafeRow::new_with_num_fields(1);
                    row_data.to_spark_row(&mut spark_row);
                    spark_row.set_not_null_at(0);
                    spark_row
                })
                .collect();

            let mut row_addresses: Vec<i64> =
                spark_rows.iter().map(|row| row.get_row_addr()).collect();
            let mut row_sizes: Vec<i32> = spark_rows.iter().map(|row| row.get_row_size()).collect();

            let row_address_ptr = row_addresses.as_mut_ptr();
            let row_size_ptr = row_sizes.as_mut_ptr();

            group.bench_with_input(
                BenchmarkId::new(
                    format!("inner_fields_{}", num_fields),
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

            std::mem::drop(spark_rows);
        }
    }

    group.finish();
}

fn benchmark_deeply_nested_struct_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("deeply_nested_struct_conversion");

    // Test deeply nested structs (3 levels) with different inner field counts
    for num_fields in [5, 10, 20] {
        for num_rows in [1000, 10000] {
            let schema = vec![make_deeply_nested_struct_schema(num_fields)];

            // Create row data
            let rows: Vec<DeeplyNestedRowData> = (0..num_rows)
                .map(|_| DeeplyNestedRowData::new(num_fields))
                .collect();

            let spark_rows: Vec<SparkUnsafeRow> = rows
                .iter()
                .map(|row_data| {
                    let mut spark_row = SparkUnsafeRow::new_with_num_fields(1);
                    row_data.to_spark_row(&mut spark_row);
                    spark_row.set_not_null_at(0);
                    spark_row
                })
                .collect();

            let mut row_addresses: Vec<i64> =
                spark_rows.iter().map(|row| row.get_row_addr()).collect();
            let mut row_sizes: Vec<i32> = spark_rows.iter().map(|row| row.get_row_size()).collect();

            let row_address_ptr = row_addresses.as_mut_ptr();
            let row_size_ptr = row_sizes.as_mut_ptr();

            group.bench_with_input(
                BenchmarkId::new(
                    format!("inner_fields_{}", num_fields),
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

            std::mem::drop(spark_rows);
        }
    }

    group.finish();
}

/// Create a schema with a list column: List<Int64>
fn make_list_schema(element_type: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", element_type, true)))
}

/// Calculate row size for a list with the given number of elements.
/// UnsafeRow layout for list: [null bits] [list pointer (offset, size)]
/// List data: [num_elements (8 bytes)] [null bits] [element data]
fn get_list_row_size(num_elements: usize, element_size: usize) -> usize {
    // Top-level row has 1 column (the list)
    let top_level_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
    let list_pointer_size = 8;

    // List header: num_elements (8 bytes) + null bitset
    let list_null_bitset = ((num_elements + 63) / 64) * 8;
    let list_header = 8 + list_null_bitset;
    let list_data_size = num_elements * element_size;

    top_level_bitset_width + list_pointer_size + list_header + list_data_size
}

struct ListRowData {
    data: Vec<u8>,
}

impl ListRowData {
    fn new_int64_list(num_elements: usize) -> Self {
        let row_size = get_list_row_size(num_elements, 8);
        let mut data = vec![0u8; row_size];

        let top_level_bitset_width = SparkUnsafeRow::get_row_bitset_width(1);
        let list_null_bitset = ((num_elements + 63) / 64) * 8;

        // List starts after top-level header + pointer
        let list_offset = top_level_bitset_width + 8;
        let list_size = 8 + list_null_bitset + num_elements * 8;

        // Write list pointer (offset in upper 32 bits, size in lower 32 bits)
        let offset_and_size = ((list_offset as i64) << 32) | (list_size as i64);
        data[top_level_bitset_width..top_level_bitset_width + 8]
            .copy_from_slice(&offset_and_size.to_le_bytes());

        // Write number of elements at list start
        data[list_offset..list_offset + 8].copy_from_slice(&(num_elements as i64).to_le_bytes());

        // Fill list with data (after header)
        let data_start = list_offset + 8 + list_null_bitset;
        for i in 0..num_elements {
            let value_offset = data_start + i * 8;
            let value = (i as i64) * 100;
            data[value_offset..value_offset + 8].copy_from_slice(&value.to_le_bytes());
        }

        ListRowData { data }
    }

    fn to_spark_row(&self, spark_row: &mut SparkUnsafeRow) {
        spark_row.point_to_slice(&self.data);
    }
}

fn benchmark_list_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_conversion");

    // Test with different list sizes and row counts
    for num_elements in [10, 100] {
        for num_rows in [1000, 10000] {
            let schema = vec![make_list_schema(DataType::Int64)];

            // Create row data - each row has a list with num_elements items
            let rows: Vec<ListRowData> = (0..num_rows)
                .map(|_| ListRowData::new_int64_list(num_elements))
                .collect();

            let spark_rows: Vec<SparkUnsafeRow> = rows
                .iter()
                .map(|row_data| {
                    let mut spark_row = SparkUnsafeRow::new_with_num_fields(1);
                    row_data.to_spark_row(&mut spark_row);
                    spark_row.set_not_null_at(0);
                    spark_row
                })
                .collect();

            let mut row_addresses: Vec<i64> =
                spark_rows.iter().map(|row| row.get_row_addr()).collect();
            let mut row_sizes: Vec<i32> = spark_rows.iter().map(|row| row.get_row_size()).collect();

            let row_address_ptr = row_addresses.as_mut_ptr();
            let row_size_ptr = row_sizes.as_mut_ptr();

            group.bench_with_input(
                BenchmarkId::new(
                    format!("elements_{}", num_elements),
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
    targets = benchmark_struct_conversion, benchmark_nested_struct_conversion, benchmark_deeply_nested_struct_conversion, benchmark_list_conversion
}
criterion_main!(benches);
