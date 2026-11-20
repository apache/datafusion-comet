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

//! Benchmarks for JVM shuffle row-to-columnar conversion.
//!
//! Measures `process_sorted_row_partition()` performance for converting Spark
//! UnsafeRow data to Arrow arrays, covering primitive, struct (flat/nested),
//! list, and map types, and compares it with `process_sorted_partitions()` on
//! one sorted spill of the sort-based writer.

use arrow::datatypes::{DataType as ArrowDataType, Field, Fields};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_comet_shuffle::spark_unsafe::row::{
    process_sorted_partitions, process_sorted_row_partition, SparkUnsafeRow,
};
use datafusion_comet_shuffle::spark_unsafe::unsafe_object::SparkUnsafeObject;
use datafusion_comet_shuffle::CompressionCodec;
use std::sync::Arc;
use tempfile::Builder;

const BATCH_SIZE: usize = 5000;

/// Size of an Int64 value in bytes.
const INT64_SIZE: usize = 8;

/// Size of a pointer in Spark's UnsafeRow format. Encodes a 32-bit offset
/// (upper bits) and 32-bit size (lower bits) — always 8 bytes regardless of
/// hardware architecture.
const UNSAFE_ROW_POINTER_SIZE: usize = 8;

/// Size of the element-count field in UnsafeRow array/map headers.
const ARRAY_HEADER_SIZE: usize = 8;

// ─── UnsafeRow helpers ──────────────────────────────────────────────────────

/// Write an UnsafeRow offset+size pointer at `pos` in `data`.
fn write_pointer(data: &mut [u8], pos: usize, offset: usize, size: usize) {
    let value = ((offset as i64) << 32) | (size as i64);
    data[pos..pos + UNSAFE_ROW_POINTER_SIZE].copy_from_slice(&value.to_le_bytes());
}

/// Byte size of a null-bitset for `n` elements (64-bit words, rounded up).
fn null_bitset_size(n: usize) -> usize {
    n.div_ceil(64) * 8
}

// ─── Schema builders ────────────────────────────────────────────────────────

/// Create a struct schema with `depth` nesting levels and `num_leaf_fields`
/// Int64 leaf fields.
///
/// - depth=1: `Struct<f0: Int64, f1: Int64, …>`
/// - depth=2: `Struct<nested: Struct<f0: Int64, …>>`
/// - depth=3: `Struct<nested: Struct<nested: Struct<f0: Int64, …>>>`
fn make_struct_schema(depth: usize, num_leaf_fields: usize) -> ArrowDataType {
    let leaf_fields: Vec<Field> = (0..num_leaf_fields)
        .map(|i| Field::new(format!("f{i}"), ArrowDataType::Int64, true))
        .collect();
    let mut dt = ArrowDataType::Struct(Fields::from(leaf_fields));
    for _ in 0..depth - 1 {
        dt = ArrowDataType::Struct(Fields::from(vec![Field::new("nested", dt, true)]));
    }
    dt
}

fn make_list_schema() -> ArrowDataType {
    ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int64, true)))
}

fn make_map_schema() -> ArrowDataType {
    let entries = Field::new(
        "entries",
        ArrowDataType::Struct(Fields::from(vec![
            Field::new("key", ArrowDataType::Int64, false),
            Field::new("value", ArrowDataType::Int64, true),
        ])),
        false,
    );
    ArrowDataType::Map(Arc::new(entries), false)
}

// ─── Row data builders ──────────────────────────────────────────────────────

/// Build a binary UnsafeRow containing a struct column with `depth` nesting
/// levels and `num_leaf_fields` Int64 fields at the innermost level.
fn build_struct_row(depth: usize, num_leaf_fields: usize) -> Vec<u8> {
    let top_bitset = SparkUnsafeRow::get_row_bitset_width(1);
    let inter_bitset = SparkUnsafeRow::get_row_bitset_width(1);
    let leaf_bitset = SparkUnsafeRow::get_row_bitset_width(num_leaf_fields);

    let inter_level_size = inter_bitset + UNSAFE_ROW_POINTER_SIZE;
    let leaf_level_size = leaf_bitset + num_leaf_fields * INT64_SIZE;

    let total =
        top_bitset + UNSAFE_ROW_POINTER_SIZE + (depth - 1) * inter_level_size + leaf_level_size;
    let mut data = vec![0u8; total];

    // Absolute start position of each struct level in the buffer
    let mut struct_starts = Vec::with_capacity(depth);
    let mut pos = top_bitset + UNSAFE_ROW_POINTER_SIZE;
    for level in 0..depth {
        struct_starts.push(pos);
        if level < depth - 1 {
            pos += inter_level_size;
        }
    }

    // Top-level pointer → first struct (absolute offset from row start)
    let first_size = if depth == 1 {
        leaf_level_size
    } else {
        inter_level_size
    };
    write_pointer(&mut data, top_bitset, struct_starts[0], first_size);

    // Intermediate struct pointers (offsets relative to their own struct start)
    for level in 0..depth - 1 {
        let next_size = if level + 1 == depth - 1 {
            leaf_level_size
        } else {
            inter_level_size
        };
        write_pointer(
            &mut data,
            struct_starts[level] + inter_bitset,
            struct_starts[level + 1] - struct_starts[level],
            next_size,
        );
    }

    // Fill leaf struct with sample data
    let leaf_start = *struct_starts.last().unwrap();
    for i in 0..num_leaf_fields {
        let off = leaf_start + leaf_bitset + i * INT64_SIZE;
        data[off..off + INT64_SIZE].copy_from_slice(&((i as i64) * 100).to_le_bytes());
    }

    data
}

/// Build a binary UnsafeRow containing a `List<Int64>` column.
fn build_list_row(num_elements: usize) -> Vec<u8> {
    let top_bitset = SparkUnsafeRow::get_row_bitset_width(1);
    let elem_null_bitset = null_bitset_size(num_elements);
    let list_size = ARRAY_HEADER_SIZE + elem_null_bitset + num_elements * INT64_SIZE;
    let total = top_bitset + UNSAFE_ROW_POINTER_SIZE + list_size;
    let mut data = vec![0u8; total];

    let list_offset = top_bitset + UNSAFE_ROW_POINTER_SIZE;
    write_pointer(&mut data, top_bitset, list_offset, list_size);

    // Element count
    data[list_offset..list_offset + ARRAY_HEADER_SIZE]
        .copy_from_slice(&(num_elements as i64).to_le_bytes());

    // Element values
    let data_start = list_offset + ARRAY_HEADER_SIZE + elem_null_bitset;
    for i in 0..num_elements {
        let off = data_start + i * INT64_SIZE;
        data[off..off + INT64_SIZE].copy_from_slice(&((i as i64) * 100).to_le_bytes());
    }

    data
}

/// Build a binary UnsafeRow containing a `Map<Int64, Int64>` column.
fn build_map_row(num_entries: usize) -> Vec<u8> {
    let top_bitset = SparkUnsafeRow::get_row_bitset_width(1);
    let entry_null_bitset = null_bitset_size(num_entries);
    let array_size = ARRAY_HEADER_SIZE + entry_null_bitset + num_entries * INT64_SIZE;
    // Map layout: [key_array_size header] [key_array] [value_array]
    let map_size = ARRAY_HEADER_SIZE + 2 * array_size;
    let total = top_bitset + UNSAFE_ROW_POINTER_SIZE + map_size;
    let mut data = vec![0u8; total];

    let map_offset = top_bitset + UNSAFE_ROW_POINTER_SIZE;
    write_pointer(&mut data, top_bitset, map_offset, map_size);

    // Key array size header
    data[map_offset..map_offset + ARRAY_HEADER_SIZE]
        .copy_from_slice(&(array_size as i64).to_le_bytes());

    // Key array: [element count] [null bitset] [data]
    let key_offset = map_offset + ARRAY_HEADER_SIZE;
    data[key_offset..key_offset + ARRAY_HEADER_SIZE]
        .copy_from_slice(&(num_entries as i64).to_le_bytes());
    let key_data = key_offset + ARRAY_HEADER_SIZE + entry_null_bitset;
    for i in 0..num_entries {
        let off = key_data + i * INT64_SIZE;
        data[off..off + INT64_SIZE].copy_from_slice(&(i as i64).to_le_bytes());
    }

    // Value array: [element count] [null bitset] [data]
    let val_offset = key_offset + array_size;
    data[val_offset..val_offset + ARRAY_HEADER_SIZE]
        .copy_from_slice(&(num_entries as i64).to_le_bytes());
    let val_data = val_offset + ARRAY_HEADER_SIZE + entry_null_bitset;
    for i in 0..num_entries {
        let off = val_data + i * INT64_SIZE;
        data[off..off + INT64_SIZE].copy_from_slice(&((i as i64) * 100).to_le_bytes());
    }

    data
}

// ─── Benchmark runner ───────────────────────────────────────────────────────

/// Common benchmark harness: wraps raw row bytes in SparkUnsafeRow and runs
/// `process_sorted_row_partition` under Criterion.
fn run_benchmark(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    name: &str,
    param: &str,
    schema: &[ArrowDataType],
    rows: &[Vec<u8>],
    num_top_level_fields: usize,
) {
    let num_rows = rows.len();

    let spark_rows: Vec<SparkUnsafeRow> = rows
        .iter()
        .map(|data| {
            let mut row = SparkUnsafeRow::new_with_num_fields(num_top_level_fields);
            row.point_to_slice(data);
            for i in 0..num_top_level_fields {
                row.set_not_null_at(i);
            }
            row
        })
        .collect();

    let mut addrs: Vec<i64> = spark_rows.iter().map(|r| r.get_row_addr()).collect();
    let mut sizes: Vec<i32> = spark_rows.iter().map(|r| r.get_row_size()).collect();
    let addr_ptr = addrs.as_mut_ptr();
    let size_ptr = sizes.as_mut_ptr();

    group.bench_with_input(BenchmarkId::new(name, param), &num_rows, |b, &n| {
        b.iter(|| {
            let tmp = Builder::new().tempfile().unwrap();
            process_sorted_row_partition(
                n,
                BATCH_SIZE,
                addr_ptr,
                size_ptr,
                schema,
                tmp.path().to_str().unwrap().to_string(),
                1.0,
                false,
                0,
                None,
                &CompressionCodec::Zstd(1),
                false,
            )
            .unwrap();
        });
    });

    drop(spark_rows);
}

// ─── Benchmarks ─────────────────────────────────────────────────────────────

/// 100 primitive Int64 columns — baseline without complex-type overhead.
fn benchmark_primitive_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitive_columns");
    const NUM_COLS: usize = 100;
    let bitset = SparkUnsafeRow::get_row_bitset_width(NUM_COLS);
    let row_size = bitset + NUM_COLS * INT64_SIZE;

    for num_rows in [1000, 10000] {
        let schema = vec![ArrowDataType::Int64; NUM_COLS];
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|_| {
                let mut data = vec![0u8; row_size];
                for (i, byte) in data.iter_mut().enumerate().take(row_size).skip(bitset) {
                    *byte = i as u8;
                }
                data
            })
            .collect();

        run_benchmark(
            &mut group,
            "cols_100",
            &format!("rows_{num_rows}"),
            &schema,
            &rows,
            NUM_COLS,
        );
    }

    group.finish();
}

/// Struct columns at varying nesting depths (1 = flat, 2 = nested, 3 = deeply nested).
fn benchmark_struct_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("struct_conversion");

    for (depth, label) in [(1, "flat"), (2, "nested"), (3, "deeply_nested")] {
        for num_fields in [5, 10, 20] {
            for num_rows in [1000, 10000] {
                let schema = vec![make_struct_schema(depth, num_fields)];
                let rows: Vec<Vec<u8>> = (0..num_rows)
                    .map(|_| build_struct_row(depth, num_fields))
                    .collect();

                run_benchmark(
                    &mut group,
                    &format!("{label}_fields_{num_fields}"),
                    &format!("rows_{num_rows}"),
                    &schema,
                    &rows,
                    1,
                );
            }
        }
    }

    group.finish();
}

/// List<Int64> columns with varying element counts.
fn benchmark_list_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_conversion");

    for num_elements in [10, 100] {
        for num_rows in [1000, 10000] {
            let schema = vec![make_list_schema()];
            let rows: Vec<Vec<u8>> = (0..num_rows)
                .map(|_| build_list_row(num_elements))
                .collect();

            run_benchmark(
                &mut group,
                &format!("elements_{num_elements}"),
                &format!("rows_{num_rows}"),
                &schema,
                &rows,
                1,
            );
        }
    }

    group.finish();
}

/// Map<Int64, Int64> columns with varying entry counts.
fn benchmark_map_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_conversion");

    for num_entries in [10, 100] {
        for num_rows in [1000, 10000] {
            let schema = vec![make_map_schema()];
            let rows: Vec<Vec<u8>> = (0..num_rows).map(|_| build_map_row(num_entries)).collect();

            run_benchmark(
                &mut group,
                &format!("entries_{num_entries}"),
                &format!("rows_{num_rows}"),
                &schema,
                &rows,
                1,
            );
        }
    }

    group.finish();
}

// ─── Sorted spill ───────────────────────────────────────────────────────────

/// Rows in one spill of the sort-based JVM shuffle writer.
const SPILL_ROWS: usize = 100_000;

/// Defaults of `spark.comet.shuffle.jvm.batchSize` and
/// `spark.comet.shuffle.jvm.preferDictionary.ratio`.
const JVM_SHUFFLE_BATCH_SIZE: usize = 8192;
const JVM_SHUFFLE_PREFER_DICTIONARY_RATIO: f64 = 10.0;

/// Spark's default shuffle checksum, `spark.shuffle.checksum.algorithm=ADLER32`.
const ADLER32: i32 = 1;

/// Bytes Spark's sorter stores before each record for its length (`UnsafeAlignedOffset`).
const UAO_SIZE: usize = 4;

/// Bytes `UnsafeRowSerializer` writes before each row for its size.
const SERIALIZED_ROW_SIZE_BYTES: usize = 4;

fn spill_schema() -> Vec<ArrowDataType> {
    vec![
        ArrowDataType::Int64,
        ArrowDataType::Int32,
        ArrowDataType::Float64,
        ArrowDataType::Utf8,
        ArrowDataType::Date32,
    ]
}

/// One spill of `SPILL_ROWS` rows of `spill_schema`, laid out in a single page the way
/// `SpillSorter.insertRecord` stores records, and sorted by partition.
struct SortedSpill {
    /// Keeps the rows alive: `u64` elements keep every row 8-byte aligned, as in a Spark page.
    page: Vec<u64>,
    /// The packed record pointers, sorted by partition, as the sorter holds them.
    record_pointers: Vec<i64>,
    /// Address and size of each row, grouped by partition: the arrays the JVM builds for each
    /// call when it writes the spill one partition at a time.
    partitions: Vec<(Vec<i64>, Vec<i32>)>,
}

fn build_sorted_spill(num_partitions: usize) -> SortedSpill {
    let schema = spill_schema();
    let bitset = SparkUnsafeRow::get_row_bitset_width(schema.len());
    let fixed = bitset + schema.len() * UNSAFE_ROW_POINTER_SIZE;

    let mut bytes: Vec<u8> = Vec::new();
    let mut records: Vec<(usize, usize, usize)> = Vec::with_capacity(SPILL_ROWS);
    for i in 0..SPILL_ROWS {
        // A low-cardinality string column, padded to a word as UnsafeRow pads variable data.
        let s = format!("status-{:03}", i % 100);
        let row_size = fixed + s.len().div_ceil(8) * 8;
        let mut row = vec![0u8; row_size];
        let slot = |field: usize| bitset + field * UNSAFE_ROW_POINTER_SIZE;
        row[slot(0)..slot(0) + 8].copy_from_slice(&(i as i64).to_le_bytes());
        row[slot(1)..slot(1) + 4].copy_from_slice(&((i % 1000) as i32).to_le_bytes());
        row[slot(2)..slot(2) + 8].copy_from_slice(&(i as f64 * 0.5).to_le_bytes());
        write_pointer(&mut row, slot(3), fixed, s.len());
        row[fixed..fixed + s.len()].copy_from_slice(s.as_bytes());
        row[slot(4)..slot(4) + 4].copy_from_slice(&((i % 20000) as i32).to_le_bytes());

        let record_offset = bytes.len();
        let record_len = SERIALIZED_ROW_SIZE_BYTES + row_size;
        bytes.extend_from_slice(&(record_len as u32).to_ne_bytes());
        bytes.extend_from_slice(&(row_size as u32).to_be_bytes());
        bytes.extend_from_slice(&row);

        // Spread rows over the partitions the way a hash partitioner would.
        let partition =
            ((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize % num_partitions;
        records.push((partition, record_offset, row_size));
    }
    // A stable sort keeps each partition's rows in page order, like sorting packed pointers.
    records.sort_by_key(|&(partition, _, _)| partition);

    let mut page = vec![0u64; bytes.len().div_ceil(8)];
    // SAFETY: `page` holds at least `bytes.len()` bytes.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), page.as_mut_ptr() as *mut u8, bytes.len());
    }
    let page_addr = page.as_ptr() as i64;

    let partitions = records
        .chunk_by(|a, b| a.0 == b.0)
        .map(|run| {
            run.iter()
                .map(|&(_, record_offset, row_size)| {
                    let row_offset = record_offset + UAO_SIZE + SERIALIZED_ROW_SIZE_BYTES;
                    (page_addr + row_offset as i64, row_size as i32)
                })
                .unzip()
        })
        .collect();
    // Every record is in page 0, so a pointer is the partition id and the offset in the page.
    let record_pointers = records
        .iter()
        .map(|&(partition, record_offset, _)| ((partition as i64) << 40) | record_offset as i64)
        .collect();

    SortedSpill {
        page,
        record_pointers,
        partitions,
    }
}

/// Writes one sorted spill the two ways the JVM shuffle writers call native code, at partition
/// counts where a partition gets 1000, 100 and 10 of the spill's rows: one call per partition,
/// with the partition's row addresses and sizes, and one call for the whole spill, with the
/// sorter's packed record pointers.
fn benchmark_sorted_spill(c: &mut Criterion) {
    let mut group = c.benchmark_group("sorted_spill");
    let schema = spill_schema();
    let codec = CompressionCodec::Lz4Frame;

    for num_partitions in [100, 1000, 10000] {
        let spill = build_sorted_spill(num_partitions);

        group.bench_function(
            BenchmarkId::new("per_partition", format!("partitions_{num_partitions}")),
            |b| {
                b.iter(|| {
                    let tmp = Builder::new().tempfile().unwrap();
                    let path = tmp.path().to_str().unwrap().to_string();
                    for (addrs, sizes) in &spill.partitions {
                        process_sorted_row_partition(
                            addrs.len(),
                            JVM_SHUFFLE_BATCH_SIZE,
                            addrs.as_ptr() as *mut i64,
                            sizes.as_ptr() as *mut i32,
                            &schema,
                            path.clone(),
                            JVM_SHUFFLE_PREFER_DICTIONARY_RATIO,
                            true,
                            ADLER32,
                            None,
                            &codec,
                            true,
                        )
                        .unwrap();
                    }
                });
            },
        );

        let page_addresses = [spill.page.as_ptr() as i64];
        group.bench_function(
            BenchmarkId::new("per_spill", format!("partitions_{num_partitions}")),
            |b| {
                b.iter(|| {
                    let tmp = Builder::new().tempfile().unwrap();
                    let mut checksums = vec![i64::MIN; num_partitions];
                    let mut lengths = vec![0_i64; num_partitions];
                    unsafe {
                        process_sorted_partitions(
                            &spill.record_pointers,
                            &page_addresses,
                            UAO_SIZE,
                            &schema,
                            tmp.path().to_str().unwrap(),
                            JVM_SHUFFLE_PREFER_DICTIONARY_RATIO,
                            JVM_SHUFFLE_BATCH_SIZE,
                            Some(ADLER32),
                            &mut checksums,
                            &mut lengths,
                            &codec,
                            true,
                        )
                    }
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = benchmark_primitive_columns,
              benchmark_struct_conversion,
              benchmark_list_conversion,
              benchmark_map_conversion,
              benchmark_sorted_spill
}
criterion_main!(benches);
