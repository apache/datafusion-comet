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
//! UnsafeRow data to Arrow arrays, covering primitive, string (plain and
//! dictionary-encoded), struct (flat/nested), list, and map (Int64 and Utf8
//! keys) types, primitive and map columns holding nulls, and compares it with
//! `process_sorted_partitions()` on one sorted spill of the sort-based writer.

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::{cast, concat};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Int64Type};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_comet_shuffle::spark_unsafe::row::{
    process_sorted_partitions, process_sorted_row_partition, SparkUnsafeRow,
};
use datafusion_comet_shuffle::spark_unsafe::unsafe_object::SparkUnsafeObject;
use datafusion_comet_shuffle::{read_ipc_compressed, CompressionCodec};
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

/// Set the null bit of element `index` in the null bitset that starts at `bitset_start`. The
/// bitset is little-endian 64-bit words, so bit `index` is bit `index % 8` of byte `index / 8`.
fn set_null_bit(data: &mut [u8], bitset_start: usize, index: usize) {
    data[bitset_start + index / 8] |= 1 << (index % 8);
}

/// Whether element `index` of row `row` is null in a fixture with `null_fraction` nulls. A hash
/// rather than a stride, so the nulls neither line up in one column nor recur every few rows.
fn is_null(row: usize, index: usize, null_fraction: f64) -> bool {
    let mut h = (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (index as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    h ^= h >> 31;
    h = h.wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^= h >> 29;
    h % 1000 < (null_fraction * 1000.0) as u64
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

fn make_utf8_map_schema() -> ArrowDataType {
    let entries = Field::new(
        "entries",
        ArrowDataType::Struct(Fields::from(vec![
            Field::new("key", ArrowDataType::Utf8, false),
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

/// Build a binary UnsafeRow containing a `Map<Int64, Int64>` column. Keys run from `row` and
/// values from `row * 100`, and each value is null with probability `value_null_fraction`: its
/// bit is set in the value array's null bitset and its slot left zero, as Spark writes a null
/// element. Keys are never null, as Spark requires.
fn build_map_row(num_entries: usize, row: usize, value_null_fraction: f64) -> Vec<u8> {
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
        data[off..off + INT64_SIZE].copy_from_slice(&((row + i) as i64).to_le_bytes());
    }

    // Value array: [element count] [null bitset] [data]
    let val_offset = key_offset + array_size;
    data[val_offset..val_offset + ARRAY_HEADER_SIZE]
        .copy_from_slice(&(num_entries as i64).to_le_bytes());
    let val_data = val_offset + ARRAY_HEADER_SIZE + entry_null_bitset;
    for i in 0..num_entries {
        if is_null(row, i, value_null_fraction) {
            set_null_bit(&mut data, val_offset + ARRAY_HEADER_SIZE, i);
            continue;
        }
        let off = val_data + i * INT64_SIZE;
        data[off..off + INT64_SIZE].copy_from_slice(&(((row + i) as i64) * 100).to_le_bytes());
    }

    data
}

/// Build a binary UnsafeRow of `num_cols` Int64 fields with values running from
/// `row * num_cols`. Each field is null with probability `null_fraction`: its bit is set in the
/// row's null bitset and its slot left zero, as Spark's `UnsafeRowWriter.setNullAt` leaves it.
fn build_primitive_row(row: usize, num_cols: usize, null_fraction: f64) -> Vec<u8> {
    let bitset = SparkUnsafeRow::get_row_bitset_width(num_cols);
    let mut data = vec![0u8; bitset + num_cols * INT64_SIZE];
    for col in 0..num_cols {
        if is_null(row, col, null_fraction) {
            set_null_bit(&mut data, 0, col);
            continue;
        }
        let off = bitset + col * INT64_SIZE;
        data[off..off + INT64_SIZE].copy_from_slice(&((row * num_cols + col) as i64).to_le_bytes());
    }
    data
}

/// Build a binary UnsafeRow of Utf8 fields holding `values`.
///
/// Each field's fixed-length slot holds an offset+size pointer, the offset counted from the
/// row start. The bytes go in the variable-length region after the slots, each value starting
/// on an 8-byte boundary, as Spark's `UnsafeRowWriter` pads them.
fn build_string_row(values: &[String]) -> Vec<u8> {
    let bitset = SparkUnsafeRow::get_row_bitset_width(values.len());
    let fixed_size = bitset + values.len() * UNSAFE_ROW_POINTER_SIZE;
    let variable_size: usize = values.iter().map(|v| v.len().next_multiple_of(8)).sum();
    let mut data = vec![0u8; fixed_size + variable_size];

    let mut offset = fixed_size;
    for (i, value) in values.iter().enumerate() {
        write_pointer(
            &mut data,
            bitset + i * UNSAFE_ROW_POINTER_SIZE,
            offset,
            value.len(),
        );
        data[offset..offset + value.len()].copy_from_slice(value.as_bytes());
        offset += value.len().next_multiple_of(8);
    }

    data
}

/// Build a binary UnsafeRow containing a `Map<Utf8, Int64>` column.
///
/// The map is laid out as `build_map_row` lays it out, except that the key array holds strings.
/// A string element of an UnsafeArrayData takes an 8-byte slot holding an offset+size pointer,
/// the offset counted from the start of that array (not of the row), and its bytes go after the
/// slots, each starting on an 8-byte boundary. So the key array is
/// `[count] [null bitset] [slots] [bytes]`, and its size, which the map header records, covers
/// the bytes too.
fn build_utf8_map_row(keys: &[String], values: &[i64]) -> Vec<u8> {
    assert_eq!(keys.len(), values.len());
    let num_entries = keys.len();
    let top_bitset = SparkUnsafeRow::get_row_bitset_width(1);
    let entry_null_bitset = null_bitset_size(num_entries);
    let key_slots_size = ARRAY_HEADER_SIZE + entry_null_bitset + num_entries * INT64_SIZE;
    let key_array_size = key_slots_size
        + keys
            .iter()
            .map(|k| k.len().next_multiple_of(8))
            .sum::<usize>();
    let value_array_size = ARRAY_HEADER_SIZE + entry_null_bitset + num_entries * INT64_SIZE;
    let map_size = ARRAY_HEADER_SIZE + key_array_size + value_array_size;
    let total = top_bitset + UNSAFE_ROW_POINTER_SIZE + map_size;
    let mut data = vec![0u8; total];

    let map_offset = top_bitset + UNSAFE_ROW_POINTER_SIZE;
    write_pointer(&mut data, top_bitset, map_offset, map_size);

    // Key array size header
    data[map_offset..map_offset + ARRAY_HEADER_SIZE]
        .copy_from_slice(&(key_array_size as i64).to_le_bytes());

    // Key array: [element count] [null bitset] [offset+size slots] [string bytes]
    let key_offset = map_offset + ARRAY_HEADER_SIZE;
    data[key_offset..key_offset + ARRAY_HEADER_SIZE]
        .copy_from_slice(&(num_entries as i64).to_le_bytes());
    let key_slots = key_offset + ARRAY_HEADER_SIZE + entry_null_bitset;
    let mut bytes_offset = key_slots_size;
    for (i, key) in keys.iter().enumerate() {
        write_pointer(
            &mut data,
            key_slots + i * UNSAFE_ROW_POINTER_SIZE,
            bytes_offset,
            key.len(),
        );
        let start = key_offset + bytes_offset;
        data[start..start + key.len()].copy_from_slice(key.as_bytes());
        bytes_offset += key.len().next_multiple_of(8);
    }

    // Value array: [element count] [null bitset] [data]
    let val_offset = key_offset + key_array_size;
    data[val_offset..val_offset + ARRAY_HEADER_SIZE]
        .copy_from_slice(&(num_entries as i64).to_le_bytes());
    let val_data = val_offset + ARRAY_HEADER_SIZE + entry_null_bitset;
    for (i, value) in values.iter().enumerate() {
        let off = val_data + i * INT64_SIZE;
        data[off..off + INT64_SIZE].copy_from_slice(&value.to_le_bytes());
    }

    data
}

// ─── Benchmark runner ───────────────────────────────────────────────────────

/// Common benchmark harness: wraps raw row bytes in SparkUnsafeRow and runs
/// `process_sorted_row_partition` under Criterion. `prefer_dictionary_ratio` is passed
/// through: at 1.0 or below strings are built plain, above it they go through a dictionary
/// builder and stay dictionary-encoded when a batch repeats each value more than that often.
fn run_benchmark(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    name: &str,
    param: &str,
    schema: &[ArrowDataType],
    rows: &[Vec<u8>],
    num_top_level_fields: usize,
    prefer_dictionary_ratio: f64,
) {
    let num_rows = rows.len();

    // The null bits are the fixture's own. Every builder starts from a zeroed bitset, and the
    // nullable fixtures set the bits of their null fields, which clearing them here would undo.
    let spark_rows: Vec<SparkUnsafeRow> = rows
        .iter()
        .map(|data| {
            let mut row = SparkUnsafeRow::new_with_num_fields(num_top_level_fields);
            row.point_to_slice(data);
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
                prefer_dictionary_ratio,
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

/// Converts `rows` through `process_sorted_row_partition`, the path the benchmarks time, and
/// decodes the blocks it writes. A hand-built fixture is checked with this against the values it
/// was built from before it is timed: a malformed row would otherwise be converted without
/// complaint, and the benchmark would time garbage.
fn convert_and_read_back(
    schema: &[ArrowDataType],
    rows: &[Vec<u8>],
    prefer_dictionary_ratio: f64,
) -> RecordBatch {
    let mut addrs: Vec<i64> = rows.iter().map(|row| row.as_ptr() as i64).collect();
    let mut sizes: Vec<i32> = rows.iter().map(|row| row.len() as i32).collect();
    let tmp = Builder::new().tempfile().unwrap();
    process_sorted_row_partition(
        rows.len(),
        BATCH_SIZE,
        addrs.as_mut_ptr(),
        sizes.as_mut_ptr(),
        schema,
        tmp.path().to_str().unwrap().to_string(),
        prefer_dictionary_ratio,
        false,
        0,
        None,
        &CompressionCodec::Zstd(1),
        false,
    )
    .unwrap();

    // Each block is an 8-byte length of the rest, an 8-byte field count, then the codec tag
    // and IPC stream that `read_ipc_compressed` takes.
    let bytes = std::fs::read(tmp.path()).unwrap();
    let mut batches = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let len = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        batches.push(read_ipc_compressed(&bytes[pos + 16..pos + 8 + len]).unwrap());
        pos += 8 + len;
    }
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        rows.len()
    );
    let columns = (0..schema.len())
        .map(|i| {
            let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(i).as_ref()).collect();
            concat(&arrays).unwrap()
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(batches[0].schema(), columns).unwrap()
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
            1.0,
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
                    1.0,
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
                1.0,
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
            let rows: Vec<Vec<u8>> = (0..num_rows)
                .map(|_| build_map_row(num_entries, 0, 0.0))
                .collect();

            run_benchmark(
                &mut group,
                &format!("entries_{num_entries}"),
                &format!("rows_{num_rows}"),
                &schema,
                &rows,
                1,
                1.0,
            );
        }
    }

    group.finish();
}

/// Utf8 columns, plain and dictionary-encoded.
///
/// Strings are read from the variable-length region through an offset+size pointer, unlike the
/// fixed-width values of the cases above, and `prefer_dictionary_ratio` picks between two
/// builders for them:
///
/// - `plain`: ratio 1.0, so every value is appended to a `StringBuilder`.
/// - `dictionary`: ratio 10.0, Comet's default, over low-cardinality values (16 distinct per
///   column), so the column stays `Dictionary(Int32, Utf8)`.
/// - `dictionary_fallback`: ratio 10.0 over the plain case's distinct values, so the column is
///   built as a dictionary and then cast back to plain. This is what a high-cardinality string
///   column costs under the default ratio.
///
/// Every row is built separately, with values that vary in length so the 8-byte padding of the
/// variable-length region varies too. Each fixture is converted and read back once, and checked
/// against the values it was built from, before it is timed.
fn benchmark_string_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_conversion");
    const NUM_COLS: usize = 10;

    for (label, low_cardinality, prefer_dictionary_ratio) in [
        ("plain", false, 1.0),
        ("dictionary", true, 10.0),
        ("dictionary_fallback", false, 10.0),
    ] {
        for num_rows in [1000, 10000] {
            let schema = vec![ArrowDataType::Utf8; NUM_COLS];
            let values: Vec<Vec<String>> = (0..num_rows)
                .map(|row| {
                    (0..NUM_COLS)
                        .map(|col| {
                            if low_cardinality {
                                format!("category_{}", (row + col) % 16)
                            } else {
                                format!("value {} of column {col}", row * 7919 % 100_003)
                            }
                        })
                        .collect()
                })
                .collect();
            let rows: Vec<Vec<u8>> = values.iter().map(|row| build_string_row(row)).collect();

            let batch = convert_and_read_back(&schema, &rows, prefer_dictionary_ratio);
            for col in 0..NUM_COLS {
                let column = batch.column(col);
                let expected_type = if low_cardinality {
                    ArrowDataType::Dictionary(
                        Box::new(ArrowDataType::Int32),
                        Box::new(ArrowDataType::Utf8),
                    )
                } else {
                    ArrowDataType::Utf8
                };
                assert_eq!(column.data_type(), &expected_type, "{label} column {col}");
                let column = cast(column, &ArrowDataType::Utf8).unwrap();
                let strings = column.as_string::<i32>();
                assert_eq!(strings.null_count(), 0);
                for (row, expected) in values.iter().enumerate() {
                    assert_eq!(strings.value(row), expected[col], "{label} row {row}");
                }
            }

            run_benchmark(
                &mut group,
                &format!("{label}_cols_{NUM_COLS}"),
                &format!("rows_{num_rows}"),
                &schema,
                &rows,
                NUM_COLS,
                prefer_dictionary_ratio,
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

/// Map<Utf8, Int64> columns, at the entry counts of `map_conversion` so the two can be compared.
///
/// Keys go through the string path of the array reader: each one is read through an
/// offset+size slot relative to the key array and appended to a `StringBuilder` one at a time,
/// where Int64 keys are copied out in one slice. Keys are distinct within a map and vary from
/// row to row, in length too, and values are distinct per row. Each fixture is converted and
/// read back once, and checked against the entries it was built from, before it is timed.
fn benchmark_utf8_map_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_utf8_conversion");

    for num_entries in [10, 100] {
        for num_rows in [1000, 10000] {
            let schema = vec![make_utf8_map_schema()];
            let entries: Vec<(Vec<String>, Vec<i64>)> = (0..num_rows)
                .map(|row| {
                    (0..num_entries)
                        .map(|i| (format!("key_{i}_{}", row % 97), (row * 100 + i) as i64))
                        .unzip()
                })
                .collect();
            let rows: Vec<Vec<u8>> = entries
                .iter()
                .map(|(keys, values)| build_utf8_map_row(keys, values))
                .collect();

            let batch = convert_and_read_back(&schema, &rows, 1.0);
            let maps = batch.column(0).as_map();
            assert_eq!(maps.null_count(), 0);
            for (row, (keys, values)) in entries.iter().enumerate() {
                let entry = maps.value(row);
                let actual_keys = entry.column(0).as_string::<i32>();
                let actual_values = entry.column(1).as_primitive::<Int64Type>();
                assert_eq!(entry.len(), num_entries, "row {row}");
                for (i, (key, value)) in keys.iter().zip(values).enumerate() {
                    assert_eq!(actual_keys.value(i), key, "row {row} key {i}");
                    assert!(actual_values.is_valid(i), "row {row} value {i}");
                    assert_eq!(actual_values.value(i), *value, "row {row} value {i}");
                }
            }

            run_benchmark(
                &mut group,
                &format!("entries_{num_entries}"),
                &format!("rows_{num_rows}"),
                &schema,
                &rows,
                1,
                1.0,
            );
        }
    }

    group.finish();
}

/// Primitive and map columns holding nulls. Every other case here is free of nulls, so it never
/// reads a set null bit or appends a null.
///
/// - `cols_100_null_10pct`: the 100 Int64 columns of `primitive_columns`, with a tenth of the
///   fields null. Each null is found through the row's null bitset and appended as a null.
/// - `map_entries_{10,100}_value_null_10pct`: `Map<Int64, Int64>` with a tenth of the values
///   null. The value array is read as nullable, and an array with a null skips the plain copy
///   that a null-free array takes. At 10 entries about a third of the maps hold no null and the
///   rest go element by element; at 100 entries nearly every map holds one and takes the copy
///   with a validity buffer built from the null bitset (64 elements and up, in `list.rs`).
///
/// The nulls fall by a hash of row and position, values vary per row, and every row is built
/// separately. Each fixture is converted and read back once, and checked null for null and value
/// for value against how it was built, before it is timed.
fn benchmark_nullable_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("nullable_conversion");
    const NULL_FRACTION: f64 = 0.1;
    const NUM_COLS: usize = 100;

    for num_rows in [1000, 10000] {
        let schema = vec![ArrowDataType::Int64; NUM_COLS];
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|row| build_primitive_row(row, NUM_COLS, NULL_FRACTION))
            .collect();

        let batch = convert_and_read_back(&schema, &rows, 1.0);
        let mut nulls = 0;
        for col in 0..NUM_COLS {
            let column = batch.column(col).as_primitive::<Int64Type>();
            for row in 0..num_rows {
                if is_null(row, col, NULL_FRACTION) {
                    assert!(column.is_null(row), "row {row} column {col}");
                    nulls += 1;
                } else {
                    assert!(column.is_valid(row), "row {row} column {col}");
                    assert_eq!(column.value(row), (row * NUM_COLS + col) as i64);
                }
            }
        }
        assert!(nulls > 0, "the fixture must hold nulls");

        run_benchmark(
            &mut group,
            &format!("cols_{NUM_COLS}_null_10pct"),
            &format!("rows_{num_rows}"),
            &schema,
            &rows,
            NUM_COLS,
            1.0,
        );
    }

    for num_entries in [10, 100] {
        for num_rows in [1000, 10000] {
            let schema = vec![make_map_schema()];
            let rows: Vec<Vec<u8>> = (0..num_rows)
                .map(|row| build_map_row(num_entries, row, NULL_FRACTION))
                .collect();

            let batch = convert_and_read_back(&schema, &rows, 1.0);
            let maps = batch.column(0).as_map();
            let mut nulls = 0;
            for row in 0..num_rows {
                let entry = maps.value(row);
                assert_eq!(entry.len(), num_entries, "row {row}");
                let keys = entry.column(0).as_primitive::<Int64Type>();
                let values = entry.column(1).as_primitive::<Int64Type>();
                for i in 0..num_entries {
                    assert_eq!(keys.value(i), (row + i) as i64, "row {row} key {i}");
                    if is_null(row, i, NULL_FRACTION) {
                        assert!(values.is_null(i), "row {row} value {i}");
                        nulls += 1;
                    } else {
                        assert!(values.is_valid(i), "row {row} value {i}");
                        assert_eq!(values.value(i), ((row + i) * 100) as i64);
                    }
                }
            }
            assert!(nulls > 0, "the fixture must hold nulls");

            run_benchmark(
                &mut group,
                &format!("map_entries_{num_entries}_value_null_10pct"),
                &format!("rows_{num_rows}"),
                &schema,
                &rows,
                1,
                1.0,
            );
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
    targets = benchmark_primitive_columns,
              benchmark_struct_conversion,
              benchmark_list_conversion,
              benchmark_map_conversion,
              benchmark_sorted_spill,
              benchmark_string_conversion,
              benchmark_utf8_map_conversion,
              benchmark_nullable_conversion
}
criterion_main!(benches);
