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
//! list, and map types.

use arrow::datatypes::{DataType as ArrowDataType, Field, Fields};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_comet_shuffle::spark_unsafe::row::{process_sorted_row_partition, SparkUnsafeRow};
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

// ─── Generic UnsafeRow builder ──────────────────────────────────────────────

/// A value to encode in Spark's UnsafeRow / UnsafeArrayData layout. Used for the
/// nested shapes whose layout is too involved to hand-roll, such as collections of
/// strings or structs.
enum Value {
    Int64(i64),
    Utf8(String),
    Null,
    Struct(Vec<Value>),
    List(Vec<Value>),
    Map(Vec<(Value, Value)>),
}

fn pad_to_8(buf: &mut Vec<u8>) {
    while !buf.len().is_multiple_of(8) {
        buf.push(0);
    }
}

/// Append the variable-length payload of `value` to `buf` and return the
/// `(offset, size)` pointer for it, with `offset` relative to the start of `buf`.
/// Returns `None` for fixed-width values, which live in the slot itself.
fn append_variable(buf: &mut Vec<u8>, value: &Value) -> Option<(usize, usize)> {
    let payload = match value {
        Value::Int64(_) | Value::Null => return None,
        Value::Utf8(s) => s.as_bytes().to_vec(),
        Value::Struct(fields) => encode_struct(fields),
        Value::List(elements) => encode_list(elements),
        Value::Map(entries) => encode_map(entries),
    };
    let offset = buf.len();
    let size = payload.len();
    buf.extend_from_slice(&payload);
    pad_to_8(buf);
    Some((offset, size))
}

/// Encode a struct (or a top-level row) with 8-byte slots per field.
fn encode_struct(fields: &[Value]) -> Vec<u8> {
    let bitset = SparkUnsafeRow::get_row_bitset_width(fields.len());
    let mut buf = vec![0u8; bitset + fields.len() * 8];
    for (i, field) in fields.iter().enumerate() {
        let slot = bitset + i * 8;
        match field {
            Value::Null => buf[i / 64 * 8 + (i % 64) / 8] |= 1 << (i % 8),
            Value::Int64(v) => buf[slot..slot + 8].copy_from_slice(&v.to_le_bytes()),
            other => {
                let (offset, size) = append_variable(&mut buf, other).unwrap();
                write_pointer(&mut buf, slot, offset, size);
            }
        }
    }
    buf
}

/// Encode an UnsafeArrayData: element count, null bitset, 8-byte slots, then
/// the variable-length payloads.
fn encode_list(elements: &[Value]) -> Vec<u8> {
    let n = elements.len();
    let bitset = null_bitset_size(n);
    let slots = ARRAY_HEADER_SIZE + bitset;
    let mut buf = vec![0u8; slots + n * 8];
    buf[..ARRAY_HEADER_SIZE].copy_from_slice(&(n as i64).to_le_bytes());
    for (i, element) in elements.iter().enumerate() {
        let slot = slots + i * 8;
        match element {
            Value::Null => buf[ARRAY_HEADER_SIZE + i / 64 * 8 + (i % 64) / 8] |= 1 << (i % 8),
            Value::Int64(v) => buf[slot..slot + 8].copy_from_slice(&v.to_le_bytes()),
            other => {
                let (offset, size) = append_variable(&mut buf, other).unwrap();
                write_pointer(&mut buf, slot, offset, size);
            }
        }
    }
    buf
}

/// Encode an UnsafeMapData: key-array size, key array, value array.
fn encode_map(entries: &[(Value, Value)]) -> Vec<u8> {
    let keys = encode_list(
        &entries
            .iter()
            .map(|(k, _)| clone_value(k))
            .collect::<Vec<_>>(),
    );
    let values = encode_list(
        &entries
            .iter()
            .map(|(_, v)| clone_value(v))
            .collect::<Vec<_>>(),
    );
    let mut buf = Vec::with_capacity(ARRAY_HEADER_SIZE + keys.len() + values.len());
    buf.extend_from_slice(&(keys.len() as i64).to_le_bytes());
    buf.extend_from_slice(&keys);
    buf.extend_from_slice(&values);
    buf
}

fn clone_value(value: &Value) -> Value {
    match value {
        Value::Int64(v) => Value::Int64(*v),
        Value::Utf8(s) => Value::Utf8(s.clone()),
        Value::Null => Value::Null,
        Value::Struct(f) => Value::Struct(f.iter().map(clone_value).collect()),
        Value::List(e) => Value::List(e.iter().map(clone_value).collect()),
        Value::Map(e) => Value::Map(
            e.iter()
                .map(|(k, v)| (clone_value(k), clone_value(v)))
                .collect(),
        ),
    }
}

/// Encode a single-column top-level row holding `column`.
fn build_row(column: Value) -> Vec<u8> {
    encode_struct(&[column])
}

fn utf8_field(name: &str) -> Field {
    Field::new(name, ArrowDataType::Utf8, true)
}

fn int64_field(name: &str) -> Field {
    Field::new(name, ArrowDataType::Int64, true)
}

fn list_of(element: ArrowDataType) -> ArrowDataType {
    ArrowDataType::List(Arc::new(Field::new("item", element, true)))
}

fn struct_of(fields: Vec<Field>) -> ArrowDataType {
    ArrowDataType::Struct(Fields::from(fields))
}

fn map_of(key: ArrowDataType, value: ArrowDataType) -> ArrowDataType {
    let entries = Field::new(
        "entries",
        struct_of(vec![
            Field::new("key", key, false),
            Field::new("value", value, true),
        ]),
        false,
    );
    ArrowDataType::Map(Arc::new(entries), false)
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

/// Collections whose elements are variable-length or nested: strings, structs, and lists, plus
/// int64 lists where some elements are null.
fn benchmark_nested_collections(c: &mut Criterion) {
    let mut group = c.benchmark_group("nested_collections");

    for num_rows in [1000, 10000] {
        let param = format!("rows_{num_rows}");

        // List<Utf8>, 10 and 50 elements
        for n in [10, 50] {
            let rows: Vec<Vec<u8>> = (0..num_rows)
                .map(|r| {
                    build_row(Value::List(
                        (0..n)
                            .map(|i| Value::Utf8(format!("element_{r}_{i}")))
                            .collect(),
                    ))
                })
                .collect();
            run_benchmark(
                &mut group,
                &format!("list_utf8_elements_{n}"),
                &param,
                &[list_of(ArrowDataType::Utf8)],
                &rows,
                1,
            );
        }

        // List<Int64>, 100 elements, every 10th element null
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|_| {
                build_row(Value::List(
                    (0..100)
                        .map(|i| {
                            if i % 10 == 0 {
                                Value::Null
                            } else {
                                Value::Int64(i * 100)
                            }
                        })
                        .collect(),
                ))
            })
            .collect();
        run_benchmark(
            &mut group,
            "list_int64_10pct_null_elements_100",
            &param,
            &[list_of(ArrowDataType::Int64)],
            &rows,
            1,
        );

        // List<Struct<Int64, Utf8>>, 10 elements
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|r| {
                build_row(Value::List(
                    (0..10)
                        .map(|i| {
                            Value::Struct(vec![
                                Value::Int64(i),
                                Value::Utf8(format!("name_{r}_{i}")),
                            ])
                        })
                        .collect(),
                ))
            })
            .collect();
        run_benchmark(
            &mut group,
            "list_struct_elements_10",
            &param,
            &[list_of(struct_of(vec![
                int64_field("id"),
                utf8_field("name"),
            ]))],
            &rows,
            1,
        );

        // List<List<Int64>>, 10 x 10
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|_| {
                build_row(Value::List(
                    (0..10)
                        .map(|_| Value::List((0..10).map(|i| Value::Int64(i * 100)).collect()))
                        .collect(),
                ))
            })
            .collect();
        run_benchmark(
            &mut group,
            "list_list_10x10",
            &param,
            &[list_of(list_of(ArrowDataType::Int64))],
            &rows,
            1,
        );
    }

    group.finish();
}

/// Structs that mix strings, nulls and nested collections.
fn benchmark_mixed_struct_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_struct_conversion");

    for num_rows in [1000, 10000] {
        let param = format!("rows_{num_rows}");

        // Struct<Int64, Utf8, Utf8, Int64> with a null in every fourth row's string field
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|r| {
                build_row(Value::Struct(vec![
                    Value::Int64(r),
                    Value::Utf8(format!("name_{r}")),
                    if r % 4 == 0 {
                        Value::Null
                    } else {
                        Value::Utf8(format!("description_for_row_{r}"))
                    },
                    Value::Int64(r * 2),
                ]))
            })
            .collect();
        run_benchmark(
            &mut group,
            "struct_with_strings",
            &param,
            &[struct_of(vec![
                int64_field("id"),
                utf8_field("name"),
                utf8_field("desc"),
                int64_field("n"),
            ])],
            &rows,
            1,
        );

        // Struct<Struct<Int64, Utf8>, Struct<Utf8, Int64>>, two levels with strings
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|r| {
                build_row(Value::Struct(vec![
                    Value::Struct(vec![Value::Int64(r), Value::Utf8(format!("a_{r}"))]),
                    Value::Struct(vec![Value::Utf8(format!("b_{r}")), Value::Int64(r)]),
                ]))
            })
            .collect();
        run_benchmark(
            &mut group,
            "nested_struct_with_strings",
            &param,
            &[struct_of(vec![
                Field::new(
                    "l",
                    struct_of(vec![int64_field("id"), utf8_field("s")]),
                    true,
                ),
                Field::new(
                    "r",
                    struct_of(vec![utf8_field("s"), int64_field("id")]),
                    true,
                ),
            ])],
            &rows,
            1,
        );

        // Struct<List<Int64>, Utf8>
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|r| {
                build_row(Value::Struct(vec![
                    Value::List((0..10).map(|i| Value::Int64(i * 100)).collect()),
                    Value::Utf8(format!("label_{r}")),
                ]))
            })
            .collect();
        run_benchmark(
            &mut group,
            "struct_with_list",
            &param,
            &[struct_of(vec![
                Field::new("values", list_of(ArrowDataType::Int64), true),
                utf8_field("label"),
            ])],
            &rows,
            1,
        );
    }

    group.finish();
}

/// Maps with string keys or values, and with null values.
fn benchmark_map_variants(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_variants");

    for num_rows in [1000, 10000] {
        let param = format!("rows_{num_rows}");

        // Map<Utf8, Utf8>, 10 entries
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|r| {
                build_row(Value::Map(
                    (0..10)
                        .map(|i| {
                            (
                                Value::Utf8(format!("key_{i}")),
                                Value::Utf8(format!("value_{r}_{i}")),
                            )
                        })
                        .collect(),
                ))
            })
            .collect();
        run_benchmark(
            &mut group,
            "utf8_utf8_entries_10",
            &param,
            &[map_of(ArrowDataType::Utf8, ArrowDataType::Utf8)],
            &rows,
            1,
        );

        // Map<Utf8, Int64>, 10 entries, every third value null
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|_| {
                build_row(Value::Map(
                    (0..10)
                        .map(|i| {
                            (
                                Value::Utf8(format!("key_{i}")),
                                if i % 3 == 0 {
                                    Value::Null
                                } else {
                                    Value::Int64(i * 100)
                                },
                            )
                        })
                        .collect(),
                ))
            })
            .collect();
        run_benchmark(
            &mut group,
            "utf8_int64_null_values_entries_10",
            &param,
            &[map_of(ArrowDataType::Utf8, ArrowDataType::Int64)],
            &rows,
            1,
        );

        // Map<Int64, List<Int64>>, 5 entries of 10 elements
        let rows: Vec<Vec<u8>> = (0..num_rows)
            .map(|_| {
                build_row(Value::Map(
                    (0..5)
                        .map(|i| {
                            (
                                Value::Int64(i),
                                Value::List((0..10).map(|j| Value::Int64(j * 100)).collect()),
                            )
                        })
                        .collect(),
                ))
            })
            .collect();
        run_benchmark(
            &mut group,
            "int64_list_entries_5",
            &param,
            &[map_of(ArrowDataType::Int64, list_of(ArrowDataType::Int64))],
            &rows,
            1,
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
              benchmark_nested_collections,
              benchmark_mixed_struct_conversion,
              benchmark_map_variants
}
criterion_main!(benches);
