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

//! The native Iceberg writer's dictionary choice for one partition, over the column groups of
//! `CometIcebergEndToEndBenchmark`: primitives, structs, arrays, maps, and nested types.
//!
//! Each sample is what a partition holds back before choosing: the first data page of every
//! column, which at Iceberg's default `write.parquet.page-row-limit` is 20,000 rows, in the
//! 8192-row batches a shuffle read produces. The `primitives_1k` shape is a partition that ends
//! within its first page, as the many small partitions of a fanout write do.

use std::hint::black_box;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
    Int64Array, LargeBinaryArray, ListArray, MapArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use comet::execution::operators::DictionaryChooser;
use criterion::{criterion_group, criterion_main, Criterion};
use parquet::file::properties::WriterProperties;

const BATCH_ROWS: usize = 8192;

/// One array element or map value in this many is null.
const NULL_STRIDE: i64 = 8;

type Columns = Vec<(&'static str, ArrayRef)>;

/// Builds a group's columns from the source table's `value` for each row.
type Group = fn(&[i64]) -> Columns;

/// splitmix64.
fn mix(x: u64) -> u64 {
    let mut x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// The source table's `value` column as one of ten hash partitions receives it: ids from 2M rows,
/// in no particular order.
fn values(rows: Range<usize>) -> Vec<i64> {
    rows.map(|row| (mix(row as u64) % 2_000_000) as i64)
        .collect()
}

/// Entries in an array or a map: zero to nine.
fn entry_count(value: i64, salt: i64) -> usize {
    ((value >> 8) + salt).rem_euclid(10) as usize
}

fn entry_is_valid(value: i64, entry: i64) -> bool {
    (value + entry * 3).rem_euclid(NULL_STRIDE) != 0
}

/// The offsets of every row's entries, and the `(value, entry)` each entry is built from.
fn entries(values: &[i64], count: impl Fn(i64) -> usize) -> (OffsetBuffer<i32>, Vec<(i64, i64)>) {
    let mut flat = Vec::new();
    let offsets = OffsetBuffer::from_lengths(values.iter().map(|&value| {
        let n = count(value);
        flat.extend((0..n as i64).map(|entry| (value, entry)));
        n
    }));
    (offsets, flat)
}

fn list(offsets: OffsetBuffer<i32>, elements: ArrayRef) -> ArrayRef {
    let field = Arc::new(Field::new("element", elements.data_type().clone(), true));
    Arc::new(ListArray::new(field, offsets, elements, None))
}

fn map(offsets: OffsetBuffer<i32>, keys: ArrayRef, values: ArrayRef) -> ArrayRef {
    let fields = Fields::from(vec![
        Field::new("key", keys.data_type().clone(), false),
        Field::new("value", values.data_type().clone(), true),
    ]);
    let entries = StructArray::new(fields.clone(), vec![keys, values], None);
    let field = Arc::new(Field::new("key_value", DataType::Struct(fields), false));
    Arc::new(MapArray::new(field, offsets, entries, None, false))
}

fn struct_of(columns: Vec<(&str, ArrayRef)>, nulls: Option<NullBuffer>) -> ArrayRef {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = columns
        .into_iter()
        .map(|(name, array)| (Field::new(name, array.data_type().clone(), true), array))
        .unzip();
    Arc::new(StructArray::new(Fields::from(fields), arrays, nulls))
}

fn ints(values: impl Iterator<Item = Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from_iter(values))
}

fn strings(values: impl Iterator<Item = Option<String>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

fn primitives(values: &[i64]) -> Columns {
    vec![
        (
            "c_bool",
            Arc::new(BooleanArray::from_iter(
                values.iter().map(|v| Some(v % 2 == 0)),
            )),
        ),
        (
            "c_int",
            ints(values.iter().map(|v| Some((v % 2_147_483_647) as i32))),
        ),
        ("c_long", Arc::new(Int64Array::from(values.to_vec()))),
        (
            "c_double",
            Arc::new(Float64Array::from_iter_values(
                values.iter().map(|&v| v as f64 / 7.0),
            )),
        ),
        (
            "c_dec",
            Arc::new(
                Decimal128Array::from_iter_values(
                    values.iter().map(|v| (v % 100_000_000) as i128 * 10_000),
                )
                .with_precision_and_scale(18, 4)
                .unwrap(),
            ),
        ),
        (
            "c_str_dict",
            strings(values.iter().map(|v| Some((v % 8).to_string()))),
        ),
        (
            "c_str",
            strings(values.iter().map(|v| Some(v.to_string().repeat(3)))),
        ),
        (
            "c_bin",
            Arc::new(LargeBinaryArray::from_iter_values(
                values.iter().map(|v| v.to_string().into_bytes()),
            )),
        ),
        (
            "c_date",
            Arc::new(Date32Array::from_iter_values(
                values.iter().map(|v| (v % 20_000) as i32),
            )),
        ),
        (
            "c_ts",
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(
                    values.iter().map(|v| (v % 1_600_000_000) * 1_000_000),
                )
                .with_timezone("+00:00"),
            ),
        ),
    ]
}

fn structs(values: &[i64]) -> Columns {
    let s_flat = struct_of(
        vec![
            (
                "i",
                ints(values.iter().map(|v| Some((v % 1_000_000) as i32))),
            ),
            ("l", Arc::new(Int64Array::from(values.to_vec()))),
            (
                "d",
                Arc::new(Float64Array::from_iter_values(
                    values.iter().map(|&v| v as f64 / 7.0),
                )),
            ),
            (
                "s",
                strings(
                    values
                        .iter()
                        .map(|v| ((v >> 4) % NULL_STRIDE != 0).then(|| v.to_string())),
                ),
            ),
            (
                "dt",
                Arc::new(Date32Array::from_iter_values(
                    values.iter().map(|v| (v % 20_000) as i32),
                )),
            ),
        ],
        None,
    );
    let inner = struct_of(
        vec![
            ("x", ints(values.iter().map(|v| Some((v % 1000) as i32)))),
            (
                "y",
                strings(values.iter().map(|v| Some(format!("y{}", v % 100_000)))),
            ),
        ],
        Some(NullBuffer::from_iter(
            values.iter().map(|v| (v >> 4) % NULL_STRIDE != 1),
        )),
    );
    let s_nested = struct_of(
        vec![
            ("id", Arc::new(Int64Array::from(values.to_vec()))),
            ("inner", inner),
            (
                "flag",
                Arc::new(BooleanArray::from_iter(
                    values.iter().map(|v| Some(v % 3 == 0)),
                )),
            ),
        ],
        None,
    );
    vec![("s_flat", s_flat), ("s_nested", s_nested)]
}

fn arrays(values: &[i64]) -> Columns {
    let (offsets, flat) = entries(values, |v| entry_count(v, 0));
    let a_int = list(
        offsets,
        ints(
            flat.iter()
                .map(|&(v, i)| entry_is_valid(v, i).then_some((v % 1_000_000 + i) as i32)),
        ),
    );
    let (offsets, flat) = entries(values, |v| entry_count(v, 1));
    let a_double = list(
        offsets,
        Arc::new(Float64Array::from_iter(flat.iter().map(|&(v, i)| {
            entry_is_valid(v, i).then_some((v % 1_000_000 + i) as f64 / 7.0)
        }))),
    );
    let (offsets, flat) = entries(values, |v| entry_count(v, 2));
    let a_str = list(
        offsets,
        strings(
            flat.iter()
                .map(|&(v, i)| entry_is_valid(v, i).then(|| format!("e{}", v % 100_000 + i))),
        ),
    );
    vec![("a_int", a_int), ("a_double", a_double), ("a_str", a_str)]
}

fn maps(values: &[i64]) -> Columns {
    let (offsets, flat) = entries(values, |v| entry_count(v, 1));
    let m_str_int = map(
        offsets.clone(),
        strings(flat.iter().map(|&(_, i)| Some(format!("k{i}")))),
        ints(
            flat.iter()
                .map(|&(v, i)| entry_is_valid(v, i).then_some((v % 1000 + i) as i32)),
        ),
    );
    let m_int_str = map(
        offsets,
        ints(flat.iter().map(|&(_, i)| Some(i as i32))),
        strings(
            flat.iter()
                .map(|&(v, i)| entry_is_valid(v, i).then(|| format!("v{}", v % 100_000 + i))),
        ),
    );
    vec![("m_str_int", m_str_int), ("m_int_str", m_int_str)]
}

fn nested(values: &[i64]) -> Columns {
    let (offsets, flat) = entries(values, |v| entry_count(v, 0));
    let n_array_struct = list(
        offsets,
        struct_of(
            vec![
                (
                    "k",
                    ints(flat.iter().map(|&(v, i)| Some((v % 1000 + i) as i32))),
                ),
                (
                    "v",
                    strings(
                        flat.iter()
                            .map(|&(v, i)| Some(format!("v{}", v % 100_000 + i))),
                    ),
                ),
            ],
            Some(NullBuffer::from_iter(
                flat.iter().map(|&(v, i)| entry_is_valid(v, i)),
            )),
        ),
    );

    // A map from "k{i}" to an array of `entry_count(value, i)` ints.
    let (offsets, flat) = entries(values, |v| entry_count(v, 1));
    let mut inner_offsets = vec![0i32];
    let mut inner_values = Vec::new();
    for &(v, i) in &flat {
        let n = if entry_is_valid(v, i) {
            entry_count(v, i)
        } else {
            0
        };
        inner_values.extend(
            (0..n as i64).map(|j| entry_is_valid(v, j).then_some((v % 1000 + i * 10 + j) as i32)),
        );
        inner_offsets.push(inner_values.len() as i32);
    }
    let inner = Arc::new(ListArray::new(
        Arc::new(Field::new("element", DataType::Int32, true)),
        OffsetBuffer::new(inner_offsets.into()),
        ints(inner_values.into_iter()),
        Some(NullBuffer::from_iter(
            flat.iter().map(|&(v, i)| entry_is_valid(v, i)),
        )),
    ));
    let n_map_array = map(
        offsets,
        strings(flat.iter().map(|&(_, i)| Some(format!("k{i}")))),
        inner,
    );

    let (tag_offsets, tags) = entries(values, |v| entry_count(v, 3));
    let (attr_offsets, attrs) = entries(values, |v| entry_count(v, 1));
    let n_struct_mix =
        struct_of(
            vec![
                (
                    "tags",
                    list(
                        tag_offsets,
                        strings(tags.iter().map(|&(v, i)| {
                            entry_is_valid(v, i).then(|| format!("t{}", (v + i) % 50))
                        })),
                    ),
                ),
                (
                    "attrs",
                    map(
                        attr_offsets,
                        strings(attrs.iter().map(|&(_, i)| Some(format!("a{i}")))),
                        ints(
                            attrs.iter().map(|&(v, i)| {
                                entry_is_valid(v, i).then_some((v % 1000 + i) as i32)
                            }),
                        ),
                    ),
                ),
            ],
            None,
        );
    vec![
        ("n_array_struct", n_array_struct),
        ("n_map_array", n_map_array),
        ("n_struct_mix", n_struct_mix),
    ]
}

/// `rows` rows of a group in [`BATCH_ROWS`]-row batches.
fn sample(rows: usize, group: Group) -> Vec<RecordBatch> {
    (0..rows)
        .step_by(BATCH_ROWS)
        .map(|start| {
            let values = values(start..(start + BATCH_ROWS).min(rows));
            RecordBatch::try_from_iter(group(&values)).unwrap()
        })
        .collect()
}

/// The writer properties of a table with Iceberg's defaults: 20,000-row and 1 MiB pages, a 2 MiB
/// dictionary limit, and 128 MiB row groups.
fn properties() -> WriterProperties {
    WriterProperties::builder()
        .set_data_page_row_count_limit(20_000)
        .set_data_page_size_limit(1024 * 1024)
        .set_dictionary_page_size_limit(2 * 1024 * 1024)
        .set_max_row_group_bytes(Some(128 * 1024 * 1024))
        .set_max_row_group_row_count(None)
        .build()
}

fn criterion_benchmark(c: &mut Criterion) {
    let shapes: [(&str, usize, Group); 6] = [
        ("primitives", 20_000, primitives),
        ("primitives_1k", 1000, primitives),
        ("structs", 20_000, structs),
        ("arrays", 20_000, arrays),
        ("maps", 20_000, maps),
        ("nested", 20_000, nested),
    ];
    let mut group = c.benchmark_group("dictionary_chooser");
    for (name, rows, columns) in shapes {
        let sample = sample(rows, columns);
        let chooser = DictionaryChooser::try_new(properties(), sample[0].schema_ref()).unwrap();
        group.bench_function(name, |b| {
            b.iter(|| black_box(chooser.choose(black_box(&sample))))
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
