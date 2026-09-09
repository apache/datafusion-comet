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

//! Benchmarks for the Spark-compatible hash kernels, which back the `hash` and `xxhash64`
//! expressions and, since #5567, native shuffle hash partitioning.
//!
//! The nested shapes are the interesting ones. A list whose elements are primitives is hashed by a
//! vectorized path, while a list whose elements are themselves nested falls through to a
//! per-element path, so `array<struct<..>>` and `array<int>` exercise different code. Struct and
//! map keys are included because both are now admissible partitioning keys.
//!
//! Only murmur3 is covered: `create_xxhash64_hashes` is `pub(crate)`, so a benchmark cannot reach
//! it. The two share `create_hashes_internal!`, so the shape of the work is the same and a change
//! to that macro shows up here.

use arrow::array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder};
use arrow::array::{ArrayRef, Int32Array, ListArray, MapFieldNames, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/matched_maps.rs"]
mod matched_maps_data;

const NUM_ROWS: usize = 8192;

fn struct_fields() -> Fields {
    vec![
        Arc::new(Field::new("a", DataType::Int32, true)),
        Arc::new(Field::new("b", DataType::Utf8, true)),
    ]
    .into()
}

fn struct_builder() -> StructBuilder {
    StructBuilder::new(
        struct_fields(),
        vec![
            Box::new(Int32Builder::new()),
            Box::new(StringBuilder::new()),
        ],
    )
}

fn append_struct(sb: &mut StructBuilder, i: usize) {
    sb.field_builder::<Int32Builder>(0)
        .unwrap()
        .append_value(i as i32);
    sb.field_builder::<StringBuilder>(1)
        .unwrap()
        .append_value(format!("v{}", i % 97));
    sb.append(true);
}

/// `int32`, the cheapest leaf, as a reference point for the nested shapes.
fn primitive(num_rows: usize) -> ArrayRef {
    Arc::new(Int32Array::from((0..num_rows as i32).collect::<Vec<_>>()))
}

/// `utf8`: variable-width, so the hash reads from the values buffer per row.
fn string(num_rows: usize) -> ArrayRef {
    Arc::new(StringArray::from(
        (0..num_rows)
            .map(|i| format!("v{}", i % 97))
            .collect::<Vec<_>>(),
    ))
}

/// `struct<a: int32, b: utf8>`: hashed field by field across the whole batch.
fn structs(num_rows: usize) -> ArrayRef {
    let mut sb = struct_builder();
    for i in 0..num_rows {
        append_struct(&mut sb, i);
    }
    Arc::new(sb.finish())
}

/// `array<int32>`: elements are primitives, so this takes the vectorized element path.
fn list_of_primitive(num_rows: usize, elems: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(Int32Builder::new());
    for i in 0..num_rows {
        for j in 0..elems {
            lb.values().append_value((i * 31 + j) as i32);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// `array<struct<..>>`: elements are nested, so this takes the per-element path.
fn list_of_struct(num_rows: usize, elems: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(struct_builder());
    for i in 0..num_rows {
        for j in 0..elems {
            append_struct(lb.values(), i * 31 + j);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// `array<struct<..>>` where one row is far longer than the rest, so the element count is spread
/// very unevenly across rows rather than uniformly. The per-element path slices and re-dispatches
/// once per element, so a batch dominated by a single long list has the same total work in a very
/// different distribution, which a uniform shape cannot show.
fn skewed_list_of_struct(num_rows: usize, long_len: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(struct_builder());
    for i in 0..num_rows {
        let len = if i == 0 { long_len } else { 1 };
        for j in 0..len {
            append_struct(lb.values(), i * 31 + j);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// `map<utf8, int32>`: keys and values are hashed entry by entry.
fn maps(num_rows: usize, entries: usize) -> ArrayRef {
    let mut mb = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        StringBuilder::new(),
        Int32Builder::new(),
    );
    for i in 0..num_rows {
        for j in 0..entries {
            mb.keys().append_value(format!("k{}", (i + j) % 97));
            mb.values().append_value((i * 31 + j) as i32);
        }
        mb.append(true).unwrap();
    }
    Arc::new(mb.finish())
}

/// `struct<a: int32, m: map<utf8, int32>>`: a map inside a struct, so the struct branch recurses
/// into the map specialization rather than into a leaf.
fn struct_of_map(num_rows: usize, entries: usize) -> ArrayRef {
    let fields: Fields = vec![
        Arc::new(Field::new("a", DataType::Int32, true)),
        Arc::new(Field::new(
            "m",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Utf8, false)),
                            Arc::new(Field::new("value", DataType::Int32, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )),
    ]
    .into();
    let ints = primitive(num_rows);
    let ms = maps(num_rows, entries);
    Arc::new(StructArray::new(fields, vec![ints, ms], None))
}

/// `array<array<int32>>`: the element is a list, so the non-primitive element path recurses into
/// the vectorized leaf loop one level down.
fn list_of_list(num_rows: usize, outer: usize, inner: usize) -> ArrayRef {
    let mut lb = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
    for i in 0..num_rows {
        for j in 0..outer {
            for k in 0..inner {
                lb.values()
                    .values()
                    .append_value((i * 31 + j * 7 + k) as i32);
            }
            lb.values().append(true);
        }
        lb.append(true);
    }
    Arc::new(lb.finish())
}

/// `map<utf8, struct<..>>`: a struct as the map value, which the key/value specializations do not
/// cover, so the value array is hashed recursively instead.
fn map_of_struct(num_rows: usize, entries: usize) -> ArrayRef {
    let mut mb = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        StringBuilder::new(),
        struct_builder(),
    );
    for i in 0..num_rows {
        for j in 0..entries {
            mb.keys().append_value(format!("k{}", (i + j) % 97));
            append_struct(mb.values(), i * 31 + j);
        }
        mb.append(true).unwrap();
    }
    Arc::new(mb.finish())
}

/// `array<struct<a: int32, m: map<..>>>`: three levels, so the per-element path recurses through a
/// struct into a map.
fn list_of_struct_of_map(num_rows: usize, elems: usize, entries: usize) -> ArrayRef {
    let inner = struct_of_map(num_rows * elems, entries);
    let offsets: Vec<i32> = (0..=num_rows).map(|i| (i * elems) as i32).collect();
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", inner.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        inner,
        None,
    ))
}

fn bench(c: &mut Criterion) {
    let cases: Vec<(&str, ArrayRef)> = vec![
        ("int32", primitive(NUM_ROWS)),
        ("utf8", string(NUM_ROWS)),
        ("struct", structs(NUM_ROWS)),
        ("list_of_int32_x10", list_of_primitive(NUM_ROWS, 10)),
        ("list_of_struct_x10", list_of_struct(NUM_ROWS, 10)),
        ("map_x10", maps(NUM_ROWS, 10)),
        (
            "list_of_struct_skewed_x1024",
            skewed_list_of_struct(NUM_ROWS, 1024),
        ),
        // Nested combinations, so each distinct recursion is represented.
        ("struct_of_map_x10", struct_of_map(NUM_ROWS, 10)),
        ("list_of_list_x5x5", list_of_list(NUM_ROWS, 5, 5)),
        ("map_of_struct_x10", map_of_struct(NUM_ROWS, 10)),
        (
            "list_of_struct_of_map_x5x5",
            list_of_struct_of_map(NUM_ROWS, 5, 5),
        ),
    ];

    let mut group = c.benchmark_group("murmur3");
    for (name, array) in &cases {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let mut hashes = vec![42u32; NUM_ROWS];
                create_murmur3_hashes(std::slice::from_ref(array), &mut hashes).unwrap();
                black_box(&hashes);
            })
        });
    }
    group.finish();
}

fn matched_maps(c: &mut Criterion) {
    matched_maps_data::bench_maps(c, "hash_only");
    matched_maps_data::bench_maps(c, "normalize_hash");
    c.bench_function("matched_maps/hash_buffer_seed_reset", |b| {
        let mut hashes = vec![42u32; matched_maps_data::ROWS];
        b.iter(|| {
            hashes.fill(42);
            black_box(&hashes);
        });
    });
}

criterion_group!(benches, bench, matched_maps);
criterion_main!(benches);
