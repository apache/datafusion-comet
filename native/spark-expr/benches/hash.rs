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
// under the License.use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, RecordBatch, StringBuilder};

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

use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use std::hint::black_box;

#[path = "common/hash_shapes.rs"]
mod hash_shapes;
use hash_shapes::*;

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
        // Shapes where gathering could cost more than the dispatches it saves.
        (
            "list_of_struct_1kb_string_x4",
            list_of_struct_big_string(2048, 4, 1024),
        ),
        (
            "list_of_struct_half_null_x10",
            list_of_struct_half_null(NUM_ROWS, 10),
        ),
        (
            "list_of_struct_long_tail_x1024",
            list_of_struct_long_tail(2, 1024),
        ),
        (
            "struct_of_dict_unreferenced_8mb",
            struct_of_dict_unreferenced_big_value(8192, 8 * 1024 * 1024),
        ),
        // Shapes deliberately left on the per-element path, so a change to the eligibility rule
        // shows up as a timing move here and not only in the allocation table.
        (
            "null_parent_struct_64kb_x4",
            null_parent_struct_big_string(128, 4, 65536),
        ),
        (
            "deep_singleton_list_5_deep",
            deep_singleton_list_of_struct_big_string(2048, 5, 4096),
        ),
        (
            "width_skewed_8mb_first",
            width_skewed_list_of_struct(8 * 1024 * 1024),
        ),
        (
            "sliced_list_retaining_10m_ints",
            sliced_list_retaining_big_child(2, 10_000_000),
        ),
    ];

    let mut group = c.benchmark_group("murmur3");
    for (name, array) in &cases {
        // Size the buffer from the array rather than assuming `NUM_ROWS`, since not every shape
        // uses that row count.
        let rows = array.len();
        group.bench_function(*name, |b| {
            b.iter(|| {
                let mut hashes = vec![42u32; rows];
                create_murmur3_hashes(std::slice::from_ref(array), &mut hashes).unwrap();
                black_box(&hashes);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
