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

//! `xxhash64` is the alternative Spark hash (e.g. `xxhash64()` and bucketing). It covers a
//! representative multi-column key across row counts and null ratios, plus the compatible type
//! families and fallback paths used by `spark_xxhash64`.

use arrow::array::{
    ArrayRef, BinaryArray, Decimal128Array, DictionaryArray, Int32Array, StringArray,
};
use arrow::datatypes::Int32Type;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_xxhash64;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{f64_array, i64_array, string_array, NULL_RATIOS, ROW_COUNTS};

#[path = "common/hash_shapes.rs"]
mod hash_shapes;

const TYPE_FAMILY_ROWS: usize = 8_192;

fn seeded_args(arrays: impl IntoIterator<Item = ArrayRef>, seed: i64) -> Vec<ColumnarValue> {
    arrays
        .into_iter()
        .map(ColumnarValue::Array)
        .chain(std::iter::once(ColumnarValue::Scalar(ScalarValue::Int64(
            Some(seed),
        ))))
        .collect()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spark_xxhash64");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            // Trailing Int64 scalar is the seed; preceding columns are the key being hashed.
            let args = seeded_args(
                [
                    i64_array(rows, null_ratio, |i| i as i64),
                    string_array(rows, null_ratio, |i| format!("k{}", i % 1024)),
                    f64_array(rows, null_ratio, |i| i as f64 * 1.5),
                ],
                42,
            );
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &args,
                |b, args| b.iter(|| black_box(spark_xxhash64(black_box(args)).unwrap())),
            );
        }
    }
    group.finish();

    let primitive = i64_array(TYPE_FAMILY_ROWS, 0.0, |i| i as i64);
    let strings = string_array(TYPE_FAMILY_ROWS, 0.0, |i| format!("value_{}", i % 1024));
    let binary: ArrayRef = Arc::new(BinaryArray::from_iter_values(
        (0..TYPE_FAMILY_ROWS).map(|i| format!("bytes_{}", i % 1024).into_bytes()),
    ));
    let decimal_narrow: ArrayRef = Arc::new(
        Decimal128Array::from_iter_values((0..TYPE_FAMILY_ROWS).map(|i| i as i128 * 100))
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let decimal_wide: ArrayRef = Arc::new(
        Decimal128Array::from_iter_values(
            (0..TYPE_FAMILY_ROWS).map(|i| 10_000_000_000_000_000_000i128 + i as i128),
        )
        .with_precision_and_scale(38, 10)
        .unwrap(),
    );
    let dictionary_values: ArrayRef = Arc::new(StringArray::from(
        (0..1024)
            .map(|i| format!("dictionary_value_{i}"))
            .collect::<Vec<_>>(),
    ));
    let dictionary_keys =
        Int32Array::from_iter_values((0..TYPE_FAMILY_ROWS).map(|i| (i % 1024) as i32));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(dictionary_keys, dictionary_values).unwrap(),
    );

    let type_family_cases = vec![
        (
            "compatible/primitive_i64",
            seeded_args([Arc::clone(&primitive)], 42),
        ),
        (
            "compatible/string_binary",
            seeded_args([strings, binary], 42),
        ),
        (
            "compatible/decimal128_narrow_wide",
            seeded_args([decimal_narrow, decimal_wide], 42),
        ),
        (
            "compatible/dictionary_i32_utf8",
            seeded_args([dictionary], 42),
        ),
        (
            "compatible/list_i32_x10",
            seeded_args([hash_shapes::list_of_primitive(TYPE_FAMILY_ROWS, 10)], 42),
        ),
        (
            "compatible/map_utf8_i32_x10",
            seeded_args([hash_shapes::maps(TYPE_FAMILY_ROWS, 10)], 42),
        ),
        (
            "fallback/struct_i32_utf8",
            seeded_args([hash_shapes::structs(TYPE_FAMILY_ROWS)], 42),
        ),
        ("fallback/custom_seed_i64", seeded_args([primitive], 7)),
    ];

    let mut group = c.benchmark_group("spark_xxhash64_type_families");
    for (name, args) in type_family_cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &args, |b, args| {
            b.iter(|| black_box(spark_xxhash64(black_box(args)).unwrap()))
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
