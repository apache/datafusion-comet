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

//! Wide-decimal scalar controls and typed list hashing, including sliced inputs,
//! short/long arrays and no/sparse/dense nulls. Both public SQL hash paths are measured.

use arrow::array::{
    Array, ArrayRef, Decimal128Array, FixedSizeListArray, LargeListArray, ListArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Field;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion_comet_spark_expr::{spark_murmur3_hash, spark_xxhash64};
use std::{hint::black_box, sync::Arc};

fn bench(c: &mut Criterion) {
    let rows = 8192;
    let mut group = c.benchmark_group("decimal_hash");
    for (null_every, tag) in [(0, "no_null"), (100, "sparse_null"), (2, "dense_null")] {
        for width in [2, 32] {
            let values: ArrayRef = Arc::new(
                (0..(rows + 2) * width)
                    .map(|i| {
                        if null_every != 0 && i % null_every == 0 {
                            None
                        } else {
                            // Mix short signed encodings and values occupying all 16 bytes.
                            let magnitude = if i % 3 == 0 { 10_i128.pow(37) } else { 1 };
                            Some((i as i128 % 7 - 3) * magnitude)
                        }
                    })
                    .collect::<Decimal128Array>()
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            );
            let field = Arc::new(Field::new("item", values.data_type().clone(), true));
            let nulls = (null_every != 0)
                .then(|| NullBuffer::from_iter((0..rows + 2).map(|i| (i + 1) % null_every != 0)));
            let cases: Vec<(&str, ArrayRef)> = vec![
                ("scalar", values.slice(width, rows)),
                (
                    "list",
                    (Arc::new(ListArray::new(
                        Arc::clone(&field),
                        OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows + 2)),
                        Arc::clone(&values),
                        nulls.clone(),
                    )) as ArrayRef)
                        .slice(1, rows),
                ),
                (
                    "large_list",
                    (Arc::new(LargeListArray::new(
                        Arc::clone(&field),
                        OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows + 2)),
                        Arc::clone(&values),
                        nulls.clone(),
                    )) as ArrayRef)
                        .slice(1, rows),
                ),
                (
                    "fixed_list",
                    (Arc::new(FixedSizeListArray::new(field, width as i32, values, nulls))
                        as ArrayRef)
                        .slice(1, rows),
                ),
            ];
            for (shape, array) in cases {
                // A scalar control needs only one width.
                if shape == "scalar" && width != 2 {
                    continue;
                }
                for name in ["murmur3", "xxhash64"] {
                    let args = vec![
                        ColumnarValue::Array(Arc::clone(&array)),
                        ColumnarValue::Scalar(if name == "murmur3" {
                            ScalarValue::Int32(Some(42))
                        } else {
                            ScalarValue::Int64(Some(42))
                        }),
                    ];
                    group.bench_with_input(
                        BenchmarkId::new(name, format!("{shape}/{width}/{tag}")),
                        &args,
                        |b, args| {
                            if name == "murmur3" {
                                b.iter(|| black_box(spark_murmur3_hash(black_box(args)).unwrap()));
                            } else {
                                b.iter(|| black_box(spark_xxhash64(black_box(args)).unwrap()));
                            }
                        },
                    );
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
