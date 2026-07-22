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

use arrow::array::builder::BinaryBuilder;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_cast, BinaryOutputStyle, EvalMode, SparkCastOptions};
use std::hint::black_box;
use std::sync::Arc;

/// Build a Binary array of `size` rows, each row holding `width` pseudo-random bytes, with a
/// null every `null_every` rows (0 = no nulls).
fn build(size: usize, width: usize, null_every: usize) -> ArrayRef {
    let mut builder = BinaryBuilder::with_capacity(size, size * width);
    let mut state: u32 = 0x9e37_79b9;
    for i in 0..size {
        if null_every != 0 && i % null_every == 0 {
            builder.append_null();
        } else {
            let value: Vec<u8> = (0..width)
                .map(|_| {
                    state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    (state >> 24) as u8
                })
                .collect();
            builder.append_value(&value);
        }
    }
    Arc::new(builder.finish())
}

fn options(style: Option<BinaryOutputStyle>) -> SparkCastOptions {
    let mut options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
    options.binary_output_style = style;
    options
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;

    let short = build(size, 8, 0);
    let long = build(size, 64, 0);
    let with_nulls = build(size, 16, 17);

    let mut bench = |name: &str, arr: &ArrayRef, style: Option<BinaryOutputStyle>| {
        let cast_options = options(style);
        c.bench_function(name, |b| {
            b.iter(|| {
                black_box(
                    spark_cast(
                        ColumnarValue::Array(Arc::clone(arr)),
                        black_box(&DataType::Utf8),
                        black_box(&cast_options),
                    )
                    .unwrap(),
                )
            })
        });
    };

    bench(
        "cast_binary_to_string: hex discrete, short values",
        &short,
        Some(BinaryOutputStyle::HexDiscrete),
    );
    bench(
        "cast_binary_to_string: hex discrete, long values",
        &long,
        Some(BinaryOutputStyle::HexDiscrete),
    );
    bench(
        "cast_binary_to_string: hex discrete, with nulls",
        &with_nulls,
        Some(BinaryOutputStyle::HexDiscrete),
    );
    bench(
        "cast_binary_to_string: hex, long values",
        &long,
        Some(BinaryOutputStyle::Hex),
    );
    bench(
        "cast_binary_to_string: basic, long values",
        &long,
        Some(BinaryOutputStyle::Basic),
    );
    bench(
        "cast_binary_to_string: base64, long values",
        &long,
        Some(BinaryOutputStyle::Base64),
    );
    bench(
        "cast_binary_to_string: default style, long values",
        &long,
        None,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
