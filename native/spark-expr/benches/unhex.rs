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

use arrow::array::builder::StringBuilder;
use arrow::array::ArrayRef;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_unhex;
use std::hint::black_box;
use std::sync::Arc;

/// Build a Utf8 array of `size` rows by cycling through `samples`, inserting a
/// null every `null_every` rows (0 = no nulls).
fn build(samples: &[&str], size: usize, null_every: usize) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for i in 0..size {
        if null_every != 0 && i % null_every == 0 {
            builder.append_null();
        } else {
            builder.append_value(samples[i % samples.len()]);
        }
    }
    Arc::new(builder.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;

    // Dense column of valid, even-length hex strings, no nulls.
    let all_valid = build(
        &[
            "537061726B2053514C",
            "737472696E67",
            "1C",
            "deadbeef",
            "0011ccddeeff",
        ],
        size,
        0,
    );
    // Same shape but with ~6% nulls interleaved.
    let with_nulls = build(
        &[
            "537061726B2053514C",
            "737472696E67",
            "1C",
            "deadbeef",
            "0011ccddeeff",
        ],
        size,
        17,
    );
    // Long valid hex strings, exercising the decode loop and allocation.
    let long_valid = build(
        &[
            "0011223344556677889900aabbccddeeff0011223344556677889900aabbccddeeff",
            "537061726b2053514c537061726b2053514c537061726b2053514c537061726b2053514c",
        ],
        size,
        0,
    );
    // Mostly invalid inputs (odd length or non-hex), which decode to null.
    let invalid = build(&["ZZ", "A1B", "xyz", "12G4", "gg"], size, 0);
    // A diverse mix of the above shapes plus empty strings.
    let mixed = build(
        &[
            "537061726B2053514C",
            "1C",
            "A1B",
            "737472696E67",
            "0011223344556677889900aabbccddeeff",
            "deadBEEF",
            "ZZ",
            "",
        ],
        size,
        17,
    );

    let mut bench = |name: &str, arr: &ArrayRef| {
        let args = vec![ColumnarValue::Array(Arc::clone(arr))];
        c.bench_function(name, |b| {
            b.iter(|| black_box(spark_unhex(black_box(&args)).unwrap()))
        });
    };

    bench("spark_unhex: all valid", &all_valid);
    bench("spark_unhex: with nulls", &with_nulls);
    bench("spark_unhex: long strings", &long_valid);
    bench("spark_unhex: invalid inputs", &invalid);
    bench("spark_unhex: mixed hex column", &mixed);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
