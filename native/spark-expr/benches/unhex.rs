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

/// Build a column of hex strings representative of `unhex` inputs: mostly valid
/// hex of varying lengths (some odd), a few nulls, and some invalid strings that
/// exercise the error/null-fallback branch.
fn create_hex_array(size: usize) -> ArrayRef {
    let hex_samples = [
        "537061726B2053514C",                 // "Spark SQL"
        "1C",                                 // short even
        "A1B",                                // odd length
        "737472696E67",                       // "string"
        "0011223344556677889900aabbccddeeff", // longer, mixed case
        "deadBEEF",                           // mixed case
        "ZZ",                                 // invalid -> null
        "",                                   // empty -> empty binary
    ];
    let mut builder = StringBuilder::new();
    for i in 0..size {
        if i % 17 == 0 {
            builder.append_null();
        } else {
            builder.append_value(hex_samples[i % hex_samples.len()]);
        }
    }
    Arc::new(builder.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let array = create_hex_array(size);

    c.bench_function("spark_unhex: mixed hex column", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&array))];
        b.iter(|| black_box(spark_unhex(black_box(&args)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
