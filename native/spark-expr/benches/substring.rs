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
use datafusion_comet_spark_expr::kernels::strings::substring;
use std::hint::black_box;
use std::sync::Arc;

/// Build a string array where every tenth element is null and the elements repeat a
/// handful of values of the given character width.
fn create_string_array(size: usize, chars: usize, multibyte: bool) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for i in 0..size {
        if i % 10 == 0 {
            builder.append_null();
        } else {
            let unit = if multibyte { "アb" } else { "ab" };
            let mut value: String = unit.repeat(chars.div_ceil(2));
            value.truncate(
                value
                    .char_indices()
                    .nth(chars)
                    .map_or(value.len(), |(o, _)| o),
            );
            value.push_str(&(i % 100).to_string());
            builder.append_value(value);
        }
    }
    Arc::new(builder.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let short_ascii = create_string_array(size, 16, false);
    let long_ascii = create_string_array(size, 256, false);
    let long_utf8 = create_string_array(size, 256, true);

    c.bench_function("substring: short ascii", |b| {
        b.iter(|| black_box(substring(black_box(short_ascii.as_ref()), 2, 8).unwrap()))
    });

    c.bench_function("substring: long ascii prefix", |b| {
        b.iter(|| black_box(substring(black_box(long_ascii.as_ref()), 0, 16).unwrap()))
    });

    c.bench_function("substring: long ascii tail", |b| {
        b.iter(|| black_box(substring(black_box(long_ascii.as_ref()), 200, 1000).unwrap()))
    });

    c.bench_function("substring: long utf8 prefix", |b| {
        b.iter(|| black_box(substring(black_box(long_utf8.as_ref()), 0, 16).unwrap()))
    });

    c.bench_function("substring: long utf8 tail", |b| {
        b.iter(|| black_box(substring(black_box(long_utf8.as_ref()), 200, 1000).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
