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

use arrow::array::{ArrayRef, StringArray};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_comet_common::decode_string_arrays;
use std::hint::black_box;
use std::sync::Arc;

fn bench_valid_fast_path(c: &mut Criterion) {
    // 8192-row batch of valid mixed ASCII/multibyte strings, exercising the fast path (from_utf8
    // plus boundary check, zero-copy return) that every scanned string column pays.
    let values: Vec<String> = (0..8192).map(|i| format!("row-{i}-café-日本語")).collect();
    let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    let input: ArrayRef = Arc::new(StringArray::from(refs));

    c.bench_function("decode_string_arrays/valid_8192", |b| {
        b.iter(|| {
            let out = black_box(decode_string_arrays(black_box(&input)).unwrap());
            assert!(Arc::ptr_eq(&input, &out));
        })
    });
}

criterion_group!(benches, bench_valid_fast_path);
criterion_main!(benches);
