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

use arrow::datatypes::ToByteSlice;
use comet::parquet::read::values::{copy_i32_to_i16, copy_i32_to_u16, copy_i64_to_i64};
use criterion::{criterion_group, criterion_main, Criterion};

fn criterion_benchmark(c: &mut Criterion) {
    let num = 1000;
    let source = vec![78_i8; num * 8];
    let mut group = c.benchmark_group("parquet_decode");
    group.bench_function("decode_i32_to_i16", |b| {
        let mut dest: Vec<u8> = vec![b' '; num * 2];
        b.iter(|| {
            copy_i32_to_i16(source.to_byte_slice(), dest.as_mut_slice(), num);
        });
    });
    group.bench_function("decode_i32_to_u16", |b| {
        let mut dest: Vec<u8> = vec![b' '; num * 4];
        b.iter(|| {
            copy_i32_to_u16(source.to_byte_slice(), dest.as_mut_slice(), num);
        });
    });
    group.bench_function("decode_i64_to_i64", |b| {
        let mut dest: Vec<u8> = vec![b' '; num * 8];
        b.iter(|| {
            copy_i64_to_i64(source.to_byte_slice(), dest.as_mut_slice(), num);
        });
    });
}

// Create UTF8 batch with strings representing ints, floats, nulls
fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
