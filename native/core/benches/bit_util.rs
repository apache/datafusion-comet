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

use std::{mem::size_of, time::Duration};

use rand::{rng, Rng};

use arrow::buffer::Buffer;
use comet::common::bit::{
    log2, read_num_bytes_u32, read_num_bytes_u64, read_u32, read_u64, set_bits, trailing_bits,
    BitReader, BitWriter,
};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

/// Benchmark to measure bit_util performance.
/// To run this benchmark:
/// `cd core && cargo bench --bench bit_util`
/// Results will be written to "core/target/criterion/bit_util/"
fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("bit_util");

    const N: usize = 1024 * 1024;
    let mut writer: BitWriter = BitWriter::new(N * 10);
    for _ in 0..N {
        if !writer.put_vlq_int(rng().random::<u64>()) {
            break;
        }
    }
    let buffer = writer.consume();
    let buffer = Buffer::from(buffer.as_slice());

    // log2
    for bits in (0..64).step_by(3) {
        let x = 1u64 << bits;
        group.bench_with_input(BenchmarkId::new("log2", bits), &x, |b, &x| {
            b.iter(|| log2(black_box(x)));
        });
    }

    // set_bits
    for offset in (0..16).step_by(3) {
        for length in (0..16).step_by(3) {
            let x = (offset, length);
            group.bench_with_input(
                BenchmarkId::new("set_bits", format!("offset_{}_length_{}", x.0, x.1)),
                &x,
                |b, &x| {
                    b.iter(|| set_bits(&mut [0u8; 4], black_box(x.0), black_box(x.1)));
                },
            );
        }
    }

    // get_vlq_int
    group.bench_function("get_vlq_int", |b| {
        b.iter(|| {
            let mut reader: BitReader = BitReader::new_all(buffer.slice(0));
            bench_get_vlq_int(&mut reader)
        })
    });

    // get_bits
    for offset in (0..32).step_by(17) {
        for num_bits in (1..5).step_by(1) {
            let x = (offset, num_bits);
            group.bench_with_input(
                BenchmarkId::new("get_bits", format!("offset_{}_num_bits_{}", x.0, x.1)),
                &x,
                |b, &x| {
                    let mut reader: BitReader = BitReader::new_all(buffer.slice(0));
                    b.iter(|| reader.get_bits(&mut [0u8; 4], black_box(x.0), black_box(x.1)));
                },
            );
        }
    }

    // get_aligned
    for num_bytes in (1..=size_of::<u8>()).step_by(3) {
        let x = num_bytes;
        group.bench_with_input(
            BenchmarkId::new("get_aligned", format!("u8_num_bytes_{x}")),
            &x,
            |b, &x| {
                let mut reader: BitReader = BitReader::new_all(buffer.slice(0));
                b.iter(|| reader.get_aligned::<u8>(black_box(x)));
            },
        );
    }
    for num_bytes in (1..=size_of::<u32>()).step_by(3) {
        let x = num_bytes;
        group.bench_with_input(
            BenchmarkId::new("get_aligned", format!("u32_num_bytes_{x}")),
            &x,
            |b, &x| {
                let mut reader: BitReader = BitReader::new_all(buffer.slice(0));
                b.iter(|| reader.get_aligned::<u32>(black_box(x)));
            },
        );
    }
    for num_bytes in (1..=size_of::<i32>()).step_by(3) {
        let x = num_bytes;
        group.bench_with_input(
            BenchmarkId::new("get_aligned", format!("i32_num_bytes_{x}")),
            &x,
            |b, &x| {
                let mut reader: BitReader = BitReader::new_all(buffer.slice(0));
                b.iter(|| reader.get_aligned::<i32>(black_box(x)));
            },
        );
    }

    // get_value
    for num_bytes in (1..=size_of::<i32>()).step_by(3) {
        let x = num_bytes * 8;
        group.bench_with_input(
            BenchmarkId::new("get_value", format!("i32_num_bits_{x}")),
            &x,
            |b, &x| {
                let mut reader: BitReader = BitReader::new_all(buffer.slice(0));
                b.iter(|| reader.get_value::<i32>(black_box(x)));
            },
        );
    }

    // read_num_bytes_u64
    for num_bytes in (1..=8).step_by(7) {
        let x = num_bytes;
        group.bench_with_input(
            BenchmarkId::new("read_num_bytes_u64", format!("num_bytes_{x}")),
            &x,
            |b, &x| {
                b.iter(|| read_num_bytes_u64(black_box(x), black_box(buffer.as_slice())));
            },
        );
    }

    // read_num_bytes_u32
    for num_bytes in (1..=4).step_by(3) {
        let x = num_bytes;
        group.bench_with_input(
            BenchmarkId::new("read_num_bytes_u32", format!("num_bytes_{x}")),
            &x,
            |b, &x| {
                b.iter(|| read_num_bytes_u32(black_box(x), black_box(buffer.as_slice())));
            },
        );
    }

    // trailing_bits
    for length in (0..=64).step_by(32) {
        let x = length;
        group.bench_with_input(
            BenchmarkId::new("trailing_bits", format!("num_bits_{x}")),
            &x,
            |b, &x| {
                b.iter(|| trailing_bits(black_box(1234567890), black_box(x)));
            },
        );
    }

    // read_u64
    group.bench_function("read_u64", |b| {
        b.iter(|| read_u64(black_box(&[0u8; 8])));
    });

    // read_u32
    group.bench_function("read_u32", |b| {
        b.iter(|| read_u32(black_box(&[0u8; 4])));
    });

    // get_u32_value
    group.bench_function("get_u32_value", |b| {
        b.iter(|| {
            let mut reader: BitReader = BitReader::new_all(buffer.slice(0));
            for _ in 0..(buffer.len() * 8 / 31) {
                black_box(reader.get_u32_value(black_box(31)));
            }
        })
    });

    group.finish();
}

fn bench_get_vlq_int(reader: &mut BitReader) {
    while let Some(v) = reader.get_vlq_int() {
        black_box(v);
    }
}

fn config() -> Criterion {
    Criterion::default()
        .measurement_time(Duration::from_millis(500))
        .warm_up_time(Duration::from_millis(500))
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
