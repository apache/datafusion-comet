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

//! Benchmarks for SparkUnsafeArray to Arrow array conversion.
//! This specifically tests the append_to_builder function used in shuffle read path.

use arrow::array::builder::{ArrayBuilder, Int32Builder, Int64Builder, Float64Builder, Date32Builder, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, TimeUnit};
use comet::execution::shuffle::list::{append_to_builder, SparkUnsafeArray};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

const NUM_ELEMENTS: usize = 10000;

/// Create a SparkUnsafeArray in memory with i32 elements.
/// Layout:
/// - 8 bytes: num_elements (i64)
/// - null bitset: 8 bytes per 64 elements
/// - element data: 4 bytes per element (i32)
fn create_spark_unsafe_array_i32(num_elements: usize, with_nulls: bool) -> Vec<u8> {
    // Header size: 8 (num_elements) + ceil(num_elements/64) * 8 (null bitset)
    let null_bitset_words = (num_elements + 63) / 64;
    let header_size = 8 + null_bitset_words * 8;
    let data_size = num_elements * 4; // i32 = 4 bytes
    let total_size = header_size + data_size;

    let mut buffer = vec![0u8; total_size];

    // Write num_elements
    buffer[0..8].copy_from_slice(&(num_elements as i64).to_le_bytes());

    // Write null bitset (set every 10th element as null if with_nulls)
    if with_nulls {
        for i in (0..num_elements).step_by(10) {
            let word_idx = i / 64;
            let bit_idx = i % 64;
            let word_offset = 8 + word_idx * 8;
            let current_word = i64::from_le_bytes(buffer[word_offset..word_offset + 8].try_into().unwrap());
            let new_word = current_word | (1i64 << bit_idx);
            buffer[word_offset..word_offset + 8].copy_from_slice(&new_word.to_le_bytes());
        }
    }

    // Write element data
    for i in 0..num_elements {
        let offset = header_size + i * 4;
        buffer[offset..offset + 4].copy_from_slice(&(i as i32).to_le_bytes());
    }

    buffer
}

/// Create a SparkUnsafeArray in memory with i64 elements.
fn create_spark_unsafe_array_i64(num_elements: usize, with_nulls: bool) -> Vec<u8> {
    let null_bitset_words = (num_elements + 63) / 64;
    let header_size = 8 + null_bitset_words * 8;
    let data_size = num_elements * 8; // i64 = 8 bytes
    let total_size = header_size + data_size;

    let mut buffer = vec![0u8; total_size];

    // Write num_elements
    buffer[0..8].copy_from_slice(&(num_elements as i64).to_le_bytes());

    // Write null bitset
    if with_nulls {
        for i in (0..num_elements).step_by(10) {
            let word_idx = i / 64;
            let bit_idx = i % 64;
            let word_offset = 8 + word_idx * 8;
            let current_word = i64::from_le_bytes(buffer[word_offset..word_offset + 8].try_into().unwrap());
            let new_word = current_word | (1i64 << bit_idx);
            buffer[word_offset..word_offset + 8].copy_from_slice(&new_word.to_le_bytes());
        }
    }

    // Write element data
    for i in 0..num_elements {
        let offset = header_size + i * 8;
        buffer[offset..offset + 8].copy_from_slice(&(i as i64).to_le_bytes());
    }

    buffer
}

/// Create a SparkUnsafeArray in memory with f64 elements.
fn create_spark_unsafe_array_f64(num_elements: usize, with_nulls: bool) -> Vec<u8> {
    let null_bitset_words = (num_elements + 63) / 64;
    let header_size = 8 + null_bitset_words * 8;
    let data_size = num_elements * 8; // f64 = 8 bytes
    let total_size = header_size + data_size;

    let mut buffer = vec![0u8; total_size];

    // Write num_elements
    buffer[0..8].copy_from_slice(&(num_elements as i64).to_le_bytes());

    // Write null bitset
    if with_nulls {
        for i in (0..num_elements).step_by(10) {
            let word_idx = i / 64;
            let bit_idx = i % 64;
            let word_offset = 8 + word_idx * 8;
            let current_word = i64::from_le_bytes(buffer[word_offset..word_offset + 8].try_into().unwrap());
            let new_word = current_word | (1i64 << bit_idx);
            buffer[word_offset..word_offset + 8].copy_from_slice(&new_word.to_le_bytes());
        }
    }

    // Write element data
    for i in 0..num_elements {
        let offset = header_size + i * 8;
        buffer[offset..offset + 8].copy_from_slice(&(i as f64).to_le_bytes());
    }

    buffer
}

fn benchmark_array_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("spark_unsafe_array_to_arrow");

    // Benchmark i32 array conversion
    for with_nulls in [false, true] {
        let buffer = create_spark_unsafe_array_i32(NUM_ELEMENTS, with_nulls);
        let array = SparkUnsafeArray::new(buffer.as_ptr() as i64);
        let null_str = if with_nulls { "with_nulls" } else { "no_nulls" };

        group.bench_with_input(
            BenchmarkId::new("i32", null_str),
            &(&array, &buffer),
            |b, (array, _buffer)| {
                b.iter(|| {
                    let mut builder = Int32Builder::with_capacity(NUM_ELEMENTS);
                    if with_nulls {
                        append_to_builder::<true>(&DataType::Int32, &mut builder, array).unwrap();
                    } else {
                        append_to_builder::<false>(&DataType::Int32, &mut builder, array).unwrap();
                    }
                    builder.finish()
                });
            },
        );
    }

    // Benchmark i64 array conversion
    for with_nulls in [false, true] {
        let buffer = create_spark_unsafe_array_i64(NUM_ELEMENTS, with_nulls);
        let array = SparkUnsafeArray::new(buffer.as_ptr() as i64);
        let null_str = if with_nulls { "with_nulls" } else { "no_nulls" };

        group.bench_with_input(
            BenchmarkId::new("i64", null_str),
            &(&array, &buffer),
            |b, (array, _buffer)| {
                b.iter(|| {
                    let mut builder = Int64Builder::with_capacity(NUM_ELEMENTS);
                    if with_nulls {
                        append_to_builder::<true>(&DataType::Int64, &mut builder, array).unwrap();
                    } else {
                        append_to_builder::<false>(&DataType::Int64, &mut builder, array).unwrap();
                    }
                    builder.finish()
                });
            },
        );
    }

    // Benchmark f64 array conversion
    for with_nulls in [false, true] {
        let buffer = create_spark_unsafe_array_f64(NUM_ELEMENTS, with_nulls);
        let array = SparkUnsafeArray::new(buffer.as_ptr() as i64);
        let null_str = if with_nulls { "with_nulls" } else { "no_nulls" };

        group.bench_with_input(
            BenchmarkId::new("f64", null_str),
            &(&array, &buffer),
            |b, (array, _buffer)| {
                b.iter(|| {
                    let mut builder = Float64Builder::with_capacity(NUM_ELEMENTS);
                    if with_nulls {
                        append_to_builder::<true>(&DataType::Float64, &mut builder, array).unwrap();
                    } else {
                        append_to_builder::<false>(&DataType::Float64, &mut builder, array).unwrap();
                    }
                    builder.finish()
                });
            },
        );
    }

    // Benchmark date32 array conversion (same memory layout as i32)
    for with_nulls in [false, true] {
        let buffer = create_spark_unsafe_array_i32(NUM_ELEMENTS, with_nulls);
        let array = SparkUnsafeArray::new(buffer.as_ptr() as i64);
        let null_str = if with_nulls { "with_nulls" } else { "no_nulls" };

        group.bench_with_input(
            BenchmarkId::new("date32", null_str),
            &(&array, &buffer),
            |b, (array, _buffer)| {
                b.iter(|| {
                    let mut builder = Date32Builder::with_capacity(NUM_ELEMENTS);
                    if with_nulls {
                        append_to_builder::<true>(&DataType::Date32, &mut builder, array).unwrap();
                    } else {
                        append_to_builder::<false>(&DataType::Date32, &mut builder, array).unwrap();
                    }
                    builder.finish()
                });
            },
        );
    }

    // Benchmark timestamp array conversion (same memory layout as i64)
    for with_nulls in [false, true] {
        let buffer = create_spark_unsafe_array_i64(NUM_ELEMENTS, with_nulls);
        let array = SparkUnsafeArray::new(buffer.as_ptr() as i64);
        let null_str = if with_nulls { "with_nulls" } else { "no_nulls" };

        group.bench_with_input(
            BenchmarkId::new("timestamp", null_str),
            &(&array, &buffer),
            |b, (array, _buffer)| {
                b.iter(|| {
                    let mut builder = TimestampMicrosecondBuilder::with_capacity(NUM_ELEMENTS);
                    let dt = DataType::Timestamp(TimeUnit::Microsecond, None);
                    if with_nulls {
                        append_to_builder::<true>(&dt, &mut builder, array).unwrap();
                    } else {
                        append_to_builder::<false>(&dt, &mut builder, array).unwrap();
                    }
                    builder.finish()
                });
            },
        );
    }

    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = benchmark_array_conversion
}
criterion_main!(benches);
