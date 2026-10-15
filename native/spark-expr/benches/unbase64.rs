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
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_unbase64;
use std::hint::black_box;
use std::sync::Arc;

const LINE_LEN: usize = 76;

fn unwrapped(bytes: &[u8]) -> String {
    BASE64_STANDARD.encode(bytes)
}

/// Reproduces Spark's default `base64` output shape: MIME-wrapped at 76 chars with CRLF.
fn crlf_wrapped(bytes: &[u8]) -> String {
    let encoded = unwrapped(bytes);
    encoded
        .as_bytes()
        .chunks(LINE_LEN)
        .map(|line| std::str::from_utf8(line).unwrap())
        .collect::<Vec<_>>()
        .join("\r\n")
}

/// Density of null placement, expressed as "1 null every `stride` rows". Use `usize::MAX` for
/// no-null batches. `stride == 1` produces the all-null batch.
enum NullDensity {
    None,
    /// One null every `stride` rows. `stride == 10` is sparse, `stride == 2` is dense.
    Every(usize),
    All,
}

fn create_string_array(size: usize, value: &str, nulls: NullDensity) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for i in 0..size {
        let is_null = match nulls {
            NullDensity::None => false,
            NullDensity::All => true,
            NullDensity::Every(stride) => i % stride == 0,
        };
        if is_null {
            builder.append_null();
        } else {
            builder.append_value(value);
        }
    }
    Arc::new(builder.finish())
}

/// Builds a batch of mostly-valid inputs sprinkled with malformed values that trip
/// `Input byte array has wrong 4-byte ending unit`. The kernel returns early on the first bad
/// row, so this bench measures the error-return path (partial decode + Result short-circuit)
/// rather than the happy path.
fn create_error_shaped_array(size: usize, valid: &str) -> ArrayRef {
    let mut builder = StringBuilder::new();
    // First row is the malformed one so the kernel short-circuits immediately.
    builder.append_value("YW=");
    for _ in 1..size {
        builder.append_value(valid);
    }
    Arc::new(builder.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let short_bytes = vec![b'z'; 16];
    let long_bytes = vec![b'q'; 200];

    // Sparse-null (one every 10 rows) — the shape the previous bench measured, kept as the
    // default null density for the short / long / tiny cases.
    let short = create_string_array(size, &unwrapped(&short_bytes), NullDensity::Every(10));
    let long_clean = create_string_array(size, &unwrapped(&long_bytes), NullDensity::Every(10));
    // Long CRLF-wrapped values: matches `unbase64(base64(x))` when Spark's default
    // `spark.sql.chunkBase64String.enabled = true` is in effect (also Comet's default).
    let long_wrapped =
        create_string_array(size, &crlf_wrapped(&long_bytes), NullDensity::Every(10));
    // A batch dominated by tiny values, one per row (worst case for per-row overhead).
    let tiny = create_string_array(size, &unwrapped(b"a"), NullDensity::Every(10));

    // No-nulls / dense-nulls shapes on the long-single-line payload isolate the null-append
    // branch from the decode branch. Dense-null uses stride 2 (~50% nulls) rather than an
    // all-null shape so the decoder still runs on half the rows.
    let long_no_nulls = create_string_array(size, &unwrapped(&long_bytes), NullDensity::None);
    let long_dense_nulls =
        create_string_array(size, &unwrapped(&long_bytes), NullDensity::Every(2));
    let long_all_nulls = create_string_array(size, &unwrapped(&long_bytes), NullDensity::All);

    // Error-path shape: the first row throws, the rest are valid but never decoded. Measures the
    // early-return path independent of decode throughput.
    let error_first = create_error_shaped_array(size, &unwrapped(&long_bytes));

    c.bench_function("spark_unbase64: short", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&short))];
        b.iter(|| black_box(spark_unbase64(black_box(&args)).unwrap()))
    });

    c.bench_function("spark_unbase64: long, single line", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&long_clean))];
        b.iter(|| black_box(spark_unbase64(black_box(&args)).unwrap()))
    });

    c.bench_function("spark_unbase64: long, CRLF-wrapped", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&long_wrapped))];
        b.iter(|| black_box(spark_unbase64(black_box(&args)).unwrap()))
    });

    c.bench_function("spark_unbase64: tiny values", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&tiny))];
        b.iter(|| black_box(spark_unbase64(black_box(&args)).unwrap()))
    });

    c.bench_function("spark_unbase64: long, no nulls", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&long_no_nulls))];
        b.iter(|| black_box(spark_unbase64(black_box(&args)).unwrap()))
    });

    c.bench_function("spark_unbase64: long, dense nulls (50%)", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&long_dense_nulls))];
        b.iter(|| black_box(spark_unbase64(black_box(&args)).unwrap()))
    });

    c.bench_function("spark_unbase64: long, all nulls", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&long_all_nulls))];
        b.iter(|| black_box(spark_unbase64(black_box(&args)).unwrap()))
    });

    c.bench_function("spark_unbase64: error on first row", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&error_first))];
        b.iter(|| {
            let result = spark_unbase64(black_box(&args));
            debug_assert!(result.is_err());
            black_box(result.err());
        })
    });

    c.bench_function("spark_unbase64: scalar literal", |b| {
        let arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some(unwrapped(&long_bytes))));
        b.iter(|| black_box(spark_unbase64(black_box(std::slice::from_ref(&arg))).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
