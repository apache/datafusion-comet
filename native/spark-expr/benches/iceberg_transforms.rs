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

//! Iceberg's system functions (`bucket`, `truncate`, `years`, `months`, `days`, `hours`) over one
//! input array per supported type, with and without nulls, plus the dictionary-encoded string
//! shape a Parquet scan produces for a partition column.

use arrow::array::{
    ArrayRef, BinaryArray, Date32Array, Decimal128Array, DictionaryArray, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Int32Type};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::common::ScalarValue;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::{
    SparkIcebergBucket, SparkIcebergTemporalTransform, SparkIcebergTruncate,
};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8_192;
const MICROS_PER_DAY: i64 = 86_400_000_000;
/// Every eighth row is null, matching the corpus the correctness suite writes.
const NULL_STRIDE: usize = 8;

fn maybe_null<T>(i: usize, nulls: bool, value: T) -> Option<T> {
    if nulls && i.is_multiple_of(NULL_STRIDE) {
        None
    } else {
        Some(value)
    }
}

/// A deterministic 64-bit value per row; the transforms are data dependent (bucket hashes it,
/// truncate divides by it), so a constant column would not be representative.
fn spread(i: usize) -> i64 {
    (i as i64)
        .wrapping_mul(6_364_136_223_846_793_005)
        .rotate_left(17)
}

/// The words a low-cardinality string partition column holds; row `i` picks `i % len`.
const WORDS: [&str; 8] = [
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "日本語テキスト",
    "a😀b😀c",
];

fn inputs(nulls: bool) -> Vec<(&'static str, ArrayRef)> {
    let strings: StringArray = (0..ROWS)
        .map(|i| maybe_null(i, nulls, WORDS[i % WORDS.len()]))
        .collect();
    let dictionary: DictionaryArray<Int32Type> = (0..ROWS)
        .map(|i| maybe_null(i, nulls, WORDS[i % WORDS.len()]))
        .collect();
    let binaries: BinaryArray = (0..ROWS)
        .map(|i| maybe_null(i, nulls, spread(i).to_be_bytes()))
        .collect();
    vec![
        (
            "int",
            Arc::new(
                (0..ROWS)
                    .map(|i| maybe_null(i, nulls, spread(i) as i32))
                    .collect::<Int32Array>(),
            ),
        ),
        (
            "long",
            Arc::new(
                (0..ROWS)
                    .map(|i| maybe_null(i, nulls, spread(i)))
                    .collect::<Int64Array>(),
            ),
        ),
        (
            "decimal38",
            Arc::new(
                (0..ROWS)
                    .map(|i| maybe_null(i, nulls, (spread(i) as i128) * 1_000_000_000))
                    .collect::<Decimal128Array>()
                    .with_precision_and_scale(38, 10)
                    .unwrap(),
            ),
        ),
        ("string", Arc::new(strings)),
        ("string_dict", Arc::new(dictionary)),
        ("binary", Arc::new(binaries)),
        (
            "date",
            Arc::new(
                (0..ROWS)
                    .map(|i| maybe_null(i, nulls, (spread(i) % 40_000) as i32))
                    .collect::<Date32Array>(),
            ),
        ),
        (
            "timestamp",
            Arc::new(
                (0..ROWS)
                    .map(|i| maybe_null(i, nulls, spread(i) % (40_000 * MICROS_PER_DAY)))
                    .collect::<TimestampMicrosecondArray>()
                    .with_timezone("UTC"),
            ),
        ),
    ]
}

fn invoke(udf: &dyn ScalarUDFImpl, args: &[ColumnarValue]) -> ArrayRef {
    let arg_fields: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(i, a)| Arc::new(Field::new(format!("arg{i}"), a.data_type(), true)))
        .collect();
    let arg_types: Vec<DataType> = arg_fields.iter().map(|f| f.data_type().clone()).collect();
    let return_type = udf.return_type(&arg_types).unwrap();
    udf.invoke_with_args(ScalarFunctionArgs {
        args: args.to_vec(),
        arg_fields,
        number_rows: ROWS,
        return_field: Arc::new(Field::new(udf.name(), return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .unwrap()
    .to_array(ROWS)
    .unwrap()
}

/// `true` when `udf` accepts `input`; the transforms are typed the same way Iceberg's `bind` is,
/// so the type matrix is sparse.
fn supported(udf: &dyn ScalarUDFImpl, args: &[ColumnarValue]) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let arg_fields: Vec<_> = args
            .iter()
            .enumerate()
            .map(|(i, a)| Arc::new(Field::new(format!("arg{i}"), a.data_type(), true)))
            .collect();
        let arg_types: Vec<DataType> = arg_fields.iter().map(|f| f.data_type().clone()).collect();
        let Ok(return_type) = udf.return_type(&arg_types) else {
            return false;
        };
        udf.invoke_with_args(ScalarFunctionArgs {
            args: args.to_vec(),
            arg_fields,
            number_rows: ROWS,
            return_field: Arc::new(Field::new(udf.name(), return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .is_ok()
    }))
    .unwrap_or(false)
}

fn criterion_benchmark(c: &mut Criterion) {
    let bucket = SparkIcebergBucket::new();
    let truncate = SparkIcebergTruncate::new();
    let years = SparkIcebergTemporalTransform::years();
    let months = SparkIcebergTemporalTransform::months();
    let days = SparkIcebergTemporalTransform::days();
    let hours = SparkIcebergTemporalTransform::hours();

    // (name, udf, parameter) -- `bucket` and `truncate` take a literal first argument.
    let transforms: Vec<(&str, &dyn ScalarUDFImpl, Option<i32>)> = vec![
        ("iceberg_bucket", &bucket, Some(16)),
        ("iceberg_truncate", &truncate, Some(4)),
        ("iceberg_years", &years, None),
        ("iceberg_months", &months, None),
        ("iceberg_days", &days, None),
        ("iceberg_hours", &hours, None),
    ];

    for (name, udf, parameter) in transforms {
        let mut group = c.benchmark_group(name);
        group.throughput(Throughput::Elements(ROWS as u64));
        for (nulls, null_tag) in [(false, "no_nulls"), (true, "sparse_nulls")] {
            for (type_tag, array) in inputs(nulls) {
                let args: Vec<ColumnarValue> = parameter
                    .map(|p| ColumnarValue::Scalar(ScalarValue::Int32(Some(p))))
                    .into_iter()
                    .chain([ColumnarValue::Array(array)])
                    .collect();
                if !supported(udf, &args) {
                    continue;
                }
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!("{type_tag}/{null_tag}")),
                    &args,
                    |b, args| b.iter(|| black_box(invoke(udf, black_box(args)))),
                );
            }
        }
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
