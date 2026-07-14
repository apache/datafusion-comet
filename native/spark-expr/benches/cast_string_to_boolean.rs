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

use arrow::array::{builder::StringBuilder, ArrayRef};
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_cast, EvalMode, SparkCastOptions};
use std::sync::Arc;

const NUM_ROWS: usize = 8192;

/// Strings that Spark accepts as booleans, in the mixed casing and padding that shows
/// up in real data.
const VALID: [&str; 12] = [
    "true", "TRUE", "True", "false", "FALSE", " t ", "y", "YES", "no", "N", "1", "0",
];

/// A mix of valid boolean spellings and values that cast to NULL.
fn mixed_batch() -> ArrayRef {
    let mut b = StringBuilder::new();
    for i in 0..NUM_ROWS {
        match i % 10 {
            0 => b.append_null(),
            1 => b.append_value("maybe"),
            2 => b.append_value("2"),
            _ => b.append_value(VALID[i % VALID.len()]),
        }
    }
    Arc::new(b.finish())
}

/// Only valid boolean spellings, so every row takes the parsing path.
fn valid_batch() -> ArrayRef {
    let mut b = StringBuilder::new();
    for i in 0..NUM_ROWS {
        b.append_value(VALID[i % VALID.len()]);
    }
    Arc::new(b.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let mixed = mixed_batch();
    let valid = valid_batch();

    let mut group = c.benchmark_group("cast_string_to_boolean");
    for (mode, mode_name) in [
        (EvalMode::Legacy, "legacy"),
        (EvalMode::Try, "try"),
        (EvalMode::Ansi, "ansi"),
    ] {
        let options = SparkCastOptions::new(mode, "", false);
        // ANSI mode errors on any unparseable value, so it only gets the valid batch.
        if mode != EvalMode::Ansi {
            group.bench_function(format!("{mode_name}/mixed"), |b| {
                b.iter(|| {
                    spark_cast(
                        ColumnarValue::Array(Arc::clone(&mixed)),
                        &DataType::Boolean,
                        &options,
                    )
                    .unwrap()
                });
            });
        }
        group.bench_function(format!("{mode_name}/valid"), |b| {
            b.iter(|| {
                spark_cast(
                    ColumnarValue::Array(Arc::clone(&valid)),
                    &DataType::Boolean,
                    &options,
                )
                .unwrap()
            });
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
