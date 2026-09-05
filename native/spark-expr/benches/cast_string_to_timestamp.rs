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

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, TimeUnit};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;

const BATCH_SIZE: usize = 8192;

fn criterion_benchmark(c: &mut Criterion) {
    let expr = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;

    // Input shapes, chosen to cover each branch `timestamp_parser` can take: the canonical
    // form, the fractional-second form, an offset suffix (which takes the extract-offset
    // path), a date-only string, whitespace padding (the trim), and a mix that includes
    // invalid values so the null path is measured too.
    let batches = [
        (
            "single_digit_segments",
            create_batch(|i| format!("2020-{:02}-{}T1:2:3", i % 12 + 1, i % 9 + 1)),
        ),
        (
            "empty_fraction",
            create_batch(|i| format!("2020-01-{:02}T12:34:56.", i % 28 + 1)),
        ),
        (
            "date_only_zone",
            create_batch(|i| format!("2020-01-{:02}Z", i % 28 + 1)),
        ),
        (
            "canonical",
            create_batch(|i| {
                format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    1970 + i % 60,
                    i % 12 + 1,
                    i % 28 + 1,
                    i % 24,
                    i % 60,
                    i % 60
                )
            }),
        ),
        (
            "microseconds",
            create_batch(|i| {
                format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                    1970 + i % 60,
                    i % 12 + 1,
                    i % 28 + 1,
                    i % 24,
                    i % 60,
                    i % 60,
                    i % 1_000_000
                )
            }),
        ),
        (
            "offset_suffix",
            create_batch(|i| {
                format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}+05:30",
                    1970 + i % 60,
                    i % 12 + 1,
                    i % 28 + 1,
                    i % 24,
                    i % 60,
                    i % 60
                )
            }),
        ),
        (
            "date_only",
            create_batch(|i| format!("{:04}-{:02}-{:02}", 1970 + i % 60, i % 12 + 1, i % 28 + 1)),
        ),
        (
            "padded",
            create_batch(|i| {
                format!(
                    "  {:04}-{:02}-{:02} {:02}:00:00  ",
                    1970 + i % 60,
                    i % 12 + 1,
                    i % 28 + 1,
                    i % 24
                )
            }),
        ),
        (
            "mixed",
            create_batch(|i| match i % 5 {
                0 => format!(
                    "{:04}-{:02}-{:02} 12:34:56",
                    1970 + i % 60,
                    i % 12 + 1,
                    i % 28 + 1
                ),
                1 => format!(
                    "{:04}-{:02}-{:02}T12:34:56.123456Z",
                    1970 + i % 60,
                    i % 12 + 1,
                    i % 28 + 1
                ),
                2 => format!(
                    "  {:04}-{:02}-{:02}  ",
                    1900 + i % 200,
                    i % 12 + 1,
                    i % 28 + 1
                ),
                3 => "T12:34:56".to_string(),
                _ => "not a timestamp".to_string(),
            }),
        ),
    ];

    // Timezone-aware and NTZ go through different parsers (`timestamp_parser` vs
    // `timestamp_ntz_parser`), and a non-UTC session timezone exercises the offset lookup that
    // UTC short-circuits, so all three are measured.
    for (target_name, to_type, timezone) in [
        (
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "UTC",
        ),
        (
            "timestamp_non_utc",
            DataType::Timestamp(TimeUnit::Microsecond, Some("America/Los_Angeles".into())),
            "America/Los_Angeles",
        ),
        (
            "timestamp_ntz",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            "UTC",
        ),
    ] {
        for (mode, mode_name) in [
            (EvalMode::Legacy, "legacy"),
            (EvalMode::Ansi, "ansi"),
            (EvalMode::Try, "try"),
        ] {
            let mut group =
                c.benchmark_group(format!("cast_string_to_{}/{}", target_name, mode_name));
            for (name, batch) in &batches {
                // ANSI raises on the first invalid value, so timing it against a batch that is
                // mostly invalid would measure the error path rather than the parser.
                if mode == EvalMode::Ansi && matches!(*name, "mixed" | "date_only_zone") {
                    continue;
                }
                let cast = Cast::new(
                    Arc::clone(&expr),
                    to_type.clone(),
                    SparkCastOptions::new(mode, timezone, false),
                    None,
                    None,
                );
                group.bench_function(*name, |b| {
                    b.iter(|| cast.evaluate(batch).unwrap());
                });
            }
            group.finish();
        }
    }

    // The Spark 4 path adds a leading-whitespace check for T-prefixed time-only strings, so it
    // is measured separately on the inputs where that check can fire.
    let mut group = c.benchmark_group("cast_string_to_timestamp/spark4_legacy");
    for name in ["padded", "mixed"] {
        let batch = &batches.iter().find(|(n, _)| *n == name).unwrap().1;
        let cast = Cast::new(
            Arc::clone(&expr),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            SparkCastOptions::new_with_version(EvalMode::Legacy, "UTC", false, true),
            None,
            None,
        );
        group.bench_function(name, |b| {
            b.iter(|| cast.evaluate(batch).unwrap());
        });
    }
    group.finish();
}

fn create_batch(f: impl Fn(usize) -> String) -> RecordBatch {
    common::string_batch(BATCH_SIZE, 17, f)
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
