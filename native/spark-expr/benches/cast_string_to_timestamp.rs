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

use arrow::array::{builder::StringBuilder, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

/// Builds the cast expression for `STRING -> TIMESTAMP` in the given session timezone.
fn cast_to_timestamp(timezone: &str) -> Cast {
    Cast::new(
        Arc::new(Column::new("a", 0)),
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.into())),
        SparkCastOptions::new(EvalMode::Legacy, timezone, false),
        None,
        None,
    )
}

/// Builds the cast expression for `STRING -> TIMESTAMP_NTZ`.
fn cast_to_timestamp_ntz() -> Cast {
    Cast::new(
        Arc::new(Column::new("a", 0)),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        None,
        None,
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    // Shapes that all take the "direct pattern match" path.
    let canonical = batch(Nulls::None, |i| {
        format!(
            "{:04}-{:02}-{:02} 12:34:56",
            1970 + i % 60,
            i % 12 + 1,
            i % 28 + 1
        )
    });
    let micros = batch(Nulls::None, |i| {
        format!(
            "{:04}-{:02}-{:02}T12:34:56.{:06}",
            1970 + i % 60,
            i % 12 + 1,
            i % 28 + 1,
            i % 1_000_000
        )
    });
    let date_only = batch(Nulls::None, |i| {
        format!("{:04}-{:02}-{:02}", 1970 + i % 60, i % 12 + 1, i % 28 + 1)
    });
    let time_only = batch(Nulls::None, |i| {
        format!("T{:02}:{:02}:{:02}", i % 24, i % 60, i % 60)
    });

    // Shapes that fall through the direct-match check into timezone-suffix extraction.
    let iso_z = batch(Nulls::None, |i| {
        format!(
            "{:04}-{:02}-{:02}T12:34:56Z",
            1970 + i % 60,
            i % 12 + 1,
            i % 28 + 1
        )
    });
    let named_tz = batch(Nulls::None, |i| {
        format!(
            "{:04}-{:02}-{:02}T12:34:56 Europe/Moscow",
            1970 + i % 60,
            i % 12 + 1,
            i % 28 + 1
        )
    });
    let invalid = batch(Nulls::None, |_| "not a timestamp".to_string());

    // Null density: the parser is only called for non-null slots, so a dense-null batch
    // must not get slower.
    let sparse_nulls = batch(Nulls::Sparse, |i| {
        format!(
            "{:04}-{:02}-{:02} 12:34:56",
            1970 + i % 60,
            i % 12 + 1,
            i % 28 + 1
        )
    });
    let dense_nulls = batch(Nulls::Dense, |i| {
        format!(
            "{:04}-{:02}-{:02} 12:34:56",
            1970 + i % 60,
            i % 12 + 1,
            i % 28 + 1
        )
    });

    // Non-ASCII digits: matched by the Unicode-aware `\d` in the regex patterns.
    let non_ascii = batch(Nulls::None, |_| "٢٠٢٠-٠١-٠١".to_string());

    let mixed = batch(Nulls::Sparse, |i| match i % 6 {
        0 => format!("{:04}-{:02}-{:02} 12:34:56", 1970 + i % 60, i % 12 + 1, 1),
        1 => format!("{:04}-{:02}-{:02}T12:34:56.123456Z", 1970 + i % 60, 1, 1),
        2 => format!("{:04}-{:02}-{:02}", 1970 + i % 60, i % 12 + 1, 1),
        3 => format!("  {:04}-{:02}-{:02} 01:02:03  ", 1970 + i % 60, 1, 1),
        4 => format!("{:04}", 1970 + i % 60),
        _ => "garbage".to_string(),
    });

    let utc = cast_to_timestamp("UTC");
    let mut group = c.benchmark_group("cast_string_to_timestamp");
    for (name, batch) in [
        ("canonical", &canonical),
        ("microseconds", &micros),
        ("date_only", &date_only),
        ("time_only", &time_only),
        ("iso_z_suffix", &iso_z),
        ("named_tz_suffix", &named_tz),
        ("invalid", &invalid),
        ("sparse_nulls", &sparse_nulls),
        ("dense_nulls", &dense_nulls),
        ("non_ascii_digits", &non_ascii),
        ("mixed", &mixed),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| utc.evaluate(batch).unwrap());
        });
    }
    group.finish();

    // A named zone exercises the DST-aware local-time resolution path.
    let named_zone = cast_to_timestamp("America/New_York");
    let mut group = c.benchmark_group("cast_string_to_timestamp_named_zone");
    group.bench_function("canonical", |b| {
        b.iter(|| named_zone.evaluate(&canonical).unwrap());
    });
    group.finish();

    let ntz = cast_to_timestamp_ntz();
    let mut group = c.benchmark_group("cast_string_to_timestamp_ntz");
    for (name, batch) in [
        ("canonical", &canonical),
        ("microseconds", &micros),
        ("date_only", &date_only),
        ("iso_z_suffix", &iso_z),
        ("invalid", &invalid),
        ("dense_nulls", &dense_nulls),
        ("mixed", &mixed),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| ntz.evaluate(batch).unwrap());
        });
    }
    group.finish();
}

/// Null densities used by the benchmark shapes.
#[derive(Copy, Clone)]
enum Nulls {
    /// Every slot is valid.
    None,
    /// Roughly 1 null in 17 slots.
    Sparse,
    /// Roughly 9 nulls in 10 slots.
    Dense,
}

impl Nulls {
    fn is_null(self, i: usize) -> bool {
        match self {
            Nulls::None => false,
            Nulls::Sparse => i.is_multiple_of(17),
            Nulls::Dense => !i.is_multiple_of(10),
        }
    }
}

/// Builds a batch of `BATCH_SIZE` strings from `f` at the requested null density.
fn batch(nulls: Nulls, f: impl Fn(usize) -> String) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let mut builder = StringBuilder::new();
    for i in 0..BATCH_SIZE {
        if nulls.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(f(i));
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap()
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
