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

use arrow::array::{
    Array, DictionaryArray, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_expr::expressions::{lit, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::TimestampTruncExpr;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{is_null, timestamp_micros_array, NULL_RATIOS, ROW_COUNTS};

// Cover the default UTC path and non-UTC timezone resolution with identical input shapes.
const MICROS_PER_DAY: i64 = 86_400_000_000;
const BASE_MICROS: i64 = 1_704_067_200_123_456; // 2024-01-01T00:00:00.123456Z

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("timestamp_trunc");
    for timezone in ["UTC", "America/Los_Angeles"] {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.into())),
            true,
        )]));
        for format in [
            "YEAR",
            "QUARTER",
            "MONTH",
            "WEEK",
            "DAY",
            "HOUR",
            "MINUTE",
            "SECOND",
            "MILLISECOND",
            "MICROSECOND",
        ] {
            let expr = TimestampTruncExpr::new(
                Arc::new(Column::new("a", 0)),
                lit(format),
                timezone.to_string(),
                true,
            );
            for rows in ROW_COUNTS {
                for (null_ratio, tag) in NULL_RATIOS.into_iter().chain([(0.875, "dense")]) {
                    // Keep every row in the modern range, including the largest batch. Vary
                    // sub-day values so fine-unit truncation does actual work as well.
                    let ts = timestamp_micros_array(rows, null_ratio, Some(timezone), |i| {
                        BASE_MICROS
                            + (i % 366) as i64 * MICROS_PER_DAY
                            + (i as i64 * 1_234_567).rem_euclid(MICROS_PER_DAY)
                    });
                    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![ts]).unwrap();
                    group.bench_with_input(
                        BenchmarkId::from_parameter(format!("{timezone}/{format}/{rows}/{tag}")),
                        &batch,
                        |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
                    );
                }
            }
        }
    }
    group.finish();

    // Many repeated keys should cost only as much as truncating the distinct values.
    // Keep NULL keys and unused/used overflowing entries in the same matched benchmark.
    let mut group = c.benchmark_group("timestamp_trunc_dictionary");
    for rows in [8_192, 65_536] {
        for (format, cardinality) in [
            ("MICROSECOND", 32),
            ("SECOND", 32),
            ("YEAR", 32),
            ("YEAR", rows),
        ] {
            for (null_ratio, tag) in NULL_RATIOS.into_iter().chain([(0.875, "dense")]) {
                let values = TimestampMicrosecondArray::from(
                    (0..cardinality)
                        .map(|i| BASE_MICROS + i as i64 * 1_234_567)
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC");
                let keys = Int32Array::from_iter(
                    (0..rows)
                        .map(|i| (!is_null(i, null_ratio)).then_some((i % cardinality) as i32)),
                );
                let input = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "a",
                    input.data_type().clone(),
                    true,
                )]));
                let batch = RecordBatch::try_new(schema, vec![Arc::new(input)]).unwrap();
                let expr = TimestampTruncExpr::new(
                    Arc::new(Column::new("a", 0)),
                    lit(format),
                    "UTC".into(),
                    true,
                );
                let cardinality_tag = if cardinality == rows { "high" } else { "low" };
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!("{format}/{rows}/{cardinality_tag}/{tag}")),
                    &batch,
                    |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
                );
            }
        }
        for (tag, referenced) in [("unused_overflow", false), ("used_overflow", true)] {
            let values =
                TimestampMicrosecondArray::from(vec![BASE_MICROS, i64::MIN]).with_timezone("UTC");
            let keys = Int32Array::from_iter((0..rows).map(|i| {
                if referenced && i == rows - 1 {
                    1
                } else {
                    0
                }
            }));
            let input = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new(
                "a",
                input.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(input)]).unwrap();
            let expr = TimestampTruncExpr::new(
                Arc::new(Column::new("a", 0)),
                lit("YEAR"),
                "UTC".into(),
                true,
            );
            assert_eq!(expr.evaluate(&batch).is_err(), referenced);
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("YEAR/{rows}/{tag}")),
                &batch,
                |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)))),
            );
        }
    }
    group.finish();

    // Formats from a column: one format for every row, and every unit mixed across the rows.
    // Daytime instants (16:00 to 22:00 UTC) avoid DST transitions, so older kernels can be
    // measured on the same input.
    let mut group = c.benchmark_group("timestamp_trunc_format_column");
    let rows = 8_192;
    let units = [
        "YEAR",
        "QUARTER",
        "MONTH",
        "WEEK",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MILLISECOND",
        "MICROSECOND",
    ];
    for timezone in ["UTC", "America/Los_Angeles"] {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "a",
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.into())),
                true,
            ),
            Field::new("fmt", DataType::Utf8, true),
        ]));
        let timestamps = TimestampMicrosecondArray::from_iter_values((0..rows).map(|i| {
            BASE_MICROS
                + (i % 366) as i64 * MICROS_PER_DAY
                + 57_600_000_000
                + (i as i64 * 1_234_567) % 21_600_000_000
        }))
        .with_timezone(timezone);
        for (tag, unit) in [("YEAR", Some(0)), ("HOUR", Some(5)), ("mixed", None)] {
            let formats = StringArray::from_iter_values(
                (0..rows).map(|i| units[unit.unwrap_or(i % units.len())]),
            );
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(timestamps.clone()), Arc::new(formats)],
            )
            .unwrap();
            let expr = TimestampTruncExpr::new(
                Arc::new(Column::new("a", 0)),
                Arc::new(Column::new("fmt", 1)),
                timezone.to_string(),
                true,
            );
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{timezone}/{tag}/{rows}")),
                &batch,
                |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
            );
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
