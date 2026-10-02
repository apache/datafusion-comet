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
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_expr::expressions::{lit, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::TimestampTruncExpr;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{timestamp_micros_array, NULL_RATIOS, ROW_COUNTS};

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
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
