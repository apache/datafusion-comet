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

use arrow::array::{Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::TimestampTruncExpr;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 10000;

fn create_timestamp_batch(timezone: &str) -> RecordBatch {
    // Generate timestamps spread across time
    // Each row is 1 billion microseconds (~16.7 minutes) apart
    // This gives us about 115 days of spread across 10000 rows
    let mut vec: Vec<Option<i64>> = Vec::with_capacity(NUM_ROWS);
    for i in 0..NUM_ROWS {
        if i % 100 == 0 {
            // Add some nulls (1% of data)
            vec.push(None);
        } else {
            vec.push(Some(i as i64 * 1_000_000_001));
        }
    }

    let array = TimestampMicrosecondArray::from(vec).with_timezone(timezone);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        array.data_type().clone(),
        true,
    )]));

    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

fn make_col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, index))
}

fn make_format_literal(format: &str) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Utf8(Some(format.to_string()))))
}

fn benchmark_utc(c: &mut Criterion) {
    let timezone = "UTC";
    let batch = create_timestamp_batch(timezone);
    let ts_col = make_col("ts", 0);

    let formats = vec![
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

    let mut group = c.benchmark_group("timestamp_trunc_utc");

    for format in formats {
        let expr = TimestampTruncExpr::new(
            Arc::clone(&ts_col),
            make_format_literal(format),
            timezone.to_string(),
        );

        group.bench_function(format, |b| {
            b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
        });
    }

    group.finish();
}

fn benchmark_new_york(c: &mut Criterion) {
    let timezone = "America/New_York";
    let batch = create_timestamp_batch(timezone);
    let ts_col = make_col("ts", 0);

    // Test key formats that showed regression in Spark benchmarks
    let formats = vec!["YEAR", "MONTH", "DAY", "SECOND"];

    let mut group = c.benchmark_group("timestamp_trunc_new_york");

    for format in formats {
        let expr = TimestampTruncExpr::new(
            Arc::clone(&ts_col),
            make_format_literal(format),
            timezone.to_string(),
        );

        group.bench_function(format, |b| {
            b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
        });
    }

    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = benchmark_utc, benchmark_new_york
}
criterion_main!(benches);
