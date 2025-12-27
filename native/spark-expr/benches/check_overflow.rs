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

use arrow::array::builder::Decimal128Builder;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::CheckOverflow;
use std::sync::Arc;

fn create_decimal_batch(size: usize, precision: u8, scale: i8, with_nulls: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Decimal128(precision, scale),
        true,
    )]));
    let mut builder = Decimal128Builder::with_capacity(size);

    for i in 0..size {
        if with_nulls && i % 10 == 0 {
            builder.append_null();
        } else {
            // Values that fit within precision 10 (max ~9999999999)
            builder.append_value((i as i128) * 12345);
        }
    }

    let array = builder
        .finish()
        .with_precision_and_scale(precision, scale)
        .unwrap();
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

fn create_batch_with_overflow(
    size: usize,
    input_precision: u8,
    target_precision: u8,
    scale: i8,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Decimal128(input_precision, scale),
        true,
    )]));
    let mut builder = Decimal128Builder::with_capacity(size);

    // Create values where ~10% will overflow the target precision
    let max_for_target = 10i128.pow(target_precision as u32) - 1;
    for i in 0..size {
        if i % 10 == 0 {
            // This value will overflow target precision
            builder.append_value(max_for_target + (i as i128) + 1);
        } else {
            // This value is within target precision
            builder.append_value((i as i128) % max_for_target);
        }
    }

    let array = builder
        .finish()
        .with_precision_and_scale(input_precision, scale)
        .unwrap();
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let sizes = [1000, 10000];

    let mut group = c.benchmark_group("check_overflow");

    for size in sizes {
        // Benchmark: No overflow possible (precision already fits)
        // This tests the fast path where input precision <= target precision
        let batch_no_overflow = create_decimal_batch(size, 10, 2, false);

        // Create CheckOverflow that goes from precision 10 to 18 (no overflow possible)
        let check_overflow_no_validation = Arc::new(CheckOverflow::new(
            Arc::new(datafusion::physical_expr::expressions::Column::new("a", 0)),
            DataType::Decimal128(18, 2), // larger precision = no overflow possible
            false,
        ));

        group.bench_with_input(
            BenchmarkId::new("no_overflow_possible", size),
            &batch_no_overflow,
            |b, batch| {
                b.iter(|| check_overflow_no_validation.evaluate(batch).unwrap());
            },
        );

        // Benchmark: Validation needed, but no overflows occur
        let batch_valid = create_decimal_batch(size, 18, 2, true);
        let check_overflow_valid = Arc::new(CheckOverflow::new(
            Arc::new(datafusion::physical_expr::expressions::Column::new("a", 0)),
            DataType::Decimal128(10, 2), // smaller precision, need to validate
            false,
        ));

        group.bench_with_input(
            BenchmarkId::new("validation_no_overflow", size),
            &batch_valid,
            |b, batch| {
                b.iter(|| check_overflow_valid.evaluate(batch).unwrap());
            },
        );

        // Benchmark: With ~10% overflows (requires null insertion)
        let batch_with_overflow = create_batch_with_overflow(size, 18, 8, 2);
        let check_overflow_with_nulls = Arc::new(CheckOverflow::new(
            Arc::new(datafusion::physical_expr::expressions::Column::new("a", 0)),
            DataType::Decimal128(8, 2),
            false,
        ));

        group.bench_with_input(
            BenchmarkId::new("with_overflow_to_null", size),
            &batch_with_overflow,
            |b, batch| {
                b.iter(|| check_overflow_with_nulls.evaluate(batch).unwrap());
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
    targets = criterion_benchmark
}
criterion_main!(benches);
