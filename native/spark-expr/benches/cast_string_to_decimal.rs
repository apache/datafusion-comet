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
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

/// A batch of decimal strings covering the shapes Spark sees in practice: plain
/// integers, fixed-point values, negatives, and scientific notation.
fn create_decimal_string_batch(size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let mut rng = StdRng::seed_from_u64(42);
    let mut b = StringBuilder::new();
    for i in 0..size {
        if i % 10 == 0 {
            b.append_null();
        } else {
            match i % 5 {
                0 => b.append_value(format!(
                    "{}.{}",
                    rng.random_range(0..1_000_000u32),
                    rng.random_range(0..100_000u32)
                )),
                1 => b.append_value(format!(
                    "{}.{}E{}",
                    rng.random_range(0..10u32),
                    rng.random_range(0..100u32),
                    rng.random_range(0..10u32)
                )),
                2 => b.append_value(format!(
                    "-{}.{}",
                    rng.random_range(0..1_000_000u32),
                    rng.random_range(0..100_000u32)
                )),
                3 => b.append_value(format!("{}", rng.random_range(-1_000_000..1_000_000i32))),
                _ => b.append_value(format!("0.{:05}", rng.random_range(0..100_000u32))),
            }
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(b.finish())]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let batch = create_decimal_string_batch(8192);
    let expr = Arc::new(Column::new("a", 0));

    for (mode, mode_name) in [
        (EvalMode::Legacy, "legacy"),
        (EvalMode::Ansi, "ansi"),
        (EvalMode::Try, "try"),
    ] {
        let mut group = c.benchmark_group(format!("cast_string_to_decimal/{mode_name}"));
        for (data_type, name) in [
            (DataType::Decimal128(38, 10), "decimal_38_10"),
            (DataType::Decimal128(18, 2), "decimal_18_2"),
        ] {
            let cast = Cast::new(
                expr.clone(),
                data_type,
                SparkCastOptions::new(mode, "", false),
                None,
                None,
            );
            group.bench_function(name, |b| {
                b.iter(|| black_box(cast.evaluate(&batch).unwrap()));
            });
        }
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
