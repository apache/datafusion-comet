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

//! f64, decimal, and boolean casts to timestamp. 8192 rows. f32 uses the same
//! kernel as f64. Shapes: no nulls, sparse nulls (1 in 10), dense nulls (7 in 8).
//! Float covers legacy and ANSI, plus a legacy batch of NaN and infinities.

use arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, RecordBatch,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const BATCH_SIZE: usize = 8192;

#[derive(Clone, Copy)]
enum Nulls {
    None,
    Sparse,
    Dense,
    All,
}

fn is_null(i: usize, nulls: Nulls) -> bool {
    match nulls {
        Nulls::None => false,
        Nulls::Sparse => i.is_multiple_of(10),
        Nulls::Dense => !i.is_multiple_of(8),
        Nulls::All => true,
    }
}

fn timestamp_cast(mode: EvalMode) -> Cast {
    Cast::new(
        Arc::new(Column::new("a", 0)),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        SparkCastOptions::new(mode, "UTC"),
        None,
        None,
    )
}

fn record_batch(dtype: DataType, column: ArrayRef) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", dtype, true)]));
    RecordBatch::try_new(schema, vec![column]).unwrap()
}

fn batch_f64(nulls: Nulls, non_finite: bool) -> RecordBatch {
    let values: Float64Array = (0..BATCH_SIZE)
        .map(|i| {
            if is_null(i, nulls) {
                None
            } else if non_finite && i.is_multiple_of(16) {
                Some(f64::NAN)
            } else if non_finite && i % 16 == 1 {
                Some(f64::INFINITY)
            } else if non_finite && i % 16 == 2 {
                Some(f64::NEG_INFINITY)
            } else {
                // Fractional seconds, all inside the i64 microsecond range.
                Some((i as f64) * 0.5 - 100.0)
            }
        })
        .collect();
    record_batch(DataType::Float64, Arc::new(values))
}

fn batch_f32() -> RecordBatch {
    let values: Float32Array = (0..BATCH_SIZE)
        .map(|i| Some((i as f32) * 0.5 - 100.0))
        .collect();
    record_batch(DataType::Float32, Arc::new(values))
}

fn batch_bool(nulls: Nulls) -> RecordBatch {
    let values: BooleanArray = (0..BATCH_SIZE)
        .map(|i| {
            if is_null(i, nulls) {
                None
            } else {
                Some(i.is_multiple_of(2))
            }
        })
        .collect();
    record_batch(DataType::Boolean, Arc::new(values))
}

fn batch_decimal(scale: i8, nulls: Nulls) -> RecordBatch {
    let factor = 10_i128.pow(scale as u32);
    // Unscaled values of a few thousand at this scale: small enough that
    // value * 1_000_000 fits in i128 for every scale we bench.
    let values: Decimal128Array = (0..BATCH_SIZE)
        .map(|i| {
            if is_null(i, nulls) {
                None
            } else {
                Some((i as i128 - 100) * factor / 2)
            }
        })
        .collect();
    let array = values.with_precision_and_scale(38, scale).unwrap();
    record_batch(DataType::Decimal128(38, scale), Arc::new(array))
}

fn criterion_benchmark(c: &mut Criterion) {
    let legacy = timestamp_cast(EvalMode::Legacy);
    let ansi = timestamp_cast(EvalMode::Ansi);

    let mut bench = |name: &str, cast: &Cast, batch: RecordBatch| {
        c.bench_function(name, |b| {
            b.iter(|| black_box(cast.evaluate(black_box(&batch)).unwrap()))
        });
    };

    bench(
        "cast_f32_to_timestamp: legacy, no nulls",
        &legacy,
        batch_f32(),
    );
    bench(
        "cast_f64_to_timestamp: legacy, no nulls",
        &legacy,
        batch_f64(Nulls::None, false),
    );
    bench(
        "cast_f64_to_timestamp: legacy, sparse nulls",
        &legacy,
        batch_f64(Nulls::Sparse, false),
    );
    bench(
        "cast_f64_to_timestamp: legacy, dense nulls",
        &legacy,
        batch_f64(Nulls::Dense, false),
    );
    bench(
        "cast_f64_to_timestamp: legacy, all nulls",
        &legacy,
        batch_f64(Nulls::All, false),
    );
    bench(
        "cast_f64_to_timestamp: legacy, nan and inf",
        &legacy,
        batch_f64(Nulls::None, true),
    );

    bench(
        "cast_f64_to_timestamp: ansi, no nulls",
        &ansi,
        batch_f64(Nulls::None, false),
    );
    bench(
        "cast_f64_to_timestamp: ansi, sparse nulls",
        &ansi,
        batch_f64(Nulls::Sparse, false),
    );
    bench(
        "cast_f64_to_timestamp: ansi, dense nulls",
        &ansi,
        batch_f64(Nulls::Dense, false),
    );
    bench(
        "cast_f64_to_timestamp: ansi, all nulls",
        &ansi,
        batch_f64(Nulls::All, false),
    );

    bench(
        "cast_decimal_to_timestamp: scale 6, no nulls",
        &legacy,
        batch_decimal(6, Nulls::None),
    );
    bench(
        "cast_decimal_to_timestamp: scale 6, sparse nulls",
        &legacy,
        batch_decimal(6, Nulls::Sparse),
    );
    bench(
        "cast_decimal_to_timestamp: scale 6, dense nulls",
        &legacy,
        batch_decimal(6, Nulls::Dense),
    );
    bench(
        "cast_decimal_to_timestamp: scale 6, all nulls",
        &legacy,
        batch_decimal(6, Nulls::All),
    );
    bench(
        "cast_bool_to_timestamp: no nulls",
        &legacy,
        batch_bool(Nulls::None),
    );
    bench(
        "cast_bool_to_timestamp: sparse nulls",
        &legacy,
        batch_bool(Nulls::Sparse),
    );
    bench(
        "cast_bool_to_timestamp: dense nulls",
        &legacy,
        batch_bool(Nulls::Dense),
    );
    bench(
        "cast_bool_to_timestamp: all nulls",
        &legacy,
        batch_bool(Nulls::All),
    );
}

fn config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2))
        .sample_size(50)
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
