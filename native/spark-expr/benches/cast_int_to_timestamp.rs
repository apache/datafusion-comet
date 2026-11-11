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

//! i64 to timestamp. 8192 rows. The narrower integer widths use the same
//! `unary` kernel. Shapes: no nulls, sparse nulls (1 in 10), dense nulls
//! (7 in 8), and an all-null batch.

use arrow::array::{Int32Array, Int64Array, RecordBatch};
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
    /// One null in every ten rows.
    Sparse,
    /// Seven nulls in every eight rows.
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

fn timestamp_cast() -> Cast {
    Cast::new(
        Arc::new(Column::new("a", 0)),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        SparkCastOptions::new(EvalMode::Legacy, "UTC"),
        None,
        None,
    )
}

fn batch_i32() -> RecordBatch {
    let values: Int32Array = (0..BATCH_SIZE)
        .map(|i| Some(1_600_000_000 + i as i32))
        .collect();
    record_batch(DataType::Int32, Arc::new(values))
}

fn batch_i64(nulls: Nulls) -> RecordBatch {
    let values: Int64Array = (0..BATCH_SIZE)
        .map(|i| {
            if is_null(i, nulls) {
                None
            } else {
                Some(1_600_000_000 + i as i64)
            }
        })
        .collect();
    record_batch(DataType::Int64, Arc::new(values))
}

fn record_batch(dtype: DataType, column: arrow::array::ArrayRef) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", dtype, true)]));
    RecordBatch::try_new(schema, vec![column]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let cast = timestamp_cast();
    let mut bench = |name: &str, batch: RecordBatch| {
        c.bench_function(name, |b| {
            b.iter(|| black_box(cast.evaluate(black_box(&batch)).unwrap()))
        });
    };

    bench("cast_i32_to_timestamp: no nulls", batch_i32());
    bench("cast_i64_to_timestamp: no nulls", batch_i64(Nulls::None));
    bench(
        "cast_i64_to_timestamp: sparse nulls",
        batch_i64(Nulls::Sparse),
    );
    bench(
        "cast_i64_to_timestamp: dense nulls",
        batch_i64(Nulls::Dense),
    );
    bench("cast_i64_to_timestamp: all nulls", batch_i64(Nulls::All));
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
