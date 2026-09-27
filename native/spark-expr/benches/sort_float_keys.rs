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

//! Compare the scalar sort keys before/after #5469 in the same optimized binary.
//!
//! This runs DataFusion's full SortExec over an in-memory, single-partition input;
//! it does not time Spark planning, JNI, input generation, I/O, or a distributed
//! range shuffle. Both variants retain the original output values. The mixed
//! inputs intentionally have different ordering semantics before normalization.
//!
//! Run with `cargo bench -p datafusion-comet-spark-expr --bench sort_float_keys`.
//! Set COMET_SORT_BENCH_REVERSE_VARIANTS=1 for a second pass in the opposite order.

use std::cmp::Ordering;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, Int64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use datafusion::common::Result;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion_comet_spark_expr::NormalizeNaNAndZero;
use futures::TryStreamExt;
use tokio::runtime::Builder;

const BATCH_ROWS: usize = 8192;
const BATCH_COUNT: usize = 32;
const ROWS: usize = BATCH_ROWS * BATCH_COUNT;
const MEMORY_LIMIT: usize = 128 * 1024 * 1024;

fn input_batches(data_type: &DataType, mixed: bool) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", data_type.clone(), mixed),
        Field::new("row_id", DataType::Int64, false),
    ]));
    let mut state = 0x1234_5678_9abc_def0_u64;
    (0..BATCH_COUNT)
        .map(|batch| {
            let start = batch * BATCH_ROWS;
            let values: Vec<Option<f64>> = (start..start + BATCH_ROWS)
                .map(|row| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let finite = (state % 1_000_000) as f64 / 16.0 - 31_250.0;
                    match (mixed, row % 100) {
                        (true, 0..=19) => None,
                        (true, 20..=24) => Some(f64::from_bits(0xfff8_0000_0000_0000 | row as u64)),
                        (true, 25..=29) => Some(f64::from_bits(0x7ff8_0000_0000_0000 | row as u64)),
                        (true, 30..=34) => Some(-0.0),
                        (true, 35..=39) => Some(0.0),
                        _ => Some(finite),
                    }
                })
                .collect();
            let keys: ArrayRef = match data_type {
                DataType::Float32 => Arc::new(Float32Array::from(
                    values
                        .into_iter()
                        .enumerate()
                        .map(|(row, value)| {
                            value.map(|v| {
                                if v.is_nan() {
                                    let sign = if v.is_sign_negative() { 0x8000_0000 } else { 0 };
                                    f32::from_bits(sign | 0x7fc0_0000 | (start + row) as u32)
                                } else {
                                    v as f32
                                }
                            })
                        })
                        .collect::<Vec<_>>(),
                )),
                DataType::Float64 => Arc::new(Float64Array::from(values)),
                _ => unreachable!(),
            };
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    keys,
                    Arc::new(Int64Array::from_iter_values(
                        (start..start + BATCH_ROWS).map(|row| row as i64),
                    )),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn sort_plan(batches: &[RecordBatch], normalized: bool) -> Arc<SortExec> {
    let input =
        MemorySourceConfig::try_new_exec(&[batches.to_vec()], batches[0].schema(), None).unwrap();
    let mut key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    if normalized {
        key = Arc::new(NormalizeNaNAndZero::new(
            batches[0].schema().field(0).data_type().clone(),
            key,
        ));
    }
    Arc::new(SortExec::new(
        [PhysicalSortExpr {
            expr: key,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }]
        .into(),
        input,
    ))
}

fn task_context(pool: Arc<dyn MemoryPool>) -> Arc<TaskContext> {
    let mut config = SessionConfig::new()
        .with_batch_size(BATCH_ROWS)
        .with_target_partitions(1);
    config.options_mut().execution.sort_in_place_threshold_bytes = 1024 * 1024;
    config.options_mut().execution.sort_spill_reservation_bytes = 10 * 1024 * 1024;
    Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(pool)
                    .build_arc()
                    .unwrap(),
            ),
    )
}

/// Only used by the untimed validation run. Pool reservations do not include
/// every Arrow allocation, including temporary normalized comparison keys.
#[derive(Debug)]
struct PeakPool {
    inner: GreedyMemoryPool,
    peak: AtomicUsize,
}

impl PeakPool {
    fn record_peak(&self) {
        self.peak
            .fetch_max(self.inner.reserved(), AtomicOrdering::Relaxed);
    }
}

impl std::fmt::Display for PeakPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sort benchmark peak memory pool")
    }
}

impl MemoryPool for PeakPool {
    fn name(&self) -> &str {
        "sort_benchmark_peak"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.record_peak();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record_peak();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

fn raw_value(array: &ArrayRef, row: usize) -> (Option<f64>, u64) {
    if array.is_null(row) {
        return (None, 0);
    }
    match array.data_type() {
        DataType::Float32 => {
            let value = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row);
            (Some(value as f64), u64::from(value.to_bits()))
        }
        DataType::Float64 => {
            let value = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            (Some(value), value.to_bits())
        }
        _ => unreachable!(),
    }
}

fn compare(left: Option<f64>, right: Option<f64>, normalized: bool) -> Ordering {
    let canonical = |v: f64| {
        if normalized && v.is_nan() {
            f64::NAN
        } else if normalized && v == 0.0 {
            0.0
        } else {
            v
        }
    };
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(a), Some(b)) => canonical(a).total_cmp(&canonical(b)),
    }
}

async fn validate(batches: &[RecordBatch], normalized: bool) -> usize {
    let pool = Arc::new(PeakPool {
        inner: GreedyMemoryPool::new(MEMORY_LIMIT),
        peak: AtomicUsize::new(0),
    });
    let sort = sort_plan(batches, normalized);
    let output = collect(
        Arc::clone(&sort) as Arc<dyn ExecutionPlan>,
        task_context(Arc::clone(&pool) as Arc<dyn MemoryPool>),
    )
    .await
    .unwrap();
    let mut seen = vec![false; ROWS];
    let mut previous = None;
    for batch in output {
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let id = ids.value(row) as usize;
            assert!(!seen[id]);
            seen[id] = true;
            let (value, bits) = raw_value(batch.column(0), row);
            let (original, original_bits) =
                raw_value(batches[id / BATCH_ROWS].column(0), id % BATCH_ROWS);
            assert_eq!(value.is_none(), original.is_none());
            assert_eq!(
                bits, original_bits,
                "sort changed the returned floating bits"
            );
            if let Some(previous) = previous {
                assert_ne!(compare(previous, value, normalized), Ordering::Greater);
            }
            previous = Some(value);
        }
    }
    assert!(seen.into_iter().all(|value| value));
    assert_eq!(sort.metrics().unwrap().spill_count().unwrap_or(0), 0);
    assert_eq!(pool.reserved(), 0);
    pool.peak.load(AtomicOrdering::Relaxed)
}

fn benchmark(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let context = task_context(Arc::new(GreedyMemoryPool::new(MEMORY_LIMIT)));
    let mut variants = [("bare", false), ("normalized", true)];
    if std::env::var_os("COMET_SORT_BENCH_REVERSE_VARIANTS").is_some() {
        variants.reverse();
    }
    let mut group = c.benchmark_group("sort_float_keys");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(ROWS as u64));
    for (name, data_type) in [
        ("float32", DataType::Float32),
        ("float64", DataType::Float64),
    ] {
        for (shape, mixed) in [("finite", false), ("mixed", true)] {
            let batches = input_batches(&data_type, mixed);
            for (variant, normalized) in variants {
                let peak = runtime.block_on(validate(&batches, normalized));
                eprintln!(
                    "{name}/{shape}/{variant}: rows={ROWS}, pool_peak_reserved_bytes={peak}, spills=0"
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("{name}_{shape}"), variant),
                    &normalized,
                    |b, &normalized| {
                        b.to_async(&runtime).iter_batched(
                            || sort_plan(&batches, normalized),
                            |sort| {
                                let context = Arc::clone(&context);
                                async move {
                                    let mut stream = sort.execute(0, context).unwrap();
                                    let mut rows = 0;
                                    while let Some(batch) = stream.try_next().await.unwrap() {
                                        rows += black_box(batch).num_rows();
                                    }
                                    assert_eq!(rows, ROWS);
                                    black_box(rows)
                                }
                            },
                            BatchSize::LargeInput,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
