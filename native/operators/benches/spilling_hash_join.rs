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

//! Micro-benchmarks for `SpillingHashJoinExec`, the operator behind a native shuffled hash join
//! whose build side may not fit in memory.
//!
//! `CometHashJoinMemoryBenchmark` on the JVM side measures the same join end to end, where the
//! scans and the shuffles are most of the time. This one runs one task's join over in-memory
//! batches, so the cost of spilling shows up undiluted. The data mirrors one task of that
//! benchmark: a build side of unique keys with a string payload, and a probe side five times as
//! long, half of whose rows find a match. The keys are a quarter of a range, as a task's share of
//! a hash-partitioned table is, so `HashJoinExec` looks the in-memory build side up with its
//! perfect hash table, as it does in the JVM benchmark.
//!
//! Each case runs under a memory pool of a fixed size. The unbounded cases are the cost of the
//! join when nothing spills, plain and wrapped. The bounded cases are roughly what one join gets
//! of a task's pool in the JVM benchmark, where three memory consumers share it: 64 MiB there is
//! about 20 MiB here.
//!
//! To profile one case: `cargo bench --bench spilling_hash_join -- --profile-time 20 20MiB_lz4_frame`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::SpillCompression;
use datafusion::common::{JoinType, NullEquality};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_comet_operators::{SpillingHashJoinConfig, SpillingHashJoinExec};
use futures::StreamExt;
use tokio::runtime::Runtime;

const BATCH_SIZE: usize = 8192;
const BUILD_ROWS: usize = 1_000_000;
const PROBE_ROWS: usize = 5 * BUILD_ROWS;
/// Keys are drawn from a range of this many values per build row. A quarter of them belong to
/// the task, and half of the task's are build keys.
const KEY_SPREAD: u64 = 8;
const MIB: usize = 1024 * 1024;

/// A deterministic stream of pseudo-random numbers.
struct SplitMix64(u64);

impl SplitMix64 {
    fn mix(mut z: u64) -> u64 {
        z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        Self::mix(self.0)
    }
}

/// Whether `key` belongs to the task, as a hash partitioning into four would decide.
fn in_task(key: u64) -> bool {
    SplitMix64::mix(key ^ 0x5bd1_e995).is_multiple_of(4)
}

fn build_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("w", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]))
}

fn probe_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]))
}

/// The build side: the task's keys of the lower half of the range, once each, in order.
fn build_batches() -> Vec<RecordBatch> {
    let range = BUILD_ROWS as u64 * KEY_SPREAD / 2;
    let keys: Vec<i64> = (0..range)
        .filter(|key| in_task(*key))
        .map(|key| key as i64)
        .collect();
    keys.chunks(BATCH_SIZE)
        .map(|keys| {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from_iter_values(
                    keys.iter().map(|key| SplitMix64::mix(*key as u64) as i64),
                )),
                Arc::new(StringArray::from_iter_values(
                    keys.iter()
                        .map(|key| format!("payload-{key}-xxxxxxxxxxxxxxxx")),
                )),
            ];
            RecordBatch::try_new(build_schema(), columns).unwrap()
        })
        .collect()
}

/// The probe side: the task's keys of the whole range, drawn at random, so that half match.
fn probe_batches() -> Vec<RecordBatch> {
    let range = BUILD_ROWS as u64 * KEY_SPREAD;
    let mut random = SplitMix64(42);
    let mut keys = Vec::with_capacity(PROBE_ROWS);
    while keys.len() < PROBE_ROWS {
        let key = random.next() % range;
        if in_task(key) {
            keys.push(key as i64);
        }
    }
    let mut row = 0;
    keys.chunks(BATCH_SIZE)
        .map(|keys| {
            let values = Int64Array::from_iter_values(row..row + keys.len() as i64);
            row += keys.len() as i64;
            let columns: Vec<ArrayRef> =
                vec![Arc::new(Int64Array::from(keys.to_vec())), Arc::new(values)];
            RecordBatch::try_new(probe_schema(), columns).unwrap()
        })
        .collect()
}

fn hash_join(build: &[RecordBatch], probe: &[RecordBatch]) -> HashJoinExec {
    let build_input =
        MemorySourceConfig::try_new_exec(&[build.to_vec()], build_schema(), None).unwrap();
    let probe_input =
        MemorySourceConfig::try_new_exec(&[probe.to_vec()], probe_schema(), None).unwrap();
    let build_key: PhysicalExprRef = Arc::new(Column::new("k", 0));
    let probe_key: PhysicalExprRef = Arc::new(Column::new("k", 0));
    HashJoinExec::try_new(
        build_input,
        probe_input,
        vec![(build_key, probe_key)],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap()
}

fn context(pool_bytes: Option<usize>, compression: SpillCompression) -> Arc<TaskContext> {
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(bytes) = pool_bytes {
        runtime = runtime.with_memory_pool(Arc::new(GreedyMemoryPool::new(bytes)));
    }
    Arc::new(
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(BATCH_SIZE)
                    .with_spill_compression(compression),
            )
            .with_runtime(runtime.build_arc().unwrap()),
    )
}

/// Prints what a spilling join did, once per case, so that a change in its timing can be told
/// apart from a change in what it spilled.
fn print_spill_metrics(name: &str, plan: &dyn ExecutionPlan) {
    let metrics = plan.metrics().unwrap().aggregate_by_name();
    let value = |name: &str| {
        metrics
            .sum_by_name(name)
            .map_or(0, |value| value.as_usize())
    };
    eprintln!(
        "{name}: spilled_buckets={} split_buckets={} chunked_buckets={} spill_files={} \
         spilled_rows={} spilled_bytes={} build_mem_used={} budget_overflow_bytes={}",
        value("spilled_buckets"),
        value("split_buckets"),
        value("chunked_buckets"),
        metrics.spill_count().unwrap_or(0),
        metrics.spilled_rows().unwrap_or(0),
        metrics.spilled_bytes().unwrap_or(0),
        value("build_mem_used"),
        value("budget_overflow_bytes"),
    );
}

/// Runs the join and returns its output row count.
fn run(
    runtime: &Runtime,
    plan: Arc<dyn ExecutionPlan>,
    pool_bytes: Option<usize>,
    compression: SpillCompression,
) -> usize {
    runtime.block_on(async {
        let mut stream = plan.execute(0, context(pool_bytes, compression)).unwrap();
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows();
        }
        rows
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let build = build_batches();
    let probe = probe_batches();
    let plain = hash_join(&build, &probe);
    // A `HashJoinExec` keeps its build side once built, so every run takes a fresh copy.
    let fresh = || Arc::new(plain.builder().reset_state().build().unwrap());
    let lz4 = SpillCompression::Lz4Frame;
    let expected = run(&runtime, fresh(), None, lz4);

    let mut group = c.benchmark_group("spilling_hash_join");
    group.sample_size(10);
    group.bench_function("hash_join_unbounded", |b| {
        b.iter(|| run(&runtime, fresh(), None, lz4))
    });
    // Comet compresses spill files with lz4 by default. The other codecs run at one budget.
    let mut cases = vec![(None, lz4)];
    cases.extend([64, 40, 20, 10].map(|mib| (Some(mib), lz4)));
    cases.extend(
        [SpillCompression::Zstd, SpillCompression::Uncompressed].map(|codec| (Some(20), codec)),
    );
    for (budget, compression) in cases {
        let name = match budget {
            None => "unbounded".to_string(),
            Some(mib) => format!("{mib}MiB_{compression}"),
        };
        let pool_bytes = budget.map(|mib| mib * MIB);
        let spilling = Arc::new(
            SpillingHashJoinExec::try_new(&plain, SpillingHashJoinConfig::default())
                .unwrap()
                .unwrap(),
        );
        assert_eq!(
            run(
                &runtime,
                Arc::clone(&spilling) as _,
                pool_bytes,
                compression
            ),
            expected
        );
        print_spill_metrics(&name, spilling.as_ref());
        group.bench_function(BenchmarkId::new("spilling", name), |b| {
            b.iter(|| {
                let spilling =
                    SpillingHashJoinExec::try_new(&plain, SpillingHashJoinConfig::default())
                        .unwrap()
                        .unwrap();
                let rows = run(&runtime, Arc::new(spilling), pool_bytes, compression);
                assert_eq!(rows, expected);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
