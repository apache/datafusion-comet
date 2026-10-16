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

//! Compares [`SpillingHashJoinExec`] with the `HashJoinExec` it wraps, row for row, for every join
//! type, with each of its paths forced.

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::config::SpillCompression;
use datafusion::common::{DataFusionError, JoinSide, JoinType, NullEquality, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionConfig;
use futures::StreamExt;

use super::{SpillingHashJoinConfig, SpillingHashJoinExec};

const JOIN_TYPES: [JoinType; 10] = [
    JoinType::Inner,
    JoinType::Left,
    JoinType::Right,
    JoinType::Full,
    JoinType::LeftSemi,
    JoinType::LeftAnti,
    JoinType::RightSemi,
    JoinType::RightAnti,
    JoinType::LeftMark,
    JoinType::RightMark,
];

/// A deterministic stream of pseudo-random numbers.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// How the keys of a test table are drawn.
#[derive(Clone, Copy)]
struct Keys {
    /// Keys are drawn from 0 to `range`.
    range: u64,
    /// Every `null_every`th key is null.
    null_every: usize,
    /// This fraction of the rows, in percent, have key 7.
    hot_percent: u64,
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]))
}

/// A table of a nullable key `k`, a value `v` and a string `s`, in batches of 1000 rows.
fn table(rows: usize, keys: Keys, seed: u64) -> Vec<RecordBatch> {
    let mut random = SplitMix64(seed);
    let mut batches = Vec::new();
    let mut row = 0;
    while row < rows {
        let len = 1000.min(rows - row);
        let mut k = Vec::with_capacity(len);
        let mut v = Vec::with_capacity(len);
        let mut s = Vec::with_capacity(len);
        for i in row..row + len {
            let draw = random.next();
            k.push(if keys.null_every > 0 && i % keys.null_every == 0 {
                None
            } else if draw % 100 < keys.hot_percent {
                Some(7)
            } else {
                Some(((draw >> 8) % keys.range) as i64)
            });
            v.push((random.next() % 1000) as i64);
            s.push(format!("row-{i}-{}", draw % 97));
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(StringArray::from(s)),
        ];
        batches.push(RecordBatch::try_new(schema(), columns).unwrap());
        row += len;
    }
    batches
}

/// `build.v < probe.v`.
fn filter() -> JoinFilter {
    let schema = Arc::new(Schema::new(vec![
        Field::new("bv", DataType::Int64, false),
        Field::new("pv", DataType::Int64, false),
    ]));
    let expression = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("bv", 0)),
        Operator::Lt,
        Arc::new(Column::new("pv", 1)),
    ));
    JoinFilter::new(
        expression,
        vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ],
        schema,
    )
}

fn hash_join(
    build: &[RecordBatch],
    probe: &[RecordBatch],
    join_type: JoinType,
    with_filter: bool,
    on: &[&str],
) -> Result<HashJoinExec> {
    let build_input = MemorySourceConfig::try_new_exec(&[build.to_vec()], schema(), None)?;
    let probe_input = MemorySourceConfig::try_new_exec(&[probe.to_vec()], schema(), None)?;
    let on = on
        .iter()
        .map(|name| {
            let index = schema().index_of(name).unwrap();
            let build: PhysicalExprRef = Arc::new(Column::new(name, index));
            let probe: PhysicalExprRef = Arc::new(Column::new(name, index));
            (build, probe)
        })
        .collect();
    HashJoinExec::try_new(
        build_input,
        probe_input,
        on,
        with_filter.then(filter),
        &join_type,
        None,
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
        false,
    )
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
                    .with_batch_size(1000)
                    .with_spill_compression(compression),
            )
            .with_runtime(runtime.build_arc().unwrap()),
    )
}

/// Every output row, formatted, in sorted order.
fn rows(batches: &[RecordBatch]) -> Vec<String> {
    let options = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            let values: Vec<String> = formatters
                .iter()
                .map(|formatter| formatter.value(row).to_string())
                .collect();
            rows.push(values.join("|"));
        }
    }
    rows.sort();
    rows
}

fn metric(plan: &dyn ExecutionPlan, name: &str) -> usize {
    let metrics = plan.metrics().map(|metrics| metrics.aggregate_by_name());
    match name {
        // `output_rows` has a variant of its own, which `sum_by_name` does not match.
        "output_rows" => metrics.and_then(|metrics| metrics.output_rows()),
        _ => metrics
            .and_then(|metrics| metrics.sum_by_name(name))
            .map(|value| value.as_usize()),
    }
    .unwrap_or(0)
}

/// What a spilling join's run did.
#[derive(Debug, Default)]
struct Spilled {
    spilled_buckets: usize,
    split_buckets: usize,
    chunked_buckets: usize,
}

/// Runs the join both ways and checks that they emit the same rows. The spilling join runs with
/// uncompressed and with compressed spill files, which hold different memory once read back.
async fn check(
    build: &[RecordBatch],
    probe: &[RecordBatch],
    join_type: JoinType,
    with_filter: bool,
    on: &[&str],
    config: SpillingHashJoinConfig,
    pool_bytes: Option<usize>,
) -> Result<Spilled> {
    let plain = Arc::new(hash_join(build, probe, join_type, with_filter, on)?);
    let unlimited = context(None, SpillCompression::Uncompressed);
    let expected = rows(&collect(Arc::clone(&plain) as _, unlimited).await?);
    let mut spilled = Spilled::default();
    for compression in [SpillCompression::Uncompressed, SpillCompression::Lz4Frame] {
        let spilling = Arc::new(
            SpillingHashJoinExec::try_new(&plain, config.clone())?
                .expect("a partitioned join is wrapped"),
        );
        assert_eq!(spilling.schema(), plain.schema());
        let context = context(pool_bytes, compression);
        let actual = rows(&collect(Arc::clone(&spilling) as _, context).await?);
        let case = format!("{join_type} with filter {with_filter}, {compression:?} spill files");
        assert_eq!(actual.len(), expected.len(), "row count for {case}");
        assert_eq!(actual, expected, "rows for {case}");
        assert_eq!(
            metric(spilling.as_ref(), "budget_overflow_bytes"),
            0,
            "an inner join of {case} reserved more than estimated"
        );
        // The operator reports its own input and output rather than what its inner joins add up
        // to. It reads every probe row, unless an empty build side lets it stop first.
        // `HashJoinExec` itself also counts its final pass over the build rows as probe input.
        let rows =
            |batches: &[RecordBatch]| -> usize { batches.iter().map(|b| b.num_rows()).sum() };
        let probe_rows = if rows(build) == 0 && !emits_probe_rows_without_build(join_type) {
            0
        } else {
            rows(probe)
        };
        assert_eq!(
            metric(spilling.as_ref(), "input_rows"),
            probe_rows,
            "{case}"
        );
        assert_eq!(
            metric(spilling.as_ref(), "build_input_rows"),
            rows(build),
            "{case}"
        );
        assert_eq!(
            metric(spilling.as_ref(), "output_rows"),
            metric(plain.as_ref(), "output_rows"),
            "{case}"
        );
        spilled = Spilled {
            spilled_buckets: metric(spilling.as_ref(), "spilled_buckets"),
            split_buckets: metric(spilling.as_ref(), "split_buckets"),
            chunked_buckets: metric(spilling.as_ref(), "chunked_buckets"),
        };
    }
    Ok(spilled)
}

fn emits_probe_rows_without_build(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Right | JoinType::Full | JoinType::RightAnti | JoinType::RightMark
    )
}

fn limit(bytes: usize) -> SpillingHashJoinConfig {
    SpillingHashJoinConfig {
        max_build_buffer_bytes: Some(bytes),
    }
}

const UNIFORM: Keys = Keys {
    range: 6000,
    null_every: 97,
    hot_percent: 0,
};

/// Without a reason to spill, the operator is the join it wraps.
#[tokio::test]
async fn in_memory() -> Result<()> {
    let build = table(5000, UNIFORM, 1);
    let probe = table(
        8000,
        Keys {
            range: 12000,
            null_every: 89,
            ..UNIFORM
        },
        2,
    );
    for join_type in JOIN_TYPES {
        for with_filter in [false, true] {
            let spilled = check(
                &build,
                &probe,
                join_type,
                with_filter,
                &["k"],
                SpillingHashJoinConfig::default(),
                None,
            )
            .await?;
            assert_eq!(spilled.spilled_buckets, 0);
        }
    }
    Ok(())
}

/// Some buckets stay in memory and the spilled ones each fit when read back.
#[tokio::test]
async fn bucketed() -> Result<()> {
    let build = table(5000, UNIFORM, 3);
    let probe = table(
        8000,
        Keys {
            range: 12000,
            null_every: 89,
            ..UNIFORM
        },
        4,
    );
    for join_type in JOIN_TYPES {
        for with_filter in [false, true] {
            let spilled = check(
                &build,
                &probe,
                join_type,
                with_filter,
                &["k"],
                limit(96 * 1024),
                None,
            )
            .await?;
            assert!(spilled.spilled_buckets > 0, "{join_type} did not spill");
            assert_eq!(spilled.split_buckets, 0);
            assert_eq!(spilled.chunked_buckets, 0);
        }
    }
    Ok(())
}

/// Every bucket spills and has to be split again before it fits: a bucket holds about 10 KB of
/// build rows, over the limit, and a bucket split again about 600 bytes.
#[tokio::test]
async fn split_again() -> Result<()> {
    let build = table(5000, UNIFORM, 5);
    let probe = table(
        8000,
        Keys {
            range: 12000,
            null_every: 89,
            ..UNIFORM
        },
        6,
    );
    for join_type in JOIN_TYPES {
        let spilled = check(
            &build,
            &probe,
            join_type,
            true,
            &["k"],
            limit(6 * 1024),
            None,
        )
        .await?;
        assert!(spilled.split_buckets > 0, "{join_type} did not split");
        assert_eq!(spilled.chunked_buckets, 0);
    }
    Ok(())
}

/// A key that holds most of the build rows cannot be split, so its bucket is joined in chunks.
#[tokio::test]
async fn hot_key_in_chunks() -> Result<()> {
    let hot = Keys {
        range: 3000,
        null_every: 0,
        hot_percent: 60,
    };
    let build = table(4000, hot, 7);
    let probe = table(
        3000,
        Keys {
            hot_percent: 5,
            ..hot
        },
        8,
    );
    for join_type in JOIN_TYPES {
        for with_filter in [false, true] {
            let spilled = check(
                &build,
                &probe,
                join_type,
                with_filter,
                &["k"],
                limit(32 * 1024),
                None,
            )
            .await?;
            assert!(spilled.chunked_buckets > 0, "{join_type} was not chunked");
        }
    }
    Ok(())
}

/// Build rows with a null key all land in one bucket, which cannot be split either.
#[tokio::test]
async fn many_null_keys_in_chunks() -> Result<()> {
    let nulls = Keys {
        range: 3000,
        null_every: 2,
        hot_percent: 0,
    };
    let build = table(4000, nulls, 9);
    let probe = table(3000, nulls, 10);
    for join_type in JOIN_TYPES {
        let spilled = check(
            &build,
            &probe,
            join_type,
            false,
            &["k"],
            limit(24 * 1024),
            None,
        )
        .await?;
        assert!(spilled.chunked_buckets > 0, "{join_type} was not chunked");
    }
    Ok(())
}

/// A real memory pool, rather than the configured limit, makes the join spill.
#[tokio::test]
async fn pool_refusals() -> Result<()> {
    let build = table(
        20000,
        Keys {
            range: 30000,
            ..UNIFORM
        },
        11,
    );
    let probe = table(
        20000,
        Keys {
            range: 60000,
            ..UNIFORM
        },
        12,
    );
    for join_type in JOIN_TYPES {
        let spilled = check(
            &build,
            &probe,
            join_type,
            false,
            &["k"],
            SpillingHashJoinConfig::default(),
            Some(1024 * 1024),
        )
        .await?;
        assert!(spilled.spilled_buckets > 0, "{join_type} did not spill");
    }
    Ok(())
}

/// Two keys, one of them a string, so the inner joins use a hash map, not an `ArrayMap`.
#[tokio::test]
async fn composite_keys() -> Result<()> {
    let build = table(
        3000,
        Keys {
            range: 50,
            ..UNIFORM
        },
        13,
    );
    let probe = table(
        3000,
        Keys {
            range: 60,
            ..UNIFORM
        },
        14,
    );
    for join_type in JOIN_TYPES {
        check(
            &build,
            &probe,
            join_type,
            true,
            &["k", "s"],
            limit(16 * 1024),
            None,
        )
        .await?;
        check(
            &build,
            &probe,
            join_type,
            false,
            &["k", "v"],
            limit(16 * 1024),
            None,
        )
        .await?;
    }
    Ok(())
}

/// Every bucket spills, so the join of the resident buckets has no build rows and stops before it
/// reads the probe input for the join types that cannot emit anything then. The rest of the
/// probe input has to be drained into the probe files.
#[tokio::test]
async fn everything_spills() -> Result<()> {
    let build = table(
        300,
        Keys {
            range: 200,
            ..UNIFORM
        },
        16,
    );
    let probe = table(
        500,
        Keys {
            range: 400,
            ..UNIFORM
        },
        17,
    );
    for join_type in JOIN_TYPES {
        let spilled = check(&build, &probe, join_type, false, &["k"], limit(1), None).await?;
        assert!(spilled.spilled_buckets > 0, "{join_type} did not spill");
    }
    Ok(())
}

/// Empty build and probe sides, with and without spilling.
#[tokio::test]
async fn empty_sides() -> Result<()> {
    let rows = table(2000, UNIFORM, 15);
    for join_type in JOIN_TYPES {
        for config in [SpillingHashJoinConfig::default(), limit(1)] {
            check(&[], &rows, join_type, false, &["k"], config.clone(), None).await?;
            check(&rows, &[], join_type, false, &["k"], config, None).await?;
        }
    }
    Ok(())
}

/// Fails a request the way a failed Spark acquire does: with an error that is not a refusal.
#[derive(Debug)]
struct FailingPool {
    used: AtomicUsize,
    limit: usize,
}

impl std::fmt::Display for FailingPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailingPool")
    }
}

impl MemoryPool for FailingPool {
    fn name(&self) -> &str {
        "FailingPool"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, AtomicOrdering::SeqCst);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, AtomicOrdering::SeqCst);
    }

    fn try_grow(&self, _reservation: &MemoryReservation, additional: usize) -> Result<()> {
        if self.used.load(AtomicOrdering::SeqCst) + additional > self.limit {
            return Err(DataFusionError::External("the acquire failed".into()));
        }
        self.used.fetch_add(additional, AtomicOrdering::SeqCst);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(AtomicOrdering::SeqCst)
    }
}

/// Only a refused request is a reason to spill. Any other error from the pool is the query's.
#[tokio::test]
async fn a_failed_acquire_is_not_a_refusal() -> Result<()> {
    let build = table(
        20000,
        Keys {
            range: 30000,
            ..UNIFORM
        },
        18,
    );
    let probe = table(
        20000,
        Keys {
            range: 60000,
            ..UNIFORM
        },
        19,
    );
    let plain = hash_join(&build, &probe, JoinType::Inner, false, &["k"])?;
    let spilling = SpillingHashJoinExec::try_new(&plain, SpillingHashJoinConfig::default())?
        .expect("a partitioned join is wrapped");
    let pool = Arc::new(FailingPool {
        used: AtomicUsize::new(0),
        limit: 256 * 1024,
    });
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()?;
    let context = TaskContext::default()
        .with_session_config(SessionConfig::new().with_batch_size(1000))
        .with_runtime(runtime);
    let error = collect(Arc::new(spilling), Arc::new(context))
        .await
        .expect_err("the failed acquire fails the join");
    assert!(error.to_string().contains("the acquire failed"), "{error}");
    Ok(())
}

/// A join dropped before it finishes deletes its spill files.
#[tokio::test]
async fn dropping_the_join_deletes_its_spill_files() -> Result<()> {
    let build = table(5000, UNIFORM, 20);
    let probe = table(
        8000,
        Keys {
            range: 12000,
            null_every: 89,
            ..UNIFORM
        },
        21,
    );
    let plain = hash_join(&build, &probe, JoinType::Inner, false, &["k"])?;
    let spilling = SpillingHashJoinExec::try_new(&plain, limit(96 * 1024))?
        .expect("a partitioned join is wrapped");
    let context = context(None, SpillCompression::Uncompressed);
    let disk = Arc::clone(&context.runtime_env().disk_manager);
    let mut output = spilling.execute(0, context)?;
    // The join of the resident buckets emits first, once every spilled bucket has its build file.
    assert!(output.next().await.transpose()?.is_some());
    assert!(disk.used_disk_space() > 0, "nothing was spilled");
    drop(output);
    // A spill file being read back is also held by the reader's blocking task until that task
    // sees the stream is gone.
    for _ in 0..200 {
        if disk.used_disk_space() == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(disk.used_disk_space(), 0);
    Ok(())
}
