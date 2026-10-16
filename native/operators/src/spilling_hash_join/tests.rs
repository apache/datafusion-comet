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

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
    FixedSizeBinaryArray, Int64Array, LargeStringArray, ListArray, RecordBatch, StringArray,
    StringViewArray, StructArray, TimestampMicrosecondArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef, TimeUnit};
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

/// The inputs of a join under test.
struct Input<'a> {
    build_schema: SchemaRef,
    build: &'a [RecordBatch],
    probe_schema: SchemaRef,
    probe: &'a [RecordBatch],
    /// The session's batch size, which is also the most rows a batch of a spill file holds.
    batch_size: usize,
}

impl<'a> Input<'a> {
    /// Tables of [`schema`], joined with a batch size of 1000.
    fn new(build: &'a [RecordBatch], probe: &'a [RecordBatch]) -> Self {
        Self {
            build_schema: schema(),
            build,
            probe_schema: schema(),
            probe,
            batch_size: 1000,
        }
    }
}

fn hash_join(
    build: &[RecordBatch],
    probe: &[RecordBatch],
    join_type: JoinType,
    with_filter: bool,
    on: &[&str],
) -> Result<HashJoinExec> {
    hash_join_of(&Input::new(build, probe), join_type, with_filter, on)
}

fn hash_join_of(
    input: &Input,
    join_type: JoinType,
    with_filter: bool,
    on: &[&str],
) -> Result<HashJoinExec> {
    let build_input = MemorySourceConfig::try_new_exec(
        &[input.build.to_vec()],
        Arc::clone(&input.build_schema),
        None,
    )?;
    let probe_input = MemorySourceConfig::try_new_exec(
        &[input.probe.to_vec()],
        Arc::clone(&input.probe_schema),
        None,
    )?;
    let on = on
        .iter()
        .map(|name| {
            let build: PhysicalExprRef = Arc::new(Column::new(
                name,
                input.build_schema.index_of(name).unwrap(),
            ));
            let probe: PhysicalExprRef = Arc::new(Column::new(
                name,
                input.probe_schema.index_of(name).unwrap(),
            ));
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
    context_with(pool_bytes, compression, 1000)
}

fn context_with(
    pool_bytes: Option<usize>,
    compression: SpillCompression,
    batch_size: usize,
) -> Arc<TaskContext> {
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(bytes) = pool_bytes {
        runtime = runtime.with_memory_pool(Arc::new(GreedyMemoryPool::new(bytes)));
    }
    Arc::new(
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(batch_size)
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
    bloom_filtered_rows: usize,
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
    let input = Input::new(build, probe);
    check_input(&input, join_type, with_filter, on, config, pool_bytes).await
}

async fn check_input(
    input: &Input<'_>,
    join_type: JoinType,
    with_filter: bool,
    on: &[&str],
    config: SpillingHashJoinConfig,
    pool_bytes: Option<usize>,
) -> Result<Spilled> {
    let (build, probe) = (input.build, input.probe);
    let plain = Arc::new(hash_join_of(input, join_type, with_filter, on)?);
    let unlimited = context_with(None, SpillCompression::Uncompressed, input.batch_size);
    let expected = rows(&collect(Arc::clone(&plain) as _, unlimited).await?);
    let mut spilled = Spilled::default();
    for compression in [SpillCompression::Uncompressed, SpillCompression::Lz4Frame] {
        let spilling = Arc::new(
            SpillingHashJoinExec::try_new(&plain, config.clone())?
                .expect("a partitioned join is wrapped"),
        );
        assert_eq!(spilling.schema(), plain.schema());
        let context = context_with(pool_bytes, compression, input.batch_size);
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
            bloom_filtered_rows: metric(spilling.as_ref(), "bloom_filtered_rows"),
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

fn emits_build_rows_without_probe(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::Full | JoinType::LeftAnti | JoinType::LeftMark
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
            // Half of the probe keys are not build keys, so the filters of the spilled buckets
            // keep many probe rows off disk.
            assert!(
                spilled.bloom_filtered_rows > 1000,
                "{join_type} filtered {} probe rows",
                spilled.bloom_filtered_rows
            );
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

/// Build rows with a null key all land in one bucket, which cannot be split either. A join type
/// that emits unmatched build rows joins that bucket in chunks. Others may skip it, when the
/// bucket's filter keeps every probe row of it off disk.
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
        if emits_build_rows_without_probe(join_type) {
            assert!(spilled.chunked_buckets > 0, "{join_type} was not chunked");
        }
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

/// A column of `keys`, as `data_type`. Equal keys give equal values, and a null key a null.
fn key_column(keys: &[Option<i64>], data_type: &DataType) -> ArrayRef {
    let text = |key: i64| format!("key-{key}-of-a-long-enough-string");
    match data_type {
        DataType::Decimal128(precision, scale) => Arc::new(
            Decimal128Array::from_iter(keys.iter().map(|key| key.map(|key| key as i128 * 101)))
                .with_precision_and_scale(*precision, *scale)
                .unwrap(),
        ),
        DataType::Date32 => Arc::new(Date32Array::from_iter(
            keys.iter().map(|key| key.map(|key| key as i32)),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => Arc::new(
            TimestampMicrosecondArray::from_iter(
                keys.iter().map(|key| key.map(|key| key * 1_000_003)),
            )
            .with_timezone_opt(timezone.clone()),
        ),
        DataType::Utf8View => Arc::new(StringViewArray::from_iter(
            keys.iter().map(|key| key.map(text)),
        )),
        DataType::Binary => Arc::new(BinaryArray::from_iter(
            keys.iter().map(|key| key.map(|key| text(key).into_bytes())),
        )),
        DataType::Dictionary(_, _) => {
            let values: Vec<Option<String>> = keys.iter().map(|key| key.map(text)).collect();
            Arc::new(
                values
                    .iter()
                    .map(Option::as_deref)
                    .collect::<DictionaryArray<Int32Type>>(),
            )
        }
        other => unimplemented!("key type {other}"),
    }
}

/// A payload column of `rows` rows, as `data_type`, with a null in every seventh row.
fn payload_column(rows: std::ops::Range<usize>, data_type: &DataType) -> ArrayRef {
    let value = |row: usize| (row % 7 != 3).then_some(row);
    match data_type {
        DataType::List(_) => Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(rows.map(
            |row| {
                value(row).map(|row| {
                    (0..row % 4)
                        .map(|i| Some((row + i) as i32))
                        .collect::<Vec<_>>()
                })
            },
        ))),
        DataType::Struct(fields) => {
            let a = Int64Array::from_iter(rows.clone().map(|row| value(row).map(|row| row as i64)));
            let b =
                StringArray::from_iter(rows.clone().map(|row| value(row).map(|r| format!("b{r}"))));
            let nulls = NullBuffer::from_iter(rows.map(|row| row % 11 != 5));
            Arc::new(StructArray::new(
                fields.clone(),
                vec![Arc::new(a), Arc::new(b)],
                Some(nulls),
            ))
        }
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from_iter(
            rows.map(|row| value(row).map(|row| format!("large-{row}"))),
        )),
        DataType::Utf8View => {
            Arc::new(StringViewArray::from_iter(rows.map(|row| {
                value(row).map(|row| format!("a view longer than twelve bytes {row}"))
            })))
        }
        DataType::Dictionary(_, _) => {
            let values: Vec<Option<String>> = rows
                .map(|row| value(row).map(|row| format!("d{}", row % 13)))
                .collect();
            Arc::new(
                values
                    .iter()
                    .map(Option::as_deref)
                    .collect::<DictionaryArray<Int32Type>>(),
            )
        }
        DataType::Decimal128(precision, scale) => Arc::new(
            Decimal128Array::from_iter(
                rows.map(|row| value(row).map(|row| row as i128 * 1_000_000_007)),
            )
            .with_precision_and_scale(*precision, *scale)
            .unwrap(),
        ),
        DataType::FixedSizeBinary(width) => Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                rows.map(|row| value(row).map(|row| vec![row as u8; *width as usize])),
                *width,
            )
            .unwrap(),
        ),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(
            rows.map(|row| value(row).map(|row| row % 3 == 0)),
        )),
        DataType::Int64 => Arc::new(Int64Array::from_iter(
            rows.map(|row| value(row).map(|row| row as i64 * 31)),
        )),
        DataType::Utf8 => {
            Arc::new(StringArray::from_iter(rows.map(|row| {
                value(row).map(|row| format!("{row}-{}", "w".repeat(2000)))
            })))
        }
        other => unimplemented!("payload type {other}"),
    }
}

/// A table of a key `k` of `key_type`, drawn as [`table`] draws it, and payload columns `p0`,
/// `p1` and so on of `payload`, in batches of `batch_rows`.
fn typed_table(
    rows: usize,
    keys: Keys,
    seed: u64,
    key_type: &DataType,
    payload: &[DataType],
    batch_rows: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    let mut fields = vec![Field::new("k", key_type.clone(), true)];
    for (index, data_type) in payload.iter().enumerate() {
        fields.push(Field::new(format!("p{index}"), data_type.clone(), true));
    }
    let schema = Arc::new(Schema::new(fields));
    let mut random = SplitMix64(seed);
    let all_keys: Vec<Option<i64>> = (0..rows)
        .map(|row| {
            let draw = random.next();
            if keys.null_every > 0 && row % keys.null_every == 0 {
                None
            } else if draw % 100 < keys.hot_percent {
                Some(7)
            } else {
                Some(((draw >> 8) % keys.range) as i64)
            }
        })
        .collect();
    let batches = (0..rows)
        .step_by(batch_rows)
        .map(|start| {
            let end = (start + batch_rows).min(rows);
            let mut columns = vec![key_column(&all_keys[start..end], key_type)];
            columns.extend(
                payload
                    .iter()
                    .map(|data_type| payload_column(start..end, data_type)),
            );
            RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
        })
        .collect();
    (schema, batches)
}

/// Keys of every type a hash join key commonly has, routed, spilled, read back, split again, and
/// joined in chunks.
#[tokio::test]
async fn key_types() -> Result<()> {
    let key_types = [
        DataType::Decimal128(18, 2),
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Utf8View,
        DataType::Binary,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ];
    let hot = Keys {
        range: 3000,
        null_every: 53,
        hot_percent: 30,
    };
    for key_type in &key_types {
        let payload = [DataType::Int64];
        let (build_schema, build) = typed_table(3000, hot, 30, key_type, &payload, 500);
        let (probe_schema, probe) = typed_table(3000, UNIFORM, 31, key_type, &payload, 500);
        let input = Input {
            build_schema,
            build: &build,
            probe_schema,
            probe: &probe,
            batch_size: 1000,
        };
        for join_type in JOIN_TYPES {
            for config in [limit(64 * 1024), limit(8 * 1024)] {
                check_input(&input, join_type, false, &["k"], config, None)
                    .await
                    .map_err(|e| e.context(format!("key {key_type}, {join_type}")))?;
            }
        }
    }
    Ok(())
}

/// Payloads of nested, variable-width and dictionary types survive routing, the parts a spill
/// file's writer concatenates, and the copy of a build side read back.
#[tokio::test]
async fn payload_types() -> Result<()> {
    let payload = [
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
        DataType::Struct(
            vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
        ),
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        DataType::Decimal128(38, 10),
        DataType::FixedSizeBinary(16),
        DataType::Boolean,
    ];
    let key_type = DataType::Utf8View;
    let (build_schema, build) = typed_table(4000, UNIFORM, 32, &key_type, &payload, 500);
    let (probe_schema, probe) = typed_table(
        4000,
        Keys {
            range: 12000,
            null_every: 89,
            ..UNIFORM
        },
        33,
        &key_type,
        &payload,
        500,
    );
    let input = Input {
        build_schema,
        build: &build,
        probe_schema,
        probe: &probe,
        batch_size: 1000,
    };
    for join_type in JOIN_TYPES {
        for config in [limit(128 * 1024), limit(16 * 1024)] {
            let spilled = check_input(&input, join_type, false, &["k"], config, None).await?;
            assert!(spilled.spilled_buckets > 0, "{join_type} did not spill");
        }
    }
    Ok(())
}

/// Rows of 2 KB, so that the batches a spill file's writer writes are larger than the chunks
/// DataFusion reads spill files in. A message that spans chunks is read into a buffer that grows
/// by doubling, and the build side read back is copied so that it holds what was reserved.
#[tokio::test]
async fn wide_rows() -> Result<()> {
    let payload = [DataType::Utf8];
    let (build_schema, build) = typed_table(1500, UNIFORM, 34, &DataType::Utf8View, &payload, 100);
    let (probe_schema, probe) = typed_table(
        3000,
        Keys {
            range: 12000,
            ..UNIFORM
        },
        35,
        &DataType::Utf8View,
        &[DataType::Int64],
        100,
    );
    let input = Input {
        build_schema,
        build: &build,
        probe_schema,
        probe: &probe,
        batch_size: 8192,
    };
    for join_type in [JoinType::Inner, JoinType::Full, JoinType::RightAnti] {
        let spilled =
            check_input(&input, join_type, false, &["k"], limit(1024 * 1024), None).await?;
        assert!(spilled.spilled_buckets > 0, "{join_type} did not spill");
    }
    Ok(())
}
