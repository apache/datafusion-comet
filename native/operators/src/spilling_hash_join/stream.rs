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

//! The execution of [`super::SpillingHashJoinExec`] on one partition.

use std::mem;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::utils::memory::{get_record_batch_memory_size, RecordBatchMemoryCounter};
use datafusion::common::{internal_err, JoinType, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, SpillMetrics,
};
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;
use parking_lot::Mutex;

use super::bloom::KeyFilter;
use super::chunked::ChunkedPair;
use super::memory::{
    try_reserve, BudgetPool, BuildMemory, BuildMemoryEstimator, PerfectHashConfig,
};
use super::probe::{ProbeRouter, RoutedProbeStream};
use super::routing::{rows_by_bucket, take_rows, Router, MAX_LEVEL, NUM_BUCKETS};
use super::source::{batches_stream, StreamSourceExec};
use super::spill::{SpillFileWriter, SpillWriterFactory, SpilledFile};
use super::SpillingHashJoinConfig;

/// What every execution of a [`super::SpillingHashJoinExec`] runs.
#[derive(Debug)]
pub(super) struct JoinSpec {
    /// The `HashJoinExec` that this operator runs, without execution state. Its left input is the
    /// build side and its right input the probe side.
    pub(super) template: HashJoinExec,
    pub(super) config: SpillingHashJoinConfig,
}

impl JoinSpec {
    fn build_keys(&self) -> Vec<PhysicalExprRef> {
        self.template
            .on()
            .iter()
            .map(|(build, _)| Arc::clone(build))
            .collect()
    }

    fn probe_keys(&self) -> Vec<PhysicalExprRef> {
        self.template
            .on()
            .iter()
            .map(|(_, probe)| Arc::clone(probe))
            .collect()
    }

    fn build_schema(&self) -> SchemaRef {
        self.template.left().schema()
    }

    fn probe_schema(&self) -> SchemaRef {
        self.template.right().schema()
    }
}

/// A spilled bucket: the build rows and probe rows that hash to it.
struct Pair {
    build: Option<SpilledFile>,
    probe: Option<SpilledFile>,
    level: u32,
    /// False when splitting it last time left most of its build rows in it.
    splittable: bool,
}

enum Phase {
    /// Before the build side is read.
    Start(SendableRecordBatchStream, SendableRecordBatchStream),
    /// Emitting the output of an inner join, or of the chunked fallback's final pass.
    Running(SendableRecordBatchStream, After),
    /// Between spilled pairs.
    Next,
    Done,
}

/// What follows the stream that is running.
enum After {
    /// An inner join over a build side held in memory or read back from a pair's files.
    Join,
    /// The join of the resident buckets. The probe rows of the spilled buckets still have to be
    /// written to their files, from the router, before the spilled pairs are joined.
    Resident(Arc<Mutex<ProbeRouter>>, Vec<Option<SpilledFile>>),
    /// One chunk of a spilled bucket joined in chunks.
    Chunk(Box<ChunkedPair>),
    /// The final pass of the chunked fallback.
    FinalPass,
}

/// The build rows of each bucket during a bucketed build: held in memory, or written to the
/// bucket's file once the bucket has been destaged, with a filter of the bucket's keys.
struct BuildBuckets {
    buckets: Vec<BuildBucket>,
    /// Hashes the rows of spilled buckets for their filters.
    filter_router: Router,
    filter_memory: Arc<MemoryReservation>,
}

enum BuildBucket {
    Resident {
        batches: Vec<RecordBatch>,
        bytes: usize,
    },
    Spilled {
        writer: Box<SpillFileWriter>,
        /// None if the pool refused its memory.
        filter: Option<KeyFilter>,
    },
}

impl BuildBuckets {
    fn new(keys: Vec<PhysicalExprRef>, filter_memory: Arc<MemoryReservation>) -> Self {
        Self {
            buckets: (0..NUM_BUCKETS)
                .map(|_| BuildBucket::Resident {
                    batches: Vec::new(),
                    bytes: 0,
                })
                .collect(),
            filter_router: Router::new(keys),
            filter_memory,
        }
    }

    /// The resident bucket that holds the most memory, unless none holds any.
    fn largest_resident(&self) -> Option<usize> {
        self.buckets
            .iter()
            .enumerate()
            .filter_map(|(bucket, state)| match state {
                BuildBucket::Resident { bytes, .. } if *bytes > 0 => Some((bucket, *bytes)),
                _ => None,
            })
            .max_by_key(|(_, bytes)| *bytes)
            .map(|(bucket, _)| bucket)
    }

    /// Writes a resident bucket to `writer`, which then takes the bucket's later rows, and starts
    /// the filter of the bucket's keys. The bucket's memory leaves `held` before its rows are
    /// written, so that the writer can hold the last of them until they make a full batch.
    fn destage(
        &mut self,
        bucket: usize,
        mut writer: SpillFileWriter,
        held: &MemoryReservation,
    ) -> Result<()> {
        let BuildBucket::Resident { batches, bytes } = &mut self.buckets[bucket] else {
            return internal_err!("bucket {bucket} is already spilled");
        };
        let batches = mem::take(batches);
        held.shrink(mem::take(bytes));
        let rows = batches.iter().map(RecordBatch::num_rows).sum();
        let mut filter = KeyFilter::try_new(rows, Arc::clone(&self.filter_memory))?;
        for batch in batches {
            add_to_filter(&mut filter, &mut self.filter_router, &batch)?;
            writer.append(batch)?;
        }
        self.buckets[bucket] = BuildBucket::Spilled {
            writer: Box::new(writer),
            filter,
        };
        Ok(())
    }

    fn resident_batches(&self) -> Vec<RecordBatch> {
        self.buckets
            .iter()
            .flat_map(|state| match state {
                BuildBucket::Resident { batches, .. } => batches.clone(),
                BuildBucket::Spilled { .. } => vec![],
            })
            .collect()
    }

    /// The batches of the resident buckets, and the build file and key filter of each spilled
    /// bucket.
    fn finish(self) -> Result<FinishedBuild> {
        let mut resident = Vec::new();
        let mut files = Vec::with_capacity(NUM_BUCKETS);
        let mut filters = Vec::with_capacity(NUM_BUCKETS);
        for state in self.buckets {
            match state {
                BuildBucket::Resident { batches, .. } => {
                    resident.extend(batches);
                    files.push(None);
                    filters.push(None);
                }
                BuildBucket::Spilled { writer, filter } => {
                    files.push(Some((*writer).finish()?));
                    filters.push(filter);
                }
            }
        }
        Ok(FinishedBuild {
            resident,
            files,
            filters,
        })
    }
}

/// A bucketed build once the build input has ended.
struct FinishedBuild {
    resident: Vec<RecordBatch>,
    files: Vec<Option<SpilledFile>>,
    filters: Vec<Option<KeyFilter>>,
}

/// Adds the keys of `batch` to `filter`, and drops the filter if the pool refuses it the memory
/// to grow, because a filter missing a key would send that key's probe rows to the wrong join.
fn add_to_filter(
    filter: &mut Option<KeyFilter>,
    router: &mut Router,
    batch: &RecordBatch,
) -> Result<()> {
    let Some(keys) = filter.as_mut() else {
        return Ok(());
    };
    let mut complete = true;
    for hash in router.hash(batch)? {
        if !keys.insert(*hash)? {
            complete = false;
            break;
        }
    }
    if !complete {
        *filter = None;
    }
    Ok(())
}

/// Joins one partition.
pub(super) struct Driver {
    spec: Arc<JoinSpec>,
    partition: usize,
    /// The context of the inner joins, whose memory pool is `budget`.
    inner_context: Arc<TaskContext>,
    budget: Arc<BudgetPool>,
    estimator: BuildMemoryEstimator,
    build_files: SpillWriterFactory,
    probe_files: SpillWriterFactory,
    build_reader: SpillManager,
    probe_reader: SpillManager,
    plan_metrics: ExecutionPlanMetricsSet,
    spilled_buckets: Count,
    split_buckets: Count,
    chunked_buckets: Count,
    /// The most memory the operator has reserved at once.
    build_mem_used: Gauge,
    /// Build batches held in memory: the whole build side while it fits, the resident buckets,
    /// or a spilled pair's or a chunk's build side while its inner join runs.
    held: MemoryReservation,
    /// What the running inner join needs besides its build batches: its budget beyond them, the
    /// memory it allocates without reserving, and the read-ahead of the files it reads.
    inner: MemoryReservation,
    /// The running inner join's concatenated copy of its build batches, until it emits.
    concat: MemoryReservation,
    /// Memory to route one batch with, and to encode one batch for a spill file.
    headroom: MemoryReservation,
    /// The chunked fallback's record of matched probe rows.
    bitmap: MemoryReservation,
    /// Rows that spill file writers hold until they make a full batch.
    pending: Arc<MemoryReservation>,
    /// The filters of spilled buckets' build keys.
    filter_memory: Arc<MemoryReservation>,
    /// Probe rows of spilled buckets that a filter showed to match nothing, and that were joined
    /// with the resident buckets instead of being written to disk.
    filtered_probe_rows: Count,
    phase: Phase,
    pairs: Vec<Pair>,
}

impl Driver {
    pub(super) fn try_new(
        spec: Arc<JoinSpec>,
        partition: usize,
        context: Arc<TaskContext>,
        plan_metrics: ExecutionPlanMetricsSet,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<Self> {
        // One consumer for everything, as a plain `HashJoinExec` has, because Comet's fair pool
        // divides its memory by the consumers registered in the task.
        let held = MemoryConsumer::new(format!("HashJoinInput[{partition}]"))
            .with_can_spill(true)
            .register(context.memory_pool());
        let budget = Arc::new(BudgetPool::new(
            held.new_empty(),
            MetricBuilder::new(&plan_metrics).counter("budget_overflow_bytes", partition),
        ));
        let env = context.runtime_env();
        let inner_env = RuntimeEnvBuilder::from_runtime_env(&env)
            .with_memory_pool(Arc::clone(&budget) as Arc<dyn MemoryPool>)
            .build_arc()?;
        let inner_context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            context.session_config().clone(),
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            inner_env,
        ));
        let execution = &context.session_config().options().execution;
        let estimator = BuildMemoryEstimator::try_new(
            &spec.build_keys(),
            &spec.build_schema(),
            spec.template.join_type,
            spec.template.null_equality(),
            PerfectHashConfig {
                small_build_threshold: execution.perfect_hash_join_small_build_threshold,
                min_key_density: execution.perfect_hash_join_min_key_density,
            },
        )?;
        let compression = context.session_config().spill_compression();
        let batch_rows = context.session_config().batch_size();
        let pending = Arc::new(held.new_empty());
        let spill_metrics = SpillMetrics::new(&plan_metrics, partition);
        let build_files = SpillWriterFactory::new(
            Arc::clone(&env),
            spec.build_schema(),
            compression,
            spill_metrics.clone(),
            "HashJoinInput build spill",
            batch_rows,
            Arc::clone(&pending),
        );
        let probe_files = SpillWriterFactory::new(
            Arc::clone(&env),
            spec.probe_schema(),
            compression,
            spill_metrics.clone(),
            "HashJoinInput probe spill",
            batch_rows,
            Arc::clone(&pending),
        );
        let build_reader =
            SpillManager::new(Arc::clone(&env), spill_metrics.clone(), spec.build_schema());
        let probe_reader = SpillManager::new(env, spill_metrics, spec.probe_schema());
        Ok(Self {
            partition,
            inner_context,
            budget,
            estimator,
            build_files,
            probe_files,
            build_reader,
            probe_reader,
            spilled_buckets: MetricBuilder::new(&plan_metrics)
                .counter("spilled_buckets", partition),
            split_buckets: MetricBuilder::new(&plan_metrics).counter("split_buckets", partition),
            build_mem_used: MetricBuilder::new(&plan_metrics)
                .peak_memory_usage("build_mem_used", partition),
            chunked_buckets: MetricBuilder::new(&plan_metrics)
                .counter("chunked_buckets", partition),
            filtered_probe_rows: MetricBuilder::new(&plan_metrics)
                .counter("bloom_filtered_rows", partition),
            plan_metrics,
            inner: held.new_empty(),
            concat: held.new_empty(),
            headroom: held.new_empty(),
            bitmap: held.new_empty(),
            pending,
            filter_memory: Arc::new(held.new_empty()),
            held,
            phase: Phase::Start(build, probe),
            pairs: Vec::new(),
            spec,
        })
    }

    /// The next output batch, or None when the join is complete.
    pub(super) async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            match &mut self.phase {
                Phase::Running(output, after) => match output.next().await {
                    Some(Ok(batch)) => {
                        // The inner join has finished its build, and with it the copy.
                        self.concat.free();
                        let batch = match after {
                            After::Chunk(chunked) => chunked.on_output(batch)?,
                            _ => Some(batch),
                        };
                        if let Some(batch) = batch {
                            return Ok(Some(batch));
                        }
                    }
                    Some(Err(e)) => return Err(e),
                    None => {
                        // Dropping the stream drops the inner join and frees what it reserved.
                        let Phase::Running(_, after) = mem::replace(&mut self.phase, Phase::Next)
                        else {
                            unreachable!()
                        };
                        self.finish_stream(after).await?;
                    }
                },
                Phase::Start(..) => {
                    let Phase::Start(build, probe) = mem::replace(&mut self.phase, Phase::Next)
                    else {
                        unreachable!()
                    };
                    self.start(build, probe).await?;
                }
                Phase::Next => match self.pairs.pop() {
                    Some(pair) => self.start_pair(pair).await?,
                    None => self.phase = Phase::Done,
                },
                Phase::Done => return Ok(None),
            }
        }
    }

    /// Reads the build side, and joins it in memory if it fits.
    async fn start(
        &mut self,
        mut build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<()> {
        // Count the batches the way the inner join will, so that its budget covers them.
        let mut counter = RecordBatchMemoryCounter::new();
        let mut batches = Vec::new();
        let mut sizes = Vec::new();
        while let Some(batch) = build.next().await.transpose()? {
            let size = counter.count_batch(&batch);
            if !self.reserve_headroom(&batch)? || !self.reserve_held(size)? {
                return self
                    .bucketed(batches, sizes, Some(batch), build, probe)
                    .await;
            }
            batches.push(batch);
            sizes.push(size);
        }
        let memory = self.estimator.for_batches(&batches, self.held.size())?;
        if self.reserve_inner(memory, 0)? {
            let budget = self.held.size() + memory.reserved;
            let join = self.inner_join(batches_stream(self.spec.build_schema(), batches), probe)?;
            return self.start_inner(join, budget, After::Join);
        }
        let rest = batches_stream(self.spec.build_schema(), vec![]);
        self.bucketed(batches, sizes, None, rest, probe).await
    }

    /// Splits the build side into buckets, destaging the largest whenever memory runs out, and
    /// then joins the buckets that stayed in memory. `buffered` holds the batches read so far,
    /// reserved at `sizes`, and `pending` one read but not reserved.
    async fn bucketed(
        &mut self,
        buffered: Vec<RecordBatch>,
        sizes: Vec<usize>,
        pending: Option<RecordBatch>,
        mut build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<()> {
        let mut buckets =
            BuildBuckets::new(self.spec.build_keys(), Arc::clone(&self.filter_memory));
        let mut router = Router::new(self.spec.build_keys());
        for (batch, size) in buffered.into_iter().zip(sizes) {
            // The batch's memory passes to the buckets that its rows go to.
            self.held.shrink(size);
            self.place_build_batch(&mut buckets, &mut router, &batch)?;
        }
        if let Some(batch) = pending {
            self.place_build_batch(&mut buckets, &mut router, &batch)?;
        }
        while let Some(batch) = build.next().await.transpose()? {
            // Routing holds a copy of the batch's rows while it splits them. If the headroom
            // cannot grow, destaging still makes the room.
            self.reserve_headroom(&batch)?;
            self.place_build_batch(&mut buckets, &mut router, &batch)?;
        }
        // Make room for the join of the resident buckets, destaging more of them if needed.
        let memory = loop {
            let memory = self
                .estimator
                .for_batches(&buckets.resident_batches(), self.held.size())?;
            if self.reserve_inner(memory, 0)? {
                break memory;
            }
            match buckets.largest_resident() {
                Some(bucket) => self.destage(&mut buckets, bucket)?,
                None => {
                    // Nothing is resident. The join still runs, without build rows, for the
                    // probe rows with a null key.
                    self.inner.grow(memory.reserved + memory.unreserved);
                    break memory;
                }
            }
        };
        let FinishedBuild {
            resident,
            files: build_files,
            filters,
        } = buckets.finish()?;
        let mut is_resident = [true; NUM_BUCKETS];
        for (bucket, file) in build_files.iter().enumerate() {
            is_resident[bucket] = file.is_none();
        }
        let router = Arc::new(Mutex::new(ProbeRouter::new(
            probe,
            self.spec.probe_keys(),
            is_resident,
            filters,
            self.probe_files.clone(),
            self.filtered_probe_rows.clone(),
        )));
        let routed = RoutedProbeStream::new_stream(Arc::clone(&router), self.spec.probe_schema());
        let budget = self.held.size() + memory.reserved;
        let join = self.inner_join(batches_stream(self.spec.build_schema(), resident), routed)?;
        self.start_inner(join, budget, After::Resident(router, build_files))
    }

    fn place_build_batch(
        &mut self,
        buckets: &mut BuildBuckets,
        router: &mut Router,
        batch: &RecordBatch,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let rows = rows_by_bucket(&router.route(batch, 0, false)?);
        for (bucket, rows) in rows.into_iter().enumerate() {
            if !rows.is_empty() {
                let part = take_rows(batch, rows)?;
                self.add_build_rows(buckets, bucket, part)?;
            }
        }
        Ok(())
    }

    fn add_build_rows(
        &mut self,
        buckets: &mut BuildBuckets,
        bucket: usize,
        part: RecordBatch,
    ) -> Result<()> {
        let size = get_record_batch_memory_size(&part);
        loop {
            match &mut buckets.buckets[bucket] {
                BuildBucket::Spilled { writer, filter } => {
                    add_to_filter(filter, &mut buckets.filter_router, &part)?;
                    return writer.append(part);
                }
                BuildBucket::Resident { batches, bytes } => {
                    if self.reserve_held(size)? {
                        batches.push(part);
                        *bytes += size;
                        return Ok(());
                    }
                }
            }
            // Destage the largest resident bucket, or this one when no other holds anything.
            let victim = buckets.largest_resident().unwrap_or(bucket);
            self.destage(buckets, victim)?;
        }
    }

    fn destage(&mut self, buckets: &mut BuildBuckets, bucket: usize) -> Result<()> {
        // Memory peaks when it runs out.
        self.build_mem_used.set_max(self.reserved());
        buckets.destage(bucket, self.build_files.create()?, &self.held)?;
        self.spilled_buckets.add(1);
        Ok(())
    }

    /// Joins a spilled pair if it fits in memory, and otherwise splits it or joins it in chunks.
    async fn start_pair(&mut self, pair: Pair) -> Result<()> {
        let join_type = self.spec.template.join_type;
        let build_rows = pair.build.as_ref().map_or(0, |file| file.rows);
        let probe_rows = pair.probe.as_ref().map_or(0, |file| file.rows);
        if (build_rows == 0 && !emits_probe_rows_without_build(join_type))
            || (probe_rows == 0 && !emits_build_rows_without_probe(join_type))
        {
            return Ok(());
        }
        let build_bytes = pair.build.as_ref().map_or(0, |file| file.bytes);
        let read_ahead = pair.build.as_ref().map_or(0, SpilledFile::read_ahead_bytes)
            + pair.probe.as_ref().map_or(0, SpilledFile::read_ahead_bytes);
        let mut memory = self.estimator.for_rows(build_rows, build_bytes)?;
        if pair
            .build
            .as_ref()
            .is_some_and(SpilledFile::reads_as_one_batch)
        {
            // The inner join keeps a single build batch as it is, without a concatenated copy.
            memory.transient = 0;
        }
        if self.reserve_held(build_bytes)? {
            if self.reserve_inner(memory, read_ahead)? {
                // The build side is held whole, so it is read back as one batch if it can be, and
                // compacted otherwise.
                let build = match &pair.build {
                    Some(file) => match file.read_as_one_batch(&self.build_reader).await? {
                        Some(batch) => batches_stream(self.spec.build_schema(), vec![batch]),
                        None => file.read_compacted(&self.build_reader)?,
                    },
                    None => batches_stream(self.spec.build_schema(), vec![]),
                };
                let probe = match &pair.probe {
                    Some(file) => file.read(&self.probe_reader)?,
                    None => batches_stream(self.spec.probe_schema(), vec![]),
                };
                let join = self.inner_join(build, probe)?;
                return self.start_inner(join, build_bytes + memory.reserved, After::Join);
            }
            self.held.free();
        }
        if pair.splittable && pair.level < MAX_LEVEL {
            return self.split_pair(pair).await;
        }
        self.chunk_pair(pair).await
    }

    /// Splits a spilled pair on the next bits of its rows' hash.
    async fn split_pair(&mut self, pair: Pair) -> Result<()> {
        self.split_buckets.add(1);
        let level = pair.level + 1;
        let parent_rows = pair.build.as_ref().map_or(0, |file| file.rows);
        let build = match &pair.build {
            Some(file) => {
                let (keys, files) = (self.spec.build_keys(), self.build_files.clone());
                let reader = SpillManager::clone(&self.build_reader);
                self.split_file(file, keys, &reader, &files, level).await?
            }
            None => (0..NUM_BUCKETS).map(|_| None).collect(),
        };
        let probe = match &pair.probe {
            Some(file) => {
                let (keys, files) = (self.spec.probe_keys(), self.probe_files.clone());
                let reader = SpillManager::clone(&self.probe_reader);
                self.split_file(file, keys, &reader, &files, level).await?
            }
            None => (0..NUM_BUCKETS).map(|_| None).collect(),
        };
        // Dropping the pair deletes its files.
        drop(pair);
        for (build, probe) in build.into_iter().zip(probe).rev() {
            if build.is_none() && probe.is_none() {
                continue;
            }
            // A child that kept most of its parent's build rows holds rows of one key, which
            // splitting cannot separate.
            let rows = build.as_ref().map_or(0, |file| file.rows);
            let splittable = rows.saturating_mul(10) < parent_rows.saturating_mul(9);
            self.pairs.push(Pair {
                build,
                probe,
                level,
                splittable,
            });
        }
        Ok(())
    }

    async fn split_file(
        &mut self,
        file: &SpilledFile,
        keys: Vec<PhysicalExprRef>,
        reader: &SpillManager,
        files: &SpillWriterFactory,
        level: u32,
    ) -> Result<Vec<Option<SpilledFile>>> {
        // Splitting needs its read-ahead whether or not the pool grants it, because nothing else
        // makes progress, so it is accounted without being refused.
        self.inner.grow(file.read_ahead_bytes());
        let mut router = Router::new(keys);
        let mut writers: Vec<Option<SpillFileWriter>> = (0..NUM_BUCKETS).map(|_| None).collect();
        let mut input = file.read(reader)?;
        while let Some(batch) = input.next().await.transpose()? {
            let rows = rows_by_bucket(&router.route(&batch, level, false)?);
            for (bucket, rows) in rows.into_iter().enumerate() {
                if rows.is_empty() {
                    continue;
                }
                let part = take_rows(&batch, rows)?;
                let writer = match writers[bucket].take() {
                    Some(writer) => writer,
                    None => files.create()?,
                };
                writers[bucket].insert(writer).append(part)?;
            }
        }
        self.inner.free();
        writers
            .into_iter()
            .map(|writer| writer.map(SpillFileWriter::finish).transpose())
            .collect()
    }

    /// Joins a spilled pair that is too big and cannot be split, in chunks.
    async fn chunk_pair(&mut self, pair: Pair) -> Result<()> {
        let Some(build) = pair.build else {
            return internal_err!("a pair without build rows always fits in memory");
        };
        self.chunked_buckets.add(1);
        let chunked = ChunkedPair::try_new(
            &build,
            pair.probe,
            self.spec.template.join_type,
            &self.spec.template,
            &self.build_reader,
        )?;
        // The record of matched probe rows is needed whether or not the pool grants it.
        self.bitmap.grow(chunked.bitmap_bytes());
        self.next_chunk(Box::new(chunked)).await
    }

    /// Starts the join of the next chunk, or the final pass once every chunk has run.
    async fn next_chunk(&mut self, mut chunked: Box<ChunkedPair>) -> Result<()> {
        let mut batches = Vec::new();
        while let Some(batch) = chunked.next_build_batch().await? {
            let size = get_record_batch_memory_size(&batch);
            let bound = self.estimator.chunk_bound(&batch, size);
            if batches.is_empty() {
                // A chunk always takes one batch, or the join could not make progress.
                self.held.grow(size);
                self.inner.grow(bound);
            } else if !self.reserve_held(size)? {
                chunked.put_back(batch);
                break;
            } else if !try_reserve(&self.inner, bound)? {
                self.held.shrink(size);
                chunked.put_back(batch);
                break;
            }
            batches.push(batch);
        }
        if batches.is_empty() {
            self.held.free();
            self.inner.free();
            match chunked.final_pass(&self.probe_reader)? {
                Some(stream) => self.phase = Phase::Running(stream, After::FinalPass),
                None => {
                    self.bitmap.free();
                    self.phase = Phase::Next;
                }
            }
            return Ok(());
        }
        // The chunk's keys are known now, so reserve exactly what its join needs.
        let memory = self.estimator.for_batches(&batches, self.held.size())?;
        self.inner
            .resize(memory.reserved + memory.unreserved + chunked.read_ahead_bytes());
        self.concat.resize(memory.transient);
        let budget = self.held.size() + memory.reserved;
        let join = chunked.chunk_join(&self.spec.template, batches, &self.probe_reader)?;
        self.start_inner(join, budget, After::Chunk(chunked))
    }

    /// Continues after a stream has emitted everything.
    async fn finish_stream(&mut self, after: After) -> Result<()> {
        self.held.free();
        self.inner.free();
        self.concat.free();
        match after {
            After::Join => {}
            After::Resident(router, build_files) => {
                let probe_files = self.drain_probe(router).await?;
                for (build, probe) in build_files.into_iter().zip(probe_files).rev() {
                    if build.is_some() || probe.is_some() {
                        self.pairs.push(Pair {
                            build,
                            probe,
                            level: 0,
                            splittable: true,
                        });
                    }
                }
            }
            After::Chunk(chunked) => return self.next_chunk(chunked).await,
            After::FinalPass => {
                self.bitmap.free();
            }
        }
        self.phase = Phase::Next;
        Ok(())
    }

    /// Writes the probe rows of spilled buckets that the resident join did not read, which
    /// happens when it stops early because it can emit nothing more, and returns the probe
    /// files.
    async fn drain_probe(
        &mut self,
        router: Arc<Mutex<ProbeRouter>>,
    ) -> Result<Vec<Option<SpilledFile>>> {
        let input = router.lock().take_input();
        if let Some(mut input) = input {
            while let Some(batch) = input.next().await.transpose()? {
                router.lock().route(&batch, false)?;
            }
        }
        let files = router.lock().finish()?;
        Ok(files)
    }

    fn inner_join(
        &self,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<HashJoinExec> {
        self.spec
            .template
            .builder()
            .reset_state()
            .with_new_children(vec![
                StreamSourceExec::new_arc(build),
                StreamSourceExec::new_arc(probe),
            ])?
            .build()
    }

    fn start_inner(&mut self, join: HashJoinExec, budget: usize, after: After) -> Result<()> {
        self.budget.set_budget(budget);
        let output = join.execute(self.partition, Arc::clone(&self.inner_context));
        // `HashJoinExec` registers its metrics in `execute`. Keep the live counters of what adds up
        // across the inner joins by name: time and the hit and fanout ratios. Rows, batches and
        // memory are counted for the operator as a whole.
        for metric in join.metrics().unwrap_or_default().iter() {
            if !OWN_METRICS.contains(&metric.value().name()) {
                self.plan_metrics.register(Arc::clone(metric));
            }
        }
        self.build_mem_used.set_max(self.reserved());
        self.phase = Phase::Running(output?, after);
        Ok(())
    }

    /// Everything the operator has reserved.
    fn reserved(&self) -> usize {
        self.held.size()
            + self.inner.size()
            + self.concat.size()
            + self.headroom.size()
            + self.bitmap.size()
            + self.pending.size()
            + self.filter_memory.size()
    }

    /// Reserves build batches, within `max_build_buffer_bytes` when it is set.
    fn reserve_held(&self, bytes: usize) -> Result<bool> {
        if let Some(max) = self.spec.config.max_build_buffer_bytes {
            if self.held.size().saturating_add(bytes) > max {
                return Ok(false);
            }
        }
        try_reserve(&self.held, bytes)
    }

    /// Reserves what an inner join needs besides its build batches, plus `read_ahead`.
    fn reserve_inner(&self, memory: BuildMemory, read_ahead: usize) -> Result<bool> {
        if !try_reserve(
            &self.inner,
            memory.reserved + memory.unreserved + read_ahead,
        )? {
            return Ok(false);
        }
        if !try_reserve(&self.concat, memory.transient)? {
            self.inner.free();
            return Ok(false);
        }
        Ok(true)
    }

    /// Keeps the headroom at three times `batch`'s size: one copy while its rows are routed, and,
    /// when a spill file writer writes the parts it holds, one while they are concatenated and
    /// one while the result is encoded.
    fn reserve_headroom(&self, batch: &RecordBatch) -> Result<bool> {
        let wanted = get_record_batch_memory_size(batch).saturating_mul(3);
        if self.headroom.size() >= wanted {
            return Ok(true);
        }
        try_reserve(&self.headroom, wanted - self.headroom.size())
    }
}

/// The metrics of an inner `HashJoinExec` that the operator reports for itself instead, because
/// the inner joins' values do not add up to the operator's.
const OWN_METRICS: [&str; 10] = [
    "build_input_rows",
    "build_input_batches",
    "input_rows",
    "input_batches",
    "output_rows",
    "output_batches",
    "output_bytes",
    "build_mem_used",
    "start_timestamp",
    "end_timestamp",
];

/// Whether the join type emits probe rows that match no build row.
fn emits_probe_rows_without_build(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Right | JoinType::Full | JoinType::RightAnti | JoinType::RightMark
    )
}

/// Whether the join type emits build rows that match no probe row.
fn emits_build_rows_without_probe(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::Full | JoinType::LeftAnti | JoinType::LeftMark
    )
}
