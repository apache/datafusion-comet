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

//! Grace Hash Join operator for Apache DataFusion Comet.
//!
//! Partitions both build and probe sides into N buckets by hashing join keys,
//! then performs per-partition hash joins. Spills partitions to disk (Arrow IPC)
//! when memory is tight.
//!
//! Supports all join types. Recursively repartitions oversized partitions
//! up to `MAX_RECURSION_DEPTH` levels.

use std::any::Any;
use std::fmt;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::UInt32Array;
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result as DFResult};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::TaskContext;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use futures::Stream;
use log::info;

/// Type alias for join key expression pairs.
type JoinOnRef<'a> = &'a [(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)];

/// Number of partitions (buckets) for the grace hash join.
const DEFAULT_NUM_PARTITIONS: usize = 16;

/// Maximum recursion depth for repartitioning oversized partitions.
/// At depth 3 with 16 partitions per level, effective partitions = 16^3 = 4096.
const MAX_RECURSION_DEPTH: usize = 3;

/// Random state for hashing join keys into partitions. Uses fixed seeds
/// different from DataFusion's HashJoinExec to avoid correlation.
/// The `recursion_level` is XORed into the seed so that recursive
/// repartitioning uses different hash functions at each level.
fn partition_random_state(recursion_level: usize) -> RandomState {
    RandomState::with_seeds(
        0x517cc1b727220a95 ^ (recursion_level as u64),
        0x3a8b7c9d1e2f4056,
        0,
        0,
    )
}

// ---------------------------------------------------------------------------
// SpillWriter: incremental append to Arrow IPC spill files
// ---------------------------------------------------------------------------

/// Wraps an Arrow IPC `StreamWriter` for incremental spill writes.
/// Avoids the O(n²) read-rewrite pattern by keeping the writer open.
struct SpillWriter {
    writer: StreamWriter<BufWriter<File>>,
    temp_file: RefCountedTempFile,
    bytes_written: usize,
}

impl SpillWriter {
    /// Create a new spill writer backed by a temp file.
    fn new(temp_file: RefCountedTempFile, schema: &SchemaRef) -> DFResult<Self> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(temp_file.path())
            .map_err(|e| DataFusionError::Execution(format!("Failed to open spill file: {e}")))?;
        let buf_writer = BufWriter::new(file);
        let writer = StreamWriter::try_new(buf_writer, schema)?;
        Ok(Self {
            writer,
            temp_file,
            bytes_written: 0,
        })
    }

    /// Append a single batch to the spill file.
    fn write_batch(&mut self, batch: &RecordBatch) -> DFResult<()> {
        if batch.num_rows() > 0 {
            self.bytes_written += batch.get_array_memory_size();
            self.writer.write(batch)?;
        }
        Ok(())
    }

    /// Append multiple batches to the spill file.
    fn write_batches(&mut self, batches: &[RecordBatch]) -> DFResult<()> {
        for batch in batches {
            self.write_batch(batch)?;
        }
        Ok(())
    }

    /// Finish writing. Must be called before reading back.
    fn finish(mut self) -> DFResult<(RefCountedTempFile, usize)> {
        self.writer.finish()?;
        Ok((self.temp_file, self.bytes_written))
    }
}

// ---------------------------------------------------------------------------
// GraceHashJoinMetrics
// ---------------------------------------------------------------------------

/// Production metrics for the Grace Hash Join operator.
struct GraceHashJoinMetrics {
    /// Baseline metrics (output rows, elapsed compute)
    baseline: BaselineMetrics,
    /// Time spent partitioning the build side
    build_time: Time,
    /// Time spent partitioning the probe side
    probe_time: Time,
    /// Time spent performing per-partition hash joins
    join_time: Time,
    /// Number of spill events
    spill_count: Count,
    /// Total bytes spilled to disk
    spilled_bytes: Count,
    /// Number of build-side input rows
    build_input_rows: Count,
    /// Number of build-side input batches
    build_input_batches: Count,
    /// Number of probe-side input rows
    input_rows: Count,
    /// Number of probe-side input batches
    input_batches: Count,
}

impl GraceHashJoinMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            build_time: MetricBuilder::new(metrics).subset_time("build_time", partition),
            probe_time: MetricBuilder::new(metrics).subset_time("probe_time", partition),
            join_time: MetricBuilder::new(metrics).subset_time("join_time", partition),
            spill_count: MetricBuilder::new(metrics).spill_count(partition),
            spilled_bytes: MetricBuilder::new(metrics).spilled_bytes(partition),
            build_input_rows: MetricBuilder::new(metrics).counter("build_input_rows", partition),
            build_input_batches: MetricBuilder::new(metrics)
                .counter("build_input_batches", partition),
            input_rows: MetricBuilder::new(metrics).counter("input_rows", partition),
            input_batches: MetricBuilder::new(metrics).counter("input_batches", partition),
        }
    }
}

// ---------------------------------------------------------------------------
// GraceHashJoinExec
// ---------------------------------------------------------------------------

/// Grace Hash Join execution plan.
///
/// Partitions both sides into N buckets, then joins each bucket independently
/// using DataFusion's HashJoinExec. Spills partitions to disk when memory
/// pressure is detected.
#[derive(Debug)]
pub struct GraceHashJoinExec {
    /// Left input
    left: Arc<dyn ExecutionPlan>,
    /// Right input
    right: Arc<dyn ExecutionPlan>,
    /// Join key pairs: (left_key, right_key)
    on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    /// Optional join filter applied after key matching
    filter: Option<JoinFilter>,
    /// Join type
    join_type: JoinType,
    /// Number of hash partitions
    num_partitions: usize,
    /// Whether left is the build side (true) or right is (false)
    build_left: bool,
    /// Output schema
    schema: SchemaRef,
    /// Plan properties cache
    cache: PlanProperties,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
}

impl GraceHashJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        num_partitions: usize,
        build_left: bool,
    ) -> DFResult<Self> {
        // Build the output schema using HashJoinExec's logic.
        // HashJoinExec expects left=build, right=probe. When build_left=false,
        // we swap inputs + keys + join type for schema derivation, then store
        // original values for our own partitioning logic.
        let hash_join = HashJoinExec::try_new(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter.clone(),
            join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )?;
        let (schema, cache) = if build_left {
            (hash_join.schema(), hash_join.properties().clone())
        } else {
            // Swap to get correct output schema for build-right
            let swapped = hash_join.swap_inputs(PartitionMode::CollectLeft)?;
            (swapped.schema(), swapped.properties().clone())
        };

        Ok(Self {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            num_partitions: if num_partitions == 0 {
                DEFAULT_NUM_PARTITIONS
            } else {
                num_partitions
            },
            build_left,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for GraceHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let on: Vec<String> = self
                    .on
                    .iter()
                    .map(|(l, r)| format!("({l}, {r})"))
                    .collect();
                write!(
                    f,
                    "GraceHashJoinExec: join_type={:?}, on=[{}], num_partitions={}",
                    self.join_type,
                    on.join(", "),
                    self.num_partitions,
                )
            }
        }
    }
}

impl ExecutionPlan for GraceHashJoinExec {
    fn name(&self) -> &str {
        "GraceHashJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GraceHashJoinExec::try_new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            self.num_partitions,
            self.build_left,
        )?))
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let left_stream = self.left.execute(partition, Arc::clone(&context))?;
        let right_stream = self.right.execute(partition, Arc::clone(&context))?;

        let join_metrics = GraceHashJoinMetrics::new(&self.metrics, partition);

        // Determine build/probe streams and schemas based on build_left.
        // The internal execution always treats first arg as build, second as probe.
        let (build_stream, probe_stream, build_schema, probe_schema, build_on, probe_on) =
            if self.build_left {
                let build_keys: Vec<_> =
                    self.on.iter().map(|(l, _)| Arc::clone(l)).collect();
                let probe_keys: Vec<_> =
                    self.on.iter().map(|(_, r)| Arc::clone(r)).collect();
                (
                    left_stream,
                    right_stream,
                    self.left.schema(),
                    self.right.schema(),
                    build_keys,
                    probe_keys,
                )
            } else {
                // Build right: right is build side, left is probe side
                let build_keys: Vec<_> =
                    self.on.iter().map(|(_, r)| Arc::clone(r)).collect();
                let probe_keys: Vec<_> =
                    self.on.iter().map(|(l, _)| Arc::clone(l)).collect();
                (
                    right_stream,
                    left_stream,
                    self.right.schema(),
                    self.left.schema(),
                    build_keys,
                    probe_keys,
                )
            };

        let on = self.on.clone();
        let filter = self.filter.clone();
        let join_type = self.join_type;
        let num_partitions = self.num_partitions;
        let build_left = self.build_left;
        let output_schema = Arc::clone(&self.schema);

        let result_stream = futures::stream::once(async move {
            execute_grace_hash_join(
                build_stream,
                probe_stream,
                build_on,
                probe_on,
                on,
                filter,
                join_type,
                num_partitions,
                build_left,
                build_schema,
                probe_schema,
                output_schema,
                context,
                join_metrics,
            )
            .await
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            result_stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Per-partition state
// ---------------------------------------------------------------------------

/// Per-partition state tracking buffered data or spill writers.
struct HashPartition {
    /// In-memory build-side batches for this partition.
    build_batches: Vec<RecordBatch>,
    /// In-memory probe-side batches for this partition.
    probe_batches: Vec<RecordBatch>,
    /// Incremental spill writer for build side (if spilling).
    build_spill_writer: Option<SpillWriter>,
    /// Incremental spill writer for probe side (if spilling).
    probe_spill_writer: Option<SpillWriter>,
    /// Approximate memory used by build-side batches in this partition.
    build_mem_size: usize,
    /// Approximate memory used by probe-side batches in this partition.
    probe_mem_size: usize,
}

impl HashPartition {
    fn new() -> Self {
        Self {
            build_batches: Vec::new(),
            probe_batches: Vec::new(),
            build_spill_writer: None,
            probe_spill_writer: None,
            build_mem_size: 0,
            probe_mem_size: 0,
        }
    }

    /// Whether the build side has been spilled to disk.
    fn build_spilled(&self) -> bool {
        self.build_spill_writer.is_some()
    }
}

// ---------------------------------------------------------------------------
// Main execution logic
// ---------------------------------------------------------------------------

/// Main execution logic for the grace hash join.
///
/// `build_stream`/`probe_stream`: already swapped based on build_left.
/// `build_keys`/`probe_keys`: key expressions for their respective sides.
/// `original_on`: original (left_key, right_key) pairs for HashJoinExec.
/// `build_left`: whether left is build side (affects HashJoinExec construction).
#[allow(clippy::too_many_arguments)]
async fn execute_grace_hash_join(
    build_stream: SendableRecordBatchStream,
    probe_stream: SendableRecordBatchStream,
    build_keys: Vec<Arc<dyn PhysicalExpr>>,
    probe_keys: Vec<Arc<dyn PhysicalExpr>>,
    original_on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    num_partitions: usize,
    build_left: bool,
    build_schema: SchemaRef,
    probe_schema: SchemaRef,
    _output_schema: SchemaRef,
    context: Arc<TaskContext>,
    metrics: GraceHashJoinMetrics,
) -> DFResult<impl Stream<Item = DFResult<RecordBatch>>> {
    // Set up memory reservation
    let reservation = MemoryConsumer::new("GraceHashJoinExec")
        .with_can_spill(true)
        .register(&context.runtime_env().memory_pool);

    let mut partitions: Vec<HashPartition> =
        (0..num_partitions).map(|_| HashPartition::new()).collect();

    // Phase 1: Partition the build side
    {
        let _timer = metrics.build_time.timer();
        partition_build_side(
            build_stream,
            &build_keys,
            num_partitions,
            &build_schema,
            &mut partitions,
            &mut MutableReservation(reservation),
            &context,
            &metrics,
        )
        .await?;
    }

    // Phase 2: Partition the probe side
    {
        let _timer = metrics.probe_time.timer();
        partition_probe_side(
            probe_stream,
            &probe_keys,
            num_partitions,
            &probe_schema,
            &mut partitions,
            &context,
            &metrics,
        )
        .await?;
    }

    // Finish all open spill writers before reading back
    let finished_partitions =
        finish_spill_writers(partitions, &build_schema, &probe_schema, &metrics)?;

    // Phase 3: Join each partition
    let partition_results = {
        let _timer = metrics.join_time.timer();
        join_all_partitions(
            finished_partitions,
            &original_on,
            &filter,
            &join_type,
            build_left,
            &build_schema,
            &probe_schema,
            Arc::clone(&context),
        )
        .await?
    };

    // Flatten all partition results into a single stream
    let output_metrics = metrics.baseline.clone();
    let result_stream =
        stream::iter(partition_results.into_iter().map(Ok::<_, DataFusionError>))
            .try_flatten()
            .inspect_ok(move |batch| {
                output_metrics.record_output(batch.num_rows());
            });

    Ok(result_stream)
}

/// Wraps MemoryReservation to allow mutation through reference.
struct MutableReservation(MemoryReservation);

impl MutableReservation {
    fn try_grow(&mut self, additional: usize) -> DFResult<()> {
        self.0.try_grow(additional)
    }

    fn shrink(&mut self, amount: usize) {
        self.0.shrink(amount);
    }
}

// ---------------------------------------------------------------------------
// Hash partitioning
// ---------------------------------------------------------------------------

/// Compute hash partition indices for a batch given join key expressions.
fn compute_partition_indices(
    batch: &RecordBatch,
    keys: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    recursion_level: usize,
) -> DFResult<Vec<Vec<u32>>> {
    // Evaluate key columns
    let key_columns: Vec<_> = keys
        .iter()
        .map(|expr| {
            expr.evaluate(batch)
                .and_then(|cv| cv.into_array(batch.num_rows()))
        })
        .collect::<DFResult<Vec<_>>>()?;

    // Hash the key columns with our partition random state
    let random_state = partition_random_state(recursion_level);
    let mut hashes = vec![0u64; batch.num_rows()];
    create_hashes(&key_columns, &random_state, &mut hashes)?;

    // Assign rows to partitions
    let mut indices: Vec<Vec<u32>> = (0..num_partitions).map(|_| Vec::new()).collect();
    for (row_idx, hash) in hashes.iter().enumerate() {
        let partition = (*hash as usize) % num_partitions;
        indices[partition].push(row_idx as u32);
    }

    Ok(indices)
}

/// Split a batch into N sub-batches by partition assignment.
fn partition_batch(
    batch: &RecordBatch,
    indices: &[Vec<u32>],
) -> DFResult<Vec<Option<RecordBatch>>> {
    indices
        .iter()
        .map(|idx| {
            if idx.is_empty() {
                Ok(None)
            } else {
                let idx_array = UInt32Array::from(idx.clone());
                let columns: Vec<_> = batch
                    .columns()
                    .iter()
                    .map(|col| take(col.as_ref(), &idx_array, None))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Some(RecordBatch::try_new(batch.schema(), columns)?))
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Spill reading
// ---------------------------------------------------------------------------

/// Read record batches from a finished spill file.
fn read_spilled_batches(
    spill_file: &RefCountedTempFile,
    _schema: &SchemaRef,
) -> DFResult<Vec<RecordBatch>> {
    let file = File::open(spill_file.path())
        .map_err(|e| DataFusionError::Execution(format!("Failed to open spill file: {e}")))?;
    let reader = BufReader::new(file);
    let stream_reader = StreamReader::try_new(reader, None)?;
    let batches: Vec<RecordBatch> = stream_reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    Ok(batches)
}

// ---------------------------------------------------------------------------
// Phase 1: Build-side partitioning
// ---------------------------------------------------------------------------

/// Phase 1: Read all build-side batches, hash-partition into N buckets.
/// Spills the largest partition when memory pressure is detected.
#[allow(clippy::too_many_arguments)]
async fn partition_build_side(
    mut input: SendableRecordBatchStream,
    keys: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    schema: &SchemaRef,
    partitions: &mut [HashPartition],
    reservation: &mut MutableReservation,
    context: &Arc<TaskContext>,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<()> {
    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }

        metrics.build_input_batches.add(1);
        metrics.build_input_rows.add(batch.num_rows());

        let indices = compute_partition_indices(&batch, keys, num_partitions, 0)?;
        let sub_batches = partition_batch(&batch, &indices)?;

        for (part_idx, sub_batch) in sub_batches.into_iter().enumerate() {
            if let Some(sub_batch) = sub_batch {
                let batch_size = sub_batch.get_array_memory_size();

                if partitions[part_idx].build_spilled() {
                    // This partition is already spilled; append incrementally
                    if let Some(ref mut writer) = partitions[part_idx].build_spill_writer {
                        writer.write_batch(&sub_batch)?;
                    }
                } else {
                    // Try to reserve memory
                    if reservation.try_grow(batch_size).is_err() {
                        // Memory pressure: spill the largest in-memory partition
                        info!(
                            "GraceHashJoin: memory pressure during build, spilling largest partition"
                        );
                        spill_largest_partition(partitions, schema, context, reservation, metrics)?;

                        // Retry reservation after spilling
                        if reservation.try_grow(batch_size).is_err() {
                            // Still can't fit; spill this partition too
                            info!(
                                "GraceHashJoin: still under pressure, spilling partition {}",
                                part_idx
                            );
                            spill_partition_build(
                                &mut partitions[part_idx],
                                schema,
                                context,
                                reservation,
                                metrics,
                            )?;
                            if let Some(ref mut writer) = partitions[part_idx].build_spill_writer {
                                writer.write_batch(&sub_batch)?;
                            }
                            continue;
                        }
                    }

                    partitions[part_idx].build_mem_size += batch_size;
                    partitions[part_idx].build_batches.push(sub_batch);
                }
            }
        }
    }

    Ok(())
}

/// Spill the largest in-memory build partition to disk.
fn spill_largest_partition(
    partitions: &mut [HashPartition],
    schema: &SchemaRef,
    context: &Arc<TaskContext>,
    reservation: &mut MutableReservation,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<()> {
    // Find the largest non-spilled partition
    let largest_idx = partitions
        .iter()
        .enumerate()
        .filter(|(_, p)| !p.build_spilled() && !p.build_batches.is_empty())
        .max_by_key(|(_, p)| p.build_mem_size)
        .map(|(idx, _)| idx);

    if let Some(idx) = largest_idx {
        info!(
            "GraceHashJoin: spilling partition {} ({} bytes, {} batches)",
            idx, partitions[idx].build_mem_size, partitions[idx].build_batches.len()
        );
        spill_partition_build(&mut partitions[idx], schema, context, reservation, metrics)?;
    }

    Ok(())
}

/// Spill a single partition's build-side data to disk using SpillWriter.
fn spill_partition_build(
    partition: &mut HashPartition,
    schema: &SchemaRef,
    context: &Arc<TaskContext>,
    reservation: &mut MutableReservation,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<()> {
    let temp_file = context
        .runtime_env()
        .disk_manager
        .create_tmp_file("grace hash join build")?;

    let mut writer = SpillWriter::new(temp_file, schema)?;
    writer.write_batches(&partition.build_batches)?;

    // Free memory
    let freed = partition.build_mem_size;
    reservation.shrink(freed);

    metrics.spill_count.add(1);
    metrics.spilled_bytes.add(freed);

    partition.build_spill_writer = Some(writer);
    partition.build_batches.clear();
    partition.build_mem_size = 0;

    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 2: Probe-side partitioning
// ---------------------------------------------------------------------------

/// Phase 2: Read all probe-side batches, route to in-memory buffers or spill files.
#[allow(clippy::too_many_arguments)]
async fn partition_probe_side(
    mut input: SendableRecordBatchStream,
    keys: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    schema: &SchemaRef,
    partitions: &mut [HashPartition],
    context: &Arc<TaskContext>,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<()> {
    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }

        metrics.input_batches.add(1);
        metrics.input_rows.add(batch.num_rows());

        let indices = compute_partition_indices(&batch, keys, num_partitions, 0)?;
        let sub_batches = partition_batch(&batch, &indices)?;

        for (part_idx, sub_batch) in sub_batches.into_iter().enumerate() {
            if let Some(sub_batch) = sub_batch {
                if partitions[part_idx].build_spilled() {
                    // Build side was spilled, so spill probe side too
                    if partitions[part_idx].probe_spill_writer.is_none() {
                        let temp_file = context
                            .runtime_env()
                            .disk_manager
                            .create_tmp_file("grace hash join probe")?;
                        let mut writer = SpillWriter::new(temp_file, schema)?;
                        // Write any accumulated in-memory probe batches first
                        if !partitions[part_idx].probe_batches.is_empty() {
                            let batches = std::mem::take(&mut partitions[part_idx].probe_batches);
                            writer.write_batches(&batches)?;
                            partitions[part_idx].probe_mem_size = 0;
                        }
                        partitions[part_idx].probe_spill_writer = Some(writer);
                    }
                    if let Some(ref mut writer) = partitions[part_idx].probe_spill_writer {
                        writer.write_batch(&sub_batch)?;
                    }
                } else {
                    partitions[part_idx].probe_mem_size +=
                        sub_batch.get_array_memory_size();
                    partitions[part_idx].probe_batches.push(sub_batch);
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Finish spill writers
// ---------------------------------------------------------------------------

/// State of a finished partition ready for joining.
struct FinishedPartition {
    build_batches: Vec<RecordBatch>,
    probe_batches: Vec<RecordBatch>,
    build_spill_file: Option<RefCountedTempFile>,
    probe_spill_file: Option<RefCountedTempFile>,
}

/// Finish all open spill writers so files can be read back.
fn finish_spill_writers(
    partitions: Vec<HashPartition>,
    _left_schema: &SchemaRef,
    _right_schema: &SchemaRef,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<Vec<FinishedPartition>> {
    let mut finished = Vec::with_capacity(partitions.len());

    for partition in partitions {
        let build_spill_file = if let Some(writer) = partition.build_spill_writer {
            let (file, bytes) = writer.finish()?;
            metrics.spilled_bytes.add(0); // bytes already tracked at spill time
            let _ = bytes; // suppress unused warning
            Some(file)
        } else {
            None
        };

        let probe_spill_file = if let Some(writer) = partition.probe_spill_writer {
            let (file, _bytes) = writer.finish()?;
            Some(file)
        } else {
            None
        };

        finished.push(FinishedPartition {
            build_batches: partition.build_batches,
            probe_batches: partition.probe_batches,
            build_spill_file,
            probe_spill_file,
        });
    }

    Ok(finished)
}

// ---------------------------------------------------------------------------
// Phase 3: Per-partition hash joins
// ---------------------------------------------------------------------------

/// Phase 3: For each partition, create a per-partition HashJoinExec and collect results.
/// Recursively repartitions oversized partitions up to `MAX_RECURSION_DEPTH`.
#[allow(clippy::too_many_arguments)]
async fn join_all_partitions(
    partitions: Vec<FinishedPartition>,
    original_on: JoinOnRef<'_>,
    filter: &Option<JoinFilter>,
    join_type: &JoinType,
    build_left: bool,
    build_schema: &SchemaRef,
    probe_schema: &SchemaRef,
    context: Arc<TaskContext>,
) -> DFResult<Vec<SendableRecordBatchStream>> {
    let mut streams = Vec::new();

    for partition in partitions {
        // Get build-side batches (from memory or disk)
        let build_batches = if let Some(ref spill_file) = partition.build_spill_file {
            read_spilled_batches(spill_file, build_schema)?
        } else {
            partition.build_batches
        };

        // Get probe-side batches (from memory or disk)
        let probe_batches = if let Some(ref spill_file) = partition.probe_spill_file {
            read_spilled_batches(spill_file, probe_schema)?
        } else {
            partition.probe_batches
        };

        join_partition_recursive(
            build_batches,
            probe_batches,
            original_on,
            filter,
            join_type,
            build_left,
            build_schema,
            probe_schema,
            &context,
            1, // recursion starts at level 1 (level 0 was initial partitioning)
            &mut streams,
        )?;
    }

    Ok(streams)
}

/// Join a single partition, recursively repartitioning if the build side is too large.
///
/// `build_keys` / `probe_keys` for repartitioning are extracted from `original_on`
/// based on `build_left`.
#[allow(clippy::too_many_arguments)]
fn join_partition_recursive(
    build_batches: Vec<RecordBatch>,
    probe_batches: Vec<RecordBatch>,
    original_on: JoinOnRef<'_>,
    filter: &Option<JoinFilter>,
    join_type: &JoinType,
    build_left: bool,
    build_schema: &SchemaRef,
    probe_schema: &SchemaRef,
    context: &Arc<TaskContext>,
    recursion_level: usize,
    streams: &mut Vec<SendableRecordBatchStream>,
) -> DFResult<()> {
    // Skip partitions that cannot produce output based on join type.
    // The join type uses Spark's left/right semantics. Map build/probe
    // back to left/right based on build_left.
    let (left_empty, right_empty) = if build_left {
        (build_batches.is_empty(), probe_batches.is_empty())
    } else {
        (probe_batches.is_empty(), build_batches.is_empty())
    };
    let skip = match join_type {
        JoinType::Inner | JoinType::LeftSemi | JoinType::LeftAnti => {
            left_empty || right_empty
        }
        JoinType::Left | JoinType::LeftMark => left_empty,
        JoinType::Right => right_empty,
        JoinType::Full => left_empty && right_empty,
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            left_empty || right_empty
        }
    };
    if skip {
        return Ok(());
    }

    // Check if build side is too large and needs recursive repartitioning.
    // Try to reserve memory for the build side — if it fails, the partition
    // is too large to fit and needs to be sub-partitioned.
    let build_size: usize = build_batches
        .iter()
        .map(|b| b.get_array_memory_size())
        .sum();
    let needs_repartition = if build_size > 0 && build_batches.len() > 1 {
        let mut test_reservation = MemoryConsumer::new("GraceHashJoinExec repartition check")
            .register(&context.runtime_env().memory_pool);
        let can_fit = test_reservation.try_grow(build_size).is_ok();
        if can_fit {
            test_reservation.shrink(build_size);
        }
        !can_fit
    } else {
        false
    };

    if needs_repartition {
        if recursion_level >= MAX_RECURSION_DEPTH {
            let total_build_rows: usize =
                build_batches.iter().map(|b| b.num_rows()).sum();
            return Err(DataFusionError::ResourcesExhausted(format!(
                "GraceHashJoin: build side partition is still too large after {} levels of \
                 repartitioning ({} bytes, {} rows). Consider increasing \
                 spark.comet.exec.graceHashJoin.numPartitions or \
                 spark.executor.memory.",
                MAX_RECURSION_DEPTH, build_size, total_build_rows
            )));
        }

        info!(
            "GraceHashJoin: repartitioning oversized partition at level {} \
             (build: {} bytes, {} batches)",
            recursion_level,
            build_size,
            build_batches.len()
        );

        return repartition_and_join(
            build_batches,
            probe_batches,
            original_on,
            filter,
            join_type,
            build_left,
            build_schema,
            probe_schema,
            context,
            recursion_level,
            streams,
        );
    }

    // For outer joins, one side may be empty — provide an empty batch
    // with the correct schema so MemorySourceConfig has a valid partition.
    let build_data = if build_batches.is_empty() {
        vec![RecordBatch::new_empty(Arc::clone(build_schema))]
    } else {
        build_batches
    };
    let probe_data = if probe_batches.is_empty() {
        vec![RecordBatch::new_empty(Arc::clone(probe_schema))]
    } else {
        probe_batches
    };

    // Create per-partition hash join.
    // HashJoinExec expects left=build (CollectLeft mode).
    let (left_data, left_schema_ref, right_data, right_schema_ref) = if build_left {
        (build_data, build_schema, probe_data, probe_schema)
    } else {
        (probe_data, probe_schema, build_data, build_schema)
    };

    let left_source = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[left_data], Arc::clone(left_schema_ref), None)?,
    )));

    let right_source = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[right_data], Arc::clone(right_schema_ref), None)?,
    )));

    let stream = if build_left {
        let hash_join = HashJoinExec::try_new(
            left_source,
            right_source,
            original_on.to_vec(),
            filter.clone(),
            join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )?;
        hash_join.execute(0, Arc::clone(context))?
    } else {
        let hash_join = Arc::new(HashJoinExec::try_new(
            left_source,
            right_source,
            original_on.to_vec(),
            filter.clone(),
            join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )?);
        let swapped = hash_join.swap_inputs(PartitionMode::CollectLeft)?;
        swapped.execute(0, Arc::clone(context))?
    };

    streams.push(stream);
    Ok(())
}

/// Repartition build and probe batches into sub-partitions using a different
/// hash seed, then recursively join each sub-partition.
#[allow(clippy::too_many_arguments)]
fn repartition_and_join(
    build_batches: Vec<RecordBatch>,
    probe_batches: Vec<RecordBatch>,
    original_on: JoinOnRef<'_>,
    filter: &Option<JoinFilter>,
    join_type: &JoinType,
    build_left: bool,
    build_schema: &SchemaRef,
    probe_schema: &SchemaRef,
    context: &Arc<TaskContext>,
    recursion_level: usize,
    streams: &mut Vec<SendableRecordBatchStream>,
) -> DFResult<()> {
    let num_sub_partitions = DEFAULT_NUM_PARTITIONS;

    // Extract build/probe key expressions from original_on
    let (build_keys, probe_keys): (Vec<_>, Vec<_>) = if build_left {
        original_on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip()
    } else {
        original_on
            .iter()
            .map(|(l, r)| (Arc::clone(r), Arc::clone(l)))
            .unzip()
    };

    // Sub-partition the build side
    let mut build_sub: Vec<Vec<RecordBatch>> =
        (0..num_sub_partitions).map(|_| Vec::new()).collect();
    for batch in &build_batches {
        let indices =
            compute_partition_indices(batch, &build_keys, num_sub_partitions, recursion_level)?;
        let sub_batches = partition_batch(batch, &indices)?;
        for (i, sub) in sub_batches.into_iter().enumerate() {
            if let Some(sub) = sub {
                build_sub[i].push(sub);
            }
        }
    }

    // Sub-partition the probe side
    let mut probe_sub: Vec<Vec<RecordBatch>> =
        (0..num_sub_partitions).map(|_| Vec::new()).collect();
    for batch in &probe_batches {
        let indices =
            compute_partition_indices(batch, &probe_keys, num_sub_partitions, recursion_level)?;
        let sub_batches = partition_batch(batch, &indices)?;
        for (i, sub) in sub_batches.into_iter().enumerate() {
            if let Some(sub) = sub {
                probe_sub[i].push(sub);
            }
        }
    }

    // Recursively join each sub-partition
    for (build_part, probe_part) in build_sub.into_iter().zip(probe_sub.into_iter()) {
        join_partition_recursive(
            build_part,
            probe_part,
            original_on,
            filter,
            join_type,
            build_left,
            build_schema,
            probe_schema,
            context,
            recursion_level + 1,
            streams,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;

    fn make_batch(ids: &[i32], values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_grace_hash_join_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));

        let left_batches = vec![
            make_batch(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]),
            make_batch(&[6, 7, 8], &["f", "g", "h"]),
        ];
        let right_batches = vec![
            make_batch(&[2, 4, 6, 8], &["x", "y", "z", "w"]),
            make_batch(&[1, 3, 5, 7], &["p", "q", "r", "s"]),
        ];

        let left_source = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[left_batches], Arc::clone(&left_schema), None)?,
        )));
        let right_source = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[right_batches], Arc::clone(&right_schema), None)?,
        )));

        let on = vec![(
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
        )];

        let grace_join = GraceHashJoinExec::try_new(
            left_source,
            right_source,
            on,
            None,
            &JoinType::Inner,
            4, // Use 4 partitions for testing
            true,
        )?;

        let stream = grace_join.execute(0, task_ctx)?;
        let result_batches: Vec<RecordBatch> = stream.try_collect().await?;

        // Count total rows - should be 8 (each left id matches exactly one right id)
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8, "Expected 8 matching rows for inner join");

        Ok(())
    }

    #[tokio::test]
    async fn test_grace_hash_join_empty_partition() -> DFResult<()> {
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();

        let left_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));

        let left_batches = vec![RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?];
        let right_batches = vec![RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )?];

        let left_source = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[left_batches], Arc::clone(&left_schema), None)?,
        )));
        let right_source = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[right_batches], Arc::clone(&right_schema), None)?,
        )));

        let on = vec![(
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
        )];

        let grace_join = GraceHashJoinExec::try_new(
            left_source,
            right_source,
            on,
            None,
            &JoinType::Inner,
            4,
            true,
        )?;

        let stream = grace_join.execute(0, task_ctx)?;
        let result_batches: Vec<RecordBatch> = stream.try_collect().await?;

        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Expected 0 rows for non-matching keys");

        Ok(())
    }
}
