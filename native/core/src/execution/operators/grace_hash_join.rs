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
use std::sync::Mutex;

use ahash::RandomState;
use arrow::array::UInt32Array;
use arrow::compute::{concat_batches, take};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::CompressionType;
use arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result as DFResult};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::TaskContext;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::{StreamExt, TryStreamExt};
use futures::Stream;
use log::info;
use tokio::sync::mpsc;

/// Type alias for join key expression pairs.
type JoinOnRef<'a> = &'a [(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)];

/// Number of partitions (buckets) for the grace hash join.
const DEFAULT_NUM_PARTITIONS: usize = 16;

/// Maximum recursion depth for repartitioning oversized partitions.
/// At depth 3 with 16 partitions per level, effective partitions = 16^3 = 4096.
const MAX_RECURSION_DEPTH: usize = 3;

/// I/O buffer size for spill file reads and writes. The default BufReader/BufWriter
/// size (8 KB) is far too small for multi-GB spill files. 1 MB provides good
/// sequential throughput while keeping per-partition memory overhead modest.
const SPILL_IO_BUFFER_SIZE: usize = 1024 * 1024;

/// Target number of rows per coalesced batch when reading spill files.
/// Spill files contain many tiny sub-batches (from partitioning). Coalescing
/// into larger batches reduces per-batch overhead in the hash join kernel
/// and channel send/recv costs.
const SPILL_READ_COALESCE_TARGET: usize = 8192;

/// Target build-side size per merged partition. After Phase 2, adjacent
/// `FinishedPartition`s are merged so each group has roughly this much
/// build data, reducing the number of per-partition HashJoinExec calls.
const TARGET_PARTITION_BUILD_SIZE: usize = 32 * 1024 * 1024;

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
        let buf_writer = BufWriter::with_capacity(SPILL_IO_BUFFER_SIZE, file);
        let write_options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let writer =
            StreamWriter::try_new_with_options(buf_writer, schema, write_options)?;
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
// SpillReaderExec: streaming ExecutionPlan for reading spill files
// ---------------------------------------------------------------------------

/// An ExecutionPlan that streams record batches from an Arrow IPC spill file.
/// Used during the join phase so that spilled probe data is read on-demand
/// instead of loaded entirely into memory.
#[derive(Debug)]
struct SpillReaderExec {
    spill_file: RefCountedTempFile,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl SpillReaderExec {
    fn new(spill_file: RefCountedTempFile, schema: SchemaRef) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            spill_file,
            schema,
            cache,
        }
    }
}

impl DisplayAs for SpillReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpillReaderExec")
    }
}

impl ExecutionPlan for SpillReaderExec {
    fn name(&self) -> &str {
        "SpillReaderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let coalesce_schema = Arc::clone(&self.schema);
        let path = self.spill_file.path().to_path_buf();
        // Move the spill file handle into the blocking closure to keep
        // the temp file alive until the reader is done.
        let spill_file_handle = self.spill_file.clone();

        // Use a channel so file I/O runs on a blocking thread and doesn't
        // block the async executor. This lets select_all interleave multiple
        // partition streams effectively.
        let (tx, rx) = mpsc::channel::<DFResult<RecordBatch>>(4);

        tokio::task::spawn_blocking(move || {
            let _keep_alive = spill_file_handle;
            let file = match File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::Execution(format!(
                        "Failed to open spill file: {e}"
                    ))));
                    return;
                }
            };
            let reader = match StreamReader::try_new(
                BufReader::with_capacity(SPILL_IO_BUFFER_SIZE, file),
                None,
            ) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::ArrowError(Box::new(e), None)));
                    return;
                }
            };

            // Coalesce small sub-batches into larger ones to reduce per-batch
            // overhead in the downstream hash join.
            let mut pending: Vec<RecordBatch> = Vec::new();
            let mut pending_rows = 0usize;

            for batch_result in reader {
                let batch = match batch_result {
                    Ok(b) => b,
                    Err(e) => {
                        let _ =
                            tx.blocking_send(Err(DataFusionError::ArrowError(Box::new(e), None)));
                        return;
                    }
                };
                if batch.num_rows() == 0 {
                    continue;
                }
                pending_rows += batch.num_rows();
                pending.push(batch);

                if pending_rows >= SPILL_READ_COALESCE_TARGET {
                    let merged = if pending.len() == 1 {
                        Ok(pending.pop().unwrap())
                    } else {
                        concat_batches(&coalesce_schema, &pending)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                    };
                    pending.clear();
                    pending_rows = 0;
                    if tx.blocking_send(merged).is_err() {
                        return;
                    }
                }
            }

            // Flush remaining
            if !pending.is_empty() {
                let merged = if pending.len() == 1 {
                    Ok(pending.pop().unwrap())
                } else {
                    concat_batches(&coalesce_schema, &pending)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                };
                let _ = tx.blocking_send(merged);
            }
        });

        let batch_stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|batch| (batch, rx))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            batch_stream,
        )))
    }
}


// ---------------------------------------------------------------------------
// StreamSourceExec: wrap an existing stream as an ExecutionPlan
// ---------------------------------------------------------------------------

/// An ExecutionPlan that yields batches from a pre-existing stream.
/// Used in the fast path to feed the probe side's live stream into
/// a `HashJoinExec` without buffering or spilling.
struct StreamSourceExec {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl StreamSourceExec {
    fn new(stream: SendableRecordBatchStream, schema: SchemaRef) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            stream: Mutex::new(Some(stream)),
            schema,
            cache,
        }
    }
}

impl fmt::Debug for StreamSourceExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StreamSourceExec").finish()
    }
}

impl DisplayAs for StreamSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamSourceExec")
    }
}

impl ExecutionPlan for StreamSourceExec {
    fn name(&self) -> &str {
        "StreamSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.stream
            .lock()
            .map_err(|e| DataFusionError::Internal(format!("lock poisoned: {e}")))?
            .take()
            .ok_or_else(|| {
                DataFusionError::Internal("StreamSourceExec: stream already consumed".to_string())
            })
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
    /// Maximum build-side bytes for the fast path (0 = disabled)
    fast_path_threshold: usize,
    /// Output schema
    schema: SchemaRef,
    /// Plan properties cache
    cache: PlanProperties,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
}

impl GraceHashJoinExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        num_partitions: usize,
        build_left: bool,
        fast_path_threshold: usize,
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
            fast_path_threshold,
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
                let on: Vec<String> = self.on.iter().map(|(l, r)| format!("({l}, {r})")).collect();
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
            self.fast_path_threshold,
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
        info!(
            "GraceHashJoin: execute() called. build_left={}, join_type={:?}, \
             num_partitions={}, fast_path_threshold={}\n  left: {}\n  right: {}",
            self.build_left, self.join_type, self.num_partitions, self.fast_path_threshold,
            DisplayableExecutionPlan::new(self.left.as_ref()).one_line(),
            DisplayableExecutionPlan::new(self.right.as_ref()).one_line(),
        );
        let left_stream = self.left.execute(partition, Arc::clone(&context))?;
        let right_stream = self.right.execute(partition, Arc::clone(&context))?;

        let join_metrics = GraceHashJoinMetrics::new(&self.metrics, partition);

        // Determine build/probe streams and schemas based on build_left.
        // The internal execution always treats first arg as build, second as probe.
        let (build_stream, probe_stream, build_schema, probe_schema, build_on, probe_on) =
            if self.build_left {
                let build_keys: Vec<_> = self.on.iter().map(|(l, _)| Arc::clone(l)).collect();
                let probe_keys: Vec<_> = self.on.iter().map(|(_, r)| Arc::clone(r)).collect();
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
                let build_keys: Vec<_> = self.on.iter().map(|(_, r)| Arc::clone(r)).collect();
                let probe_keys: Vec<_> = self.on.iter().map(|(l, _)| Arc::clone(l)).collect();
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
        let fast_path_threshold = self.fast_path_threshold;
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
                fast_path_threshold,
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
    fast_path_threshold: usize,
    build_schema: SchemaRef,
    probe_schema: SchemaRef,
    _output_schema: SchemaRef,
    context: Arc<TaskContext>,
    metrics: GraceHashJoinMetrics,
) -> DFResult<impl Stream<Item = DFResult<RecordBatch>>> {
    // Set up memory reservation (shared across build and probe phases)
    let mut reservation = MutableReservation(
        MemoryConsumer::new("GraceHashJoinExec")
            .with_can_spill(true)
            .register(&context.runtime_env().memory_pool),
    );

    let mut partitions: Vec<HashPartition> =
        (0..num_partitions).map(|_| HashPartition::new()).collect();

    let mut scratch = ScratchSpace::default();

    // Phase 1: Partition the build side
    {
        let _timer = metrics.build_time.timer();
        partition_build_side(
            build_stream,
            &build_keys,
            num_partitions,
            &build_schema,
            &mut partitions,
            &mut reservation,
            &context,
            &metrics,
            &mut scratch,
        )
        .await?;
    }

    // Log build-side partition summary
    {
        let pool = &context.runtime_env().memory_pool;
        let total_build_rows: usize = partitions
            .iter()
            .flat_map(|p| p.build_batches.iter())
            .map(|b| b.num_rows())
            .sum();
        let total_build_bytes: usize = partitions.iter().map(|p| p.build_mem_size).sum();
        let spilled_count = partitions.iter().filter(|p| p.build_spilled()).count();
        info!(
            "GraceHashJoin: build phase complete. {} partitions ({} spilled), \
             total build: {} rows, {} bytes. Memory pool reserved={}",
            num_partitions,
            spilled_count,
            total_build_rows,
            total_build_bytes,
            pool.reserved(),
        );
        for (i, p) in partitions.iter().enumerate() {
            if !p.build_batches.is_empty() || p.build_spilled() {
                let rows: usize = p.build_batches.iter().map(|b| b.num_rows()).sum();
                info!(
                    "GraceHashJoin: partition[{}] build: {} batches, {} rows, {} bytes, spilled={}",
                    i,
                    p.build_batches.len(),
                    rows,
                    p.build_mem_size,
                    p.build_spilled(),
                );
            }
        }
    }

    // Fast path: if no build partitions spilled and the build side is
    // genuinely tiny, skip probe partitioning and stream the probe directly
    // through a single HashJoinExec. This avoids spilling gigabytes of
    // probe data to disk for a trivial hash table (e.g. 10-row build side).
    //
    // The threshold uses actual batch sizes (not the unreliable proportional
    // estimate). The configured value is divided by spark.executor.cores in
    // the planner so each concurrent task gets its fair share.
    // Configurable via spark.comet.exec.graceHashJoin.fastPathThreshold.

    let build_spilled = partitions.iter().any(|p| p.build_spilled());
    let actual_build_bytes: usize = partitions
        .iter()
        .flat_map(|p| p.build_batches.iter())
        .map(|b| b.get_array_memory_size())
        .sum();

    if !build_spilled && fast_path_threshold > 0 && actual_build_bytes <= fast_path_threshold {
        let total_build_rows: usize = partitions
            .iter()
            .flat_map(|p| p.build_batches.iter())
            .map(|b| b.num_rows())
            .sum();
        info!(
            "GraceHashJoin: fast path — build side tiny ({} rows, {} bytes). \
             Streaming probe directly through HashJoinExec.",
            total_build_rows, actual_build_bytes,
        );

        // Release our reservation — HashJoinExec tracks its own memory.
        reservation.free();

        let all_build_batches: Vec<RecordBatch> = partitions
            .into_iter()
            .flat_map(|p| p.build_batches)
            .collect();
        let build_data = if all_build_batches.is_empty() {
            vec![RecordBatch::new_empty(Arc::clone(&build_schema))]
        } else {
            vec![concat_batches(&build_schema, &all_build_batches)?]
        };

        let build_source = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[build_data], Arc::clone(&build_schema), None)?,
        )));

        let probe_source: Arc<dyn ExecutionPlan> = Arc::new(StreamSourceExec::new(
            probe_stream,
            Arc::clone(&probe_schema),
        ));

        let (left_source, right_source): (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) =
            if build_left {
                (build_source, probe_source)
            } else {
                (probe_source, build_source)
            };

        info!(
            "GraceHashJoin: FAST PATH creating HashJoinExec, \
             build_left={}, actual_build_bytes={}",
            build_left, actual_build_bytes,
        );

        let stream = if build_left {
            let hash_join = HashJoinExec::try_new(
                left_source,
                right_source,
                original_on,
                filter,
                &join_type,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
            )?;
            info!(
                "GraceHashJoin: FAST PATH plan:\n{}",
                DisplayableExecutionPlan::new(&hash_join).indent(true)
            );
            hash_join.execute(0, Arc::clone(&context))?
        } else {
            let hash_join = Arc::new(HashJoinExec::try_new(
                left_source,
                right_source,
                original_on,
                filter,
                &join_type,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
            )?);
            let swapped = hash_join.swap_inputs(PartitionMode::CollectLeft)?;
            info!(
                "GraceHashJoin: FAST PATH (swapped) plan:\n{}",
                DisplayableExecutionPlan::new(swapped.as_ref()).indent(true)
            );
            swapped.execute(0, Arc::clone(&context))?
        };

        let output_metrics = metrics.baseline.clone();
        let result_stream = stream.inspect_ok(move |batch| {
            output_metrics.record_output(batch.num_rows());
        });

        return Ok(result_stream.boxed());
    }

    let total_build_rows: usize = partitions
        .iter()
        .flat_map(|p| p.build_batches.iter())
        .map(|b| b.num_rows())
        .sum();
    info!(
        "GraceHashJoin: slow path — build spilled={}, {} rows, {} bytes (actual). \
         join_type={:?}, build_left={}. Partitioning probe side.",
        build_spilled, total_build_rows, actual_build_bytes, join_type, build_left,
    );

    // Phase 2: Partition the probe side
    {
        let _timer = metrics.probe_time.timer();
        partition_probe_side(
            probe_stream,
            &probe_keys,
            num_partitions,
            &probe_schema,
            &mut partitions,
            &mut reservation,
            &build_schema,
            &context,
            &metrics,
            &mut scratch,
        )
        .await?;
    }

    // Log probe-side partition summary
    {
        let total_probe_rows: usize = partitions
            .iter()
            .flat_map(|p| p.probe_batches.iter())
            .map(|b| b.num_rows())
            .sum();
        let total_probe_bytes: usize = partitions.iter().map(|p| p.probe_mem_size).sum();
        let probe_spilled = partitions
            .iter()
            .filter(|p| p.probe_spill_writer.is_some())
            .count();
        info!(
            "GraceHashJoin: probe phase complete. \
             total probe (in-memory): {} rows, {} bytes, {} spilled",
            total_probe_rows, total_probe_bytes, probe_spilled,
        );
    }

    // Finish all open spill writers before reading back
    let finished_partitions =
        finish_spill_writers(partitions, &build_schema, &probe_schema, &metrics)?;

    // Merge adjacent partitions to reduce the number of HashJoinExec calls.
    // Compute desired partition count from total build bytes.
    let total_build_bytes: usize = finished_partitions.iter().map(|p| p.build_bytes).sum();
    let desired_partitions = if total_build_bytes > 0 {
        let desired = total_build_bytes.div_ceil(TARGET_PARTITION_BUILD_SIZE);
        desired.max(1).min(num_partitions)
    } else {
        1
    };
    let original_partition_count = finished_partitions.len();
    let finished_partitions = merge_finished_partitions(finished_partitions, desired_partitions);
    if finished_partitions.len() < original_partition_count {
        info!(
            "GraceHashJoin: merged {} partitions into {} (total build {} bytes, \
             target {} bytes/partition)",
            original_partition_count,
            finished_partitions.len(),
            total_build_bytes,
            TARGET_PARTITION_BUILD_SIZE,
        );
    }

    // Release all remaining reservation before Phase 3. The in-memory
    // partition data is now owned by finished_partitions and will be moved
    // into per-partition HashJoinExec instances (which track memory via
    // their own HashJoinInput reservations). Keeping our reservation alive
    // would double-count the memory and starve other consumers.
    reservation.free();

    // Phase 3: Join partitions sequentially.
    // We use a concurrency limit of 1 to avoid creating multiple simultaneous
    // HashJoinInput reservations per task. With multiple Spark tasks sharing
    // the same memory pool, even modest build sides (e.g. 22 MB) can exhaust
    // memory when many tasks run concurrent hash table builds simultaneously.
    const MAX_CONCURRENT_PARTITIONS: usize = 1;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_PARTITIONS));
    let (tx, rx) = mpsc::channel::<DFResult<RecordBatch>>(MAX_CONCURRENT_PARTITIONS * 2);

    for partition in finished_partitions {
        let tx = tx.clone();
        let sem = Arc::clone(&semaphore);
        let original_on = original_on.clone();
        let filter = filter.clone();
        let build_schema = Arc::clone(&build_schema);
        let probe_schema = Arc::clone(&probe_schema);
        let context = Arc::clone(&context);

        tokio::spawn(async move {
            let _permit = match sem.acquire().await {
                Ok(p) => p,
                Err(_) => return, // semaphore closed
            };
            match join_single_partition(
                partition,
                original_on,
                filter,
                join_type,
                build_left,
                build_schema,
                probe_schema,
                context,
            )
            .await
            {
                Ok(streams) => {
                    for mut stream in streams {
                        while let Some(batch) = stream.next().await {
                            if tx.send(batch).await.is_err() {
                                return;
                            }
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                }
            }
        });
    }
    drop(tx);

    let output_metrics = metrics.baseline.clone();
    let output_row_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&output_row_count);
    let jt = join_type;
    let bl = build_left;
    let result_stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|batch| (batch, rx))
    })
    .inspect_ok(move |batch| {
        output_metrics.record_output(batch.num_rows());
        let prev = counter.fetch_add(
            batch.num_rows(),
            std::sync::atomic::Ordering::Relaxed,
        );
        let new_total = prev + batch.num_rows();
        // Log every ~1M rows to detect exploding joins
        if new_total / 1_000_000 > prev / 1_000_000 {
            info!(
                "GraceHashJoin: slow path output: {} rows emitted so far \
                 (join_type={:?}, build_left={})",
                new_total, jt, bl,
            );
        }
    });

    Ok(result_stream.boxed())
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

    fn free(&mut self) -> usize {
        self.0.free()
    }
}

// ---------------------------------------------------------------------------
// ScratchSpace: reusable buffers for efficient hash partitioning
// ---------------------------------------------------------------------------

/// Reusable scratch buffers for partitioning batches. Uses a prefix-sum
/// algorithm (borrowed from the shuffle `multi_partition.rs`) to compute
/// contiguous row-index regions per partition in a single pass, avoiding
/// N separate `take()` kernel calls.
#[derive(Default)]
struct ScratchSpace {
    /// Hash values for each row.
    hashes: Vec<u64>,
    /// Partition id assigned to each row.
    partition_ids: Vec<u32>,
    /// Row indices reordered so that each partition's rows are contiguous.
    partition_row_indices: Vec<u32>,
    /// `partition_starts[k]..partition_starts[k+1]` gives the slice of
    /// `partition_row_indices` belonging to partition k.
    partition_starts: Vec<u32>,
}

impl ScratchSpace {
    /// Compute hashes and partition ids, then build the prefix-sum index
    /// structures for the given batch.
    fn compute_partitions(
        &mut self,
        batch: &RecordBatch,
        keys: &[Arc<dyn PhysicalExpr>],
        num_partitions: usize,
        recursion_level: usize,
    ) -> DFResult<()> {
        let num_rows = batch.num_rows();

        // Evaluate key columns
        let key_columns: Vec<_> = keys
            .iter()
            .map(|expr| expr.evaluate(batch).and_then(|cv| cv.into_array(num_rows)))
            .collect::<DFResult<Vec<_>>>()?;

        // Hash
        self.hashes.resize(num_rows, 0);
        self.hashes.truncate(num_rows);
        self.hashes.fill(0);
        let random_state = partition_random_state(recursion_level);
        create_hashes(&key_columns, &random_state, &mut self.hashes)?;

        // Assign partition ids
        self.partition_ids.resize(num_rows, 0);
        for (i, hash) in self.hashes[..num_rows].iter().enumerate() {
            self.partition_ids[i] = (*hash as u32) % (num_partitions as u32);
        }

        // Prefix-sum to get contiguous regions
        self.map_partition_ids_to_starts_and_indices(num_partitions, num_rows);

        Ok(())
    }

    /// Prefix-sum algorithm from `multi_partition.rs`.
    fn map_partition_ids_to_starts_and_indices(&mut self, num_partitions: usize, num_rows: usize) {
        let partition_ids = &self.partition_ids[..num_rows];

        // Count each partition size
        let partition_counters = &mut self.partition_starts;
        partition_counters.resize(num_partitions + 1, 0);
        partition_counters.fill(0);
        partition_ids
            .iter()
            .for_each(|pid| partition_counters[*pid as usize] += 1);

        // Accumulate into partition ends
        let mut accum = 0u32;
        for v in partition_counters.iter_mut() {
            *v += accum;
            accum = *v;
        }

        // Build partition_row_indices (iterate in reverse to turn ends into starts)
        self.partition_row_indices.resize(num_rows, 0);
        for (index, pid) in partition_ids.iter().enumerate().rev() {
            self.partition_starts[*pid as usize] -= 1;
            let pos = self.partition_starts[*pid as usize];
            self.partition_row_indices[pos as usize] = index as u32;
        }
    }

    /// Get the row index slice for a given partition.
    fn partition_slice(&self, partition_id: usize) -> &[u32] {
        let start = self.partition_starts[partition_id] as usize;
        let end = self.partition_starts[partition_id + 1] as usize;
        &self.partition_row_indices[start..end]
    }

    /// Number of rows in a given partition.
    fn partition_len(&self, partition_id: usize) -> usize {
        (self.partition_starts[partition_id + 1] - self.partition_starts[partition_id]) as usize
    }

    fn take_partition(
        &self,
        batch: &RecordBatch,
        partition_id: usize,
    ) -> DFResult<Option<RecordBatch>> {
        let row_indices = self.partition_slice(partition_id);
        if row_indices.is_empty() {
            return Ok(None);
        }
        let indices_array = UInt32Array::from(row_indices.to_vec());
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices_array, None))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(RecordBatch::try_new(batch.schema(), columns)?))
    }
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
    let reader = BufReader::with_capacity(SPILL_IO_BUFFER_SIZE, file);
    let stream_reader = StreamReader::try_new(reader, None)?;
    let batches: Vec<RecordBatch> = stream_reader.into_iter().collect::<Result<Vec<_>, _>>()?;
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
    scratch: &mut ScratchSpace,
) -> DFResult<()> {
    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }

        metrics.build_input_batches.add(1);
        metrics.build_input_rows.add(batch.num_rows());

        // Track total batch size once, estimate per-partition proportionally
        let total_batch_size = batch.get_array_memory_size();
        let total_rows = batch.num_rows();

        scratch.compute_partitions(&batch, keys, num_partitions, 0)?;

        #[allow(clippy::needless_range_loop)]
        for part_idx in 0..num_partitions {
            if scratch.partition_len(part_idx) == 0 {
                continue;
            }

            let sub_rows = scratch.partition_len(part_idx);
            let sub_batch = if sub_rows == total_rows {
                batch.clone()
            } else {
                scratch.take_partition(&batch, part_idx)?.unwrap()
            };
            let batch_size = if total_rows > 0 {
                (total_batch_size as u64 * sub_rows as u64 / total_rows as u64) as usize
            } else {
                0
            };

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
            idx,
            partitions[idx].build_mem_size,
            partitions[idx].build_batches.len()
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

/// Spill a single partition's probe-side data to disk using SpillWriter.
fn spill_partition_probe(
    partition: &mut HashPartition,
    schema: &SchemaRef,
    context: &Arc<TaskContext>,
    reservation: &mut MutableReservation,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<()> {
    if partition.probe_batches.is_empty() && partition.probe_spill_writer.is_some() {
        return Ok(());
    }

    let temp_file = context
        .runtime_env()
        .disk_manager
        .create_tmp_file("grace hash join probe")?;

    let mut writer = SpillWriter::new(temp_file, schema)?;
    writer.write_batches(&partition.probe_batches)?;

    let freed = partition.probe_mem_size;
    reservation.shrink(freed);

    metrics.spill_count.add(1);
    metrics.spilled_bytes.add(freed);

    partition.probe_spill_writer = Some(writer);
    partition.probe_batches.clear();
    partition.probe_mem_size = 0;

    Ok(())
}

/// Spill both build and probe sides of a partition to disk.
/// When spilling during the probe phase, both sides must be spilled so the
/// join phase reads both consistently from disk.
fn spill_partition_both_sides(
    partition: &mut HashPartition,
    probe_schema: &SchemaRef,
    build_schema: &SchemaRef,
    context: &Arc<TaskContext>,
    reservation: &mut MutableReservation,
    metrics: &GraceHashJoinMetrics,
) -> DFResult<()> {
    if !partition.build_spilled() {
        spill_partition_build(partition, build_schema, context, reservation, metrics)?;
    }
    if partition.probe_spill_writer.is_none() {
        spill_partition_probe(partition, probe_schema, context, reservation, metrics)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 2: Probe-side partitioning
// ---------------------------------------------------------------------------

/// Phase 2: Read all probe-side batches, route to in-memory buffers or spill files.
/// Tracks probe-side memory in the reservation and spills partitions when pressure
/// is detected, preventing OOM when the probe side is much larger than the build side.
#[allow(clippy::too_many_arguments)]
async fn partition_probe_side(
    mut input: SendableRecordBatchStream,
    keys: &[Arc<dyn PhysicalExpr>],
    num_partitions: usize,
    schema: &SchemaRef,
    partitions: &mut [HashPartition],
    reservation: &mut MutableReservation,
    build_schema: &SchemaRef,
    context: &Arc<TaskContext>,
    metrics: &GraceHashJoinMetrics,
    scratch: &mut ScratchSpace,
) -> DFResult<()> {
    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }

        metrics.input_batches.add(1);
        metrics.input_rows.add(batch.num_rows());

        let total_rows = batch.num_rows();
        scratch.compute_partitions(&batch, keys, num_partitions, 0)?;

        #[allow(clippy::needless_range_loop)]
        for part_idx in 0..num_partitions {
            if scratch.partition_len(part_idx) == 0 {
                continue;
            }

            let sub_batch = if scratch.partition_len(part_idx) == total_rows {
                batch.clone()
            } else {
                scratch.take_partition(&batch, part_idx)?.unwrap()
            };

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
                        let freed = partitions[part_idx].probe_mem_size;
                        let batches = std::mem::take(&mut partitions[part_idx].probe_batches);
                        writer.write_batches(&batches)?;
                        partitions[part_idx].probe_mem_size = 0;
                        reservation.shrink(freed);
                    }
                    partitions[part_idx].probe_spill_writer = Some(writer);
                }
                if let Some(ref mut writer) = partitions[part_idx].probe_spill_writer {
                    writer.write_batch(&sub_batch)?;
                }
            } else {
                let batch_size = sub_batch.get_array_memory_size();
                if reservation.try_grow(batch_size).is_err() {
                    // Memory pressure: spill ALL non-spilled partitions.
                    // With multiple concurrent GHJ instances sharing the pool,
                    // partial spilling just lets data re-accumulate. Spilling
                    // everything ensures all subsequent probe data goes directly
                    // to disk, keeping in-memory footprint near zero.
                    let total_in_memory: usize = partitions
                        .iter()
                        .filter(|p| !p.build_spilled())
                        .map(|p| p.build_mem_size + p.probe_mem_size)
                        .sum();
                    let spillable_count = partitions.iter().filter(|p| !p.build_spilled()).count();

                    info!(
                        "GraceHashJoin: memory pressure during probe, \
                         spilling all {} non-spilled partitions ({} bytes)",
                        spillable_count, total_in_memory,
                    );

                    for i in 0..partitions.len() {
                        if !partitions[i].build_spilled() {
                            spill_partition_both_sides(
                                &mut partitions[i],
                                schema,
                                build_schema,
                                context,
                                reservation,
                                metrics,
                            )?;
                        }
                    }
                }

                if partitions[part_idx].build_spilled() {
                    // Partition was just spilled above — write to spill writer
                    if partitions[part_idx].probe_spill_writer.is_none() {
                        let temp_file = context
                            .runtime_env()
                            .disk_manager
                            .create_tmp_file("grace hash join probe")?;
                        partitions[part_idx].probe_spill_writer =
                            Some(SpillWriter::new(temp_file, schema)?);
                    }
                    if let Some(ref mut writer) = partitions[part_idx].probe_spill_writer {
                        writer.write_batch(&sub_batch)?;
                    }
                } else {
                    partitions[part_idx].probe_mem_size += batch_size;
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
/// After merging, a partition may hold multiple spill files from adjacent
/// original partitions.
struct FinishedPartition {
    build_batches: Vec<RecordBatch>,
    probe_batches: Vec<RecordBatch>,
    build_spill_files: Vec<RefCountedTempFile>,
    probe_spill_files: Vec<RefCountedTempFile>,
    /// Total build-side bytes (in-memory + spilled) for merge decisions.
    build_bytes: usize,
}

/// Finish all open spill writers so files can be read back.
fn finish_spill_writers(
    partitions: Vec<HashPartition>,
    _left_schema: &SchemaRef,
    _right_schema: &SchemaRef,
    _metrics: &GraceHashJoinMetrics,
) -> DFResult<Vec<FinishedPartition>> {
    let mut finished = Vec::with_capacity(partitions.len());

    for partition in partitions {
        let (build_spill_files, spilled_build_bytes) =
            if let Some(writer) = partition.build_spill_writer {
                let (file, bytes) = writer.finish()?;
                (vec![file], bytes)
            } else {
                (vec![], 0)
            };

        let probe_spill_files = if let Some(writer) = partition.probe_spill_writer {
            let (file, _bytes) = writer.finish()?;
            vec![file]
        } else {
            vec![]
        };

        finished.push(FinishedPartition {
            build_bytes: partition.build_mem_size + spilled_build_bytes,
            build_batches: partition.build_batches,
            probe_batches: partition.probe_batches,
            build_spill_files,
            probe_spill_files,
        });
    }

    Ok(finished)
}

/// Merge adjacent finished partitions to reduce the number of per-partition
/// HashJoinExec calls. Groups adjacent partitions so each merged group has
/// roughly `TARGET_PARTITION_BUILD_SIZE` bytes of build data.
fn merge_finished_partitions(
    partitions: Vec<FinishedPartition>,
    target_count: usize,
) -> Vec<FinishedPartition> {
    let original_count = partitions.len();
    if target_count >= original_count {
        return partitions;
    }

    // Divide original_count partitions into target_count groups as evenly as possible
    let base_group_size = original_count / target_count;
    let remainder = original_count % target_count;

    let mut merged = Vec::with_capacity(target_count);
    let mut iter = partitions.into_iter();

    for group_idx in 0..target_count {
        // First `remainder` groups get one extra partition
        let group_size = base_group_size + if group_idx < remainder { 1 } else { 0 };

        let mut build_batches = Vec::new();
        let mut probe_batches = Vec::new();
        let mut build_spill_files = Vec::new();
        let mut probe_spill_files = Vec::new();
        let mut build_bytes = 0usize;

        for _ in 0..group_size {
            if let Some(p) = iter.next() {
                build_batches.extend(p.build_batches);
                probe_batches.extend(p.probe_batches);
                build_spill_files.extend(p.build_spill_files);
                probe_spill_files.extend(p.probe_spill_files);
                build_bytes += p.build_bytes;
            }
        }

        merged.push(FinishedPartition {
            build_batches,
            probe_batches,
            build_spill_files,
            probe_spill_files,
            build_bytes,
        });
    }

    merged
}

// ---------------------------------------------------------------------------
// Phase 3: Per-partition hash joins
// ---------------------------------------------------------------------------

/// Join a single partition: reads build-side spill (if any) via spawn_blocking,
/// then delegates to `join_with_spilled_probe` or `join_partition_recursive`.
/// Returns the resulting streams for this partition.
///
/// Takes all owned data so it can be called inside `tokio::spawn`.
#[allow(clippy::too_many_arguments)]
async fn join_single_partition(
    partition: FinishedPartition,
    original_on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    build_left: bool,
    build_schema: SchemaRef,
    probe_schema: SchemaRef,
    context: Arc<TaskContext>,
) -> DFResult<Vec<SendableRecordBatchStream>> {
    // Get build-side batches (from memory or disk — build side is typically small).
    // Use spawn_blocking for spill reads to avoid blocking the async executor.
    let mut build_batches = partition.build_batches;
    if !partition.build_spill_files.is_empty() {
        let schema = Arc::clone(&build_schema);
        let spill_files = partition.build_spill_files;
        let spilled = tokio::task::spawn_blocking(move || {
            let mut all = Vec::new();
            for spill_file in &spill_files {
                all.extend(read_spilled_batches(spill_file, &schema)?);
            }
            Ok::<_, DataFusionError>(all)
        })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "GraceHashJoin: build spill read task failed: {e}"
            ))
        })??;
        build_batches.extend(spilled);
    }

    // Coalesce many tiny sub-batches (one per original input batch) into a
    // single batch per side. This avoids repeated concat_batches downstream
    // and reduces overhead in HashJoinExec.
    let build_batches = if build_batches.len() > 1 {
        vec![concat_batches(&build_schema, &build_batches)?]
    } else {
        build_batches
    };

    let mut streams = Vec::new();

    if !partition.probe_spill_files.is_empty() {
        // Probe side has spill file(s). Also include any in-memory probe
        // batches (possible after merging adjacent partitions).
        join_with_spilled_probe(
            build_batches,
            partition.probe_spill_files,
            partition.probe_batches,
            &original_on,
            &filter,
            &join_type,
            build_left,
            &build_schema,
            &probe_schema,
            &context,
            &mut streams,
        )?;
    } else {
        // Probe side is in-memory: coalesce and use repartitioning support
        let probe_batches = if partition.probe_batches.len() > 1 {
            vec![concat_batches(&probe_schema, &partition.probe_batches)?]
        } else {
            partition.probe_batches
        };
        join_partition_recursive(
            build_batches,
            probe_batches,
            &original_on,
            &filter,
            &join_type,
            build_left,
            &build_schema,
            &probe_schema,
            &context,
            1,
            &mut streams,
        )?;
    }

    Ok(streams)
}

/// Join a partition where the probe side was spilled to disk.
/// Uses SpillReaderExec to stream probe data from the spill file instead of
/// loading it all into memory. The build side (typically small) is loaded
/// into a MemorySourceConfig for the hash table.
#[allow(clippy::too_many_arguments)]
fn join_with_spilled_probe(
    build_batches: Vec<RecordBatch>,
    probe_spill_files: Vec<RefCountedTempFile>,
    probe_in_memory: Vec<RecordBatch>,
    original_on: JoinOnRef<'_>,
    filter: &Option<JoinFilter>,
    join_type: &JoinType,
    build_left: bool,
    build_schema: &SchemaRef,
    probe_schema: &SchemaRef,
    context: &Arc<TaskContext>,
    streams: &mut Vec<SendableRecordBatchStream>,
) -> DFResult<()> {
    let probe_spill_files_count = probe_spill_files.len();

    // Skip if build side is empty and join type requires it
    let build_empty = build_batches.is_empty();
    let skip = match join_type {
        JoinType::Inner | JoinType::LeftSemi | JoinType::LeftAnti => {
            if build_left {
                build_empty
            } else {
                false // probe emptiness unknown without reading
            }
        }
        JoinType::Left | JoinType::LeftMark => {
            if build_left {
                build_empty
            } else {
                false
            }
        }
        JoinType::Right => {
            if !build_left {
                build_empty
            } else {
                false
            }
        }
        _ => false,
    };
    if skip {
        return Ok(());
    }

    let build_size: usize = build_batches
        .iter()
        .map(|b| b.get_array_memory_size())
        .sum();
    let build_rows: usize = build_batches.iter().map(|b| b.num_rows()).sum();
    info!(
        "GraceHashJoin: join_with_spilled_probe build: {} batches/{} rows/{} bytes, \
         probe: streaming from spill file",
        build_batches.len(),
        build_rows,
        build_size,
    );

    // If build side exceeds the target partition size, fall back to eager
    // read + recursive repartitioning. This prevents creating HashJoinExec
    // with oversized build sides that expand into huge hash tables.
    let needs_repartition = build_size > TARGET_PARTITION_BUILD_SIZE;

    if needs_repartition {
        info!(
            "GraceHashJoin: build too large for streaming probe ({} bytes > {} target), \
             falling back to eager read + repartition",
            build_size, TARGET_PARTITION_BUILD_SIZE,
        );
        let mut probe_batches = probe_in_memory;
        for spill_file in &probe_spill_files {
            probe_batches.extend(read_spilled_batches(spill_file, probe_schema)?);
        }
        return join_partition_recursive(
            build_batches,
            probe_batches,
            original_on,
            filter,
            join_type,
            build_left,
            build_schema,
            probe_schema,
            context,
            1,
            streams,
        );
    }

    // Concatenate build side into single batch
    let build_data = if build_batches.is_empty() {
        vec![RecordBatch::new_empty(Arc::clone(build_schema))]
    } else if build_batches.len() == 1 {
        build_batches
    } else {
        vec![concat_batches(build_schema, &build_batches)?]
    };

    // Build side: MemorySourceConfig (small)
    let build_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
        &[build_data],
        Arc::clone(build_schema),
        None,
    )?)));

    // Probe side: streaming from spill file(s).
    // With a single spill file and no in-memory batches, use the streaming
    // SpillReaderExec. Otherwise read eagerly since the merged group sizes
    // are bounded by TARGET_PARTITION_BUILD_SIZE.
    let probe_source: Arc<dyn ExecutionPlan> =
        if probe_spill_files.len() == 1 && probe_in_memory.is_empty() {
            Arc::new(SpillReaderExec::new(
                probe_spill_files.into_iter().next().unwrap(),
                Arc::clone(probe_schema),
            ))
        } else {
            let mut probe_batches = probe_in_memory;
            for spill_file in &probe_spill_files {
                probe_batches.extend(read_spilled_batches(spill_file, probe_schema)?);
            }
            let probe_data = if probe_batches.is_empty() {
                vec![RecordBatch::new_empty(Arc::clone(probe_schema))]
            } else {
                vec![concat_batches(probe_schema, &probe_batches)?]
            };
            Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
                &[probe_data],
                Arc::clone(probe_schema),
                None,
            )?)))
        };

    // HashJoinExec expects left=build in CollectLeft mode
    let (left_source, right_source) = if build_left {
        (build_source as Arc<dyn ExecutionPlan>, probe_source)
    } else {
        (probe_source, build_source as Arc<dyn ExecutionPlan>)
    };

    info!(
        "GraceHashJoin: SPILLED PROBE PATH creating HashJoinExec, \
         build_left={}, build_size={}, probe_source={}",
        build_left,
        build_size,
        if probe_spill_files_count == 1 { "SpillReaderExec" } else { "MemorySourceConfig" },
    );

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
        info!(
            "GraceHashJoin: SPILLED PROBE PATH plan:\n{}",
            DisplayableExecutionPlan::new(&hash_join).indent(true)
        );
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
        info!(
            "GraceHashJoin: SPILLED PROBE PATH (swapped) plan:\n{}",
            DisplayableExecutionPlan::new(swapped.as_ref()).indent(true)
        );
        swapped.execute(0, Arc::clone(context))?
    };

    streams.push(stream);
    Ok(())
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
        JoinType::Inner | JoinType::LeftSemi | JoinType::LeftAnti => left_empty || right_empty,
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
    let build_size: usize = build_batches
        .iter()
        .map(|b| b.get_array_memory_size())
        .sum();
    let build_rows: usize = build_batches.iter().map(|b| b.num_rows()).sum();
    let probe_size: usize = probe_batches
        .iter()
        .map(|b| b.get_array_memory_size())
        .sum();
    let probe_rows: usize = probe_batches.iter().map(|b| b.num_rows()).sum();
    let pool_reserved = context.runtime_env().memory_pool.reserved();
    info!(
        "GraceHashJoin: join_partition_recursive level={}, \
         build: {} batches/{} rows/{} bytes, \
         probe: {} batches/{} rows/{} bytes, \
         pool reserved={}",
        recursion_level,
        build_batches.len(),
        build_rows,
        build_size,
        probe_batches.len(),
        probe_rows,
        probe_size,
        pool_reserved,
    );
    // Repartition if the build side exceeds the target size. This prevents
    // creating HashJoinExec with oversized build sides whose hash tables
    // can expand well beyond the raw data size and exhaust the memory pool.
    let needs_repartition = build_size > TARGET_PARTITION_BUILD_SIZE;
    if needs_repartition {
        info!(
            "GraceHashJoin: repartition needed at level {}: \
             build_size={} > target={}, pool reserved={}",
            recursion_level,
            build_size,
            TARGET_PARTITION_BUILD_SIZE,
            context.runtime_env().memory_pool.reserved(),
        );
    }

    if needs_repartition {
        if recursion_level >= MAX_RECURSION_DEPTH {
            let total_build_rows: usize = build_batches.iter().map(|b| b.num_rows()).sum();
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

    // Concatenate small sub-batches into single batches to reduce per-batch overhead
    let build_data = if build_batches.is_empty() {
        vec![RecordBatch::new_empty(Arc::clone(build_schema))]
    } else if build_batches.len() == 1 {
        build_batches
    } else {
        vec![concat_batches(build_schema, &build_batches)?]
    };
    let probe_data = if probe_batches.is_empty() {
        vec![RecordBatch::new_empty(Arc::clone(probe_schema))]
    } else if probe_batches.len() == 1 {
        probe_batches
    } else {
        vec![concat_batches(probe_schema, &probe_batches)?]
    };

    // Create per-partition hash join.
    // HashJoinExec expects left=build (CollectLeft mode).
    let (left_data, left_schema_ref, right_data, right_schema_ref) = if build_left {
        (build_data, build_schema, probe_data, probe_schema)
    } else {
        (probe_data, probe_schema, build_data, build_schema)
    };

    let left_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
        &[left_data],
        Arc::clone(left_schema_ref),
        None,
    )?)));

    let right_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
        &[right_data],
        Arc::clone(right_schema_ref),
        None,
    )?)));

    info!(
        "GraceHashJoin: RECURSIVE PATH creating HashJoinExec at level={}, \
         build_left={}, build_size={}, probe_size={}",
        recursion_level, build_left, build_size, probe_size,
    );

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
        info!(
            "GraceHashJoin: RECURSIVE PATH plan (level={}):\n{}",
            recursion_level,
            DisplayableExecutionPlan::new(&hash_join).indent(true)
        );
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
        info!(
            "GraceHashJoin: RECURSIVE PATH (swapped, level={}) plan:\n{}",
            recursion_level,
            DisplayableExecutionPlan::new(swapped.as_ref()).indent(true)
        );
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

    let mut scratch = ScratchSpace::default();

    // Sub-partition the build side
    let mut build_sub: Vec<Vec<RecordBatch>> =
        (0..num_sub_partitions).map(|_| Vec::new()).collect();
    for batch in &build_batches {
        let total_rows = batch.num_rows();
        scratch.compute_partitions(batch, &build_keys, num_sub_partitions, recursion_level)?;
        for (i, sub_vec) in build_sub.iter_mut().enumerate() {
            if scratch.partition_len(i) == 0 {
                continue;
            }
            if scratch.partition_len(i) == total_rows {
                sub_vec.push(batch.clone());
            } else if let Some(sub) = scratch.take_partition(batch, i)? {
                sub_vec.push(sub);
            }
        }
    }

    // Sub-partition the probe side
    let mut probe_sub: Vec<Vec<RecordBatch>> =
        (0..num_sub_partitions).map(|_| Vec::new()).collect();
    for batch in &probe_batches {
        let total_rows = batch.num_rows();
        scratch.compute_partitions(batch, &probe_keys, num_sub_partitions, recursion_level)?;
        for (i, sub_vec) in probe_sub.iter_mut().enumerate() {
            if scratch.partition_len(i) == 0 {
                continue;
            }
            if scratch.partition_len(i) == total_rows {
                sub_vec.push(batch.clone());
            } else if let Some(sub) = scratch.take_partition(batch, i)? {
                sub_vec.push(sub);
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
    use datafusion::execution::memory_pool::FairSpillPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::prelude::SessionConfig;
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

        let left_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
            &[left_batches],
            Arc::clone(&left_schema),
            None,
        )?)));
        let right_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
            &[right_batches],
            Arc::clone(&right_schema),
            None,
        )?)));

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
            10 * 1024 * 1024, // 10 MB fast path threshold
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

        let left_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let left_batches = vec![RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?];
        let right_batches = vec![RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )?];

        let left_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
            &[left_batches],
            Arc::clone(&left_schema),
            None,
        )?)));
        let right_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
            &[right_batches],
            Arc::clone(&right_schema),
            None,
        )?)));

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
            10 * 1024 * 1024, // 10 MB fast path threshold
        )?;

        let stream = grace_join.execute(0, task_ctx)?;
        let result_batches: Vec<RecordBatch> = stream.try_collect().await?;

        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Expected 0 rows for non-matching keys");

        Ok(())
    }

    /// Helper to create a SessionContext with a bounded FairSpillPool.
    fn context_with_memory_limit(pool_bytes: usize) -> SessionContext {
        let pool = Arc::new(FairSpillPool::new(pool_bytes));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .unwrap();
        let config = SessionConfig::new();
        SessionContext::new_with_config_rt(config, runtime)
    }

    /// Generate a batch of N rows with sequential IDs and a padding string
    /// column to control memory size. Each row is ~100 bytes of padding.
    fn make_large_batch(start_id: i32, count: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let ids: Vec<i32> = (start_id..start_id + count as i32).collect();
        let padding = "x".repeat(100);
        let vals: Vec<&str> = (0..count).map(|_| padding.as_str()).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(vals)),
            ],
        )
        .unwrap()
    }

    /// Test that GHJ correctly repartitions a large build side instead of
    /// creating an oversized HashJoinExec hash table that OOMs.
    ///
    /// Setup: 256 MB memory pool, ~80 MB build side, ~10 MB probe side.
    /// Without repartitioning, the hash table would be ~240 MB and could
    /// exhaust the 256 MB pool. With repartitioning (32 MB threshold),
    /// the build side is split into sub-partitions of ~5 MB each.
    #[tokio::test]
    async fn test_grace_hash_join_repartitions_large_build() -> DFResult<()> {
        // 256 MB pool — tight enough that a 80 MB build → ~240 MB hash table fails
        let ctx = context_with_memory_limit(256 * 1024 * 1024);
        let task_ctx = ctx.task_ctx();

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));

        // Build side: ~80 MB (800K rows × ~100 bytes)
        let left_batches = vec![
            make_large_batch(0, 200_000),
            make_large_batch(200_000, 200_000),
            make_large_batch(400_000, 200_000),
            make_large_batch(600_000, 200_000),
        ];
        let build_bytes: usize = left_batches
            .iter()
            .map(|b| b.get_array_memory_size())
            .sum();
        eprintln!("Test build side: {} bytes ({} MB)", build_bytes, build_bytes / (1024 * 1024));

        // Probe side: small (~1 MB, 10K rows)
        let right_batches = vec![make_large_batch(0, 10_000)];

        let left_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
            &[left_batches],
            Arc::clone(&left_schema),
            None,
        )?)));
        let right_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
            &[right_batches],
            Arc::clone(&right_schema),
            None,
        )?)));

        let on = vec![(
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
        )];

        // Disable fast path to force slow path
        let grace_join = GraceHashJoinExec::try_new(
            left_source,
            right_source,
            on,
            None,
            &JoinType::Inner,
            16,
            true, // build_left
            0,    // fast_path_threshold = 0 (disabled)
        )?;

        let stream = grace_join.execute(0, task_ctx)?;
        let result_batches: Vec<RecordBatch> = stream.try_collect().await?;

        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        // All 10K probe rows match (IDs 0..10000 exist in build)
        assert_eq!(total_rows, 10_000, "Expected 10000 matching rows");

        Ok(())
    }

    /// Same test but with build_left=false to exercise the swap_inputs path.
    #[tokio::test]
    async fn test_grace_hash_join_repartitions_large_build_right() -> DFResult<()> {
        let ctx = context_with_memory_limit(256 * 1024 * 1024);
        let task_ctx = ctx.task_ctx();

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));

        // Probe side (left): small
        let left_batches = vec![make_large_batch(0, 10_000)];

        // Build side (right): ~80 MB
        let right_batches = vec![
            make_large_batch(0, 200_000),
            make_large_batch(200_000, 200_000),
            make_large_batch(400_000, 200_000),
            make_large_batch(600_000, 200_000),
        ];

        let left_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
            &[left_batches],
            Arc::clone(&left_schema),
            None,
        )?)));
        let right_source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
            &[right_batches],
            Arc::clone(&right_schema),
            None,
        )?)));

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
            16,
            false, // build_left=false → right is build side
            0,     // fast_path_threshold = 0 (disabled)
        )?;

        let stream = grace_join.execute(0, task_ctx)?;
        let result_batches: Vec<RecordBatch> = stream.try_collect().await?;

        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10_000, "Expected 10000 matching rows");

        Ok(())
    }
}
