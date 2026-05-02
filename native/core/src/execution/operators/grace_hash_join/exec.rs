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

//! Execution flow for Grace Hash Join.
//!
//! The entry point is [`execute_grace_hash_join`], which orchestrates:
//! 1. Optimistic fast path (skip partitioning when the build side fits).
//! 2. Slow path: Phase 2 probe partitioning, merge, Phase 3 per-partition joins.
//! 3. Per-partition `HashJoinExec` creation, with recursive repartitioning
//!    for oversized groups.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result as DFResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::stream::{self, StreamExt, TryStreamExt};
use futures::Stream;
use log::info;
use tokio::sync::mpsc;

use super::metrics::GraceHashJoinMetrics;
use super::partition::{
    buffer_build_optimistic, finish_spill_writers, merge_finished_partitions,
    partition_build_side, partition_from_buffer, partition_probe_side, sub_partition_batches,
    BuildBufferResult, FinishedPartition, HashPartition, ScratchSpace,
};
use super::spill::{read_spilled_batches, SpillReaderExec, StreamSourceExec};
use super::{JoinOnRef, DEFAULT_NUM_PARTITIONS, MAX_RECURSION_DEPTH, TARGET_PARTITION_BUILD_SIZE};

/// Global atomic counter for unique GHJ instance IDs (debug tracing).
static GHJ_INSTANCE_COUNTER: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// The output batch size for HashJoinExec within GHJ.
///
/// With the default Comet batch size (8192), HashJoinExec produces thousands
/// of small output batches, causing significant per-batch overhead for large
/// joins (e.g., 150M output rows = 18K batches at 8192).
///
/// 1M rows gives ~150 batches for a 150M row join — enough to avoid
/// per-batch overhead while keeping each output batch at a few hundred MB.
/// Cannot use `usize::MAX` because HashJoinExec pre-allocates Vec with
/// capacity = batch_size in `get_matched_indices_with_limit_offset`.
/// Cannot use 10M+ because output batches become multi-GB and cause OOM.
const GHJ_OUTPUT_BATCH_SIZE: usize = 1_000_000;

// ---------------------------------------------------------------------------
// Top-level execution
// ---------------------------------------------------------------------------

/// Main execution logic for the grace hash join.
///
/// `build_stream`/`probe_stream`: already swapped based on build_left.
/// `build_keys`/`probe_keys`: key expressions for their respective sides.
/// `original_on`: original (left_key, right_key) pairs for HashJoinExec.
/// `build_left`: whether left is build side (affects HashJoinExec construction).
#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_grace_hash_join(
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
    max_concurrent_partitions: usize,
    build_schema: SchemaRef,
    probe_schema: SchemaRef,
    context: Arc<TaskContext>,
    metrics: GraceHashJoinMetrics,
) -> DFResult<impl Stream<Item = DFResult<RecordBatch>>> {
    let ghj_id = GHJ_INSTANCE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // Set up memory reservation (shared across build and probe phases)
    let mut reservation = MemoryConsumer::new("GraceHashJoinExec")
        .with_can_spill(true)
        .register(&context.runtime_env().memory_pool);

    info!(
        "GHJ#{}: started. build_left={}, join_type={:?}, pool reserved={}",
        ghj_id,
        build_left,
        join_type,
        context.runtime_env().memory_pool.reserved(),
    );

    // Optimistic fast path: if the fast path threshold is set, try buffering
    // the build side without partitioning. This avoids the overhead of hash
    // computation, prefix-sum, and per-partition take() for every build batch,
    // which is wasted work when the build side fits in memory and the fast path
    // is taken (the common case with a generous threshold).
    if fast_path_threshold > 0 {
        let build_result = {
            let _timer = metrics.build_time.timer();
            buffer_build_optimistic(build_stream, &mut reservation, &metrics).await?
        };

        match build_result {
            BuildBufferResult::Complete(build_batches, actual_build_bytes)
                if actual_build_bytes <= fast_path_threshold =>
            {
                // Fast path: all build data buffered, no memory pressure.
                // Skip partitioning entirely and stream probe through HashJoinExec.
                let total_build_rows: usize = build_batches.iter().map(|b| b.num_rows()).sum();
                info!(
                    "GHJ#{}: optimistic fast path — build side ({} rows, {} bytes). \
                     Streaming probe directly through HashJoinExec. pool reserved={}",
                    ghj_id,
                    total_build_rows,
                    actual_build_bytes,
                    context.runtime_env().memory_pool.reserved(),
                );

                reservation.free();

                // Wrap probe stream to count input_batches and input_rows
                // (normally counted during partition_probe_side, which is
                // skipped in the fast path).
                let probe_input_batches = metrics.input_batches.clone();
                let probe_input_rows = metrics.input_rows.clone();
                let probe_schema_clone = Arc::clone(&probe_schema);
                let counting_probe = probe_stream.inspect_ok(move |batch| {
                    probe_input_batches.add(1);
                    probe_input_rows.add(batch.num_rows());
                });
                let counting_probe: SendableRecordBatchStream = Box::pin(
                    RecordBatchStreamAdapter::new(probe_schema_clone, counting_probe),
                );

                let stream = create_fast_path_stream(
                    build_batches,
                    counting_probe,
                    &original_on,
                    &filter,
                    &join_type,
                    build_left,
                    &build_schema,
                    &probe_schema,
                    &context,
                )?;

                let output_metrics = metrics.baseline.clone();
                let output_batch_count = metrics.output_batches.clone();
                let join_time = metrics.join_time.clone();
                let result_stream = stream.inspect_ok(move |batch| {
                    let _timer = join_time.timer();
                    output_metrics.record_output(batch.num_rows());
                    output_batch_count.add(1);
                });

                return Ok(result_stream.boxed());
            }
            result => {
                // Build side too large for fast path, or memory pressure occurred.
                // Partition the buffered batches offline and continue with slow path.
                let (buffered_batches, remaining_stream) = match result {
                    BuildBufferResult::Complete(batches, _) => (batches, None),
                    BuildBufferResult::NeedPartition(batches, stream) => (batches, Some(stream)),
                };

                info!(
                    "GHJ#{}: optimistic buffer fallback — partitioning {} buffered batches. \
                     pool reserved={}",
                    ghj_id,
                    buffered_batches.len(),
                    context.runtime_env().memory_pool.reserved(),
                );

                // Free reservation for buffered batches; partition_from_buffer
                // and partition_build_side will re-track per-partition memory.
                reservation.free();

                let mut partitions: Vec<HashPartition> =
                    (0..num_partitions).map(|_| HashPartition::new()).collect();
                let mut scratch = ScratchSpace::default();

                {
                    let _timer = metrics.build_time.timer();
                    partition_from_buffer(
                        buffered_batches,
                        &build_keys,
                        num_partitions,
                        &build_schema,
                        &mut partitions,
                        &mut reservation,
                        &context,
                        &metrics,
                        &mut scratch,
                    )?;

                    // Continue reading remaining stream if optimistic buffer was interrupted
                    if let Some(remaining) = remaining_stream {
                        partition_build_side(
                            remaining,
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
                }

                return execute_slow_path(
                    ghj_id,
                    partitions,
                    probe_stream,
                    probe_keys,
                    original_on,
                    filter,
                    join_type,
                    num_partitions,
                    build_left,
                    max_concurrent_partitions,
                    build_schema,
                    probe_schema,
                    context,
                    metrics,
                    reservation,
                    scratch,
                )
                .await
                .map(|s| s.boxed());
            }
        }
    }

    // Non-optimistic path: fast_path_threshold == 0 (disabled).
    // Always partition the build side.
    let mut partitions: Vec<HashPartition> =
        (0..num_partitions).map(|_| HashPartition::new()).collect();
    let mut scratch = ScratchSpace::default();

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

    execute_slow_path(
        ghj_id,
        partitions,
        probe_stream,
        probe_keys,
        original_on,
        filter,
        join_type,
        num_partitions,
        build_left,
        max_concurrent_partitions,
        build_schema,
        probe_schema,
        context,
        metrics,
        reservation,
        scratch,
    )
    .await
    .map(|s| s.boxed())
}

/// Execute the slow path: partition probe side, merge partitions, and join.
#[allow(clippy::too_many_arguments)]
async fn execute_slow_path(
    ghj_id: usize,
    mut partitions: Vec<HashPartition>,
    probe_stream: SendableRecordBatchStream,
    probe_keys: Vec<Arc<dyn PhysicalExpr>>,
    original_on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    num_partitions: usize,
    build_left: bool,
    max_concurrent_partitions: usize,
    build_schema: SchemaRef,
    probe_schema: SchemaRef,
    context: Arc<TaskContext>,
    metrics: GraceHashJoinMetrics,
    mut reservation: MemoryReservation,
    mut scratch: ScratchSpace,
) -> DFResult<impl Stream<Item = DFResult<RecordBatch>>> {
    let build_spilled = partitions.iter().any(|p| p.build_spilled());
    let actual_build_bytes: usize = partitions
        .iter()
        .flat_map(|p| p.build_batches.iter())
        .map(|b| b.get_array_memory_size())
        .sum();
    let total_build_rows: usize = partitions
        .iter()
        .flat_map(|p| p.build_batches.iter())
        .map(|b| b.num_rows())
        .sum();
    info!(
        "GHJ#{}: slow path — build spilled={}, {} rows, {} bytes (actual). \
         join_type={:?}, build_left={}. pool reserved={}. Partitioning probe side.",
        ghj_id,
        build_spilled,
        total_build_rows,
        actual_build_bytes,
        join_type,
        build_left,
        context.runtime_env().memory_pool.reserved(),
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
            "GHJ#{}: probe phase complete. \
             total probe (in-memory): {} rows, {} bytes, {} spilled. \
             reservation={}, pool reserved={}",
            ghj_id,
            total_probe_rows,
            total_probe_bytes,
            probe_spilled,
            reservation.size(),
            context.runtime_env().memory_pool.reserved(),
        );
    }

    // Finish all open spill writers before reading back
    let finished_partitions = finish_spill_writers(partitions)?;

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
    info!(
        "GHJ#{}: freeing reservation ({} bytes) before Phase 3. pool reserved={}",
        ghj_id,
        reservation.size(),
        context.runtime_env().memory_pool.reserved(),
    );
    reservation.free();

    // Phase 3: Join partitions with bounded concurrency. Keeping this low
    // avoids creating many simultaneous HashJoinInput reservations per task
    // when multiple Spark tasks share the same memory pool.
    let max_concurrent_partitions = max_concurrent_partitions.max(1);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_partitions));
    let (tx, rx) = mpsc::channel::<DFResult<RecordBatch>>(max_concurrent_partitions * 2);

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
    let output_batch_count = metrics.output_batches.clone();
    let join_time = metrics.join_time.clone();
    let output_row_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&output_row_count);
    let jt = join_type;
    let bl = build_left;
    let result_stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|batch| (batch, rx))
    })
    .inspect_ok(move |batch| {
        let _timer = join_time.timer();
        output_metrics.record_output(batch.num_rows());
        output_batch_count.add(1);
        let prev = counter.fetch_add(batch.num_rows(), std::sync::atomic::Ordering::Relaxed);
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

// ---------------------------------------------------------------------------
// HashJoinExec helpers
// ---------------------------------------------------------------------------

/// Create and execute a HashJoinExec, handling build_left swap logic.
///
/// When `build_left` is true, the left source is the build side and CollectLeft
/// mode works directly. When `build_left` is false, we create the join with
/// the original left/right order then swap inputs so the right side is collected.
fn execute_hash_join(
    left_source: Arc<dyn ExecutionPlan>,
    right_source: Arc<dyn ExecutionPlan>,
    original_on: JoinOnRef<'_>,
    filter: &Option<JoinFilter>,
    join_type: &JoinType,
    build_left: bool,
    context: &Arc<TaskContext>,
) -> DFResult<SendableRecordBatchStream> {
    if build_left {
        let hash_join = HashJoinExec::try_new(
            left_source,
            right_source,
            original_on.to_vec(),
            filter.clone(),
            join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        hash_join.execute(0, context_for_join_output(context))
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
            false,
        )?);
        let swapped = hash_join.swap_inputs(PartitionMode::CollectLeft)?;
        swapped.execute(0, context_for_join_output(context))
    }
}

/// Create the fast-path HashJoinExec stream (no partitioning, no spilling).
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn create_fast_path_stream(
    build_data: Vec<RecordBatch>,
    probe_stream: SendableRecordBatchStream,
    original_on: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
    filter: &Option<JoinFilter>,
    join_type: &JoinType,
    build_left: bool,
    build_schema: &SchemaRef,
    probe_schema: &SchemaRef,
    context: &Arc<TaskContext>,
) -> DFResult<SendableRecordBatchStream> {
    let build_source = memory_source_exec(build_data, build_schema)?;
    let probe_source: Arc<dyn ExecutionPlan> = Arc::new(StreamSourceExec::new(
        probe_stream,
        Arc::clone(probe_schema),
    ));

    let (left_source, right_source): (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) =
        if build_left {
            (build_source, probe_source)
        } else {
            (probe_source, build_source)
        };

    execute_hash_join(
        left_source,
        right_source,
        original_on,
        filter,
        join_type,
        build_left,
        context,
    )
}

/// Create a TaskContext with a larger output batch size for HashJoinExec.
///
/// Input splitting is handled by StreamSourceExec (not batch_size).
fn context_for_join_output(context: &Arc<TaskContext>) -> Arc<TaskContext> {
    let batch_size = GHJ_OUTPUT_BATCH_SIZE.max(context.session_config().batch_size());
    Arc::new(TaskContext::new(
        context.task_id(),
        context.session_id(),
        context.session_config().clone().with_batch_size(batch_size),
        context.scalar_functions().clone(),
        context.aggregate_functions().clone(),
        context.window_functions().clone(),
        context.runtime_env(),
    ))
}

/// Create a `StreamSourceExec` that yields `data` batches without splitting.
///
/// Unlike `DataSourceExec(MemorySourceConfig)`, `StreamSourceExec` does NOT
/// wrap its output in `BatchSplitStream`. This is critical for the build side
/// because Arrow's zero-copy `batch.slice()` shares underlying buffers, so
/// `get_record_batch_memory_size()` reports the full buffer size for every
/// slice — causing `collect_left_input` to vastly over-count memory and
/// trigger spurious OOM. Additionally, using `batch_size` large enough to
/// prevent splitting can cause Arrow i32 offset overflow for string columns.
fn memory_source_exec(
    data: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let schema_clone = Arc::clone(schema);
    let stream =
        RecordBatchStreamAdapter::new(Arc::clone(schema), stream::iter(data.into_iter().map(Ok)));
    Ok(Arc::new(StreamSourceExec::new(
        Box::pin(stream),
        schema_clone,
    )))
}

// ---------------------------------------------------------------------------
// Phase 3: Per-partition joins (with optional recursive repartitioning)
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
        let spill_files = partition.build_spill_files;
        let spilled = tokio::task::spawn_blocking(move || {
            let mut all = Vec::new();
            for spill_file in &spill_files {
                all.extend(read_spilled_batches(spill_file)?);
            }
            Ok::<_, DataFusionError>(all)
        })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("GraceHashJoin: build spill read task failed: {e}"))
        })??;
        build_batches.extend(spilled);
    }

    // Coalesce many tiny sub-batches into single batches to reduce per-batch
    // overhead in HashJoinExec. Per-partition data is bounded by
    // TARGET_PARTITION_BUILD_SIZE so concat won't hit i32 offset overflow.
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
        // Probe side is in-memory: coalesce before joining
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
            probe_batches.extend(read_spilled_batches(spill_file)?);
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

    // Concatenate build side into single batch. Per-partition data is bounded
    // by TARGET_PARTITION_BUILD_SIZE so this won't hit i32 offset overflow.
    let build_data = if build_batches.is_empty() {
        vec![RecordBatch::new_empty(Arc::clone(build_schema))]
    } else if build_batches.len() == 1 {
        build_batches
    } else {
        vec![concat_batches(build_schema, &build_batches)?]
    };

    // Build side: StreamSourceExec to avoid BatchSplitStream splitting
    let build_source = memory_source_exec(build_data, build_schema)?;

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
                probe_batches.extend(read_spilled_batches(spill_file)?);
            }
            let probe_data = if probe_batches.is_empty() {
                vec![RecordBatch::new_empty(Arc::clone(probe_schema))]
            } else {
                vec![concat_batches(probe_schema, &probe_batches)?]
            };
            memory_source_exec(probe_data, probe_schema)?
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
        if probe_spill_files_count == 1 {
            "SpillReaderExec"
        } else {
            "StreamSourceExec"
        },
    );

    let stream = execute_hash_join(
        left_source,
        right_source,
        original_on,
        filter,
        join_type,
        build_left,
        context,
    )?;

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

    // Concatenate sub-batches into single batches to reduce per-batch overhead
    // in HashJoinExec. Per-partition data is bounded by TARGET_PARTITION_BUILD_SIZE
    // so this won't hit i32 offset overflow.
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
    // Both sides use StreamSourceExec to avoid DataSourceExec's BatchSplitStream.
    let build_source = memory_source_exec(build_data, build_schema)?;
    let probe_source = memory_source_exec(probe_data, probe_schema)?;

    let (left_source, right_source) = if build_left {
        (build_source, probe_source)
    } else {
        (probe_source, build_source)
    };

    info!(
        "GraceHashJoin: RECURSIVE PATH creating HashJoinExec at level={}, \
         build_left={}, build_size={}, probe_size={}, pool reserved={}",
        recursion_level,
        build_left,
        build_size,
        probe_size,
        context.runtime_env().memory_pool.reserved(),
    );

    let stream = execute_hash_join(
        left_source,
        right_source,
        original_on,
        filter,
        join_type,
        build_left,
        context,
    )?;

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

    let build_sub = sub_partition_batches(
        &build_batches,
        &build_keys,
        num_sub_partitions,
        recursion_level,
        &mut scratch,
    )?;
    let probe_sub = sub_partition_batches(
        &probe_batches,
        &probe_keys,
        num_sub_partitions,
        recursion_level,
        &mut scratch,
    )?;

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
