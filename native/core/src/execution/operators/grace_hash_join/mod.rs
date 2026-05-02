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
//!
//! Module layout:
//! - [`exec`]: execution flow (fast path, slow path, per-partition joins).
//! - [`partition`]: prefix-sum hash partitioning, Phase 1/2 streaming
//!   partitioners, spill bookkeeping, merge of adjacent partitions.
//! - [`spill`]: Arrow IPC spill writer/reader and stream-source wrappers.
//! - [`metrics`]: operator metrics.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::{JoinType, NullEquality, Result as DFResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::stream::TryStreamExt;
use log::info;

mod exec;
mod metrics;
mod partition;
mod spill;

use exec::execute_grace_hash_join;
use metrics::GraceHashJoinMetrics;

/// Type alias for join key expression pairs.
pub(crate) type JoinOnRef<'a> = &'a [(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)];

/// Number of partitions (buckets) for the grace hash join.
pub(crate) const DEFAULT_NUM_PARTITIONS: usize = 16;

/// Maximum recursion depth for repartitioning oversized partitions.
/// At depth 3 with 16 partitions per level, effective partitions = 16^3 = 4096.
pub(crate) const MAX_RECURSION_DEPTH: usize = 3;

/// I/O buffer size for spill file reads and writes. The default BufReader/BufWriter
/// size (8 KB) is far too small for multi-GB spill files. 1 MB provides good
/// sequential throughput while keeping per-partition memory overhead modest.
pub(crate) const SPILL_IO_BUFFER_SIZE: usize = 1024 * 1024;

/// Log progress every N probe rows accumulated.
pub(crate) const PROBE_PROGRESS_MILESTONE_ROWS: usize = 5_000_000;

/// Target number of rows per coalesced batch when reading spill files.
/// Spill files contain many tiny sub-batches (from partitioning). Coalescing
/// into larger batches reduces per-batch overhead in the hash join kernel
/// and channel send/recv costs.
pub(crate) const SPILL_READ_COALESCE_TARGET: usize = 8192;

/// Target build-side size per merged partition. After Phase 2, adjacent
/// `FinishedPartition`s are merged so each group has roughly this much
/// build data, reducing the number of per-partition HashJoinExec calls.
pub(crate) const TARGET_PARTITION_BUILD_SIZE: usize = 32 * 1024 * 1024;

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
    /// Maximum number of partitions to join concurrently in Phase 3
    max_concurrent_partitions: usize,
    /// Output schema
    schema: SchemaRef,
    /// Plan properties cache
    cache: Arc<PlanProperties>,
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
        max_concurrent_partitions: usize,
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
            false,
        )?;
        let (schema, cache) = if build_left {
            (hash_join.schema(), Arc::clone(hash_join.properties()))
        } else {
            // Swap to get correct output schema for build-right
            let swapped = hash_join.swap_inputs(PartitionMode::CollectLeft)?;
            (swapped.schema(), Arc::clone(swapped.properties()))
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
            max_concurrent_partitions: max_concurrent_partitions.max(1),
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
            self.max_concurrent_partitions,
        )?))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
            self.build_left,
            self.join_type,
            self.num_partitions,
            self.fast_path_threshold,
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
        let max_concurrent_partitions = self.max_concurrent_partitions;

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
                max_concurrent_partitions,
                build_schema,
                probe_schema,
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
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
            2,                // max_concurrent_partitions
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
            2,                // max_concurrent_partitions
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
        let build_bytes: usize = left_batches.iter().map(|b| b.get_array_memory_size()).sum();
        eprintln!(
            "Test build side: {} bytes ({} MB)",
            build_bytes,
            build_bytes / (1024 * 1024)
        );

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
            2,    // max_concurrent_partitions
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
            2,     // max_concurrent_partitions
        )?;

        let stream = grace_join.execute(0, task_ctx)?;
        let result_batches: Vec<RecordBatch> = stream.try_collect().await?;

        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10_000, "Expected 10000 matching rows");

        Ok(())
    }
}
