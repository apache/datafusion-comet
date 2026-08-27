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

//! Streaming top-K per partition operator for Spark's `WindowGroupLimitExec`.
//!
//! The child stream must be sorted by `[partition_keys..., order_keys...]`.
//! Spark's `WindowGroupLimitExec.requiredChildOrdering` guarantees this via
//! `EnsureRequirements`; the operator relies on the injected sort so a single
//! streaming pass decides emit-or-drop per row. Tie behavior matches Spark's
//! `RankLimitIterator` / `SimpleLimitIterator` exactly.
//!
//! ROW_NUMBER without PARTITION BY is served by a plain `LocalLimitExec` in the
//! planner and never reaches this operator.

use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, BooleanArray, BooleanBufferBuilder, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{
    LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    apply_expression_roots, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFnKind {
    RowNumber,
    Rank,
    DenseRank,
}

#[derive(Debug)]
pub struct PartitionedRankLimitExec {
    input: Arc<dyn ExecutionPlan>,
    /// PARTITION BY expressions. Empty means "no PARTITION BY" (global top-K
    /// within each input DataFusion partition).
    partition_keys: Vec<PhysicalSortExpr>,
    /// ORDER BY expressions. Empty means "no ORDER BY" and every row within a
    /// partition ties.
    order_keys: Vec<PhysicalSortExpr>,
    fetch: usize,
    kind: WindowFnKind,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl PartitionedRankLimitExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_keys: Vec<PhysicalSortExpr>,
        order_keys: Vec<PhysicalSortExpr>,
        fetch: usize,
        kind: WindowFnKind,
    ) -> Result<Self> {
        let cache = Arc::new(Self::compute_properties(
            &input,
            &partition_keys,
            &order_keys,
        )?);
        Ok(Self {
            input,
            partition_keys,
            order_keys,
            fetch,
            kind,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        partition_keys: &[PhysicalSortExpr],
        order_keys: &[PhysicalSortExpr],
    ) -> Result<PlanProperties> {
        let mut eq_properties = input.equivalence_properties().clone();
        if let Some(ordering) = full_ordering(partition_keys, order_keys) {
            eq_properties.reorder(ordering)?;
        }
        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

/// `[partition_keys..., order_keys...]` as a single `LexOrdering`, or `None` when both lists
/// are empty. Dedup by `LexOrdering::new` is fine here because this ordering is only used to
/// declare equivalence properties and the input-ordering requirement; the streaming operator
/// itself operates on the un-deduped `partition_keys` / `order_keys` slices so a duplicate
/// (e.g. `PARTITION BY a, a`) never turns into an internal error.
fn full_ordering(
    partition_keys: &[PhysicalSortExpr],
    order_keys: &[PhysicalSortExpr],
) -> Option<LexOrdering> {
    let sort_exprs: Vec<PhysicalSortExpr> = partition_keys
        .iter()
        .chain(order_keys.iter())
        .cloned()
        .collect();
    LexOrdering::new(sort_exprs)
}

impl DisplayAs for PartitionedRankLimitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition = self
                    .partition_keys
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let order = self
                    .order_keys
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "CometPartitionedRankLimitExec: kind={:?}, fetch={}, partition_by=[{}], order_by=[{}]",
                    self.kind, self.fetch, partition, order
                )
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for PartitionedRankLimitExec {
    fn name(&self) -> &str {
        "CometPartitionedRankLimitExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(
            self.partition_keys
                .iter()
                .chain(self.order_keys.iter())
                .map(|sort_expr| &sort_expr.expr),
            f,
        )
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(PartitionedRankLimitExec::try_new(
            Arc::clone(&children[0]),
            self.partition_keys.clone(),
            self.order_keys.clone(),
            self.fetch,
            self.kind,
        )?))
    }

    // The operator's correctness depends on the input being sorted by
    // `[partition_keys..., order_keys...]`. Spark's Catalyst injects the required sort above
    // `WindowGroupLimitExec`, and Comet executes the deserialized plan directly without
    // running any DataFusion physical optimizer pass, so this method is informational: it
    // documents the ordering contract and shows up in `DisplayableExecutionPlan` output. It
    // is not a safety net -- if the sort is missing upstream, results are wrong.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![full_ordering(&self.partition_keys, &self.order_keys).map(OrderingRequirements::from)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();

        let partition_key = build_key_encoder(&self.partition_keys, &schema)?;

        // ROW_NUMBER's rank formula is just the running count, so it never reads the
        // ORDER BY key. Skip building the converter and evaluating order columns.
        // For RANK/DENSE_RANK, the encoder drives tie detection on the ORDER BY suffix.
        // When the suffix is empty (query has no ORDER BY) `build_key_encoder` returns
        // `None` and every row within a partition ties.
        let order_key = if self.kind == WindowFnKind::RowNumber {
            None
        } else {
            build_key_encoder(&self.order_keys, &schema)?
        };

        Ok(Box::pin(RankLimitStream {
            input,
            schema,
            partition_key,
            order_key,
            limit: self.fetch as u64,
            kind: self.kind,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            prev_partition: None,
            prev_order: None,
            rank: 0,
            count: 0,
            partition_exhausted: false,
        }))
    }
}

/// Row-encoded key for either PARTITION BY or ORDER BY columns. Only constructed
/// when the corresponding expression list is non-empty.
struct KeyEncoder {
    converter: RowConverter,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl KeyEncoder {
    fn encode(&self, batch: &RecordBatch) -> Result<Rows> {
        let num_rows = batch.num_rows();
        let cols: Vec<ArrayRef> = self
            .exprs
            .iter()
            .map(|e| e.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;
        Ok(self.converter.convert_columns(&cols)?)
    }
}

fn build_key_encoder(exprs: &[PhysicalSortExpr], schema: &SchemaRef) -> Result<Option<KeyEncoder>> {
    if exprs.is_empty() {
        return Ok(None);
    }
    let sort_fields = build_sort_fields(exprs, schema)?;
    let converter = RowConverter::new(sort_fields)?;
    let exprs = exprs.iter().map(|e| Arc::clone(&e.expr)).collect();
    Ok(Some(KeyEncoder { converter, exprs }))
}

fn build_sort_fields(ordering: &[PhysicalSortExpr], schema: &SchemaRef) -> Result<Vec<SortField>> {
    ordering
        .iter()
        .map(|e| {
            Ok(SortField::new_with_options(
                e.expr.data_type(schema)?,
                e.options,
            ))
        })
        .collect()
}

struct RankLimitStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    /// `None` when there is no PARTITION BY (global top-K per DF input partition).
    partition_key: Option<KeyEncoder>,
    /// `None` when there is no ORDER BY (every row within a partition ties), and
    /// always `None` for ROW_NUMBER (rank formula never reads order keys).
    order_key: Option<KeyEncoder>,
    limit: u64,
    kind: WindowFnKind,
    baseline_metrics: BaselineMetrics,

    // Per-partition streaming state, persisted across batches.
    prev_partition: Option<OwnedRow>,
    prev_order: Option<OwnedRow>,
    /// Rank of the most recently seen row (0-indexed). Only meaningful when
    /// `prev_order.is_some()` -- the two reads below both sit past the point where
    /// `prev_order` was set for the current partition.
    rank: u64,
    /// 0-indexed cursor into the current partition for rank arithmetic. Advances only on
    /// non-exhausted rows -- once `partition_exhausted` fires, subsequent rows in the same
    /// partition skip the increment. `count` is thus NOT rows-seen; it freezes at
    /// `first_dropped_at` for the tail of an exhausted partition.
    count: u64,
    /// Set once `this_rank >= limit` inside the current partition and cleared when a new
    /// partition starts. Mirrors Spark's `GroupedLimitIterator.skipRemainingRows`: for a
    /// partition already past the limit we skip order-key encoding, tie detection, and
    /// rank arithmetic on the remaining rows.
    partition_exhausted: bool,
}

impl RankLimitStream {
    /// Filter a batch to the rows this operator keeps. `Ok(None)` means the batch produced no
    /// output; the caller must not surface it downstream. Passing an empty batch also returns
    /// `Ok(None)` (nothing to emit) rather than an empty pass-through, so the caller never
    /// has to strip zero-row batches.
    fn process_batch(&mut self, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(None);
        }

        let partition_rows = self
            .partition_key
            .as_ref()
            .map(|k| k.encode(batch))
            .transpose()?;
        // Lazily encoded: skipped entirely for a batch that is wholly inside an already-
        // exhausted partition, so a giant skewed partition after the limit costs O(rows)
        // partition-key checks instead of O(rows) full row encodings.
        let mut order_rows: Option<Rows> = None;

        let mut mask_builder = BooleanBufferBuilder::new(num_rows);
        let mut kept: usize = 0;
        // Position of the first dropped row in this batch, recorded by BOTH drop paths (the
        // exhausted-partition fast drop inside the loop and the rank-based drop below). When
        // `kept == first_dropped_at`, every kept row lies at positions `0..kept`, so the
        // output is `batch.slice(0, kept)` -- one `Arc` reslice per column, no bitmap scan or
        // per-column filter kernel.
        let mut first_dropped_at: Option<usize> = None;
        for i in 0..num_rows {
            // Only a PARTITION-BY-shaped stream has partition boundaries. With no
            // PARTITION BY the whole stream is one partition, so state accumulates
            // across every row and no reset is needed.
            if let Some(pr) = &partition_rows {
                let same_partition =
                    matches!(&self.prev_partition, Some(prev) if prev.row() == pr.row(i));
                if !same_partition {
                    self.prev_partition = Some(pr.row(i).owned());
                    self.prev_order = None;
                    self.rank = 0;
                    self.count = 0;
                    self.partition_exhausted = false;
                }
            }

            if self.partition_exhausted {
                // Record the first drop here too. A batch can open inside a partition the
                // previous batch already exhausted, dropping these leading rows before the
                // rank-based drop below ever runs. The clean-prefix shortcut keys off
                // `first_dropped_at`, so leaving it unset here lets a later partition's kept
                // row at position `kept` fire `batch.slice(0, kept)` and lose the kept tail
                // (see the `exhausted_partition_prefix_then_fresh_partitions` test).
                first_dropped_at.get_or_insert(i);
                mask_builder.append(false);
                continue;
            }

            if order_rows.is_none() {
                if let Some(k) = self.order_key.as_ref() {
                    order_rows = Some(k.encode(batch)?);
                }
            }

            // Whether this row's ORDER BY key ties with the previous emitted row. `false`
            // on the first row of a partition and (vacuously) when there is no ORDER BY --
            // `prev_order` stays `None` across the whole partition in that case.
            let ties_with_prev = matches!(
                (&self.prev_order, &order_rows),
                (Some(prev_o), Some(rows)) if prev_o.row() == rows.row(i)
            );

            let this_rank: u64 = match self.kind {
                WindowFnKind::RowNumber => self.count,
                _ if ties_with_prev => self.rank,
                WindowFnKind::Rank => self.count,
                WindowFnKind::DenseRank if self.prev_order.is_none() => 0,
                WindowFnKind::DenseRank => self.rank + 1,
            };

            let keep = this_rank < self.limit;
            mask_builder.append(keep);
            if keep {
                kept += 1;
            } else {
                first_dropped_at.get_or_insert(i);
                // `this_rank` is monotonically nondecreasing within a partition for all three
                // kinds (ROW_NUMBER: strictly, RANK / DENSE_RANK: nondecreasing), so once
                // `keep` flips false every remaining row of this partition is dropped.
                self.partition_exhausted = true;
            }

            self.rank = this_rank;
            // Only clone into an `OwnedRow` when the key actually changed. Under
            // RANK/DENSE_RANK the common tail of a partition is a run of ties,
            // so this avoids O(rows) heap allocations there.
            if let Some(rows) = &order_rows {
                if !ties_with_prev {
                    self.prev_order = Some(rows.row(i).owned());
                }
            }
            self.count += 1;
        }

        if kept == num_rows {
            return Ok(Some(batch.clone()));
        }
        if kept == 0 {
            return Ok(None);
        }
        // Clean prefix: kept rows are `0..kept`, dropped rows are `kept..num_rows`. `slice` is
        // O(1) per column (`Arc` reslice), skipping the bitmap `true_count` and the per-column
        // filter kernel that `filter_record_batch` would run.
        if first_dropped_at == Some(kept) {
            return Ok(Some(batch.slice(0, kept)));
        }
        let mask = BooleanArray::new(mask_builder.finish(), None);
        Ok(Some(filter_record_batch(batch, &mask)?))
    }
}

impl Stream for RankLimitStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // With no PARTITION BY, `partition_exhausted` never clears -- one virtual
            // partition per DF partition. Terminate early instead of pulling the rest
            // of the child stream just to drop it (matches how `LocalLimitExec` bails
            // once it's satisfied its fetch).
            if self.partition_exhausted && self.partition_key.is_none() {
                return Poll::Ready(None);
            }
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    // Clone the `Time` metric into a local so the `ScopedTimerGuard` borrows
                    // the local rather than `self.baseline_metrics`. Otherwise the guard would
                    // hold an immutable borrow of `self` for the duration of the block and
                    // `self.process_batch(&batch)` (which needs `&mut self`) would fail with
                    // E0502.
                    let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                    let processed = {
                        let _timer = elapsed_compute.timer();
                        self.process_batch(&batch)
                    };
                    match processed {
                        // `process_batch` returns `None` when the batch produces no output,
                        // so downstream never sees a spurious empty batch between real ones.
                        Ok(None) => continue,
                        Ok(Some(out)) => {
                            return self
                                .baseline_metrics
                                .record_poll(Poll::Ready(Some(Ok(out))));
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for RankLimitStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    //! Multi-batch state-persistence tests for `RankLimitStream`. The SQL fixtures under
    //! `spark/src/test/resources/sql-tests/windows/window_group_limit_*.sql` are single-batch
    //! sized under the default `COMET_BATCH_SIZE`, so cross-`poll_next` carry of
    //! `prev_partition`, `prev_order`, `rank`, `count`, and `partition_exhausted` is only
    //! covered here.

    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("ord", DataType::Int64, false),
        ]))
    }

    fn batch(part: Vec<i32>, ord: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(part)) as ArrayRef,
                Arc::new(Int64Array::from(ord)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    /// `PARTITION BY part ORDER BY ord ASC`.
    fn keys() -> (Vec<PhysicalSortExpr>, Vec<PhysicalSortExpr>) {
        let partition = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("part", 0)) as Arc<dyn PhysicalExpr>,
            options: SortOptions::default(),
        }];
        let order = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("ord", 1)) as Arc<dyn PhysicalExpr>,
            options: SortOptions::default(),
        }];
        (partition, order)
    }

    async fn run(batches: Vec<RecordBatch>, fetch: usize, kind: WindowFnKind) -> Vec<RecordBatch> {
        let (partition_keys, order_keys) = keys();
        let input = MemorySourceConfig::try_new_exec(&[batches], schema(), None).unwrap();
        let plan = Arc::new(
            PartitionedRankLimitExec::try_new(input, partition_keys, order_keys, fetch, kind)
                .unwrap(),
        );
        collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap()
    }

    fn flatten(batches: &[RecordBatch]) -> Vec<(i32, i64)> {
        let mut out = vec![];
        for b in batches {
            let p = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let o = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push((p.value(i), o.value(i)));
            }
        }
        out
    }

    /// Batch 0 ends exactly on the last row of partition 1; batch 1 opens partition 2. Verifies
    /// the reset branch fires cleanly at a batch boundary.
    #[tokio::test]
    async fn partition_boundary_aligned_with_batch_boundary() {
        let b0 = batch(vec![1, 1, 1], vec![10, 20, 30]);
        let b1 = batch(vec![2, 2, 2], vec![40, 50, 60]);
        let out = run(vec![b0, b1], 2, WindowFnKind::RowNumber).await;
        assert_eq!(flatten(&out), vec![(1, 10), (1, 20), (2, 40), (2, 50)]);
    }

    /// Partition 1 hits the limit inside batch 0 (extra rows must be dropped); batch 1 opens
    /// partition 2 whose first row must not inherit partition 1's `rank` / `count`.
    #[tokio::test]
    async fn limit_hit_mid_batch_new_partition_next_batch() {
        let b0 = batch(vec![1, 1, 1, 1], vec![10, 20, 30, 40]);
        let b1 = batch(vec![2, 2], vec![50, 60]);
        let out = run(vec![b0, b1], 2, WindowFnKind::RowNumber).await;
        assert_eq!(flatten(&out), vec![(1, 10), (1, 20), (2, 50), (2, 60)]);
    }

    /// An empty batch in the middle of the stream must not advance state and must not surface
    /// downstream as a zero-row output batch.
    #[tokio::test]
    async fn empty_batch_between_real_batches() {
        let b0 = batch(vec![1, 1], vec![10, 20]);
        let empty = RecordBatch::new_empty(schema());
        let b1 = batch(vec![1, 1], vec![30, 40]);
        let out = run(vec![b0, empty, b1], 3, WindowFnKind::RowNumber).await;
        assert!(out.iter().all(|b| b.num_rows() > 0));
        assert_eq!(flatten(&out), vec![(1, 10), (1, 20), (1, 30)]);
    }

    /// A RANK tie run straddles the batch boundary. `prev_order` must survive across
    /// `poll_next` so tied rows keep the same rank as the last row of the prior batch. The
    /// second sub-case breaks the tie on the last row of batch 1 with `fetch = 1`: the broken
    /// tie ranks 1 (>= fetch) and must be dropped.
    #[tokio::test]
    async fn rank_tie_run_spans_batch_boundary() {
        let out = run(
            vec![
                batch(vec![1, 1], vec![10, 10]),
                batch(vec![1, 1], vec![10, 10]),
            ],
            1,
            WindowFnKind::Rank,
        )
        .await;
        assert_eq!(flatten(&out).len(), 4);

        let out = run(
            vec![
                batch(vec![1, 1], vec![10, 10]),
                batch(vec![1, 1], vec![10, 20]),
            ],
            1,
            WindowFnKind::Rank,
        )
        .await;
        assert_eq!(flatten(&out), vec![(1, 10), (1, 10), (1, 10)]);
    }

    /// DENSE_RANK must not skip a rank across a batch-boundary tie run. Two rows tie at rank 1
    /// in batch 0, batch 1 opens a distinct value that must rank 2 (not 3).
    #[tokio::test]
    async fn dense_rank_no_skip_across_batch_boundary() {
        let out = run(
            vec![
                batch(vec![1, 1], vec![10, 10]),
                batch(vec![1, 1], vec![20, 30]),
            ],
            2,
            WindowFnKind::DenseRank,
        )
        .await;
        assert_eq!(flatten(&out), vec![(1, 10), (1, 10), (1, 20)]);
    }

    /// A batch that lands entirely inside an already-exhausted partition must not encode
    /// order rows. This test only pins the visible behavior (all rows dropped); the cost win
    /// is covered by the fact that `order_key.encode` is not called on the fast path.
    #[tokio::test]
    async fn batch_entirely_inside_exhausted_partition_is_dropped() {
        let b0 = batch(vec![1, 1, 1], vec![10, 20, 30]);
        // Batch 1 stays inside partition 1 after the limit is hit at rank 1 in batch 0.
        let b1 = batch(vec![1, 1, 1], vec![40, 50, 60]);
        let out = run(vec![b0, b1], 1, WindowFnKind::RowNumber).await;
        assert_eq!(flatten(&out), vec![(1, 10)]);
    }

    /// Regression for the clean-prefix shortcut mis-firing after an exhausted-partition
    /// prefix. Batch 1 opens with the tail of the partition batch 0 exhausted (dropped), then
    /// crosses into two fresh partitions whose top rows must be kept. The mask is
    /// `[false, true, false, true]`. If the exhausted-partition branch does not record the
    /// leading drops, `first_dropped_at` stays at the rank-based drop (position 2) and equals
    /// `kept` (2), so the shortcut returns `batch.slice(0, 2)` -- keeping `(1,50)` and losing
    /// partition 3's top row `(3,10)`. Covered for all three window kinds; the ORDER BY values
    /// are distinct so RANK, DENSE_RANK, and ROW_NUMBER all yield the same top-1-per-partition.
    #[tokio::test]
    async fn exhausted_partition_prefix_then_fresh_partitions() {
        let b0 = batch(vec![1, 1, 1, 1], vec![10, 20, 30, 40]);
        let b1 = batch(vec![1, 2, 2, 3], vec![50, 10, 20, 10]);
        for kind in [
            WindowFnKind::RowNumber,
            WindowFnKind::Rank,
            WindowFnKind::DenseRank,
        ] {
            let out = run(vec![b0.clone(), b1.clone()], 1, kind).await;
            assert_eq!(
                flatten(&out),
                vec![(1, 10), (2, 10), (3, 10)],
                "kind={kind:?}"
            );
        }
    }
}
