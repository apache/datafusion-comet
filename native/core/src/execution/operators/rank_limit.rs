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
//! Comet routes both `RANK()` and the degenerate `ROW_NUMBER()` cases (empty
//! effective ORDER BY suffix) to this operator. It relies on the fact that
//! Spark's `WindowGroupLimitExec.requiredChildOrdering` guarantees the child is
//! already sorted by `[partition_keys..., order_keys...]`, so a single
//! streaming pass decides, row-by-row, whether to emit or drop. Tie behavior
//! matches Spark's `RankLimitIterator` / `SimpleLimitIterator` exactly.
//!
//! For the common non-degenerate `ROW_NUMBER()` case (non-empty PARTITION BY
//! AND non-empty ORDER BY that isn't fully covered by PARTITION BY), the
//! planner still routes to DataFusion's heap-based `PartitionedTopKExec` which
//! is O(N log K) and doesn't require sorted input.

use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{LexOrdering, LexRequirement, OrderingRequirements, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

/// Which rank-like window function drives the top-K semantics.
///
/// * [`RowNumber`](Self::RowNumber): each row within a partition gets a unique
///   monotonic 1-indexed rank (0-indexed internally). No tie handling; first K
///   rows are kept.
/// * [`Rank`](Self::Rank): rows tied on ORDER BY share the same rank, then rank
///   jumps to the running count. All rows with rank <= K are kept, which can
///   emit more than K rows when ties straddle the K-th boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFnKind {
    RowNumber,
    Rank,
}

/// Streaming top-K per partition operator matching Spark's
/// `WindowGroupLimitExec`.
///
/// # Preconditions
/// The child stream MUST be sorted by `[partition_keys..., order_keys...]`.
/// Spark's `WindowGroupLimitExec.requiredChildOrdering` guarantees this via
/// `EnsureRequirements`, so Comet's serde relies on the injected sort rather
/// than declaring a hard ordering requirement here that would trigger a second
/// DataFusion-side sort.
///
/// When `partition_prefix_len == 0` there is no PARTITION BY (global top-K):
/// state accumulates across all rows in the DF partition and never resets,
/// so each DF input partition produces its own local top-K (matching
/// Spark's Partial WGL semantics; the Final WGL then runs on the merged
/// single-partition stream downstream).
///
/// When `partition_prefix_len == expr.len()` the ORDER BY suffix is empty
/// (either the query has no ORDER BY, or every ORDER BY column was
/// deduplicated with a PARTITION BY column by `LexOrdering::new`). In that
/// case:
///   * `Rank`: every row within a partition ties at rank 0. All rows are
///     emitted when `limit >= 1`.
///   * `RowNumber`: rank still increments per row, so the first `limit` rows
///     per partition are emitted (matching Spark's `SimpleLimitIterator`).
///
/// # Semantics
/// Row-by-row, we maintain per-partition state: `rank` (0-indexed rank of the
/// last emitted row) and `count` (total rows seen in the partition). On each
/// input row:
///   * Reset state on a partition-key change.
///   * Compute `this_rank` according to [`WindowFnKind`]:
///     * `RowNumber`: `this_rank = count` (each row's own position).
///     * `Rank`: `0` for the first row of a partition; `rank` if the ORDER BY
///       key matches the previous row (or there is no ORDER BY suffix), else
///       `count`.
///   * Emit iff `this_rank < limit`.
///
/// Consequence for `Rank`: all rows tied at the K-th ORDER BY value ARE emitted
/// — matches Spark's `RankLimitIterator` exactly.
#[derive(Debug)]
pub struct PartitionedRankLimitExec {
    input: Arc<dyn ExecutionPlan>,
    /// Full sort expression `[partition_keys..., order_keys...]`.
    expr: LexOrdering,
    /// Leading count of expressions in `expr` that form the partition key.
    /// Zero means "no PARTITION BY" (global top-K within each input partition).
    /// Can equal `expr.len()` when the ORDER BY suffix collapsed away.
    partition_prefix_len: usize,
    /// `K` in "top-K per partition".
    fetch: usize,
    /// Which rank-like function drives the emit/skip decision.
    kind: WindowFnKind,
    cache: Arc<PlanProperties>,
}

impl PartitionedRankLimitExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        expr: LexOrdering,
        partition_prefix_len: usize,
        fetch: usize,
        kind: WindowFnKind,
    ) -> Result<Self> {
        assert!(
            partition_prefix_len <= expr.len(),
            "PartitionedRankLimitExec: partition prefix must not exceed the ordering length"
        );
        assert!(fetch > 0, "PartitionedRankLimitExec requires fetch > 0");
        let cache = Arc::new(Self::compute_properties(&input, expr.clone())?);
        Ok(Self {
            input,
            expr,
            partition_prefix_len,
            fetch,
            kind,
            cache,
        })
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        sort_exprs: LexOrdering,
    ) -> Result<PlanProperties> {
        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.reorder(sort_exprs)?;
        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn expr(&self) -> &LexOrdering {
        &self.expr
    }
    pub fn partition_prefix_len(&self) -> usize {
        self.partition_prefix_len
    }
    pub fn fetch(&self) -> usize {
        self.fetch
    }
    pub fn kind(&self) -> WindowFnKind {
        self.kind
    }
}

impl DisplayAs for PartitionedRankLimitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "CometPartitionedRankLimitExec: kind={:?}, fetch={}, partition_prefix_len={}, \
                 expr=[{}]",
                self.kind, self.fetch, self.partition_prefix_len, self.expr
            ),
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(PartitionedRankLimitExec::try_new(
            Arc::clone(&children[0]),
            self.expr.clone(),
            self.partition_prefix_len,
            self.fetch,
            self.kind,
        )?))
    }

    // The operator's correctness depends on the input being sorted by
    // `[partition_keys..., order_keys...]`. Declaring this lets DataFusion's
    // `EnforceSorting` insert a `SortExec` if the sort somehow got dropped
    // during plan construction, though in practice Spark's Catalyst already
    // injects the sort above `WindowGroupLimitExec`.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        if self.expr.is_empty() {
            vec![None]
        } else {
            let req = LexRequirement::from(self.expr.clone());
            vec![Some(OrderingRequirements::new(req))]
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();

        // Order-by suffix drives tie detection for `Rank`. When the suffix is
        // empty (either the query has no ORDER BY, or every ORDER BY column
        // was deduplicated with a PARTITION BY column by `LexOrdering::new`),
        // we skip the order converter and treat every row within a partition
        // as a tie.
        let (order_converter, order_exprs) = if self.partition_prefix_len < self.expr.len() {
            let order_sort_fields =
                build_sort_fields(&self.expr[self.partition_prefix_len..], &schema)?;
            let converter = RowConverter::new(order_sort_fields)?;
            let exprs: Vec<Arc<dyn PhysicalExpr>> = self.expr[self.partition_prefix_len..]
                .iter()
                .map(|e| Arc::clone(&e.expr))
                .collect();
            (Some(converter), exprs)
        } else {
            (None, Vec::new())
        };

        // Partition prefix is optional: `PARTITION BY` may be empty, in which
        // case state accumulates across every row in the DF partition.
        let (partition_converter, partition_exprs) = if self.partition_prefix_len == 0 {
            (None, Vec::new())
        } else {
            let partition_sort_fields =
                build_sort_fields(&self.expr[..self.partition_prefix_len], &schema)?;
            let converter = RowConverter::new(partition_sort_fields)?;
            let exprs: Vec<Arc<dyn PhysicalExpr>> = self.expr[..self.partition_prefix_len]
                .iter()
                .map(|e| Arc::clone(&e.expr))
                .collect();
            (Some(converter), exprs)
        };

        Ok(Box::pin(RankLimitStream {
            input,
            schema,
            partition_converter,
            order_converter,
            partition_exprs,
            order_exprs,
            limit: self.fetch,
            kind: self.kind,
            prev_partition: None,
            prev_order: None,
            rank: 0,
            count: 0,
            started: false,
        }))
    }
}

fn build_sort_fields(
    ordering: &[datafusion::physical_expr::PhysicalSortExpr],
    schema: &SchemaRef,
) -> Result<Vec<SortField>> {
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
    partition_converter: Option<RowConverter>,
    /// `None` when the ORDER BY suffix is empty (fully covered by PARTITION BY
    /// or absent entirely). In that case every row within a partition ties
    /// under `Rank`, or ranks monotonically by position under `RowNumber`.
    order_converter: Option<RowConverter>,
    /// Empty when `partition_converter` is `None`.
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Empty when `order_converter` is `None`.
    order_exprs: Vec<Arc<dyn PhysicalExpr>>,
    limit: usize,
    kind: WindowFnKind,

    // Per-partition streaming state, persisted across batches.
    prev_partition: Option<OwnedRow>,
    prev_order: Option<OwnedRow>,
    /// Rank of the most recently seen row (0-indexed). Only meaningful when
    /// `count > 0`.
    rank: u64,
    /// Total rows seen in the current partition (also 0-indexed cursor).
    count: u64,
    /// Whether any row has been observed yet. Only used when there is no
    /// PARTITION BY: on the first row we still need to initialize state.
    started: bool,
}

impl RankLimitStream {
    fn process_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(batch.clone());
        }

        let partition_rows = match &self.partition_converter {
            Some(converter) => {
                let partition_cols: Vec<ArrayRef> = self
                    .partition_exprs
                    .iter()
                    .map(|e| e.evaluate(batch).and_then(|v| v.into_array(num_rows)))
                    .collect::<Result<_>>()?;
                Some(converter.convert_columns(&partition_cols)?)
            }
            None => None,
        };

        let order_rows = match &self.order_converter {
            Some(converter) => {
                let order_cols: Vec<ArrayRef> = self
                    .order_exprs
                    .iter()
                    .map(|e| e.evaluate(batch).and_then(|v| v.into_array(num_rows)))
                    .collect::<Result<_>>()?;
                Some(converter.convert_columns(&order_cols)?)
            }
            None => None,
        };

        let mut keep: Vec<u32> = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let same_partition = match &partition_rows {
                Some(pr) => {
                    let p_row = pr.row(i);
                    matches!(&self.prev_partition, Some(prev) if prev.row() == p_row)
                }
                // No PARTITION BY: state accumulates across every row, resetting
                // only on the very first row of the stream.
                None => self.started,
            };
            if !same_partition {
                if let Some(pr) = &partition_rows {
                    self.prev_partition = Some(pr.row(i).owned());
                }
                self.prev_order = None;
                self.rank = 0;
                self.count = 0;
            }
            self.started = true;

            let this_rank: u64 = match self.kind {
                // ROW_NUMBER: 0-indexed position within the partition.
                WindowFnKind::RowNumber => self.count,
                // RANK: 0 on first row of partition; else same as prior rank
                // when ORDER BY key ties (or when there is no ORDER BY),
                // otherwise jump to the running count.
                WindowFnKind::Rank => match (&self.prev_order, &order_rows) {
                    (None, _) => 0,
                    (Some(_), None) => self.rank,
                    (Some(prev_o), Some(rows)) if prev_o.row() == rows.row(i) => self.rank,
                    (Some(_), Some(_)) => self.count,
                },
            };

            if (this_rank as usize) < self.limit {
                keep.push(i as u32);
            }

            self.rank = this_rank;
            if let Some(rows) = &order_rows {
                self.prev_order = Some(rows.row(i).owned());
            }
            self.count += 1;
        }

        let indices = UInt32Array::from(keep);
        Ok(take_record_batch(batch, &indices)?)
    }
}

impl Stream for RankLimitStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => match self.process_batch(&batch) {
                    // Skip fully-filtered batches so downstream never sees
                    // spurious empty batches between real ones.
                    Ok(out) if out.num_rows() == 0 => continue,
                    Ok(out) => return Poll::Ready(Some(Ok(out))),
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
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
