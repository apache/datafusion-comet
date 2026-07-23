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

//! Streaming top-K per partition operator with `RANK()` semantics.
//!
//! Comet routes Spark's `WindowGroupLimitExec` with `RANK()` to this operator.
//! Unlike DataFusion's heap-based `PartitionedTopKExec` (which handles
//! `ROW_NUMBER` only in the pinned DF 54.1.0 release), this operator relies on
//! the fact that Spark's `WindowGroupLimitExec.requiredChildOrdering` guarantees
//! the child is already sorted by `[partition_keys..., order_keys...]` — so a
//! single streaming pass decides, row-by-row, whether to emit or drop, matching
//! the exact tie behavior of Spark's `RankLimitIterator`.

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

/// Streaming top-K per partition operator matching Spark's
/// `WindowGroupLimitExec` with `RANK()`.
///
/// # Preconditions
/// The child stream MUST be sorted by `[partition_keys..., order_keys...]`.
/// Spark's `WindowGroupLimitExec.requiredChildOrdering` guarantees this via
/// `EnsureRequirements`, so Comet's serde relies on the injected sort rather
/// than declaring a hard ordering requirement here that would trigger a second
/// DataFusion-side sort.
///
/// # Semantics
/// Row-by-row, we maintain per-partition state: `rank` (0-indexed rank of the
/// last emitted row) and `count` (total rows seen in the partition). On each
/// input row:
///   * Reset state on a partition-key change.
///   * `this_rank = 0` for the first row of a partition; else `this_rank =
///     rank` if the ORDER BY key matches the previous row, else `count`.
///   * Emit iff `this_rank < limit`.
///
/// Consequence: all rows tied at the K-th ORDER BY value ARE emitted — RANK can
/// return more than K rows when ties straddle the boundary, exactly like
/// Spark's `RankLimitIterator`.
#[derive(Debug)]
pub struct PartitionedRankLimitExec {
    input: Arc<dyn ExecutionPlan>,
    /// Full sort expression `[partition_keys..., order_keys...]`.
    expr: LexOrdering,
    /// Leading count of expressions in `expr` that form the partition key.
    partition_prefix_len: usize,
    /// `K` in "top-K per partition".
    fetch: usize,
    cache: Arc<PlanProperties>,
}

impl PartitionedRankLimitExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        expr: LexOrdering,
        partition_prefix_len: usize,
        fetch: usize,
    ) -> Result<Self> {
        assert!(
            partition_prefix_len > 0,
            "PartitionedRankLimitExec requires a non-empty partition prefix"
        );
        assert!(
            partition_prefix_len < expr.len(),
            "PartitionedRankLimitExec requires at least one ORDER BY expression after the partition prefix"
        );
        assert!(fetch > 0, "PartitionedRankLimitExec requires fetch > 0");
        let cache = Arc::new(Self::compute_properties(&input, expr.clone())?);
        Ok(Self {
            input,
            expr,
            partition_prefix_len,
            fetch,
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
}

impl DisplayAs for PartitionedRankLimitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "CometPartitionedRankLimitExec: fetch={}, partition_prefix_len={}, expr=[{}]",
                self.fetch, self.partition_prefix_len, self.expr
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
        )?))
    }

    // The operator's correctness depends on the input being sorted by
    // `[partition_keys..., order_keys...]`. Declaring this lets DataFusion's
    // `EnforceSorting` insert a `SortExec` if the sort somehow got dropped
    // during plan construction, though in practice Spark's Catalyst already
    // injects the sort above `WindowGroupLimitExec`.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let req = LexRequirement::from(self.expr.clone());
        vec![Some(OrderingRequirements::new(req))]
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

        // Two independent RowConverters: one for the partition prefix (used
        // only for equality against the previous row) and one for the ORDER BY
        // suffix (used to detect a rank-changing tie boundary).
        let partition_sort_fields =
            build_sort_fields(&self.expr[..self.partition_prefix_len], &schema)?;
        let order_sort_fields =
            build_sort_fields(&self.expr[self.partition_prefix_len..], &schema)?;

        let partition_converter = RowConverter::new(partition_sort_fields)?;
        let order_converter = RowConverter::new(order_sort_fields)?;

        let partition_exprs: Vec<Arc<dyn PhysicalExpr>> = self.expr[..self.partition_prefix_len]
            .iter()
            .map(|e| Arc::clone(&e.expr))
            .collect();
        let order_exprs: Vec<Arc<dyn PhysicalExpr>> = self.expr[self.partition_prefix_len..]
            .iter()
            .map(|e| Arc::clone(&e.expr))
            .collect();

        Ok(Box::pin(RankLimitStream {
            input,
            schema,
            partition_converter,
            order_converter,
            partition_exprs,
            order_exprs,
            limit: self.fetch,
            prev_partition: None,
            prev_order: None,
            rank: 0,
            count: 0,
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
    partition_converter: RowConverter,
    order_converter: RowConverter,
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    order_exprs: Vec<Arc<dyn PhysicalExpr>>,
    limit: usize,

    // Per-partition streaming state, persisted across batches.
    prev_partition: Option<OwnedRow>,
    prev_order: Option<OwnedRow>,
    /// Rank of the most recently seen row (0-indexed). Only meaningful when
    /// `count > 0`.
    rank: u64,
    /// Total rows seen in the current partition (also 0-indexed cursor).
    count: u64,
}

impl RankLimitStream {
    fn process_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(batch.clone());
        }

        let partition_cols: Vec<ArrayRef> = self
            .partition_exprs
            .iter()
            .map(|e| e.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;
        let order_cols: Vec<ArrayRef> = self
            .order_exprs
            .iter()
            .map(|e| e.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;

        let partition_rows = self.partition_converter.convert_columns(&partition_cols)?;
        let order_rows = self.order_converter.convert_columns(&order_cols)?;

        let mut keep: Vec<u32> = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let p_row = partition_rows.row(i);
            let o_row = order_rows.row(i);

            let same_partition = matches!(&self.prev_partition, Some(prev) if prev.row() == p_row);
            if !same_partition {
                self.prev_partition = Some(p_row.owned());
                self.prev_order = None;
                self.rank = 0;
                self.count = 0;
            }

            let this_rank: u64 = match &self.prev_order {
                None => 0,
                Some(prev_o) if prev_o.row() == o_row => self.rank,
                Some(_) => self.count,
            };

            if (this_rank as usize) < self.limit {
                keep.push(i as u32);
            }

            self.rank = this_rank;
            self.prev_order = Some(o_row.owned());
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
