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
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{
    LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
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
    /// Full sort expression `[partition_keys..., order_keys...]`.
    expr: LexOrdering,
    /// Leading count of expressions in `expr` that form the partition key.
    /// Zero means "no PARTITION BY" (global top-K within each input partition).
    /// Can equal `expr.len()` when `LexOrdering::new` dedup collapses the ORDER BY suffix.
    partition_prefix_len: usize,
    fetch: usize,
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
        // Guard against `LexOrdering::new` dedup dropping a partition key.
        if partition_prefix_len > expr.len() {
            return Err(DataFusionError::Internal(format!(
                "PartitionedRankLimitExec: partition prefix ({partition_prefix_len}) exceeds \
                 ordering length ({})",
                expr.len()
            )));
        }
        let cache = Arc::new(Self::compute_properties(&input, &expr)?);
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
        sort_exprs: &LexOrdering,
    ) -> Result<PlanProperties> {
        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.reorder(sort_exprs.clone())?;
        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
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
        vec![Some(OrderingRequirements::from(self.expr.clone()))]
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

        let partition_key = build_key_encoder(&self.expr[..self.partition_prefix_len], &schema)?;

        // ROW_NUMBER's rank formula is just the running count, so it never reads the
        // ORDER BY key. Skip building the converter and evaluating order columns.
        // For RANK/DENSE_RANK, the encoder drives tie detection on the ORDER BY
        // suffix. When the suffix is empty (query has no ORDER BY, or every ORDER BY
        // column was deduplicated with a PARTITION BY column by `LexOrdering::new`),
        // `build_key_encoder` returns `None` and every row within a partition ties.
        let order_key = if self.kind == WindowFnKind::RowNumber {
            None
        } else {
            build_key_encoder(&self.expr[self.partition_prefix_len..], &schema)?
        };

        Ok(Box::pin(RankLimitStream {
            input,
            schema,
            partition_key,
            order_key,
            limit: self.fetch as u64,
            kind: self.kind,
            prev_partition: None,
            prev_order: None,
            rank: 0,
            count: 0,
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
    /// `None` when the ORDER BY suffix is empty (fully covered by PARTITION BY
    /// or absent entirely), and always `None` for ROW_NUMBER (rank formula
    /// never reads order keys).
    order_key: Option<KeyEncoder>,
    limit: u64,
    kind: WindowFnKind,

    // Per-partition streaming state, persisted across batches.
    prev_partition: Option<OwnedRow>,
    prev_order: Option<OwnedRow>,
    /// Rank of the most recently seen row (0-indexed). Only meaningful when `count > 0`.
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

        let partition_rows = self
            .partition_key
            .as_ref()
            .map(|k| k.encode(batch))
            .transpose()?;
        let order_rows = self
            .order_key
            .as_ref()
            .map(|k| k.encode(batch))
            .transpose()?;

        let mut mask_builder = BooleanBufferBuilder::new(num_rows);
        let mut kept: usize = 0;
        for i in 0..num_rows {
            let same_partition = match &partition_rows {
                Some(pr) => matches!(&self.prev_partition, Some(prev) if prev.row() == pr.row(i)),
                // No PARTITION BY: state accumulates across every row, resetting
                // only on the very first row of the stream.
                None => self.count > 0,
            };
            if !same_partition {
                if let Some(pr) = &partition_rows {
                    self.prev_partition = Some(pr.row(i).owned());
                }
                self.prev_order = None;
                self.rank = 0;
                self.count = 0;
            }

            // Whether this row's ORDER BY key ties with the previous emitted
            // row. `false` on the first row of a partition and (vacuously) when
            // there is no ORDER BY suffix — `prev_order` stays `None` across
            // the whole partition in that case.
            let ties_with_prev = matches!(
                (&self.prev_order, &order_rows),
                (Some(prev_o), Some(rows)) if prev_o.row() == rows.row(i)
            );

            let this_rank: u64 =
                if self.prev_order.is_none() && self.kind != WindowFnKind::RowNumber {
                    // First row of a partition ranks 0 under RANK/DENSE_RANK.
                    0
                } else {
                    match self.kind {
                        WindowFnKind::RowNumber => self.count,
                        _ if ties_with_prev => self.rank,
                        WindowFnKind::DenseRank => self.rank + 1,
                        WindowFnKind::Rank => self.count,
                    }
                };

            let keep = this_rank < self.limit;
            mask_builder.append(keep);
            if keep {
                kept += 1;
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
            return Ok(batch.clone());
        }
        if kept == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let mask = BooleanArray::new(mask_builder.finish(), None);
        Ok(filter_record_batch(batch, &mask)?)
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
