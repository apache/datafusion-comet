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

//! Joining the rest of a partition with a sort-merge join, once its probe rows turn out to match
//! many build rows each.
//!
//! A hash join pays for every match with a step along a chain of build rows and with gathers from
//! wherever the matched rows are in the build batch, and once the build side outgrows the CPU
//! cache, most of them miss. A sort-merge join pays once to sort both sides, and then reads the
//! matches of each key in order. So when probe rows match many build rows each, the sort-merge
//! join is several times faster. In TPC-DS q72, the join of `catalog_sales` and `inventory`
//! matches each probe row with hundreds of build rows on the item alone, and its residual filter
//! keeps a few of them.
//!
//! A running inner join switches at the first probe batch boundary where
//!
//! - its probe rows have matched at least `min_matches_per_probe_row` build rows each on average,
//!   counted before the residual filter,
//! - its probe has spent at least `min_nanos_per_match` on each match, which shows that the build
//!   side does not fit in the cache, and
//! - its probe has taken at least `min_probe_to_build_time` times its build. Sorting the build side
//!   costs about as much as building its hash table, so by then the hash join has spent more than
//!   switching costs, however much probe input is left.
//!
//! It reads all three from the `HashJoinExec`'s own metrics. The inner join then stops reading
//! probe batches and finishes the one it has, and a sort-merge join joins its build side with the
//! rest of the probe input. Each probe row is joined by one of the two, which is exact for inner
//! joins, the only join type that switches. The spilled pairs that follow are joined by sort-merge
//! joins too.

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use arrow::compute::SortOptions;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{internal_datafusion_err, internal_err, JoinSide, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExprRef, PhysicalSortExpr};
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::metrics::{Metric, MetricValue, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;

use super::source::StreamSourceExec;

/// When an inner join switches to a sort-merge join. See the [module documentation](self).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SortMergeFallback {
    /// Probe rows an inner join reads before it can switch.
    pub min_probe_rows: usize,
    /// Build rows each probe row has matched on average, before the residual filter.
    pub min_matches_per_probe_row: f64,
    /// Time the probe has spent on each match, in nanoseconds.
    pub min_nanos_per_match: f64,
    /// Time the probe has taken, as a multiple of the time the build took.
    pub min_probe_to_build_time: f64,
}

impl Default for SortMergeFallback {
    fn default() -> Self {
        Self {
            min_probe_rows: 16 * 1024,
            min_matches_per_probe_row: 16.0,
            min_nanos_per_match: 30.0,
            min_probe_to_build_time: 4.0,
        }
    }
}

impl SortMergeFallback {
    /// Switches at the first probe batch boundary after a match, for tests.
    pub fn always() -> Self {
        Self {
            min_probe_rows: 1,
            min_matches_per_probe_row: 0.0,
            min_nanos_per_match: 0.0,
            min_probe_to_build_time: 0.0,
        }
    }
}

/// The metrics of a running inner `HashJoinExec` that decide whether it switches.
pub(super) struct JoinProgress {
    probe_rows: Arc<Metric>,
    /// `avg_fanout`, whose part counts the matches before the residual filter.
    matches: Arc<Metric>,
    probe_time: Arc<Metric>,
    build_time: Arc<Metric>,
}

impl JoinProgress {
    /// Finds the metrics among those of an inner join that has started executing.
    pub(super) fn find(metrics: &MetricsSet) -> Option<Self> {
        let find = |name: &str| {
            metrics
                .iter()
                .find(|metric| metric.value().name() == name)
                .cloned()
        };
        Some(Self {
            probe_rows: find("input_rows")?,
            matches: find("avg_fanout")?,
            probe_time: find("join_time")?,
            build_time: find("build_time")?,
        })
    }

    fn should_switch(&self, fallback: &SortMergeFallback) -> bool {
        let probe_rows = self.probe_rows.value().as_usize();
        if probe_rows < fallback.min_probe_rows {
            return false;
        }
        let MetricValue::Ratio { ratio_metrics, .. } = self.matches.value() else {
            return false;
        };
        let matches = ratio_metrics.part() as f64;
        let probe_nanos = self.probe_time.value().as_usize() as f64;
        let build_nanos = self.build_time.value().as_usize() as f64;
        matches > 0.0
            && matches >= fallback.min_matches_per_probe_row * probe_rows as f64
            && probe_nanos >= fallback.min_nanos_per_match * matches
            && probe_nanos >= fallback.min_probe_to_build_time * build_nanos
    }
}

/// The probe input of an inner join that may switch to a sort-merge join, shared by the inner
/// join, which reads it through [`HandoffStream`], and the operator.
pub(super) struct ProbeHandoff {
    state: Mutex<HandoffState>,
    fallback: SortMergeFallback,
}

struct HandoffState {
    input: Option<SendableRecordBatchStream>,
    /// Set once the inner join has started executing.
    progress: Option<JoinProgress>,
    switched: bool,
}

impl ProbeHandoff {
    /// Returns the handoff, and the stream for the inner join to read `input` through.
    pub(super) fn new(
        input: SendableRecordBatchStream,
        fallback: SortMergeFallback,
    ) -> (Arc<Self>, SendableRecordBatchStream) {
        let schema = input.schema();
        let handoff = Arc::new(Self {
            state: Mutex::new(HandoffState {
                input: Some(input),
                progress: None,
                switched: false,
            }),
            fallback,
        });
        let stream = HandoffStream {
            handoff: Arc::clone(&handoff),
            schema,
        };
        (handoff, Box::pin(stream))
    }

    /// Lets the handoff watch the metrics of the inner join that reads it.
    pub(super) fn watch(&self, progress: Option<JoinProgress>) {
        self.state.lock().progress = progress;
    }

    /// The probe input that the inner join left unread when it switched, or None if it did not.
    pub(super) fn take_switched_input(&self) -> Option<SendableRecordBatchStream> {
        let mut state = self.state.lock();
        if state.switched {
            state.input.take()
        } else {
            None
        }
    }
}

/// The inner join's view of a [`ProbeHandoff`]: the probe input, which ends early at a batch
/// boundary when the join switches.
struct HandoffStream {
    handoff: Arc<ProbeHandoff>,
    schema: SchemaRef,
}

impl Stream for HandoffStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.handoff.state.lock();
        if state.switched {
            return Poll::Ready(None);
        }
        let switch = state
            .progress
            .as_ref()
            .is_some_and(|progress| progress.should_switch(&self.handoff.fallback));
        if switch {
            state.switched = true;
            return Poll::Ready(None);
        }
        let Some(input) = state.input.as_mut() else {
            return Poll::Ready(None);
        };
        match ready!(input.poll_next_unpin(cx)) {
            None => {
                state.input = None;
                Poll::Ready(None)
            }
            batch => Poll::Ready(batch),
        }
    }
}

impl RecordBatchStream for HandoffStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Joins `build` with `probe` by a sort-merge join, as `template`, an inner join, would join them,
/// and returns the output, with `template`'s schema. The sorts reserve their memory from
/// `context`'s pool, and spill when it runs out.
pub(super) fn sort_merge_join(
    template: &HashJoinExec,
    build: SendableRecordBatchStream,
    probe: SendableRecordBatchStream,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let sort_options = vec![SortOptions::default(); template.on().len()];
    let sorted = |input: SendableRecordBatchStream,
                  keys: Vec<PhysicalExprRef>|
     -> Result<Arc<dyn ExecutionPlan>> {
        let ordering = LexOrdering::new(
            keys.into_iter()
                .zip(&sort_options)
                .map(|(key, options)| PhysicalSortExpr::new(key, *options)),
        )
        .ok_or_else(|| internal_datafusion_err!("a hash join has join keys"))?;
        Ok(Arc::new(SortExec::new(
            ordering,
            StreamSourceExec::new_arc(input),
        )))
    };
    let build_keys = template
        .on()
        .iter()
        .map(|(key, _)| Arc::clone(key))
        .collect();
    let probe_keys = template
        .on()
        .iter()
        .map(|(_, key)| Arc::clone(key))
        .collect();
    let join: Arc<dyn ExecutionPlan> = Arc::new(SortMergeJoinExec::try_new(
        sorted(build, build_keys)?,
        sorted(probe, probe_keys)?,
        template.on().to_vec(),
        template.filter().map(left_columns_first).transpose()?,
        template.join_type,
        sort_options.clone(),
        template.null_equality(),
    )?);
    let plan: Arc<dyn ExecutionPlan> = match &template.projection {
        Some(projection) => {
            let schema = join.schema();
            let columns = projection.iter().map(|&index| {
                let name = schema.field(index).name();
                let column: PhysicalExprRef = Arc::new(Column::new(name, index));
                (column, name.clone())
            });
            Arc::new(ProjectionExec::try_new(columns, join)?)
        }
        None => join,
    };
    if plan.schema().fields() != template.schema().fields() {
        return internal_err!(
            "the sort-merge join of a hash join emits {:?} instead of {:?}",
            plan.schema(),
            template.schema()
        );
    }
    // The plan reads one stream for each side, so it has a single partition.
    plan.execute(0, context)
}

/// `filter`, with the columns of the left side listed first. In DataFusion 55, `SortMergeJoinExec`
/// passes a filter the columns of the left side and then those of the right side, whatever the
/// order of the filter's own columns, and a filter swapped along with the inputs of its join lists
/// those of the right side first. apache/datafusion#25489 fixes it.
fn left_columns_first(filter: &JoinFilter) -> Result<JoinFilter> {
    let columns = filter.column_indices();
    let (left, right): (Vec<usize>, Vec<usize>) =
        (0..columns.len()).partition(|&column| columns[column].side == JoinSide::Left);
    let order: Vec<usize> = left.into_iter().chain(right).collect();
    // The position of each of the filter's columns in `order`.
    let mut positions = vec![0; order.len()];
    for (position, &column) in order.iter().enumerate() {
        positions[column] = position;
    }
    let expression = Arc::clone(filter.expression())
        .transform(|expr| {
            Ok(match expr.downcast_ref::<Column>() {
                Some(column) => Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    positions[column.index()],
                )) as PhysicalExprRef),
                None => Transformed::no(expr),
            })
        })
        .data()?;
    let schema = filter.schema();
    let fields: Vec<_> = order
        .iter()
        .map(|&column| Arc::clone(&schema.fields()[column]))
        .collect();
    Ok(JoinFilter::new(
        expression,
        order
            .iter()
            .map(|&column| columns[column].clone())
            .collect(),
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
    ))
}
