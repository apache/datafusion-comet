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
//!
//! The build side is one batch in memory, which a `SortExec` could sort only by reserving twice its
//! size at once. So the operator sorts the batch's row indices by the join keys instead, and the
//! sort-merge join reads the rows in that order. The join switches only once the memory for that
//! sort is reserved, beyond what the inner join's hash table, which it drops, had reserved.

use std::fmt::Debug;
use std::mem::size_of;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use arrow::array::{Array, UInt32Array};
use arrow::compute::{lexsort_to_indices, take_record_batch, SortColumn, SortOptions};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{internal_datafusion_err, internal_err, JoinSide, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExprRef, PhysicalSortExpr};
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::metrics::{Metric, MetricValue, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;

use super::memory::try_reserve;
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
            // In TPC-DS q72, a spilled pair whose build side fits in the cache takes about 10 ns a
            // match. The join of the buckets left in memory, which does not fit, takes 30 ns or
            // more in most tasks, and 28 ns in some, where it still loses to a sort-merge join.
            min_nanos_per_match: 20.0,
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
    /// What sorting the build side needs beyond what the inner join's hash table has reserved,
    /// which the join reserves here before it switches.
    sort_memory: MemoryReservation,
    sort_memory_needed: usize,
    switched: bool,
    /// The probe batch the join switched before, which the sort-merge join reads first.
    next: Option<RecordBatch>,
}

impl ProbeHandoff {
    /// Returns the handoff, and the stream for the inner join to read `input` through. The join
    /// switches only once `sort_memory` has reserved `sort_memory_needed` bytes.
    pub(super) fn new(
        input: SendableRecordBatchStream,
        fallback: SortMergeFallback,
        sort_memory: MemoryReservation,
        sort_memory_needed: usize,
    ) -> (Arc<Self>, SendableRecordBatchStream) {
        let schema = input.schema();
        let handoff = Arc::new(Self {
            state: Mutex::new(HandoffState {
                input: Some(input),
                progress: None,
                sort_memory,
                sort_memory_needed,
                switched: false,
                next: None,
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

    /// The probe input that the inner join left unread when it switched, and the memory reserved
    /// for sorting its build side, or None if it did not switch.
    pub(super) fn take_switched_input(
        &self,
    ) -> Option<(SendableRecordBatchStream, MemoryReservation)> {
        let mut state = self.state.lock();
        if !state.switched {
            return None;
        }
        let input = state.input.take()?;
        let schema = input.schema();
        let rest = futures::stream::iter(state.next.take().map(Ok)).chain(input);
        let rest = Box::pin(RecordBatchStreamAdapter::new(schema, rest));
        Some((rest, state.sort_memory.take()))
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
        let Some(input) = state.input.as_mut() else {
            return Poll::Ready(None);
        };
        let batch = match ready!(input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => batch,
            None => {
                state.input = None;
                return Poll::Ready(None);
            }
            error => return Poll::Ready(error),
        };
        // The join switches only with probe input left to give the sort-merge join. Without the
        // memory to sort the build side, it carries on, and asks again at the next batch.
        let switch = state
            .progress
            .as_ref()
            .is_some_and(|progress| progress.should_switch(&self.handoff.fallback));
        if switch && try_reserve(&state.sort_memory, state.sort_memory_needed)? {
            state.switched = true;
            state.next = Some(batch);
            return Poll::Ready(None);
        }
        Poll::Ready(Some(Ok(batch)))
    }
}

impl RecordBatchStream for HandoffStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// The memory that sorting the row indices of `rows` build rows by `keys` join keys can take: the
/// indices, and while they are sorted, an index and a key or a reference to it for each row.
pub(super) fn sort_memory(rows: usize, keys: usize) -> usize {
    rows.saturating_mul(2 * size_of::<u32>() + keys * 24)
}

/// A build batch and the order of its rows by the join keys, in which a sort-merge join reads them.
pub(super) struct SortedBuild {
    batch: RecordBatch,
    order: UInt32Array,
}

impl SortedBuild {
    /// Sorts the rows of `batch`, the build side of `template`, by its join keys.
    pub(super) fn try_new(template: &HashJoinExec, batch: RecordBatch) -> Result<Self> {
        let rows = batch.num_rows();
        let keys = template
            .on()
            .iter()
            .map(|(key, _)| {
                Ok(SortColumn {
                    values: key.evaluate(&batch)?.into_array(rows)?,
                    options: Some(SortOptions::default()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let order = lexsort_to_indices(&keys, None)?;
        Ok(Self { batch, order })
    }

    /// The memory that the order of the rows takes.
    pub(super) fn order_size(&self) -> usize {
        self.order.get_array_memory_size()
    }

    /// The rows in order, in batches of up to `batch_size` rows.
    fn into_stream(self, batch_size: usize) -> SendableRecordBatchStream {
        let Self { batch, order } = self;
        let schema = batch.schema();
        let rows = order.len();
        let batch_size = batch_size.max(1);
        let batches = (0..rows).step_by(batch_size).map(move |start| {
            let indices = order.slice(start, batch_size.min(rows - start));
            Ok(take_record_batch(&batch, &indices)?)
        });
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches),
        ))
    }
}

/// The build side of a sort-merge join.
pub(super) enum SortMergeBuild {
    /// The build batch of an inner join that switched, read in order.
    Sorted(SortedBuild),
    /// The build side of a spilled pair, which a `SortExec` sorts.
    Unsorted(SendableRecordBatchStream),
}

/// Joins `build` with `probe` by a sort-merge join, as `template`, an inner join, would join them,
/// and returns the output, with `template`'s schema. The sorts reserve their memory from
/// `context`'s pool, and spill when it runs out.
pub(super) fn sort_merge_join(
    template: &HashJoinExec,
    build: SortMergeBuild,
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
    let build = match build {
        SortMergeBuild::Sorted(build) => {
            StreamSourceExec::new_arc(build.into_stream(context.session_config().batch_size()))
        }
        SortMergeBuild::Unsorted(build) => sorted(build, build_keys)?,
    };
    let join: Arc<dyn ExecutionPlan> = Arc::new(SortMergeJoinExec::try_new(
        build,
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
