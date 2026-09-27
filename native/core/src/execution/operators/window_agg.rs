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

//! Window functions that need their whole window partition before they produce a value.
//!
//! `PERCENT_RANK`, `CUME_DIST`, `NTILE`, and aggregates whose frame ends at
//! `UNBOUNDED FOLLOWING` cannot run in DataFusion's `BoundedWindowAggExec`. DataFusion's
//! `WindowAggExec` runs them by buffering its whole input, which is the task's entire Spark
//! partition, and concatenating it before it evaluates anything. It reserves memory for
//! neither, and it cannot spill (apache/datafusion#22946).
//!
//! [`CometWindowAggExec`] evaluates the same window expressions, one window partition at a
//! time as `WindowAggExec` does, with two differences:
//!
//! - The input is sorted by the `PARTITION BY` keys, so once a batch starts a new window
//!   partition, every earlier one is complete. Those are evaluated and emitted right away, and
//!   only the window partition that may continue is kept. Without `PARTITION BY`, the whole
//!   input is one window partition.
//! - The buffered batches, and the copy made when concatenating them for evaluation, are
//!   reserved against the task's memory pool. The operator still cannot spill, so a window
//!   partition that does not fit fails the task with a memory error instead of growing
//!   untracked.
//!
//! Once DataFusion's `WindowAggExec` does both, this operator can be removed.

use std::fmt::Formatter;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, RecordBatch};
use arrow::compute::{concat, concat_batches, SortColumn};
use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::memory::RecordBatchMemoryCounter;
use datafusion::common::utils::{evaluate_partition_ranges, transpose};
use datafusion::common::{internal_err, DataFusionError, Result, Statistics};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::window::WindowExpr;
use datafusion::physical_expr::{OrderingRequirements, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, ChildrenPropertiesMode, EmissionType, ReplaceChildrenOptions,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::windows::WindowAggExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan,
    InputDistributionRequirements, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{ready, Stream, StreamExt};

#[derive(Debug)]
pub(crate) struct CometWindowAggExec {
    /// DataFusion's operator for the same window expressions. It provides the output schema,
    /// the plan properties and the ordered `PARTITION BY` keys, and is never executed.
    window: WindowAggExec,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CometWindowAggExec {
    pub(crate) fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        can_repartition: bool,
    ) -> Result<Self> {
        Ok(Self::from_datafusion(WindowAggExec::try_new(
            window_expr,
            input,
            can_repartition,
        )?))
    }

    fn from_datafusion(window: WindowAggExec) -> Self {
        // Unlike `WindowAggExec`, the output is emitted as window partitions complete.
        let emission_type = if window.window_expr()[0].partition_by().is_empty() {
            EmissionType::Final
        } else {
            EmissionType::Incremental
        };
        let cache = Arc::new(
            window
                .properties()
                .as_ref()
                .clone()
                .with_emission_type(emission_type),
        );
        Self {
            window,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for CometWindowAggExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        // `WindowAggExec` starts these formats with its own name.
        if matches!(t, DisplayFormatType::Default | DisplayFormatType::Verbose) {
            write!(f, "Comet")?;
        }
        self.window.fmt_as(t, f)
    }
}

impl ExecutionPlan for CometWindowAggExec {
    fn name(&self) -> &str {
        "CometWindowAggExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.window.input()]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.window.apply_expressions(f)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.window.maintains_input_order()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.window.required_input_ordering()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.window.input_distribution_requirements()
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let replaced = Arc::new(self.window.clone()).replace_children(children, options)?;
        let Some(window) = replaced.downcast_ref::<WindowAggExec>() else {
            return internal_err!("WindowAggExec child replacement changed its plan type");
        };
        Ok(Arc::new(Self::from_datafusion(window.clone())))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partition_by_sort_keys = self.window.partition_by_sort_keys()?;
        // The same check as `WindowAggStream::new`.
        if partition_by_sort_keys.len() != self.window.window_expr()[0].partition_by().len() {
            return internal_err!("All partition by columns should have an ordering");
        }
        let input = self
            .window
            .input()
            .execute(partition, Arc::clone(&context))?;
        let reservation = MemoryConsumer::new(format!("{}[{partition}]", self.name()))
            .register(context.memory_pool());
        Ok(Box::pin(CometWindowAggStream {
            schema: self.window.schema(),
            input,
            window_expr: self.window.window_expr().to_vec(),
            partition_by_sort_keys,
            buffered: vec![],
            buffered_memory: RecordBatchMemoryCounter::new(),
            reservation,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            finished: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.window.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.window.statistics_from_inputs(input_stats, args)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.window.cardinality_effect()
    }
}

struct CometWindowAggStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    /// The rows of the last window partition seen so far, which may continue in the next
    /// batch. Without `PARTITION BY`, every row seen so far. Either way, one window partition.
    buffered: Vec<RecordBatch>,
    /// Counts each buffer that `buffered` retains once, however many batches share it.
    buffered_memory: RecordBatchMemoryCounter,
    reservation: MemoryReservation,
    baseline_metrics: BaselineMetrics,
    finished: bool,
}

impl CometWindowAggStream {
    fn evaluate_partition_keys(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.partition_by_sort_keys
            .iter()
            .map(|key| key.evaluate_to_sort_column(batch))
            .collect()
    }

    /// Buffers `batch`, then evaluates the window partitions that it completes.
    fn push_batch(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(None);
        }
        let added = self.buffered_memory.count_batch(&batch);
        self.reservation.try_grow(added).map_err(with_oom_context)?;
        if self.partition_by_sort_keys.is_empty() {
            self.buffered.push(batch);
            return Ok(None);
        }

        let keys = self.evaluate_partition_keys(&batch)?;
        let mut ranges = evaluate_partition_ranges(num_rows, &keys)?;
        // The batch's last window partition may continue in the next batch.
        let open_start = ranges.pop().map_or(0, |open| open.start);
        let continues = self.continues_buffered_partition(&keys)?;
        if ranges.is_empty() && continues {
            self.buffered.push(batch);
            return Ok(None);
        }

        // The window partitions that have ended are the buffered one, followed by those of the
        // batch's rows before `open_start`. If the batch continues the buffered window
        // partition, its first range extends it.
        let offset = self
            .buffered
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        let mut ranges: Vec<_> = ranges
            .into_iter()
            .map(|range| range.start + offset..range.end + offset)
            .collect();
        if continues {
            ranges[0].start = 0;
        } else if offset > 0 {
            ranges.insert(0, 0..offset);
        }
        let mut complete = std::mem::take(&mut self.buffered);
        if open_start > 0 {
            complete.push(batch.slice(0, open_start));
        }
        self.buffered
            .push(batch.slice(open_start, num_rows - open_start));
        self.evaluate(complete, ranges)
    }

    /// Whether the first row of the batch whose partition keys are `keys` belongs to the window
    /// partition of the last buffered row.
    fn continues_buffered_partition(&self, keys: &[SortColumn]) -> Result<bool> {
        let Some(last) = self.buffered.last() else {
            return Ok(false);
        };
        let last_keys = self.evaluate_partition_keys(&last.slice(last.num_rows() - 1, 1))?;
        let pairs = last_keys
            .into_iter()
            .zip(keys)
            .map(|(last, key)| {
                Ok(SortColumn {
                    values: concat(&[last.values.as_ref(), key.values.slice(0, 1).as_ref()])?,
                    options: key.options,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(evaluate_partition_ranges(2, &pairs)?.len() == 1)
    }

    /// Evaluates the window expressions over `batches`, whose rows make up the window
    /// partitions `ranges`, and returns the rows with the window columns appended.
    fn evaluate(
        &mut self,
        batches: Vec<RecordBatch>,
        ranges: impl IntoIterator<Item = Range<usize>>,
    ) -> Result<Option<RecordBatch>> {
        if batches.is_empty() {
            return Ok(None);
        }
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let batch = concat_batches(&self.input.schema(), &batches)?;
        // Concatenating a single batch is zero-copy. Otherwise reserve the buffers of the copy
        // that it does not share with `batches`, which are still reserved. Counting them after
        // the fact is exact, where estimating them from each batch would charge every batch
        // sliced from one list array for all of its values.
        if batches.len() > 1 {
            let mut counter = RecordBatchMemoryCounter::new();
            for input in &batches {
                counter.count_batch(input);
            }
            self.reservation
                .try_grow(counter.count_batch(&batch))
                .map_err(with_oom_context)?;
        }
        drop(batches);

        let mut partition_results = vec![];
        for range in ranges {
            let partition = batch.slice(range.start, range.len());
            partition_results.push(
                self.window_expr
                    .iter()
                    .map(|expr| expr.evaluate(&partition))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        let mut columns = batch.columns().to_vec();
        for results in transpose(partition_results) {
            columns.push(concat(
                &results
                    .iter()
                    .map(|array| array.as_ref())
                    .collect::<Vec<_>>(),
            )?);
        }
        let output = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;

        // The output is the consumer's to reserve, so only the rows still buffered stay
        // reserved.
        self.buffered_memory = RecordBatchMemoryCounter::new();
        for batch in &self.buffered {
            self.buffered_memory.count_batch(batch);
        }
        self.reservation.resize(self.buffered_memory.memory_usage());
        Ok(Some(output))
    }

    fn poll_next_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            if self.finished {
                return Poll::Ready(None);
            }
            let output = match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => self.push_batch(batch),
                Some(Err(e)) => Err(e),
                None => {
                    self.finished = true;
                    // Release the input pipeline's resources before evaluating the last window
                    // partition.
                    self.input = Box::pin(EmptyRecordBatchStream::new(self.input.schema()));
                    let buffered = std::mem::take(&mut self.buffered);
                    let num_rows = buffered.iter().map(RecordBatch::num_rows).sum();
                    self.evaluate(buffered, std::iter::once(0..num_rows))
                }
            };
            match output {
                Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                Ok(None) => continue,
                Err(e) => {
                    // Rows may have been dropped, so nothing after this would be correct.
                    self.finished = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}

impl Stream for CometWindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for CometWindowAggStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Tells the user what to change when a reservation is refused.
fn with_oom_context(error: DataFusionError) -> DataFusionError {
    match error {
        DataFusionError::ResourcesExhausted(_) => error.context(
            "Not enough memory to buffer a window partition. CometWindowAggExec cannot spill, \
             so each window partition must fit in memory. Consider increasing \
             spark.memory.offHeap.size, or setting spark.comet.exec.window.enabled=false to run \
             window functions in Spark",
        ),
        error => error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int32Array, Int64Array, ListArray, UInt32Array};
    use arrow::buffer::OffsetBuffer;
    use arrow::compute::take_record_batch;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::FunctionRegistry;
    use datafusion::logical_expr::{
        WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    };
    use datafusion::physical_expr::expressions::{col, lit};
    use datafusion::physical_expr::LexOrdering;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::windows::create_window_expr;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use rand::{rngs::StdRng, RngExt, SeedableRng};

    const CONSUMER: &str = "CometWindowAggExec[0]";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("p1", DataType::Int32, true),
            Field::new("p2", DataType::Int32, false),
            Field::new("o", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]))
    }

    /// Rows sorted by `p1 NULLS FIRST, p2, o`, where window partition `i` holds `sizes[i]` rows.
    /// The first two window partitions have a null `p1`, and `o` repeats so that rows have
    /// peers.
    fn sorted_rows(sizes: &[usize], rng: &mut StdRng) -> RecordBatch {
        let (mut p1, mut p2, mut o, mut v) = (vec![], vec![], vec![], vec![]);
        for (partition, &size) in sizes.iter().enumerate() {
            let mut order = 0;
            for _ in 0..size {
                order += rng.random_range(0..3);
                p1.push((partition >= 2).then_some(partition as i32 / 3));
                p2.push(partition as i32 % 3);
                o.push(order);
                v.push((rng.random_range(0..10) > 0).then(|| rng.random_range(-100..100)));
            }
        }
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(p1)),
                Arc::new(Int32Array::from(p2)),
                Arc::new(Int64Array::from(o)),
                Arc::new(Int64Array::from(v)),
            ],
        )
        .unwrap()
    }

    /// Slices `batch` into batches of up to `max_size` rows, including empty ones.
    fn random_slices(batch: &RecordBatch, max_size: usize, rng: &mut StdRng) -> Vec<RecordBatch> {
        let mut batches = vec![];
        let mut offset = 0;
        while offset < batch.num_rows() {
            let size = rng
                .random_range(0..=max_size)
                .min(batch.num_rows() - offset);
            batches.push(batch.slice(offset, size));
            offset += size;
        }
        batches
    }

    /// Copies `batch` into batches of `size` rows that share no buffers.
    fn chunks(batch: &RecordBatch, size: usize) -> Vec<RecordBatch> {
        (0..batch.num_rows())
            .step_by(size)
            .map(|start| {
                let end = (start + size).min(batch.num_rows());
                let indices = UInt32Array::from_iter_values(start as u32..end as u32);
                take_record_batch(batch, &indices).unwrap()
            })
            .collect()
    }

    fn memory_size(batches: &[RecordBatch]) -> usize {
        let mut counter = RecordBatchMemoryCounter::new();
        for batch in batches {
            counter.count_batch(batch);
        }
        counter.memory_usage()
    }

    fn input(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        let ordering = LexOrdering::new(
            ["p1", "p2", "o"]
                .map(|name| PhysicalSortExpr::new_default(col(name, &schema).unwrap())),
        )
        .unwrap();
        let source = MemorySourceConfig::try_new(&[batches], schema, None)
            .unwrap()
            .try_with_sort_information(vec![ordering])
            .unwrap();
        DataSourceExec::from_data_source(source)
    }

    /// Window functions over `PARTITION BY p1, p2 ORDER BY o`, or over `ORDER BY o` alone.
    fn window_exprs(partitioned: bool) -> Vec<Arc<dyn WindowExpr>> {
        let state = SessionContext::new().state();
        let schema = schema();
        let partition_by = if partitioned {
            vec![col("p1", &schema).unwrap(), col("p2", &schema).unwrap()]
        } else {
            vec![]
        };
        let order_by = [PhysicalSortExpr::new_default(col("o", &schema).unwrap())];
        let ranking = Arc::new(WindowFrame::new(Some(true)));
        let whole_partition = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::Following(ScalarValue::UInt64(None)),
        ));
        let to_end = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::CurrentRow,
            WindowFrameBound::Following(ScalarValue::Int64(None)),
        ));
        let udwf = |name| WindowFunctionDefinition::WindowUDF(state.udwf(name).unwrap());
        let udaf = |name| WindowFunctionDefinition::AggregateUDF(state.udaf(name).unwrap());
        let v = col("v", &schema).unwrap();
        [
            (udwf("percent_rank"), vec![], Arc::clone(&ranking)),
            (udwf("cume_dist"), vec![], Arc::clone(&ranking)),
            (udwf("ntile"), vec![lit(3i64)], Arc::clone(&ranking)),
            (udaf("sum"), vec![Arc::clone(&v)], whole_partition),
            (udaf("count"), vec![v], to_end),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, (fun, args, frame))| {
            create_window_expr(
                &fun,
                format!("w{i}"),
                &args,
                &partition_by,
                &order_by,
                frame,
                Arc::clone(&schema),
                false,
                false,
                None,
            )
            .unwrap()
        })
        .collect()
    }

    fn comet_window(batches: Vec<RecordBatch>, partitioned: bool) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            CometWindowAggExec::try_new(window_exprs(partitioned), input(batches), partitioned)
                .unwrap(),
        )
    }

    async fn run(batches: Vec<RecordBatch>, partitioned: bool) -> Vec<RecordBatch> {
        collect(
            comet_window(batches, partitioned),
            SessionContext::new().task_ctx(),
        )
        .await
        .unwrap()
    }

    /// Checks `output` against what DataFusion's `WindowAggExec` returns for the same input.
    async fn assert_matches_window_agg_exec(
        output: &[RecordBatch],
        batches: Vec<RecordBatch>,
        partitioned: bool,
    ) {
        assert!(output.iter().all(|batch| batch.num_rows() > 0));
        let plan = Arc::new(
            WindowAggExec::try_new(window_exprs(partitioned), input(batches), partitioned).unwrap(),
        );
        let schema = plan.schema();
        let expected = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        assert_eq!(
            concat_batches(&schema, output).unwrap(),
            concat_batches(&schema, &expected).unwrap()
        );
    }

    fn row_counts(batches: &[RecordBatch]) -> Vec<usize> {
        batches.iter().map(RecordBatch::num_rows).collect()
    }

    /// Runs `plan` with a `limit`-byte pool. On success, also returns the peak the window
    /// reserved, after checking that it released everything.
    async fn run_with_limit(
        plan: Arc<dyn ExecutionPlan>,
        limit: usize,
    ) -> Result<(Vec<RecordBatch>, usize)> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(limit, 1.0)
            .build_arc()?;
        let context =
            SessionContext::new_with_config_rt(SessionConfig::new(), Arc::clone(&runtime))
                .task_ctx();
        let mut stream = plan.execute(0, context)?;
        let mut output = vec![];
        while let Some(batch) = stream.next().await {
            output.push(batch?);
        }
        // Read the consumer before dropping the stream unregisters it.
        let consumer = runtime
            .memory_pool
            .downcast_ref::<TrackConsumersPool<GreedyMemoryPool>>()
            .unwrap()
            .metrics()
            .into_iter()
            .find(|consumer| consumer.name == CONSUMER)
            .unwrap();
        assert_eq!(consumer.reserved, 0);
        Ok((output, consumer.peak))
    }

    fn assert_refused(error: DataFusionError) {
        let message = error.to_string();
        assert!(
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
            "{message}"
        );
        assert!(message.contains(CONSUMER), "{message}");
        assert!(
            message.contains("CometWindowAggExec cannot spill"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn matches_window_agg_exec() {
        for seed in 0..20 {
            let mut rng = StdRng::seed_from_u64(seed);
            let sizes: Vec<usize> = (0..rng.random_range(1..40))
                .map(|_| match rng.random_range(0..10) {
                    0 => rng.random_range(50..200),
                    _ => rng.random_range(1..8),
                })
                .collect();
            let rows = sorted_rows(&sizes, &mut rng);
            let batches = random_slices(&rows, rng.random_range(1..40), &mut rng);
            for partitioned in [true, false] {
                // Shown only if the test fails.
                println!("seed={seed}, partitioned={partitioned}");
                let output = run(batches.clone(), partitioned).await;
                assert_matches_window_agg_exec(&output, batches.clone(), partitioned).await;
            }
        }
    }

    #[tokio::test]
    async fn emits_each_window_partition_once_the_next_one_starts() {
        let mut rng = StdRng::seed_from_u64(0);
        let rows = sorted_rows(&[3, 3, 5, 1], &mut rng);
        // Batch boundaries after rows 3 (between window partitions), 6, 8 and 10 (inside one).
        let batches = [0..3, 3..6, 6..8, 8..10, 10..12]
            .map(|range| rows.slice(range.start, range.len()))
            .to_vec();
        let output = run(batches.clone(), true).await;
        // The second batch emits the first window partition, and the third the second. The
        // third window partition spans three batches and is emitted with the batch after it.
        assert_eq!(row_counts(&output), [3, 3, 5, 1]);
        assert_matches_window_agg_exec(&output, batches.clone(), true).await;

        // Without PARTITION BY, all rows are one window partition.
        assert_eq!(row_counts(&run(batches, false).await), [12]);
    }

    #[tokio::test]
    async fn empty_input_and_empty_batches() {
        let empty = RecordBatch::new_empty(schema());
        for partitioned in [true, false] {
            assert!(run(vec![], partitioned).await.is_empty());
            assert!(run(vec![empty.clone()], partitioned).await.is_empty());

            let mut rng = StdRng::seed_from_u64(0);
            let rows = sorted_rows(&[2, 2], &mut rng);
            let batches = vec![
                empty.clone(),
                rows.slice(0, 1),
                empty.clone(),
                rows.slice(1, 3),
                empty.clone(),
            ];
            let output = run(batches.clone(), partitioned).await;
            assert_matches_window_agg_exec(&output, batches, partitioned).await;
        }
    }

    #[tokio::test]
    async fn buffers_one_window_partition_at_a_time() {
        let mut rng = StdRng::seed_from_u64(0);
        let batches = chunks(&sorted_rows(&[50; 1000], &mut rng), 64);
        let limit = 8 * memory_size(&batches[..1]);
        assert!(memory_size(&batches) > 50 * limit);
        let (output, peak) = run_with_limit(comet_window(batches.clone(), true), limit)
            .await
            .unwrap();
        assert!(peak > 0);
        assert_matches_window_agg_exec(&output, batches, true).await;

        // The same number of rows in one window partition does not fit.
        let batches = chunks(&sorted_rows(&[50_000], &mut rng), 64);
        assert_refused(
            run_with_limit(comet_window(batches, true), limit)
                .await
                .unwrap_err(),
        );
    }

    #[tokio::test]
    async fn batches_sliced_from_one_list_array_are_charged_once() {
        // Ten batches sliced from one batch share its list values. Estimating each batch's
        // share of the copy from its slice would charge every batch for all of them.
        let item = Arc::new(Field::new_list_field(DataType::Int64, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("a", DataType::List(Arc::clone(&item)), false),
        ]));
        let list = ListArray::new(
            item,
            OffsetBuffer::from_lengths([64; 10_000]),
            Arc::new(Int64Array::from_iter_values(0..640_000)),
            None,
        );
        let rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..10_000)),
                Arc::new(list),
            ],
        )
        .unwrap();
        let batches = (0..10).map(|i| rows.slice(i * 1_000, 1_000)).collect();
        // COUNT(k) OVER (), so all the rows are one window partition.
        let count = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(
                SessionContext::new().state().udaf("count").unwrap(),
            ),
            "count".to_string(),
            &[col("k", &schema).unwrap()],
            &[],
            &[],
            Arc::new(WindowFrame::new(None)),
            Arc::clone(&schema),
            false,
            false,
            None,
        )
        .unwrap();
        let input = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
        let plan = Arc::new(CometWindowAggExec::try_new(vec![count], input, false).unwrap());

        // The rows fit together with their copy.
        let (output, _) = run_with_limit(plan, 3 * memory_size(&[rows]))
            .await
            .unwrap();
        assert_eq!(row_counts(&output), [10_000]);
        let counts = output[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(counts.iter().all(|count| count == Some(10_000)));
    }

    #[tokio::test]
    async fn concatenated_copy_is_reserved() {
        let mut rng = StdRng::seed_from_u64(0);
        let rows = sorted_rows(&[640], &mut rng);
        let batches = chunks(&rows, 64);
        let inputs = memory_size(&batches);
        for partitioned in [true, false] {
            // The batches fit, but not together with the copy made to evaluate them.
            assert_refused(
                run_with_limit(comet_window(batches.clone(), partitioned), inputs * 3 / 2)
                    .await
                    .unwrap_err(),
            );
            let (output, peak) =
                run_with_limit(comet_window(batches.clone(), partitioned), inputs * 3)
                    .await
                    .unwrap();
            assert!(peak > inputs * 3 / 2, "peak={peak}, inputs={inputs}");
            assert_eq!(row_counts(&output), [640]);
        }
    }
}
