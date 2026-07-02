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

use crate::execution::operators::{copy_or_unpack_array, AlignedArrowStreamReader, CopyMode};
use crate::{errors::CometError, execution::planner::TEST_EXEC_CONTEXT_ID};
use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use datafusion::common::{arrow_datafusion_err, DataFusionError, Result as DataFusionResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::{
    execution::TaskContext,
    physical_expr::*,
    physical_plan::{ExecutionPlan, *},
};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use std::{
    fmt::Debug,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

/// Abstraction over the source that feeds batches into a [`ScanExec`]. `ScanExec` was originally
/// hard-wired to a JVM-exported `ArrowArrayStream` (via [`AlignedArrowStreamReader`]); this trait
/// lets it be driven by that reader OR by a purely native producer (e.g. a DataFusion
/// `SendableRecordBatchStream`), with no JVM involved in the latter case. Mirrors
/// `Iterator<Item = Result<RecordBatch, ArrowError>>`, which `AlignedArrowStreamReader` already
/// implements, but is spelled out as `next_batch` so it stays object-safe (`Iterator` itself
/// isn't dyn-compatible because of its many default/adapter methods).
pub trait InputBatchStream: Send + Debug {
    /// Pull the next batch. `Ok(None)` signals end of stream.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError>;
}

impl InputBatchStream for AlignedArrowStreamReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.next().transpose()
    }
}

/// Feeds a `ScanExec` from a native DataFusion [`SendableRecordBatchStream`] instead of a
/// JVM-exported `ArrowArrayStream`. This is what lets a `ScanExec` sit at the bottom of a plan
/// fed purely natively, e.g. by a future Ballista shuffle-reader stream.
///
/// `SendableRecordBatchStream` is async, but `ScanExec` pulls its input synchronously (via
/// `reader.next()`/`next_batch()` from `poll_next`, itself invoked off the batch producer thread
/// rather than awaited). To bridge that without restructuring `ScanExec`, each call blocks the
/// current thread on the stream's next item via `futures::executor::block_on`. This is safe here
/// because `block_on` merely parks the calling thread on a `Future`/`Waker` pair — unlike
/// `Runtime::block_on`, it does not require (and will not panic inside) an existing Tokio
/// runtime, so it composes with Comet's own executor threads. It does mean the calling thread is
/// unavailable for other work while a batch is pending, which is fine for the in-memory /
/// channel-backed producers this abstraction targets (e.g. a shuffle reader), but would be a poor
/// fit for a producer that itself does blocking I/O on the same thread.
pub struct NativeBatchStream {
    stream: SendableRecordBatchStream,
}

impl NativeBatchStream {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

impl Debug for NativeBatchStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeBatchStream").finish_non_exhaustive()
    }
}

impl InputBatchStream for NativeBatchStream {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        match futures::executor::block_on(self.stream.next()) {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(ArrowError::ExternalError(Box::new(e))),
        }
    }
}

/// `ScanExec` reads batches of data from an upstream [`InputBatchStream`]. The common case is
/// Spark, over the Arrow C Stream Interface: the `input_source` is moved out of the
/// JVM-exported `ArrowArrayStream` at plan-construction time via `AlignedArrowStreamReader`;
/// dropping the reader (when this exec drops) fires the stream's release callback, which closes
/// the JVM-side `ArrowReader` and its `VectorSchemaRoot`. `ScanExec` can equally be fed by a
/// native `SendableRecordBatchStream` (see [`ScanExec::new_native`] / [`NativeBatchStream`]),
/// with no JVM involved.
#[derive(Debug, Clone)]
pub struct ScanExec {
    /// JVM execution-context id used to look up the `JNIEnv` for callbacks.
    pub exec_context_id: i64,
    /// The batch source: the C Stream Interface reader for the JVM path, or a native
    /// [`NativeBatchStream`] wrapping a `SendableRecordBatchStream`. `None` only in unit tests
    /// that seed input via `set_input_batch`.
    pub input_source: Option<Arc<Mutex<dyn InputBatchStream>>>,
    pub input_source_description: String,
    pub data_types: Vec<DataType>,
    pub schema: SchemaRef,
    /// Used in unit tests to mock the input batch; otherwise written by `pull_next` on each
    /// poll.
    pub batch: Arc<Mutex<Option<InputBatch>>>,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    baseline_metrics: BaselineMetrics,
}

impl ScanExec {
    pub fn new(
        exec_context_id: i64,
        input_source: Option<Arc<Mutex<dyn InputBatchStream>>>,
        input_source_description: &str,
        data_types: Vec<DataType>,
    ) -> Result<Self, CometError> {
        let metrics_set = ExecutionPlanMetricsSet::default();
        let baseline_metrics = BaselineMetrics::new(&metrics_set, 0);

        // Build schema directly from data types since get_next now always unpacks dictionaries
        let schema = schema_from_data_types(&data_types);

        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            // The partitioning is not important because we are not using DataFusion's
            // query planner or optimizer
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Ok(Self {
            exec_context_id,
            input_source,
            input_source_description: input_source_description.to_string(),
            data_types,
            batch: Arc::new(Mutex::new(None)),
            cache,
            metrics: metrics_set,
            baseline_metrics,
            schema,
        })
    }

    /// Convenience constructor for a `ScanExec` fed by a native `SendableRecordBatchStream`
    /// (no JVM involved), e.g. a Ballista shuffle-reader stream. Wraps `stream` in a
    /// [`NativeBatchStream`] and delegates to [`ScanExec::new`].
    pub fn new_native(
        exec_context_id: i64,
        stream: SendableRecordBatchStream,
        input_source_description: &str,
        data_types: Vec<DataType>,
    ) -> Result<Self, CometError> {
        let input_source: Arc<Mutex<dyn InputBatchStream>> =
            Arc::new(Mutex::new(NativeBatchStream::new(stream)));
        Self::new(
            exec_context_id,
            Some(input_source),
            input_source_description,
            data_types,
        )
    }

    /// Inject a native [`SendableRecordBatchStream`] into an already-constructed
    /// `ScanExec` handle (e.g. one returned by `PhysicalPlanner::create_plan` for a
    /// `Scan` leaf, which is built with `input_source = None`). This is what lets a
    /// [`crate::execution::fragment`] feed the fragment's `Scan` leaves from its
    /// DataFusion children after the plan has been built.
    ///
    /// A non-`TEST_EXEC_CONTEXT_ID` `exec_context_id` MUST be supplied so that
    /// `pull_next` actually pulls from the stream instead of short-circuiting to
    /// EOF (that short-circuit is reserved for unit tests that seed batches via
    /// `set_input_batch`). Only the handle that `get_next_batch` is driven on needs
    /// this — the executable leaf shares this handle's `batch` slot (an `Arc`), so
    /// batches pulled here become visible to the plan node without touching it.
    pub fn set_native_input(
        &mut self,
        exec_context_id: i64,
        stream: SendableRecordBatchStream,
    ) {
        self.exec_context_id = exec_context_id;
        self.input_source = Some(Arc::new(Mutex::new(NativeBatchStream::new(stream))));
    }

    /// Unpack all dictionary types because some DataFusion operators
    /// and expressions do not support dictionary types
    fn unpack_dictionary_type(dt: &DataType) -> DataType {
        if let DataType::Dictionary(_, vt) = dt {
            // return the underlying data type
            return vt.as_ref().clone();
        }
        dt.clone()
    }

    /// Feeds input batch into this `Scan`. Only used in unit test.
    pub fn set_input_batch(&mut self, input: InputBatch) {
        *self.batch.try_lock().unwrap() = Some(input);
    }

    /// Pull next input batch from the upstream `ArrowArrayStreamReader`.
    pub fn get_next_batch(&mut self) -> Result<(), CometError> {
        if self.input_source.is_none() {
            // This is a unit test. Input batches are seeded via `set_input_batch`.
            return Ok(());
        }

        let mut current_batch = self.batch.try_lock().unwrap();
        if current_batch.is_none() {
            let mut timer = self.baseline_metrics.elapsed_compute().timer();
            let next_batch =
                ScanExec::pull_next(self.exec_context_id, self.input_source.as_ref().unwrap())?;
            *current_batch = Some(next_batch);
            timer.stop();
        }

        Ok(())
    }

    /// Pull the next `RecordBatch` from the stream and convert it to an `InputBatch`. Dictionary
    /// columns are unpacked because Comet's downstream operators do not handle them.
    fn pull_next(
        exec_context_id: i64,
        reader: &Arc<Mutex<dyn InputBatchStream>>,
    ) -> Result<InputBatch, CometError> {
        if exec_context_id == TEST_EXEC_CONTEXT_ID {
            // Unit test path; input batches are seeded directly.
            return Ok(InputBatch::EOF);
        }

        // The `Mutex` is for interior mutability (`next_batch` needs `&mut`, but the exec holds
        // the reader behind an `Arc`); access is already serialized by the `self.batch` lock held
        // in `get_next_batch`, so a contended `try_lock` here would signal a caller bug, not
        // races.
        let mut reader = reader
            .try_lock()
            .map_err(|_| CometError::Internal("input batch stream contended".to_string()))?;

        match reader.next_batch()? {
            None => Ok(InputBatch::EOF),
            Some(record_batch) => {
                let num_rows = record_batch.num_rows();
                let columns = record_batch.columns();
                let mut inputs: Vec<ArrayRef> = Vec::with_capacity(columns.len());
                for col in columns {
                    inputs.push(copy_or_unpack_array(col, &CopyMode::UnpackOrClone)?);
                }
                Ok(InputBatch::new(inputs, Some(num_rows)))
            }
        }
    }
}

fn schema_from_data_types(data_types: &[DataType]) -> SchemaRef {
    let fields = data_types
        .iter()
        .enumerate()
        .map(|(idx, dt)| {
            let datatype = ScanExec::unpack_dictionary_type(dt);
            // We don't use the field name. Put a placeholder.
            Field::new(format!("col_{idx}"), datatype, true)
        })
        .collect::<Vec<Field>>();

    Arc::new(Schema::new(fields))
}

impl ExecutionPlan for ScanExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(ScanStream::new(
            self.clone(),
            self.schema(),
            partition,
            self.baseline_metrics.clone(),
        )))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn name(&self) -> &str {
        "ScanExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for ScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ScanExec: source=[{}], ", self.input_source_description)?;
                let fields: Vec<String> = self
                    .data_types
                    .iter()
                    .enumerate()
                    .map(|(idx, dt)| format!("col_{idx:}: {dt:}"))
                    .collect();
                write!(f, "schema=[{}]", fields.join(", "))?;
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
        Ok(())
    }
}

/// A async-stream feeds input batch from `Scan` into DataFusion physical plan.
struct ScanStream<'a> {
    /// The `Scan` node producing input batches
    scan: ScanExec,
    /// Schema representing the data
    schema: SchemaRef,
    /// Metrics
    baseline_metrics: BaselineMetrics,
    /// Cast options
    cast_options: CastOptions<'a>,
    /// elapsed time for casting columns to different data types during scan
    cast_time: Time,
}

impl ScanStream<'_> {
    pub fn new(
        scan: ScanExec,
        schema: SchemaRef,
        partition: usize,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let cast_time = MetricBuilder::new(&scan.metrics).subset_time("cast_time", partition);
        Self {
            scan,
            schema,
            baseline_metrics,
            cast_options: CastOptions::default(),
            cast_time,
        }
    }

    fn build_record_batch(
        &self,
        columns: &[ArrayRef],
        num_rows: usize,
    ) -> DataFusionResult<RecordBatch, DataFusionError> {
        let schema_fields = self.schema.fields();
        assert_eq!(columns.len(), schema_fields.len());

        // Cast dictionary-encoded primitive arrays to regular arrays and cast
        // Utf8/LargeUtf8/Binary arrays to dictionary-encoded if the schema is
        // defined as dictionary-encoded and the data in this batch is not
        // dictionary-encoded (could also be the other way around)
        let new_columns: Vec<ArrayRef> = columns
            .iter()
            .zip(schema_fields.iter())
            .map(|(column, f)| {
                if column.data_type() != f.data_type() {
                    let mut timer = self.cast_time.timer();
                    let cast_array = cast_with_options(column, f.data_type(), &self.cast_options);
                    timer.stop();
                    cast_array
                } else {
                    Ok(Arc::clone(column))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        RecordBatch::try_new_with_options(Arc::clone(&self.schema), new_columns, &options)
            .map_err(|e| arrow_datafusion_err!(e))
    }
}

impl Stream for ScanStream<'_> {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut timer = self.baseline_metrics.elapsed_compute().timer();
        let mut scan_batch = self.scan.batch.try_lock().unwrap();

        let input_batch = &*scan_batch;
        let input_batch = if let Some(batch) = input_batch {
            batch
        } else {
            timer.stop();
            return Poll::Pending;
        };

        let result = match input_batch {
            InputBatch::EOF => Poll::Ready(None),
            InputBatch::Batch(columns, num_rows) => {
                self.baseline_metrics.record_output(*num_rows);
                let maybe_batch = self.build_record_batch(columns, *num_rows);
                Poll::Ready(Some(maybe_batch))
            }
        };

        *scan_batch = None;

        timer.stop();

        result
    }
}

impl RecordBatchStream for ScanStream<'_> {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::memory::MemoryStream;

    /// A `ScanExec` fed by a native `SendableRecordBatchStream` (no JVM involved) must pass
    /// batches through unchanged, row-for-row and value-for-value. This is the enabling case for
    /// a future non-JVM producer (e.g. a Ballista shuffle-reader stream) driving a Comet
    /// fragment's `ScanExec` leaf.
    #[test]
    fn scan_exec_reads_native_record_batch_stream() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef],
        )
        .unwrap();

        let mem_stream = MemoryStream::try_new(
            vec![batch1.clone(), batch2.clone()],
            Arc::clone(&schema),
            None,
        )
        .unwrap();
        let native_stream: SendableRecordBatchStream = Box::pin(mem_stream);

        // Any id other than `TEST_EXEC_CONTEXT_ID` so `pull_next` actually pulls from the native
        // stream instead of short-circuiting to EOF (that short-circuit is what lets *other*
        // unit tests seed batches directly via `set_input_batch`).
        let exec_context_id = TEST_EXEC_CONTEXT_ID + 1;
        let mut scan = ScanExec::new_native(
            exec_context_id,
            native_stream,
            "native-test-stream",
            vec![DataType::Int32],
        )
        .unwrap();

        let task_ctx = Arc::new(TaskContext::default());
        let mut output_stream = scan.execute(0, task_ctx).unwrap();

        let mut collected = Vec::new();
        loop {
            scan.get_next_batch().unwrap();
            match futures::executor::block_on(output_stream.next()) {
                Some(Ok(batch)) => collected.push(batch),
                Some(Err(e)) => panic!("unexpected error polling ScanExec: {e}"),
                None => break,
            }
        }

        assert_eq!(
            collected.len(),
            2,
            "expected both native batches to pass through"
        );
        // `ScanExec` rebuilds the schema from `data_types` with placeholder field names (see
        // `schema_from_data_types`), so compare columns/row counts rather than full batch/schema
        // equality.
        assert_eq!(collected[0].columns(), batch1.columns());
        assert_eq!(collected[1].columns(), batch2.columns());
        let total_rows: usize = collected.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }
}

#[derive(Clone, Debug)]
pub enum InputBatch {
    /// The end of input batches.
    EOF,

    /// A normal batch with columns and a number of rows.
    /// It is possible to have a zero-column batch with a non-zero number of rows,
    /// i.e. reading empty schema from scan.
    Batch(Vec<ArrayRef>, usize),
}

impl InputBatch {
    /// Constructs an ` InputBatch ` from columns and an optional number of rows.
    /// If `num_rows` is none, this function will calculate it from given
    /// columns.
    pub fn new(columns: Vec<ArrayRef>, num_rows: Option<usize>) -> Self {
        let num_rows = num_rows.unwrap_or_else(|| {
            let lengths = columns.iter().map(|a| a.len()).unique().collect::<Vec<_>>();
            assert!(lengths.len() <= 1, "Columns have different lengths.");

            if lengths.is_empty() {
                // All are scalar values
                1
            } else {
                lengths[0]
            }
        });

        InputBatch::Batch(columns, num_rows)
    }
}
