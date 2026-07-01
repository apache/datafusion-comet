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

//! `SchemaAlignExec` reshapes a shuffle writer's input so each column's Arrow type and field-level
//! nullability match what Spark catalyst declared, casting where necessary.
//!
//! This concern is enclosed by shuffle on purpose: everywhere else in the native runtime,
//! return-type drift from DataFusion / `datafusion-spark` is self-healing. When a native plan's
//! output crosses back to the JVM and feeds another native plan, the consuming `ScanExec` casts
//! every imported column to the catalyst-declared type, so a wrong Arrow type never survives the
//! boundary. Shuffle is the lone exception, on two counts:
//!
//!   1. The writer hash-partitions on these columns, and Spark's hash differs by type (e.g. `Int32`
//!      vs `Int64`), so a drifted type would route rows to the wrong partition. A read-side cast
//!      cannot undo a wrong partition assignment, so the type must be corrected before partitioning.
//!   2. The shuffle read path (`ShuffleScanExec`) does not cast; it stamps the catalyst schema onto
//!      the decoded block and errors on any mismatch. The schema is serialized into the block on
//!      write and trusted on read.
//!
//! Both force the alignment to happen on the writer input. See
//! <https://github.com/apache/datafusion-comet/issues/4515> for the running list of mismatched
//! functions.

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, LargeBinaryArray, LargeStringArray, RecordBatch,
    RecordBatchOptions, StringBuilder,
};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream,
    },
};
use futures::{Stream, StreamExt};
use std::{
    collections::{HashSet, VecDeque},
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
};

/// Process-wide set of `(column, actual, expected)` signatures we have already warned about.
/// Each schema drift produces the same warning on every partition of every query that runs
/// the offending expression; deduping here keeps logs readable while still surfacing each
/// distinct mismatch once.
fn warn_dedup() -> &'static Mutex<HashSet<String>> {
    static SET: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    SET.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Casts each column of `child`'s output to the data_type Spark catalyst declared, widening
/// nullability to `actual.nullable || expected.nullable`. See
/// <https://github.com/apache/datafusion-comet/issues/4515>.
#[derive(Debug)]
pub struct SchemaAlignExec {
    child: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
    column_actions: Arc<Vec<ColumnAction>>,
    cache: Arc<PlanProperties>,
}

#[derive(Debug, Clone)]
enum ColumnAction {
    /// Pass the input column through unchanged. Any nullability/metadata difference is
    /// absorbed when the batch is re-stamped via `RecordBatch::try_new_with_options`.
    Passthrough,
    /// Cast the input column to the target data_type.
    Cast,
    /// Cast a LargeUtf8 input down to Utf8 (i64 -> i32 offsets). Listed separately because
    /// the cast kernel rejects any column whose values buffer exceeds `i32::MAX`, so the
    /// stream must pre-split input batches into row ranges whose per-batch byte total
    /// stays under that cap.
    CastLargeStringToString,
    /// Cast a LargeBinary input down to Binary (i64 -> i32 offsets). Same shrink-split
    /// constraint as `CastLargeStringToString`.
    CastLargeBinaryToBinary,
}

impl SchemaAlignExec {
    /// Build a SchemaAlignExec that aligns `child`'s output to `expected`. Returns
    /// `Ok(child)` unchanged when no per-column reshape is needed; otherwise wraps `child`
    /// in a SchemaAlignExec whose target schema preserves `expected`'s data_type and metadata
    /// but widens nullability to `actual.nullable || expected.nullable`.
    pub fn try_new_or_passthrough(
        child: Arc<dyn ExecutionPlan>,
        expected: &SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let actual = child.schema();
        if actual.fields().len() != expected.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "SchemaAlignExec: expected {} fields, child produces {}",
                expected.fields().len(),
                actual.fields().len()
            )));
        }
        let mut needs_alignment = false;
        let mut actions = Vec::with_capacity(actual.fields().len());
        let mut target_fields = Vec::with_capacity(actual.fields().len());
        for (idx, (actual_field, expected_field)) in actual
            .fields()
            .iter()
            .zip(expected.fields().iter())
            .enumerate()
        {
            let action = if actual_field.data_type() == expected_field.data_type() {
                ColumnAction::Passthrough
            } else {
                let signature = format!(
                    "{}|{:?}|{:?}",
                    expected_field.name(),
                    actual_field.data_type(),
                    expected_field.data_type()
                );
                if warn_dedup().lock().unwrap().insert(signature) {
                    log::warn!(
                        "ShuffleWriter input schema mismatch on col[{idx}] '{}': child produced \
                         {:?}, catalyst declared {:?}. Inserting a cast; please file the upstream \
                         function bug at https://github.com/apache/datafusion-comet/issues/4515.",
                        expected_field.name(),
                        actual_field.data_type(),
                        expected_field.data_type()
                    );
                }
                match (actual_field.data_type(), expected_field.data_type()) {
                    (DataType::LargeUtf8, DataType::Utf8) => {
                        ColumnAction::CastLargeStringToString
                    }
                    (DataType::LargeBinary, DataType::Binary) => {
                        ColumnAction::CastLargeBinaryToBinary
                    }
                    _ => ColumnAction::Cast,
                }
            };
            let target_nullable = actual_field.is_nullable() || expected_field.is_nullable();
            let field_changed = !matches!(action, ColumnAction::Passthrough)
                || target_nullable != actual_field.is_nullable()
                || expected_field.metadata() != actual_field.metadata()
                || expected_field.name() != actual_field.name();
            if field_changed {
                needs_alignment = true;
            }
            target_fields.push(
                Field::new(
                    expected_field.name(),
                    expected_field.data_type().clone(),
                    target_nullable,
                )
                .with_metadata(expected_field.metadata().clone()),
            );
            actions.push(action);
        }
        if !needs_alignment {
            return Ok(child);
        }
        let target_schema: SchemaRef = Arc::new(Schema::new(target_fields));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&target_schema)),
            child.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(Self {
            child,
            target_schema,
            column_actions: Arc::new(actions),
            cache,
        }))
    }
}

impl DisplayAs for SchemaAlignExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometSchemaAlignExec")
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for SchemaAlignExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.target_schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // Rebuild PlanProperties from the new child since `output_partitioning` may differ.
        let new_child = Arc::clone(&children[0]);
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&self.target_schema)),
            new_child.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(Self {
            child: new_child,
            target_schema: Arc::clone(&self.target_schema),
            column_actions: Arc::clone(&self.column_actions),
            cache,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        Ok(Box::pin(SchemaAlignStream {
            child_stream,
            target_schema: Arc::clone(&self.target_schema),
            column_actions: Arc::clone(&self.column_actions),
            pending: VecDeque::new(),
        }))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn name(&self) -> &str {
        "CometSchemaAlignExec"
    }
}

struct SchemaAlignStream {
    child_stream: SendableRecordBatchStream,
    target_schema: SchemaRef,
    column_actions: Arc<Vec<ColumnAction>>,
    /// Sub-batches produced by the last input batch and not yet yielded. Used when a
    /// `CastLargeStringToString` / `CastLargeBinaryToBinary` column would overflow the
    /// destination `i32` offsets, so the input is split into multiple Utf8/Binary outputs.
    pending: VecDeque<RecordBatch>,
}

/// `i32::MAX` bytes — the cap on a Utf8/Binary values buffer (its offsets are `i32`).
const I32_BYTE_CAP: i64 = i32::MAX as i64;

impl SchemaAlignStream {
    /// Apply the per-column actions to `batch` and push the resulting (possibly multiple)
    /// aligned batches into `out`. Splits the input by row ranges when any
    /// `CastLargeStringToString` / `CastLargeBinaryToBinary` column would otherwise emit a
    /// values buffer larger than `i32::MAX`.
    fn align_into(
        &self,
        batch: RecordBatch,
        out: &mut VecDeque<RecordBatch>,
    ) -> Result<(), DataFusionError> {
        let ranges = self.compute_row_ranges(&batch)?;
        for (start, length) in ranges {
            let slice = if start == 0 && length == batch.num_rows() {
                batch.clone()
            } else {
                batch.slice(start, length)
            };
            out.push_back(self.align_slice(slice)?);
        }
        Ok(())
    }

    /// Apply `column_actions` to a single row range that is already known to fit each
    /// shrinking-cast column's destination offset width.
    fn align_slice(&self, batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        for (idx, action) in self.column_actions.iter().enumerate() {
            let column = batch.column(idx);
            let aligned = match action {
                ColumnAction::Passthrough => Arc::clone(column),
                ColumnAction::Cast => cast_with_options(
                    column,
                    self.target_schema.field(idx).data_type(),
                    &CastOptions::default(),
                )?,
                // Build a fresh Utf8/Binary array from the slice rather than calling
                // arrow's cast kernel. `cast_byte_container` reads the underlying
                // offsets buffer in full and verifies every absolute offset fits the
                // destination offset type — slicing the source array does not rebase
                // the offsets, so a slice that is logically small can still trip the
                // i32::MAX check if its offsets sit far into the values buffer. We
                // copy values explicitly so the new offsets start at 0.
                ColumnAction::CastLargeStringToString => {
                    let arr = column
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "SchemaAlignExec: column[{idx}] expected LargeStringArray, \
                                 got {:?}",
                                column.data_type()
                            ))
                        })?;
                    let mut builder = StringBuilder::with_capacity(arr.len(), 0);
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            builder.append_null();
                        } else {
                            builder.append_value(arr.value(i));
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                ColumnAction::CastLargeBinaryToBinary => {
                    let arr = column
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "SchemaAlignExec: column[{idx}] expected LargeBinaryArray, \
                                 got {:?}",
                                column.data_type()
                            ))
                        })?;
                    let mut builder = BinaryBuilder::with_capacity(arr.len(), 0);
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            builder.append_null();
                        } else {
                            builder.append_value(arr.value(i));
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
            };
            columns.push(aligned);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        RecordBatch::try_new_with_options(Arc::clone(&self.target_schema), columns, &options)
            .map_err(DataFusionError::from)
    }

    /// Compute `(start_row, length)` ranges that split `batch` so each shrinking-cast
    /// column's per-slice values buffer stays under `i32::MAX`. Returns a single full-batch
    /// range when no split is needed (the common case).
    fn compute_row_ranges(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<(usize, usize)>, DataFusionError> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(vec![(0, 0)]);
        }

        // Per-column running-byte cursors used to decide each split point. Empty when no
        // column requires shrink-splitting; in that case we always return a single range.
        let mut shrinking_cols: Vec<&[i64]> = Vec::new();
        for (idx, action) in self.column_actions.iter().enumerate() {
            match action {
                ColumnAction::CastLargeStringToString => {
                    let arr = batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "SchemaAlignExec: column[{idx}] expected LargeStringArray for \
                                 LargeUtf8 -> Utf8 cast, got {:?}",
                                batch.column(idx).data_type()
                            ))
                        })?;
                    shrinking_cols.push(arr.value_offsets());
                }
                ColumnAction::CastLargeBinaryToBinary => {
                    let arr = batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "SchemaAlignExec: column[{idx}] expected LargeBinaryArray for \
                                 LargeBinary -> Binary cast, got {:?}",
                                batch.column(idx).data_type()
                            ))
                        })?;
                    shrinking_cols.push(arr.value_offsets());
                }
                _ => {}
            }
        }

        if shrinking_cols.is_empty() {
            return Ok(vec![(0, num_rows)]);
        }

        let mut ranges: Vec<(usize, usize)> = Vec::new();
        let mut chunk_start = 0usize;
        // The base offsets at the start of the current chunk, one per shrinking col, used to
        // measure each row's contribution to the chunk so far.
        let mut chunk_base: Vec<i64> = shrinking_cols
            .iter()
            .map(|offs| offs[chunk_start])
            .collect();

        for row in 0..num_rows {
            // First pass: reject the whole batch if any single row already exceeds the
            // destination cap on ANY shrinking column. Must scan every column even after
            // a split fires later in this iteration -- otherwise a fat single value in a
            // column past the one that triggered the split would slip through and later
            // panic inside StringBuilder/BinaryBuilder when the row lands in a chunk.
            for (col_idx, offsets) in shrinking_cols.iter().enumerate() {
                let row_bytes = offsets[row + 1] - offsets[row];
                if row_bytes > I32_BYTE_CAP {
                    return Err(DataFusionError::Execution(format!(
                        "SchemaAlignExec: cannot cast Large variant down to small offsets — \
                         row {row} of column[{col_idx}] is {row_bytes} bytes which exceeds the \
                         i32 offset cap ({I32_BYTE_CAP} bytes)"
                    )));
                }
            }

            // Second pass: decide whether to close the current chunk before this row.
            // The oversized-row guard above already ensures the new chunk's first row fits.
            for col_idx in 0..shrinking_cols.len() {
                let projected = shrinking_cols[col_idx][row + 1] - chunk_base[col_idx];
                if projected > I32_BYTE_CAP {
                    let length = row - chunk_start;
                    debug_assert!(length > 0, "split would emit an empty chunk");
                    ranges.push((chunk_start, length));
                    chunk_start = row;
                    for (i, offs) in shrinking_cols.iter().enumerate() {
                        chunk_base[i] = offs[chunk_start];
                    }
                    break;
                }
            }
        }

        // Flush the final chunk (always non-empty: starts at chunk_start <= num_rows-1).
        ranges.push((chunk_start, num_rows - chunk_start));
        Ok(ranges)
    }
}

impl Stream for SchemaAlignStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Drain any sub-batches buffered from a prior split before pulling more input.
            if let Some(ready) = self.pending.pop_front() {
                return Poll::Ready(Some(Ok(ready)));
            }
            match self.child_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let mut buf = std::mem::take(&mut self.pending);
                    if let Err(e) = self.align_into(batch, &mut buf) {
                        self.pending = buf;
                        return Poll::Ready(Some(Err(e)));
                    }
                    self.pending = buf;
                    // Loop back to pop_front and yield the first sub-batch (or pull again on
                    // an input batch that produced zero outputs, e.g. zero-row inputs).
                    continue;
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for SchemaAlignStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.target_schema)
    }
}

#[cfg(test)]
mod tests {
    //! Tests anchored to the concrete drifts tracked in
    //! <https://github.com/apache/datafusion-comet/issues/4515>. This operator exists only to
    //! reshape those drifts before the shuffle writer; once a function's upstream return type is
    //! corrected, its drift pair becomes identical, `try_new_or_passthrough` returns the child
    //! unwrapped, and the corresponding test below flips to the passthrough assertion. That is the
    //! signal the workaround for that function can be removed.

    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, ListArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Int32Type, TimeUnit};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::prelude::SessionContext;

    fn memory_child(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        let config = MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap();
        Arc::new(DataSourceExec::new(Arc::new(config)))
    }

    async fn collect_one_partition(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
        let ctx = SessionContext::new().task_ctx();
        let mut stream = plan.execute(0, ctx).unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        batches
    }

    /// `width_bucket`: catalyst declares `Int64`, DataFusion produces `Int32`. A top-level cast.
    #[tokio::test]
    async fn aligns_width_bucket_int32_to_int64() {
        let child_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            child_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let expected = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let aligned =
            SchemaAlignExec::try_new_or_passthrough(memory_child(batch), &expected).unwrap();
        assert_eq!(aligned.schema().field(0).data_type(), &DataType::Int64);

        let out = collect_one_partition(aligned).await;
        let col = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.values(), &[1, 2, 3]);
    }

    /// `date_trunc`: catalyst declares `Timestamp(us, "UTC")`, DataFusion produces `Timestamp(us)`
    /// with no timezone. The same i64 instants are reinterpreted as UTC; no value should change.
    #[tokio::test]
    async fn aligns_date_trunc_timestamp_timezone() {
        let no_tz = DataType::Timestamp(TimeUnit::Microsecond, None);
        let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let child_schema = Arc::new(Schema::new(vec![Field::new("ts", no_tz, false)]));
        let batch = RecordBatch::try_new(
            child_schema,
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                0, 1_000_000,
            ]))],
        )
        .unwrap();
        let expected = Arc::new(Schema::new(vec![Field::new("ts", utc.clone(), false)]));

        let aligned =
            SchemaAlignExec::try_new_or_passthrough(memory_child(batch), &expected).unwrap();
        assert_eq!(aligned.schema().field(0).data_type(), &utc);

        let out = collect_one_partition(aligned).await;
        let col = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(col.values(), &[0, 1_000_000]);
    }

    /// `collect_set`: catalyst declares `List(non-null Int32)`, DataFusion produces
    /// `List(nullable Int32)`. This narrows the *inner* element nullability, which arrow's list
    /// cast may or may not perform. If it does not, the re-stamp fails on DataType inequality and
    /// this test surfaces that the operator does not actually fix the `collect_set` drift.
    #[tokio::test]
    async fn aligns_collect_set_list_element_nullability() {
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
        ]);
        let actual_list = list_array.data_type().clone();
        let child_schema = Arc::new(Schema::new(vec![Field::new("s", actual_list, true)]));
        let batch = RecordBatch::try_new(child_schema, vec![Arc::new(list_array)]).unwrap();

        let expected_list = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false)));
        let expected = Arc::new(Schema::new(vec![Field::new(
            "s",
            expected_list.clone(),
            true,
        )]));

        let aligned =
            SchemaAlignExec::try_new_or_passthrough(memory_child(batch), &expected).unwrap();
        assert_eq!(aligned.schema().field(0).data_type(), &expected_list);

        let out = collect_one_partition(aligned).await;
        let col = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(col.len(), 2);
        let first = col
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(first, vec![1, 2]);
    }
}
