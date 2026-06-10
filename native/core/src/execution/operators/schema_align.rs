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

//! `SchemaAlignExec` reshapes its child's output so the per-column Arrow type and field-level
//! nullability match what Spark catalyst declared, casting where necessary. Sits between a native
//! subtree and `ShuffleWriterExec` so DataFusion / `datafusion-spark` return-type drift is caught
//! before it reaches shuffle blocks. See
//! <https://github.com/apache/datafusion-comet/issues/4515> for the running list of mismatched
//! functions.

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{Field, Schema, SchemaRef};
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
    any::Any,
    collections::HashSet,
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
                ColumnAction::Cast
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
}

impl SchemaAlignStream {
    fn align(&self, batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
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
            };
            columns.push(aligned);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        RecordBatch::try_new_with_options(Arc::clone(&self.target_schema), columns, &options)
            .map_err(DataFusionError::from)
    }
}

impl Stream for SchemaAlignStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.child_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.align(batch))),
            other => other,
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
