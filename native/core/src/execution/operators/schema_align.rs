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
//! nullability match what Spark catalyst declared. Used between an inlined native subtree and
//! `ShuffleWriterExec` when the FFI deep-copy + `ScanExec` cast in `build_record_batch` are both
//! gone, so DataFusion / `datafusion-spark` return-type drift would otherwise be written into
//! shuffle blocks. See <https://github.com/apache/datafusion-comet/issues/4515> for the running
//! list of mismatched functions.

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
    /// but widens nullability to `actual.nullable || expected.nullable` (matching the
    /// reconciliation rule used at the FFI boundary on `main`).
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
