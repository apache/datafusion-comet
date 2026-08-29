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

use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream,
    },
};
use datafusion_comet_common::{cast_and_stamp_schema, widen_nested_nullability};
use futures::{Stream, StreamExt};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// A Comet native operator that expands a single row into multiple rows. This behaves as same as
/// Spark Expand operator.
#[derive(Debug)]
pub struct ExpandExec {
    projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl ExpandExec {
    /// Create a new ExpandExec
    pub fn new(
        projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
        child: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Self {
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Self {
            projections,
            child,
            schema,
            cache,
        }
    }

    /// Derives the operator's output schema from *every* projection, not just the first.
    ///
    /// Each projection produces the same output columns, so their types agree except where a
    /// native kernel drifts on nested field nullability. Widening each column's nested `nullable`
    /// flags across all projections gives a schema that is valid for all of them; deriving it from
    /// `projections[0]` alone yields a schema that the remaining projections cannot be stamped
    /// with. See <https://github.com/apache/datafusion-comet/issues/5137>.
    pub fn build_schema(
        projections: &[Vec<Arc<dyn PhysicalExpr>>],
        input_schema: &SchemaRef,
    ) -> Result<SchemaRef, DataFusionError> {
        let mut data_types = projections
            .first()
            .ok_or_else(|| {
                DataFusionError::Internal("Expand should have at least one projection".to_string())
            })?
            .iter()
            .map(|expr| expr.data_type(input_schema))
            .collect::<Result<Vec<_>, _>>()?;

        for projection in &projections[1..] {
            if projection.len() != data_types.len() {
                return Err(DataFusionError::Internal(format!(
                    "Expand projections produce differing column counts: {} and {}",
                    data_types.len(),
                    projection.len()
                )));
            }
            for (data_type, expr) in data_types.iter_mut().zip(projection) {
                *data_type = widen_nested_nullability(data_type, &expr.data_type(input_schema)?);
            }
        }

        // `col_{idx}` is the name the planner used before this logic moved here, kept so that only
        // the nullability derivation changes. A projection is an arbitrary expression with no
        // natural output name, and parent operators bind to these columns by index (a bound
        // reference takes its name from the child schema), so the name is positional either way.
        let fields: Vec<Field> = data_types
            .into_iter()
            .enumerate()
            .map(|(idx, dt)| Field::new(format!("col_{idx}"), dt, true))
            .collect();
        Ok(Arc::new(Schema::new(fields)))
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometExpandExec")?;
                write!(f, "Projections: [")?;
                for projection in &self.projections {
                    write!(f, "[")?;
                    for expr in projection {
                        write!(f, "{expr}, ")?;
                    }
                    write!(f, "], ")?;
                }
                write!(f, "]")?;

                Ok(())
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for ExpandExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let new_expand = ExpandExec::new(
            self.projections.clone(),
            Arc::clone(&children[0]),
            Arc::clone(&self.schema),
        );
        Ok(Arc::new(new_expand))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let expand_stream = ExpandStream::new(
            self.projections.clone(),
            child_stream,
            Arc::clone(&self.schema),
        );
        Ok(Box::pin(expand_stream))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn name(&self) -> &str {
        "CometExpandExec"
    }
}

pub struct ExpandStream {
    projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    child_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    current_index: i32,
    max_index: i32,
    current_batch: Option<RecordBatch>,
}

impl ExpandStream {
    /// Create a new ExpandStream
    pub fn new(
        projections: Vec<Vec<Arc<dyn PhysicalExpr>>>,
        child_stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Self {
        let max_index = projections.len() as i32;
        Self {
            projections,
            child_stream,
            schema,
            current_index: -1,
            max_index,
            current_batch: None,
        }
    }

    fn expand(
        &self,
        batch: &RecordBatch,
        projection: &[Arc<dyn PhysicalExpr>],
    ) -> Result<RecordBatch, DataFusionError> {
        let mut columns = vec![];

        projection.iter().try_for_each(|expr| {
            let column = expr.evaluate(batch)?;
            columns.push(column.into_array(batch.num_rows())?);

            Ok::<(), DataFusionError>(())
        })?;

        // A projection whose nested nullability is narrower than the operator's declared schema is
        // reconciled here rather than rejected by the stamp.
        cast_and_stamp_schema("CometExpandExec", &self.schema, columns, batch.num_rows())
    }
}

impl Stream for ExpandStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.current_index == -1 {
            let next = self.child_stream.poll_next_unpin(cx);
            match next {
                Poll::Ready(Some(Ok(batch))) => {
                    self.current_batch = Some(batch);
                    self.current_index = 0;
                }
                other => return other,
            }
        }
        assert!(self.current_batch.is_some());

        let projection = &self.projections[self.current_index as usize];
        let batch = self.expand(self.current_batch.as_ref().unwrap(), projection);

        self.current_index += 1;

        if self.current_index == self.max_index {
            self.current_index = -1;
            self.current_batch = None;
        }
        Poll::Ready(Some(batch))
    }
}

impl RecordBatchStream for ExpandStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    //! `ExpandExec` stamps its declared schema onto the output of every projection. Nested field
    //! nullability is part of arrow's `DataType` identity, so a projection whose nested `nullable`
    //! flags differ from the declared schema used to abort the task. These tests pin both halves of
    //! the fix independently: `build_schema` widens the declared schema across all projections, and
    //! the stamp reconciles anything still narrower. See
    //! <https://github.com/apache/datafusion-comet/issues/5137>.

    use super::*;
    use crate::execution::operators::nested_nullability_fixture::{
        list_of_struct, list_of_struct_type,
    };
    use arrow::array::{Array, ListArray};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    /// Child with two columns of the same logical `array<struct<id,flag>>` type that disagree on
    /// the `flag` field's nullability, so that projecting one per Expand projection reproduces the
    /// drift.
    fn drifting_child() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", list_of_struct_type(true), true),
            Field::new("b", list_of_struct_type(false), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![list_of_struct(true), list_of_struct(false)],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn drifting_projections() -> Vec<Vec<Arc<dyn PhysicalExpr>>> {
        vec![
            vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>],
            vec![Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>],
        ]
    }

    async fn expand_all(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
        collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap()
    }

    /// Each output batch must carry the operator's declared schema and the values the projection
    /// produced, unchanged.
    fn assert_expanded(batches: &[RecordBatch], schema: &SchemaRef) {
        assert_eq!(batches.len(), 2, "one batch per projection");
        for batch in batches {
            assert_eq!(&batch.schema(), schema);
            assert_eq!(batch.num_rows(), 2);
            let list = batch
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(list.value(0).len(), 2);
            assert_eq!(list.value(1).len(), 1);
        }
    }

    /// `build_schema` must widen the nested `nullable` flags across all projections, not read them
    /// off `projections[0]`.
    #[test]
    fn build_schema_widens_nested_nullability_across_projections() {
        let child = drifting_child();
        // `projections[0]` alone would declare the widened type here, so drive the widening from
        // the narrow side to prove it is not just projections[0] being echoed back.
        let projections = vec![
            vec![Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>],
            vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>],
        ];
        let schema = ExpandExec::build_schema(&projections, &child.schema()).unwrap();
        assert_eq!(
            schema.field(0).data_type(),
            &list_of_struct_type(true),
            "the flag field must be nullable because projection[1] produces it nullable"
        );
    }

    #[test]
    fn build_schema_rejects_projections_of_differing_width() {
        let child = drifting_child();
        let projections = vec![
            vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>],
            vec![
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>,
            ],
        ];
        let err = ExpandExec::build_schema(&projections, &child.schema()).unwrap_err();
        assert!(err.to_string().contains("differing column counts"), "{err}");
    }

    /// End-to-end: projections that disagree on nested nullability both expand successfully under
    /// the widened schema.
    #[tokio::test]
    async fn expands_projections_with_divergent_nested_nullability() {
        let child = drifting_child();
        let projections = drifting_projections();
        let schema = ExpandExec::build_schema(&projections, &child.schema()).unwrap();
        let expand = Arc::new(ExpandExec::new(projections, child, Arc::clone(&schema)));
        assert_expanded(&expand_all(expand).await, &schema);
    }

    /// The stamp reconciles on its own: even given the narrow `projections[0]`-derived schema that
    /// the planner used to build, the projection producing a non-null child no longer aborts.
    #[tokio::test]
    async fn stamp_reconciles_a_schema_narrower_than_a_projection() {
        let child = drifting_child();
        let narrow = Arc::new(Schema::new(vec![Field::new(
            "col_0",
            list_of_struct_type(false),
            true,
        )]));
        let expand = Arc::new(ExpandExec::new(
            drifting_projections(),
            child,
            Arc::clone(&narrow),
        ));
        assert_expanded(&expand_all(expand).await, &narrow);
    }

    /// When no projection drifts, the schema is exactly what `projections[0]` yields, and the stamp
    /// is a no-op passthrough.
    #[tokio::test]
    async fn no_drift_is_unchanged() {
        let child = drifting_child();
        let projections = vec![
            vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>],
            vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>],
        ];
        let schema = ExpandExec::build_schema(&projections, &child.schema()).unwrap();
        assert_eq!(schema.field(0).data_type(), &list_of_struct_type(true));
        let expand = Arc::new(ExpandExec::new(projections, child, Arc::clone(&schema)));
        assert_expanded(&expand_all(expand).await, &schema);
    }
}
