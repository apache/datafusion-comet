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

use arrow::array::{
    make_array, Array, ArrayRef, GenericListArray, MutableArrayData, RecordBatch,
    RecordBatchOptions,
};
use arrow::datatypes::{DataType, SchemaRef};
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
use futures::{Stream, StreamExt};
use std::{
    any::Any,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// A Comet native operator that explodes an array column into multiple rows.
/// This behaves the same as Spark's explode/explode_outer functions.
#[derive(Debug)]
pub struct ExplodeExec {
    /// The expression that produces the array to explode
    child_expr: Arc<dyn PhysicalExpr>,
    /// Whether this is explode_outer (produces null row for empty/null arrays)
    outer: bool,
    /// Expressions for other columns to project alongside the exploded values
    projections: Vec<Arc<dyn PhysicalExpr>>,
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl ExplodeExec {
    /// Create a new ExplodeExec
    pub fn new(
        child_expr: Arc<dyn PhysicalExpr>,
        outer: bool,
        projections: Vec<Arc<dyn PhysicalExpr>>,
        child: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            child_expr,
            outer,
            projections,
            child,
            schema,
            cache,
        }
    }
}

impl DisplayAs for ExplodeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CometExplodeExec: child_expr={}, outer={}",
                    self.child_expr, self.outer
                )
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for ExplodeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        let new_explode = ExplodeExec::new(
            Arc::clone(&self.child_expr),
            self.outer,
            self.projections.clone(),
            Arc::clone(&children[0]),
            Arc::clone(&self.schema),
        );
        Ok(Arc::new(new_explode))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        let explode_stream = ExplodeStream::new(
            Arc::clone(&self.child_expr),
            self.outer,
            self.projections.clone(),
            child_stream,
            Arc::clone(&self.schema),
        );
        Ok(Box::pin(explode_stream))
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn name(&self) -> &str {
        "CometExplodeExec"
    }
}

pub struct ExplodeStream {
    child_expr: Arc<dyn PhysicalExpr>,
    outer: bool,
    projections: Vec<Arc<dyn PhysicalExpr>>,
    child_stream: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl ExplodeStream {
    /// Create a new ExplodeStream
    pub fn new(
        child_expr: Arc<dyn PhysicalExpr>,
        outer: bool,
        projections: Vec<Arc<dyn PhysicalExpr>>,
        child_stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Self {
        Self {
            child_expr,
            outer,
            projections,
            child_stream,
            schema,
        }
    }

    fn explode(&self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        // Evaluate the array expression
        let array_column = self.child_expr.evaluate(batch)?;
        let array_column = array_column.into_array(batch.num_rows())?;

        // Cast to GenericListArray to access array elements
        let list_array = array_column
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Expected list array for explode, got {:?}",
                    array_column.data_type()
                ))
            })?;

        // Calculate output row count and build row index mapping
        let mut row_indices = Vec::new();
        let mut array_element_indices = Vec::new();

        for row_idx in 0..batch.num_rows() {
            if list_array.is_null(row_idx) {
                // Null array
                if self.outer {
                    // explode_outer produces one null row for null arrays
                    row_indices.push(row_idx);
                    array_element_indices.push(None);
                }
                // else: explode skips null arrays
            } else {
                let array = list_array.value(row_idx);
                let array_len = array.len();

                if array_len == 0 {
                    // Empty array
                    if self.outer {
                        // explode_outer produces one null row for empty arrays
                        row_indices.push(row_idx);
                        array_element_indices.push(None);
                    }
                    // else: explode skips empty arrays
                } else {
                    // Non-empty array: produce one row per element
                    for elem_idx in 0..array_len {
                        row_indices.push(row_idx);
                        array_element_indices.push(Some((row_idx, elem_idx)));
                    }
                }
            }
        }

        let output_row_count = row_indices.len();

        if output_row_count == 0 {
            // No output rows, return empty batch
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        // Build output columns
        let mut output_columns: Vec<ArrayRef> = Vec::new();

        // First, replicate the projected columns
        for proj_expr in &self.projections {
            let column = proj_expr.evaluate(batch)?;
            let column = column.into_array(batch.num_rows())?;

            // Use MutableArrayData to efficiently replicate rows
            let column_data = column.to_data();
            let mut mutable = MutableArrayData::new(vec![&column_data], false, output_row_count);

            for &row_idx in &row_indices {
                mutable.extend(0, row_idx, row_idx + 1);
            }

            output_columns.push(make_array(mutable.freeze()));
        }

        // Now add the exploded array element column
        // Get the element type from the list array
        let element_type = match list_array.data_type() {
            DataType::List(field) => field.data_type().clone(),
            DataType::LargeList(field) => field.data_type().clone(),
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported array type for explode: {:?}",
                    list_array.data_type()
                )))
            }
        };

        // Extract all array values into a flat structure
        let mut all_arrays = Vec::new();
        for row_idx in 0..batch.num_rows() {
            if !list_array.is_null(row_idx) {
                all_arrays.push(list_array.value(row_idx));
            }
        }

        // Build the exploded element column
        if all_arrays.is_empty() {
            // All arrays were null/empty, create a null array
            let null_array = arrow::array::new_null_array(&element_type, output_row_count);
            output_columns.push(null_array);
        } else {
            let array_data_refs: Vec<_> = all_arrays.iter().map(|a| a.to_data()).collect();
            let array_data_refs_borrowed: Vec<_> = array_data_refs.iter().collect();
            // Use `true` for nullable parameter to support extend_nulls()
            let mut element_mutable =
                MutableArrayData::new(array_data_refs_borrowed, true, output_row_count);

            // Build a mapping from row_idx to which array it came from
            let mut row_to_array_idx = vec![None; batch.num_rows()];
            let mut array_idx = 0;
            for (row_idx, item) in row_to_array_idx
                .iter_mut()
                .enumerate()
                .take(batch.num_rows())
            {
                if !list_array.is_null(row_idx) {
                    *item = Some(array_idx);
                    array_idx += 1;
                }
            }

            for elem_info in &array_element_indices {
                if let Some((row_idx, elem_idx)) = elem_info {
                    if let Some(arr_idx) = row_to_array_idx[*row_idx] {
                        element_mutable.extend(arr_idx, *elem_idx, *elem_idx + 1);
                    } else {
                        // This shouldn't happen, but handle gracefully
                        element_mutable.extend_nulls(1);
                    }
                } else {
                    // This is a null element (from empty or null array with outer=true)
                    element_mutable.extend_nulls(1);
                }
            }

            output_columns.push(make_array(element_mutable.freeze()));
        }

        let options = RecordBatchOptions::new().with_row_count(Some(output_row_count));
        RecordBatch::try_new_with_options(Arc::clone(&self.schema), output_columns, &options)
            .map_err(|e| e.into())
    }
}

impl Stream for ExplodeStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = self.child_stream.poll_next_unpin(cx);
        match next {
            Poll::Ready(Some(Ok(batch))) => {
                let result = self.explode(&batch);
                Poll::Ready(Some(result))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for ExplodeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
