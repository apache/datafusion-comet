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

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{exec_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    apply_expression_roots, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream,
};
use futures::stream;
use futures::StreamExt;

use crate::execution::python_udf::ArrowPythonUdf;

#[derive(Debug, Clone)]
pub struct ArrowPythonUdfSpec {
    pub command: Vec<u8>,
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    pub arg_names: Vec<String>,
    pub return_type: DataType,
    pub return_name: String,
    pub python_version: String,
}

/// Evaluates scalar PyArrow UDFs inside the native pipeline. Workers are
/// instantiated in `execute`, once per partition. Python module state remains
/// shared by every task in the executor's embedded interpreter.
#[derive(Debug)]
pub struct ArrowPythonUdfExec {
    child: Arc<dyn ExecutionPlan>,
    specs: Vec<ArrowPythonUdfSpec>,
    max_records_per_batch: usize,
    max_bytes_per_batch: usize,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl ArrowPythonUdfExec {
    pub fn try_new(
        child: Arc<dyn ExecutionPlan>,
        specs: Vec<ArrowPythonUdfSpec>,
        max_records_per_batch: i32,
        max_bytes_per_batch: i64,
    ) -> Result<Self> {
        if specs.is_empty() {
            return exec_err!("ArrowPythonUdfExec requires at least one UDF");
        }
        let mut fields: Vec<Field> = child
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for spec in &specs {
            if spec.args.len() != spec.arg_names.len() {
                return exec_err!("ArrowPythonUdf argument names are not aligned with arguments");
            }
            for arg in &spec.args {
                arg.data_type(&child.schema())?;
            }
            fields.push(Field::new(
                &spec.return_name,
                spec.return_type.clone(),
                true,
            ));
        }
        let schema = Arc::new(Schema::new(fields));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            child.output_partitioning().clone(),
            EmissionType::Incremental,
            child.boundedness(),
        ));
        Ok(Self {
            child,
            specs,
            max_records_per_batch: max_records_per_batch.max(0) as usize,
            max_bytes_per_batch: max_bytes_per_batch.max(0) as usize,
            schema,
            cache,
        })
    }

    fn evaluate_args(
        specs: &[ArrowPythonUdfSpec],
        batch: &RecordBatch,
    ) -> Result<Vec<Vec<ArrayRef>>> {
        specs
            .iter()
            .map(|spec| {
                spec.args
                    .iter()
                    .map(|arg| arg.evaluate(batch)?.into_array(batch.num_rows()))
                    .collect::<Result<Vec<_>>>()
            })
            .collect()
    }

    // Spark's row-based Arrow writer checks its buffer size after each row, so
    // the row that reaches the byte limit remains in that batch. The native
    // path uses logical Arrow buffer sizes for its verified scalar types.
    fn input_bytes(args: &[Vec<ArrayRef>], offset: usize, length: usize) -> Result<usize> {
        if length == 0 {
            return Ok(0);
        }
        let mut bytes = 0usize;
        for array in args.iter().flatten() {
            let value_bytes = match array.data_type() {
                DataType::Boolean => length.div_ceil(8),
                DataType::Int8 | DataType::UInt8 => length,
                DataType::Int16 | DataType::UInt16 => length.saturating_mul(2),
                DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => {
                    length.saturating_mul(4)
                }
                DataType::Int64
                | DataType::UInt64
                | DataType::Float64
                | DataType::Timestamp(TimeUnit::Microsecond, None) => length.saturating_mul(8),
                DataType::Decimal128(_, _) => length.saturating_mul(16),
                DataType::Utf8 => {
                    let values = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(
                                "Arrow UDF string argument has an unexpected array type"
                                    .to_string(),
                            )
                        })?;
                    let offsets = values.value_offsets();
                    (offsets[offset + length] - offsets[offset]) as usize
                        + (length + 1).saturating_mul(4)
                }
                DataType::Binary => {
                    let values = array
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(
                                "Arrow UDF binary argument has an unexpected array type"
                                    .to_string(),
                            )
                        })?;
                    let offsets = values.value_offsets();
                    (offsets[offset + length] - offsets[offset]) as usize
                        + (length + 1).saturating_mul(4)
                }
                other => return exec_err!("Unsupported Arrow UDF argument type: {other}"),
            };
            bytes = bytes.saturating_add(value_bytes);
            // Arrow Java's getBufferSizeFor counts the validity bitmap even
            // when every value is non-null.
            bytes = bytes.saturating_add(length.div_ceil(8));
        }
        Ok(bytes)
    }

    fn next_batch_length(
        args: &[Vec<ArrayRef>],
        offset: usize,
        remaining: usize,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<usize> {
        let limit = if max_records == 0 {
            remaining
        } else {
            remaining.min(max_records)
        };
        if limit == 0 || max_bytes == 0 || args.iter().all(Vec::is_empty) {
            return Ok(limit);
        }
        if Self::input_bytes(args, offset, limit)? < max_bytes {
            return Ok(limit);
        }
        let (mut low, mut high) = (1, limit);
        while low < high {
            let middle = low + (high - low) / 2;
            if Self::input_bytes(args, offset, middle)? >= max_bytes {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        Ok(low)
    }

    fn evaluate_batch(
        specs: &[ArrowPythonUdfSpec],
        workers: &[ArrowPythonUdf],
        schema: SchemaRef,
        batch: &RecordBatch,
        args: &[Vec<ArrayRef>],
        offset: usize,
        length: usize,
    ) -> Result<RecordBatch> {
        let mut columns = batch.slice(offset, length).columns().to_vec();
        for ((spec, worker), function_args) in specs.iter().zip(workers).zip(args) {
            let sliced_args: Vec<_> = function_args
                .iter()
                .map(|array| array.slice(offset, length))
                .collect();
            columns.push(worker.evaluate_named(&sliced_args, &spec.arg_names, length)?);
        }
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

impl DisplayAs for ArrowPythonUdfExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "CometArrowPythonUdfExec: {} UDF(s)", self.specs.len())
            }
        }
    }
}

impl ExecutionPlan for ArrowPythonUdfExec {
    fn name(&self) -> &str {
        "CometArrowPythonUdfExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.specs.iter().flat_map(|spec| spec.args.iter()), f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return exec_err!("ArrowPythonUdfExec requires exactly one child");
        }
        Ok(Arc::new(Self::try_new(
            Arc::clone(&children[0]),
            self.specs.clone(),
            self.max_records_per_batch as i32,
            self.max_bytes_per_batch as i64,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.child.execute(partition, context)?;
        let workers: Vec<_> = self
            .specs
            .iter()
            .map(|spec| {
                ArrowPythonUdf::from_command(
                    &spec.command,
                    spec.return_type.clone(),
                    true,
                    true,
                    &spec.python_version,
                )
            })
            .collect::<std::result::Result<_, _>>()?;
        let workers = Arc::new(workers);
        let specs = Arc::new(self.specs.clone());
        let schema = Arc::clone(&self.schema);
        let max_records_per_batch = self.max_records_per_batch;
        let max_bytes_per_batch = self.max_bytes_per_batch;
        let stream = input.flat_map(move |batch| {
            let workers = Arc::clone(&workers);
            let specs = Arc::clone(&specs);
            let schema = Arc::clone(&schema);
            let (batch, args, mut error) = match batch {
                Ok(batch) => {
                    match tokio::task::block_in_place(|| Self::evaluate_args(&specs, &batch)) {
                        Ok(args) => (Some(batch), Some(args), None),
                        Err(error) => (None, None, Some(error)),
                    }
                }
                Err(error) => (None, None, Some(error)),
            };
            let mut offset = 0;
            let mut emitted_empty_batch = false;
            let mut failed = false;
            // RecordBatch::slice shares Arrow buffers. Produce one result per poll
            // so the remaining slices do not pin a second set of output batches.
            stream::iter(std::iter::from_fn(move || {
                if let Some(error) = error.take() {
                    return Some(Err(error));
                }
                if failed {
                    return None;
                }
                let batch = batch.as_ref()?;
                let args = args.as_ref()?;
                if offset == batch.num_rows() && (offset != 0 || emitted_empty_batch) {
                    return None;
                }
                let length = match Self::next_batch_length(
                    args,
                    offset,
                    batch.num_rows() - offset,
                    max_records_per_batch,
                    max_bytes_per_batch,
                ) {
                    Ok(length) => length,
                    Err(error) => {
                        failed = true;
                        return Some(Err(error));
                    }
                };
                // Keep the JVM scan path synchronous so its Pending loop does not spin while
                // Python runs. On a tokio worker, this hands its other tasks to another worker.
                let result = tokio::task::block_in_place(|| {
                    Self::evaluate_batch(
                        &specs,
                        &workers,
                        Arc::clone(&schema),
                        batch,
                        args,
                        offset,
                        length,
                    )
                });
                offset += length;
                if length == 0 {
                    emitted_empty_batch = true;
                }
                Some(result)
            }))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};

    use super::ArrowPythonUdfExec;

    #[test]
    fn byte_limit_splits_fixed_width_udf_arguments() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        let args = vec![vec![values]];
        assert_eq!(
            ArrowPythonUdfExec::next_batch_length(&args, 0, 4, 10_000, 16).unwrap(),
            2
        );
        assert_eq!(ArrowPythonUdfExec::input_bytes(&args, 0, 2).unwrap(), 17);
        assert_eq!(
            ArrowPythonUdfExec::next_batch_length(&args, 0, 4, 10_000, 9).unwrap(),
            1
        );
        assert_eq!(
            ArrowPythonUdfExec::next_batch_length(&args, 2, 2, 10_000, 16).unwrap(),
            2
        );
        assert_eq!(
            ArrowPythonUdfExec::next_batch_length(&args, 0, 4, 1, 16).unwrap(),
            1
        );
        assert_eq!(
            ArrowPythonUdfExec::next_batch_length(&args, 0, 4, 10_000, 0).unwrap(),
            4
        );
    }

    #[test]
    fn byte_limit_counts_all_arguments_and_keeps_oversized_row() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        let args = vec![vec![Arc::clone(&values), values]];
        assert_eq!(
            ArrowPythonUdfExec::next_batch_length(&args, 0, 4, 10_000, 16).unwrap(),
            1
        );
        assert_eq!(
            ArrowPythonUdfExec::next_batch_length(&args, 0, 4, 10_000, 8).unwrap(),
            1
        );
    }

    #[test]
    fn byte_limit_counts_variable_width_values_and_offsets() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "bb", "ccc", "d"]));
        let args = vec![vec![values]];
        // Two values use 3 data bytes, 3 four-byte offsets, and 1 validity byte.
        assert_eq!(ArrowPythonUdfExec::input_bytes(&args, 0, 2).unwrap(), 16);
        assert_eq!(
            ArrowPythonUdfExec::next_batch_length(&args, 0, 4, 10_000, 15).unwrap(),
            2
        );
        assert_eq!(
            ArrowPythonUdfExec::next_batch_length(&args, 2, 2, 10_000, 15).unwrap(),
            2
        );
    }
}
