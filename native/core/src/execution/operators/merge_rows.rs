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

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow::compute::kernels::boolean::{and, and_not, not};
use arrow::compute::{filter_record_batch, prep_null_mask_filter};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::utils::memory::estimate_memory_size;
use datafusion::common::{DataFusionError, HashSet, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream,
    },
};
use datafusion_comet_common::{cast_and_stamp_schema, SparkError};
use futures::{Stream, StreamExt};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// A MergeRows instruction: condition plus zero (Discard), one (Keep), or two (Split)
/// output row projections.
#[derive(Debug, Clone)]
pub struct MergeInstructionExec {
    pub condition: Arc<dyn PhysicalExpr>,
    pub outputs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
}

#[derive(Debug)]
struct MergeConfig {
    is_source_row_present: Arc<dyn PhysicalExpr>,
    is_target_row_present: Arc<dyn PhysicalExpr>,
    matched_instructions: Vec<MergeInstructionExec>,
    not_matched_instructions: Vec<MergeInstructionExec>,
    not_matched_by_source_instructions: Vec<MergeInstructionExec>,
    row_id_ordinal: Option<usize>,
}

impl MergeConfig {
    fn validate(
        &self,
        child: &Arc<dyn ExecutionPlan>,
        output_schema: &SchemaRef,
    ) -> Result<(), DataFusionError> {
        if let Some(ordinal) = self.row_id_ordinal {
            let child_schema = child.schema();
            let child_fields = child_schema.fields().len();
            if ordinal >= child_fields {
                return Err(DataFusionError::Internal(format!(
                    "MergeRows: row id ordinal {ordinal} is out of range for a child with \
                     {child_fields} columns"
                )));
            }
            let data_type = child_schema.field(ordinal).data_type();
            if data_type != &DataType::Int64 {
                return Err(DataFusionError::Internal(format!(
                    "MergeRows: row id column at ordinal {ordinal} must be Int64, got {data_type}"
                )));
            }
        }

        let output_width = output_schema.fields().len();
        for (group, instructions) in [
            ("matched", &self.matched_instructions),
            ("not matched", &self.not_matched_instructions),
            (
                "not matched by source",
                &self.not_matched_by_source_instructions,
            ),
        ] {
            for (instruction_index, instruction) in instructions.iter().enumerate() {
                if instruction.outputs.len() > 2 {
                    return Err(DataFusionError::Internal(format!(
                        "MergeRows: {group} instruction {instruction_index} has {} output rows; expected at most 2",
                        instruction.outputs.len()
                    )));
                }
                for (output_index, output) in instruction.outputs.iter().enumerate() {
                    if output.len() != output_width {
                        return Err(DataFusionError::Internal(format!(
                            "MergeRows: {group} instruction {instruction_index} output {output_index} has {} expressions; expected {output_width}",
                            output.len()
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MergeRowsExec {
    config: Arc<MergeConfig>,
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl MergeRowsExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        is_source_row_present: Arc<dyn PhysicalExpr>,
        is_target_row_present: Arc<dyn PhysicalExpr>,
        matched_instructions: Vec<MergeInstructionExec>,
        not_matched_instructions: Vec<MergeInstructionExec>,
        not_matched_by_source_instructions: Vec<MergeInstructionExec>,
        row_id_ordinal: Option<usize>,
        child: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let config = Arc::new(MergeConfig {
            is_source_row_present,
            is_target_row_present,
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            row_id_ordinal,
        });
        config.validate(&child, &schema)?;

        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            config,
            child,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for MergeRowsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometMergeRowsExec")
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for MergeRowsExec {
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
        let [child] = children.as_slice() else {
            return Err(DataFusionError::Internal(format!(
                "MergeRows expects exactly one child, got {}",
                children.len()
            )));
        };
        let child = Arc::clone(child);
        self.config.validate(&child, &self.schema)?;
        Ok(Arc::new(MergeRowsExec {
            config: Arc::clone(&self.config),
            child,
            schema: Arc::clone(&self.schema),
            cache: Arc::clone(&self.cache),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let reservation = MemoryConsumer::new(format!("CometMergeRowsExec[{partition}]"))
            .register(&context.runtime_env().memory_pool);
        let child_stream = self.child.execute(partition, Arc::clone(&context))?;
        Ok(Box::pin(MergeRowsStream {
            config: Arc::clone(&self.config),
            child_stream,
            schema: Arc::clone(&self.schema),
            seen: HashSet::new(),
            reservation,
            baseline: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn name(&self) -> &str {
        "CometMergeRowsExec"
    }
}

pub struct MergeRowsStream {
    config: Arc<MergeConfig>,
    child_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    // Partition-scoped so duplicate matches across Arrow batches are still detected.
    seen: HashSet<i64>,
    reservation: MemoryReservation,
    baseline: BaselineMetrics,
}

const SEEN_FIXED_BYTES: usize = std::mem::size_of::<HashSet<i64>>();
// Covers hashbrown's mirrored control group and minimum small-table allocation.
const SEEN_HASH_TABLE_SLACK_BYTES: usize = 64;

fn estimate_seen_memory_size(num_elements: usize) -> Result<usize, DataFusionError> {
    estimate_memory_size::<i64>(num_elements, SEEN_FIXED_BYTES)?
        .checked_add(SEEN_HASH_TABLE_SLACK_BYTES)
        .ok_or_else(|| {
            DataFusionError::ResourcesExhausted(
                "MergeRows: cardinality memory estimate overflow".to_string(),
            )
        })
}

/// Spark predicates treat NULL as false; Arrow boolean kernels preserve NULL.
fn null_to_false(array: &BooleanArray) -> BooleanArray {
    if array.null_count() == 0 {
        array.clone()
    } else {
        prep_null_mask_filter(array)
    }
}

fn eval_bool(
    expr: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<BooleanArray, DataFusionError> {
    let array: ArrayRef = expr.evaluate(batch)?.into_array(batch.num_rows())?;
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .map(null_to_false)
        .ok_or_else(|| DataFusionError::Internal("MergeRows: expected boolean array".to_string()))
}

fn project(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let mut columns = Vec::with_capacity(exprs.len());
    for expr in exprs {
        columns.push(expr.evaluate(batch)?.into_array(batch.num_rows())?);
    }
    // Different instructions can infer different nested nullability for the same output column.
    cast_and_stamp_schema("MergeRows", schema, columns, batch.num_rows())
}

fn filter_or_pass_through(
    batch: &RecordBatch,
    mask: &BooleanArray,
) -> Result<RecordBatch, DataFusionError> {
    if mask.true_count() == batch.num_rows() {
        Ok(batch.clone())
    } else {
        filter_record_batch(batch, mask).map_err(|e| e.into())
    }
}

/// Applies an ordered instruction group with Spark's first-match-wins semantics.
/// Output is grouped by the producing instruction; no physical row ordering is advertised.
fn run_group(
    batch: &RecordBatch,
    group_mask: &BooleanArray,
    instructions: &[MergeInstructionExec],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if instructions.is_empty() || group_mask.true_count() == 0 {
        return Ok(vec![]);
    }

    // Only rows routed to this group may evaluate its clause predicates.
    let mut current = filter_or_pass_through(batch, group_mask)?;
    let mut out = Vec::new();
    let last = instructions.len() - 1;

    for (idx, instr) in instructions.iter().enumerate() {
        if current.num_rows() == 0 {
            break;
        }

        // Remove claimed rows before evaluating later clauses. This matters for ANSI errors in
        // predicates that Spark would never evaluate after an earlier clause matched.
        let fire = match instr.condition.evaluate(&current)? {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                BooleanArray::from(vec![true; current.num_rows()])
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false) | None)) => continue,
            value => value
                .into_array(current.num_rows())?
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(null_to_false)
                .ok_or_else(|| {
                    DataFusionError::Internal("MergeRows: expected boolean array".to_string())
                })?,
        };

        if fire.true_count() == 0 {
            continue;
        }

        let filtered = filter_or_pass_through(&current, &fire)?;
        for output_exprs in &instr.outputs {
            out.push(project(&filtered, output_exprs, schema)?);
        }

        if idx != last {
            current = if fire.true_count() == current.num_rows() {
                current.slice(0, 0)
            } else {
                filter_record_batch(&current, &not(&fire)?)?
            };
        }
    }

    Ok(out)
}

fn cardinality_violation() -> DataFusionError {
    DataFusionError::External(Box::new(SparkError::MergeCardinalityViolation))
}

fn reserve_seen_growth(
    seen: &mut HashSet<i64>,
    reservation: &mut MemoryReservation,
) -> Result<(), DataFusionError> {
    let next_len = seen.len().checked_add(1).ok_or_else(|| {
        DataFusionError::ResourcesExhausted(
            "MergeRows: cardinality set length overflow".to_string(),
        )
    })?;
    let projected_bytes = estimate_seen_memory_size(next_len)?;
    let additional = projected_bytes.saturating_sub(reservation.size());
    reservation.try_grow(additional)?;

    if let Err(e) = seen.try_reserve(1) {
        if additional != 0 {
            reservation.shrink(additional);
        }
        return Err(DataFusionError::ResourcesExhausted(format!(
            "MergeRows: failed to allocate cardinality set: {e}"
        )));
    }
    Ok(())
}

/// Detects a target row matched by more than one source row.
fn check_cardinality(
    batch: &RecordBatch,
    matched_mask: &BooleanArray,
    row_id_ordinal: usize,
    seen: &mut HashSet<i64>,
    reservation: &mut MemoryReservation,
) -> Result<(), DataFusionError> {
    let row_ids = batch
        .column(row_id_ordinal)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("MergeRows: row id column must be Int64".to_string())
        })?;

    for i in matched_mask.values().set_indices() {
        // Spark's row-id read treats a null long slot as zero.
        let id = if row_ids.is_null(i) {
            0
        } else {
            row_ids.value(i)
        };

        if seen.len() < seen.capacity() {
            if !seen.insert(id) {
                return Err(cardinality_violation());
            }
        } else {
            // Preserve Spark's error precedence: detect duplicates before memory admission.
            if seen.contains(&id) {
                return Err(cardinality_violation());
            }
            reserve_seen_growth(seen, reservation)?;
            seen.insert(id);
        }
    }
    Ok(())
}

fn process_batch(
    batch: RecordBatch,
    config: &MergeConfig,
    seen: &mut HashSet<i64>,
    reservation: &mut MemoryReservation,
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let source_present = eval_bool(&config.is_source_row_present, &batch)?;
    let target_present = eval_bool(&config.is_target_row_present, &batch)?;

    let matched_mask = and(&target_present, &source_present)?;
    let not_matched_mask = and_not(&source_present, &target_present)?;
    let not_matched_by_source_mask = and_not(&target_present, &source_present)?;

    // Vectorized cardinality validation can surface before an unrelated per-row expression error;
    // both paths fail the query, but the error selected can differ from Spark in that rare case.
    if let Some(row_id_ordinal) = config.row_id_ordinal {
        check_cardinality(&batch, &matched_mask, row_id_ordinal, seen, reservation)?;
    }

    let mut batches = Vec::new();
    for (mask, instructions) in [
        (&matched_mask, &config.matched_instructions),
        (&not_matched_mask, &config.not_matched_instructions),
        (
            &not_matched_by_source_mask,
            &config.not_matched_by_source_instructions,
        ),
    ] {
        batches.extend(run_group(&batch, mask, instructions, schema)?);
    }

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }

    arrow::compute::concat_batches(schema, &batches).map_err(|e| e.into())
}

// Bound synchronous all-discard processing so a delete-heavy stream yields cooperatively.
const MAX_DISCARDED_BATCHES_PER_POLL: u32 = 128;

impl Stream for MergeRowsStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut discarded_budget = MAX_DISCARDED_BATCHES_PER_POLL;
        loop {
            let poll = match this.child_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    // Keep elapsed_compute scoped to this operator, not the upstream poll.
                    let _timer = this.baseline.elapsed_compute().timer();
                    let result = process_batch(
                        batch,
                        &this.config,
                        &mut this.seen,
                        &mut this.reservation,
                        &this.schema,
                    );
                    match result {
                        Ok(batch) if batch.num_rows() == 0 => {
                            discarded_budget -= 1;
                            if discarded_budget == 0 {
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                            continue;
                        }
                        other => Poll::Ready(Some(other)),
                    }
                }
                other => other,
            };
            return this.baseline.record_poll(poll);
        }
    }
}

impl RecordBatchStream for MergeRowsStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StructArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
    use datafusion::logical_expr::Operator as DFOperator;
    use datafusion::physical_expr::expressions::{binary, col, lit};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, true),
            Field::new("val", DataType::Int32, true),
            Field::new("target_present", DataType::Boolean, false),
            Field::new("source_present", DataType::Boolean, false),
        ]))
    }

    fn test_batch(
        row_ids: Vec<i64>,
        vals: Vec<i32>,
        target: Vec<bool>,
        source: Vec<bool>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(row_ids)),
                Arc::new(Int32Array::from(vals)),
                Arc::new(BooleanArray::from(target)),
                Arc::new(BooleanArray::from(source)),
            ],
        )
        .unwrap()
    }

    fn test_reservation() -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        MemoryConsumer::new("test").register(&pool)
    }

    fn out_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, true)]))
    }

    fn keep_all() -> MergeInstructionExec {
        MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![col("val", &test_schema()).unwrap()]],
        }
    }

    fn discard_all() -> MergeInstructionExec {
        MergeInstructionExec {
            condition: lit(true),
            outputs: vec![],
        }
    }

    fn test_config(
        matched_instructions: Vec<MergeInstructionExec>,
        not_matched_instructions: Vec<MergeInstructionExec>,
        not_matched_by_source_instructions: Vec<MergeInstructionExec>,
        row_id_ordinal: Option<usize>,
    ) -> MergeConfig {
        MergeConfig {
            is_source_row_present: col("source_present", &test_schema()).unwrap(),
            is_target_row_present: col("target_present", &test_schema()).unwrap(),
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            row_id_ordinal,
        }
    }

    #[test]
    fn keep_matched_discard_rest() {
        let batch = test_batch(
            vec![1, 2, 3],
            vec![10, 20, 30],
            vec![true, false, true],
            vec![true, true, false],
        );
        let config = test_config(
            vec![keep_all()],
            vec![keep_all()],
            vec![discard_all()],
            None,
        );
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut got: Vec<i32> = vals.iter().flatten().collect();
        got.sort();
        assert_eq!(got, vec![10, 20]);
    }

    #[test]
    fn first_match_wins_ordering() {
        let batch = test_batch(vec![1], vec![5], vec![true], vec![true]);
        let cond_false = MergeInstructionExec {
            condition: binary(
                col("val", &test_schema()).unwrap(),
                DFOperator::Gt,
                lit(100i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(1i32)]],
        };
        let cond_true = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(2i32)]],
        };
        let config = test_config(vec![cond_false, cond_true], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.value(0), 2);
    }

    #[test]
    fn later_condition_is_not_evaluated_on_an_already_claimed_row() {
        let batch = test_batch(vec![1, 2], vec![0, 2], vec![true, true], vec![true, true]);
        let claims_zero = MergeInstructionExec {
            condition: binary(
                col("val", &test_schema()).unwrap(),
                DFOperator::Eq,
                lit(0i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(111i32)]],
        };
        let divides_by_val = MergeInstructionExec {
            condition: binary(
                binary(
                    lit(2i32),
                    DFOperator::Divide,
                    col("val", &test_schema()).unwrap(),
                    &test_schema(),
                )
                .unwrap(),
                DFOperator::Gt,
                lit(0i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(222i32)]],
        };
        let config = test_config(vec![claims_zero, divides_by_val], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut got: Vec<i32> = vals.iter().flatten().collect();
        got.sort();
        assert_eq!(got, vec![111, 222]);
    }

    #[test]
    fn null_condition_falls_through_to_next_instruction() {
        let batch = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int32Array::from(vec![None::<i32>])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )
        .unwrap();
        let cond_null = MergeInstructionExec {
            condition: binary(
                col("val", &test_schema()).unwrap(),
                DFOperator::Gt,
                lit(100i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(1i32)]],
        };
        let keep_catch_all = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(2i32)]],
        };
        let config = test_config(vec![cond_null, keep_catch_all], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        assert_eq!(
            out.num_rows(),
            1,
            "row with a NULL clause condition must fall through to the catch-all Keep, not \
             disappear from the rewritten data file"
        );
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.value(0), 2);
    }

    #[test]
    fn null_row_presence_flag_treated_as_false() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, true),
            Field::new("val", DataType::Int32, true),
            Field::new("target_present", DataType::Boolean, true),
            Field::new("source_present", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(BooleanArray::from(vec![None::<bool>])),
            ],
        )
        .unwrap();
        let config = MergeConfig {
            is_source_row_present: col("source_present", &schema).unwrap(),
            is_target_row_present: col("target_present", &schema).unwrap(),
            matched_instructions: vec![],
            not_matched_instructions: vec![],
            not_matched_by_source_instructions: vec![MergeInstructionExec {
                condition: lit(true),
                outputs: vec![vec![col("val", &schema).unwrap()]],
            }],
            row_id_ordinal: None,
        };
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 1);
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.value(0), 10);
    }

    #[test]
    fn condition_not_evaluated_outside_its_group() {
        let batch = test_batch(vec![1, 2], vec![0, 5], vec![true, false], vec![true, true]);
        let div_cond = MergeInstructionExec {
            condition: binary(
                binary(
                    lit(10i32),
                    DFOperator::Divide,
                    col("val", &test_schema()).unwrap(),
                    &test_schema(),
                )
                .unwrap(),
                DFOperator::Gt,
                lit(1i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![col("val", &test_schema()).unwrap()]],
        };
        assert!(
            eval_bool(&div_cond.condition, &batch).is_err(),
            "test is only meaningful if batch-wide evaluation of this condition errors"
        );
        let config = test_config(vec![keep_all()], vec![div_cond], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .expect("not-matched condition must not be evaluated against the matched row");
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut got: Vec<i32> = vals.iter().flatten().collect();
        got.sort();
        assert_eq!(got, vec![0, 5]);
    }

    fn bounded_reservation(limit: usize) -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        MemoryConsumer::new("test").register(&pool)
    }

    fn run_cardinality(
        n: usize,
        batch_rows: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<HashSet<i64>, DataFusionError> {
        let mut seen = HashSet::new();
        let mut next = 0i64;
        while (next as usize) < n {
            let end = ((next as usize) + batch_rows).min(n) as i64;
            let ids: Vec<i64> = (next..end).collect();
            let len = ids.len();
            let batch = test_batch(ids, vec![0; len], vec![true; len], vec![true; len]);
            let mask = BooleanArray::from(vec![true; len]);
            check_cardinality(&batch, &mask, 0, &mut seen, reservation)?;
            next = end;
        }
        Ok(seen)
    }

    fn assert_seen_fully_reserved(seen: &HashSet<i64>, reservation: &MemoryReservation) {
        let actual = seen.allocation_size().saturating_add(SEEN_FIXED_BYTES);
        assert!(
            reservation.size() >= actual,
            "reserved {} bytes < actual HashSet footprint {} bytes (len={}, capacity={})",
            reservation.size(),
            actual,
            seen.len(),
            seen.capacity()
        );
    }

    #[test]
    fn cardinality_state_is_accounted_to_the_memory_pool() {
        let mut reservation = test_reservation();
        let seen = run_cardinality(9, 4, &mut reservation).unwrap();
        assert!(
            reservation.size() > 0,
            "`seen` must be visible to the memory pool"
        );
        assert_seen_fully_reserved(&seen, &reservation);
    }

    #[test]
    fn cardinality_reservation_covers_actual_hashbrown_allocations() {
        for &(n, batch_rows) in &[
            (1usize, 1usize),
            (2, 1),
            (3, 1),
            (7, 1),
            (8, 1),
            (9, 1),
            (17, 1),
            (64, 8),
            (200, 16),
        ] {
            let mut reservation = test_reservation();
            let seen = run_cardinality(n, batch_rows, &mut reservation).unwrap();
            assert_seen_fully_reserved(&seen, &reservation);
        }
    }

    #[test]
    fn cardinality_state_cannot_exceed_a_bounded_pool() {
        let n = 917_505;
        let mut reservation = bounded_reservation(16 * 1024 * 1024);
        let err = run_cardinality(n, 4096, &mut reservation).unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "expected the pool to reject the oversized cardinality table, got {err}"
        );

        let needed = estimate_seen_memory_size(n).unwrap();
        let mut ok_reservation = bounded_reservation(needed + 8 * 1024 * 1024);
        let seen = run_cardinality(n, 4096, &mut ok_reservation).unwrap();
        assert_eq!(seen.len(), n);
        assert_seen_fully_reserved(&seen, &ok_reservation);
    }

    #[test]
    fn cardinality_violation_wins_over_memory_exhaustion() {
        let mut seen = HashSet::new();
        let mut warmup_reservation = test_reservation();
        let mut next_id = 1i64;

        while seen.len() < seen.capacity().max(1) {
            let batch = test_batch(vec![next_id], vec![0], vec![true], vec![true]);
            check_cardinality(
                &batch,
                &BooleanArray::from(vec![true]),
                0,
                &mut seen,
                &mut warmup_reservation,
            )
            .unwrap();
            next_id += 1;
        }
        assert_eq!(seen.len(), seen.capacity(), "table must be full");

        let duplicate = test_batch(vec![1], vec![0], vec![true], vec![true]);
        let mut zero_budget = bounded_reservation(0);
        let duplicate_err = check_cardinality(
            &duplicate,
            &BooleanArray::from(vec![true]),
            0,
            &mut seen,
            &mut zero_budget,
        )
        .unwrap_err();
        assert!(
            duplicate_err
                .to_string()
                .contains("MERGE_CARDINALITY_VIOLATION"),
            "expected cardinality violation before pool admission, got {duplicate_err}"
        );

        let new_id = test_batch(vec![next_id], vec![0], vec![true], vec![true]);
        let memory_err = check_cardinality(
            &new_id,
            &BooleanArray::from(vec![true]),
            0,
            &mut seen,
            &mut zero_budget,
        )
        .unwrap_err();
        assert!(
            matches!(memory_err, DataFusionError::ResourcesExhausted(_)),
            "expected memory exhaustion for a new id at capacity, got {memory_err}"
        );
    }

    #[test]
    fn cardinality_violation_detected() {
        let batch = test_batch(vec![1, 1], vec![10, 20], vec![true, true], vec![true, true]);
        let matched_mask = BooleanArray::from(vec![true, true]);
        let mut seen = HashSet::new();
        let result =
            check_cardinality(&batch, &matched_mask, 0, &mut seen, &mut test_reservation());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }

    #[test]
    fn split_produces_two_rows() {
        let batch = test_batch(vec![1], vec![7], vec![true], vec![true]);
        let split = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(1i32)], vec![lit(2i32)]],
        };
        let config = test_config(vec![split], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut HashSet::new(),
            &mut test_reservation(),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let got: Vec<i32> = vals.iter().flatten().collect();
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn cardinality_violation_detected_for_null_row_ids() {
        let batch = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![None, None])),
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(BooleanArray::from(vec![true, true])),
                Arc::new(BooleanArray::from(vec![true, true])),
            ],
        )
        .unwrap();
        let matched_mask = BooleanArray::from(vec![true, true]);
        let result = check_cardinality(
            &batch,
            &matched_mask,
            0,
            &mut HashSet::new(),
            &mut test_reservation(),
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }

    #[tokio::test]
    async fn all_discarded_batch_is_not_emitted() {
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::prelude::SessionContext;

        let discarded = test_batch(vec![1], vec![10], vec![true], vec![true]);
        let kept = test_batch(vec![2], vec![20], vec![false], vec![true]);
        let source =
            MemorySourceConfig::try_new_exec(&[vec![discarded, kept]], test_schema(), None)
                .unwrap();

        let exec = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![discard_all()],
            vec![keep_all()],
            vec![],
            None,
            source,
            out_schema(),
        )
        .unwrap();

        let ctx = SessionContext::new();
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn instruction_with_more_than_two_outputs_is_rejected() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let invalid = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(1i32)], vec![lit(2i32)], vec![lit(3i32)]],
        };
        let err = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![invalid],
            vec![],
            vec![],
            None,
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected at most 2"));
    }

    #[test]
    fn instruction_output_width_must_match_operator_schema() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let invalid = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(1i32), lit(2i32)]],
        };
        let err = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![invalid],
            vec![],
            vec![],
            None,
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected 1"));
    }

    #[test]
    fn out_of_range_row_id_ordinal_is_rejected() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let err = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![keep_all()],
            vec![],
            vec![],
            Some(99),
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("row id ordinal"));
    }

    #[test]
    fn non_int64_row_id_is_rejected_at_plan_construction() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let wrong_schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
            Field::new("target_present", DataType::Boolean, false),
            Field::new("source_present", DataType::Boolean, false),
        ]));
        let source =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&wrong_schema), None).unwrap();
        let err = MergeRowsExec::try_new(
            col("source_present", &wrong_schema).unwrap(),
            col("target_present", &wrong_schema).unwrap(),
            vec![],
            vec![],
            vec![],
            Some(0),
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("must be Int64"));
    }

    #[test]
    fn with_new_children_rejects_ordinal_out_of_range_for_new_child() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let original_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("row_id", DataType::Int64, true),
        ]));
        let source =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&original_schema), None)
                .unwrap();
        let exec = Arc::new(
            MergeRowsExec::try_new(
                lit(true),
                lit(true),
                vec![],
                vec![],
                vec![],
                Some(3),
                source,
                out_schema(),
            )
            .unwrap(),
        );

        let narrow_schema = Arc::new(Schema::new(vec![Field::new(
            "row_id",
            DataType::Int64,
            true,
        )]));
        let narrow_child =
            MemorySourceConfig::try_new_exec(&[vec![]], narrow_schema, None).unwrap();
        let err = exec.with_new_children(vec![narrow_child]).unwrap_err();
        assert!(
            err.to_string().contains("out of range"),
            "expected an out-of-range ordinal error, got: {err}"
        );
    }

    #[test]
    fn with_new_children_rejects_wrong_arity() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let exec = Arc::new(
            MergeRowsExec::try_new(
                col("source_present", &test_schema()).unwrap(),
                col("target_present", &test_schema()).unwrap(),
                vec![keep_all()],
                vec![],
                vec![],
                Some(0),
                source,
                out_schema(),
            )
            .unwrap(),
        );
        let no_children = Arc::clone(&exec).with_new_children(vec![]).unwrap_err();
        assert!(no_children.to_string().contains("exactly one child"));

        let child_a = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let child_b = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let two_children = exec.with_new_children(vec![child_a, child_b]).unwrap_err();
        assert!(two_children.to_string().contains("exactly one child"));
    }

    #[test]
    fn with_new_children_revalidates_row_id_schema() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let exec = Arc::new(
            MergeRowsExec::try_new(
                col("source_present", &test_schema()).unwrap(),
                col("target_present", &test_schema()).unwrap(),
                vec![keep_all()],
                vec![],
                vec![],
                Some(0),
                source,
                out_schema(),
            )
            .unwrap(),
        );

        let narrow_schema = Arc::new(Schema::new(vec![Field::new(
            "only_col",
            DataType::Int32,
            true,
        )]));
        let narrow_child =
            MemorySourceConfig::try_new_exec(&[vec![]], narrow_schema, None).unwrap();
        let err = exec.with_new_children(vec![narrow_child]).unwrap_err();
        assert!(err.to_string().contains("must be Int64"));
    }

    #[test]
    fn cardinality_violation_detected_across_batches() {
        let mut seen = HashSet::new();
        let batch1 = test_batch(vec![1], vec![10], vec![true], vec![true]);
        let batch2 = test_batch(vec![1], vec![20], vec![true], vec![true]);
        let config = test_config(vec![keep_all()], vec![], vec![], Some(0));

        let first = process_batch(
            batch1,
            &config,
            &mut seen,
            &mut test_reservation(),
            &out_schema(),
        );
        assert!(first.is_ok());

        let second = process_batch(
            batch2,
            &config,
            &mut seen,
            &mut test_reservation(),
            &out_schema(),
        );
        assert!(second.is_err());
        assert!(second
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }

    #[test]
    fn project_reconciles_nested_struct_nullability_with_declared_schema() {
        let source_field = Field::new("n", DataType::Boolean, false);
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(vec![source_field.clone()].into()),
            true,
        )]));
        let inner_values: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let payload_array: ArrayRef = Arc::new(StructArray::new(
            vec![source_field].into(),
            vec![inner_values],
            None,
        ));
        let batch = RecordBatch::try_new(Arc::clone(&source_schema), vec![payload_array]).unwrap();

        let target_field = Field::new("n", DataType::Boolean, true);
        let out_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(vec![target_field].into()),
            true,
        )]));

        let out = project(
            &batch,
            &[col("payload", &source_schema).unwrap()],
            &out_schema,
        )
        .expect("project must reconcile projected nested nullability with the declared schema");
        assert_eq!(
            out.schema().field(0).data_type(),
            out_schema.field(0).data_type()
        );
    }
}
