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

//! Builder for the native `MergeRows` operator, which applies the matched and not-matched
//! instructions of a row-level `MERGE INTO`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_operators::ExpandExec;
use datafusion_comet_proto::spark_operator::{self, Operator};
use jni::objects::{Global, JObject};

use super::{operator_registry::OperatorBuilder, PhysicalPlanner, PlanCreationResult};
use crate::{
    execution::{
        operators::{ExecutionError, MergeActionContext, MergeInstructionExec, MergeRowsExec},
        serde::to_arrow_datatype,
        spark_plan::SparkPlan,
    },
    extract_op,
};

/// Builder for `MergeRows`.
pub struct MergeRowsBuilder;

impl OperatorBuilder for MergeRowsBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let merge = extract_op!(spark_plan, MergeRows);
        let children = &spark_plan.children;

        let [child] = children.as_slice() else {
            return Err(ExecutionError::GeneralError(format!(
                "MergeRows expects exactly one child, got {}",
                children.len()
            )));
        };
        let (scans, shuffle_scans, child) = planner.create_plan(child, inputs, partition_count)?;

        let missing_field =
            |name: &str| ExecutionError::GeneralError(format!("MergeRows proto missing `{name}`"));
        let is_source_row_present = planner.create_expr(
            merge
                .is_source_row_present
                .as_ref()
                .ok_or_else(|| missing_field("is_source_row_present"))?,
            child.schema(),
        )?;
        let is_target_row_present = planner.create_expr(
            merge
                .is_target_row_present
                .as_ref()
                .ok_or_else(|| missing_field("is_target_row_present"))?,
            child.schema(),
        )?;

        let compile_instructions = |instrs: &[spark_operator::MergeInstruction]| -> Result<
            Vec<MergeInstructionExec>,
            ExecutionError,
        > {
            instrs
                .iter()
                .map(|instr| {
                    let condition = planner.create_expr(
                        instr
                            .condition
                            .as_ref()
                            .ok_or_else(|| missing_field("instruction condition"))?,
                        child.schema(),
                    )?;
                    let outputs = instr
                        .outputs
                        .iter()
                        .map(|row| {
                            row.exprs
                                .iter()
                                .map(|e| planner.create_expr(e, child.schema()))
                                .collect::<Result<Vec<_>, _>>()
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let context = match instr.context {
                        None | Some(0) => None,
                        Some(1) => Some(MergeActionContext::Copy),
                        Some(2) => Some(MergeActionContext::Delete),
                        Some(3) => Some(MergeActionContext::Insert),
                        Some(4) => Some(MergeActionContext::Update),
                        Some(value) => {
                            return Err(ExecutionError::GeneralError(format!(
                                "MergeRows instruction has unknown action context {value}"
                            )))
                        }
                    };
                    Ok(MergeInstructionExec {
                        condition,
                        outputs,
                        context,
                    })
                })
                .collect()
        };

        let matched_instructions = compile_instructions(&merge.matched_instructions)?;
        let not_matched_instructions = compile_instructions(&merge.not_matched_instructions)?;
        let not_matched_by_source_instructions =
            compile_instructions(&merge.not_matched_by_source_instructions)?;

        // Spark's declared MergeRows output is the contract presented to the downstream
        // V2 write. Derive the expression schema as an independent check, then stamp native
        // batches with Spark's declared types so compatible nested-nullability widening does
        // not leak a narrower physical schema across the JVM/native boundary.
        let output_rows: Vec<Vec<Arc<dyn PhysicalExpr>>> = matched_instructions
            .iter()
            .chain(&not_matched_instructions)
            .chain(&not_matched_by_source_instructions)
            .flat_map(|instr| instr.outputs.iter().cloned())
            .collect();
        let expected_fields: Vec<Field> = merge
            .output_types
            .iter()
            .map(to_arrow_datatype)
            .enumerate()
            .map(|(idx, dt)| Field::new(format!("col_{idx}"), dt, true))
            .collect();
        let schema = Arc::new(Schema::new(expected_fields));

        if !output_rows.is_empty() {
            let derived_schema = ExpandExec::build_schema(&output_rows, &child.schema())?;
            if derived_schema.fields().len() != schema.fields().len() {
                return Err(ExecutionError::GeneralError(format!(
                    "MergeRows projected {} columns but Spark declared {}",
                    derived_schema.fields().len(),
                    schema.fields().len()
                )));
            }
            for (idx, (actual, expected)) in derived_schema
                .fields()
                .iter()
                .zip(schema.fields().iter())
                .enumerate()
            {
                if !merge_output_type_compatible(actual.data_type(), expected.data_type()) {
                    return Err(ExecutionError::GeneralError(format!(
                        "MergeRows output column {idx} has projected type {:?}, incompatible with Spark output type {:?}",
                        actual.data_type(),
                        expected.data_type()
                    )));
                }
            }
        }

        let exec = Arc::new(MergeRowsExec::try_new_with_semantic_metrics(
            is_source_row_present,
            is_target_row_present,
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            merge.row_id_ordinal.map(|ord| ord as usize),
            merge.semantic_metrics_required,
            Arc::clone(&child.native_plan),
            schema,
        )?);

        Ok((
            scans,
            shuffle_scans,
            Arc::new(SparkPlan::new(spark_plan.plan_id, exec, vec![child])),
        ))
    }
}

/// Returns true when `actual` and `expected` have the same nested shape and leaf types.
/// Nested field nullability is intentionally ignored: MergeRows normalizes runtime batches to
/// Spark's declared output schema, validating any nullability narrowing against the actual data.
fn merge_output_type_compatible(actual: &DataType, expected: &DataType) -> bool {
    match (actual, expected) {
        (DataType::Struct(actual_fields), DataType::Struct(expected_fields)) => {
            actual_fields.len() == expected_fields.len()
                && actual_fields.iter().zip(expected_fields.iter()).all(
                    |(actual_field, expected_field)| {
                        actual_field.name() == expected_field.name()
                            && merge_output_type_compatible(
                                actual_field.data_type(),
                                expected_field.data_type(),
                            )
                    },
                )
        }
        (DataType::List(actual_field), DataType::List(expected_field))
        | (DataType::LargeList(actual_field), DataType::LargeList(expected_field)) => {
            merge_output_type_compatible(actual_field.data_type(), expected_field.data_type())
        }
        (
            DataType::FixedSizeList(actual_field, actual_size),
            DataType::FixedSizeList(expected_field, expected_size),
        ) => {
            actual_size == expected_size
                && merge_output_type_compatible(
                    actual_field.data_type(),
                    expected_field.data_type(),
                )
        }
        (
            DataType::Map(actual_entries, actual_sorted),
            DataType::Map(expected_entries, expected_sorted),
        ) => {
            actual_sorted == expected_sorted
                && merge_output_type_compatible(
                    actual_entries.data_type(),
                    expected_entries.data_type(),
                )
        }
        _ => actual == expected,
    }
}
